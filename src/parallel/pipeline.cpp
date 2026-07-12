#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/settings.hpp"

#include <functional>
#include <thread>

#ifdef __linux__
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace duckdb {

static uint64_t PipelineTaskThreadId() {
#ifdef __linux__
	return static_cast<uint64_t>(syscall(SYS_gettid));
#else
	return static_cast<uint64_t>(std::hash<std::thread::id> {}(std::this_thread::get_id()));
#endif
}

static int PipelineTaskCurrentCPU() {
#ifdef __linux__
	return sched_getcpu();
#else
	return -1;
#endif
}

static bool PipelineTaskProfilingRequested(Pipeline &pipeline) {
	if (!pipeline.GetProfilerPipelineId()) {
		return false;
	}
	const auto &settings = ClientConfig::GetConfig(pipeline.GetClientContext()).pipeline_profiling;
	return settings.task_trace || settings.throughput ||
	       QueryRequestMetadataManager::HasActiveMetadata(pipeline.GetClientContext());
}

static idx_t SourceInputChunksEquiv(const SourceInputVolume &source_input_volume) {
	if (source_input_volume.chunks_equiv > 0) {
		return source_input_volume.chunks_equiv;
	}
	if (source_input_volume.rows > 0) {
		return SourceThroughputCounters::EstimateStandardChunks(source_input_volume.rows);
	}
	if (source_input_volume.native_units > 0) {
		return source_input_volume.native_units;
	}
	return 0;
}

static bool SourceInputUsesNativeWorkUnits(const SourceInputVolume &source_input_volume, idx_t chunks_equiv) {
	if (chunks_equiv == 0 || source_input_volume.native_units == 0 || source_input_volume.native_unit.empty()) {
		return false;
	}
	if (source_input_volume.rows > 0 &&
	    chunks_equiv == SourceThroughputCounters::EstimateStandardChunks(source_input_volume.rows)) {
		return false;
	}
	return chunks_equiv == source_input_volume.native_units;
}

static bool SourceWorkKindMatches(const string &planned_kind, const string &reported_kind) {
	if (planned_kind == reported_kind) {
		return true;
	}
	if (planned_kind == "table_rows_upper_bound" && reported_kind == "base_table_rows") {
		return true;
	}
	if (planned_kind == "index_scan_row_ids" && reported_kind == "index_rowids") {
		return true;
	}
	return false;
}

static bool SourceWorkUnitMatches(const string &planned_unit, const string &reported_unit) {
	if (planned_unit.empty() || planned_unit == "none") {
		return true;
	}
	return planned_unit == reported_unit;
}

static idx_t SaturatingAtomicAdd(atomic<idx_t> &target, idx_t delta, idx_t limit, idx_t *new_value = nullptr) {
	if (delta == 0) {
		if (new_value) {
			*new_value = target.load();
		}
		return 0;
	}
	if (limit == 0) {
		auto previous = target.fetch_add(delta);
		if (new_value) {
			*new_value = previous + delta;
		}
		return delta;
	}
	auto current = target.load();
	while (current < limit) {
		auto allowed = MinValue<idx_t>(delta, limit - current);
		if (target.compare_exchange_weak(current, current + allowed)) {
			if (new_value) {
				*new_value = current + allowed;
			}
			return allowed;
		}
	}
	if (new_value) {
		*new_value = current;
	}
	return 0;
}

PipelineTask::PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p) {
}

bool PipelineTask::TaskBlockedOnResult() const {
	// If this returns true, it means the pipeline this task belongs to has a cached chunk
	// that was the result of the Sink method returning BLOCKED
	return pipeline_executor->RemainingSinkChunk();
}

const PipelineExecutor &PipelineTask::GetPipelineExecutor() const {
	return *pipeline_executor;
}

TaskExecutionResult PipelineTask::ExecuteTask(TaskExecutionMode mode) {
	const bool energy_attribution_active = pipeline.GetEnergyAttributionQueryState() != nullptr;
	const bool profiler_task_requested = PipelineTaskProfilingRequested(pipeline);
	const bool live_throughput_requested = pipeline.SourceWorkTrackingEnabled();
	const bool task_timing_requested = profiler_task_requested || live_throughput_requested;
	uint64_t task_start_ns = 0;
	if (task_timing_requested) {
		task_start_ns = PipelineDVFSProfiler::TimestampNs();
	}
	int start_cpu = -1;
	if (energy_attribution_active || profiler_task_requested) {
		start_cpu = PipelineTaskCurrentCPU();
	}
	idx_t profiler_task_id = 0;
	if (profiler_task_requested) {
		profiler_task_id = pipeline.RecordProfilerTaskStart(PipelineTaskThreadId(), start_cpu);
	}
	EnergySegmentScope energy_scope(pipeline, start_cpu);
	auto finish_profiler_task = [&]() {
		auto empty_counters = SourceThroughputCounters();
		const auto &counters =
		    profiler_task_id && pipeline_executor ? pipeline_executor->GetSourceThroughputCounters() : empty_counters;
		idx_t pipeline_input_tuples = 0;
		idx_t pipeline_input_chunks = 0;
		if ((energy_attribution_active || profiler_task_id) && pipeline_executor) {
			const auto &pipeline_input = pipeline_executor->GetPipelineInputCounters();
			pipeline_input_tuples = pipeline_input.tuples;
			pipeline_input_chunks = pipeline_input.chunks;
		}
		if (energy_attribution_active) {
			energy_scope.SetWork(pipeline_input_tuples, pipeline_input_chunks);
		}
		int end_cpu = -1;
		if (energy_attribution_active || profiler_task_id) {
			end_cpu = PipelineTaskCurrentCPU();
		}
		if (energy_attribution_active) {
			energy_scope.SetEndCPUHint(end_cpu);
		}
		if (profiler_task_id || live_throughput_requested) {
			auto task_end_ns = PipelineDVFSProfiler::TimestampNs();
			if (pipeline_executor) {
				pipeline_executor->FinishThroughputTask(task_end_ns);
			}
			if (profiler_task_id) {
				pipeline.RecordProfilerTaskEnd(profiler_task_id, end_cpu, counters, pipeline_input_tuples,
				                               pipeline_input_chunks);
			}
		}
	};

	if (!pipeline_executor) {
		pipeline_executor = make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
	}
	pipeline_executor->SetPipelineEventForDebug(event.get());
	pipeline_executor->ResetSourceThroughputCounters();
	pipeline_executor->BeginThroughputTask(task_start_ns);

	pipeline_executor->SetTaskForInterrupts(shared_from_this());

	if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
		auto res = pipeline_executor->Execute(PARTIAL_CHUNK_COUNT);

		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			finish_profiler_task();
			return TaskExecutionResult::TASK_NOT_FINISHED;
		case PipelineExecuteResult::INTERRUPTED:
			finish_profiler_task();
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	} else {
		auto res = pipeline_executor->Execute();
		switch (res) {
		case PipelineExecuteResult::NOT_FINISHED:
			throw InternalException("Execute without limit should not return NOT_FINISHED");
		case PipelineExecuteResult::INTERRUPTED:
			finish_profiler_task();
			return TaskExecutionResult::TASK_BLOCKED;
		case PipelineExecuteResult::FINISHED:
			break;
		}
	}

	finish_profiler_task();
	event->FinishTask();
	pipeline_executor.reset();
	return TaskExecutionResult::TASK_FINISHED;
}

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), ready(false), initialized(false), source(nullptr), sink(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(ProgressData &progress) {
	D_ASSERT(source);
	idx_t source_cardinality = MinValue<idx_t>(source->estimated_cardinality, 1ULL << 48ULL);
	if (source_cardinality < 1) {
		source_cardinality = 1;
	}
	if (!initialized) {
		progress.done = 0;
		progress.total = double(source_cardinality);
		return true;
	}
	auto &client = executor.context;

	progress = source->GetProgress(client, *source_state);
	progress.Normalize(double(source_cardinality));
	progress = sink->GetSinkProgress(client, *sink->sink_state, progress);
	return progress.IsValid();
}

void Pipeline::ResetSourceWorkTracking() {
	source_work_tracking_enabled.store(false);
	source_work_debug_sampling_enabled.store(false);
	lock_guard<mutex> guard(source_work_lock);
	source_work_valid = false;
	source_work_uses_native_work_units = false;
	source_work_kind.clear();
	source_work_confidence.clear();
	source_work_native_unit.clear();
	source_work_total_rows = 0;
	source_work_total_chunks_equiv = 0;
	source_work_total_native_units = 0;
	source_work_source_max_threads = 0;
	source_work_effective_max_threads = 0;
	source_work_scalable = false;
	source_work_completed_rows.store(0);
	source_work_completed_chunks_equiv.store(0);
	source_work_completed_native_units.store(0);
	source_work_throughput_completed_chunks_equiv.store(0);
	source_work_throughput_worker_time_ns.store(0);
	source_work_debug_sample_step_chunks_equiv.store(0);
	source_work_debug_next_sample_chunks_equiv.store(0);
	source_work_debug_total_chunks_equiv.store(0);
}

void Pipeline::InitializeSourceWorkTracking(const SourceInputVolume &source_input_volume, idx_t source_max_threads,
                                            idx_t effective_max_threads) {
	auto tracking_enabled = executor.activation_scheduler != nullptr;
	// Live scheduler progress is tracked in chunk-equivalent work units. For many sources this is a native physical
	// unit (e.g. hash_table_chunk, sort_partition, aggregate_partition_phase), not necessarily a STANDARD_VECTOR_SIZE
	// row chunk. Historical scheduler rates must therefore be derived in this same chunks_equiv unit.
	auto total_chunks_equiv = SourceInputChunksEquiv(source_input_volume);
	auto uses_native_work_units = SourceInputUsesNativeWorkUnits(source_input_volume, total_chunks_equiv);
	auto debug_sampling_enabled =
	    tracking_enabled && total_chunks_equiv > 0 && QueryPipelineDebug::Enabled(GetClientContext());
	auto debug_sample_step = debug_sampling_enabled ? MaxValue<idx_t>(idx_t(1), total_chunks_equiv / 10) : idx_t(0);
	{
		lock_guard<mutex> guard(source_work_lock);
		source_work_valid = tracking_enabled;
		source_work_uses_native_work_units = uses_native_work_units;
		source_work_kind = source_input_volume.kind;
		source_work_confidence = source_input_volume.confidence;
		source_work_native_unit = source_input_volume.native_unit;
		source_work_total_rows = source_input_volume.rows;
		source_work_total_chunks_equiv = total_chunks_equiv;
		source_work_total_native_units = source_input_volume.native_units;
		source_work_source_max_threads = source_max_threads;
		source_work_effective_max_threads = effective_max_threads;
		source_work_scalable = source_work_total_chunks_equiv > 0 && effective_max_threads > 1;
		source_work_completed_rows.store(0);
		source_work_completed_chunks_equiv.store(0);
		source_work_completed_native_units.store(0);
		source_work_throughput_completed_chunks_equiv.store(0);
		source_work_throughput_worker_time_ns.store(0);
		source_work_debug_sample_step_chunks_equiv.store(debug_sample_step);
		source_work_debug_next_sample_chunks_equiv.store(debug_sample_step);
		source_work_debug_total_chunks_equiv.store(total_chunks_equiv);
	}
	source_work_tracking_enabled.store(tracking_enabled);
	source_work_debug_sampling_enabled.store(debug_sampling_enabled);
}

bool Pipeline::SourceWorkTrackingEnabled() const {
	return source_work_tracking_enabled.load();
}

SourceWorkCounterSnapshot Pipeline::GetMatchingSourceWorkCounters(const SourceThroughputCounters &counters) const {
	SourceWorkCounterSnapshot result;
	if (!SourceWorkTrackingEnabled()) {
		return result;
	}
	lock_guard<mutex> guard(source_work_lock);
	if (!source_work_valid) {
		return result;
	}
	if (source_work_uses_native_work_units) {
		if (!counters.work_reported || !SourceWorkKindMatches(source_work_kind, counters.work_kind) ||
		    !SourceWorkUnitMatches(source_work_native_unit, counters.work_unit)) {
			return result;
		}
		result.chunks_equiv = counters.work_units_touched;
		result.native_units = counters.work_units_touched;
		result.valid = true;
		return result;
	}
	if (!counters.reported || !SourceWorkKindMatches(source_work_kind, counters.tuple_kind)) {
		return result;
	}
	result.rows = counters.tuples_touched;
	result.chunks_equiv = counters.chunks_touched > 0 ? counters.chunks_touched
	                                                  : SourceThroughputCounters::EstimateStandardChunks(result.rows);
	result.native_units = counters.native_units_touched;
	result.valid = true;
	return result;
}

void Pipeline::RecordThroughputProgress(idx_t chunks_equiv, uint64_t worker_time_ns) {
	if (!SourceWorkTrackingEnabled() || worker_time_ns == 0 || source_work_total_chunks_equiv == 0) {
		return;
	}
	if (chunks_equiv > 0) {
		SaturatingAtomicAdd(source_work_throughput_completed_chunks_equiv, chunks_equiv, source_work_total_chunks_equiv);
	}
	source_work_throughput_worker_time_ns.fetch_add(worker_time_ns);
}

void Pipeline::RecordSourceWorkProgress(idx_t rows, idx_t chunks_equiv, idx_t native_units, Event *event) {
	if (!SourceWorkTrackingEnabled()) {
		return;
	}
	if (chunks_equiv == 0 && rows > 0) {
		chunks_equiv = SourceThroughputCounters::EstimateStandardChunks(rows);
	}
	if (rows > 0) {
		SaturatingAtomicAdd(source_work_completed_rows, rows, source_work_total_rows);
	}
	idx_t completed_chunks_equiv = 0;
	if (chunks_equiv > 0) {
		SaturatingAtomicAdd(source_work_completed_chunks_equiv, chunks_equiv, source_work_total_chunks_equiv,
		                    &completed_chunks_equiv);
	}
	if (native_units > 0) {
		SaturatingAtomicAdd(source_work_completed_native_units, native_units, source_work_total_native_units);
	}
	if (completed_chunks_equiv > 0 && ShouldRecordSourceWorkDebugSample(completed_chunks_equiv)) {
		QueryPipelineDebug::RecordWorkProgress(*this, event);
	}
}

bool Pipeline::ShouldRecordSourceWorkDebugSample(idx_t completed_chunks_equiv) {
	if (!source_work_debug_sampling_enabled.load()) {
		return false;
	}
	auto sample_step = source_work_debug_sample_step_chunks_equiv.load();
	auto total_chunks_equiv = source_work_debug_total_chunks_equiv.load();
	if (sample_step == 0 || total_chunks_equiv == 0) {
		return false;
	}
	auto next_sample = source_work_debug_next_sample_chunks_equiv.load();
	while (next_sample > 0 && completed_chunks_equiv >= next_sample) {
		auto new_next_sample = next_sample + sample_step;
		if (new_next_sample >= total_chunks_equiv) {
			new_next_sample = 0;
		}
		if (source_work_debug_next_sample_chunks_equiv.compare_exchange_weak(next_sample, new_next_sample)) {
			return true;
		}
	}
	return false;
}

bool Pipeline::GetWorkSnapshot(PipelineWorkSnapshot &snapshot) const {
	lock_guard<mutex> guard(source_work_lock);
	if (!source_work_valid) {
		return false;
	}
	snapshot = PipelineWorkSnapshot();
	snapshot.valid = true;
	snapshot.pipeline_id = profiler_pipeline_id;
	snapshot.source_input_kind = source_work_kind;
	snapshot.source_input_confidence = source_work_confidence;
	snapshot.total_rows = source_work_total_rows;
	snapshot.total_chunks_equiv = source_work_total_chunks_equiv;
	snapshot.total_native_units = source_work_total_native_units;
	snapshot.native_unit = source_work_native_unit;
	snapshot.completed_rows = source_work_completed_rows.load();
	snapshot.completed_chunks_equiv = source_work_completed_chunks_equiv.load();
	snapshot.completed_native_units = source_work_completed_native_units.load();
	snapshot.throughput_completed_chunks_equiv = source_work_throughput_completed_chunks_equiv.load();
	snapshot.throughput_worker_time_ns = source_work_throughput_worker_time_ns.load();
	if (snapshot.throughput_completed_chunks_equiv > 0 && snapshot.throughput_worker_time_ns > 0) {
		snapshot.single_worker_chunks_per_s =
		    (static_cast<double>(snapshot.throughput_completed_chunks_equiv) * 1000000000.0) /
		    static_cast<double>(snapshot.throughput_worker_time_ns);
		snapshot.throughput_valid = true;
	}
	snapshot.remaining_chunks_equiv =
	    snapshot.completed_chunks_equiv >= snapshot.total_chunks_equiv
	        ? 0
	        : snapshot.total_chunks_equiv - snapshot.completed_chunks_equiv;
	snapshot.source_max_threads = source_work_source_max_threads;
	snapshot.effective_max_threads = source_work_effective_max_threads;
	snapshot.scalable = source_work_scalable;
	return true;
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelineTask>(*this, event));
	auto source_max_threads = source_state ? source_state->MaxThreads() : 0;
	auto source_input_volume = source_state ? source_state->GetSourceInputVolume() : SourceInputVolume();
	InitializeSourceWorkTracking(source_input_volume, source_max_threads, 1);
	RecordProfilerStart(tasks.size(), source_max_threads, source_input_volume);
	event->SetTasks(std::move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event, string &parallel_blocker, idx_t &effective_max_threads,
                                idx_t &source_max_threads, idx_t &scheduler_threads) {
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	scheduler_threads = NumericCast<idx_t>(scheduler.NumberOfThreads());
	// check if the sink, source and all intermediate operators support parallelism
	if (!sink->ParallelSink()) {
		parallel_blocker = "sink_not_parallel";
		return false;
	}
	if (!source->ParallelSource()) {
		parallel_blocker = "source_not_parallel";
		return false;
	}
	auto max_threads = source_state->MaxThreads();
	source_max_threads = max_threads;

	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (!op.ParallelOperator()) {
			parallel_blocker = "operator_not_parallel:" + PhysicalOperatorToString(op.type);
			return false;
		}
		max_threads = MinValue<idx_t>(max_threads, op.op_state->MaxThreads(max_threads));
	}

	auto partition_info = sink->RequiredPartitionInfo();
	if (partition_info.batch_index) {
		if (!source->SupportsPartitioning(OperatorPartitionInfo::BatchIndex())) {
			throw InternalException(
			    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it");
		}
	}

	auto active_threads = scheduler_threads;
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	if (sink && sink->sink_state) {
		max_threads = sink->sink_state->MaxThreads(max_threads);
	}
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	effective_max_threads = max_threads;
	if (max_threads <= 1) {
		parallel_blocker = "max_threads_le_1";
	}
	return LaunchScanTasks(event, max_threads);
}

bool Pipeline::IsOrderDependent() const {
	if (source) {
		auto source_order = source->SourceOrder();
		if (source_order == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
		if (source_order == OrderPreservationType::NO_ORDER) {
			return false;
		}
	}
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		if (op.OperatorOrder() == OrderPreservationType::NO_ORDER) {
			return false;
		}
		if (op.OperatorOrder() == OrderPreservationType::FIXED_ORDER) {
			return true;
		}
	}
	if (!Settings::Get<PreserveInsertionOrderSetting>(executor.context)) {
		return false;
	}
	if (sink && sink->SinkOrderDependent()) {
		return true;
	}
	return false;
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(sink);
	Reset();
	string parallel_blocker;
	idx_t effective_max_threads = 0;
	idx_t source_max_threads = source_state ? source_state->MaxThreads() : 0;
	idx_t scheduler_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(executor.context).NumberOfThreads());
	auto scheduled_parallel =
	    ScheduleParallel(event, parallel_blocker, effective_max_threads, source_max_threads, scheduler_threads);
	if (!scheduled_parallel) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
		effective_max_threads = 1;
	}
	QueryPipelineDebug::RecordPipelineSchedule(*this, *event, scheduled_parallel, parallel_blocker,
	                                           effective_max_threads, source_max_threads, scheduler_threads);
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < max_threads; i++) {
		tasks.push_back(make_uniq<PipelineTask>(*this, event));
	}
	auto source_max_threads = source_state ? source_state->MaxThreads() : 0;
	auto source_input_volume = source_state ? source_state->GetSourceInputVolume() : SourceInputVolume();
	InitializeSourceWorkTracking(source_input_volume, source_max_threads, max_threads);
	RecordProfilerStart(tasks.size(), source_max_threads, source_input_volume);
	event->SetTasks(std::move(tasks));
	return true;
}

void Pipeline::ResetSink() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			sink->sink_state = sink->GetGlobalSinkState(GetClientContext());
		}
	}
}

void Pipeline::PrepareFinalize() {
	if (sink) {
		if (!sink->IsSink()) {
			throw InternalException("Sink of pipeline does not have IsSink set");
		}
		lock_guard<mutex> guard(sink->lock);
		if (!sink->sink_state) {
			throw InternalException("Sink of pipeline does not have sink state");
		}
		sink->PrepareFinalize(GetClientContext(), *sink->sink_state);
	}
}

void Pipeline::Reset() {
	ResetSourceWorkTracking();
	ResetSink();
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		lock_guard<mutex> guard(op.lock);
		if (!op.op_state) {
			op.op_state = op.GetGlobalOperatorState(GetClientContext());
		}
	}
	ResetSource(false);
	// we no longer reset source here because this function is no longer guaranteed to be called by the main thread
	// source reset needs to be called by the main thread because resetting a source may call into clients like R
	initialized = true;
}

void Pipeline::ResetSource(bool force) {
	if (source && !source->IsSource()) {
		throw InternalException("Source of pipeline does not have IsSource set");
	}
	if (force || !source_state) {
		source_state = source->GetGlobalSourceState(GetClientContext());
	}
}

void Pipeline::Ready() {
	if (ready) {
		return;
	}
	ready = true;
	std::reverse(operators.begin(), operators.end());
	RegisterProfilerPipeline();
}

void Pipeline::RegisterProfilerPipeline() {
	if (!source || !sink) {
		return;
	}
	if (!profiler_pipeline_id) {
		profiler_pipeline_id = QueryProfiler::Get(GetClientContext()).RegisterPipelineProfile(*source, operators, sink);
	}
	if (!energy_attribution_query_handle) {
		EnergyAttributionManager::AttachPipeline(GetClientContext(), *this, profiler_pipeline_id);
	}
}

void Pipeline::RecordProfilerStart(idx_t task_count, idx_t source_max_threads,
                                   const SourceInputVolume &source_input_volume) {
	if (profiler_pipeline_id) {
		QueryProfiler::Get(GetClientContext())
		    .RecordPipelineProfileStart(profiler_pipeline_id, task_count, source_max_threads, source_input_volume);
	}
}

void Pipeline::RecordProfilerTasksDone() {
	if (profiler_pipeline_id) {
		QueryProfiler::Get(GetClientContext()).RecordPipelineProfileTasksDone(profiler_pipeline_id);
	}
}

void Pipeline::RecordProfilerFinishDone() {
	if (profiler_pipeline_id) {
		QueryProfiler::Get(GetClientContext()).RecordPipelineProfileFinishDone(profiler_pipeline_id);
	}
}

idx_t Pipeline::GetProfilerPipelineId() const {
	return profiler_pipeline_id;
}

void Pipeline::SetEnergyAttributionPipeline(shared_ptr<EnergyAttributionQueryHandle> query_handle, idx_t pipeline_id) {
	energy_attribution_query_handle = std::move(query_handle);
	energy_attribution_pipeline_id = pipeline_id;
}

EnergyAttributionQueryHandle *Pipeline::GetEnergyAttributionQueryState() const {
	return energy_attribution_query_handle.get();
}

shared_ptr<EnergyAttributionQueryHandle> Pipeline::GetEnergyAttributionQueryHandle() const {
	return energy_attribution_query_handle;
}

idx_t Pipeline::GetEnergyAttributionPipelineId() const {
	return energy_attribution_pipeline_id;
}

idx_t Pipeline::RecordProfilerTaskStart(uint64_t thread_id, int start_cpu) {
	if (!profiler_pipeline_id) {
		return 0;
	}
	return QueryProfiler::Get(GetClientContext()).RecordPipelineTaskStart(profiler_pipeline_id, thread_id, start_cpu);
}

void Pipeline::RecordProfilerTaskEnd(idx_t task_id, int end_cpu, const SourceThroughputCounters &source_throughput,
                                     idx_t pipeline_input_tuples, idx_t pipeline_input_chunks) {
	if (!task_id) {
		return;
	}
	QueryProfiler::Get(GetClientContext())
	    .RecordPipelineTaskEnd(task_id, end_cpu, source_throughput, pipeline_input_tuples, pipeline_input_chunks);
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

vector<weak_ptr<Pipeline>> Pipeline::GetDependencies() const {
	return dependencies;
}

string Pipeline::ToString() const {
	TextTreeRenderer renderer;
	return renderer.ToString(*this);
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

void Pipeline::PrintDependencies() const {
	for (auto &dep : dependencies) {
		shared_ptr<Pipeline>(dep)->Print();
	}
}

vector<reference<PhysicalOperator>> Pipeline::GetOperators() {
	vector<reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

vector<const_reference<PhysicalOperator>> Pipeline::GetOperators() const {
	vector<const_reference<PhysicalOperator>> result;
	D_ASSERT(source);
	result.push_back(*source);
	for (auto &op : operators) {
		result.push_back(op.get());
	}
	if (sink) {
		result.push_back(*sink);
	}
	return result;
}

const vector<reference<PhysicalOperator>> &Pipeline::GetIntermediateOperators() const {
	return operators;
}

void Pipeline::ClearSource() {
	source_state.reset();
	batch_indexes.clear();
}

idx_t Pipeline::RegisterNewBatchIndex() {
	lock_guard<mutex> l(batch_lock);
	idx_t minimum = batch_indexes.empty() ? base_batch_index : *batch_indexes.begin();
	batch_indexes.insert(minimum);
	return minimum;
}

idx_t Pipeline::UpdateBatchIndex(idx_t old_index, idx_t new_index) {
	lock_guard<mutex> l(batch_lock);
	if (new_index < *batch_indexes.begin()) {
		throw InternalException("Processing batch index %llu, but previous min batch index was %llu", new_index,
		                        *batch_indexes.begin());
	}
	auto entry = batch_indexes.find(old_index);
	if (entry == batch_indexes.end()) {
		throw InternalException("Batch index %llu was not found in set of active batch indexes", old_index);
	}
	batch_indexes.erase(entry);
	batch_indexes.insert(new_index);
	return *batch_indexes.begin();
}
//===--------------------------------------------------------------------===//
// Pipeline Build State
//===--------------------------------------------------------------------===//
void PipelineBuildState::SetPipelineSource(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.source = &op;
}

void PipelineBuildState::SetPipelineSink(Pipeline &pipeline, optional_ptr<PhysicalOperator> op,
                                         idx_t sink_pipeline_count) {
	pipeline.sink = op;
	// set the base batch index of this pipeline based on how many other pipelines have this node as their sink
	pipeline.base_batch_index = BATCH_INCREMENT * sink_pipeline_count;
}

void PipelineBuildState::AddPipelineOperator(Pipeline &pipeline, PhysicalOperator &op) {
	pipeline.operators.push_back(op);
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSource(Pipeline &pipeline) {
	return pipeline.source;
}

optional_ptr<PhysicalOperator> PipelineBuildState::GetPipelineSink(Pipeline &pipeline) {
	return pipeline.sink;
}

void PipelineBuildState::SetPipelineOperators(Pipeline &pipeline, vector<reference<PhysicalOperator>> operators) {
	pipeline.operators = std::move(operators);
}

shared_ptr<Pipeline> PipelineBuildState::CreateChildPipeline(Executor &executor, Pipeline &pipeline,
                                                             PhysicalOperator &op) {
	return executor.CreateChildPipeline(pipeline, op);
}

vector<reference<PhysicalOperator>> PipelineBuildState::GetPipelineOperators(Pipeline &pipeline) {
	return pipeline.operators;
}

} // namespace duckdb
