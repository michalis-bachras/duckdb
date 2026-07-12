//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_pipeline_debug.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_pipeline_debug.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>

namespace duckdb {

namespace {

struct QueryPipelineDebugStore {
	mutex lock;
	vector<QueryPipelineDebugEventSnapshot> events;
};

static QueryPipelineDebugStore &GetPipelineDebugStore() {
	static QueryPipelineDebugStore store;
	return store;
}

static uint64_t TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static const char *EventKindName(QueryActivationEventKind kind) {
	switch (kind) {
	case QueryActivationEventKind::UNREGISTERED:
		return "unregistered";
	case QueryActivationEventKind::INITIALIZE:
		return "initialize";
	case QueryActivationEventKind::PIPELINE:
		return "pipeline";
	case QueryActivationEventKind::PREPARE_FINISH:
		return "prepare_finish";
	case QueryActivationEventKind::FINISH:
		return "finish";
	case QueryActivationEventKind::COMPLETE:
		return "complete";
	case QueryActivationEventKind::INTERNAL:
		return "internal";
	default:
		return "unknown";
	}
}

static void AppendOperatorType(string &result, const string &operator_type) {
	if (!result.empty()) {
		result += ">";
	}
	result += operator_type;
}

static void PopulatePipelineInfo(QueryPipelineDebugEventSnapshot &snapshot, Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	if (source) {
		snapshot.source_type = PhysicalOperatorToString(source->type);
		AppendOperatorType(snapshot.operator_type_sequence, snapshot.source_type);
	}
	for (auto &op_ref : pipeline.GetIntermediateOperators()) {
		auto &op = op_ref.get();
		AppendOperatorType(snapshot.operator_type_sequence, PhysicalOperatorToString(op.type));
	}
	auto sink = pipeline.GetSink();
	if (sink) {
		snapshot.sink_type = PhysicalOperatorToString(sink->type);
		AppendOperatorType(snapshot.operator_type_sequence, snapshot.sink_type);
	}
}

static bool PopulateMetadata(ClientContext &context, QueryPipelineDebugEventSnapshot &snapshot) {
	if (!QueryPipelineDebug::Enabled(context)) {
		return false;
	}
	QueryRequestMetadata metadata;
	if (!QueryRequestMetadataManager::TryGetActive(context, metadata)) {
		return false;
	}
	snapshot.db_query_id = metadata.db_query_id;
	snapshot.request_id = metadata.request_id;
	snapshot.template_id = metadata.template_id;
	snapshot.scale_factor = metadata.scale_factor;
	return true;
}

static void PopulateEventInfo(QueryPipelineDebugEventSnapshot &snapshot, Event &event) {
	if (event.HasQueryActivationInfo()) {
		snapshot.activation_group_id = event.GetQueryActivationGroupId();
		snapshot.pipeline_id = event.GetQueryActivationPipelineId();
		snapshot.event_kind = EventKindName(event.GetQueryActivationKind());
	} else {
		snapshot.event_kind = "unregistered";
	}
	snapshot.timestamp_ns = TimestampNs();
	snapshot.total_tasks = event.GetTotalTasks();
	snapshot.finished_tasks = event.GetFinishedTasks();
	if (snapshot.finished_tasks > snapshot.total_tasks) {
		snapshot.finished_tasks = snapshot.total_tasks;
	}
	snapshot.remaining_tasks = snapshot.total_tasks - snapshot.finished_tasks;
	snapshot.preferred_parallelism = snapshot.remaining_tasks;
	snapshot.parallelism_valid = snapshot.total_tasks > 0;
}

static void PopulateWorkInfo(QueryPipelineDebugEventSnapshot &snapshot, const PipelineWorkSnapshot &work) {
	snapshot.work_valid = work.valid;
	snapshot.scalable = work.scalable;
	snapshot.source_input_kind = work.source_input_kind;
	snapshot.source_input_confidence = work.source_input_confidence;
	snapshot.total_rows = work.total_rows;
	snapshot.total_chunks_equiv = work.total_chunks_equiv;
	snapshot.total_native_units = work.total_native_units;
	snapshot.native_unit = work.native_unit;
	snapshot.completed_rows = work.completed_rows;
	snapshot.completed_chunks_equiv = work.completed_chunks_equiv;
	snapshot.completed_native_units = work.completed_native_units;
	snapshot.remaining_chunks_equiv = work.remaining_chunks_equiv;
	if (work.parallelism_valid && !snapshot.parallelism_valid) {
		snapshot.remaining_tasks = work.remaining_tasks;
		snapshot.preferred_parallelism = work.preferred_parallelism;
		snapshot.parallelism_valid = true;
	}
	if (!snapshot.pipeline_id) {
		snapshot.pipeline_id = work.pipeline_id;
	}
	if (!snapshot.source_max_threads) {
		snapshot.source_max_threads = work.source_max_threads;
	}
	if (!snapshot.effective_max_threads) {
		snapshot.effective_max_threads = work.effective_max_threads;
	}
}

static void PopulatePipelineWorkInfo(QueryPipelineDebugEventSnapshot &snapshot, Pipeline &pipeline) {
	PipelineWorkSnapshot work;
	if (pipeline.GetWorkSnapshot(work)) {
		PopulateWorkInfo(snapshot, work);
	}
}

static void PopulateEventWorkInfo(QueryPipelineDebugEventSnapshot &snapshot, Event &event) {
	PipelineWorkSnapshot work;
	if (event.GetPipelineWorkSnapshot(work)) {
		PopulateWorkInfo(snapshot, work);
	}
}

static void AppendDebugEvent(QueryPipelineDebugEventSnapshot snapshot) {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.push_back(std::move(snapshot));
}

} // namespace

bool QueryPipelineDebug::Enabled(ClientContext &context) {
	return ClientConfig::GetConfig(context).query_activation_debug_enabled;
}

void QueryPipelineDebug::RecordPipelineSchedule(Pipeline &pipeline, Event &event, bool parallel,
                                                const string &parallel_blocker, idx_t effective_max_threads,
                                                idx_t source_max_threads, idx_t scheduler_threads) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(pipeline.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = parallel ? "scheduled_parallel" : "scheduled_sequential";
	snapshot.parallel = parallel;
	snapshot.parallel_blocker = parallel_blocker;
	snapshot.effective_max_threads = effective_max_threads;
	snapshot.source_max_threads = source_max_threads;
	snapshot.scheduler_threads = scheduler_threads;
	if (!snapshot.pipeline_id) {
		snapshot.pipeline_id = pipeline.GetProfilerPipelineId();
	}
	PopulatePipelineWorkInfo(snapshot, pipeline);
	PopulatePipelineInfo(snapshot, pipeline);
	AppendDebugEvent(std::move(snapshot));
}

void QueryPipelineDebug::RecordLifecycleSchedule(Pipeline &pipeline, Event &event, const string &state) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(pipeline.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = state;
	snapshot.parallel = false;
	snapshot.parallel_blocker = "lifecycle_single_task";
	snapshot.effective_max_threads = event.GetTotalTasks();
	snapshot.source_max_threads = 0;
	snapshot.scheduler_threads =
	    NumericCast<idx_t>(TaskScheduler::GetScheduler(pipeline.GetClientContext()).NumberOfThreads());
	if (!snapshot.pipeline_id) {
		snapshot.pipeline_id = pipeline.GetProfilerPipelineId();
	}
	PopulateEventWorkInfo(snapshot, event);
	PopulatePipelineInfo(snapshot, pipeline);
	AppendDebugEvent(std::move(snapshot));
}

void QueryPipelineDebug::RecordWorkProgress(Pipeline &pipeline, Event *event) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(pipeline.GetClientContext(), snapshot)) {
		return;
	}
	PipelineWorkSnapshot work;
	if (!pipeline.GetWorkSnapshot(work) || !work.valid || work.total_chunks_equiv == 0) {
		return;
	}
	if (event) {
		PopulateEventInfo(snapshot, *event);
	} else {
		snapshot.pipeline_id = pipeline.GetProfilerPipelineId();
		snapshot.event_kind = "pipeline";
		snapshot.timestamp_ns = TimestampNs();
	}
	snapshot.event_state = "work_progress";
	snapshot.scheduler_threads =
	    NumericCast<idx_t>(TaskScheduler::GetScheduler(pipeline.GetClientContext()).NumberOfThreads());
	PopulateWorkInfo(snapshot, work);
	PopulatePipelineInfo(snapshot, pipeline);
	AppendDebugEvent(std::move(snapshot));
}

void QueryPipelineDebug::RecordEventFinished(Event &event) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(event.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = "finished";
	snapshot.parallel = false;
	PopulateEventWorkInfo(snapshot, event);
	AppendDebugEvent(std::move(snapshot));
}

vector<QueryPipelineDebugEventSnapshot> QueryPipelineDebug::GetDebugSnapshot() {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	return store.events;
}

void QueryPipelineDebug::ClearDebugSnapshot() {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.clear();
}

} // namespace duckdb
