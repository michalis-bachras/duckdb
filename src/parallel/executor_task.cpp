#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <chrono>
#include <cstdlib>
#ifdef __linux__
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace duckdb {

ExecutorTask::ExecutorTask(Executor &executor_p, shared_ptr<Event> event_p)
    : executor(executor_p), event(std::move(event_p)), context(executor_p.context) {
	executor.RegisterTask();
}

ExecutorTask::ExecutorTask(ClientContext &context_p, shared_ptr<Event> event_p, const PhysicalOperator &op_p)
    : executor(Executor::Get(context_p)), event(std::move(event_p)), op(&op_p), context(context_p) {
	thread_context = make_uniq<ThreadContext>(context_p);
	executor.RegisterTask();
}

ExecutorTask::~ExecutorTask() {
	if (thread_context) {
		executor.Flush(*thread_context);
	}
	executor.UnregisterTask();
}

void ExecutorTask::Deschedule() {
	if (event && !task_blocked.exchange(true)) {
		event->MarkTaskBlocked();
	}
	auto this_ptr = shared_from_this();
	executor.AddToBeRescheduled(this_ptr);
}

void ExecutorTask::Reschedule() {
	if (event && task_blocked.exchange(false)) {
		event->MarkTaskUnblocked();
	}
	auto this_ptr = shared_from_this();
	executor.RescheduleTask(this_ptr);
}

TaskExecutionResult ExecutorTask::Execute(TaskExecutionMode mode) {
	// Measure dispatches, not just PipelineTask bodies: sink finalization can
	// replace a finish event with several additional ExecutorTask types.
	PipelineTaskProfilingInfo pmu_segment;
	QueryProfiler *pmu_profiler = nullptr;
	auto pmu_setting = std::getenv("OLAP_PIPELINE_PMU_GROUP");
	auto owner = event ? event->GetOwningPipeline() : nullptr;
	if (pmu_setting && *pmu_setting && owner && owner->GetProfilerPipelineId()) {
		auto &profiler = QueryProfiler::Get(context);
		if (profiler.BeginPipelinePMUSegment(owner->GetProfilerPipelineId())) {
			pmu_profiler = &profiler;
			pmu_segment.pipeline_id = owner->GetProfilerPipelineId();
			pmu_segment.task_type = TaskType();
			pmu_segment.event_position = event->GetInternalEventPosition();
			pmu_segment.activation_group_id = event->GetQueryActivationGroupId();
			auto kind = event->GetQueryActivationKind();
			switch (kind) {
			case QueryActivationEventKind::PIPELINE: pmu_segment.phase = "pipeline"; break;
			case QueryActivationEventKind::INITIALIZE: pmu_segment.phase = "initialize"; break;
			case QueryActivationEventKind::PREPARE_FINISH: pmu_segment.phase = "prepare_finish"; break;
			case QueryActivationEventKind::FINISH: pmu_segment.phase = "finish"; break;
			case QueryActivationEventKind::INTERNAL:
				pmu_segment.phase = InternalEventTypeToString(event->GetInternalEventType()); break;
			default: pmu_segment.phase = "other"; break;
			}
			pmu_segment.ownership = kind == QueryActivationEventKind::PIPELINE ? "pipeline_task" : "event_owner_may_be_shared";
			for (auto id : event->GetEnergyLifecycleMemberPipelineIds()) {
				if (!pmu_segment.shared_pipeline_ids.empty()) pmu_segment.shared_pipeline_ids += "|";
				pmu_segment.shared_pipeline_ids += std::to_string(id);
			}
#ifdef __linux__
			pmu_segment.thread_id = static_cast<uint64_t>(syscall(SYS_gettid));
			pmu_segment.start_cpu = sched_getcpu();
#endif
			pmu_segment.pmu_start = ReadPipelineTaskPMU();
			pmu_segment.start_ns = PipelineDVFSProfiler::TimestampNs();
		}
	}
	auto pmu_outcome = TaskExecutionResult::TASK_ERROR;
	auto finish_pmu = [&]() {
		if (!pmu_profiler) return;
		pmu_segment.end_ns = PipelineDVFSProfiler::TimestampNs();
		pmu_segment.pmu = PipelineTaskPMUDelta(pmu_segment.pmu_start, ReadPipelineTaskPMU());
		if (pmu_segment.pmu.group.empty()) {
			pmu_segment.pmu.group = "off";
			pmu_segment.pmu.error = "disabled";
		}
#ifdef __linux__
		pmu_segment.end_cpu = sched_getcpu();
#endif
		switch (pmu_outcome) {
		case TaskExecutionResult::TASK_FINISHED: pmu_segment.outcome = "finished"; break;
		case TaskExecutionResult::TASK_BLOCKED: pmu_segment.outcome = "blocked"; break;
		case TaskExecutionResult::TASK_NOT_FINISHED: pmu_segment.outcome = "yielded"; break;
		default: pmu_segment.outcome = "error"; break;
		}
		pmu_profiler->FinishPipelinePMUSegment(std::move(pmu_segment));
		pmu_profiler = nullptr;
	};
	auto internal_start = std::chrono::steady_clock::time_point();
	const bool internal_event = event && event->GetQueryActivationKind() == QueryActivationEventKind::INTERNAL;
	const bool track_internal = internal_event && event->InternalWorkTrackingEnabled();
	unique_ptr<EnergySegmentScope> internal_energy_scope;
	if (internal_event) {
		auto pipeline = event->GetOwningPipeline();
		if (pipeline) {
			internal_energy_scope = make_uniq<EnergySegmentScope>(*pipeline, *event, EnergySegmentPhase::INTERNAL);
		}
	}
	if (track_internal) {
		internal_start = std::chrono::steady_clock::now();
	}
	auto record_internal_time = [&]() {
		if (!track_internal) {
			return;
		}
		auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() -
		                                                                 internal_start)
		                   .count();
		if (elapsed > 0) {
			event->ReportInternalWorkerTime(NumericCast<uint64_t>(elapsed));
		}
	};
	try {
		if (thread_context) {
			TaskExecutionResult result;
			do {
				TaskNotifier task_notifier {context};
				thread_context->profiler.StartOperator(op);
				// to allow continuous profiling, always execute in small steps
				result = ExecuteTask(TaskExecutionMode::PROCESS_PARTIAL);
				thread_context->profiler.EndOperator(nullptr);
				executor.Flush(*thread_context);
			} while (mode == TaskExecutionMode::PROCESS_ALL && result == TaskExecutionResult::TASK_NOT_FINISHED);
			record_internal_time();
			pmu_outcome = result;
			finish_pmu();
			return result;
		} else {
			TaskNotifier task_notifier {context};
			auto result = ExecuteTask(mode);
			record_internal_time();
			pmu_outcome = result;
			finish_pmu();
			return result;
		}
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception in ExecutorTask::Execute"));
	} // LCOV_EXCL_STOP
	record_internal_time();
	finish_pmu();
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
