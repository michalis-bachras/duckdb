#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"

#include <chrono>

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
			return result;
		} else {
			TaskNotifier task_notifier {context};
			auto result = ExecuteTask(mode);
			record_internal_time();
			return result;
		}
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception in ExecutorTask::Execute"));
	} // LCOV_EXCL_STOP
	record_internal_time();
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
