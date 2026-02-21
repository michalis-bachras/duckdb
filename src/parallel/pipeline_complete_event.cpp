#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

PipelineCompleteEvent::PipelineCompleteEvent(Executor &executor, bool complete_pipeline_p)
    : Event(executor), complete_pipeline(complete_pipeline_p) {
}

void PipelineCompleteEvent::Schedule() {
}

void PipelineCompleteEvent::FinalizeFinish() {
	if (complete_pipeline) {
		executor.CompletePipeline();

		if (executor.IsRegisteredWithScheduler()) {
			if (executor.ExecutionIsFinished()) {
				// Query is done — deregister slot (lazy, no worker notification)
				auto &scheduler = TaskScheduler::GetScheduler(executor.context);
				scheduler.GetSlotArray().DeregisterQuery(executor.GetSchedulerSlotIndex());
			} else {
				// More pipelines remain — push return mask (local pass → global_pass)
				auto &scheduler = TaskScheduler::GetScheduler(executor.context);
				scheduler.GetSlotArray().PushReturnToWorkers(executor.GetSchedulerSlotIndex());
			}
		}
	}
}

} // namespace duckdb
