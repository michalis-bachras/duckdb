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

		// If the query still has more meta-pipelines, push return mask
		// so workers set local pass = global_pass for fair scheduling of the next pipeline
		if (executor.IsRegisteredWithScheduler() && !executor.ExecutionIsFinished()) {
			auto &scheduler = TaskScheduler::GetScheduler(executor.context);
			scheduler.GetSlotArray().PushReturnToWorkers(executor.GetSchedulerSlotIndex());
		}
	}
}

} // namespace duckdb
