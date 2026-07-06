#include "duckdb/parallel/pipeline_prepare_finish_event.hpp"

#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"

namespace duckdb {

PipelinePrepareFinishEvent::PipelinePrepareFinishEvent(shared_ptr<Pipeline> pipeline_p)
    : BasePipelineEvent(std::move(pipeline_p)) {
}

class PipelinePreFinishTask : public ExecutorTask {
public:
	explicit PipelinePreFinishTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p) {
	}

	Pipeline &pipeline;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		EnergySegmentScope energy_scope(pipeline, *event, EnergySegmentPhase::PREPARE_FINISH);
		pipeline.PrepareFinalize();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "PipelinePreFinishTask";
	}
};

void PipelinePrepareFinishEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelinePreFinishTask>(*pipeline, shared_from_this()));
	SetTasks(std::move(tasks));
	QueryPipelineDebug::RecordLifecycleSchedule(*pipeline, *this, "scheduled_lifecycle");
}

void PipelinePrepareFinishEvent::FinishEvent() {
}

} // namespace duckdb
