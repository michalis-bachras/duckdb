//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/base_pipeline_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! A BasePipelineEvent is used as the basis of any event that belongs to a specific pipeline
class BasePipelineEvent : public Event {
public:
	explicit BasePipelineEvent(shared_ptr<Pipeline> pipeline);
	explicit BasePipelineEvent(Pipeline &pipeline);

	void PrintPipeline() override {
		pipeline->Print();
	}
	bool GetPipelineWorkSnapshot(PipelineWorkSnapshot &snapshot) const override {
		if (!pipeline) {
			return false;
		}
		auto populate_parallelism = [this](PipelineWorkSnapshot &snapshot) {
			auto scheduled_tasks = GetTotalTasks();
			auto finished_tasks = GetFinishedTasks();
			auto blocked_tasks = GetBlockedTasks();
			if (finished_tasks > scheduled_tasks) {
				finished_tasks = scheduled_tasks;
			}
			blocked_tasks = MinValue<idx_t>(blocked_tasks, scheduled_tasks - finished_tasks);
			snapshot.scheduled_tasks = scheduled_tasks;
			snapshot.finished_tasks = finished_tasks;
			snapshot.remaining_tasks = scheduled_tasks - finished_tasks;
			snapshot.preferred_parallelism = snapshot.remaining_tasks - blocked_tasks;
			snapshot.parallelism_valid = scheduled_tasks > 0;
		};
		if (GetQueryActivationKind() != QueryActivationEventKind::PIPELINE) {
			if (!pipeline->GetWorkSnapshot(snapshot)) {
				snapshot = PipelineWorkSnapshot();
			}
			snapshot.pipeline_id = pipeline->GetProfilerPipelineId();
			snapshot.source_input_kind = "lifecycle";
			snapshot.source_input_confidence = "exact";
			snapshot.valid = true;
			snapshot.scalable = false;
			populate_parallelism(snapshot);
			return true;
		}
		auto has_work_snapshot = pipeline->GetWorkSnapshot(snapshot);
		if (!has_work_snapshot) {
			snapshot = PipelineWorkSnapshot();
			snapshot.pipeline_id = pipeline->GetProfilerPipelineId();
		}
		populate_parallelism(snapshot);
		return true;
	}

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
};

} // namespace duckdb
