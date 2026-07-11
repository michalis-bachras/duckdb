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
		if (GetQueryActivationKind() != QueryActivationEventKind::PIPELINE) {
			snapshot = PipelineWorkSnapshot();
			snapshot.pipeline_id = pipeline->GetProfilerPipelineId();
			snapshot.source_input_kind = "lifecycle";
			snapshot.source_input_confidence = "exact";
			snapshot.valid = true;
			snapshot.scalable = false;
			return true;
		}
		return pipeline->GetWorkSnapshot(snapshot);
	}

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
};

} // namespace duckdb
