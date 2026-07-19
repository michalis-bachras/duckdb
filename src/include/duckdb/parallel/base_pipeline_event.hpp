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
	optional_ptr<Pipeline> GetOwningPipeline() override {
		return pipeline.get();
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
			PipelineWorkSnapshot pipeline_snapshot;
			pipeline->GetWorkSnapshot(pipeline_snapshot);
			snapshot = PipelineWorkSnapshot();
			snapshot.pipeline_id = pipeline->GetProfilerPipelineId();
			snapshot.profile_identity = pipeline_snapshot.profile_identity;
			snapshot.lifecycle_tail_estimate = pipeline_snapshot.lifecycle_tail_estimate;
			snapshot.internal_event_position = GetInternalEventPosition();
			snapshot.internal_event_type = InternalEventTypeToString(GetInternalEventType());
			snapshot.source_input_kind = GetQueryActivationKind() == QueryActivationEventKind::INTERNAL
			                                 ? snapshot.internal_event_type
			                                 : "lifecycle";
			snapshot.source_input_confidence = "exact";
			snapshot.native_unit = GetInternalNativeUnit();
			snapshot.total_native_units = GetInternalTotalWork();
			snapshot.total_chunks_equiv = GetInternalTotalWork();
			snapshot.completed_native_units = MinValue(GetInternalCompletedWork(), GetInternalTotalWork());
			snapshot.completed_chunks_equiv = snapshot.completed_native_units;
			snapshot.remaining_chunks_equiv = snapshot.total_chunks_equiv - snapshot.completed_chunks_equiv;
			snapshot.throughput_completed_chunks_equiv = snapshot.completed_chunks_equiv;
			snapshot.throughput_worker_time_ns = GetInternalWorkerTimeNs();
			snapshot.historical_throughput_estimate = internal_historical_throughput;
			snapshot.continuation_estimate = internal_continuation_estimate;
			snapshot.lifecycle_tail_estimate = internal_tail_estimate.valid ? internal_tail_estimate
			                                                               : pipeline_snapshot.lifecycle_tail_estimate;
			if (snapshot.throughput_completed_chunks_equiv > 0 && snapshot.throughput_worker_time_ns > 0) {
				snapshot.single_worker_chunks_per_s =
				    static_cast<double>(snapshot.throughput_completed_chunks_equiv) * 1000000000.0 /
				    static_cast<double>(snapshot.throughput_worker_time_ns);
				snapshot.throughput_valid = true;
				snapshot.selected_throughput_valid = true;
				snapshot.selected_throughput_is_live = true;
				snapshot.selected_single_worker_chunks_per_s = snapshot.single_worker_chunks_per_s;
			} else if (internal_historical_throughput.valid) {
				snapshot.selected_throughput_valid = true;
				snapshot.selected_single_worker_chunks_per_s = internal_historical_throughput.ewma_work_units_per_s > 0
				                                                     ? internal_historical_throughput.ewma_work_units_per_s
				                                                     : internal_historical_throughput.mean_work_units_per_s;
			}
			snapshot.valid = true;
			populate_parallelism(snapshot);
			snapshot.scalable = GetQueryActivationKind() == QueryActivationEventKind::INTERNAL &&
			                    InternalWorkScalable() && snapshot.preferred_parallelism > 1;
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
