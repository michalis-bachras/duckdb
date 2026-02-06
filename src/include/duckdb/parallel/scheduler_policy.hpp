//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/scheduler_policy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/scheduler_type.hpp"

namespace duckdb {

class Executor;
class Pipeline;

//! SchedulerPolicy is the abstract interface for different scheduling strategies.
//! Designed to support both the stride scheduler (SIGMOD 2021 paper) and future ML schedulers.
//!
//! Only GetThreadsForPipeline() and GetType() are required.
//! All other methods have default empty implementations for schedulers that don't need them.
class SchedulerPolicy {
public:
	virtual ~SchedulerPolicy() = default;

	//===--------------------------------------------------------------------===//
	// Required Methods (Pure Virtual)
	//===--------------------------------------------------------------------===//

	//! Get the number of threads to allocate for a pipeline.
	//! This is the core scheduling decision.
	//! @param pipeline The pipeline requesting threads
	//! @param available_threads Total available threads in the system
	//! @return Number of threads to use for this pipeline
	virtual idx_t GetThreadsForPipeline(Pipeline &pipeline, idx_t available_threads) = 0;

	//! Get the type of this scheduler.
	virtual SchedulerType GetType() const = 0;

	//===--------------------------------------------------------------------===//
	// Optional Hooks (Default Empty Implementations)
	//===--------------------------------------------------------------------===//

	//! Called when a query starts execution.
	virtual void OnQueryStart(Executor &executor) {
	}

	//! Called when a query completes execution.
	//! @param latency_ms Total query execution time in milliseconds
	virtual void OnQueryComplete(Executor &executor, double latency_ms) {
	}

	//! Called when a pipeline starts execution.
	virtual void OnPipelineStart(Pipeline &pipeline) {
	}

	//! Called when a pipeline completes execution.
	virtual void OnPipelineComplete(Pipeline &pipeline) {
	}

	//! Called after a task completes. Used for priority updates.
	//! @param pipeline The pipeline owning this task
	//! @param cpu_time_ms CPU time consumed by this task
	virtual void OnTaskComplete(Pipeline &pipeline, double cpu_time_ms) {
	}

	//===--------------------------------------------------------------------===//
	// Stride Scheduler Specific (Default Implementations)
	//===--------------------------------------------------------------------===//

	//! Get the current priority for a query. Used by stride scheduler.
	virtual double GetQueryPriority(Executor &executor) const {
		return 1.0;
	}

	//! Get morsel size for adaptive scheduling. Used by stride scheduler.
	//! @return 0 means use default morsel size
	virtual idx_t GetMorselSize(Pipeline &pipeline, idx_t remaining_tuples, idx_t worker_count) {
		return 0; // Use default
	}

	//! Update throughput estimate. Used for adaptive morsel sizing.
	virtual void UpdateThroughput(Pipeline &pipeline, idx_t tuples_processed, double execution_time_ms) {
	}
};

} // namespace duckdb
