//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/default_scheduler_policy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/scheduler_policy.hpp"

namespace duckdb {

//! DefaultSchedulerPolicy implements the current DuckDB scheduling behavior.
//! This is a no-op policy that allows all available threads for each pipeline,
//! maintaining backward compatibility with existing behavior.
class DefaultSchedulerPolicy : public SchedulerPolicy {
public:
	DefaultSchedulerPolicy() = default;
	~DefaultSchedulerPolicy() override = default;


	//! Returns all available threads (default DuckDB behavior)
	idx_t GetThreadsForPipeline(Pipeline &pipeline, idx_t available_threads) override {
		return available_threads;
	}

	//! Returns DEFAULT scheduler type
	SchedulerType GetType() const override {
		return SchedulerType::DEFAULT;
	}
};

} // namespace duckdb
