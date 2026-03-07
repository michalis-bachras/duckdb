//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/scheduler_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/scheduler_slot_array.hpp"

namespace duckdb {

//! Result of the optimizer's search: best (d_start, lambda) pair.
struct OptimalParams {
	int d_start;
	double lambda;
};

//! SchedulerOptimizer implements the self-tuning optimizer from Section 4
//! of "Self-Tuning Query Scheduling for Analytical Workloads".
//!
//! Given a tracked workload, it evaluates candidate (d_start, λ) parameter
//! pairs by calling WorkloadSimulator::Simulate and returns the pair that
//! minimizes mean relative slowdown (paper Equation 3).
//!
//! The search procedure:
//! 1. Generate d_start candidates from workload quantiles (5%-35%)
//! 2. For each d_start, refine λ via directional search (7 steps)
//! 3. Return the globally best (d_start, λ)
class SchedulerOptimizer {
public:
	//! Number of directional search steps per d_start candidate (paper Section 4)
	static constexpr int SEARCH_STEPS = 7;
	//! Search direction magnitude (paper: D = {+0.05, -0.05})
	static constexpr double DIRECTION = 0.05;
	//! Maximum step width to prevent degenerate out-of-bounds oscillation
	static constexpr double ALPHA_MAX = 10.0;

	//! Find optimal decay parameters for the given tracked workload.
	//! @param workload    Tracked queries from a completed tracking window
	//! @param num_workers Number of worker threads (W)
	//! @param prev_lambda λ from the previous optimization run (0.9 on first run)
	//! @return The (d_start, λ) pair that minimizes mean relative slowdown
	static OptimalParams Optimize(const vector<QueryTraceEntry> &workload, idx_t num_workers, double prev_lambda);
};

} // namespace duckdb
