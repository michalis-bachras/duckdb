//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/workload_simulator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/scheduler_slot_array.hpp"

namespace duckdb {

//! WorkloadSimulator replays a tracked workload with candidate scheduling
//! parameters using a discrete-event simulation of W asynchronous workers.
//!
//! Used by the self-tuning optimizer (Section 4 of the paper) to evaluate
//! candidate (λ, d_start) parameter pairs without affecting live scheduling.
//!
//! The simulation faithfully models:
//! - Stride scheduling: min-pass query selection per worker
//! - Priority decay: configurable λ and d_start
//! - Asynchronous workers: each worker advances independently by avg_quantum_ms
//! - Wall-clock asymmetry: fast quanta free workers sooner than slow ones
class WorkloadSimulator {
public:
	//! Simulate the tracked workload with candidate decay parameters.
	//! Returns the mean relative slowdown (cost function from paper Eq. 3).
	//!
	//! @param workload   Tracked queries from a completed tracking window
	//! @param num_workers Number of worker threads (W)
	//! @param lambda     Candidate decay factor (λ)
	//! @param d_start    Candidate decay onset (in quanta)
	//! @return Cost = (1/N) × Σ(sim_wall_time / base_time), lower is better
	static double Simulate(const vector<QueryTraceEntry> &workload, idx_t num_workers, double lambda, int d_start);
};

} // namespace duckdb
