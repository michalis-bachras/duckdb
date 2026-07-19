//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_stride_optimizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct QueryStrideTraceEntry {
	double arrival_time_ms = 0;
	idx_t window_quanta = 0;
	double average_quantum_ms = 0;
	double in_window_wall_time_ms = 0;
	double initial_priority = 0;
	double minimum_priority = 0;
	bool static_priority = false;
};

struct QueryStrideOptimalParameters {
	QueryStrideOptimalParameters() = default;
	QueryStrideOptimalParameters(int decay_start_p, double decay_lambda_p, double objective_p)
	    : decay_start(decay_start_p), decay_lambda(decay_lambda_p), objective(objective_p) {
	}

	int decay_start = 10;
	double decay_lambda = 0.9;
	double objective = 0;
};

class QueryStrideWorkloadSimulator {
public:
	static double Simulate(const vector<QueryStrideTraceEntry> &workload, idx_t worker_count, double decay_lambda,
	                       int decay_start);
};

class QueryStrideOptimizer {
public:
	static constexpr int SEARCH_STEPS = 7;
	static constexpr double DIRECTION = 0.05;
	static constexpr double MAXIMUM_STEP_SCALE = 10.0;

	static QueryStrideOptimalParameters Optimize(const vector<QueryStrideTraceEntry> &workload, idx_t worker_count,
	                                             double previous_lambda);
};

} // namespace duckdb
