//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_profile.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct PipelineProfile {
	// Identity
	idx_t pipeline_id = DConstants::INVALID_INDEX;

	// Operator composition
	string source_name;
	vector<string> operator_names; // intermediate operators
	string sink_name;

	// Metrics
	atomic<double> cpu_time_seconds {0};
	double latency_seconds = 0;
	idx_t parallelism = 0;

	// Latency tracking
	time_point<steady_clock> schedule_start_time;

	void MarkScheduleStart() {
		schedule_start_time = steady_clock::now();
	}

	void MarkFinished() {
		auto end = steady_clock::now();
		latency_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - schedule_start_time).count();
	}

	void AddCPUTime(double seconds) {
		double old_val = cpu_time_seconds.load();
		while (!cpu_time_seconds.compare_exchange_weak(old_val, old_val + seconds)) {
		}
	}

	double GetCPUTime() const {
		return cpu_time_seconds.load();
	}
};

} // namespace duckdb
