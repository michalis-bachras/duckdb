//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/pipeline_dvfs_profiler.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct PipelineProfilingSettings;
struct PipelineProfilingInfo;

//! Snapshot of one grouped perf counter set used by pipeline-level DVFS profiling.
struct PipelinePerfGroupSnapshot {
	bool valid = false;
	uint64_t time_enabled = 0;
	uint64_t time_running = 0;
	vector<uint64_t> values;
};

//! Per-CPU counter deltas for one pipeline interval.
struct PipelinePerfCPUCounters {
	int cpu = -1;
	double min_running_pct = 0;
	double cycles = 0;
	double instructions = 0;
	double cache_references = 0;
	double cache_misses = 0;
};

//! Move-only owner for perf_event_open file descriptors.
class PipelinePerfFD {
public:
	PipelinePerfFD();
	explicit PipelinePerfFD(int fd);
	~PipelinePerfFD();

	PipelinePerfFD(PipelinePerfFD &&other) noexcept;
	PipelinePerfFD &operator=(PipelinePerfFD &&other) noexcept;
	PipelinePerfFD(const PipelinePerfFD &) = delete;
	PipelinePerfFD &operator=(const PipelinePerfFD &) = delete;

	int Get() const;
	void Reset(int fd = -1);
	int Release();

private:
	int fd;
};

//! Query-level pre-opened RAPL power PMU group used by pipeline-level DVFS profiling.
struct PipelineRaplPerfGroupInfo {
	int leader_fd = -1;
	vector<PipelinePerfFD> fds;
	vector<idx_t> counter_ids;
	vector<double> scales;
};

//! Query-level pre-opened perf group used by pipeline-level DVFS profiling.
struct PipelinePerfGroupInfo {
	int cpu = -1;
	int leader_fd = -1;
	vector<PipelinePerfFD> fds;
	vector<idx_t> counter_ids;
};

//! Owns optional Linux perf/RAPL state for direct pipeline-level DVFS measurements.
class PipelineDVFSProfiler {
public:
	DUCKDB_API PipelineDVFSProfiler();
	DUCKDB_API ~PipelineDVFSProfiler();

	DUCKDB_API bool MetricsEnabled() const;
	DUCKDB_API bool TaskTraceEnabled() const;
	DUCKDB_API void Reset();
	DUCKDB_API void Initialize(const PipelineProfilingSettings &settings);
	DUCKDB_API void Start(PipelineProfilingInfo &profile);
	DUCKDB_API void Stop(PipelineProfilingInfo &profile);

	DUCKDB_API static uint64_t TimestampNs();

private:
	void Close();

private:
	bool pipeline_dvfs_metrics_enabled;
	bool pipeline_task_trace_enabled;
	bool pipeline_perf_setup_attempted;
	bool pipeline_perf_supported;
	bool pipeline_perf_per_cpu_enabled;
	string pipeline_perf_scope;
	string pipeline_perf_cgroup_path;
	string pipeline_perf_error;
	idx_t pipeline_perf_fd_count;
	idx_t pipeline_perf_cpu_count;
	vector<PipelinePerfGroupInfo> pipeline_perf_groups;
	bool pipeline_rapl_perf_supported;
	string pipeline_rapl_perf_error;
	idx_t pipeline_rapl_perf_fd_count;
	vector<PipelineRaplPerfGroupInfo> pipeline_rapl_perf_groups;
};

} // namespace duckdb
