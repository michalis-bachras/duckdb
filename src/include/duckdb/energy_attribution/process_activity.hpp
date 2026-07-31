//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/energy_attribution/process_activity.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class ProcessActivityKind : uint8_t {
	PROCESS,
	HOST_KERNEL,
	HOST_IRQ,
	MEASUREMENT_OVERHEAD,
	SYSTEM_IDLE
};

struct ProcessActivityRecord {
	uint64_t generation = 0;
	uint64_t process_start_time_ns = 0;
	uint32_t tgid = 0;
	uint32_t tid = 0;
	int socket_id = -1;
	ProcessActivityKind kind = ProcessActivityKind::PROCESS;
	string comm;
	uint64_t runtime_ns = 0;
	uint64_t cycles = 0;
	uint64_t ref_cycles = 0;
	uint64_t time_enabled_ns = 0;
	uint64_t time_running_ns = 0;
	uint64_t ref_time_enabled_ns = 0;
	uint64_t ref_time_running_ns = 0;
	uint64_t switch_count = 0;
	double corrected_cycles = 0;
	double frequency_ratio = 0;
	double pmu_running_ratio = 0;
	double ref_pmu_running_ratio = 0;
	bool pmu_valid = false;
};

struct ProcessActivitySocketTopology {
	int socket_id = -1;
	uint32_t monitored_cpu_count = 0;
};

struct ProcessActivitySocketCheckpoint {
	int socket_id = -1;
	uint32_t helper_count = 0;
	uint32_t acknowledged_helper_count = 0;
	uint32_t missing_helper_count = 0;
	uint64_t boundary_min_ns = 0;
	uint64_t boundary_max_ns = 0;
	uint64_t maximum_checkpoint_delay_ns = 0;
	uint64_t late_helper_count = 0;
	int32_t slowest_helper_cpu = -1;
	string missing_helper_cpus;
	bool valid = false;
};

struct ProcessActivityWindow {
	uint64_t generation = 0;
	uint64_t requested_boundary_ns = 0;
	uint64_t transition_ns = 0;
	uint64_t boundary_min_ns = 0;
	uint64_t boundary_max_ns = 0;
	uint64_t maximum_checkpoint_delay_ns = 0;
	uint64_t checkpoint_duration_ns = 0;
	uint64_t helper_count = 0;
	int64_t helper_futex_wake_count = -1;
	uint64_t acknowledged_helper_count = 0;
	uint64_t missing_helper_count = 0;
	int32_t slowest_helper_cpu = -1;
	string missing_helper_cpus;
	uint64_t activity_map_insert_failure = 0;
	uint64_t missing_counter = 0;
	uint64_t invalid_counter_delta = 0;
	uint64_t interrupt_underflow = 0;
	uint64_t boundary_split_count = 0;
	uint64_t boundary_split_cpu_count = 0;
	uint64_t boundary_split_runtime_ns = 0;
	uint64_t boundary_split_cycles = 0;
	uint64_t boundary_split_ref_cycles = 0;
	uint64_t late_helper_count = 0;
	uint64_t lost_records = 0;
	uint64_t lookup_failures = 0;
	vector<ProcessActivitySocketTopology> socket_topology;
	vector<ProcessActivitySocketCheckpoint> socket_checkpoints;
	vector<ProcessActivityRecord> records;
	bool boundary_corrected = false;
	bool valid = false;
	string status;
};

class ProcessActivityCollector {
public:
	virtual ~ProcessActivityCollector() = default;

	static unique_ptr<ProcessActivityCollector> Create();
	static bool IsCompiled();

	virtual bool Start(bool required) = 0;
	virtual ProcessActivityWindow RotateWindow(uint64_t requested_boundary_ns, uint64_t maximum_skew_ns,
	                                           uint64_t completion_timeout_ns) = 0;
	virtual void Stop() = 0;
	virtual bool Available() const = 0;
	virtual uint32_t DuckDBProcessID() const = 0;
	virtual const string &LastError() const = 0;
};

const char *ProcessActivityKindToString(ProcessActivityKind kind);

} // namespace duckdb
