//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_hardware_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

#include <memory>

namespace duckdb {

class DatabaseInstance;
struct QueryHardwareManagerState;

struct QueryHardwareWorkerTarget {
	idx_t worker_id = 0;
	QueryRequestHardwareConfiguration hardware;
	bool assigned = false;
};

struct QueryHardwareTargetPlan {
	uint64_t generation = 0;
	vector<QueryHardwareWorkerTarget> workers;
};

struct QueryHardwareManagerSnapshot {
	bool active = false;
	bool hardware_control_enabled = false;
	uint64_t published_generation = 0;
	uint64_t applied_generation = 0;
	idx_t registered_workers = 0;
	idx_t physical_core_domains = 0;
	idx_t socket_domains = 0;
	uint64_t msr_write_count = 0;
	uint64_t verification_failure_count = 0;
	uint64_t restoration_attempt_count = 0;
	uint64_t restoration_verified_count = 0;
	uint64_t restoration_failure_count = 0;
	bool power_model_loaded = false;
	string status;
	string restoration_status;
};

struct QueryHardwareWorkerTopology {
	idx_t worker_id = 0;
	int logical_cpu = -1;
	int socket_id = -1;
	int physical_core_id = -1;
};

struct QueryHardwarePowerCalibrationEntry {
	int socket_id = -1;
	uint32_t core_frequency_khz = 0;
	uint32_t uncore_frequency_khz = 0;
	double base_power_w = 0;
};

struct QueryHardwarePowerModelSnapshot {
	bool valid = false;
	uint32_t minimum_core_frequency_khz = 0;
	uint32_t minimum_uncore_frequency_khz = 0;
	vector<QueryHardwarePowerCalibrationEntry> entries;

	double SocketIncrementalPowerW(int socket_id, uint32_t uncore_frequency_khz) const;
	double CoreIncrementalPowerW(int socket_id, uint32_t core_frequency_khz, uint32_t uncore_frequency_khz,
	                           idx_t physical_cores_on_socket) const;
};

//! Applies scheduler hardware targets outside scheduler/dispatcher locks. Core requests are reduced by physical-core
//! max and uncore requests by socket max. Every changed target is applied and verified synchronously before work.
class QueryHardwareManager {
public:
	explicit QueryHardwareManager(DatabaseInstance &db);
	~QueryHardwareManager();

	QueryHardwareManager(const QueryHardwareManager &) = delete;
	QueryHardwareManager &operator=(const QueryHardwareManager &) = delete;

	void RegisterWorker(idx_t worker_id, int logical_cpu);
	void UnregisterWorker(idx_t worker_id);
	void Activate();
	void Deactivate();
	void PublishTargets(std::shared_ptr<const QueryHardwareTargetPlan> plan);
	void UpdateWorkerTarget(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware, bool assigned,
	                        uint64_t generation);
	bool IsWorkerAtConfiguration(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware) const;
	QueryRequestHardwareConfiguration GetAppliedConfigurationForCPU(int logical_cpu) const;

	QueryRequestHardwareConfiguration ReferenceConfiguration() const;
	QueryRequestHardwareConfiguration MinimumConfiguration() const;
	const vector<uint32_t> &CoreFrequencyLevelsKHz() const;
	const vector<uint32_t> &UncoreFrequencyLevelsKHz() const;
	vector<QueryHardwareWorkerTopology> GetWorkerTopology() const;
	QueryHardwarePowerModelSnapshot GetPowerModelSnapshot() const;
	QueryHardwareManagerSnapshot GetSnapshot() const;

private:
	DatabaseInstance &db;
	unique_ptr<QueryHardwareManagerState> state;
};

} // namespace duckdb
