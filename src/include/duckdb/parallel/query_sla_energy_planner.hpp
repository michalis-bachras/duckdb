//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_sla_energy_planner.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_energy_model.hpp"

namespace duckdb {

struct QuerySLAEnergyPlacedWorker {
	idx_t worker_id = DConstants::INVALID_INDEX;
	idx_t query_index = DConstants::INVALID_INDEX;
	QueryRequestHardwareConfiguration preferred_hardware;
	QueryRequestHardwareConfiguration normal_hardware;
	QueryRequestHardwareConfiguration execution_hardware;
	bool mandatory = false;
	bool optional = false;
	bool liveness = false;
	bool exploration = false;
};

struct QuerySLAEnergyPlannerDiagnostics {
	idx_t mandatory_workers = 0;
	idx_t o1_workers = 0;
	idx_t o2_workers = 0;
	idx_t o3_workers = 0;
	idx_t liveness_workers = 0;
	idx_t parked_workers = 0;
	idx_t core_probes = 0;
	idx_t uncore_probes = 0;
	idx_t forced_uncore_probes = 0;
	bool optional_only_socket_available = false;
	idx_t profile_fallback_count = 0;
	idx_t immature_pair_rejections = 0;
	idx_t domain_pair_promotions = 0;
	double mandatory_footprint_energy_j = 0;
	double optional_incremental_energy_j = 0;
};

struct QuerySLAEnergyPlannerInput {
	vector<QuerySLAEnergyModelInput> queries;
	vector<QueryHardwareWorkerTopology> topology;
	QueryHardwarePowerModelSnapshot power_model;
	vector<uint32_t> core_frequency_levels_khz;
	vector<uint32_t> uncore_frequency_levels_khz;
	idx_t worker_budget = 0;
	uint64_t epoch_ns = 0;
	uint64_t epoch_generation = 0;
	double energy_lambda = 1.0;
	bool exploration_enabled = false;
	uint64_t exploration_seed = 1;
	idx_t epochs_without_optional_only_socket = 0;
};

struct QuerySLAEnergyEpochPlan {
	vector<QuerySLAEnergyQueryAllocation> queries;
	vector<QuerySLAEnergyPlacedWorker> workers;
	QuerySLAEnergyPlannerDiagnostics diagnostics;
	bool valid = false;
	string error;
};

//! Pure topology-aware planner. It performs Phase 1a/2a, mandatory placement, O1-O3 optional allocation, and the
//! deterministic epoch-scoped exploration overlay without touching scheduler state or hardware.
QuerySLAEnergyEpochPlan PlanQuerySLAEnergyEpoch(const QuerySLAEnergyPlannerInput &input);

} // namespace duckdb
