//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_sla_energy_model.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/query_sla_model.hpp"

namespace duckdb {

struct QuerySLAEnergyModelInput {
	QuerySLAModelInput sla_input;
	QuerySLAModel reference_model;
	QueryRequestHardwareConfiguration reference_hardware;
	vector<QueryRequestPipelineHardwareProfileEstimate> hardware_profiles;
};

struct QuerySLAEnergyWorkerAssignment {
	QueryRequestHardwareConfiguration hardware;
	bool mandatory = false;
	bool optional = false;
	bool liveness = false;
	bool exploration = false;
};

struct QuerySLAEnergyQueryAllocation {
	vector<QuerySLAEnergyWorkerAssignment> workers;
	double aggregate_service_rate = 0;
	double aggregate_active_power_w = 0;
	double predicted_epoch_energy_j = 0;
	double predicted_sla_cost = 0;
	double predicted_optional_risk = 0;
	idx_t mandatory_workers = 0;
	idx_t optional_workers = 0;
	idx_t liveness_workers = 0;
};

struct QuerySLAEnergyAllocation {
	vector<QuerySLAEnergyQueryAllocation> queries;
	idx_t unassigned_workers = 0;
};

QuerySLAEnergyAllocation AllocateQuerySLAEnergyWorkers(const vector<QuerySLAEnergyModelInput> &inputs,
	                                                   idx_t worker_budget, double energy_lambda,
	                                                   bool include_optional_and_liveness = true);

} // namespace duckdb
