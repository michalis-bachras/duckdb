//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_sla_energy_model.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/query_sla_model.hpp"

namespace duckdb {

enum class QuerySLAEnergyOptionalStage : uint8_t { NONE = 0, O1, O2, O3, REFERENCE_FALLBACK, DYNAMIC };

struct QuerySLAEnergyModelInput {
	QuerySLAModelInput sla_input;
	QuerySLAModel reference_model;
	QueryRequestHardwareConfiguration reference_hardware;
	vector<QueryRequestPipelineHardwareProfileEstimate> hardware_profiles;
};

struct QuerySLAEnergyWorkerAssignment {
	bool valid = false;
	QueryRequestHardwareConfiguration hardware;
	double safe_throughput = 0;
	double active_power_w = 0;
	double normalized_edp = 0;
	double mandatory_gain = 0;
	double optional_fragility_before = 0;
	double optional_fragility_target = 0;
	double optional_fragility_after = 0;
	double optional_gain = 0;
	uint64_t profile_version = 0;
	idx_t worker_ordinal = DConstants::INVALID_INDEX;
	QuerySLAEnergyOptionalStage optional_stage = QuerySLAEnergyOptionalStage::NONE;
	bool mandatory = false;
	bool optional = false;
	bool exploration = false;
};

struct QuerySLAEnergyQueryAllocation {
	vector<QuerySLAEnergyWorkerAssignment> workers;
	vector<QuerySLAEnergyWorkerAssignment> dynamic_mandatory_workers;
	vector<QuerySLAEnergyWorkerAssignment> dynamic_optional_workers;
	double aggregate_service_rate = 0;
	double aggregate_active_power_w = 0;
	double predicted_epoch_energy_j = 0;
	double predicted_sla_cost = 0;
	double predicted_optional_risk = 0;
	double mandatory_target_sla_cost = 0;
	double mandatory_target_optional_risk = 0;
	double mandatory_preferred_service_rate = 0;
	double mandatory_reference_safe_throughput = 0;
	double mandatory_preferred_normalized_edp = 0;
	uint64_t mandatory_profile_version = 0;
	idx_t mandatory_workers = 0;
	idx_t optional_workers = 0;
};

struct QuerySLAEnergyAllocation {
	vector<QuerySLAEnergyQueryAllocation> queries;
	idx_t unassigned_workers = 0;
	bool valid = false;
	string error;
};

//! Active-work energy clipped to the epoch duration and the remaining pipeline work.
double QuerySLAPredictedEpochEnergy(const QuerySLAModelInput &input, double service_rate, double power_w);

QuerySLAModel BuildQuerySLAEnergyReferenceModel(const QuerySLAEnergyModelInput &input);
QuerySLAEnergyWorkerAssignment SelectQuerySLAMandatoryAddition(const QuerySLAEnergyModelInput &input,
	                                                           const QuerySLAModel &reference_model,
	                                                           double existing_service_rate,
	                                                           idx_t existing_worker_count);
QuerySLAEnergyWorkerAssignment SelectQuerySLAOptionalAddition(
    const QuerySLAEnergyModelInput &input, double existing_service_rate, double existing_optional_risk,
    idx_t existing_worker_count, const QueryRequestHardwareConfiguration &reference_hardware,
    const vector<QueryRequestHardwareConfiguration> &allowed_hardware,
    QuerySLAEnergyOptionalStage stage = QuerySLAEnergyOptionalStage::DYNAMIC);
QuerySLAEnergyAllocation AllocateQuerySLAMandatoryWorkers(const vector<QuerySLAEnergyModelInput> &inputs,
	                                                      const vector<idx_t> &mandatory_targets);

} // namespace duckdb
