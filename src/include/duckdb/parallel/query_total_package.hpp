#pragma once

#include "duckdb/parallel/query_sla_energy_planner.hpp"

namespace duckdb {

//! Explicit units: seconds of one reference worker, never sums of native work units.
struct TotalPackageStage {
	idx_t pipeline_id = 0;
	double remaining_reference_work_s = 0;
	idx_t parallelism_cap = 0;
	vector<double> relative_rates;
	vector<double> work_j_per_reference_s;
};

struct TotalPackageProjection {
	double useful_reference_s = 0;
	double total_package_j = 0;
	double idle_j = 0;
	double socket_j = 0;
	double core_j = 0;
	double work_j = 0;
	idx_t final_lease = 0;
	idx_t completed_stages = 0;
	bool valid = false;
	string reason;
};

//! A matched operational baseline. ADD/awake calibration is not an isolated physical constant.
struct TotalPackageSocketCalibration {
	int socket_id = -1;
	double idle_w = 0;
	double awake_w = 0;
	vector<double> physical_core_w;
};

struct TotalPackageCandidateTrace {
	uint64_t epoch_generation = 0;
	idx_t candidate_id = 0;
	double reference_work_s = 0;
	double package_energy_j = 0;
	double reference_efficiency = 0;
	double candidate_efficiency = 0;
	double modeled_cost = 0;
	double greedy_allocation_cost = 0;
	double exact_allocation_cost = -1;
	idx_t non_diminishing_queries = 0;
	bool selected = false;
	bool shadow_selected = false;
	string reason;
};

TotalPackageProjection ProjectTotalPackageChain(const vector<TotalPackageStage> &stages, idx_t initial_lease,
                                               const vector<idx_t> &worker_frequency_indexes, double epoch_s,
                                               double idle_w, double awake_w, const vector<double> &core_w);
double TotalPackageEfficiency(double useful_reference_s, double total_package_j);
bool TotalPackagePreferCandidate(double service, double energy, double reference_service, double reference_energy,
                                double minimum_gain);
bool TotalPackageDiminishingBenefits(const vector<double> &costs);
//! Exact resource-allocation oracle for tests; never used to fabricate experimental measurements.
double TotalPackageExactAllocationCost(const vector<vector<double>> &costs, idx_t budget);
vector<QueryRequestPipelineHardwareProfileEstimate> LoadTotalPackageIsolatedProfiles(const string &path);

//! Native topology-aware, grant-preserving implementation. Reference-only is also the shadow action.
QuerySLAEnergyEpochPlan PlanTotalPackageEpoch(const QuerySLAEnergyPlannerInput &input,
                                            const vector<TotalPackageSocketCalibration> &calibration,
                                            bool reference_only, bool lowest_safe, double improvement_margin,
	                                            vector<TotalPackageCandidateTrace> &trace, bool shadow = false);

} // namespace duckdb
