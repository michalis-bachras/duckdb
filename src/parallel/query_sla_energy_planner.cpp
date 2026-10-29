//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_energy_planner.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_energy_planner.hpp"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <map>
#include <set>
#include <sstream>

namespace duckdb {

namespace {

struct CoreKey {
	CoreKey() {
	}
	CoreKey(int socket_id_p, int core_id_p) : socket_id(socket_id_p), core_id(core_id_p) {
	}

	int socket_id = -1;
	int core_id = -1;

	bool operator<(const CoreKey &other) const {
		if (socket_id != other.socket_id) {
			return socket_id < other.socket_id;
		}
		return core_id < other.core_id;
	}
};

struct PlannerHardwareCandidate {
	bool valid = false;
	QueryRequestHardwareConfiguration hardware;
	double throughput = 0;
	double active_power_w = 0;
	double normalized_edp = 0;
	uint64_t profile_version = 0;
};

struct MandatoryToken {
	MandatoryToken(idx_t query_index_p, const QuerySLAEnergyWorkerAssignment &assignment_p)
	    : query_index(query_index_p), assignment(assignment_p) {
	}

	idx_t query_index = 0;
	QuerySLAEnergyWorkerAssignment assignment;
};

struct CoreSlots {
	CoreKey key;
	vector<QueryHardwareWorkerTopology> workers;
};

struct OptionalToken {
	bool valid = false;
	idx_t query_index = DConstants::INVALID_INDEX;
	idx_t worker_ordinal = DConstants::INVALID_INDEX;
	int socket_id = -1;
	QueryRequestHardwareConfiguration hardware;
	double throughput = 0;
	double active_power_w = 0;
	double normalized_edp = 0;
	double fragility_before = 0;
	double fragility_target = 0;
	double fragility_after = 0;
	double gain = 0;
	uint64_t profile_version = 0;
	QuerySLAEnergyOptionalStage stage = QuerySLAEnergyOptionalStage::NONE;
};

struct HardwareReconciliationScore {
	bool valid = false;
	bool energy_valid = false;
	double sla_cost_violation = 0;
	double fragility_violation = 0;
	double normalized_edp = 0;
	uint64_t frequency_sum_khz = 0;
};

static QueryRequestHardwareConfiguration PlannerHardware(uint32_t core_khz, uint32_t uncore_khz) {
	QueryRequestHardwareConfiguration result;
	result.core_frequency_khz = core_khz;
	result.uncore_frequency_khz = uncore_khz;
	return result;
}

static PlannerHardwareCandidate FindHardwareCandidate(const QuerySLAEnergyModelInput &query,
	                                                   const QueryRequestHardwareConfiguration &hardware,
	                                                   QuerySLAEnergyPlannerDiagnostics *diagnostics = nullptr) {
	PlannerHardwareCandidate result;
	result.hardware = hardware;
	for (const auto &profile : query.hardware_profiles) {
		if (!(profile.hardware == hardware)) {
			continue;
		}
		if (!profile.mature || profile.safe_work_units_per_s <= 0) {
			if (diagnostics) {
				diagnostics->immature_pair_rejections++;
			}
			break;
		}
		result.valid = true;
		result.throughput = profile.safe_work_units_per_s;
		result.active_power_w =
		    profile.ewma_active_power_w > 0 ? profile.ewma_active_power_w : profile.mean_active_power_w;
		result.normalized_edp = result.active_power_w > 0
		                            ? result.active_power_w / (result.throughput * result.throughput)
		                            : 0;
		result.profile_version = profile.profile_version;
		return result;
	}
	if (hardware == query.reference_hardware && query.reference_model.valid &&
	    query.reference_model.throughput > 0) {
		result.valid = true;
		result.throughput = query.reference_model.throughput;
		if (diagnostics) {
			diagnostics->profile_fallback_count++;
		}
	}
	return result;
}

static idx_t ExactPairSampleCount(const QuerySLAEnergyModelInput &query,
	                              const QueryRequestHardwareConfiguration &hardware) {
	for (const auto &profile : query.hardware_profiles) {
		if (profile.hardware == hardware) {
			return MinValue<idx_t>(profile.throughput_sample_count, profile.power_sample_count);
		}
	}
	return 0;
}

static uint64_t MixExplorationSeed(uint64_t value) {
	value += 0x9e3779b97f4a7c15ULL;
	value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
	value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
	return value ^ (value >> 31U);
}

static idx_t ExplorationIndex(uint64_t seed, uint64_t epoch_generation, uint64_t salt, idx_t count) {
	D_ASSERT(count > 0);
	auto mixed = MixExplorationSeed(seed ^ MixExplorationSeed(epoch_generation) ^ salt);
	return NumericCast<idx_t>(mixed % count);
}

static vector<CoreSlots> BuildCoreSlots(const vector<QueryHardwareWorkerTopology> &topology) {
	std::map<CoreKey, vector<QueryHardwareWorkerTopology>> grouped;
	for (const auto &worker : topology) {
		if (worker.logical_cpu < 0 || worker.socket_id < 0 || worker.physical_core_id < 0) {
			continue;
		}
		grouped[CoreKey(worker.socket_id, worker.physical_core_id)].push_back(worker);
	}
	vector<CoreSlots> result;
	result.reserve(grouped.size());
	for (auto &entry : grouped) {
		std::sort(entry.second.begin(), entry.second.end(), [](const QueryHardwareWorkerTopology &left,
		                                                       const QueryHardwareWorkerTopology &right) {
			return left.logical_cpu < right.logical_cpu;
		});
		CoreSlots slots;
		slots.key = entry.first;
		slots.workers = std::move(entry.second);
		result.push_back(std::move(slots));
	}
	return result;
}

static vector<QueryHardwareWorkerTopology> FlattenSlots(const vector<CoreSlots> &cores) {
	vector<QueryHardwareWorkerTopology> result;
	for (const auto &core : cores) {
		for (const auto &worker : core.workers) {
			result.push_back(worker);
		}
	}
	return result;
}

static void ResolveActualHardware(vector<QuerySLAEnergyPlacedWorker> &workers,
	                              const vector<QueryHardwareWorkerTopology> &topology) {
	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	for (const auto &worker : topology) {
		worker_topology[worker.worker_id] = worker;
	}
	std::map<CoreKey, uint32_t> core_targets;
	std::map<int, uint32_t> socket_targets;
	for (const auto &worker : workers) {
		auto topology_entry = worker_topology.find(worker.worker_id);
		if (topology_entry == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		core_targets[core] = MaxValue<uint32_t>(core_targets[core], worker.preferred_hardware.core_frequency_khz);
		socket_targets[core.socket_id] =
		    MaxValue<uint32_t>(socket_targets[core.socket_id], worker.preferred_hardware.uncore_frequency_khz);
	}
	for (auto &worker : workers) {
		auto topology_entry = worker_topology.find(worker.worker_id);
		if (topology_entry == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		worker.normal_hardware.core_frequency_khz = core_targets[core];
		worker.normal_hardware.uncore_frequency_khz = socket_targets[core.socket_id];
		worker.execution_hardware = worker.normal_hardware;
	}
}

static bool HardwareAtLeast(const QueryRequestHardwareConfiguration &actual,
	                        const QueryRequestHardwareConfiguration &preferred) {
	return actual.core_frequency_khz >= preferred.core_frequency_khz &&
	       actual.uncore_frequency_khz >= preferred.uncore_frequency_khz;
}

static idx_t CountWorkerFrequencyAdjustments(const vector<QuerySLAEnergyPlacedWorker> &workers) {
	idx_t result = 0;
	for (const auto &worker : workers) {
		result += (worker.normal_hardware == worker.preferred_hardware) ? 0 : 1;
	}
	return result;
}

static double PlannerServiceTolerance(double expected, idx_t worker_count) {
	return 64.0 * std::numeric_limits<double>::epsilon() *
	       MaxValue<double>(1.0, std::abs(expected)) *
	       static_cast<double>(MaxValue<idx_t>(1, worker_count));
}

static string PlannerValidationDetail(idx_t query_index, const QuerySLAEnergyPlannerInput &input,
	                                  const string &message) {
	std::ostringstream result;
	result << std::setprecision(17) << "query index " << query_index;
	if (query_index < input.queries.size()) {
		result << " db_query_id=" << input.queries[query_index].sla_input.metadata.db_query_id
		       << " pipeline=" << input.queries[query_index].sla_input.work.pipeline_id;
	}
	result << ": " << message;
	return result.str();
}

static bool RebuildQueryAllocationsFromPreferences(QuerySLAEnergyEpochPlan &plan,
	                                                const QuerySLAEnergyPlannerInput &input, string &error) {
	auto previous = plan.queries;
	vector<QuerySLAEnergyQueryAllocation> allocations(input.queries.size());
	vector<vector<bool>> seen(input.queries.size());
	vector<long double> reconstructed_service(input.queries.size(), 0);
	vector<long double> reconstructed_power(input.queries.size(), 0);
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		if (query_index < previous.size()) {
			allocations[query_index].mandatory_target_sla_cost =
			    previous[query_index].mandatory_target_sla_cost;
			allocations[query_index].mandatory_target_optional_risk =
			    previous[query_index].mandatory_target_optional_risk;
			allocations[query_index].mandatory_preferred_service_rate =
			    previous[query_index].mandatory_preferred_service_rate;
			allocations[query_index].mandatory_reference_safe_throughput =
			    previous[query_index].mandatory_reference_safe_throughput;
			allocations[query_index].mandatory_preferred_normalized_edp =
			    previous[query_index].mandatory_preferred_normalized_edp;
			allocations[query_index].mandatory_profile_version =
			    previous[query_index].mandatory_profile_version;
			allocations[query_index].aggregate_service_rate =
			    previous[query_index].aggregate_service_rate;
			allocations[query_index].aggregate_active_power_w =
			    previous[query_index].aggregate_active_power_w;
			seen[query_index].resize(previous[query_index].workers.size(), false);
		}
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input, 0, 0);
		if (evaluation.valid) {
			allocations[query_index].predicted_sla_cost = evaluation.mandatory_cost;
			allocations[query_index].predicted_optional_risk = evaluation.optional_risk;
		}
	}
	for (const auto &worker : plan.workers) {
		if (worker.query_index >= input.queries.size() ||
		    !HardwareAtLeast(worker.normal_hardware, worker.preferred_hardware) ||
		    worker.preferred_safe_throughput < 0) {
			error = "placed worker has an invalid query, hardware preference, or throughput";
			return false;
		}
		auto query_index = worker.query_index;
		if (query_index >= previous.size() || worker.worker_ordinal >= previous[query_index].workers.size()) {
			error = PlannerValidationDetail(query_index, input, "placed worker has an invalid ordinal");
			return false;
		}
		auto &expected = previous[query_index].workers[worker.worker_ordinal];
		if (seen[query_index][worker.worker_ordinal]) {
			error = PlannerValidationDetail(query_index, input, "placed worker ordinal appears more than once");
			return false;
		}
		if (worker.mandatory != expected.mandatory || worker.optional != expected.optional ||
		    !(worker.preferred_hardware == expected.hardware) ||
		    worker.preferred_safe_throughput != expected.safe_throughput ||
		    worker.profile_version != expected.profile_version ||
		    worker.optional_stage != expected.optional_stage) {
			error = PlannerValidationDetail(query_index, input, "placed worker no longer matches its accepted token");
			return false;
		}
		seen[query_index][worker.worker_ordinal] = true;
		auto &allocation = allocations[worker.query_index];
		QuerySLAEnergyWorkerAssignment assignment;
		assignment.valid = true;
		assignment.hardware = worker.preferred_hardware;
		assignment.safe_throughput = worker.preferred_safe_throughput;
			assignment.active_power_w = worker.preferred_active_power_w;
			assignment.normalized_edp = worker.preferred_normalized_edp;
			assignment.optional_fragility_before = worker.optional_fragility_before;
			assignment.optional_fragility_target = worker.optional_fragility_target;
			assignment.optional_fragility_after = worker.optional_fragility_after;
			assignment.optional_gain = worker.optional_gain;
			assignment.profile_version = worker.profile_version;
			assignment.worker_ordinal = worker.worker_ordinal;
			assignment.optional_stage = worker.optional_stage;
			assignment.mandatory = worker.mandatory;
			assignment.optional = worker.optional;
			allocation.workers.push_back(assignment);
		reconstructed_service[query_index] += static_cast<long double>(worker.preferred_safe_throughput);
		reconstructed_power[query_index] += static_cast<long double>(worker.preferred_active_power_w);
		allocation.mandatory_workers += worker.mandatory ? 1 : 0;
			allocation.optional_workers += worker.optional ? 1 : 0;
		}
		for (auto &allocation : allocations) {
			std::sort(allocation.workers.begin(), allocation.workers.end(),
			          [](const QuerySLAEnergyWorkerAssignment &left,
			             const QuerySLAEnergyWorkerAssignment &right) {
				          return left.worker_ordinal < right.worker_ordinal;
		          });
	}
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto &allocation = allocations[query_index];
		if (query_index >= previous.size() ||
		    allocation.mandatory_workers != previous[query_index].mandatory_workers ||
		    allocation.optional_workers != previous[query_index].optional_workers ||
		    std::find(seen[query_index].begin(), seen[query_index].end(), false) != seen[query_index].end()) {
			error = PlannerValidationDetail(query_index, input, "placed worker set differs from the accepted allocation");
			return false;
		}
		auto reconstructed_service_rate = static_cast<double>(reconstructed_service[query_index]);
		auto service_tolerance =
		    PlannerServiceTolerance(allocation.aggregate_service_rate, allocation.workers.size());
		if (std::abs(reconstructed_service_rate - allocation.aggregate_service_rate) > service_tolerance) {
			std::ostringstream message;
			message << std::setprecision(17) << "token service sum=" << reconstructed_service_rate
			        << " differs from canonical service=" << allocation.aggregate_service_rate
			        << " tolerance=" << service_tolerance;
			error = PlannerValidationDetail(query_index, input, message.str());
			return false;
		}
		auto reconstructed_power_w = static_cast<double>(reconstructed_power[query_index]);
		auto power_tolerance =
		    PlannerServiceTolerance(allocation.aggregate_active_power_w, allocation.workers.size());
		if (std::abs(reconstructed_power_w - allocation.aggregate_active_power_w) > power_tolerance) {
			error = PlannerValidationDetail(query_index, input, "token power sum differs from canonical power");
			return false;
		}
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input,
		                                             allocation.aggregate_service_rate, allocation.workers.size());
		if (!evaluation.valid) {
			error = PlannerValidationDetail(query_index, input, "canonical service cannot be evaluated");
			return false;
		}
		allocation.predicted_sla_cost = evaluation.mandatory_cost;
		allocation.predicted_optional_risk = evaluation.optional_risk;
		allocation.predicted_epoch_energy_j = QuerySLAPredictedEpochEnergy(
		    input.queries[query_index].sla_input, allocation.aggregate_service_rate, allocation.aggregate_active_power_w);
		if (allocation.mandatory_workers > 0) {
			auto cost_tolerance =
			    1e-9 * MaxValue<double>(1.0, std::abs(allocation.mandatory_target_sla_cost));
			auto risk_tolerance =
			    1e-9 * MaxValue<double>(1.0, std::abs(allocation.mandatory_target_optional_risk));
			if (allocation.predicted_sla_cost >
			        allocation.mandatory_target_sla_cost + cost_tolerance ||
			    allocation.predicted_optional_risk >
			        allocation.mandatory_target_optional_risk + risk_tolerance) {
				std::ostringstream message;
				message << std::setprecision(17)
				        << "mandatory SLA target regression: actual_cost=" << allocation.predicted_sla_cost
				        << " target_cost=" << allocation.mandatory_target_sla_cost
				        << " actual_risk=" << allocation.predicted_optional_risk
				        << " target_risk=" << allocation.mandatory_target_optional_risk
				        << " service=" << allocation.aggregate_service_rate
				        << " workers=" << allocation.workers.size();
				error = PlannerValidationDetail(query_index, input, message.str());
				return false;
			}
		}
	}
	plan.queries = std::move(allocations);
	return true;
}

static bool PlannerIsLifecycleEvent(QueryActivationEventKind event_kind) {
	return event_kind == QueryActivationEventKind::INITIALIZE ||
	       event_kind == QueryActivationEventKind::PREPARE_FINISH ||
	       event_kind == QueryActivationEventKind::FINISH ||
	       event_kind == QueryActivationEventKind::COMPLETE;
}

static bool ResolveNormalWorkerEstimate(QuerySLAEnergyPlacedWorker &worker,
	                                    const QuerySLAEnergyModelInput &query,
	                                    const QuerySLAEnergyWorkerAssignment &accepted) {
	worker.normal_safe_throughput = 0;
	worker.normal_active_power_w = 0;
	worker.normal_normalized_edp = 0;
	worker.normal_profile_version = 0;
	if (!worker.normal_hardware.IsValid()) {
		return false;
	}
	if (PlannerIsLifecycleEvent(query.reference_model.event_kind)) {
		worker.normal_safe_throughput = accepted.safe_throughput;
		return true;
	}
	if (worker.normal_hardware == worker.preferred_hardware) {
		worker.normal_safe_throughput = accepted.safe_throughput;
		worker.normal_active_power_w = accepted.active_power_w;
		worker.normal_normalized_edp = accepted.normalized_edp;
		worker.normal_profile_version = accepted.profile_version;
		return true;
	}
	auto candidate = FindHardwareCandidate(query, worker.normal_hardware);
	if (!candidate.valid) {
		return false;
	}
	if (worker.normal_hardware == query.reference_hardware) {
		auto ordinal = worker.worker_ordinal;
		if (ordinal + 1 >= query.reference_model.service_rates.size()) {
			return false;
		}
		worker.normal_safe_throughput =
		    MaxValue<double>(0, query.reference_model.service_rates[ordinal + 1] -
		                            query.reference_model.service_rates[ordinal]);
	} else {
		worker.normal_safe_throughput = candidate.throughput;
	}
	worker.normal_active_power_w = candidate.active_power_w;
	worker.normal_normalized_edp = candidate.normalized_edp;
	worker.normal_profile_version = candidate.profile_version;
	return worker.normal_safe_throughput >= 0;
}

static bool RebuildQueryAllocationsFromNormalHardware(
    QuerySLAEnergyEpochPlan &plan, const QuerySLAEnergyPlannerInput &input,
    const vector<QuerySLAEnergyQueryAllocation> &accepted, HardwareReconciliationScore &score, string &error) {
	vector<QuerySLAEnergyQueryAllocation> allocations(input.queries.size());
	vector<vector<bool>> seen(input.queries.size());
	vector<long double> reconstructed_service(input.queries.size(), 0);
	vector<long double> reconstructed_power(input.queries.size(), 0);
	vector<bool> all_workers_at_preferred_pair(input.queries.size(), true);
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		if (query_index >= accepted.size()) {
			error = "accepted SLA-energy allocation does not match the query count";
			return false;
		}
		auto &allocation = allocations[query_index];
		const auto &target = accepted[query_index];
		allocation.mandatory_target_sla_cost = target.mandatory_target_sla_cost;
		allocation.mandatory_target_optional_risk = target.mandatory_target_optional_risk;
		allocation.mandatory_preferred_service_rate = target.mandatory_preferred_service_rate;
		allocation.mandatory_reference_safe_throughput = target.mandatory_reference_safe_throughput;
		allocation.mandatory_preferred_normalized_edp = target.mandatory_preferred_normalized_edp;
		allocation.mandatory_profile_version = target.mandatory_profile_version;
		seen[query_index].resize(target.workers.size(), false);
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input, 0, 0);
		if (evaluation.valid) {
			allocation.predicted_sla_cost = evaluation.mandatory_cost;
			allocation.predicted_optional_risk = evaluation.optional_risk;
		}
	}
	for (auto &worker : plan.workers) {
		if (worker.query_index >= input.queries.size() || worker.query_index >= accepted.size()) {
			error = "placed worker has an invalid query index";
			return false;
		}
		auto query_index = worker.query_index;
		if (worker.worker_ordinal >= accepted[query_index].workers.size()) {
			error = PlannerValidationDetail(query_index, input, "placed worker has an invalid ordinal");
			return false;
		}
		const auto &expected = accepted[query_index].workers[worker.worker_ordinal];
		if (seen[query_index][worker.worker_ordinal]) {
			error = PlannerValidationDetail(query_index, input, "placed worker ordinal appears more than once");
			return false;
		}
		if (worker.mandatory != expected.mandatory || worker.optional != expected.optional ||
		    !(worker.preferred_hardware == expected.hardware) ||
		    worker.preferred_safe_throughput != expected.safe_throughput ||
		    worker.profile_version != expected.profile_version ||
		    worker.optional_stage != expected.optional_stage) {
			error = PlannerValidationDetail(query_index, input, "placed worker no longer matches its accepted token");
			return false;
		}
		if (!ResolveNormalWorkerEstimate(worker, input.queries[query_index], expected)) {
			error = PlannerValidationDetail(query_index, input, "shared-domain hardware pair has no usable profile");
			return false;
		}
		seen[query_index][worker.worker_ordinal] = true;
		auto &allocation = allocations[query_index];
		QuerySLAEnergyWorkerAssignment assignment = expected;
		assignment.hardware = worker.normal_hardware;
		assignment.safe_throughput = worker.normal_safe_throughput;
		assignment.active_power_w = worker.normal_active_power_w;
		assignment.normalized_edp = worker.normal_normalized_edp;
		assignment.profile_version = worker.normal_profile_version;
		allocation.workers.push_back(assignment);
		reconstructed_service[query_index] += static_cast<long double>(worker.normal_safe_throughput);
		reconstructed_power[query_index] += static_cast<long double>(worker.normal_active_power_w);
		all_workers_at_preferred_pair[query_index] =
		    all_workers_at_preferred_pair[query_index] &&
		    (worker.normal_hardware == worker.preferred_hardware);
		allocation.mandatory_workers += worker.mandatory ? 1 : 0;
		allocation.optional_workers += worker.optional ? 1 : 0;
		if (!PlannerIsLifecycleEvent(input.queries[query_index].reference_model.event_kind)) {
			if (!(worker.normal_active_power_w > 0) || !(worker.normal_normalized_edp > 0)) {
				score.energy_valid = false;
			} else {
				score.normalized_edp += worker.normal_normalized_edp;
			}
		}
	}
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto &allocation = allocations[query_index];
		const auto &target = accepted[query_index];
		if (allocation.mandatory_workers != target.mandatory_workers ||
		    allocation.optional_workers != target.optional_workers ||
		    std::find(seen[query_index].begin(), seen[query_index].end(), false) != seen[query_index].end()) {
			error = PlannerValidationDetail(query_index, input, "placed worker set differs from the accepted allocation");
			return false;
		}
		std::sort(allocation.workers.begin(), allocation.workers.end(),
		          [](const QuerySLAEnergyWorkerAssignment &left, const QuerySLAEnergyWorkerAssignment &right) {
			          return left.worker_ordinal < right.worker_ordinal;
		          });
		if (all_workers_at_preferred_pair[query_index]) {
			allocation.aggregate_service_rate = target.aggregate_service_rate;
			allocation.aggregate_active_power_w = target.aggregate_active_power_w;
		} else {
			allocation.aggregate_service_rate = static_cast<double>(reconstructed_service[query_index]);
			allocation.aggregate_active_power_w = static_cast<double>(reconstructed_power[query_index]);
		}
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input,
		                                             allocation.aggregate_service_rate, allocation.workers.size());
		if (!evaluation.valid) {
			error = PlannerValidationDetail(query_index, input, "shared-domain service cannot be evaluated");
			return false;
		}
		allocation.predicted_sla_cost = evaluation.mandatory_cost;
		allocation.predicted_optional_risk = evaluation.optional_risk;
		allocation.predicted_epoch_energy_j =
		    QuerySLAPredictedEpochEnergy(input.queries[query_index].sla_input, allocation.aggregate_service_rate,
		                                allocation.aggregate_active_power_w);
		auto worker_count = allocation.workers.size();
		auto reference_cost = target.predicted_sla_cost;
		auto reference_risk = target.predicted_optional_risk;
		if (worker_count < input.queries[query_index].reference_model.mandatory_cost.size() &&
		    worker_count < input.queries[query_index].reference_model.optional_risk.size()) {
			reference_cost = input.queries[query_index].reference_model.mandatory_cost[worker_count];
			reference_risk = input.queries[query_index].reference_model.optional_risk[worker_count];
		} else if (worker_count > 0) {
			std::ostringstream message;
			message << "reference model does not cover fixed worker count " << worker_count
			        << " with demand cap " << input.queries[query_index].reference_model.demand_cap;
			error = PlannerValidationDetail(query_index, input, message.str());
			return false;
		}
		auto cost_tolerance = 1e-9 * MaxValue<double>(1.0, std::abs(reference_cost));
		auto risk_tolerance = 1e-9 * MaxValue<double>(1.0, std::abs(reference_risk));
		score.sla_cost_violation +=
		    MaxValue<double>(0, allocation.predicted_sla_cost - reference_cost - cost_tolerance);
		score.fragility_violation +=
		    MaxValue<double>(0, allocation.predicted_optional_risk - reference_risk - risk_tolerance);
	}
	plan.queries = std::move(allocations);
	score.valid = true;
	return true;
}

static int CompareReconciliationMetric(double left, double right) {
	auto tolerance = 1e-12 * MaxValue<double>(1.0, MaxValue<double>(std::abs(left), std::abs(right)));
	if (left < right - tolerance) {
		return -1;
	}
	if (left > right + tolerance) {
		return 1;
	}
	return 0;
}

static bool ReconciliationScorePreferred(const HardwareReconciliationScore &candidate,
	                                     const HardwareReconciliationScore &current) {
	if (!candidate.valid) {
		return false;
	}
	if (!current.valid) {
		return true;
	}
	auto comparison = CompareReconciliationMetric(candidate.sla_cost_violation, current.sla_cost_violation);
	if (comparison != 0) {
		return comparison < 0;
	}
	comparison = CompareReconciliationMetric(candidate.fragility_violation, current.fragility_violation);
	if (comparison != 0) {
		return comparison < 0;
	}
	if (candidate.energy_valid != current.energy_valid) {
		return candidate.energy_valid;
	}
	if (!candidate.energy_valid) {
		return false;
	}
	comparison = CompareReconciliationMetric(candidate.normalized_edp, current.normalized_edp);
	if (comparison != 0) {
		return comparison < 0;
	}
	return candidate.frequency_sum_khz < current.frequency_sum_khz;
}

static void SetReferenceDomainHardware(vector<QuerySLAEnergyPlacedWorker> &workers,
	                                   const QuerySLAEnergyPlannerInput &input,
	                                   const vector<QueryHardwareWorkerTopology> &topology) {
	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	std::map<CoreKey, uint32_t> core_targets;
	std::map<int, uint32_t> socket_targets;
	for (const auto &entry : topology) {
		worker_topology[entry.worker_id] = entry;
	}
	for (const auto &worker : workers) {
		auto topology_entry = worker_topology.find(worker.worker_id);
		if (topology_entry == worker_topology.end() || worker.query_index >= input.queries.size()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		const auto &reference = input.queries[worker.query_index].reference_hardware;
		core_targets[core] = MaxValue<uint32_t>(core_targets[core], reference.core_frequency_khz);
		socket_targets[core.socket_id] =
		    MaxValue<uint32_t>(socket_targets[core.socket_id], reference.uncore_frequency_khz);
	}
	for (auto &worker : workers) {
		auto topology_entry = worker_topology.find(worker.worker_id);
		if (topology_entry == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		worker.normal_hardware = PlannerHardware(core_targets[core], socket_targets[core.socket_id]);
		worker.execution_hardware = worker.normal_hardware;
	}
}

static uint64_t ReconciliationFrequencySum(
    const vector<QuerySLAEnergyPlacedWorker> &workers,
    const std::unordered_map<idx_t, QueryHardwareWorkerTopology> &worker_topology) {
	std::map<CoreKey, uint32_t> cores;
	std::map<int, uint32_t> sockets;
	for (const auto &worker : workers) {
		auto topology = worker_topology.find(worker.worker_id);
		if (topology == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology->second.socket_id, topology->second.physical_core_id);
		cores[core] = worker.normal_hardware.core_frequency_khz;
		sockets[core.socket_id] = worker.normal_hardware.uncore_frequency_khz;
	}
	uint64_t result = 0;
	for (const auto &entry : cores) {
		result += entry.second;
	}
	for (const auto &entry : sockets) {
		result += entry.second;
	}
	return result;
}

static bool ReconcileHardwareDomains(QuerySLAEnergyEpochPlan &plan, const QuerySLAEnergyPlannerInput &input,
	                                 string &error) {
	auto accepted = plan.queries;
	auto preferred_domain_plan = plan;
	ResolveActualHardware(preferred_domain_plan.workers, input.topology);
	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	std::map<CoreKey, vector<idx_t>> core_workers;
	std::map<int, vector<idx_t>> socket_workers;
	std::map<CoreKey, std::set<uint32_t>> core_candidates;
	std::map<int, std::set<uint32_t>> socket_candidates;
	for (const auto &entry : input.topology) {
		worker_topology[entry.worker_id] = entry;
	}
	for (idx_t worker_index = 0; worker_index < plan.workers.size(); worker_index++) {
		const auto &worker = plan.workers[worker_index];
		auto topology = worker_topology.find(worker.worker_id);
		if (topology == worker_topology.end() || worker.query_index >= input.queries.size()) {
			error = "placed worker is missing pinned topology or query state";
			return false;
		}
		CoreKey core(topology->second.socket_id, topology->second.physical_core_id);
		core_workers[core].push_back(worker_index);
		socket_workers[core.socket_id].push_back(worker_index);
		core_candidates[core].insert(worker.preferred_hardware.core_frequency_khz);
		core_candidates[core].insert(input.queries[worker.query_index].reference_hardware.core_frequency_khz);
		socket_candidates[core.socket_id].insert(worker.preferred_hardware.uncore_frequency_khz);
		socket_candidates[core.socket_id].insert(input.queries[worker.query_index].reference_hardware.uncore_frequency_khz);
	}
	SetReferenceDomainHardware(plan.workers, input, input.topology);
	HardwareReconciliationScore current_score;
	current_score.energy_valid = true;
	if (!RebuildQueryAllocationsFromNormalHardware(plan, input, accepted, current_score, error)) {
		return false;
	}
	current_score.frequency_sum_khz = ReconciliationFrequencySum(plan.workers, worker_topology);
	HardwareReconciliationScore preferred_domain_score;
	preferred_domain_score.energy_valid = true;
	string preferred_domain_error;
	if (RebuildQueryAllocationsFromNormalHardware(preferred_domain_plan, input, accepted, preferred_domain_score,
	                                              preferred_domain_error)) {
		preferred_domain_score.frequency_sum_khz =
		    ReconciliationFrequencySum(preferred_domain_plan.workers, worker_topology);
		if (ReconciliationScorePreferred(preferred_domain_score, current_score)) {
			plan = std::move(preferred_domain_plan);
			current_score = preferred_domain_score;
		}
	}

	idx_t candidate_count = 0;
	for (const auto &entry : core_candidates) {
		candidate_count += entry.second.size();
	}
	for (const auto &entry : socket_candidates) {
		candidate_count += entry.second.size();
	}
	auto max_iterations = MaxValue<idx_t>(1, 4 * candidate_count);
	for (idx_t iteration = 0; iteration < max_iterations; iteration++) {
		QuerySLAEnergyEpochPlan best_plan;
		HardwareReconciliationScore best_score;
		bool found = false;
		for (const auto &entry : core_candidates) {
			for (auto frequency_khz : entry.second) {
				auto candidate = plan;
				bool changed = false;
				for (auto worker_index : core_workers[entry.first]) {
					auto &worker = candidate.workers[worker_index];
					changed = changed || worker.normal_hardware.core_frequency_khz != frequency_khz;
					worker.normal_hardware.core_frequency_khz = frequency_khz;
					worker.execution_hardware = worker.normal_hardware;
				}
				if (!changed) {
					continue;
				}
				HardwareReconciliationScore candidate_score;
				candidate_score.energy_valid = true;
				string candidate_error;
				if (!RebuildQueryAllocationsFromNormalHardware(candidate, input, accepted, candidate_score,
				                                               candidate_error)) {
					continue;
				}
				candidate_score.frequency_sum_khz =
				    ReconciliationFrequencySum(candidate.workers, worker_topology);
				if (ReconciliationScorePreferred(candidate_score, best_score)) {
					best_plan = std::move(candidate);
					best_score = candidate_score;
					found = true;
				}
			}
		}
		for (const auto &entry : socket_candidates) {
			for (auto frequency_khz : entry.second) {
				auto candidate = plan;
				bool changed = false;
				for (auto worker_index : socket_workers[entry.first]) {
					auto &worker = candidate.workers[worker_index];
					changed = changed || worker.normal_hardware.uncore_frequency_khz != frequency_khz;
					worker.normal_hardware.uncore_frequency_khz = frequency_khz;
					worker.execution_hardware = worker.normal_hardware;
				}
				if (!changed) {
					continue;
				}
				HardwareReconciliationScore candidate_score;
				candidate_score.energy_valid = true;
				string candidate_error;
				if (!RebuildQueryAllocationsFromNormalHardware(candidate, input, accepted, candidate_score,
				                                               candidate_error)) {
					continue;
				}
				candidate_score.frequency_sum_khz =
				    ReconciliationFrequencySum(candidate.workers, worker_topology);
				if (ReconciliationScorePreferred(candidate_score, best_score)) {
					best_plan = std::move(candidate);
					best_score = candidate_score;
					found = true;
				}
			}
		}
		if (!found || !ReconciliationScorePreferred(best_score, current_score)) {
			break;
		}
		plan = std::move(best_plan);
		current_score = best_score;
	}
	if (CompareReconciliationMetric(current_score.sla_cost_violation, 0) > 0 ||
	    CompareReconciliationMetric(current_score.fragility_violation, 0) > 0) {
		std::ostringstream message;
		message << std::setprecision(17)
		        << "shared-domain reconciliation cannot preserve the accepted SLA plan: cost_violation="
		        << current_score.sla_cost_violation
		        << " fragility_violation=" << current_score.fragility_violation;
		error = message.str();
		return false;
	}
	return true;
}

static vector<QuerySLAEnergyPlacedWorker>
BuildMandatoryCandidate(const vector<MandatoryToken> &tokens, const vector<QueryHardwareWorkerTopology> &slots,
	                    const QuerySLAEnergyPlannerInput &input) {
	vector<QuerySLAEnergyPlacedWorker> result;
	if (tokens.size() > slots.size()) {
		return result;
	}
	result.reserve(tokens.size());
	for (idx_t i = 0; i < tokens.size(); i++) {
		QuerySLAEnergyPlacedWorker worker;
		worker.worker_id = slots[i].worker_id;
		worker.query_index = tokens[i].query_index;
		worker.preferred_hardware = tokens[i].assignment.hardware;
		worker.preferred_safe_throughput = tokens[i].assignment.safe_throughput;
		worker.preferred_active_power_w = tokens[i].assignment.active_power_w;
		worker.preferred_normalized_edp = tokens[i].assignment.normalized_edp;
		worker.profile_version = tokens[i].assignment.profile_version;
		worker.worker_ordinal = tokens[i].assignment.worker_ordinal;
		worker.mandatory = true;
		result.push_back(worker);
	}
	ResolveActualHardware(result, input.topology);
	return result;
}

static bool OptionalTokenPreferred(const OptionalToken &candidate, const OptionalToken &current) {
	if (!current.valid || candidate.gain != current.gain) {
		return !current.valid || candidate.gain > current.gain;
	}
	if (candidate.query_index != current.query_index) {
		return candidate.query_index < current.query_index;
	}
	if (candidate.socket_id != current.socket_id) {
		return candidate.socket_id < current.socket_id;
	}
	if (candidate.hardware.uncore_frequency_khz != current.hardware.uncore_frequency_khz) {
		return candidate.hardware.uncore_frequency_khz < current.hardware.uncore_frequency_khz;
	}
	return candidate.hardware.core_frequency_khz < current.hardware.core_frequency_khz;
}

static OptionalToken BuildOptionalToken(const QuerySLAEnergyModelInput &query,
	                                    const QuerySLAEnergyQueryAllocation &allocation, idx_t query_index,
	                                    QuerySLAEnergyOptionalStage stage, int socket_id,
	                                    const QueryRequestHardwareConfiguration &reference_hardware,
	                                    const vector<QueryRequestHardwareConfiguration> &allowed_hardware) {
	OptionalToken result;
	auto worker_ordinal = allocation.workers.size();
	if (worker_ordinal >= query.reference_model.service_rates.size() ||
	    worker_ordinal >= query.reference_model.optional_risk.size()) {
		return result;
	}
		auto selected = SelectQuerySLAOptionalAddition(
		    query, allocation.aggregate_service_rate, allocation.predicted_optional_risk, worker_ordinal,
		    reference_hardware, allowed_hardware, stage);
	if (!selected.valid) {
		return result;
	}
	result.valid = true;
	result.query_index = query_index;
	result.worker_ordinal = selected.worker_ordinal;
	result.socket_id = socket_id;
	result.hardware = selected.hardware;
	result.throughput = selected.safe_throughput;
	result.active_power_w = selected.active_power_w;
	result.normalized_edp = selected.normalized_edp;
	result.fragility_before = selected.optional_fragility_before;
	result.fragility_target = selected.optional_fragility_target;
	result.fragility_after = selected.optional_fragility_after;
	result.gain = selected.optional_gain;
	result.profile_version = selected.profile_version;
	result.stage = stage;
	return result;
}

static void CommitOptionalToken(QuerySLAEnergyQueryAllocation &allocation,
	                            const QuerySLAEnergyModelInput &query, const OptionalToken &token) {
	QuerySLAEnergyWorkerAssignment worker;
	worker.valid = true;
	worker.hardware = token.hardware;
	worker.safe_throughput = token.throughput;
	worker.active_power_w = token.active_power_w;
	worker.normalized_edp = token.normalized_edp;
	worker.optional_fragility_before = token.fragility_before;
	worker.optional_fragility_target = token.fragility_target;
	worker.optional_fragility_after = token.fragility_after;
	worker.optional_gain = token.gain;
	worker.profile_version = token.profile_version;
	worker.worker_ordinal = token.worker_ordinal;
	worker.optional_stage = token.stage;
	worker.optional = true;
	allocation.workers.push_back(worker);
	allocation.aggregate_service_rate += token.throughput;
	allocation.aggregate_active_power_w += token.active_power_w;
	auto evaluation = QuerySLAEvaluateServiceRate(query.sla_input, allocation.aggregate_service_rate,
	                                             allocation.workers.size());
	if (evaluation.valid) {
		allocation.predicted_sla_cost = evaluation.mandatory_cost;
		allocation.predicted_optional_risk = evaluation.optional_risk;
	}
	allocation.optional_workers++;
}

static void PlaceOptionalToken(QuerySLAEnergyEpochPlan &plan, const OptionalToken &token, idx_t worker_id) {
	QuerySLAEnergyPlacedWorker worker;
	worker.worker_id = worker_id;
	worker.query_index = token.query_index;
	worker.preferred_hardware = token.hardware;
	worker.normal_hardware = token.hardware;
	worker.execution_hardware = token.hardware;
	worker.preferred_safe_throughput = token.throughput;
	worker.preferred_active_power_w = token.active_power_w;
	worker.preferred_normalized_edp = token.normalized_edp;
	worker.optional_fragility_before = token.fragility_before;
	worker.optional_fragility_target = token.fragility_target;
	worker.optional_fragility_after = token.fragility_after;
	worker.optional_gain = token.gain;
	worker.profile_version = token.profile_version;
	worker.worker_ordinal = token.worker_ordinal;
	worker.optional_stage = token.stage;
	worker.optional = true;
	plan.workers.push_back(worker);
	switch (token.stage) {
	case QuerySLAEnergyOptionalStage::O1:
		plan.diagnostics.o1_workers++;
		break;
	case QuerySLAEnergyOptionalStage::O2:
		plan.diagnostics.o2_workers++;
		break;
	case QuerySLAEnergyOptionalStage::O3:
		plan.diagnostics.o3_workers++;
		break;
	case QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK:
		plan.diagnostics.reference_fallback_workers++;
		break;
	default:
		break;
	}
}

static vector<QueryRequestHardwareConfiguration>
BuildHardwarePairs(const vector<uint32_t> &core_levels, const vector<uint32_t> &uncore_levels) {
	vector<QueryRequestHardwareConfiguration> result;
	result.reserve(core_levels.size() * uncore_levels.size());
	for (auto uncore_khz : uncore_levels) {
		for (auto core_khz : core_levels) {
			result.push_back(PlannerHardware(core_khz, uncore_khz));
		}
	}
	return result;
}

static void SortOptionalTokensForPacking(vector<OptionalToken> &tokens) {
	std::stable_sort(tokens.begin(), tokens.end(), [](const OptionalToken &left, const OptionalToken &right) {
		if (left.hardware.uncore_frequency_khz != right.hardware.uncore_frequency_khz) {
			return left.hardware.uncore_frequency_khz > right.hardware.uncore_frequency_khz;
		}
		if (left.hardware.core_frequency_khz != right.hardware.core_frequency_khz) {
			return left.hardware.core_frequency_khz > right.hardware.core_frequency_khz;
		}
		if (left.query_index != right.query_index) {
			return left.query_index < right.query_index;
		}
		return left.worker_ordinal < right.worker_ordinal;
	});
}

static void ApplyExplorationOverlay(QuerySLAEnergyEpochPlan &plan, const QuerySLAEnergyPlannerInput &input,
	                                const vector<QueryHardwareWorkerTopology> &topology) {
	if (!input.exploration_enabled || plan.workers.empty()) {
		return;
	}
	static constexpr uint64_t UNCORE_DOMAIN_SALT = 0x756e636f72655f64ULL;
	static constexpr uint64_t UNCORE_FREQUENCY_SALT = 0x756e636f72655f66ULL;
	static constexpr uint64_t CORE_DOMAIN_SALT = 0x636f72655f646f6dULL;
	static constexpr uint64_t CORE_FREQUENCY_SALT = 0x636f72655f667265ULL;
	static constexpr idx_t FORCED_UNCORE_AFTER_EPOCHS = 4;

	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	std::map<CoreKey, vector<idx_t>> core_workers;
	std::map<int, vector<idx_t>> socket_workers;
	for (const auto &worker : topology) {
		worker_topology[worker.worker_id] = worker;
	}
	for (idx_t i = 0; i < plan.workers.size(); i++) {
		auto topology_entry = worker_topology.find(plan.workers[i].worker_id);
		if (topology_entry == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		core_workers[core].push_back(i);
		socket_workers[core.socket_id].push_back(i);
	}
	struct CoreProbe {
		CoreKey core;
		vector<uint32_t> frequencies_khz;
	};
	struct SocketProbe {
		int socket_id = -1;
		vector<uint32_t> frequencies_khz;
	};
	auto worker_needs_pair = [&](const QuerySLAEnergyPlacedWorker &worker,
	                             const QueryRequestHardwareConfiguration &hardware) {
		return worker.query_index < input.queries.size() &&
		       ExactPairSampleCount(input.queries[worker.query_index], hardware) <
		           PIPELINE_HARDWARE_PROFILE_MIN_STABLE_SAMPLES;
	};
	auto optional_only = [&](const vector<idx_t> &worker_indexes) {
		if (worker_indexes.empty()) {
			return false;
		}
		for (auto worker_index : worker_indexes) {
			const auto &worker = plan.workers[worker_index];
			if (!worker.optional || worker.mandatory) {
				return false;
			}
		}
		return true;
	};
	auto incomplete_uncore_frequencies = [&](const vector<idx_t> &worker_indexes) {
		vector<uint32_t> result;
		if (worker_indexes.empty()) {
			return result;
		}
		auto normal_khz = plan.workers[worker_indexes.front()].normal_hardware.uncore_frequency_khz;
		for (auto uncore_khz : input.uncore_frequency_levels_khz) {
			if (uncore_khz == normal_khz) {
				continue;
			}
			bool needs_sample = false;
			for (auto worker_index : worker_indexes) {
				auto hardware = plan.workers[worker_index].normal_hardware;
				hardware.uncore_frequency_khz = uncore_khz;
				needs_sample = needs_sample || worker_needs_pair(plan.workers[worker_index], hardware);
			}
			if (needs_sample) {
				result.push_back(uncore_khz);
			}
		}
		return result;
	};
	auto incomplete_core_frequencies = [&](const vector<idx_t> &worker_indexes) {
		vector<uint32_t> result;
		if (worker_indexes.empty()) {
			return result;
		}
		auto normal_khz = plan.workers[worker_indexes.front()].normal_hardware.core_frequency_khz;
		for (auto core_khz : input.core_frequency_levels_khz) {
			if (core_khz == normal_khz) {
				continue;
			}
			bool needs_sample = false;
			for (auto worker_index : worker_indexes) {
				auto hardware = plan.workers[worker_index].normal_hardware;
				hardware.core_frequency_khz = core_khz;
				needs_sample = needs_sample || worker_needs_pair(plan.workers[worker_index], hardware);
			}
			if (needs_sample) {
				result.push_back(core_khz);
			}
		}
		return result;
	};

	vector<SocketProbe> optional_socket_probes;
	for (const auto &entry : socket_workers) {
		if (!optional_only(entry.second)) {
			continue;
		}
		plan.diagnostics.optional_only_socket_available = true;
		auto frequencies = incomplete_uncore_frequencies(entry.second);
		if (!frequencies.empty()) {
			SocketProbe probe;
			probe.socket_id = entry.first;
			probe.frequencies_khz = std::move(frequencies);
			optional_socket_probes.push_back(std::move(probe));
		}
	}
	vector<SocketProbe> socket_probes = std::move(optional_socket_probes);
	bool forced_uncore = false;
	if (!plan.diagnostics.optional_only_socket_available &&
	    input.epochs_without_optional_only_socket + 1 >= FORCED_UNCORE_AFTER_EPOCHS) {
		forced_uncore = true;
		for (const auto &entry : socket_workers) {
			auto frequencies = incomplete_uncore_frequencies(entry.second);
			if (frequencies.empty()) {
				continue;
			}
			SocketProbe probe;
			probe.socket_id = entry.first;
			probe.frequencies_khz = std::move(frequencies);
			socket_probes.push_back(std::move(probe));
		}
	}
	int probed_socket = -1;
	if (!socket_probes.empty()) {
		auto probe_index = ExplorationIndex(input.exploration_seed, input.epoch_generation,
		                                    UNCORE_DOMAIN_SALT, socket_probes.size());
		const auto &probe = socket_probes[probe_index];
		auto frequency_index = ExplorationIndex(input.exploration_seed, input.epoch_generation,
		                                        UNCORE_FREQUENCY_SALT, probe.frequencies_khz.size());
		auto frequency_khz = probe.frequencies_khz[frequency_index];
		probed_socket = probe.socket_id;
		for (auto worker_index : socket_workers[probe.socket_id]) {
			auto &worker = plan.workers[worker_index];
			worker.execution_hardware.uncore_frequency_khz = frequency_khz;
			worker.exploration = true;
		}
		plan.diagnostics.uncore_probes = 1;
		plan.diagnostics.forced_uncore_probes = forced_uncore ? 1 : 0;
	}

	vector<CoreProbe> core_probes;
	for (const auto &entry : core_workers) {
		if (entry.first.socket_id == probed_socket || !optional_only(entry.second)) {
			continue;
		}
		auto frequencies = incomplete_core_frequencies(entry.second);
		if (!frequencies.empty()) {
			CoreProbe probe;
			probe.core = entry.first;
			probe.frequencies_khz = std::move(frequencies);
			core_probes.push_back(std::move(probe));
		}
	}
	if (!core_probes.empty()) {
		auto probe_index = ExplorationIndex(input.exploration_seed, input.epoch_generation,
		                                    CORE_DOMAIN_SALT, core_probes.size());
		const auto &probe = core_probes[probe_index];
		auto frequency_index = ExplorationIndex(input.exploration_seed, input.epoch_generation,
		                                        CORE_FREQUENCY_SALT, probe.frequencies_khz.size());
		auto frequency_khz = probe.frequencies_khz[frequency_index];
		for (auto worker_index : core_workers[probe.core]) {
			auto &worker = plan.workers[worker_index];
			worker.execution_hardware.core_frequency_khz = frequency_khz;
			worker.exploration = true;
		}
		plan.diagnostics.core_probes = 1;
	}
}

} // namespace

QuerySLAEnergyEpochPlan PlanQuerySLAEnergyEpoch(const QuerySLAEnergyPlannerInput &input_p) {
	auto input = input_p;
	QuerySLAEnergyEpochPlan result;
	if (input.queries.empty()) {
		result.valid = true;
		return result;
	}
	if (input.worker_budget == 0 || input.topology.empty()) {
		result.error = "SLA-energy planner requires a nonempty pinned worker topology";
		return result;
	}
	if (input.logical_allocation.mandatory.size() != input.queries.size() ||
	    input.logical_allocation.optional.size() != input.queries.size()) {
		result.error = "SLA-energy planner requires one fixed logical allocation per query";
		return result;
	}
	idx_t logical_worker_count = 0;
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto &query = input.queries[query_index];
		if (!query.reference_model.valid) {
			result.error = "SLA-energy planner requires a valid maximum-frequency reference model";
			return result;
		}
		auto target = input.logical_allocation.mandatory[query_index] +
		              input.logical_allocation.optional[query_index];
		if (target > query.reference_model.demand_cap) {
			result.error = "SLA-energy logical allocation exceeds query demand";
			return result;
		}
		logical_worker_count += target;
	}
	for (const auto &query : input.queries) {
		for (const auto &profile : query.hardware_profiles) {
			if (profile.valid && !profile.mature && !(profile.hardware == query.reference_hardware)) {
				result.diagnostics.immature_pair_rejections++;
			}
		}
	}
	auto budget = MinValue<idx_t>(input.worker_budget, input.topology.size());
	if (logical_worker_count > budget) {
		result.error = "SLA-energy logical allocation exceeds the pinned worker budget";
		return result;
	}
	auto mandatory = AllocateQuerySLAMandatoryWorkers(input.queries, input.logical_allocation.mandatory);
	if (!mandatory.valid) {
		result.error = mandatory.error.empty() ? "SLA-energy mandatory allocation failed" : mandatory.error;
		return result;
	}
	result.queries = std::move(mandatory.queries);
	vector<MandatoryToken> tokens;
	for (idx_t query_index = 0; query_index < result.queries.size(); query_index++) {
		for (const auto &worker : result.queries[query_index].workers) {
			if (worker.mandatory) {
				tokens.push_back(MandatoryToken(query_index, worker));
			}
		}
	}
	std::sort(tokens.begin(), tokens.end(), [](const MandatoryToken &left, const MandatoryToken &right) {
		if (left.assignment.hardware.uncore_frequency_khz != right.assignment.hardware.uncore_frequency_khz) {
			return left.assignment.hardware.uncore_frequency_khz > right.assignment.hardware.uncore_frequency_khz;
		}
		if (left.assignment.hardware.core_frequency_khz != right.assignment.hardware.core_frequency_khz) {
			return left.assignment.hardware.core_frequency_khz > right.assignment.hardware.core_frequency_khz;
		}
		return left.query_index < right.query_index;
	});
	auto cores = BuildCoreSlots(input.topology);
	auto dense_slots = FlattenSlots(cores);
	result.workers = BuildMandatoryCandidate(tokens, dense_slots, input);
	if (!tokens.empty() && result.workers.empty()) {
		result.error = "SLA-energy planner could not place mandatory workers";
		return result;
	}
	result.diagnostics.worker_frequency_adjustments = CountWorkerFrequencyAdjustments(result.workers);
	result.diagnostics.mandatory_workers = result.workers.size();
	string validation_error;
	if (!RebuildQueryAllocationsFromPreferences(result, input, validation_error)) {
		result.error = "SLA-energy planner could not validate mandatory preferences: " + validation_error;
		return result;
	}

	std::set<idx_t> used_worker_ids;
	std::set<CoreKey> active_cores;
	std::set<int> active_sockets;
	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	for (const auto &worker : input.topology) {
		worker_topology[worker.worker_id] = worker;
	}
	for (const auto &worker : result.workers) {
		used_worker_ids.insert(worker.worker_id);
		auto topology = worker_topology[worker.worker_id];
		active_cores.insert(CoreKey(topology.socket_id, topology.physical_core_id));
		active_sockets.insert(topology.socket_id);
	}

	// O1: unused SMT slots on active physical cores use the installed core/socket pair.
	while (used_worker_ids.size() < budget) {
		OptionalToken best;
		idx_t best_worker = DConstants::INVALID_INDEX;
		for (const auto &core : cores) {
			if (active_cores.find(core.key) == active_cores.end()) {
				continue;
			}
			for (const auto &slot : core.workers) {
				if (used_worker_ids.find(slot.worker_id) != used_worker_ids.end()) {
					continue;
				}
				QueryRequestHardwareConfiguration hardware;
				for (const auto &placed : result.workers) {
					auto placed_topology = worker_topology[placed.worker_id];
					if (placed_topology.socket_id == core.key.socket_id &&
					    placed_topology.physical_core_id == core.key.core_id) {
						hardware.core_frequency_khz = placed.normal_hardware.core_frequency_khz;
						hardware.uncore_frequency_khz = placed.normal_hardware.uncore_frequency_khz;
						break;
					}
				}
				if (!hardware.IsValid()) {
					continue;
				}
				vector<QueryRequestHardwareConfiguration> allowed {hardware};
				for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
					if (result.queries[query_index].optional_workers >=
					    input.logical_allocation.optional[query_index]) {
						continue;
					}
					auto token = BuildOptionalToken(
					    input.queries[query_index], result.queries[query_index], query_index,
					    QuerySLAEnergyOptionalStage::O1, core.key.socket_id, hardware, allowed);
					if (token.valid &&
					    (OptionalTokenPreferred(token, best) ||
					     (best.valid && token.gain == best.gain && token.query_index == best.query_index &&
					      slot.worker_id < best_worker))) {
						best = token;
						best_worker = slot.worker_id;
					}
				}
			}
		}
		if (!best.valid) {
			break;
		}
		CommitOptionalToken(result.queries[best.query_index], input.queries[best.query_index], best);
		PlaceOptionalToken(result, best, best_worker);
		used_worker_ids.insert(best_worker);
	}

	// O2: select fixed optional tokens for empty cores on active sockets, then pair similar core preferences.
	std::map<int, vector<const CoreSlots *>> o2_cores;
	std::map<int, uint32_t> active_socket_uncore;
	for (const auto &worker : result.workers) {
		auto topology = worker_topology[worker.worker_id];
		active_socket_uncore[topology.socket_id] =
		    MaxValue<uint32_t>(active_socket_uncore[topology.socket_id], worker.normal_hardware.uncore_frequency_khz);
	}
	for (const auto &core : cores) {
		if (active_sockets.find(core.key.socket_id) != active_sockets.end() &&
		    active_cores.find(core.key) == active_cores.end()) {
			o2_cores[core.key.socket_id].push_back(&core);
		}
	}
	std::map<int, vector<OptionalToken>> o2_tokens;
	idx_t selected_o2_workers = 0;
	while (used_worker_ids.size() + selected_o2_workers < budget) {
		OptionalToken best;
		for (const auto &socket_entry : o2_cores) {
			idx_t capacity = 0;
			for (const auto core : socket_entry.second) {
				capacity += core->workers.size();
			}
			if (o2_tokens[socket_entry.first].size() >= capacity) {
				continue;
			}
			auto uncore_khz = active_socket_uncore[socket_entry.first];
			if (uncore_khz == 0 || input.core_frequency_levels_khz.empty()) {
				continue;
			}
			vector<QueryRequestHardwareConfiguration> allowed;
			for (auto core_khz : input.core_frequency_levels_khz) {
				allowed.push_back(PlannerHardware(core_khz, uncore_khz));
			}
			auto reference = PlannerHardware(input.core_frequency_levels_khz.back(), uncore_khz);
			for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
				if (result.queries[query_index].optional_workers >=
				    input.logical_allocation.optional[query_index]) {
					continue;
				}
				auto token = BuildOptionalToken(
				    input.queries[query_index], result.queries[query_index], query_index,
				    QuerySLAEnergyOptionalStage::O2, socket_entry.first, reference, allowed);
				if (token.valid && OptionalTokenPreferred(token, best)) {
					best = token;
				}
			}
		}
		if (!best.valid) {
			break;
		}
		CommitOptionalToken(result.queries[best.query_index], input.queries[best.query_index], best);
		o2_tokens[best.socket_id].push_back(best);
		selected_o2_workers++;
	}
	for (auto &socket_entry : o2_tokens) {
		auto &tokens_for_socket = socket_entry.second;
		SortOptionalTokensForPacking(tokens_for_socket);
		vector<QueryHardwareWorkerTopology> slots;
		for (const auto core : o2_cores[socket_entry.first]) {
			slots.insert(slots.end(), core->workers.begin(), core->workers.end());
		}
		for (idx_t i = 0; i < tokens_for_socket.size(); i++) {
			PlaceOptionalToken(result, tokens_for_socket[i], slots[i].worker_id);
			used_worker_ids.insert(slots[i].worker_id);
			active_cores.insert(CoreKey(slots[i].socket_id, slots[i].physical_core_id));
		}
	}
	ResolveActualHardware(result.workers, input.topology);

	// O3: activate one inactive socket at a time and densely pack independently selected optional tokens.
	std::set<int> topology_sockets;
	for (const auto &core : cores) {
		topology_sockets.insert(core.key.socket_id);
	}
	auto all_hardware = BuildHardwarePairs(input.core_frequency_levels_khz, input.uncore_frequency_levels_khz);
	while (used_worker_ids.size() < budget && !all_hardware.empty()) {
		OptionalToken first;
		int selected_socket = -1;
		for (auto socket_id : topology_sockets) {
			if (active_sockets.find(socket_id) != active_sockets.end()) {
				continue;
			}
			for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
				if (result.queries[query_index].optional_workers >=
				    input.logical_allocation.optional[query_index]) {
					continue;
				}
				auto token = BuildOptionalToken(
				    input.queries[query_index], result.queries[query_index], query_index,
				    QuerySLAEnergyOptionalStage::O3, socket_id, input.queries[query_index].reference_hardware,
				    all_hardware);
				if (token.valid && OptionalTokenPreferred(token, first)) {
					first = token;
					selected_socket = socket_id;
				}
			}
		}
		if (!first.valid || selected_socket < 0) {
			break;
		}
		vector<QueryHardwareWorkerTopology> socket_slots;
		for (const auto &core : cores) {
			if (core.key.socket_id == selected_socket) {
				socket_slots.insert(socket_slots.end(), core.workers.begin(), core.workers.end());
			}
		}
		auto capacity = MinValue<idx_t>(socket_slots.size(), budget - used_worker_ids.size());
		vector<OptionalToken> socket_tokens;
		while (socket_tokens.size() < capacity) {
			OptionalToken best;
			for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
				if (result.queries[query_index].optional_workers >=
				    input.logical_allocation.optional[query_index]) {
					continue;
				}
				auto token = BuildOptionalToken(
				    input.queries[query_index], result.queries[query_index], query_index,
				    QuerySLAEnergyOptionalStage::O3, selected_socket,
				    input.queries[query_index].reference_hardware, all_hardware);
				if (token.valid && OptionalTokenPreferred(token, best)) {
					best = token;
				}
			}
			if (!best.valid) {
				break;
			}
			CommitOptionalToken(result.queries[best.query_index], input.queries[best.query_index], best);
			socket_tokens.push_back(best);
		}
		if (socket_tokens.empty()) {
			break;
		}
		SortOptionalTokensForPacking(socket_tokens);
		for (idx_t i = 0; i < socket_tokens.size(); i++) {
			PlaceOptionalToken(result, socket_tokens[i], socket_slots[i].worker_id);
			used_worker_ids.insert(socket_slots[i].worker_id);
			active_cores.insert(CoreKey(socket_slots[i].socket_id, socket_slots[i].physical_core_id));
		}
		active_sockets.insert(selected_socket);
	}

	// A fixed SLA token may not have an EDP-qualified O1-O3 placement. It must still run.
	for (const auto &logical_token : input.logical_allocation.tokens) {
		if (!logical_token.optional || logical_token.query_index >= input.queries.size() ||
		    result.queries[logical_token.query_index].optional_workers >=
		        input.logical_allocation.optional[logical_token.query_index]) {
			continue;
		}
		auto slot = std::find_if(dense_slots.begin(), dense_slots.end(), [&](const QueryHardwareWorkerTopology &worker) {
			return used_worker_ids.find(worker.worker_id) == used_worker_ids.end();
		});
		if (slot == dense_slots.end()) {
			result.error = "SLA-energy planner could not place a fixed optional token";
			return result;
		}
		auto reference_hardware = input.queries[logical_token.query_index].reference_hardware;
		auto token = BuildOptionalToken(
		    input.queries[logical_token.query_index], result.queries[logical_token.query_index],
		    logical_token.query_index, QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK, slot->socket_id,
		    reference_hardware, {reference_hardware});
		if (!token.valid) {
			const auto &query = input.queries[logical_token.query_index];
			const auto &allocation = result.queries[logical_token.query_index];
			std::ostringstream message;
			message << std::setprecision(17)
			        << "SLA-energy planner could not model a fixed optional reference fallback:"
			        << " query_index=" << logical_token.query_index
			        << " db_query_id=" << query.sla_input.metadata.db_query_id
			        << " pipeline_id=" << query.sla_input.work.pipeline_id
			        << " event_kind=" << static_cast<uint32_t>(query.reference_model.event_kind)
			        << " logical_worker_ordinal=" << logical_token.worker_ordinal
			        << " logical_gain=" << logical_token.gain
			        << " mandatory_target=" << input.logical_allocation.mandatory[logical_token.query_index]
			        << " optional_target=" << input.logical_allocation.optional[logical_token.query_index]
			        << " mandatory_placed=" << allocation.mandatory_workers
			        << " optional_placed=" << allocation.optional_workers
			        << " allocation_workers=" << allocation.workers.size()
			        << " demand_cap=" << query.reference_model.demand_cap
			        << " service_rate_count=" << query.reference_model.service_rates.size()
			        << " optional_risk_count=" << query.reference_model.optional_risk.size()
			        << " aggregate_service_rate=" << allocation.aggregate_service_rate
			        << " predicted_optional_risk=" << allocation.predicted_optional_risk
			        << " reference_core_khz=" << reference_hardware.core_frequency_khz
			        << " reference_uncore_khz=" << reference_hardware.uncore_frequency_khz;
			auto ordinal = allocation.workers.size();
			if (ordinal + 1 < query.reference_model.service_rates.size()) {
				message << " reference_increment="
				        << query.reference_model.service_rates[ordinal + 1] -
				               query.reference_model.service_rates[ordinal];
			}
			result.error = message.str();
			return result;
		}
		CommitOptionalToken(result.queries[token.query_index], input.queries[token.query_index], token);
		PlaceOptionalToken(result, token, slot->worker_id);
		used_worker_ids.insert(slot->worker_id);
	}
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		if (result.queries[query_index].mandatory_workers != input.logical_allocation.mandatory[query_index] ||
		    result.queries[query_index].optional_workers != input.logical_allocation.optional[query_index]) {
			result.error = "SLA-energy physical placement changed the fixed logical allocation";
			return result;
		}
	}

	validation_error.clear();
	if (!ReconcileHardwareDomains(result, input, validation_error)) {
		result.error = "SLA-energy planner could not reconcile shared hardware domains: " + validation_error;
		return result;
	}
	result.diagnostics.worker_frequency_adjustments = CountWorkerFrequencyAdjustments(result.workers);
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto &allocation = result.queries[query_index];
		auto simulation = allocation;
		auto service_rate = simulation.aggregate_service_rate;
		auto worker_count = simulation.workers.size();
		while (worker_count < input.queries[query_index].reference_model.demand_cap) {
			auto assignment = SelectQuerySLAMandatoryAddition(
			    input.queries[query_index], input.queries[query_index].reference_model, service_rate, worker_count);
			if (!assignment.valid || assignment.mandatory_gain <= 0) {
				break;
			}
			allocation.dynamic_mandatory_workers.push_back(assignment);
			service_rate += assignment.safe_throughput;
			simulation.workers.push_back(assignment);
			simulation.aggregate_service_rate = service_rate;
			auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input, service_rate,
			                                             simulation.workers.size());
			if (evaluation.valid) {
				simulation.predicted_sla_cost = evaluation.mandatory_cost;
				simulation.predicted_optional_risk = evaluation.optional_risk;
			}
			worker_count++;
		}
		while (simulation.workers.size() < input.queries[query_index].reference_model.demand_cap) {
			auto token = BuildOptionalToken(
			    input.queries[query_index], simulation, query_index, QuerySLAEnergyOptionalStage::DYNAMIC, -1,
			    input.queries[query_index].reference_hardware, all_hardware);
			if (!token.valid || token.gain <= 0) {
				break;
			}
			QuerySLAEnergyWorkerAssignment assignment;
			assignment.valid = true;
			assignment.hardware = token.hardware;
			assignment.safe_throughput = token.throughput;
			assignment.active_power_w = token.active_power_w;
			assignment.normalized_edp = token.normalized_edp;
			assignment.optional_fragility_before = token.fragility_before;
			assignment.optional_fragility_target = token.fragility_target;
			assignment.optional_fragility_after = token.fragility_after;
			assignment.optional_gain = token.gain;
			assignment.profile_version = token.profile_version;
			assignment.worker_ordinal = token.worker_ordinal;
			assignment.optional_stage = token.stage;
			assignment.optional = true;
			allocation.dynamic_optional_workers.push_back(assignment);
			CommitOptionalToken(simulation, input.queries[query_index], token);
		}
	}
	ApplyExplorationOverlay(result, input, input.topology);
	result.diagnostics.parked_workers = budget - used_worker_ids.size();
	result.valid = true;
	return result;
}

const char *QuerySLAEnergyOptionalStageToString(QuerySLAEnergyOptionalStage stage) {
	switch (stage) {
	case QuerySLAEnergyOptionalStage::NONE:
		return "none";
	case QuerySLAEnergyOptionalStage::O1:
		return "o1";
	case QuerySLAEnergyOptionalStage::O2:
		return "o2";
	case QuerySLAEnergyOptionalStage::O3:
		return "o3";
	case QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK:
		return "reference_fallback";
	case QuerySLAEnergyOptionalStage::DYNAMIC:
		return "dynamic";
	default:
		return "unknown";
	}
}

} // namespace duckdb
