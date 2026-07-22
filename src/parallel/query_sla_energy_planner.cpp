//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_energy_planner.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_energy_planner.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>

namespace duckdb {

namespace {

static constexpr idx_t O3_BEAM_WIDTH = 4;
static constexpr idx_t O3_MAX_DEPTH = 8;
static constexpr idx_t OPTIONAL_QUERY_SHORTLIST = 8;

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
	bool fallback = false;
	QueryRequestHardwareConfiguration hardware;
	double throughput = 0;
	double active_power_w = 0;
	idx_t sample_count = 0;
};

struct MandatoryToken {
	MandatoryToken(idx_t query_index_p, const QueryRequestHardwareConfiguration &preferred_p)
	    : query_index(query_index_p), preferred(preferred_p) {
	}

	idx_t query_index = 0;
	QueryRequestHardwareConfiguration preferred;
};

struct CoreSlots {
	CoreKey key;
	vector<QueryHardwareWorkerTopology> workers;
};

struct OptionalAction {
	bool valid = false;
	double utility = 0;
	double risk_gain = 0;
	double energy_j = 0;
	CoreKey core;
	int socket_id = -1;
	uint32_t core_frequency_khz = 0;
	uint32_t uncore_frequency_khz = 0;
	idx_t query_a = DConstants::INVALID_INDEX;
	idx_t query_b = DConstants::INVALID_INDEX;
	double throughput_a = 0;
	double throughput_b = 0;
};

struct BeamState {
	vector<double> service_rates;
	vector<double> optional_risks;
	vector<idx_t> worker_counts;
	vector<OptionalAction> actions;
	double value = 0;
};

static bool PlannerSameHardware(const QueryRequestHardwareConfiguration &left,
	                            const QueryRequestHardwareConfiguration &right) {
	return left == right;
}

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
		if (!PlannerSameHardware(profile.hardware, hardware)) {
			continue;
		}
		result.sample_count = MinValue<idx_t>(profile.throughput_sample_count, profile.power_sample_count);
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
		return result;
	}
	if (PlannerSameHardware(hardware, query.reference_hardware) && query.reference_model.valid &&
	    query.reference_model.throughput > 0) {
		result.valid = true;
		result.fallback = true;
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
		if (PlannerSameHardware(profile.hardware, hardware)) {
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

static vector<QueryHardwareWorkerTopology> FlattenSlots(const vector<CoreSlots> &cores, bool reverse_sockets) {
	vector<QueryHardwareWorkerTopology> result;
	for (const auto &core : cores) {
		for (const auto &worker : core.workers) {
			result.push_back(worker);
		}
	}
	if (reverse_sockets) {
		std::stable_sort(result.begin(), result.end(), [](const QueryHardwareWorkerTopology &left,
		                                                   const QueryHardwareWorkerTopology &right) {
			if (left.socket_id != right.socket_id) {
				return left.socket_id > right.socket_id;
			}
			if (left.physical_core_id != right.physical_core_id) {
				return left.physical_core_id < right.physical_core_id;
			}
			return left.logical_cpu < right.logical_cpu;
		});
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

static bool IsSafeNormalHardware(const QuerySLAEnergyModelInput &query,
	                             const QueryRequestHardwareConfiguration &hardware) {
	if (PlannerSameHardware(hardware, query.reference_hardware)) {
		return true;
	}
	for (const auto &profile : query.hardware_profiles) {
		if (PlannerSameHardware(profile.hardware, hardware)) {
			return profile.valid && profile.mature && profile.safe_work_units_per_s > 0;
		}
	}
	return false;
}

static bool ReconcileNormalHardwarePairs(vector<QuerySLAEnergyPlacedWorker> &workers,
	                                     const QuerySLAEnergyPlannerInput &input, idx_t &promotion_count) {
	// Core and uncore requests are domain-coupled: SMT siblings share a core target and all workers on a socket
	// share an uncore target. Resolve those maxima first, then conservatively promote any resulting unprofiled
	// cross-product to the query's maximum-frequency reference pair. Promotions are monotone, so at most one
	// promotion per worker is needed before convergence.
	for (idx_t iteration = 0; iteration <= workers.size(); iteration++) {
		ResolveActualHardware(workers, input.topology);
		bool changed = false;
		for (auto &worker : workers) {
			if (worker.query_index >= input.queries.size()) {
				return false;
			}
			const auto &query = input.queries[worker.query_index];
			if (IsSafeNormalHardware(query, worker.normal_hardware)) {
				continue;
			}
			if (PlannerSameHardware(worker.preferred_hardware, query.reference_hardware)) {
				return false;
			}
			worker.preferred_hardware = query.reference_hardware;
			promotion_count++;
			changed = true;
		}
		if (!changed) {
			return true;
		}
	}
	return false;
}

static double PlannerPredictedEpochEnergy(const QuerySLAModelInput &input, double service_rate, double power_w) {
	if (service_rate <= 0 || power_w < 0 || !std::isfinite(service_rate) || !std::isfinite(power_w)) {
		return 0;
	}
	auto epoch_s = static_cast<double>(input.epoch_ns) / 1000000000.0;
	auto work_s = input.work.remaining_chunks_equiv > 0
	                  ? static_cast<double>(input.work.remaining_chunks_equiv) / service_rate
	                  : 0.0;
	return power_w * MinValue<double>(epoch_s, work_s);
}

static bool RebuildQueryAllocationsFromActualHardware(QuerySLAEnergyEpochPlan &plan,
	                                                   const QuerySLAEnergyPlannerInput &input) {
	vector<QuerySLAEnergyQueryAllocation> allocations(input.queries.size());
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input, 0, 0);
		if (evaluation.valid) {
			allocations[query_index].predicted_sla_cost = evaluation.mandatory_cost;
			allocations[query_index].predicted_optional_risk = evaluation.optional_risk;
		}
	}
	for (const auto &worker : plan.workers) {
		if (worker.query_index >= input.queries.size() ||
		    !IsSafeNormalHardware(input.queries[worker.query_index], worker.normal_hardware)) {
			return false;
		}
		const auto &query = input.queries[worker.query_index];
		auto candidate = FindHardwareCandidate(query, worker.normal_hardware);
		if (!candidate.valid && PlannerSameHardware(worker.normal_hardware, query.reference_hardware)) {
			// Serial lifecycle work has no source throughput. The reference pair is still the only safe cold pair.
			candidate.valid = query.reference_model.valid;
			candidate.throughput = query.reference_model.throughput;
		}
		if (!candidate.valid) {
			return false;
		}
		auto &allocation = allocations[worker.query_index];
		QuerySLAEnergyWorkerAssignment assignment;
		assignment.hardware = worker.normal_hardware;
		assignment.mandatory = worker.mandatory;
		assignment.optional = worker.optional;
		assignment.liveness = worker.liveness;
		allocation.workers.push_back(assignment);
		allocation.aggregate_service_rate += candidate.throughput;
		allocation.aggregate_active_power_w += candidate.active_power_w;
		allocation.mandatory_workers += worker.mandatory ? 1 : 0;
		allocation.optional_workers += worker.optional ? 1 : 0;
		allocation.liveness_workers += worker.liveness ? 1 : 0;
	}
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto &allocation = allocations[query_index];
		auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input,
		                                             allocation.aggregate_service_rate, allocation.workers.size());
		if (!evaluation.valid) {
			return false;
		}
		allocation.predicted_sla_cost = evaluation.mandatory_cost;
		allocation.predicted_optional_risk = evaluation.optional_risk;
		allocation.predicted_epoch_energy_j = PlannerPredictedEpochEnergy(
		    input.queries[query_index].sla_input, allocation.aggregate_service_rate, allocation.aggregate_active_power_w);
	}
	plan.queries = std::move(allocations);
	return true;
}

static double PlacementEnergy(const vector<QuerySLAEnergyPlacedWorker> &workers,
	                          const vector<QueryHardwareWorkerTopology> &topology,
	                          const QueryHardwarePowerModelSnapshot &power_model, uint64_t epoch_ns) {
	if (workers.empty()) {
		return 0;
	}
	std::unordered_map<idx_t, QueryHardwareWorkerTopology> worker_topology;
	std::map<int, idx_t> physical_cores_per_socket;
	std::set<CoreKey> all_cores;
	for (const auto &worker : topology) {
		worker_topology[worker.worker_id] = worker;
		all_cores.insert(CoreKey(worker.socket_id, worker.physical_core_id));
	}
	for (const auto &core : all_cores) {
		physical_cores_per_socket[core.socket_id]++;
	}
	std::map<CoreKey, QueryRequestHardwareConfiguration> cores;
	std::map<int, uint32_t> sockets;
	for (const auto &worker : workers) {
		auto topology_entry = worker_topology.find(worker.worker_id);
		if (topology_entry == worker_topology.end()) {
			continue;
		}
		CoreKey core(topology_entry->second.socket_id, topology_entry->second.physical_core_id);
		auto &hardware = cores[core];
		hardware.core_frequency_khz =
		    MaxValue<uint32_t>(hardware.core_frequency_khz, worker.normal_hardware.core_frequency_khz);
		hardware.uncore_frequency_khz =
		    MaxValue<uint32_t>(hardware.uncore_frequency_khz, worker.normal_hardware.uncore_frequency_khz);
		sockets[core.socket_id] =
		    MaxValue<uint32_t>(sockets[core.socket_id], worker.normal_hardware.uncore_frequency_khz);
	}
	double power_w = 0;
	for (const auto &socket : sockets) {
		auto value = power_model.SocketIncrementalPowerW(socket.first, socket.second);
		if (value < 0) {
			// Deterministic fallback keeps dense packing preferable when calibration is unavailable.
			power_w += 1000.0 + static_cast<double>(socket.second) / 1000000.0;
		} else {
			power_w += value;
		}
	}
	for (const auto &core : cores) {
		auto value = power_model.CoreIncrementalPowerW(core.first.socket_id, core.second.core_frequency_khz,
		                                             sockets[core.first.socket_id],
		                                             physical_cores_per_socket[core.first.socket_id]);
		if (value < 0) {
			power_w += static_cast<double>(core.second.core_frequency_khz) / 1000000.0;
		} else {
			power_w += value;
		}
	}
	return power_w * static_cast<double>(epoch_ns) / 1000000000.0;
}

static vector<QuerySLAEnergyPlacedWorker>
BuildMandatoryCandidate(const vector<MandatoryToken> &tokens, const vector<QueryHardwareWorkerTopology> &slots,
	                    const QuerySLAEnergyPlannerInput &input, idx_t &promotion_count) {
	vector<QuerySLAEnergyPlacedWorker> result;
	if (tokens.size() > slots.size()) {
		return result;
	}
	result.reserve(tokens.size());
	for (idx_t i = 0; i < tokens.size(); i++) {
		QuerySLAEnergyPlacedWorker worker;
		worker.worker_id = slots[i].worker_id;
		worker.query_index = tokens[i].query_index;
		worker.preferred_hardware = tokens[i].preferred;
		worker.mandatory = true;
		result.push_back(worker);
	}
	if (!ReconcileNormalHardwarePairs(result, input, promotion_count)) {
		result.clear();
	}
	return result;
}

static bool EvaluateOptionalWorker(const QuerySLAEnergyModelInput &query,
	                               const QuerySLAEnergyQueryAllocation &allocation,
	                               const QueryRequestHardwareConfiguration &hardware, double &throughput,
	                               double &risk_gain, QuerySLAEnergyPlannerDiagnostics &diagnostics) {
	if (!query.reference_model.valid || allocation.workers.size() >= query.reference_model.demand_cap) {
		return false;
	}
	auto candidate = FindHardwareCandidate(query, hardware, &diagnostics);
	if (!candidate.valid || candidate.throughput <= 0) {
		return false;
	}
	auto evaluation = QuerySLAEvaluateServiceRate(query.sla_input,
	                                             allocation.aggregate_service_rate + candidate.throughput,
	                                             allocation.workers.size() + 1);
	if (!evaluation.valid) {
		return false;
	}
	throughput = candidate.throughput;
	risk_gain = allocation.predicted_optional_risk - evaluation.optional_risk;
	return risk_gain > 0;
}

static void ApplyOptionalWorker(QuerySLAEnergyEpochPlan &plan, const QuerySLAEnergyPlannerInput &input,
	                            idx_t query_index, idx_t worker_id,
	                            const QueryRequestHardwareConfiguration &hardware, double throughput,
	                            bool liveness = false) {
	auto &allocation = plan.queries[query_index];
	auto evaluation = QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input,
	                                             allocation.aggregate_service_rate + throughput,
	                                             allocation.workers.size() + 1);
	QuerySLAEnergyWorkerAssignment query_worker;
	query_worker.hardware = hardware;
	query_worker.optional = !liveness;
	query_worker.liveness = liveness;
	allocation.workers.push_back(query_worker);
	allocation.aggregate_service_rate += throughput;
	if (evaluation.valid) {
		allocation.predicted_sla_cost = evaluation.mandatory_cost;
		allocation.predicted_optional_risk = evaluation.optional_risk;
	}
	allocation.optional_workers += liveness ? 0 : 1;
	allocation.liveness_workers += liveness ? 1 : 0;
	QuerySLAEnergyPlacedWorker worker;
	worker.worker_id = worker_id;
	worker.query_index = query_index;
	worker.preferred_hardware = hardware;
	worker.normal_hardware = hardware;
	worker.execution_hardware = hardware;
	worker.optional = !liveness;
	worker.liveness = liveness;
	plan.workers.push_back(worker);
}

static vector<idx_t> QueryShortlist(const QuerySLAEnergyPlannerInput &input, const QuerySLAEnergyEpochPlan &plan,
	                                const QueryRequestHardwareConfiguration &hardware,
	                                const vector<double> *service_rates = nullptr,
	                                const vector<double> *optional_risks = nullptr,
	                                const vector<idx_t> *worker_counts = nullptr) {
	vector<std::pair<double, idx_t>> ranked;
	for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
		auto candidate = FindHardwareCandidate(input.queries[query_index], hardware);
		if (!candidate.valid) {
			continue;
		}
		auto service = service_rates ? (*service_rates)[query_index] : plan.queries[query_index].aggregate_service_rate;
		auto risk = optional_risks ? (*optional_risks)[query_index] : plan.queries[query_index].predicted_optional_risk;
		auto workers = worker_counts ? (*worker_counts)[query_index] : plan.queries[query_index].workers.size();
		if (workers >= input.queries[query_index].reference_model.demand_cap) {
			continue;
		}
		auto evaluation =
		    QuerySLAEvaluateServiceRate(input.queries[query_index].sla_input, service + candidate.throughput, workers + 1);
		if (evaluation.valid && risk > evaluation.optional_risk) {
			ranked.emplace_back(risk - evaluation.optional_risk, query_index);
		}
	}
	std::sort(ranked.begin(), ranked.end(), [](const std::pair<double, idx_t> &left,
	                                         const std::pair<double, idx_t> &right) {
		if (left.first != right.first) {
			return left.first > right.first;
		}
		return left.second < right.second;
	});
	vector<idx_t> result;
	for (idx_t i = 0; i < MinValue<idx_t>(ranked.size(), OPTIONAL_QUERY_SHORTLIST); i++) {
		result.push_back(ranked[i].second);
	}
	return result;
}

static OptionalAction BestPairAction(const QuerySLAEnergyPlannerInput &input, const QuerySLAEnergyEpochPlan &plan,
	                                 const CoreKey &core, uint32_t core_khz, uint32_t uncore_khz,
	                                 double activation_energy_j, const vector<double> *service_rates = nullptr,
	                                 const vector<double> *optional_risks = nullptr,
	                                 const vector<idx_t> *worker_counts = nullptr) {
	auto hardware = PlannerHardware(core_khz, uncore_khz);
	auto shortlist = QueryShortlist(input, plan, hardware, service_rates, optional_risks, worker_counts);
	OptionalAction best;
	for (auto query_a : shortlist) {
		for (auto query_b : shortlist) {
			auto service_a = service_rates ? (*service_rates)[query_a] : plan.queries[query_a].aggregate_service_rate;
			auto risk_a = optional_risks ? (*optional_risks)[query_a] : plan.queries[query_a].predicted_optional_risk;
			auto workers_a = worker_counts ? (*worker_counts)[query_a] : plan.queries[query_a].workers.size();
			auto candidate_a = FindHardwareCandidate(input.queries[query_a], hardware);
			auto candidate_b = FindHardwareCandidate(input.queries[query_b], hardware);
			if (!candidate_a.valid || !candidate_b.valid) {
				continue;
			}
			double gain = 0;
			if (query_a == query_b) {
				if (workers_a + 2 > input.queries[query_a].reference_model.demand_cap) {
					continue;
				}
				auto evaluation = QuerySLAEvaluateServiceRate(
				    input.queries[query_a].sla_input, service_a + candidate_a.throughput + candidate_b.throughput,
				    workers_a + 2);
				if (!evaluation.valid) {
					continue;
				}
				gain = risk_a - evaluation.optional_risk;
			} else {
				auto service_b = service_rates ? (*service_rates)[query_b] : plan.queries[query_b].aggregate_service_rate;
				auto risk_b = optional_risks ? (*optional_risks)[query_b] : plan.queries[query_b].predicted_optional_risk;
				auto workers_b = worker_counts ? (*worker_counts)[query_b] : plan.queries[query_b].workers.size();
				if (workers_a + 1 > input.queries[query_a].reference_model.demand_cap ||
				    workers_b + 1 > input.queries[query_b].reference_model.demand_cap) {
					continue;
				}
				auto eval_a = QuerySLAEvaluateServiceRate(input.queries[query_a].sla_input,
				                                            service_a + candidate_a.throughput, workers_a + 1);
				auto eval_b = QuerySLAEvaluateServiceRate(input.queries[query_b].sla_input,
				                                            service_b + candidate_b.throughput, workers_b + 1);
				if (!eval_a.valid || !eval_b.valid) {
					continue;
				}
				gain = risk_a - eval_a.optional_risk + risk_b - eval_b.optional_risk;
			}
			auto utility = gain - input.energy_lambda * activation_energy_j;
			if (utility > 0 && (!best.valid || utility > best.utility)) {
				best.valid = true;
				best.utility = utility;
				best.risk_gain = gain;
				best.energy_j = activation_energy_j;
				best.core = core;
				best.socket_id = core.socket_id;
				best.core_frequency_khz = core_khz;
				best.uncore_frequency_khz = uncore_khz;
				best.query_a = query_a;
				best.query_b = query_b;
				best.throughput_a = candidate_a.throughput;
				best.throughput_b = candidate_b.throughput;
			}
		}
	}
	return best;
}

static void ApplyPairAction(QuerySLAEnergyEpochPlan &plan, const QuerySLAEnergyPlannerInput &input,
	                        const OptionalAction &action, const CoreSlots &core) {
	auto hardware = PlannerHardware(action.core_frequency_khz, action.uncore_frequency_khz);
	ApplyOptionalWorker(plan, input, action.query_a, core.workers[0].worker_id, hardware, action.throughput_a);
	ApplyOptionalWorker(plan, input, action.query_b, core.workers[1].worker_id, hardware, action.throughput_b);
	plan.diagnostics.optional_incremental_energy_j += action.energy_j;
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
			if (!worker.optional || worker.mandatory || worker.liveness) {
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

QuerySLAEnergyEpochPlan PlanQuerySLAEnergyEpoch(const QuerySLAEnergyPlannerInput &input) {
	QuerySLAEnergyEpochPlan result;
	if (input.queries.empty()) {
		result.valid = true;
		return result;
	}
	if (input.worker_budget == 0 || input.topology.empty()) {
		result.error = "SLA-energy planner requires a nonempty pinned worker topology";
		return result;
	}
	for (const auto &query : input.queries) {
		for (const auto &profile : query.hardware_profiles) {
			if (profile.valid && !profile.mature && !PlannerSameHardware(profile.hardware, query.reference_hardware)) {
				result.diagnostics.immature_pair_rejections++;
			}
		}
	}
	auto budget = MinValue<idx_t>(input.worker_budget, input.topology.size());
	auto mandatory = AllocateQuerySLAEnergyWorkers(input.queries, budget, input.energy_lambda, false);
	result.queries = std::move(mandatory.queries);
	vector<MandatoryToken> tokens;
	for (idx_t query_index = 0; query_index < result.queries.size(); query_index++) {
		for (const auto &worker : result.queries[query_index].workers) {
			if (worker.mandatory) {
				tokens.push_back(MandatoryToken(query_index, worker.hardware));
			}
		}
	}
	std::sort(tokens.begin(), tokens.end(), [](const MandatoryToken &left, const MandatoryToken &right) {
		if (left.preferred.uncore_frequency_khz != right.preferred.uncore_frequency_khz) {
			return left.preferred.uncore_frequency_khz > right.preferred.uncore_frequency_khz;
		}
		if (left.preferred.core_frequency_khz != right.preferred.core_frequency_khz) {
			return left.preferred.core_frequency_khz > right.preferred.core_frequency_khz;
		}
		return left.query_index < right.query_index;
	});
	auto cores = BuildCoreSlots(input.topology);
	auto dense_slots = FlattenSlots(cores, false);
	auto alternate_slots = FlattenSlots(cores, true);
	idx_t dense_promotions = 0;
	idx_t alternate_promotions = 0;
	auto dense = BuildMandatoryCandidate(tokens, dense_slots, input, dense_promotions);
	auto alternate = BuildMandatoryCandidate(tokens, alternate_slots, input, alternate_promotions);
	if (!tokens.empty() && (dense.empty() || alternate.empty())) {
		result.error = "SLA-energy planner could not reconcile mandatory hardware domains";
		return result;
	}
	auto dense_energy = PlacementEnergy(dense, input.topology, input.power_model, input.epoch_ns);
	auto alternate_energy = PlacementEnergy(alternate, input.topology, input.power_model, input.epoch_ns);
	if (alternate_energy < dense_energy) {
		result.workers = std::move(alternate);
		result.diagnostics.domain_pair_promotions += alternate_promotions;
	} else {
		result.workers = std::move(dense);
		result.diagnostics.domain_pair_promotions += dense_promotions;
	}
	result.diagnostics.mandatory_footprint_energy_j = MinValue<double>(dense_energy, alternate_energy);
	result.diagnostics.mandatory_workers = result.workers.size();
	if (!RebuildQueryAllocationsFromActualHardware(result, input)) {
		result.error = "SLA-energy planner could not model reconciled mandatory hardware";
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

	// O1: unused SMT slots on already active physical cores inherit the existing core/socket pair.
	while (used_worker_ids.size() < budget) {
		double best_gain = 0;
		idx_t best_query = DConstants::INVALID_INDEX;
		idx_t best_worker = DConstants::INVALID_INDEX;
		double best_throughput = 0;
		QueryRequestHardwareConfiguration best_hardware;
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
				for (idx_t query_index = 0; query_index < input.queries.size(); query_index++) {
					double throughput = 0;
					double gain = 0;
					if (EvaluateOptionalWorker(input.queries[query_index], result.queries[query_index], hardware,
					                           throughput, gain, result.diagnostics) &&
					    gain > best_gain) {
						best_gain = gain;
						best_query = query_index;
						best_worker = slot.worker_id;
						best_throughput = throughput;
						best_hardware = hardware;
					}
				}
			}
		}
		if (best_query == DConstants::INVALID_INDEX) {
			break;
		}
		ApplyOptionalWorker(result, input, best_query, best_worker, best_hardware, best_throughput);
		used_worker_ids.insert(best_worker);
		result.diagnostics.o1_workers++;
	}

	// O2: activate complete SMT cores on sockets already used by mandatory/O1 work.
	bool made_progress = true;
	while (made_progress && used_worker_ids.size() + 2 <= budget && input.power_model.valid) {
		made_progress = false;
		OptionalAction best;
		const CoreSlots *best_core = nullptr;
		std::map<int, idx_t> core_count_by_socket;
		for (const auto &core : cores) {
			core_count_by_socket[core.key.socket_id]++;
		}
		for (const auto &core : cores) {
			if (active_sockets.find(core.key.socket_id) == active_sockets.end() ||
			    active_cores.find(core.key) != active_cores.end() || core.workers.size() < 2) {
				continue;
			}
			uint32_t socket_uncore = 0;
			for (const auto &worker : result.workers) {
				auto topology = worker_topology[worker.worker_id];
				if (topology.socket_id == core.key.socket_id) {
					socket_uncore = MaxValue<uint32_t>(socket_uncore, worker.normal_hardware.uncore_frequency_khz);
				}
			}
			for (auto core_khz : input.core_frequency_levels_khz) {
				auto power_w = input.power_model.CoreIncrementalPowerW(
				    core.key.socket_id, core_khz, socket_uncore, core_count_by_socket[core.key.socket_id]);
				if (power_w < 0) {
					continue;
				}
				auto energy_j = power_w * static_cast<double>(input.epoch_ns) / 1000000000.0;
				auto action = BestPairAction(input, result, core.key, core_khz, socket_uncore, energy_j);
				if (action.valid && (!best.valid || action.utility > best.utility)) {
					best = action;
					best_core = &core;
				}
			}
		}
		if (best.valid && best_core) {
			ApplyPairAction(result, input, best, *best_core);
			used_worker_ids.insert(best_core->workers[0].worker_id);
			used_worker_ids.insert(best_core->workers[1].worker_id);
			active_cores.insert(best_core->key);
			result.diagnostics.o2_workers += 2;
			made_progress = true;
		}
	}

	// O3: bounded beam search over complete SMT cores on one currently inactive socket.
	std::set<int> topology_sockets;
	for (const auto &core : cores) {
		topology_sockets.insert(core.key.socket_id);
	}
	while (used_worker_ids.size() + 2 <= budget && input.power_model.valid) {
		double best_bundle_utility = 0;
		int best_socket = -1;
		uint32_t best_uncore = 0;
		vector<OptionalAction> best_actions;
		for (const auto &socket_entry : topology_sockets) {
			if (active_sockets.find(socket_entry) != active_sockets.end()) {
				continue;
			}
			vector<const CoreSlots *> socket_cores;
			for (const auto &core : cores) {
				if (core.key.socket_id == socket_entry && core.workers.size() >= 2) {
					socket_cores.push_back(&core);
				}
			}
			auto max_depth = MinValue<idx_t>(
			    O3_MAX_DEPTH, MinValue<idx_t>(socket_cores.size(), (budget - used_worker_ids.size()) / 2));
			for (auto uncore_khz : input.uncore_frequency_levels_khz) {
				auto socket_power_w = input.power_model.SocketIncrementalPowerW(socket_entry, uncore_khz);
				if (socket_power_w < 0) {
					continue;
				}
				vector<BeamState> beam(1);
				beam[0].service_rates.resize(result.queries.size());
				beam[0].optional_risks.resize(result.queries.size());
				beam[0].worker_counts.resize(result.queries.size());
				for (idx_t query_index = 0; query_index < result.queries.size(); query_index++) {
					beam[0].service_rates[query_index] = result.queries[query_index].aggregate_service_rate;
					beam[0].optional_risks[query_index] = result.queries[query_index].predicted_optional_risk;
					beam[0].worker_counts[query_index] = result.queries[query_index].workers.size();
				}
				for (idx_t depth = 0; depth < max_depth && !beam.empty(); depth++) {
					vector<BeamState> children;
					for (const auto &state : beam) {
						vector<OptionalAction> actions;
						for (auto core_khz : input.core_frequency_levels_khz) {
							auto core_power_w = input.power_model.CoreIncrementalPowerW(
							    socket_entry, core_khz, uncore_khz, socket_cores.size());
							if (core_power_w < 0) {
								continue;
							}
							auto energy_j = core_power_w * static_cast<double>(input.epoch_ns) / 1000000000.0;
							auto action = BestPairAction(input, result, socket_cores[depth]->key, core_khz,
							                             uncore_khz, energy_j, &state.service_rates,
							                             &state.optional_risks, &state.worker_counts);
							if (action.valid) {
								actions.push_back(action);
							}
						}
						std::sort(actions.begin(), actions.end(), [](const OptionalAction &left,
						                                                  const OptionalAction &right) {
							return left.utility > right.utility;
						});
						for (idx_t action_index = 0;
						     action_index < MinValue<idx_t>(actions.size(), OPTIONAL_QUERY_SHORTLIST); action_index++) {
							auto child = state;
							const auto &action = actions[action_index];
							auto hardware =
							    PlannerHardware(action.core_frequency_khz, action.uncore_frequency_khz);
							auto candidate_a = FindHardwareCandidate(input.queries[action.query_a], hardware);
							auto candidate_b = FindHardwareCandidate(input.queries[action.query_b], hardware);
							if (action.query_a == action.query_b) {
								auto q = action.query_a;
								child.service_rates[q] += candidate_a.throughput + candidate_b.throughput;
								child.worker_counts[q] += 2;
								auto evaluation = QuerySLAEvaluateServiceRate(input.queries[q].sla_input,
								                                                  child.service_rates[q], child.worker_counts[q]);
								child.optional_risks[q] = evaluation.optional_risk;
							} else {
								for (auto q : {action.query_a, action.query_b}) {
									auto candidate = FindHardwareCandidate(input.queries[q], hardware);
									child.service_rates[q] += candidate.throughput;
									child.worker_counts[q]++;
									auto evaluation = QuerySLAEvaluateServiceRate(input.queries[q].sla_input,
									                                                  child.service_rates[q], child.worker_counts[q]);
									child.optional_risks[q] = evaluation.optional_risk;
								}
							}
							child.actions.push_back(action);
							child.value += action.utility;
							children.push_back(std::move(child));
						}
					}
					std::sort(children.begin(), children.end(), [](const BeamState &left, const BeamState &right) {
						return left.value > right.value;
					});
					if (children.size() > O3_BEAM_WIDTH) {
						children.resize(O3_BEAM_WIDTH);
					}
					beam = std::move(children);
					auto socket_energy_j =
					    socket_power_w * static_cast<double>(input.epoch_ns) / 1000000000.0;
					for (const auto &state : beam) {
						auto utility = state.value - input.energy_lambda * socket_energy_j;
						if (utility > best_bundle_utility) {
							best_bundle_utility = utility;
							best_socket = socket_entry;
							best_uncore = uncore_khz;
							best_actions = state.actions;
						}
					}
				}
			}
		}
		if (best_socket < 0 || best_actions.empty() || best_bundle_utility <= 0) {
			break;
		}
		idx_t action_index = 0;
		for (const auto &core : cores) {
			if (core.key.socket_id != best_socket || action_index >= best_actions.size()) {
				continue;
			}
			auto action = best_actions[action_index++];
			action.uncore_frequency_khz = best_uncore;
			ApplyPairAction(result, input, action, core);
			used_worker_ids.insert(core.workers[0].worker_id);
			used_worker_ids.insert(core.workers[1].worker_id);
			active_cores.insert(core.key);
			result.diagnostics.o3_workers += 2;
		}
		active_sockets.insert(best_socket);
	}

	// Narrow liveness fallback: only active queries with no allocation receive one reference worker.
	for (idx_t query_index = 0; query_index < result.queries.size() && used_worker_ids.size() < budget; query_index++) {
		if (!result.queries[query_index].workers.empty() || !input.queries[query_index].reference_model.valid ||
		    input.queries[query_index].reference_model.demand_cap == 0) {
			continue;
		}
		auto slot = std::find_if(input.topology.begin(), input.topology.end(), [&](const QueryHardwareWorkerTopology &w) {
			return used_worker_ids.find(w.worker_id) == used_worker_ids.end();
		});
		if (slot == input.topology.end()) {
			break;
		}
		ApplyOptionalWorker(result, input, query_index, slot->worker_id, input.queries[query_index].reference_hardware,
		                    input.queries[query_index].reference_model.throughput, true);
		used_worker_ids.insert(slot->worker_id);
		result.diagnostics.liveness_workers++;
	}

	idx_t final_promotions = 0;
	if (!ReconcileNormalHardwarePairs(result.workers, input, final_promotions)) {
		result.error = "SLA-energy planner could not reconcile final hardware domains";
		return result;
	}
	result.diagnostics.domain_pair_promotions += final_promotions;
	if (!RebuildQueryAllocationsFromActualHardware(result, input)) {
		result.error = "SLA-energy planner could not model final reconciled hardware";
		return result;
	}
	ApplyExplorationOverlay(result, input, input.topology);
	result.diagnostics.parked_workers = budget - used_worker_ids.size();
	result.valid = true;
	return result;
}

} // namespace duckdb
