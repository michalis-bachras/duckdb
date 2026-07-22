//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_energy_model.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_energy_model.hpp"

#include <cmath>

namespace duckdb {

namespace {

struct HardwareCandidate {
	QueryRequestHardwareConfiguration hardware;
	double throughput = 0;
	double active_power_w = 0;
	bool energy_valid = false;
	bool reference = false;
};

struct CandidateAddition {
	bool valid = false;
	idx_t query_index = 0;
	HardwareCandidate candidate;
	QuerySLAServiceEvaluation evaluation;
	double aggregate_service_rate = 0;
	double aggregate_power_w = 0;
	double epoch_energy_j = 0;
	double optional_gain = 0;
	double utility = 0;
};

static bool SameHardware(const QueryRequestHardwareConfiguration &left,
	                     const QueryRequestHardwareConfiguration &right) {
	return left == right;
}

static bool SLACostEquivalent(double candidate, double reference) {
	auto tolerance = 1e-9 * MaxValue<double>(1.0, std::fabs(reference));
	return candidate <= reference + tolerance;
}

static bool HardwareNearTiePreferred(const HardwareCandidate &candidate, const HardwareCandidate &current) {
	if (candidate.hardware.uncore_frequency_khz != current.hardware.uncore_frequency_khz) {
		return candidate.hardware.uncore_frequency_khz < current.hardware.uncore_frequency_khz;
	}
	if (candidate.active_power_w != current.active_power_w) {
		return candidate.active_power_w < current.active_power_w;
	}
	return candidate.hardware.core_frequency_khz < current.hardware.core_frequency_khz;
}

static double PredictedEpochEnergy(const QuerySLAModelInput &input, double service_rate, double power_w) {
	if (service_rate <= 0 || power_w < 0 || !std::isfinite(service_rate) || !std::isfinite(power_w)) {
		return 0;
	}
	auto epoch_s = static_cast<double>(input.epoch_ns) / 1000000000.0;
	auto work_s = input.work.remaining_chunks_equiv > 0
	                  ? static_cast<double>(input.work.remaining_chunks_equiv) / service_rate
	                  : 0.0;
	return power_w * MinValue<double>(epoch_s, work_s);
}

static vector<HardwareCandidate> BuildCandidates(const QuerySLAEnergyModelInput &input) {
	vector<HardwareCandidate> result;
	HardwareCandidate reference;
	reference.hardware = input.reference_hardware;
	reference.throughput = input.reference_model.throughput;
	reference.reference = true;
	for (const auto &profile : input.hardware_profiles) {
		if (!profile.mature || !SameHardware(profile.hardware, input.reference_hardware)) {
			continue;
		}
		reference.throughput = profile.safe_work_units_per_s;
		reference.active_power_w =
		    profile.ewma_active_power_w > 0 ? profile.ewma_active_power_w : profile.mean_active_power_w;
		reference.energy_valid = reference.throughput > 0 && reference.active_power_w > 0;
		break;
	}
	if (reference.throughput > 0 ||
	    (input.reference_model.event_kind != QueryActivationEventKind::PIPELINE &&
	     input.reference_model.event_kind != QueryActivationEventKind::INTERNAL)) {
		result.push_back(reference);
	}
	for (const auto &profile : input.hardware_profiles) {
		auto active_power_w =
		    profile.ewma_active_power_w > 0 ? profile.ewma_active_power_w : profile.mean_active_power_w;
		if (!profile.mature || profile.safe_work_units_per_s <= 0 || active_power_w <= 0 ||
		    SameHardware(profile.hardware, input.reference_hardware)) {
			continue;
		}
		HardwareCandidate candidate;
		candidate.hardware = profile.hardware;
		candidate.throughput = profile.safe_work_units_per_s;
		candidate.active_power_w = active_power_w;
		candidate.energy_valid = true;
		result.push_back(candidate);
	}
	return result;
}

static CandidateAddition SelectMandatoryAddition(const QuerySLAEnergyModelInput &input,
	                                              const vector<HardwareCandidate> &candidates,
	                                              const QuerySLAEnergyQueryAllocation &current,
	                                              idx_t target_worker_count) {
	CandidateAddition best;
	if (target_worker_count >= input.reference_model.mandatory_cost.size()) {
		return best;
	}
	auto reference_cost = input.reference_model.mandatory_cost[target_worker_count];
	for (const auto &candidate : candidates) {
		auto service = current.aggregate_service_rate + candidate.throughput;
		auto evaluation = QuerySLAEvaluateServiceRate(input.sla_input, service, target_worker_count);
		if (!evaluation.valid || !SLACostEquivalent(evaluation.mandatory_cost, reference_cost)) {
			continue;
		}
		auto power = current.aggregate_active_power_w + candidate.active_power_w;
		auto energy = PredictedEpochEnergy(input.sla_input, service, power);
		if (!best.valid || (candidate.energy_valid && best.candidate.energy_valid && energy < best.epoch_energy_j) ||
		    (candidate.energy_valid && !best.candidate.energy_valid && candidate.reference) ||
		    (!best.candidate.energy_valid && candidate.reference)) {
			best.valid = true;
			best.candidate = candidate;
			best.evaluation = evaluation;
			best.aggregate_service_rate = service;
			best.aggregate_power_w = power;
			best.epoch_energy_j = energy;
		}
	}
	return best;
}

static CandidateAddition SelectMandatoryConfiguration(const QuerySLAEnergyModelInput &input,
	                                                   const vector<HardwareCandidate> &candidates,
	                                                   idx_t target_worker_count) {
	static constexpr double EFFICIENCY_NEAR_TIE_FRACTION = 0.02;
	CandidateAddition fallback;
	vector<CandidateAddition> feasible;
	double maximum_efficiency = 0;
	if (target_worker_count == 0 || target_worker_count >= input.reference_model.mandatory_cost.size()) {
		return fallback;
	}
	auto reference_cost = input.reference_model.mandatory_cost[target_worker_count];
	for (const auto &candidate : candidates) {
		auto service = static_cast<double>(target_worker_count) * candidate.throughput;
		auto evaluation = QuerySLAEvaluateServiceRate(input.sla_input, service, target_worker_count);
		if (!evaluation.valid || !SLACostEquivalent(evaluation.mandatory_cost, reference_cost)) {
			continue;
		}
		CandidateAddition addition;
		addition.valid = true;
		addition.candidate = candidate;
		addition.evaluation = evaluation;
		addition.aggregate_service_rate = service;
		addition.aggregate_power_w = static_cast<double>(target_worker_count) * candidate.active_power_w;
		addition.epoch_energy_j =
		    PredictedEpochEnergy(input.sla_input, addition.aggregate_service_rate, addition.aggregate_power_w);
		if (candidate.reference) {
			fallback = addition;
		}
		if (!candidate.energy_valid) {
			continue;
		}
		addition.utility = candidate.throughput / candidate.active_power_w;
		maximum_efficiency = MaxValue<double>(maximum_efficiency, addition.utility);
		feasible.push_back(addition);
	}
	if (feasible.empty()) {
		return fallback;
	}
	CandidateAddition best;
	auto threshold = (1.0 - EFFICIENCY_NEAR_TIE_FRACTION) * maximum_efficiency;
	for (const auto &candidate : feasible) {
		if (candidate.utility < threshold) {
			continue;
		}
		if (!best.valid || HardwareNearTiePreferred(candidate.candidate, best.candidate)) {
			best = candidate;
		}
	}
	return best.valid ? best : fallback;
}

static CandidateAddition SelectOptionalAddition(const QuerySLAEnergyModelInput &input,
	                                             const vector<HardwareCandidate> &candidates,
	                                             const QuerySLAEnergyQueryAllocation &current,
	                                             double energy_lambda) {
	CandidateAddition best;
	auto worker_count = current.workers.size();
	if (worker_count >= input.reference_model.demand_cap ||
	    worker_count + 1 >= input.reference_model.mandatory_cost.size()) {
		return best;
	}
	for (const auto &candidate : candidates) {
		if (!candidate.reference && (!candidate.energy_valid || current.workers.empty())) {
			continue;
		}
		auto service = current.aggregate_service_rate + candidate.throughput;
		auto evaluation = QuerySLAEvaluateServiceRate(input.sla_input, service, worker_count + 1);
		if (!evaluation.valid ||
		    !SLACostEquivalent(evaluation.mandatory_cost, input.reference_model.mandatory_cost[worker_count + 1])) {
			continue;
		}
		auto optional_gain = current.predicted_optional_risk - evaluation.optional_risk;
		if (optional_gain <= 0) {
			continue;
		}
		auto power = current.aggregate_active_power_w + candidate.active_power_w;
		auto energy = PredictedEpochEnergy(input.sla_input, service, power);
		auto energy_delta = candidate.energy_valid ? energy - current.predicted_epoch_energy_j : 0.0;
		auto utility = optional_gain - energy_lambda * energy_delta;
		if (utility <= 0) {
			continue;
		}
		if (!best.valid || utility > best.utility ||
		    (utility == best.utility && candidate.hardware.core_frequency_khz < best.candidate.hardware.core_frequency_khz)) {
			best.valid = true;
			best.candidate = candidate;
			best.evaluation = evaluation;
			best.aggregate_service_rate = service;
			best.aggregate_power_w = power;
			best.epoch_energy_j = energy;
			best.optional_gain = optional_gain;
			best.utility = utility;
		}
	}
	return best;
}

static void ApplyAddition(QuerySLAEnergyQueryAllocation &target, const CandidateAddition &addition,
	                     bool mandatory, bool optional, bool liveness) {
	QuerySLAEnergyWorkerAssignment worker;
	worker.hardware = addition.candidate.hardware;
	worker.mandatory = mandatory;
	worker.optional = optional;
	worker.liveness = liveness;
	target.workers.push_back(worker);
	target.aggregate_service_rate = addition.aggregate_service_rate;
	target.aggregate_active_power_w = addition.aggregate_power_w;
	target.predicted_epoch_energy_j = addition.epoch_energy_j;
	target.predicted_sla_cost = addition.evaluation.mandatory_cost;
	target.predicted_optional_risk = addition.evaluation.optional_risk;
	target.mandatory_workers += mandatory ? 1 : 0;
	target.optional_workers += optional ? 1 : 0;
	target.liveness_workers += liveness ? 1 : 0;
}

static void ApplyMandatoryConfiguration(QuerySLAEnergyQueryAllocation &target, const CandidateAddition &selection,
	                                    idx_t worker_count) {
	for (idx_t worker_index = 0; worker_index < worker_count; worker_index++) {
		QuerySLAEnergyWorkerAssignment worker;
		worker.hardware = selection.candidate.hardware;
		worker.mandatory = true;
		target.workers.push_back(worker);
	}
	target.aggregate_service_rate = selection.aggregate_service_rate;
	target.aggregate_active_power_w = selection.aggregate_power_w;
	target.predicted_epoch_energy_j = selection.epoch_energy_j;
	target.predicted_sla_cost = selection.evaluation.mandatory_cost;
	target.predicted_optional_risk = selection.evaluation.optional_risk;
	target.mandatory_workers = worker_count;
}

} // namespace

QuerySLAEnergyAllocation AllocateQuerySLAEnergyWorkers(const vector<QuerySLAEnergyModelInput> &inputs,
	                                                   idx_t worker_budget, double energy_lambda,
	                                                   bool include_optional_and_liveness) {
	QuerySLAEnergyAllocation result;
	result.queries.resize(inputs.size());
	vector<QuerySLAModel> reference_models;
	reference_models.reserve(inputs.size());
	vector<vector<HardwareCandidate>> candidates;
	candidates.reserve(inputs.size());
	for (const auto &input : inputs) {
		reference_models.push_back(input.reference_model);
		candidates.push_back(BuildCandidates(input));
	}
	for (idx_t query_index = 0; query_index < inputs.size(); query_index++) {
		auto evaluation = QuerySLAEvaluateServiceRate(inputs[query_index].sla_input, 0, 0);
		if (evaluation.valid) {
			result.queries[query_index].predicted_sla_cost = evaluation.mandatory_cost;
			result.queries[query_index].predicted_optional_risk = evaluation.optional_risk;
		}
	}
	auto reference_allocation = AllocateQuerySLAWorkers(reference_models, worker_budget, false);
	for (idx_t query_index = 0; query_index < inputs.size(); query_index++) {
		auto worker_count = MinValue<idx_t>(reference_allocation.mandatory[query_index], worker_budget);
		if (worker_count == 0) {
			continue;
		}
		auto selection = SelectMandatoryConfiguration(inputs[query_index], candidates[query_index], worker_count);
		if (selection.valid) {
			ApplyMandatoryConfiguration(result.queries[query_index], selection, worker_count);
			worker_budget -= worker_count;
		}
	}
	if (!include_optional_and_liveness) {
		result.unassigned_workers = worker_budget;
		return result;
	}

	while (worker_budget > 0) {
		CandidateAddition best;
		for (idx_t query_index = 0; query_index < inputs.size(); query_index++) {
			auto addition = SelectOptionalAddition(inputs[query_index], candidates[query_index],
			                                       result.queries[query_index], energy_lambda);
			addition.query_index = query_index;
			if (addition.valid && (!best.valid || addition.utility > best.utility)) {
				best = addition;
			}
		}
		if (!best.valid) {
			break;
		}
		ApplyAddition(result.queries[best.query_index], best, false, true, false);
		worker_budget--;
	}

	// This is the only residual assignment in SLA-energy mode: an active query with no selected worker must be able
	// to make progress even when all modeled gains are zero. It is deliberately excluded from energy exploration.
	for (idx_t query_index = 0; query_index < inputs.size() && worker_budget > 0; query_index++) {
		if (!result.queries[query_index].workers.empty() || !inputs[query_index].reference_model.valid ||
		    inputs[query_index].reference_model.demand_cap == 0 || candidates[query_index].empty()) {
			continue;
		}
		auto addition = SelectMandatoryAddition(inputs[query_index], candidates[query_index],
		                                        result.queries[query_index], 1);
		if (!addition.valid) {
			continue;
		}
		ApplyAddition(result.queries[query_index], addition, false, false, true);
		worker_budget--;
	}
	result.unassigned_workers = worker_budget;
	return result;
}

} // namespace duckdb
