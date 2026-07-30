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
	double normalized_edp = 0;
	uint64_t profile_version = 0;
	bool energy_valid = false;
	bool reference = false;
};

struct CandidateAddition {
	bool valid = false;
	HardwareCandidate candidate;
	QuerySLAServiceEvaluation evaluation;
	double aggregate_service_rate = 0;
	double aggregate_power_w = 0;
	double epoch_energy_j = 0;
};

static bool SameHardware(const QueryRequestHardwareConfiguration &left,
	                     const QueryRequestHardwareConfiguration &right) {
	return left == right;
}

static bool IsLifecycleEvent(QueryActivationEventKind event_kind) {
	switch (event_kind) {
	case QueryActivationEventKind::INITIALIZE:
	case QueryActivationEventKind::PREPARE_FINISH:
	case QueryActivationEventKind::FINISH:
	case QueryActivationEventKind::COMPLETE:
		return true;
	default:
		return false;
	}
}

static bool UsesWorkerPresenceModel(const QuerySLAEnergyModelInput &input) {
	return IsLifecycleEvent(input.reference_model.event_kind) ||
	       input.sla_input.work.remaining_chunks_equiv == 0;
}

static bool SLACostEquivalent(double candidate, double reference) {
	auto tolerance = 1e-9 * MaxValue<double>(1.0, std::fabs(reference));
	return candidate <= reference + tolerance;
}

static bool MandatoryCandidatePreferred(const HardwareCandidate &candidate, const HardwareCandidate &current) {
	if (candidate.normalized_edp != current.normalized_edp) {
		return candidate.normalized_edp < current.normalized_edp;
	}
	if (candidate.throughput != current.throughput) {
		return candidate.throughput > current.throughput;
	}
	if (candidate.active_power_w != current.active_power_w) {
		return candidate.active_power_w < current.active_power_w;
	}
	if (candidate.hardware.uncore_frequency_khz != current.hardware.uncore_frequency_khz) {
		return candidate.hardware.uncore_frequency_khz < current.hardware.uncore_frequency_khz;
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
		reference.normalized_edp =
		    reference.energy_valid ? reference.active_power_w / (reference.throughput * reference.throughput) : 0;
		reference.profile_version = profile.profile_version;
		break;
	}
	if (input.reference_model.valid && input.reference_model.demand_cap > 0) {
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
		candidate.normalized_edp = active_power_w / (candidate.throughput * candidate.throughput);
		candidate.profile_version = profile.profile_version;
		result.push_back(candidate);
	}
	return result;
}

static CandidateAddition SelectMandatoryAddition(const QuerySLAEnergyModelInput &input,
	                                              const vector<HardwareCandidate> &candidates,
	                                              const QuerySLAModel &reference_model,
	                                              double existing_service_rate, idx_t existing_worker_count) {
	CandidateAddition best;
	CandidateAddition fallback;
	auto reference = std::find_if(candidates.begin(), candidates.end(),
	                              [](const HardwareCandidate &candidate) { return candidate.reference; });
	if (reference == candidates.end()) {
		return best;
	}
	auto target_worker_count = existing_worker_count + 1;
	if (target_worker_count >= reference_model.service_rates.size() ||
	    existing_worker_count >= reference_model.service_rates.size()) {
		return best;
	}
	auto reference_increment =
	    reference_model.service_rates[target_worker_count] - reference_model.service_rates[existing_worker_count];
	auto reference_service_rate = existing_service_rate + MaxValue<double>(0, reference_increment);
	auto reference_evaluation =
	    QuerySLAEvaluateServiceRate(input.sla_input, reference_service_rate, target_worker_count);
	if (!reference_evaluation.valid) {
		return best;
	}
	for (const auto &candidate : candidates) {
		auto service = candidate.reference ? reference_service_rate : existing_service_rate + candidate.throughput;
		if (!candidate.reference && candidate.throughput < reference_increment) {
			continue;
		}
		auto evaluation = QuerySLAEvaluateServiceRate(input.sla_input, service, target_worker_count);
		if (!evaluation.valid || !SLACostEquivalent(evaluation.mandatory_cost, reference_evaluation.mandatory_cost) ||
		    !SLACostEquivalent(evaluation.optional_risk, reference_evaluation.optional_risk)) {
			continue;
		}
		CandidateAddition addition;
		addition.valid = true;
		addition.candidate = candidate;
		addition.evaluation = evaluation;
		addition.aggregate_service_rate = service;
		addition.aggregate_power_w = candidate.active_power_w;
		addition.epoch_energy_j = PredictedEpochEnergy(input.sla_input, service, candidate.active_power_w);
		if (candidate.reference) {
			fallback = addition;
		}
		if (candidate.energy_valid && (!best.valid || MandatoryCandidatePreferred(candidate, best.candidate))) {
			best = addition;
		}
	}
	return best.valid ? best : fallback;
}

static CandidateAddition SelectMandatoryConfiguration(const QuerySLAEnergyModelInput &input,
	                                                   const vector<HardwareCandidate> &candidates,
	                                                   const QuerySLAModel &reference_model,
	                                                   idx_t target_worker_count) {
	CandidateAddition fallback;
	CandidateAddition best;
	if (target_worker_count == 0 || target_worker_count >= reference_model.mandatory_cost.size() ||
	    target_worker_count >= reference_model.optional_risk.size()) {
		return fallback;
	}
	auto reference_cost = reference_model.mandatory_cost[target_worker_count];
	auto reference_fragility = reference_model.optional_risk[target_worker_count];
	auto reference_service_rate = reference_model.service_rates[target_worker_count];
	for (const auto &candidate : candidates) {
		auto service = candidate.reference ? reference_service_rate
		                                   : static_cast<double>(target_worker_count) * candidate.throughput;
		if (!candidate.reference && service < reference_service_rate) {
			continue;
		}
		auto evaluation = QuerySLAEvaluateServiceRate(input.sla_input, service, target_worker_count);
		if (!evaluation.valid || !SLACostEquivalent(evaluation.mandatory_cost, reference_cost) ||
		    !SLACostEquivalent(evaluation.optional_risk, reference_fragility)) {
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
		if (candidate.energy_valid && (!best.valid || MandatoryCandidatePreferred(candidate, best.candidate))) {
			best = addition;
		}
	}
	return best.valid ? best : fallback;
}

static void ApplyMandatoryConfiguration(QuerySLAEnergyQueryAllocation &target, const CandidateAddition &selection,
	                                    idx_t worker_count) {
	for (idx_t worker_index = 0; worker_index < worker_count; worker_index++) {
		QuerySLAEnergyWorkerAssignment worker;
		worker.valid = true;
		worker.hardware = selection.candidate.hardware;
		worker.safe_throughput = selection.aggregate_service_rate / static_cast<double>(worker_count);
		worker.active_power_w = selection.candidate.active_power_w;
		worker.normalized_edp = selection.candidate.normalized_edp;
		worker.profile_version = selection.candidate.profile_version;
		worker.worker_ordinal = worker_index;
		worker.mandatory = true;
		target.workers.push_back(worker);
	}
	target.aggregate_service_rate = selection.aggregate_service_rate;
	target.aggregate_active_power_w = selection.aggregate_power_w;
	target.predicted_epoch_energy_j = selection.epoch_energy_j;
	target.predicted_sla_cost = selection.evaluation.mandatory_cost;
	target.predicted_optional_risk = selection.evaluation.optional_risk;
	target.mandatory_target_sla_cost = selection.evaluation.mandatory_cost;
	target.mandatory_target_optional_risk = selection.evaluation.optional_risk;
	target.mandatory_preferred_service_rate = selection.aggregate_service_rate;
	target.mandatory_reference_safe_throughput =
	    worker_count > 0 ? selection.aggregate_service_rate / static_cast<double>(worker_count) : 0;
	target.mandatory_preferred_normalized_edp = selection.candidate.normalized_edp;
	target.mandatory_profile_version = selection.candidate.profile_version;
	target.mandatory_workers = worker_count;
}

} // namespace

QuerySLAModel BuildQuerySLAEnergyReferenceModel(const QuerySLAEnergyModelInput &input) {
	if (input.reference_model.event_kind != QueryActivationEventKind::PIPELINE) {
		return input.reference_model;
	}
	auto candidates = BuildCandidates(input);
	auto reference = std::find_if(candidates.begin(), candidates.end(),
	                              [](const HardwareCandidate &candidate) { return candidate.reference; });
	if (reference == candidates.end()) {
		return input.reference_model;
	}
	auto model_input = input.sla_input;
	auto demand_cap = input.reference_model.demand_cap;
	model_input.service_rates.resize(demand_cap + 1);
	for (idx_t workers = 0; workers <= demand_cap; workers++) {
		model_input.service_rates[workers] = static_cast<double>(workers) * reference->throughput;
	}
	return BuildQuerySLAModel(model_input);
}

QuerySLAEnergyWorkerAssignment SelectQuerySLAMandatoryAddition(const QuerySLAEnergyModelInput &input,
	                                                           const QuerySLAModel &reference_model,
	                                                           double existing_service_rate,
	                                                           idx_t existing_worker_count) {
	QuerySLAEnergyWorkerAssignment result;
	if (!reference_model.valid || existing_worker_count >= reference_model.demand_cap) {
		return result;
	}
	auto candidates = BuildCandidates(input);
	auto selection =
	    SelectMandatoryAddition(input, candidates, reference_model, existing_service_rate, existing_worker_count);
	if (!selection.valid) {
		return result;
	}
	auto reference = std::find_if(candidates.begin(), candidates.end(),
	                              [](const HardwareCandidate &candidate) { return candidate.reference; });
	if (reference == candidates.end()) {
		return result;
	}
	auto before = QuerySLAEvaluateServiceRate(input.sla_input, existing_service_rate, existing_worker_count);
	auto reference_increment = reference_model.service_rates[existing_worker_count + 1] -
	                           reference_model.service_rates[existing_worker_count];
	auto after = QuerySLAEvaluateServiceRate(input.sla_input,
	                                         existing_service_rate + MaxValue<double>(0, reference_increment),
	                                         existing_worker_count + 1);
	if (!before.valid || !after.valid) {
		return result;
	}
	result.valid = true;
	result.hardware = selection.candidate.hardware;
	result.safe_throughput =
	    MaxValue<double>(0, selection.aggregate_service_rate - existing_service_rate);
	result.active_power_w = selection.candidate.active_power_w;
	result.normalized_edp = selection.candidate.normalized_edp;
	result.profile_version = selection.candidate.profile_version;
	result.worker_ordinal = existing_worker_count;
	result.mandatory_gain = before.mandatory_cost - after.mandatory_cost;
	result.mandatory = true;
	return result;
}

QuerySLAEnergyWorkerAssignment SelectQuerySLAOptionalAddition(
    const QuerySLAEnergyModelInput &input, double existing_service_rate, double existing_optional_risk,
    idx_t existing_worker_count, const QueryRequestHardwareConfiguration &reference_hardware,
    const vector<QueryRequestHardwareConfiguration> &allowed_hardware, QuerySLAEnergyOptionalStage stage) {
	QuerySLAEnergyWorkerAssignment result;
	if (!input.reference_model.valid || existing_worker_count >= input.reference_model.demand_cap) {
		return result;
	}
	auto fixed_reference_fallback =
	    stage == QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK &&
	    SameHardware(reference_hardware, input.reference_hardware);
	auto acceptable_gain = [&](double gain) {
		if (gain > 0) {
			return true;
		}
		auto tolerance = 1e-9 * MaxValue<double>(1.0, std::fabs(existing_optional_risk));
		return fixed_reference_fallback && gain >= -tolerance;
	};
	if (UsesWorkerPresenceModel(input)) {
		auto next_worker_count = existing_worker_count + 1;
		if (next_worker_count >= input.reference_model.optional_risk.size()) {
			return result;
		}
		auto target_risk = input.reference_model.optional_risk[next_worker_count];
		auto gain = existing_optional_risk - target_risk;
		if (!acceptable_gain(gain)) {
			return result;
		}
		result.valid = true;
		result.hardware = reference_hardware;
		result.optional_fragility_before = existing_optional_risk;
		result.optional_fragility_target = target_risk;
		result.optional_fragility_after = target_risk;
		result.optional_gain = MaxValue<double>(0, gain);
		result.worker_ordinal = existing_worker_count;
		result.optional_stage = stage;
		result.optional = true;
		return result;
	}
	auto candidates = BuildCandidates(input);
	auto find_candidate = [&](const QueryRequestHardwareConfiguration &hardware) -> const HardwareCandidate * {
		for (const auto &candidate : candidates) {
			if (SameHardware(candidate.hardware, hardware)) {
				return &candidate;
			}
		}
		return nullptr;
	};
	auto reference = find_candidate(reference_hardware);
	auto next_worker_count = existing_worker_count + 1;
	if (!reference || next_worker_count >= input.reference_model.service_rates.size()) {
		return result;
	}
	auto reference_increment =
	    SameHardware(reference->hardware, input.reference_hardware)
	        ? input.reference_model.service_rates[next_worker_count] -
	              input.reference_model.service_rates[existing_worker_count]
	        : reference->throughput;
	if (!(reference_increment > 0)) {
		return result;
	}
	auto reference_evaluation = QuerySLAEvaluateServiceRate(
	    input.sla_input, existing_service_rate + reference_increment, next_worker_count);
	if (!reference_evaluation.valid) {
		return result;
	}
	auto reference_risk_before = existing_optional_risk;
	auto gain = reference_risk_before - reference_evaluation.optional_risk;
	if (!acceptable_gain(gain)) {
		return result;
	}
	const HardwareCandidate *selected = nullptr;
	QuerySLAServiceEvaluation selected_evaluation;
	for (const auto &hardware : allowed_hardware) {
		auto candidate = find_candidate(hardware);
		if (!candidate) {
			continue;
		}
		auto candidate_increment =
		    SameHardware(candidate->hardware, reference_hardware) ? reference_increment : candidate->throughput;
		auto service_rate = existing_service_rate + candidate_increment;
		auto evaluation = QuerySLAEvaluateServiceRate(
		    input.sla_input, service_rate, next_worker_count);
		if (!evaluation.valid ||
		    !SLACostEquivalent(evaluation.mandatory_cost, reference_evaluation.mandatory_cost) ||
		    !SLACostEquivalent(evaluation.optional_risk, reference_evaluation.optional_risk)) {
			continue;
		}
		auto preferred = !selected || (candidate->energy_valid && !selected->energy_valid) ||
		                 (candidate->energy_valid == selected->energy_valid &&
		                  MandatoryCandidatePreferred(*candidate, *selected));
		if (preferred) {
			selected = candidate;
			selected_evaluation = evaluation;
		}
	}
	if (!selected) {
		return result;
	}
	result.valid = true;
	result.hardware = selected->hardware;
	result.safe_throughput =
	    SameHardware(selected->hardware, reference_hardware) ? reference_increment : selected->throughput;
	result.active_power_w = selected->active_power_w;
	result.normalized_edp = selected->normalized_edp;
	result.optional_fragility_before = reference_risk_before;
	result.optional_fragility_target = reference_evaluation.optional_risk;
	result.optional_fragility_after = selected_evaluation.optional_risk;
	result.optional_gain = MaxValue<double>(0, gain);
	result.profile_version = selected->profile_version;
	result.worker_ordinal = existing_worker_count;
	result.optional_stage = stage;
	result.optional = true;
	return result;
}

QuerySLAEnergyAllocation AllocateQuerySLAMandatoryWorkers(const vector<QuerySLAEnergyModelInput> &inputs,
	                                                      const vector<idx_t> &mandatory_targets) {
	QuerySLAEnergyAllocation result;
	if (mandatory_targets.size() != inputs.size()) {
		result.error = "SLA-energy mandatory targets do not match the query count";
		return result;
	}
	result.queries.resize(inputs.size());
	vector<QuerySLAModel> reference_models;
	reference_models.reserve(inputs.size());
	vector<vector<HardwareCandidate>> candidates;
	candidates.reserve(inputs.size());
	for (idx_t i = 0; i < inputs.size(); i++) {
		auto reference_model = inputs[i].reference_model;
		if (!reference_model.valid) {
			result.error = "SLA-energy mandatory reference model is unavailable";
			return result;
		}
		reference_models.push_back(reference_model);
		candidates.push_back(BuildCandidates(inputs[i]));
	}
	for (idx_t query_index = 0; query_index < inputs.size(); query_index++) {
		auto evaluation = QuerySLAEvaluateServiceRate(inputs[query_index].sla_input, 0, 0);
		if (evaluation.valid) {
			result.queries[query_index].predicted_sla_cost = evaluation.mandatory_cost;
			result.queries[query_index].predicted_optional_risk = evaluation.optional_risk;
		}
	}
	for (idx_t query_index = 0; query_index < inputs.size(); query_index++) {
		auto worker_count = mandatory_targets[query_index];
		if (worker_count > reference_models[query_index].demand_cap) {
			result.error = "SLA-energy mandatory target exceeds query demand";
			return result;
		}
		if (worker_count == 0) {
			continue;
		}
		auto selection =
		    SelectMandatoryConfiguration(inputs[query_index], candidates[query_index],
		                                 reference_models[query_index], worker_count);
		if (!selection.valid) {
			result.error = "SLA-energy mandatory hardware selection failed for query index " +
			               std::to_string(query_index) + " at worker count " + std::to_string(worker_count) +
			               " with " + std::to_string(candidates[query_index].size()) + " hardware candidates";
			return result;
		}
		ApplyMandatoryConfiguration(result.queries[query_index], selection, worker_count);
		result.queries[query_index].mandatory_target_sla_cost =
		    reference_models[query_index].mandatory_cost[worker_count];
		result.queries[query_index].mandatory_target_optional_risk =
		    reference_models[query_index].optional_risk[worker_count];
		result.queries[query_index].mandatory_reference_safe_throughput =
		    reference_models[query_index].service_rates[worker_count] / static_cast<double>(worker_count);
	}
	result.valid = true;
	return result;
}

} // namespace duckdb
