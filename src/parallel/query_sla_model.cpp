//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_model.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_model.hpp"

#include <cmath>
#include <queue>

namespace duckdb {

namespace {

static double SuffixCDF(const DownstreamSuffixEstimate &suffix, double value_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		if (bucket.lower_ns == bucket.upper_ns) {
			result += value_ns >= bucket.upper_ns ? bucket.probability : 0.0;
		} else if (value_ns >= bucket.upper_ns) {
			result += bucket.probability;
		} else if (value_ns > bucket.lower_ns) {
			result += bucket.probability * (value_ns - bucket.lower_ns) / (bucket.upper_ns - bucket.lower_ns);
		}
	}
	return result;
}

struct EpochHeapEntry {
	EpochHeapEntry(double gain_p, idx_t query_index_p, idx_t workers_p)
	    : gain(gain_p), query_index(query_index_p), workers(workers_p) {
	}

	double gain;
	idx_t query_index;
	idx_t workers;

	bool operator<(const EpochHeapEntry &other) const {
		if (gain != other.gain) {
			return gain < other.gain;
		}
		return query_index > other.query_index;
	}
};

struct ResidualEpochHeapEntry {
	ResidualEpochHeapEntry(double mandatory_gain_p, double optional_gain_p, idx_t query_index_p, idx_t workers_p)
	    : mandatory_gain(mandatory_gain_p), optional_gain(optional_gain_p), query_index(query_index_p),
	      workers(workers_p) {
	}

	double mandatory_gain;
	double optional_gain;
	idx_t query_index;
	idx_t workers;

	bool operator<(const ResidualEpochHeapEntry &other) const {
		if (mandatory_gain != other.mandatory_gain) {
			return mandatory_gain < other.mandatory_gain;
		}
		if (optional_gain != other.optional_gain) {
			return optional_gain < other.optional_gain;
		}
		return query_index > other.query_index;
	}
};

static double ServiceRate(const QuerySLAModelInput &input, idx_t workers) {
	if (input.event_kind != QueryActivationEventKind::PIPELINE &&
	    input.event_kind != QueryActivationEventKind::INTERNAL) {
		return 0;
	}
	if (!input.service_rates.empty()) {
		if (workers >= input.service_rates.size()) {
			return 0;
		}
		return input.service_rates[workers];
	}
	return static_cast<double>(workers) * input.work.selected_single_worker_chunks_per_s;
}

} // namespace

double QuerySLASuffixMeanNs(const DownstreamSuffixEstimate &suffix) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		result += bucket.probability * (bucket.lower_ns + bucket.upper_ns) / 2.0;
	}
	return result;
}

double QuerySLASuffixQuantileNs(const DownstreamSuffixEstimate &suffix, double quantile) {
	if (!suffix.valid || suffix.bucket_count == 0) {
		return 0;
	}
	double lower = suffix.buckets[0].lower_ns;
	double upper = suffix.buckets[0].upper_ns;
	for (idx_t i = 1; i < suffix.bucket_count; i++) {
		lower = MinValue<double>(lower, suffix.buckets[i].lower_ns);
		upper = MaxValue<double>(upper, suffix.buckets[i].upper_ns);
	}
	for (idx_t iteration = 0; iteration < 48; iteration++) {
		auto midpoint = (lower + upper) / 2.0;
		if (SuffixCDF(suffix, midpoint) >= quantile) {
			upper = midpoint;
		} else {
			lower = midpoint;
		}
	}
	return upper;
}

double QuerySLACurrentPipelineFinishNs(const PipelineWorkSnapshot &work, QueryActivationEventKind event_kind,
	                                   double aggregate_service_rate, bool has_worker, uint64_t now_ns,
	                                   uint64_t epoch_ns) {
	auto lifecycle_tail_ns = work.lifecycle_tail_estimate.valid ? work.lifecycle_tail_estimate.p90_ns : 0.0;
	auto modeled_work = event_kind == QueryActivationEventKind::PIPELINE ||
	                    event_kind == QueryActivationEventKind::INTERNAL;
	if (!modeled_work || work.remaining_chunks_equiv == 0) {
		return static_cast<double>(now_ns) + (!has_worker ? static_cast<double>(epoch_ns) : 0.0) + lifecycle_tail_ns;
	}
	if (aggregate_service_rate <= 0) {
		if (!work.selected_throughput_valid || work.selected_single_worker_chunks_per_s <= 0) {
			return 0;
		}
	} else if (!std::isfinite(aggregate_service_rate)) {
		return 0;
	}
	auto epoch_seconds = static_cast<double>(epoch_ns) / 1000000000.0;
	auto progress = aggregate_service_rate * epoch_seconds;
	if (has_worker && progress >= static_cast<double>(work.remaining_chunks_equiv)) {
		auto active_ns = static_cast<double>(work.remaining_chunks_equiv) * 1000000000.0 / aggregate_service_rate;
		return static_cast<double>(now_ns) + active_ns + lifecycle_tail_ns;
	}
	auto remaining_after_epoch = MaxValue<double>(0.0, static_cast<double>(work.remaining_chunks_equiv) - progress);
	if (!work.continuation_estimate.valid || work.continuation_estimate.p90 <= 0) {
		return 0;
	}
	auto continuation_ns = work.continuation_estimate.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT
	                           ? remaining_after_epoch * work.continuation_estimate.p90
	                           : work.continuation_estimate.p90;
	return static_cast<double>(now_ns) + static_cast<double>(epoch_ns) + continuation_ns + lifecycle_tail_ns;
}

double QuerySLAExpectedTardiness(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		double value;
		if (bucket.lower_ns == bucket.upper_ns) {
			value = MaxValue<double>(0.0, bucket.lower_ns - downstream_budget_ns);
		} else if (downstream_budget_ns >= bucket.upper_ns) {
			value = 0;
		} else if (downstream_budget_ns > bucket.lower_ns) {
			auto late_width = bucket.upper_ns - downstream_budget_ns;
			value = (late_width * late_width) / (2.0 * (bucket.upper_ns - bucket.lower_ns));
		} else {
			value = (bucket.lower_ns + bucket.upper_ns) / 2.0 - downstream_budget_ns;
		}
		result += bucket.probability * value;
	}
	return result;
}

double QuerySLAExpectedFragility(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns,
	                             double epoch_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		double value;
		if (bucket.lower_ns == bucket.upper_ns) {
			auto slack = MaxValue<double>(0.0, downstream_budget_ns - bucket.lower_ns);
			value = epoch_ns / (slack + epoch_ns);
		} else if (downstream_budget_ns <= bucket.lower_ns) {
			value = 1.0;
		} else if (downstream_budget_ns < bucket.upper_ns) {
			auto numerator = epoch_ns * std::log((downstream_budget_ns + epoch_ns - bucket.lower_ns) / epoch_ns) +
			                 bucket.upper_ns - downstream_budget_ns;
			value = numerator / (bucket.upper_ns - bucket.lower_ns);
		} else {
			auto numerator = epoch_ns *
			                 std::log((downstream_budget_ns + epoch_ns - bucket.lower_ns) /
			                          (downstream_budget_ns + epoch_ns - bucket.upper_ns));
			value = numerator / (bucket.upper_ns - bucket.lower_ns);
		}
		result += bucket.probability * value;
	}
	return result;
}

DownstreamSuffixEstimate QuerySLALifecycleSuffixPointMass() {
	DownstreamSuffixEstimate result;
	result.valid = true;
	result.bucket_count = 1;
	result.buckets[0].probability = 1.0;
	return result;
}

QuerySLAServiceEvaluation QuerySLAEvaluateServiceRate(const QuerySLAModelInput &input,
	                                                  double aggregate_service_rate, idx_t workers) {
	QuerySLAServiceEvaluation result;
	result.current_pipeline_finish_ns = QuerySLACurrentPipelineFinishNs(
	    input.work, input.event_kind, aggregate_service_rate, workers > 0, input.now_ns, input.epoch_ns);
	if (result.current_pipeline_finish_ns <= 0 || !input.suffix.valid || input.suffix.bucket_count == 0) {
		return result;
	}
	auto downstream_budget_ns = static_cast<double>(input.metadata.deadline_ns) - result.current_pipeline_finish_ns;
	auto penalty_weight_per_ns = input.metadata.sla_penalty_per_s / 1000000000.0;
	result.mandatory_cost =
	    penalty_weight_per_ns * QuerySLAExpectedTardiness(input.suffix, downstream_budget_ns);
	result.optional_risk = input.metadata.sla_penalty_per_s *
	                       QuerySLAExpectedFragility(input.suffix, downstream_budget_ns,
	                                                  static_cast<double>(input.epoch_ns));
	result.valid = true;
	return result;
}

QuerySLAModel BuildProvisionalQuerySLAModel(const QuerySLAModelInput &input) {
	QuerySLAModel model;
	model.db_query_id = input.metadata.db_query_id;
	model.generation = input.generation;
	model.pipeline_id = input.work.pipeline_id;
	model.event_kind = input.event_kind;
	model.demand_cap = input.work.parallelism_valid ? input.work.preferred_parallelism : 0;
	model.remaining_work_units = input.work.remaining_chunks_equiv;
	model.throughput = input.work.selected_single_worker_chunks_per_s;
	model.throughput_is_live = input.work.selected_throughput_is_live;
	if (model.event_kind != QueryActivationEventKind::PIPELINE &&
	    model.event_kind != QueryActivationEventKind::INTERNAL) {
		model.demand_cap = MinValue<idx_t>(idx_t(1), model.demand_cap);
	}
	if (model.demand_cap == 0) {
		model.valid = true;
		return model;
	}
	auto modeled_work = model.event_kind == QueryActivationEventKind::PIPELINE ||
	                    model.event_kind == QueryActivationEventKind::INTERNAL;
	if (modeled_work && input.work.remaining_chunks_equiv > 0 && !input.work.selected_throughput_valid &&
	    input.service_rates.empty()) {
		model.error = "active pipeline event has no compatible historical or live throughput profile";
		return model;
	}
	if (!input.service_rates.empty() && input.service_rates.size() <= model.demand_cap) {
		model.error = "active pipeline event has an incomplete worker service-rate curve";
		return model;
	}
	if (modeled_work && input.work.remaining_chunks_equiv > 0 && !input.work.continuation_estimate.valid) {
		model.error = "active pipeline event has no compatible continuation profile";
		return model;
	}
	model.valid = true;
	return model;
}

QuerySLAModel BuildQuerySLAModel(const QuerySLAModelInput &input) {
	auto model = BuildProvisionalQuerySLAModel(input);
	if (!model.valid || model.demand_cap == 0) {
		return model;
	}
	if (!input.suffix.valid || input.suffix.bucket_count == 0) {
		model.error = "active data pipeline has no trained downstream-suffix histogram";
		return model;
	}

	model.service_rates.resize(model.demand_cap + 1);
	model.mandatory_cost.resize(model.demand_cap + 1);
	model.optional_risk.resize(model.demand_cap + 1);
	model.mandatory_gain.resize(model.demand_cap);
	model.optional_gain.resize(model.demand_cap);
	for (idx_t workers = 0; workers <= model.demand_cap; workers++) {
		model.service_rates[workers] = ServiceRate(input, workers);
		auto evaluation = QuerySLAEvaluateServiceRate(input, model.service_rates[workers], workers);
		if (!evaluation.valid) {
			model.valid = false;
			model.error = "active pipeline completion model is unavailable";
			return model;
		}
		model.mandatory_cost[workers] = evaluation.mandatory_cost;
		model.optional_risk[workers] = evaluation.optional_risk;
	}
	for (idx_t workers = 0; workers < model.demand_cap; workers++) {
		model.mandatory_gain[workers] = model.mandatory_cost[workers] - model.mandatory_cost[workers + 1];
		model.optional_gain[workers] = model.optional_risk[workers] - model.optional_risk[workers + 1];
	}
	model.valid = true;
	return model;
}

QuerySLAAllocation AllocateQuerySLAWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
	                                       bool work_conserving) {
	QuerySLAAllocation result;
	result.mandatory.resize(models.size(), 0);
	result.optional.resize(models.size(), 0);
	result.residual.resize(models.size(), 0);

	std::priority_queue<EpochHeapEntry> mandatory_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		if (models[i].valid && models[i].demand_cap > 0 && !models[i].mandatory_gain.empty()) {
			mandatory_heap.emplace(models[i].mandatory_gain[0], i, 0);
		}
	}
	while (worker_budget > 0 && !mandatory_heap.empty()) {
		auto top = mandatory_heap.top();
		mandatory_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		result.mandatory[top.query_index]++;
		worker_budget--;
		auto next = result.mandatory[top.query_index];
		if (next < models[top.query_index].demand_cap) {
			mandatory_heap.emplace(models[top.query_index].mandatory_gain[next], top.query_index, next);
		}
	}

	std::priority_queue<EpochHeapEntry> optional_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		auto workers = result.mandatory[i];
		if (models[i].valid && workers < models[i].demand_cap) {
			optional_heap.emplace(models[i].optional_gain[workers], i, workers);
		}
	}
	while (worker_budget > 0 && !optional_heap.empty()) {
		auto top = optional_heap.top();
		optional_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		result.optional[top.query_index]++;
		worker_budget--;
		auto workers = result.mandatory[top.query_index] + result.optional[top.query_index];
		if (workers < models[top.query_index].demand_cap) {
			optional_heap.emplace(models[top.query_index].optional_gain[workers], top.query_index, workers);
		}
	}

	if (work_conserving) {
		std::priority_queue<ResidualEpochHeapEntry> residual_heap;
		for (idx_t i = 0; i < models.size(); i++) {
			auto workers = result.mandatory[i] + result.optional[i];
			if (!models[i].valid || workers >= models[i].demand_cap ||
			    workers >= models[i].mandatory_gain.size() || workers >= models[i].optional_gain.size()) {
				continue;
			}
			residual_heap.emplace(models[i].mandatory_gain[workers], models[i].optional_gain[workers], i, workers);
		}
		while (worker_budget > 0 && !residual_heap.empty()) {
			auto top = residual_heap.top();
			residual_heap.pop();
			result.residual[top.query_index]++;
			worker_budget--;
			auto workers = result.mandatory[top.query_index] + result.optional[top.query_index] +
			               result.residual[top.query_index];
			if (workers < models[top.query_index].demand_cap &&
			    workers < models[top.query_index].mandatory_gain.size() &&
			    workers < models[top.query_index].optional_gain.size()) {
				residual_heap.emplace(models[top.query_index].mandatory_gain[workers],
				                      models[top.query_index].optional_gain[workers], top.query_index, workers);
			}
		}
	}
	result.unassigned_workers = worker_budget;
	return result;
}

} // namespace duckdb
