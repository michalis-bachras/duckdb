//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_model.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_model.hpp"

#include <cmath>
#include <algorithm>
#include <limits>
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
	double continuation_ns;
	if (work.continuation_estimate.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT) {
		continuation_ns = remaining_after_epoch * work.continuation_estimate.p90;
	} else if (work.continuation_estimate.kind == ContinuationEstimateKind::RAW_LATENCY_NS) {
		// A raw fallback represents the whole pipeline duration. Scale it by live progress so that assigning a
		// worker still produces a meaningful marginal gain when the pipeline cannot finish within this epoch.
		auto remaining_fraction = work.total_chunks_equiv > 0
		                              ? remaining_after_epoch / static_cast<double>(work.total_chunks_equiv)
		                              : 1.0;
		continuation_ns = remaining_fraction * work.continuation_estimate.p90;
	} else {
		return 0;
	}
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

DownstreamSuffixEstimate QuerySLALifecycleSuffixPointMass(uint64_t remaining_ns) {
	DownstreamSuffixEstimate result;
	result.valid = true;
	result.bucket_count = 1;
	result.buckets[0].probability = 1.0;
	result.buckets[0].lower_ns = static_cast<double>(remaining_ns);
	result.buckets[0].upper_ns = static_cast<double>(remaining_ns);
	if (remaining_ns > 0) {
		result.model = QuerySLASuffixModel::POINT;
	}
	return result;
}

bool QuerySLARequiresLifecycleWorker(const QuerySLAModel &model) {
	if (!model.valid || model.demand_cap == 0) {
		return false;
	}
	return model.event_kind == QueryActivationEventKind::INITIALIZE ||
	       model.event_kind == QueryActivationEventKind::PREPARE_FINISH ||
	       model.event_kind == QueryActivationEventKind::FINISH ||
	       model.event_kind == QueryActivationEventKind::COMPLETE;
}

bool QuerySLAHasCompletionForecast(const QuerySLAModel &model) {
	return model.valid && model.event_kind != QueryActivationEventKind::INITIALIZE;
}

QuerySLAServiceEvaluation QuerySLAEvaluateServiceRate(const QuerySLAModelInput &input,
	                                                  double aggregate_service_rate, idx_t workers) {
	QuerySLAServiceEvaluation result;
	auto work = input.work;
	if (input.total_package) {
		work.continuation_estimate.p90 = work.continuation_estimate.p95;
		work.lifecycle_tail_estimate.p90_ns = work.lifecycle_tail_estimate.p95_ns;
	}
	if (workers == 0 && aggregate_service_rate <= 0 && !work.selected_throughput_valid &&
	    input.service_rates.size() > 1 && input.service_rates[1] > 0) {
		// An explicit service curve fully models this event. The first positive rate is
		// only a validity witness here: a zero-worker epoch still makes no progress.
		work.selected_throughput_valid = true;
		work.selected_single_worker_chunks_per_s = input.service_rates[1];
	}
	result.current_pipeline_finish_ns = QuerySLACurrentPipelineFinishNs(
	    work, input.event_kind, aggregate_service_rate, workers > 0, input.now_ns, input.epoch_ns);
	if (result.current_pipeline_finish_ns <= 0 || !input.suffix.valid || input.suffix.bucket_count == 0) {
		return result;
	}
	auto downstream_budget_ns = static_cast<double>(input.metadata.deadline_ns) - result.current_pipeline_finish_ns;
	auto penalty_weight_per_ns = input.metadata.sla_penalty_per_s / 1000000000.0;
	if (input.total_package) {
		auto suffix95 = QuerySLASuffixQuantileNs(input.suffix, 0.95);
		result.mandatory_cost = penalty_weight_per_ns * MaxValue<double>(0, suffix95 - downstream_budget_ns);
		result.optional_risk = 0;
		result.valid = true;
		return result;
	}
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
	model.total_package = input.total_package;
	model.db_query_id = input.metadata.db_query_id;
	model.deadline_ns = input.metadata.deadline_ns;
	model.query_start_ns = input.metadata.query_start_ns;
	model.prior_service_ns = input.metadata.estimated_service_ns;
	model.model_now_ns = input.now_ns;
	model.lifecycle_tail_p90_ns = input.work.lifecycle_tail_estimate.valid ? input.work.lifecycle_tail_estimate.p90_ns : 0;
	model.suffix_p90_ns = QuerySLASuffixQuantileNs(input.suffix, 0.90);
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
		model.valid = false;
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
		model.optional_risk[workers] = input.total_package ? 0 : evaluation.optional_risk;
	}
	for (idx_t workers = 0; workers < model.demand_cap; workers++) {
		model.mandatory_gain[workers] = model.mandatory_cost[workers] - model.mandatory_cost[workers + 1];
		model.optional_gain[workers] = model.optional_risk[workers] - model.optional_risk[workers + 1];
	}
	model.valid = true;
	return model;
}


QuerySLAAllocation AllocateDeadlineBackfillWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                                 bool triage, bool shortest_remaining) {
	return AllocatePriorityBackfillWorkers(models, worker_budget,
	    shortest_remaining ? QuerySLABackfillPriority::SRPT :
	    triage ? QuerySLABackfillPriority::TRIAGE : QuerySLABackfillPriority::EDF);
}

QuerySLAAllocation AllocatePriorityBackfillWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                                  QuerySLABackfillPriority priority, uint64_t epoch_ns) {
	const bool triage = priority == QuerySLABackfillPriority::TRIAGE;
	const bool size_order = priority == QuerySLABackfillPriority::SRPT ||
	                        priority == QuerySLABackfillPriority::PSJF ||
	                        priority == QuerySLABackfillPriority::STABLE_SRPT ||
	                        priority == QuerySLABackfillPriority::MOORE ||
	                        priority == QuerySLABackfillPriority::BOUNDED_DP;
	QuerySLAAllocation result;
	result.mandatory.resize(models.size(), 0);
	result.optional.resize(models.size(), 0);
	result.residual.resize(models.size(), 0);
	struct Candidate {
		idx_t index;
		uint64_t deadline;
		double remaining_ns;
		bool potentially_on_time;
		idx_t priority_class;
		double rank_ns;
	};
	vector<Candidate> order;
	for (idx_t i = 0; i < models.size(); i++) {
		const auto &m = models[i];
		if (!m.valid || !m.demand_cap) continue;
		auto cap = MinValue(worker_budget, m.demand_cap);
		if (!cap) continue;
		double remaining_ns = m.lifecycle_tail_p90_ns + m.suffix_p90_ns;
		if ((m.event_kind == QueryActivationEventKind::PIPELINE ||
		     m.event_kind == QueryActivationEventKind::INTERNAL) && m.remaining_work_units) {
			auto rate = cap < m.service_rates.size() ? m.service_rates[cap] : 0;
			remaining_ns += rate > 0 && std::isfinite(rate)
			                    ? static_cast<double>(m.remaining_work_units) * 1e9 / rate
			                    : std::numeric_limits<double>::infinity();
		}
		auto deadline = m.deadline_ns ? m.deadline_ns : std::numeric_limits<uint64_t>::max();
		// This is individual feasibility at full capacity, not collective feasibility.
		// Unknown forecasts stay in the EDF class instead of being declared hopeless.
		bool possible = !std::isfinite(remaining_ns) || static_cast<double>(deadline) >=
		                    static_cast<double>(m.model_now_ns) + remaining_ns;
		double rank = priority == QuerySLABackfillPriority::PSJF ?
		              (m.prior_service_ns ? static_cast<double>(m.prior_service_ns) :
		                                    std::numeric_limits<double>::infinity()) : remaining_ns;
		idx_t priority_class = 0;
		if (priority == QuerySLABackfillPriority::STABLE_SRPT) {
			// Policy age may include waiting; remaining service above never subtracts it.
			const double age = m.query_start_ns && m.model_now_ns >= m.query_start_ns ?
			                   static_cast<double>(m.model_now_ns - m.query_start_ns) : 0;
			const double aging_limit = MaxValue<double>(2e9, 4.0 * m.prior_service_ns);
			const bool aged = age >= aging_limit;
			const bool urgent = !std::isfinite(remaining_ns) ||
			                    static_cast<double>(deadline) - m.model_now_ns <= remaining_ns + 2.0 * epoch_ns;
			priority_class = aged ? 0 : urgent ? 1 : 2;
			if (priority_class < 2) rank = static_cast<double>(deadline);
			else if (m.previous_workers > 0) rank *= 0.8;
			// Incumbency gives a 20% deadband. Aging is an empirical safeguard,
			// not a schedulability or starvation bound under arbitrary arrivals.
		}
		order.push_back({i, deadline, remaining_ns, possible, priority_class, rank});
	}
	const bool bounded_dp = priority == QuerySLABackfillPriority::BOUNDED_DP && order.size() <= 8 &&
	    std::all_of(order.begin(),order.end(),[](const Candidate &c) {
	        return std::isfinite(c.remaining_ns) && c.remaining_ns >= 0;
	    });
	if (bounded_dp) {
		// Exact only for the current serial-service surrogate: lexicographically
		// minimize predicted misses, tardiness, then flow time. At most 256 states;
		// use Moore fallback for larger/unknown frontiers. No future arrivals used.
		std::sort(order.begin(),order.end(),[&](const Candidate &a,const Candidate &b) {
			return models[a.index].db_query_id < models[b.index].db_query_id;
		});
		struct Cost {
			bool valid = false;
			idx_t misses = 0;
			long double tardiness = 0, flow = 0;
			uint64_t path = 0;
			idx_t last = 0;
		};
		const idx_t count = idx_t(1) << order.size();
		vector<Cost> cost(count);
		vector<long double> duration(count,0);
		cost[0].valid = true;
		for (idx_t mask = 1; mask < count; mask++) {
			for (idx_t j = 0; j < order.size(); j++) {
				if (mask & (idx_t(1)<<j)) {
					duration[mask] = duration[mask ^ (idx_t(1)<<j)] + order[j].remaining_ns;
					break;
				}
			}
		}
		for (idx_t mask = 0; mask < count; mask++) {
			if (!cost[mask].valid) continue;
			for (idx_t j = 0; j < order.size(); j++) {
				const idx_t bit = idx_t(1)<<j;
				if (mask & bit) continue;
				const idx_t next = mask | bit;
				const auto &c = order[j];
				const long double budget = static_cast<long double>(c.deadline) - models[c.index].model_now_ns;
				const long double late = MaxValue<long double>(0,duration[next]-budget);
				Cost candidate = cost[mask];
				candidate.misses += late > 0;
				candidate.tardiness += late;
				candidate.flow += duration[next];
				candidate.path = candidate.path*16+j+1;
				candidate.last = j;
				const auto &old = cost[next];
				const bool better = !old.valid || candidate.misses < old.misses ||
				    (candidate.misses == old.misses && (candidate.tardiness < old.tardiness ||
				    (candidate.tardiness == old.tardiness && (candidate.flow < old.flow ||
				    (candidate.flow == old.flow && candidate.path < old.path)))));
				if (better) cost[next] = candidate;
			}
		}
		idx_t mask = count-1;
		for (idx_t position = order.size(); position > 0; position--) {
			const idx_t j = cost[mask].last;
			order[j].priority_class = 0;
			order[j].rank_ns = static_cast<double>(position-1);
			mask ^= idx_t(1)<<j;
		}
	} else if (priority == QuerySLABackfillPriority::MOORE || priority == QuerySLABackfillPriority::BOUNDED_DP) {
		// Recompute an on-time subset for the currently known jobs. This adapts
		// Moore-Hodgson's single-machine rule; estimated service, future arrivals
		// and malleable backfill mean its optimality theorem does not apply here.
		std::sort(order.begin(), order.end(), [&](const Candidate &a, const Candidate &b) {
			if (a.deadline != b.deadline) return a.deadline < b.deadline;
			return models[a.index].db_query_id < models[b.index].db_query_id;
		});
		vector<bool> keep(order.size(), false);
		std::priority_queue<std::pair<double,idx_t>> longest;
		long double service_ns = 0;
		for (idx_t j = 0; j < order.size(); j++) {
			auto &c = order[j];
			if (!std::isfinite(c.remaining_ns) || c.remaining_ns < 0) continue;
			keep[j] = true;
			longest.push({c.remaining_ns,j});
			service_ns += c.remaining_ns;
			const long double budget_ns = static_cast<long double>(c.deadline) - models[c.index].model_now_ns;
			if (service_ns > budget_ns) {
				const auto drop = longest.top(); longest.pop();
				service_ns -= drop.first; keep[drop.second] = false;
			}
		}
		for (idx_t j = 0; j < order.size(); j++) {
			auto &c = order[j];
			const bool known = std::isfinite(c.remaining_ns) && c.remaining_ns >= 0;
			c.priority_class = !known ? 0 : keep[j] ? 1 : 2;
			c.rank_ns = c.priority_class < 2 ? static_cast<double>(c.deadline) : c.remaining_ns;
		}
	}
	std::sort(order.begin(), order.end(), [&](const Candidate &a, const Candidate &b) {
		if (size_order) {
			if (a.priority_class != b.priority_class) return a.priority_class < b.priority_class;
			// Unknown durations use deterministic EDF priority, never a NaN comparison.
			bool a_known = std::isfinite(a.rank_ns) && a.rank_ns >= 0;
			bool b_known = std::isfinite(b.rank_ns) && b.rank_ns >= 0;
			if (a_known != b_known) return !a_known;
			if (a_known && a.rank_ns != b.rank_ns) return a.rank_ns < b.rank_ns;
		}
		if (triage && a.potentially_on_time != b.potentially_on_time) return a.potentially_on_time;
		if (triage && !a.potentially_on_time && a.remaining_ns != b.remaining_ns)
			return a.remaining_ns < b.remaining_ns;
		if (a.deadline != b.deadline) return a.deadline < b.deadline;
		if (models[a.index].db_query_id != models[b.index].db_query_id)
			return models[a.index].db_query_id < models[b.index].db_query_id;
		return a.index < b.index;
	});
	for (const auto &q : order) {
		auto grant = MinValue(worker_budget, models[q.index].demand_cap);
		result.residual[q.index] = grant;
		worker_budget -= grant;
		if (!worker_budget) break;
	}
	result.unassigned_workers = worker_budget;
	return result;
}

QuerySLAAllocation AllocateCommittedSJFWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                             uint64_t &committed_query_id) {
	QuerySLAAllocation result;
	result.mandatory.resize(models.size(),0);result.optional.resize(models.size(),0);result.residual.resize(models.size(),0);
	vector<idx_t> order;
	for (idx_t i=0;i<models.size();i++) if (models[i].valid && models[i].demand_cap) order.push_back(i);
	std::sort(order.begin(),order.end(),[&](idx_t a,idx_t b) {
		const auto &x=models[a];const auto &y=models[b];
		const bool xp=committed_query_id && x.db_query_id==committed_query_id;
		const bool yp=committed_query_id && y.db_query_id==committed_query_id;
		if (xp!=yp) return xp;
		const auto xs=x.prior_service_ns ? x.prior_service_ns : std::numeric_limits<uint64_t>::max();
		const auto ys=y.prior_service_ns ? y.prior_service_ns : std::numeric_limits<uint64_t>::max();
		if (xs!=ys) return xs<ys;
		return x.db_query_id<y.db_query_id;
	});
	if (!committed_query_id && !order.empty()) committed_query_id=models[order[0]].db_query_id;
	for (auto i:order) {
		auto grant=MinValue(worker_budget,models[i].demand_cap);
		result.residual[i]=grant;worker_budget-=grant;
		if (!worker_budget) break;
	}
	result.unassigned_workers=worker_budget;
	return result;
}

QuerySLAAllocation AllocateQuerySLAWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
	                                       bool work_conserving, bool ensure_lifecycle_progress) {
	QuerySLAAllocation result;
	result.mandatory.resize(models.size(), 0);
	result.optional.resize(models.size(), 0);
	result.residual.resize(models.size(), 0);

	// Charge required serial execution to the same budget. A zero marginal SLA
	// gain must not park initialization until unrelated data pipelines finish.
	vector<idx_t> lifecycle;
	for (idx_t i = 0; i < models.size(); i++) {
		const auto &model = models[i];
		const bool total_package_progress = model.total_package && model.valid && model.demand_cap &&
		    ((model.event_kind != QueryActivationEventKind::PIPELINE &&
		      model.event_kind != QueryActivationEventKind::INTERNAL) || model.remaining_work_units == 0);
		if (total_package_progress || (ensure_lifecycle_progress && QuerySLARequiresLifecycleWorker(model))) {
			lifecycle.push_back(i);
		}
	}
	if (ensure_lifecycle_progress) {
		// More ready lifecycle events than workers: older queries make progress
		// first, independent of unordered-map capture order or new arrivals.
		std::sort(lifecycle.begin(), lifecycle.end(), [&](idx_t left, idx_t right) {
			if (models[left].query_start_ns != models[right].query_start_ns) {
				return models[left].query_start_ns < models[right].query_start_ns;
			}
			return models[left].db_query_id < models[right].db_query_id;
		});
	}
	for (auto index : lifecycle) {
		if (worker_budget == 0) {
			break;
		}
		result.mandatory[index] = 1;
		worker_budget--;
		QuerySLAAllocationToken token;
		token.query_index = index;
		token.worker_ordinal = 0;
		token.mandatory = true;
		result.tokens.push_back(token);
	}
	std::priority_queue<EpochHeapEntry> mandatory_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		if (models[i].valid && models[i].demand_cap > 0 && !models[i].mandatory_gain.empty() && result.mandatory[i] < models[i].mandatory_gain.size()) {
			auto n=result.mandatory[i]; mandatory_heap.emplace(models[i].mandatory_gain[n], i, n);
		}
	}
	while (worker_budget > 0 && !mandatory_heap.empty()) {
		auto top = mandatory_heap.top();
		mandatory_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		QuerySLAAllocationToken token;
		token.query_index = top.query_index;
		token.worker_ordinal = top.workers;
		token.gain = top.gain;
		token.mandatory = true;
		result.tokens.push_back(token);
		result.mandatory[top.query_index]++;
		worker_budget--;
		auto next = result.mandatory[top.query_index];
		if (next < models[top.query_index].demand_cap &&
		    next < models[top.query_index].mandatory_gain.size()) {
			mandatory_heap.emplace(models[top.query_index].mandatory_gain[next], top.query_index, next);
		}
	}

	std::priority_queue<EpochHeapEntry> optional_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		auto workers = result.mandatory[i];
		if (models[i].valid && workers < models[i].demand_cap && workers < models[i].optional_gain.size()) {
			optional_heap.emplace(models[i].optional_gain[workers], i, workers);
		}
	}
	while (worker_budget > 0 && !optional_heap.empty()) {
		auto top = optional_heap.top();
		optional_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		QuerySLAAllocationToken token;
		token.query_index = top.query_index;
		token.worker_ordinal = top.workers;
		token.gain = top.gain;
		token.optional = true;
		result.tokens.push_back(token);
		result.optional[top.query_index]++;
		worker_budget--;
		auto workers = result.mandatory[top.query_index] + result.optional[top.query_index];
		if (workers < models[top.query_index].demand_cap &&
		    workers < models[top.query_index].optional_gain.size()) {
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
