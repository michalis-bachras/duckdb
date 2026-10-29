#include "duckdb/parallel/query_total_package.hpp"
#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <fstream>
#include <sstream>
#include <tuple>
#include <mutex>

namespace duckdb {

vector<QueryRequestPipelineHardwareProfileEstimate> LoadTotalPackageIsolatedProfiles(const string &path) {
    vector<QueryRequestPipelineHardwareProfileEstimate> result;
    if (path.empty()) return result;
    static std::mutex cache_mutex;
    static std::map<string,vector<QueryRequestPipelineHardwareProfileEstimate>> cache;
    std::lock_guard<std::mutex> cache_guard(cache_mutex);
    auto existing=cache.find(path);if(existing!=cache.end()) return existing->second;
    std::ifstream input(path); if (!input.good()) throw std::runtime_error("Cannot read isolated pipeline calibration: "+path);
    using Key=std::tuple<uint64_t,uint64_t,uint64_t,uint64_t,uint32_t>;
    struct Aggregate { QueryRequestPipelineHardwareProfileEstimate p; double rate=0,power=0;idx_t n=0; };
    std::map<Key,Aggregate> groups;string line;
    while (std::getline(input,line)) {
        if (!line.empty() && line.back()=='\r') line.pop_back();
        if (line.empty() || line.find("template_id")==0) continue;
        std::stringstream row(line);vector<string> v;string text;
        while(std::getline(row,text,','))v.push_back(text);
        if(v.size()!=12)throw std::runtime_error("Invalid isolated calibration row");
        if(v[11]!="1")continue;
        auto q=std::stoull(v[0]),sf=std::stoull(v[1]),pipeline=std::stoull(v[2]),signature=std::stoull(v[3]);
        uint32_t f=uint32_t(std::stoul(v[4]));
        auto units=std::stod(v[6]),worker_ns=std::stod(v[7]),wall_ns=std::stod(v[8]);
        auto energy=std::stod(v[9]),baseline=std::stod(v[10]);
        if(!std::isfinite(units)||!std::isfinite(worker_ns)||!std::isfinite(wall_ns)||!std::isfinite(energy)||!std::isfinite(baseline)||units<=0||worker_ns<=0||wall_ns<=0||energy<0||baseline<0)
            throw std::runtime_error("Invalid isolated calibration units");
        auto &a=groups[Key(q,sf,pipeline,signature,f)];
        a.p.template_id=q;a.p.scale_factor=sf;a.p.pipeline_id=pipeline;a.p.pipeline_signature_hash=signature;
        a.p.hardware.core_frequency_khz=f;a.p.hardware.uncore_frequency_khz=uint32_t(std::stoul(v[5]));
        a.rate+=units*1e9/worker_ns;a.power+=energy*1e9/wall_ns-baseline;a.n++;
    }
    for(auto &entry:groups) {
        auto &a=entry.second;a.p.valid=true;a.p.mature=a.n>=15;a.p.profile_version=12;
        a.p.throughput_sample_count=a.n;a.p.power_sample_count=a.n;
        a.p.mean_work_units_per_s=a.rate/a.n;a.p.safe_work_units_per_s=a.rate/a.n;
        // Signed, matched-baseline work power. No clamping or double counting activation energy.
        a.p.mean_active_power_w=a.power/a.n;
        result.push_back(a.p);
    }
    cache[path]=result;
    return result;
}

double TotalPackageEfficiency(double service, double energy) {
	return std::isfinite(service) && std::isfinite(energy) && service >= 0 && energy > 0 ? service / energy : 0;
}

bool TotalPackagePreferCandidate(double service, double energy, double reference_service, double reference_energy,
                                double margin) {
	auto reference = TotalPackageEfficiency(reference_service, reference_energy);
	return reference > 0 && std::isfinite(margin) && margin >= 0 &&
	       TotalPackageEfficiency(service, energy) > reference * (1 + margin);
}

bool TotalPackageDiminishingBenefits(const vector<double> &costs) {
	double previous = std::numeric_limits<double>::infinity();
	for (idx_t i = 1; i < costs.size(); i++) {
		auto gain = costs[i - 1] - costs[i];
		if (!std::isfinite(gain) || gain < -1e-10 || gain > previous + 1e-10) {
			return false;
		}
		previous = gain;
	}
	return true;
}

double TotalPackageExactAllocationCost(const vector<vector<double>> &costs, idx_t budget) {
	vector<double> dp(budget + 1, std::numeric_limits<double>::infinity());
	dp[0] = 0;
	for (const auto &query : costs) {
		vector<double> next(budget + 1, std::numeric_limits<double>::infinity());
		for (idx_t used = 0; used <= budget; used++) {
			for (idx_t grant = 0; grant < query.size() && used + grant <= budget; grant++) {
				next[used + grant] = std::min(next[used + grant], dp[used] + query[grant]);
			}
		}
		dp = std::move(next);
	}
	return *std::min_element(dp.begin(), dp.end());
}

TotalPackageProjection ProjectTotalPackageChain(const vector<TotalPackageStage> &stages, idx_t lease,
                                               const vector<idx_t> &frequencies, double epoch_s,
                                               double idle_w, double awake_w, const vector<double> &core_w) {
	TotalPackageProjection result;
	if (!std::isfinite(epoch_s) || epoch_s <= 0 || !std::isfinite(idle_w) || idle_w < 0 ||
	    !std::isfinite(awake_w) || awake_w < 0 || frequencies.size() < lease) {
		result.reason = "invalid_projection_input";
		return result;
	}
	result.idle_j = epoch_s * idle_w;
	double remaining = epoch_s;
	for (const auto &stage : stages) {
		lease = std::min(lease, stage.parallelism_cap);
		if (lease == 0 || remaining <= 0) {
			break;
		}
		double rate = 0;
		double work_power = 0;
		double activation = 0;
		for (idx_t w = 0; w < lease; w++) {
			auto f = frequencies[w];
			if (f >= stage.relative_rates.size() || f >= stage.work_j_per_reference_s.size() ||
			    f >= core_w.size() || !std::isfinite(stage.relative_rates[f]) || stage.relative_rates[f] <= 0 ||
			    !std::isfinite(stage.work_j_per_reference_s[f]) || !std::isfinite(core_w[f]) || core_w[f] < 0) {
				result.reason = "missing_stage_profile";
				return result;
			}
			rate += stage.relative_rates[f];
			work_power += stage.relative_rates[f] * stage.work_j_per_reference_s[f];
			// Sorted adjacent SMT packing: physical activation is charged once per pair.
			if ((w % 2) == 0) {
				auto realized = w + 1 < lease ? std::max(f, frequencies[w + 1]) : f;
				if (realized >= core_w.size()) {
					result.reason = "invalid_smt_frequency";
					return result;
				}
				activation += core_w[realized];
			}
		}
		if (!std::isfinite(stage.remaining_reference_work_s) || stage.remaining_reference_work_s < 0) {
			result.reason = "invalid_remaining_work";
			return result;
		}
		auto duration = std::min(remaining, stage.remaining_reference_work_s / rate);
		result.useful_reference_s += rate * duration;
		result.socket_j += awake_w * duration;
		result.core_j += activation * duration;
		result.work_j += work_power * duration;
		remaining -= duration;
		if (duration * rate + 1e-12 >= stage.remaining_reference_work_s) {
			result.completed_stages++;
		} else {
			break;
		}
	}
	result.final_lease = lease;
	result.total_package_j = result.idle_j + result.socket_j + result.core_j + result.work_j;
	result.valid = std::isfinite(result.total_package_j) && result.total_package_j > 0;
	result.reason = result.valid ? "ok" : "invalid_total_package_energy";
	return result;
}

namespace {
struct Request {
	idx_t query;
	idx_t ordinal;
	uint32_t frequency;
};

struct Evaluation {
	QuerySLAEnergyEpochPlan plan;
	double service = 0;
	double energy = 0;
	double cost = 0;
	bool modeled = true;
	string reason = "ok";
	vector<uint32_t> physical_state;
};

const QueryRequestPipelineHardwareProfileEstimate *Profile(const QuerySLAEnergyModelInput &q, uint32_t f) {
	for (const auto &p : q.hardware_profiles) {
		if (p.hardware.core_frequency_khz == f &&
		    p.hardware.uncore_frequency_khz == q.reference_hardware.uncore_frequency_khz && p.mature &&
		    p.safe_work_units_per_s > 0) {
			return &p;
		}
	}
	return nullptr;
}

const TotalPackageSocketCalibration *Calibration(const vector<TotalPackageSocketCalibration> &all, int socket) {
	for (const auto &c : all) {
		if (c.socket_id == socket) {
			return &c;
		}
	}
	return nullptr;
}

Evaluation Evaluate(const QuerySLAEnergyPlannerInput &input, const vector<uint32_t> &preferences,
                    const vector<TotalPackageSocketCalibration> &calibration) {
	Evaluation out;
	out.plan.queries.resize(input.queries.size());
	vector<Request> requests;
	for (idx_t q = 0; q < input.queries.size(); q++) {
		auto n = input.logical_allocation.mandatory[q];
		for (idx_t w = 0; w < n; w++) {
			requests.push_back({q, w, preferences[q]});
		}
	}
	std::stable_sort(requests.begin(), requests.end(), [](const Request &a, const Request &b) {
		return a.frequency < b.frequency;
	});
	// Socket capacity is discovered; fill the larger available socket first, then physical-core SMT pairs.
	std::map<int, std::map<int, vector<QueryHardwareWorkerTopology>>> sockets;
	for (const auto &w : input.topology) {
		sockets[w.socket_id][w.physical_core_id].push_back(w);
	}
	vector<int> socket_order;
	for (const auto &s : sockets) {
		socket_order.push_back(s.first);
	}
	auto capacity = [&](int s) {
		idx_t n = 0;
		for (const auto &c : sockets[s]) {
			n += c.second.size();
		}
		return n;
	};
	std::stable_sort(socket_order.begin(), socket_order.end(), [&](int a, int b) { return capacity(a) > capacity(b); });
	vector<QueryHardwareWorkerTopology> ordered;
	for (auto s : socket_order) {
		for (auto &core : sockets[s]) {
			std::sort(core.second.begin(), core.second.end(), [](const QueryHardwareWorkerTopology &a,
			                                                   const QueryHardwareWorkerTopology &b) {
				return a.logical_cpu < b.logical_cpu;
			});
			for (const auto &w : core.second) {
				ordered.push_back(w);
			}
		}
	}
	if (ordered.size() < requests.size()) {
		out.plan.error = "insufficient_registered_worker_topology";
		return out;
	}
	std::map<std::pair<int, int>, uint32_t> core_frequency;
	for (idx_t i = 0; i < requests.size(); i++) {
		auto key = std::make_pair(ordered[i].socket_id, ordered[i].physical_core_id);
		core_frequency[key] = std::max(core_frequency[key], requests[i].frequency);
	}
	vector<double> rates(input.queries.size(), 0), work_power(input.queries.size(), 0);
	for (idx_t i = 0; i < requests.size(); i++) {
		auto &r = requests[i];
		auto &q = input.queries[r.query];
		auto f = core_frequency[std::make_pair(ordered[i].socket_id, ordered[i].physical_core_id)];
		auto p = Profile(q, f);
		auto rate = f == q.reference_hardware.core_frequency_khz ? q.reference_model.throughput
		                                                       : p ? p->safe_work_units_per_s : 0;
		if (rate <= 0) {
			// Lifecycle events still need their serial task, without pretending to retire pipeline work.
			if (q.sla_input.work.remaining_chunks_equiv > 0) {
				out.modeled = false;
				out.reason = "missing_candidate_throughput";
			}
		}
		QuerySLAEnergyWorkerAssignment a;
		a.valid = true;
		a.hardware.core_frequency_khz = f;
		a.hardware.uncore_frequency_khz = q.reference_hardware.uncore_frequency_khz;
		a.safe_throughput = rate;
		a.active_power_w = p ? p->mean_active_power_w : 0;
		a.mandatory = true;
		a.worker_ordinal = r.ordinal;
		a.profile_version = p ? p->profile_version : 0;
		out.plan.queries[r.query].workers.push_back(a);
		QuerySLAEnergyPlacedWorker placed;
		placed.worker_id = ordered[i].worker_id;
		placed.query_index = r.query;
		placed.worker_ordinal = r.ordinal;
		placed.preferred_hardware.core_frequency_khz = r.frequency;
		placed.preferred_hardware.uncore_frequency_khz = q.reference_hardware.uncore_frequency_khz;
		placed.normal_hardware = a.hardware;
		placed.execution_hardware = a.hardware;
		placed.preferred_safe_throughput = rate;
		placed.normal_safe_throughput = rate;
		placed.preferred_active_power_w = a.active_power_w;
		placed.normal_active_power_w = a.active_power_w;
		placed.mandatory = true;
		out.plan.workers.push_back(placed);
		rates[r.query] += rate;
		work_power[r.query] += a.active_power_w;
	}
	auto seconds = input.epoch_ns / 1e9;
	for (const auto &c : calibration) {
		out.energy += c.idle_w * seconds;
	}
	if (calibration.size() != 2) {
		out.modeled = false;
		out.reason = "missing_both_package_calibration";
	}
	std::map<std::pair<int, int>, double> core_duration;
	std::map<int, double> socket_duration;
	for (idx_t i = 0; i < input.queries.size(); i++) {
		const auto &q = input.queries[i];
		auto &a = out.plan.queries[i];
		a.mandatory_workers = input.logical_allocation.mandatory[i];
		a.aggregate_service_rate = rates[i];
		a.aggregate_active_power_w = work_power[i];
		auto e = QuerySLAEvaluateServiceRate(q.sla_input, rates[i], a.mandatory_workers);
		a.predicted_sla_cost = e.valid ? e.mandatory_cost : 0;
		a.mandatory_target_sla_cost = a.mandatory_workers < q.reference_model.mandatory_cost.size()
		                                  ? q.reference_model.mandatory_cost[a.mandatory_workers] : 0;
		if (!e.valid || a.predicted_sla_cost > a.mandatory_target_sla_cost + 1e-9) {
			out.modeled = false;
			out.reason = "realized_sla_regression";
		}
		out.cost += a.predicted_sla_cost;
		double duration = a.mandatory_workers ? seconds : 0;
		if (rates[i] > 0 && q.sla_input.work.remaining_chunks_equiv > 0) {
			duration = std::min(seconds, q.sla_input.work.remaining_chunks_equiv / rates[i]);
			out.service += rates[i] * duration / q.reference_model.throughput;
			// Runtime successors must be proven before counting their future work. Do not invent a chain by ID.
			if (duration < seconds && QuerySLASuffixQuantileNs(q.sla_input.suffix, 0.95) > 0) {
				out.modeled = false;
				out.reason = "successor_projection_not_available";
			}
		}
		out.energy += work_power[i] * duration;
		for (idx_t w = 0; w < out.plan.workers.size(); w++) {
			if (out.plan.workers[w].query_index == i) {
				auto worker_id = out.plan.workers[w].worker_id;
				for (const auto &t : input.topology) {
					if (t.worker_id == worker_id) {
						auto key = std::make_pair(t.socket_id, t.physical_core_id);
						core_duration[key] = std::max(core_duration[key], duration);
						socket_duration[t.socket_id] = std::max(socket_duration[t.socket_id], duration);
					}
				}
			}
		}
	}
	for (const auto &s : socket_duration) {
		auto c = Calibration(calibration, s.first);
		if (!c) {
			out.modeled = false;
			continue;
		}
		out.energy += c->awake_w * s.second;
	}
	for (const auto &c : core_duration) {
		auto calibration_row = Calibration(calibration, c.first.first);
		auto f = core_frequency[c.first];
		auto entry = std::find(input.core_frequency_levels_khz.begin(), input.core_frequency_levels_khz.end(), f);
		auto index = idx_t(entry - input.core_frequency_levels_khz.begin());
		if (!calibration_row || index >= calibration_row->physical_core_w.size()) {
			out.modeled = false;
			if (out.reason == "ok") out.reason = "missing_physical_core_calibration";
		} else {
			out.energy += calibration_row->physical_core_w[index] * c.second;
		}
		out.physical_state.push_back(uint32_t(c.first.first));
		out.physical_state.push_back(uint32_t(c.first.second));
		out.physical_state.push_back(f);
	}
	out.plan.diagnostics.mandatory_workers = requests.size();
	out.plan.diagnostics.parked_workers = input.worker_budget - requests.size();
	out.plan.valid = true;
	return out;
}
} // namespace

QuerySLAEnergyEpochPlan PlanTotalPackageEpoch(const QuerySLAEnergyPlannerInput &input,
                                            const vector<TotalPackageSocketCalibration> &calibration,
                                            bool reference_only, bool lowest_safe, double margin,
                                            vector<TotalPackageCandidateTrace> &trace, bool shadow) {
	if (input.queries.size() != input.logical_allocation.mandatory.size()) {
		QuerySLAEnergyEpochPlan bad;
		bad.error = "reference_grant_count_mismatch";
		return bad;
	}
	vector<uint32_t> reference, lowest;
	for (idx_t i = 0; i < input.queries.size(); i++) {
		const auto &q = input.queries[i];
		auto f = q.reference_hardware.core_frequency_khz;
		reference.push_back(f);
		auto grant = input.logical_allocation.mandatory[i];
		if (grant >= q.reference_model.mandatory_cost.size()) {
			lowest.push_back(f);
			continue;
		}
		for (auto level : input.core_frequency_levels_khz) {
			auto p = Profile(q, level);
			if (!p || level > f) {
				continue;
			}
			auto e = QuerySLAEvaluateServiceRate(q.sla_input, grant * p->safe_work_units_per_s, grant);
			if (e.valid && e.mandatory_cost <= q.reference_model.mandatory_cost[grant] + 1e-9) {
				f = level;
				break;
			}
		}
		lowest.push_back(f);
	}
	vector<vector<uint32_t>> candidates {reference};
	if (lowest != reference) {
		candidates.push_back(lowest);
	}
	// Include intermediate safe levels, even when the lowest safe preference is 1.2 GHz.
	for (auto level : input.core_frequency_levels_khz) {
		auto all = reference;
		for (idx_t i = 0; i < reference.size(); i++) {
			if (level >= lowest[i] && level < reference[i]) all[i] = level;
		}
		if (all != reference && candidates.size() < 32 &&
		    std::find(candidates.begin(), candidates.end(), all) == candidates.end()) candidates.push_back(all);
	}
	for (idx_t i = 0; i < reference.size() && candidates.size() < 32; i++) {
		for (auto level : input.core_frequency_levels_khz) {
			if (level < lowest[i] || level >= reference[i] || candidates.size() >= 32) continue;
			auto c = reference; c[i] = level;
			if (std::find(candidates.begin(), candidates.end(), c) == candidates.end()) candidates.push_back(std::move(c));
		}
	}
	auto baseline = Evaluate(input, reference, calibration);
	auto best = baseline;
	vector<vector<double>> oracle_costs;
	idx_t reserved = 0, non_diminishing = 0;
	double greedy_cost = 0;
	for (idx_t i = 0; i < input.queries.size(); i++) {
		const auto &m = input.queries[i].reference_model;
		if (m.mandatory_cost.empty()) { oracle_costs.push_back({0}); continue; }
		if (!TotalPackageDiminishingBenefits(m.mandatory_cost)) non_diminishing++;
		auto grant = input.logical_allocation.mandatory[i];
		greedy_cost += m.mandatory_cost[grant];
		auto control = m.demand_cap && ((m.event_kind != QueryActivationEventKind::PIPELINE &&
		    m.event_kind != QueryActivationEventKind::INTERNAL) || m.remaining_work_units == 0) ? idx_t(1) : idx_t(0);
		reserved += control;
		oracle_costs.emplace_back(m.mandatory_cost.begin() + control, m.mandatory_cost.end());
	}
	// A small exact diagnostic never changes the production greedy allocation.
	auto exact_cost = input.queries.size() <= 16 && input.worker_budget <= 16 && reserved <= input.worker_budget
	    ? TotalPackageExactAllocationCost(oracle_costs, input.worker_budget - reserved) : -1;
	idx_t selected = 0;
	std::set<vector<uint32_t>> seen;
	for (idx_t id = 0; id < candidates.size(); id++) {
		auto current = id == 0 ? baseline : Evaluate(input, candidates[id], calibration);
		if (id > 0 && !seen.insert(current.physical_state).second) {
			continue;
		}
		if (id == 0) {
			seen.insert(current.physical_state);
		}
		TotalPackageCandidateTrace row;
		row.epoch_generation = input.epoch_generation;
		row.candidate_id = id;
		row.reference_work_s = current.service;
		row.package_energy_j = current.energy;
		row.modeled_cost = current.cost;
		row.greedy_allocation_cost = greedy_cost;
		row.exact_allocation_cost = exact_cost;
		row.non_diminishing_queries = non_diminishing;
		row.reference_efficiency = TotalPackageEfficiency(baseline.service, baseline.energy);
		row.candidate_efficiency = TotalPackageEfficiency(current.service, current.energy);
		row.reason = id == 0 ? "reference" : current.reason;
		if (!reference_only && id > 0 && baseline.modeled && current.modeled && current.plan.valid &&
		    (lowest_safe || TotalPackagePreferCandidate(current.service, current.energy, baseline.service,
		                                                baseline.energy, margin)) &&
		    (selected == 0 || lowest_safe || row.candidate_efficiency > TotalPackageEfficiency(best.service, best.energy))) {
			best = std::move(current);
			selected = id;
		}
		trace.push_back(std::move(row));
		if (lowest_safe && selected != 0) {
			break;
		}
	}
	for (auto &row : trace) {
		row.shadow_selected = shadow && row.candidate_id == selected;
		row.selected = row.candidate_id == (shadow ? 0 : selected);
		if (row.selected && selected == 0 && !baseline.modeled) {
			row.reason = baseline.reason;
		}
	}
	return shadow ? baseline.plan : best.plan;
}

} // namespace duckdb
