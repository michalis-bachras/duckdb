#include "duckdb/parallel/query_stride_optimizer.hpp"

#include "duckdb/parallel/query_stride_scheduler.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace duckdb {

constexpr int QueryStrideOptimizer::SEARCH_STEPS;
constexpr double QueryStrideOptimizer::DIRECTION;
constexpr double QueryStrideOptimizer::MAXIMUM_STEP_SCALE;

namespace {

struct SimulatedQuery {
	double arrival_time_ms;
	idx_t total_quanta;
	double quantum_ms;
	idx_t completed_quanta;
	double initial_priority;
	double minimum_priority;
	bool static_priority;
};

struct SimulatedQueryWorkerState {
	double pass;
	double priority;
	double stride;
	double accumulated_ms;
	int decay_steps;
};

struct SimulatedWorker {
	double available_at = 0;
	double global_pass = 0;
	double global_stride = 0;
	double total_priority = 0;
	vector<SimulatedQueryWorkerState> queries;
};

static void RefreshGlobalStride(SimulatedWorker &worker) {
	worker.global_stride =
	    worker.total_priority > 0 ? QueryStrideScheduler::LARGE_CONSTANT / worker.total_priority : 0;
}

} // namespace

double QueryStrideWorkloadSimulator::Simulate(const vector<QueryStrideTraceEntry> &workload, idx_t worker_count,
                                              double decay_lambda, int decay_start) {
	if (workload.empty() || worker_count == 0) {
		return 0;
	}
	vector<idx_t> arrivals(workload.size());
	for (idx_t i = 0; i < arrivals.size(); i++) {
		arrivals[i] = i;
	}
	std::sort(arrivals.begin(), arrivals.end(), [&](idx_t left, idx_t right) {
		return workload[left].arrival_time_ms < workload[right].arrival_time_ms;
	});

	vector<SimulatedWorker> workers(worker_count);
	vector<SimulatedQuery> active;
	idx_t next_arrival = 0;
	idx_t completed = 0;
	double objective = 0;

	while (completed < workload.size()) {
		idx_t worker_index = 0;
		for (idx_t i = 1; i < workers.size(); i++) {
			if (workers[i].available_at < workers[worker_index].available_at) {
				worker_index = i;
			}
		}
		auto &worker = workers[worker_index];
		auto now = worker.available_at;
		while (next_arrival < arrivals.size() && workload[arrivals[next_arrival]].arrival_time_ms <= now) {
			auto &trace = workload[arrivals[next_arrival++]];
			if (trace.window_quanta == 0 || trace.average_quantum_ms <= 0) {
				completed++;
				continue;
			}
			SimulatedQuery query {trace.arrival_time_ms, trace.window_quanta, trace.average_quantum_ms, 0,
			                      trace.initial_priority, trace.minimum_priority, trace.static_priority};
			active.push_back(query);
			for (auto &entry : workers) {
				entry.queries.push_back({entry.global_pass, trace.initial_priority,
				                         QueryStrideScheduler::LARGE_CONSTANT / trace.initial_priority, 0, 0});
				entry.total_priority += trace.initial_priority;
				RefreshGlobalStride(entry);
			}
		}
		if (active.empty()) {
			if (next_arrival >= arrivals.size()) {
				break;
			}
			worker.available_at = workload[arrivals[next_arrival]].arrival_time_ms;
			continue;
		}

		idx_t selected = 0;
		for (idx_t i = 1; i < active.size(); i++) {
			if (worker.queries[i].pass < worker.queries[selected].pass) {
				selected = i;
			}
		}
		auto &query = active[selected];
		auto &local = worker.queries[selected];
		worker.available_at += query.quantum_ms;
		query.completed_quanta++;
		auto fraction = query.quantum_ms / QueryStrideScheduler::REFERENCE_DURATION_MS;
		local.pass += fraction * local.stride;
		worker.global_pass += fraction * worker.global_stride;
		if (!query.static_priority) {
			local.accumulated_ms += query.quantum_ms;
			while (local.accumulated_ms >= QueryStrideScheduler::REFERENCE_DURATION_MS) {
				local.accumulated_ms -= QueryStrideScheduler::REFERENCE_DURATION_MS;
				local.decay_steps++;
				if (local.decay_steps > decay_start) {
					auto old_priority = local.priority;
					local.priority = std::max(query.minimum_priority, local.priority * decay_lambda);
					local.stride = QueryStrideScheduler::LARGE_CONSTANT / local.priority;
					worker.total_priority += local.priority - old_priority;
					RefreshGlobalStride(worker);
				}
			}
		}

		if (query.completed_quanta < query.total_quanta) {
			continue;
		}
		auto response_ms = worker.available_at - query.arrival_time_ms;
		auto isolated_ms =
		    std::ceil(static_cast<double>(query.total_quanta) / static_cast<double>(worker_count)) * query.quantum_ms;
		if (isolated_ms > 0) {
			objective += response_ms / isolated_ms;
		}
		completed++;
		auto last = active.size() - 1;
		for (auto &entry : workers) {
			entry.total_priority -= entry.queries[selected].priority;
			if (selected != last) {
				entry.queries[selected] = entry.queries[last];
			}
			entry.queries.pop_back();
			RefreshGlobalStride(entry);
		}
		if (selected != last) {
			active[selected] = active[last];
		}
		active.pop_back();
	}
	return completed > 0 ? objective / static_cast<double>(completed) : 0;
}

QueryStrideOptimalParameters QueryStrideOptimizer::Optimize(const vector<QueryStrideTraceEntry> &workload,
	                                                          idx_t worker_count, double previous_lambda) {
	if (workload.empty()) {
		return {QueryStrideScheduler::DEFAULT_DECAY_START, previous_lambda, 0};
	}
	vector<int> work_steps;
	for (auto &query : workload) {
		if (!query.static_priority) {
			work_steps.push_back(static_cast<int>((query.window_quanta * query.average_quantum_ms) /
			                                      QueryStrideScheduler::REFERENCE_DURATION_MS));
		}
	}
	if (work_steps.empty()) {
		return {QueryStrideScheduler::DEFAULT_DECAY_START, previous_lambda,
		        QueryStrideWorkloadSimulator::Simulate(workload, worker_count, previous_lambda,
		                                               QueryStrideScheduler::DEFAULT_DECAY_START)};
	}
	std::sort(work_steps.begin(), work_steps.end());
	const double quantiles[] = {0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35};
	vector<int> candidates;
	for (auto quantile : quantiles) {
		auto index = std::min(static_cast<idx_t>(quantile * work_steps.size()), work_steps.size() - 1);
		if (candidates.empty() || candidates.back() != work_steps[index]) {
			candidates.push_back(work_steps[index]);
		}
	}

	QueryStrideOptimalParameters best;
	best.objective = std::numeric_limits<double>::max();
	for (auto candidate_start : candidates) {
		auto candidate_lambda = previous_lambda;
		auto candidate_cost =
		    QueryStrideWorkloadSimulator::Simulate(workload, worker_count, candidate_lambda, candidate_start);
		double step_scale = 1;
		for (int step = 0; step < SEARCH_STEPS; step++) {
			auto center = candidate_lambda;
			bool improved = false;
			for (auto direction : {DIRECTION, -DIRECTION}) {
				auto value = center + step_scale * direction;
				if (value <= 0 || value > 1) {
					continue;
				}
				auto cost = QueryStrideWorkloadSimulator::Simulate(workload, worker_count, value, candidate_start);
				if (cost < candidate_cost) {
					candidate_cost = cost;
					candidate_lambda = value;
					improved = true;
				}
			}
			step_scale = improved ? std::min(step_scale * 1.5, MAXIMUM_STEP_SCALE) : step_scale * 0.5;
		}
		if (candidate_cost < best.objective) {
			best = {candidate_start, candidate_lambda, candidate_cost};
		}
	}
	return best;
}

} // namespace duckdb
