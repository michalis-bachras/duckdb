#include "duckdb/parallel/workload_simulator.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace duckdb {

//! Internal state for a query being simulated.
struct SimulatedQuery {
	double arrival_time_ms;
	int window_quanta;
	double avg_quantum_ms;

	// Simulation state
	double sim_pass;
	double sim_priority;
	double sim_stride;
	int sim_decay_count;
	int sim_quanta_done;
	double sim_wall_time_ms; // filled on completion
};

double WorkloadSimulator::Simulate(const vector<QueryTraceEntry> &workload, idx_t num_workers, double lambda,
                                   int d_start) {
	if (workload.empty() || num_workers == 0) {
		return 0.0;
	}

	// Constants matching SchedulerSlotArray
	constexpr double P_0 = SchedulerSlotArray::INITIAL_PRIORITY;
	constexpr double P_MIN = SchedulerSlotArray::MIN_PRIORITY;
	constexpr double LC = SchedulerSlotArray::LARGE_CONSTANT;

	// Sort workload by arrival time (ascending)
	vector<idx_t> sorted_indices(workload.size());
	for (idx_t i = 0; i < workload.size(); i++) {
		sorted_indices[i] = i;
	}
	std::sort(sorted_indices.begin(), sorted_indices.end(),
	          [&workload](idx_t a, idx_t b) { return workload[a].arrival_time_ms < workload[b].arrival_time_ms; });

	// Per-worker state: when each worker becomes available
	vector<double> worker_available_at(num_workers, 0.0);

	// Simulation state
	vector<SimulatedQuery> active_queries;
	active_queries.reserve(workload.size());
	double total_cost = 0.0;
	idx_t completed_count = 0;
	idx_t next_to_admit = 0;
	double sim_global_pass = 0.0;
	double running_total_priority = 0.0; // O(1) running sum for global stride

	// Main event loop
	while (completed_count < workload.size()) {
		// Step A: Find the earliest available worker
		idx_t w = 0;
		double earliest_time = worker_available_at[0];
		for (idx_t i = 1; i < num_workers; i++) {
			if (worker_available_at[i] < earliest_time) {
				earliest_time = worker_available_at[i];
				w = i;
			}
		}
		double current_time = earliest_time;

		// Step B: Admit all queries that arrived by current_time
		while (next_to_admit < sorted_indices.size()) {
			idx_t idx = sorted_indices[next_to_admit];
			if (workload[idx].arrival_time_ms > current_time) {
				break;
			}
			// Skip entries with no work
			if (workload[idx].window_quanta <= 0) {
				next_to_admit++;
				completed_count++;
				continue;
			}

			SimulatedQuery sq;
			sq.arrival_time_ms = workload[idx].arrival_time_ms;
			sq.window_quanta = workload[idx].window_quanta;
			sq.avg_quantum_ms = workload[idx].avg_quantum_ms;
			sq.sim_pass = sim_global_pass;
			sq.sim_priority = P_0;
			sq.sim_stride = LC / P_0;
			sq.sim_decay_count = 0;
			sq.sim_quanta_done = 0;
			sq.sim_wall_time_ms = 0.0;
			active_queries.push_back(sq);
			running_total_priority += P_0; // O(1) update on admission
			next_to_admit++;
		}

		// Step C: Handle idle gap
		if (active_queries.empty()) {
			if (next_to_admit >= sorted_indices.size()) {
				break; // all done
			}
			// Fast-forward this worker to the next arrival
			idx_t next_idx = sorted_indices[next_to_admit];
			worker_available_at[w] = workload[next_idx].arrival_time_ms;
			continue;
		}

		// Step D: Worker w picks the query with minimum sim_pass
		idx_t selected = 0;
		double min_pass = active_queries[0].sim_pass;
		for (idx_t i = 1; i < active_queries.size(); i++) {
			if (active_queries[i].sim_pass < min_pass) {
				min_pass = active_queries[i].sim_pass;
				selected = i;
			}
		}

		// Step E: Execute one quantum
		auto &sq = active_queries[selected];
		worker_available_at[w] = current_time + sq.avg_quantum_ms;
		sq.sim_quanta_done++;
		sq.sim_decay_count++;

		// Step F: Apply priority decay with candidate parameters
		if (sq.sim_decay_count > d_start) {
			double old_priority = sq.sim_priority;
			sq.sim_priority = std::max(P_MIN, lambda * sq.sim_priority);
			sq.sim_stride = LC / sq.sim_priority;
			// O(1) running sum update on decay
			running_total_priority += (sq.sim_priority - old_priority);
		}

		// Step G: Update pass
		sq.sim_pass += sq.sim_stride;

		// Step H: Update global pass (O(1) using running sum)
		double sim_global_stride = (running_total_priority > 0.0) ? LC / running_total_priority : 0.0;
		sim_global_pass += sim_global_stride;

		// Step I: Check if query completed its in-window work
		if (sq.sim_quanta_done >= sq.window_quanta) {
			double completion_time = worker_available_at[w];
			double sim_wall_time = completion_time - sq.arrival_time_ms;
			// Base time = isolation latency with all W workers focused on this query
			double base_time = std::ceil(static_cast<double>(sq.window_quanta) / num_workers) * sq.avg_quantum_ms;

			if (base_time > 0.0) {
				total_cost += sim_wall_time / base_time;
			}
			completed_count++;

			// O(1) running sum update on removal
			running_total_priority -= sq.sim_priority;

			// Remove from active queries (swap-and-pop for O(1))
			active_queries[selected] = active_queries.back();
			active_queries.pop_back();
		}
	}

	// Mean relative slowdown
	return (completed_count > 0) ? total_cost / static_cast<double>(completed_count) : 0.0;
}

} // namespace duckdb
