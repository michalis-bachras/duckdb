#include "duckdb/parallel/workload_simulator.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace duckdb {

//! Per-query per-worker local scheduling state.
//! Mirrors ThreadLocalSchedulerState's per-slot fields.
struct WorkerQueryState {
	double local_pass;
	double local_priority;
	double local_stride;
	double local_cpu_accum_ms;
	int local_decay_steps;
};

//! Per-worker simulation state.
//! Mirrors ThreadLocalSchedulerState's global fields.
struct SimulatedWorker {
	double available_at;
	double local_global_pass;
	double local_global_stride;
	double local_total_priority; // O(1) running sum for global stride
	vector<WorkerQueryState> qs; // indexed same as active_queries
};

//! Per-query global state (shared truth across workers).
struct SimulatedQuery {
	double arrival_time_ms;
	int window_quanta;
	double avg_quantum_ms;
	int total_quanta_done;
	double sim_wall_time_ms;
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
	constexpr double R = SchedulerSlotArray::REFERENCE_DURATION_MS; // t_decay = r

	// Sort workload by arrival time (ascending)
	vector<idx_t> sorted_indices(workload.size());
	for (idx_t i = 0; i < workload.size(); i++) {
		sorted_indices[i] = i;
	}
	std::sort(sorted_indices.begin(), sorted_indices.end(),
	          [&workload](idx_t a, idx_t b) { return workload[a].arrival_time_ms < workload[b].arrival_time_ms; });

	// Per-worker state
	vector<SimulatedWorker> workers(num_workers);
	for (idx_t i = 0; i < num_workers; i++) {
		workers[i].available_at = 0.0;
		workers[i].local_global_pass = 0.0;
		workers[i].local_global_stride = 0.0;
		workers[i].local_total_priority = 0.0;
	}

	// Simulation state
	vector<SimulatedQuery> active_queries;
	active_queries.reserve(workload.size());
	double total_cost = 0.0;
	idx_t completed_count = 0;
	idx_t next_to_admit = 0;

	// Main event loop
	while (completed_count < workload.size()) {
		// Step A: Find the earliest available worker
		idx_t w = 0;
		double earliest_time = workers[0].available_at;
		for (idx_t i = 1; i < num_workers; i++) {
			if (workers[i].available_at < earliest_time) {
				earliest_time = workers[i].available_at;
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
			sq.total_quanta_done = 0;
			sq.sim_wall_time_ms = 0.0;
			active_queries.push_back(sq);

			// Initialize local state for this query on EVERY worker
			for (idx_t wi = 0; wi < num_workers; wi++) {
				WorkerQueryState wqs;
				wqs.local_pass = workers[wi].local_global_pass;
				wqs.local_priority = P_0;
				wqs.local_stride = LC / P_0;
				wqs.local_cpu_accum_ms = 0.0;
				wqs.local_decay_steps = 0;
				workers[wi].qs.push_back(wqs);

				// O(1) incremental update of local total priority
				workers[wi].local_total_priority += P_0;
				workers[wi].local_global_stride =
				    (workers[wi].local_total_priority > 0.0) ? LC / workers[wi].local_total_priority : 0.0;
			}
			next_to_admit++;
		}

		// Step C: Handle idle gap
		if (active_queries.empty()) {
			if (next_to_admit >= sorted_indices.size()) {
				break; // all done
			}
			// Fast-forward this worker to the next arrival
			idx_t next_idx = sorted_indices[next_to_admit];
			workers[w].available_at = workload[next_idx].arrival_time_ms;
			continue;
		}

		// Step D: Worker w picks the query with minimum local_pass in ITS OWN view
		idx_t selected = 0;
		double min_pass = workers[w].qs[0].local_pass;
		for (idx_t i = 1; i < active_queries.size(); i++) {
			if (workers[w].qs[i].local_pass < min_pass) {
				min_pass = workers[w].qs[i].local_pass;
				selected = i;
			}
		}

		// Step E: Execute one morsel
		auto &sq = active_queries[selected];
		workers[w].available_at = current_time + sq.avg_quantum_ms;
		sq.total_quanta_done++;

		// Step F: Compute f and update worker w's local pass for selected query
		double f = sq.avg_quantum_ms / R;
		workers[w].qs[selected].local_pass += f * workers[w].qs[selected].local_stride;

		// Step G: Accumulate CPU time and fire decay on worker w's local state
		workers[w].qs[selected].local_cpu_accum_ms += sq.avg_quantum_ms;

		while (workers[w].qs[selected].local_cpu_accum_ms >= R) {
			workers[w].qs[selected].local_cpu_accum_ms -= R;
			workers[w].qs[selected].local_decay_steps++;

			if (workers[w].qs[selected].local_decay_steps > d_start) {
				double old_prio = workers[w].qs[selected].local_priority;
				double new_prio = std::max(P_MIN, lambda * old_prio);
				workers[w].qs[selected].local_priority = new_prio;
				workers[w].qs[selected].local_stride = LC / new_prio;

				// O(1) incremental update of worker w's local global stride
				workers[w].local_total_priority += (new_prio - old_prio);
				workers[w].local_global_stride =
				    (workers[w].local_total_priority > 0.0) ? LC / workers[w].local_total_priority : 0.0;
			}
		}

		// Step H: Update worker w's local global pass
		workers[w].local_global_pass += f * workers[w].local_global_stride;

		// Step I: Check if query completed all its morsels
		if (sq.total_quanta_done >= sq.window_quanta) {
			double completion_time = workers[w].available_at;
			double sim_wall_time = completion_time - sq.arrival_time_ms;
			// Base time = isolation latency with all W workers focused on this query
			double base_time = std::ceil(static_cast<double>(sq.window_quanta) / num_workers) * sq.avg_quantum_ms;

			if (base_time > 0.0) {
				total_cost += sim_wall_time / base_time;
			}
			completed_count++;

			// Remove from ALL workers' active sets (swap-and-pop for O(1))
			idx_t last = active_queries.size() - 1;
			for (idx_t wi = 0; wi < num_workers; wi++) {
				// O(1) priority removal from this worker's running sum
				workers[wi].local_total_priority -= workers[wi].qs[selected].local_priority;
				workers[wi].local_global_stride =
				    (workers[wi].local_total_priority > 0.0) ? LC / workers[wi].local_total_priority : 0.0;

				// Swap-and-pop the per-worker query state
				if (selected != last) {
					workers[wi].qs[selected] = workers[wi].qs[last];
				}
				workers[wi].qs.pop_back();
			}

			// Swap-and-pop the global query entry
			if (selected != last) {
				active_queries[selected] = active_queries[last];
			}
			active_queries.pop_back();
		}
	}

	// Mean relative slowdown
	return (completed_count > 0) ? total_cost / static_cast<double>(completed_count) : 0.0;
}

} // namespace duckdb
