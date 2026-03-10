#include "duckdb/parallel/scheduler_optimizer.hpp"
#include "duckdb/parallel/workload_simulator.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace duckdb {

// Out-of-class definitions for static constexpr members (required for ODR-use in C++11/14)
constexpr int SchedulerOptimizer::SEARCH_STEPS;
constexpr double SchedulerOptimizer::DIRECTION;
constexpr double SchedulerOptimizer::ALPHA_MAX;

OptimalParams SchedulerOptimizer::Optimize(const vector<QueryTraceEntry> &workload, idx_t num_workers,
                                           double prev_lambda) {
	constexpr double R = SchedulerSlotArray::REFERENCE_DURATION_MS;

	// ── Phase 1: Generate d_start candidates from workload quantiles ──
	// Paper: "choose d_start as the minimal values that ensure that
	//         5%, 10%, ..., 35% of the tracked morsels are executed without decay."
	//
	// A query's total decay steps = floor(total_cpu_ms / t_decay).
	// Setting d_start >= this value means the query finishes before decay fires.
	// Sorting by decay steps and picking quantile positions gives us the d_start
	// values that protect the shortest P% of queries from any decay.

	vector<int> decay_step_counts;
	decay_step_counts.reserve(workload.size());
	for (auto &q : workload) {
		double total_cpu_ms = q.window_quanta * q.avg_quantum_ms;
		int steps = static_cast<int>(total_cpu_ms / R);
		decay_step_counts.push_back(steps);
	}
	std::sort(decay_step_counts.begin(), decay_step_counts.end());

	// 7 quantile percentages from the paper
	double quantiles[] = {0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35};
	vector<int> d_start_candidates;
	for (double pct : quantiles) {
		idx_t idx = std::min(static_cast<idx_t>(pct * static_cast<double>(decay_step_counts.size())),
		                     decay_step_counts.size() - 1);
		int val = decay_step_counts[idx];
		// Deduplicate: only add if different from last
		if (d_start_candidates.empty() || d_start_candidates.back() != val) {
			d_start_candidates.push_back(val);
		}
	}

	// Fallback if deduplication left us empty (all queries identical)
	if (d_start_candidates.empty()) {
		d_start_candidates.push_back(SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA);
	}

	// ── Phase 2: For each d_start, refine λ via directional search ──
	// Paper: "For each chosen value d_start, we now refine the decay λ₀
	//         through a local search procedure."

	double global_best_cost = std::numeric_limits<double>::max();
	int global_best_d = d_start_candidates[0];
	double global_best_lambda = prev_lambda;

	for (int d : d_start_candidates) {
		// Starting λ: prev_lambda (paper: "use the optimal decay parameter
		// of the previous tracking run"; caller passes 0.9 on first run)
		double best_lambda = prev_lambda;

		// Evaluate cost at the starting point
		double best_cost = WorkloadSimulator::Simulate(workload, num_workers, best_lambda, d);

		// Adaptive step width (paper: α₀ = 1)
		double alpha = 1.0;

		// 7 search steps (paper Section 4)
		for (int k = 0; k < SEARCH_STEPS; k++) {
			// Freeze center: both directions are evaluated relative to the
			// same λ_k, matching the standard compass search pattern.
			double lambda_k = best_lambda;
			bool found_better = false;

			// Try both directions: λ_k + α*0.05 and λ_k - α*0.05
			for (double dir : {DIRECTION, -DIRECTION}) {
				double candidate = lambda_k + alpha * dir;

				// λ must be in (0, 1]
				if (candidate <= 0.0 || candidate > 1.0) {
					continue;
				}

				double cost = WorkloadSimulator::Simulate(workload, num_workers, candidate, d);

				if (cost < best_cost) {
					best_cost = cost;
					best_lambda = candidate;
					found_better = true;
				}
			}

			// Adaptive step width (paper Section 4):
			// Found improvement → expand search (α *= 1.5, capped)
			// No improvement  → contract search (α *= 0.5)
			if (found_better) {
				alpha = std::min(alpha * 1.5, ALPHA_MAX);
			} else {
				alpha *= 0.5;
			}
		}

		// Track the globally best (d_start, λ) across all candidates
		if (best_cost < global_best_cost) {
			global_best_cost = best_cost;
			global_best_lambda = best_lambda;
			global_best_d = d;
		}
	}

	return {global_best_d, global_best_lambda};
}

} // namespace duckdb
