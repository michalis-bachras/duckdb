//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/thread_local_scheduler_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parallel/scheduler_slot_array.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cmath>
#include <limits>

namespace duckdb {

//! ThreadLocalSchedulerState holds per-thread scheduling state for stride scheduling.
//!
//! Three-mask update mechanism:
//! - Change mask: new resource group registered in a slot
//! - Finalization mask: pipeline entering finalization phase (local pass → 0)
//! - Return mask: new pipeline/task set in existing slot (local pass → global_pass)
//!
//! Other threads push updates via atomic fetch_or on this thread's bitmasks.
//! This thread pulls updates by atomically exchanging masks with zero.
//! Only changed slots are refreshed (no full scan needed).
//!
//! Uses a pre-sorted slot array for O(1) slot selection by pass value.
//!
//! Workflow per thread:
//! 1. PullUpdates() - check for slot changes
//! 2. sorted_slots[0] gives min-pass slot
//! 3. Iterate sorted_slots[0..n] if selected slot has no tasks
//! 4. UpdateLocalPass() after task execution
struct ThreadLocalSchedulerState {
	//===--------------------------------------------------------------------===//
	// Update Masks (written by other threads, read by this thread)
	//===--------------------------------------------------------------------===//

	//! Change mask: bits set when a NEW resource group is registered in a slot
	//! Split into two 64-bit words for 128 slots
	std::atomic<uint64_t> change_mask_low {0};
	std::atomic<uint64_t> change_mask_high {0};

	//! Finalization mask: bits set when a pipeline enters finalization phase(finish and complete events)
	//! Workers set local pass = 0 for these slots to prioritize finalization tasks
	std::atomic<uint64_t> finalization_mask_low {0};
	std::atomic<uint64_t> finalization_mask_high {0};

	//! Return mask: bits set when a new pipeline/task set is inserted into
	//! an existing resource group's slot. Workers set local pass = global_pass
	//! and retain the existing priority/stride.
	std::atomic<uint64_t> return_mask_low {0};
	std::atomic<uint64_t> return_mask_high {0};

	//===--------------------------------------------------------------------===//
	// Local Scheduling State (only accessed by owning thread)
	//===--------------------------------------------------------------------===//

	//! Bitset of which slots are active (local view)
	std::bitset<SCHEDULER_MAX_SLOTS> active_slots;

	//! Cached priorities for each slot
	std::array<double, SCHEDULER_MAX_SLOTS> priorities;

	//! Cached pass values for each slot (local view, may diverge from global)
	std::array<double, SCHEDULER_MAX_SLOTS> pass_values;

	//! Cached strides for each slot
	std::array<double, SCHEDULER_MAX_SLOTS> strides;

	//! Per-worker local global pass.
	//! Updated after each quantum by f × local_global_stride. Used to initialize pass for new queries.
	double local_global_pass {0.0};

	//! Per-worker local global stride = LARGE_CONSTANT / Σ(local priorities).
	//! Recomputed when local priorities change (decay, new slot, slot removed).
	double local_global_stride {0.0};

	//! Pointer to the global slot array. Set once at thread start in ExecuteForever.
	//! Used by ApplyLocalDecay to read live decay parameters (atomics).
	const SchedulerSlotArray *slot_array_ptr = nullptr;

	//! Per-slot CPU time accumulator (microseconds) for driving local decay.
	std::array<uint64_t, SCHEDULER_MAX_SLOTS> cpu_time_accum_us;

	//! Per-slot count of decay steps applied by this worker.
	std::array<int, SCHEDULER_MAX_SLOTS> decay_steps;

	//! Active slots sorted by pass value (ascending = lowest pass first)
	//! sorted_slots[0] = slot with minimum pass (first choice for scheduling)
	std::array<idx_t, SCHEDULER_MAX_SLOTS> sorted_slots;

	//! Number of valid entries in sorted_slots
	idx_t sorted_count = 0;

	//! Whether this thread needs a full refresh (first time or reset)
	bool needs_full_refresh = true;

	//===--------------------------------------------------------------------===//
	// Constructor
	//===--------------------------------------------------------------------===//

	ThreadLocalSchedulerState() {
		active_slots.reset();
		priorities.fill(0.0);
		pass_values.fill(0.0);
		strides.fill(0.0);
		sorted_slots.fill(DConstants::INVALID_INDEX);
		cpu_time_accum_us.fill(0);
		decay_steps.fill(0);
	}

	//===--------------------------------------------------------------------===//
	// Full Refresh (used on first call or after reset)
	//===--------------------------------------------------------------------===//

	//! Full refresh from global state - reads all active slots
	void FullRefreshFromGlobal(const SchedulerSlotArray &global) {
		vector<idx_t> active_indices;
		global.GetActiveSlots(active_indices);

		active_slots.reset();
		for (idx_t slot_idx : active_indices) {
			active_slots.set(slot_idx);
			priorities[slot_idx] = SchedulerSlotArray::INITIAL_PRIORITY;
			pass_values[slot_idx] = local_global_pass;
			strides[slot_idx] = SchedulerSlotArray::LARGE_CONSTANT / SchedulerSlotArray::INITIAL_PRIORITY;
			cpu_time_accum_us[slot_idx] = 0;
			decay_steps[slot_idx] = 0;
		}
		RecomputeLocalGlobalStride();

		// Drain any pending masks (we just did a full refresh)
		// acquire pairs with fetch_or(release) on the push side
		change_mask_low.exchange(0, std::memory_order_acquire);
		change_mask_high.exchange(0, std::memory_order_acquire);
		finalization_mask_low.exchange(0, std::memory_order_acquire);
		finalization_mask_high.exchange(0, std::memory_order_acquire);
		return_mask_low.exchange(0, std::memory_order_acquire);
		return_mask_high.exchange(0, std::memory_order_acquire);

		RebuildSortedSlots();
		needs_full_refresh = false;
	}

	//===--------------------------------------------------------------------===//
	// Incremental Update via Change/Finalization/Return Masks
	//===--------------------------------------------------------------------===//

	//! Pull updates from this thread's update masks.
	//! Atomically exchanges each mask with zero, then processes set bits.
	//! If there are no outstanding changes, this update is cheap.
	//!
	//! Returns true if any updates were applied.
	bool PullUpdates(const SchedulerSlotArray &global) {
		if (needs_full_refresh) {
			FullRefreshFromGlobal(global);
			return true;
		}

		// Atomically exchange all masks with zero
		// acquire pairs with fetch_or(release) on the push side
		uint64_t change_low = change_mask_low.exchange(0, std::memory_order_acquire);
		uint64_t change_high = change_mask_high.exchange(0, std::memory_order_acquire);
		uint64_t fin_low = finalization_mask_low.exchange(0, std::memory_order_acquire);
		uint64_t fin_high = finalization_mask_high.exchange(0, std::memory_order_acquire);
		uint64_t ret_low = return_mask_low.exchange(0, std::memory_order_acquire);
		uint64_t ret_high = return_mask_high.exchange(0, std::memory_order_acquire);

		// If nothing changed, no work needed
		if (change_low == 0 && change_high == 0 && fin_low == 0 && fin_high == 0 && ret_low == 0 && ret_high == 0) {
			return false;
		}

		bool changed = false;

		// Process finalization mask first (set local pass = 0 for finalizing slots)
		changed |= ProcessFinalizationMask(fin_low, 0);
		changed |= ProcessFinalizationMask(fin_high, 64);

		// Process return mask (new pipeline in existing slot → local pass = global_pass)
		changed |= ProcessReturnMask(ret_low, 0, global);
		changed |= ProcessReturnMask(ret_high, 64, global);

		// Process change mask (new resource group → activate slot, read all from global)
		changed |= ProcessChangeMask(change_low, 0, global);
		changed |= ProcessChangeMask(change_high, 64, global);

		if (changed) {
			RebuildSortedSlots();
		}

		return changed;
	}

	//===--------------------------------------------------------------------===//
	// Slot Selection (O(1) via sorted array)
	//===--------------------------------------------------------------------===//

	//! Get the slot with the nth-smallest pass value. O(1).
	//! n=0 gives the minimum pass slot (the stride scheduling choice).
	//! Returns INVALID_INDEX if n >= sorted_count.
	idx_t FindNthMinPassSlot(idx_t n) const {
		if (n < sorted_count) {
			return sorted_slots[n];
		}
		return DConstants::INVALID_INDEX;
	}

	//! Convenience: return the slot with minimum pass value
	idx_t FindMinPassSlot() const {
		return FindNthMinPassSlot(0);
	}

	//! Get the number of active slots in local cache
	idx_t GetActiveSlotCount() const {
		return sorted_count;
	}

	//===--------------------------------------------------------------------===//
	// Local Pass Update
	//===--------------------------------------------------------------------===//

	//! Update local pass value after working on a slot.
	//! time_fraction is the fraction of a quantum used (1.0 for a full quantum).
	//! Also updates sorted order to reflect the new pass value.
	void UpdateLocalPass(idx_t slot_idx, double time_fraction) {
		if (slot_idx < SCHEDULER_MAX_SLOTS && active_slots.test(slot_idx)) {
			pass_values[slot_idx] += strides[slot_idx] * time_fraction;
			// Re-sort to maintain ordering after pass change
			RebuildSortedSlots();
		}
	}

	//! Update per-worker local global pass after executing a quantum.
	//! f = elapsed_ms / REFERENCE_DURATION_MS. Uses local_global_stride.
	void UpdateLocalGlobalPass(double f) {
		local_global_pass += f * local_global_stride;
	}

	//! Apply priority decay locally after executing a quantum.
	//! Accumulates CPU time and fires decay steps when threshold is reached.
	//! Reads live decay parameters (d_start, lambda) from slot_array_ptr atomics.
	void ApplyLocalDecay(idx_t slot_idx, uint64_t elapsed_us) {
		if (slot_idx >= SCHEDULER_MAX_SLOTS || !active_slots.test(slot_idx)) {
			return;
		}

		cpu_time_accum_us[slot_idx] += elapsed_us;

		// Decay threshold
		static constexpr uint64_t DECAY_THRESHOLD_US =
		    static_cast<uint64_t>(SchedulerSlotArray::REFERENCE_DURATION_MS * 1000.0);

		// Fast exit: common case for cheap morsels (< 2ms accumulated)
		if (cpu_time_accum_us[slot_idx] < DECAY_THRESHOLD_US) {
			return;
		}

		// How many complete thresholds were crossed
		const int steps_crossed = static_cast<int>(cpu_time_accum_us[slot_idx] / DECAY_THRESHOLD_US);
		cpu_time_accum_us[slot_idx] %= DECAY_THRESHOLD_US;

		const int old_total = decay_steps[slot_idx];
		const int new_total = old_total + steps_crossed;
		decay_steps[slot_idx] = new_total;

		// Read live decay parameters from the global slot array (set by optimizer)
		const int d_start = slot_array_ptr->decay_start_quanta.load(std::memory_order_relaxed);
		const double lambda = slot_array_ptr->decay_lambda.load(std::memory_order_relaxed);

		// How many of the crossed steps actually fall past d_start
		const int effective_steps = std::max(0, new_total - d_start) - std::max(0, old_total - d_start);

		if (effective_steps <= 0) {
			return;
		}

		// Apply all effective decay steps in one pow() call
		const double old_prio = priorities[slot_idx];
		double new_prio = old_prio * std::pow(lambda, effective_steps);
		new_prio = std::max(new_prio, SchedulerSlotArray::MIN_PRIORITY);

		// Skip recomputation if priority didn't actually change (already at floor)
		if (new_prio == old_prio) {
			return;
		}

		priorities[slot_idx] = new_prio;
		strides[slot_idx] = SchedulerSlotArray::LARGE_CONSTANT / new_prio;
		RecomputeLocalGlobalStride();
	}

	//===--------------------------------------------------------------------===//
	// Lazy Slot Disable (for deregistration)
	//===--------------------------------------------------------------------===//

	//! Disable a slot locally when a worker discovers the global executor is nullptr.
	//! This implements the paper's lazy deactivation approach.
	void DisableSlotLocally(idx_t slot_idx) {
		if (slot_idx < SCHEDULER_MAX_SLOTS && active_slots.test(slot_idx)) {
			active_slots.reset(slot_idx);
			cpu_time_accum_us[slot_idx] = 0;
			decay_steps[slot_idx] = 0;
			RecomputeLocalGlobalStride();
			RebuildSortedSlots();
		}
	}

private:
	//===--------------------------------------------------------------------===//
	// Internal Helpers
	//===--------------------------------------------------------------------===//

	//! Process finalization mask bits — set local pass = 0 for finalizing slots
	bool ProcessFinalizationMask(uint64_t mask, idx_t offset) {
		bool changed = false;
		while (mask != 0) {
			int bit_pos = __builtin_ctzll(mask);
			idx_t slot_idx = static_cast<idx_t>(bit_pos) + offset;
			if (active_slots.test(slot_idx)) {
				pass_values[slot_idx] = 0.0;
				changed = true;
			}
			mask &= mask - 1;
		}
		return changed;
	}

	//! Process return mask bits — new pipeline in existing slot
	//! Set local pass = local_global_pass, retain existing priority/stride
	bool ProcessReturnMask(uint64_t mask, idx_t offset, const SchedulerSlotArray &global) {
		bool changed = false;
		while (mask != 0) {
			int bit_pos = __builtin_ctzll(mask);
			idx_t slot_idx = static_cast<idx_t>(bit_pos) + offset;
			if (active_slots.test(slot_idx)) {
				pass_values[slot_idx] = local_global_pass;
				// Priority and stride are retained (bound to resource group, not task set)
				changed = true;
			}
			mask &= mask - 1;
		}
		return changed;
	}

	//! Process change mask bits — new resource group, init all from constants
	bool ProcessChangeMask(uint64_t mask, idx_t offset, const SchedulerSlotArray &global) {
		bool changed = false;
		while (mask != 0) {
			int bit_pos = __builtin_ctzll(mask);
			idx_t slot_idx = static_cast<idx_t>(bit_pos) + offset;
			active_slots.set(slot_idx);
			priorities[slot_idx] = SchedulerSlotArray::INITIAL_PRIORITY;
			pass_values[slot_idx] = local_global_pass;
			strides[slot_idx] = SchedulerSlotArray::LARGE_CONSTANT / SchedulerSlotArray::INITIAL_PRIORITY;
			cpu_time_accum_us[slot_idx] = 0;
			decay_steps[slot_idx] = 0;
			changed = true;
			mask &= mask - 1;
		}
		if (changed) {
			RecomputeLocalGlobalStride();
		}
		return changed;
	}

	//! Recompute local global stride from local priorities of active slots.
	//! local_global_stride = LARGE_CONSTANT / Σ(local priorities of active slots).
	void RecomputeLocalGlobalStride() {
		double total_priority = 0.0;
		for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
			if (active_slots.test(i)) {
				total_priority += priorities[i];
			}
		}
		if (total_priority > 0.0) {
			local_global_stride = SchedulerSlotArray::LARGE_CONSTANT / total_priority;
		} else {
			local_global_stride = 0.0;
		}
	}

	//! Rebuild the sorted_slots array from active_slots.
	void RebuildSortedSlots() {
		sorted_count = 0;
		for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
			if (active_slots.test(i)) {
				sorted_slots[sorted_count++] = i;
			}
		}
		// Sort by pass value ascending (lowest pass = highest priority)
		std::sort(sorted_slots.begin(), sorted_slots.begin() + sorted_count,
		          [this](idx_t a, idx_t b) { return pass_values[a] < pass_values[b]; });
	}
};

} // namespace duckdb
