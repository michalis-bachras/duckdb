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
#include <limits>

namespace duckdb {

//! ThreadLocalSchedulerState holds per-thread scheduling state for stride scheduling.
//!
//! Change mask / return mask mechanism for efficient updates:
//! - Other threads push updates via atomic fetch_or on this thread's bitmasks
//! - This thread pulls updates by atomically exchanging masks with zero
//! - Only changed slots are refreshed (no full scan needed)
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

	//! Change mask: bits set when a slot receives a new/changed task set/pipeline
	//! Split into two 64-bit words for 128 slots
	std::atomic<uint64_t> change_mask_low {0};
	std::atomic<uint64_t> change_mask_high {0};

	//! Return mask: bits set when a slot is deactivated (query finished)
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
			priorities[slot_idx] = global.GetPriority(slot_idx);
			pass_values[slot_idx] = global.GetPass(slot_idx);
			strides[slot_idx] = global.GetStride(slot_idx);
		}

		// Drain any pending masks (we just did a full refresh)
		change_mask_low.exchange(0, std::memory_order_relaxed);
		change_mask_high.exchange(0, std::memory_order_relaxed);
		return_mask_low.exchange(0, std::memory_order_relaxed);
		return_mask_high.exchange(0, std::memory_order_relaxed);

		RebuildSortedSlots();
		needs_full_refresh = false;
	}

	//===--------------------------------------------------------------------===//
	// Incremental Update via Change/Return Masks
	//===--------------------------------------------------------------------===//

	//! Pull updates from this thread's change/return masks.
	//! Paper: "each worker can easily pull updates into its local scheduling state.
	//! It first performs an atomic exchange of its update masks with zero."
	//!
	//! Returns true if any updates were applied.
	bool PullUpdates(const SchedulerSlotArray &global) {
		if (needs_full_refresh) {
			FullRefreshFromGlobal(global);
			return true;
		}

		// Atomically exchange masks with zero - resets them for future updates
		uint64_t change_low = change_mask_low.exchange(0, std::memory_order_relaxed);
		uint64_t change_high = change_mask_high.exchange(0, std::memory_order_relaxed);
		uint64_t return_low = return_mask_low.exchange(0, std::memory_order_relaxed);
		uint64_t return_high = return_mask_high.exchange(0, std::memory_order_relaxed);

		// If nothing changed, this is very cheap - no cache invalidation
		// Cache invalidations are avoided since there were no atomic writes
		// to the local update masks since the last read."
		if (change_low == 0 && change_high == 0 && return_low == 0 && return_high == 0) {
			return false;
		}

		bool changed = false;

		// Process return mask first (slots that became inactive)
		// Paper: "the worker has to extract the indices of set bits in the old mask values"
		// Using __builtin_ctzll for "counting the leading zeros" as the paper describes
		changed |= ProcessReturnMask(return_low, 0);
		changed |= ProcessReturnMask(return_high, 64);

		// Process change mask (slots that became active or were updated)
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
			// This is O(n log n) but n is typically very small (1-128 queries)
			RebuildSortedSlots();
		}
	}

private:
	//===--------------------------------------------------------------------===//
	// Internal Helpers
	//===--------------------------------------------------------------------===//

	//! Process return mask bits - deactivate slots
	bool ProcessReturnMask(uint64_t mask, idx_t offset) {
		bool changed = false;
		while (mask != 0) {
			// Find lowest set bit using hardware ctz instruction
			int bit_pos = __builtin_ctzll(mask);
			idx_t slot_idx = static_cast<idx_t>(bit_pos) + offset;
			active_slots.reset(slot_idx);
			changed = true;
			// Clear the bit we just processed
			mask &= mask - 1;
		}
		return changed;
	}

	//! Process change mask bits - activate/update slots from global state
	bool ProcessChangeMask(uint64_t mask, idx_t offset, const SchedulerSlotArray &global) {
		bool changed = false;
		while (mask != 0) {
			int bit_pos = __builtin_ctzll(mask);
			idx_t slot_idx = static_cast<idx_t>(bit_pos) + offset;
			active_slots.set(slot_idx);
			priorities[slot_idx] = global.GetPriority(slot_idx);
			pass_values[slot_idx] = global.GetPass(slot_idx);
			strides[slot_idx] = global.GetStride(slot_idx);
			changed = true;
			mask &= mask - 1;
		}
		return changed;
	}

	//! Rebuild the sorted_slots array from active_slots.
	//! Uses bitmask iteration for efficiency, then sorts by pass value.
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
