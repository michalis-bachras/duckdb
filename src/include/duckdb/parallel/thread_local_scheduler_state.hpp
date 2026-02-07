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

#include <array>
#include <bitset>

namespace duckdb {

//! ThreadLocalSchedulerState holds a thread-local COPY of the global slot array data.
//!
//! Purpose:
//! - Worker threads read from their local copy to avoid contention on the global array
//! - Only refresh from global when sequence number changes (new query arrives/leaves)
//! - Each thread independently selects which slot (query) to work on next
//!
//! Workflow per thread:
//! 1. Check if cache is stale (sequence number mismatch)
//! 2. If stale, RefreshFromGlobal()
//! 3. FindMinPassSlot() to select next query (stride scheduling)
//! 4. Get task from that slot's pipeline and execute it
//! 5. UpdateLocalPass() to record work done
struct ThreadLocalSchedulerState {
	//! Bitset of which slots are active (cached from global)
	std::bitset<SCHEDULER_MAX_SLOTS> active_slots;

	//! Cached priorities for each slot
	std::array<double, SCHEDULER_MAX_SLOTS> priorities;

	//! Cached pass values for each slot (local view, may diverge from global)
	std::array<double, SCHEDULER_MAX_SLOTS> pass_values;

	//! Cached strides for each slot
	std::array<double, SCHEDULER_MAX_SLOTS> strides;

	//! Local sequence number (compared against global to detect staleness)
	uint64_t local_sequence;

	//! Constructor - initialize to empty state
	ThreadLocalSchedulerState() : local_sequence(0) {
		active_slots.reset();
		priorities.fill(1.0);
		pass_values.fill(0.0);
		strides.fill(SchedulerSlotArray::LARGE_CONSTANT);
	}

	//! Check if the local cache is stale compared to global
	bool IsStale(const SchedulerSlotArray &global) const {
		return local_sequence != global.GetSequenceNumber();
	}

	//! Refresh local cache from global slot array
	void RefreshFromGlobal(const SchedulerSlotArray &global) {
		// Get list of active slots
		vector<idx_t> active_indices;
		global.GetActiveSlots(active_indices);

		// Clear and rebuild active bitset
		active_slots.reset();
		for (idx_t slot_idx : active_indices) {
			active_slots.set(slot_idx);
			priorities[slot_idx] = global.GetPriority(slot_idx);
			pass_values[slot_idx] = global.GetPass(slot_idx);
			strides[slot_idx] = global.GetStride(slot_idx);
		}

		// Update sequence number
		local_sequence = global.GetSequenceNumber();
	}

	//! Find the slot with minimum pass value (for stride scheduling)
	//! This is the core scheduling decision: which query should I work on next?
	//! Returns INVALID_INDEX if no active slots
	idx_t FindMinPassSlot() const {
		idx_t min_slot = DConstants::INVALID_INDEX;
		double min_pass = std::numeric_limits<double>::max();

		for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
			if (active_slots.test(i)) {
				if (pass_values[i] < min_pass) {
					min_pass = pass_values[i];
					min_slot = i;
				}
			}
		}

		return min_slot;
	}

	//! Update local pass value after working on a slot
	//! Called after executing a task. time_fraction is the fraction of a quantum used.
	//! This only updates the local view - global is not updated here.
	void UpdateLocalPass(idx_t slot_idx, double time_fraction) {
		if (slot_idx < SCHEDULER_MAX_SLOTS && active_slots.test(slot_idx)) {
			pass_values[slot_idx] += strides[slot_idx] * time_fraction;
		}
	}

	//! Get the number of active slots in local cache
	idx_t GetActiveSlotCount() const {
		return active_slots.count();
	}
};

} // namespace duckdb

