#include "duckdb/parallel/scheduler_slot_array.hpp"

#include <limits>

namespace duckdb {

SchedulerSlotArray::SchedulerSlotArray() : active_count(0), sequence_number(0), global_pass(0.0) {
	active_slots.reset();
}

idx_t SchedulerSlotArray::RegisterQuery(Executor &executor, double initial_priority) {
	lock_guard<mutex> lock(registration_lock);

	// Find first free slot
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (!active_slots.test(i)) {
			// Found a free slot
			auto &slot = slots[i];
			slot.executor.store(&executor, std::memory_order_release);
			slot.active_pipeline.store(nullptr, std::memory_order_release);
			slot.priority.store(initial_priority, std::memory_order_release);
			slot.stride.store(LARGE_CONSTANT / initial_priority, std::memory_order_release);
			// Initialize pass to global pass (so new queries start fair)
			slot.pass.store(global_pass.load(std::memory_order_acquire), std::memory_order_release);
			slot.decay_count.store(0, std::memory_order_release);
			slot.start_time_ns.store(0, std::memory_order_release);

			active_slots.set(i);
			active_count.fetch_add(1, std::memory_order_release);
			sequence_number.fetch_add(1, std::memory_order_release);

			return i;
		}
	}

	// No slots available
	return DConstants::INVALID_INDEX;
}

void SchedulerSlotArray::DeregisterQuery(idx_t slot_index) {
	lock_guard<mutex> lock(registration_lock);

	if (slot_index >= SCHEDULER_MAX_SLOTS || !active_slots.test(slot_index)) {
		return;
	}

	auto &slot = slots[slot_index];

	// Update global pass to the maximum of current global pass and this slot's pass
	double slot_pass = slot.pass.load(std::memory_order_acquire);
	double current_global = global_pass.load(std::memory_order_acquire);
	if (slot_pass > current_global) {
		global_pass.store(slot_pass, std::memory_order_release);
	}

	slot.executor.store(nullptr, std::memory_order_release);
	slot.active_pipeline.store(nullptr, std::memory_order_release);

	active_slots.reset(slot_index);
	active_count.fetch_sub(1, std::memory_order_release);
	sequence_number.fetch_add(1, std::memory_order_release);
}

void SchedulerSlotArray::SetActivePipeline(idx_t slot_index, Pipeline *pipeline) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	slots[slot_index].active_pipeline.store(pipeline, std::memory_order_release);
}

Pipeline *SchedulerSlotArray::GetActivePipeline(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return nullptr;
	}
	return slots[slot_index].active_pipeline.load(std::memory_order_acquire);
}

void SchedulerSlotArray::UpdatePriority(idx_t slot_index, double new_priority) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	auto &slot = slots[slot_index];
	slot.priority.store(new_priority, std::memory_order_release);
	slot.stride.store(LARGE_CONSTANT / new_priority, std::memory_order_release);
}

void SchedulerSlotArray::UpdatePass(idx_t slot_index, double pass_increment) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	auto &slot = slots[slot_index];
	double old_pass = slot.pass.load(std::memory_order_acquire);
	slot.pass.store(old_pass + pass_increment, std::memory_order_release);
}

void SchedulerSlotArray::IncrementDecayCount(idx_t slot_index) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	slots[slot_index].decay_count.fetch_add(1, std::memory_order_release);
}

double SchedulerSlotArray::GetPriority(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return 1.0;
	}
	return slots[slot_index].priority.load(std::memory_order_acquire);
}

double SchedulerSlotArray::GetStride(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return LARGE_CONSTANT;
	}
	return slots[slot_index].stride.load(std::memory_order_acquire);
}

double SchedulerSlotArray::GetPass(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return 0.0;
	}
	return slots[slot_index].pass.load(std::memory_order_acquire);
}

int SchedulerSlotArray::GetDecayCount(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return 0;
	}
	return slots[slot_index].decay_count.load(std::memory_order_acquire);
}

Executor *SchedulerSlotArray::GetExecutor(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return nullptr;
	}
	return slots[slot_index].executor.load(std::memory_order_acquire);
}

idx_t SchedulerSlotArray::GetActiveSlotCount() const {
	return active_count.load(std::memory_order_acquire);
}

void SchedulerSlotArray::GetActiveSlots(vector<idx_t> &out_indices) const {
	out_indices.clear();
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (slots[i].executor.load(std::memory_order_acquire) != nullptr) {
			out_indices.push_back(i);
		}
	}
}

uint64_t SchedulerSlotArray::GetSequenceNumber() const {
	return sequence_number.load(std::memory_order_acquire);
}

idx_t SchedulerSlotArray::FindMinPassSlot() const {
	idx_t min_slot = DConstants::INVALID_INDEX;
	double min_pass = std::numeric_limits<double>::max();

	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (slots[i].executor.load(std::memory_order_acquire) != nullptr) {
			double pass = slots[i].pass.load(std::memory_order_acquire);
			if (pass < min_pass) {
				min_pass = pass;
				min_slot = i;
			}
		}
	}

	return min_slot;
}

double SchedulerSlotArray::ComputeTotalPriority() const {
	double total = 0.0;
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (slots[i].executor.load(std::memory_order_acquire) != nullptr) {
			total += slots[i].priority.load(std::memory_order_acquire);
		}
	}
	return total;
}

} // namespace duckdb
