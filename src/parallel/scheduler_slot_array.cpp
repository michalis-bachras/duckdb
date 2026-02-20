#include "duckdb/parallel/scheduler_slot_array.hpp"
#include "duckdb/parallel/thread_local_scheduler_state.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {

SchedulerSlotArray::SchedulerSlotArray() : active_count(0), sequence_number(0), global_pass(0.0), global_stride(0.0) {
	active_slots.reset();
}

idx_t SchedulerSlotArray::RegisterQuery(Executor &executor) {
	lock_guard<mutex> lock(registration_lock);

	// Find first free slot
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (!active_slots.test(i)) {
			// Found a free slot - initialize with constants
			auto &slot = slots[i];
			slot.executor.store(&executor, std::memory_order_release);
			slot.active_pipeline.store(nullptr, std::memory_order_release);
			slot.priority.store(INITIAL_PRIORITY, std::memory_order_release);
			slot.stride.store(LARGE_CONSTANT / INITIAL_PRIORITY, std::memory_order_release);
			// Initialize pass to global pass (so new queries start fair)
			slot.pass.store(global_pass.load(std::memory_order_acquire), std::memory_order_release);
			slot.decay_count.store(0, std::memory_order_release);
			slot.start_time_ns.store(0, std::memory_order_release);

			active_slots.set(i);
			active_count.fetch_add(1, std::memory_order_release);
			sequence_number.fetch_add(1, std::memory_order_release);

			// Recompute global stride: LARGE_CONSTANT / Σ(priorities)
			RecomputeGlobalStride();

			// Notify all workers that slot i has a new query
			PushChangeToWorkers(i);

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

	// Recompute global stride: LARGE_CONSTANT / Σ(priorities)
	RecomputeGlobalStride();

	// Notify all workers that slot was deactivated
	PushReturnToWorkers(slot_index);
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

void SchedulerSlotArray::IncrementDecayCountAndApply(idx_t slot_index) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}

	auto &slot = slots[slot_index];
	int new_count = slot.decay_count.fetch_add(1, std::memory_order_acq_rel) + 1;

	// Apply decay formula from Section 2.3 of the paper (Formula 2):
	// p_{i+1} = p_i,                    if i < d_start
	// p_{i+1} = max(p_min, λ * p_i),    if i >= d_start
	// Only apply decay after d_start quanta
	if (new_count <= DECAY_START_QUANTA) {
		return;
	}

	// Apply exponential decay: p_new = λ * p_current
	double current_prio = slot.priority.load(std::memory_order_acquire);
	double new_prio = DECAY_LAMBDA * current_prio;

	// Enforce minimum priority bound (p_min from paper)
	if (new_prio < MIN_PRIORITY) {
		new_prio = MIN_PRIORITY;
	}

	// Update priority and stride
	slot.priority.store(new_prio, std::memory_order_release);
	slot.stride.store(LARGE_CONSTANT / new_prio, std::memory_order_release);

	// Recompute global stride since a priority changed
	RecomputeGlobalStride();
}

double SchedulerSlotArray::GetPriority(idx_t slot_index) const {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return INITIAL_PRIORITY;
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

void SchedulerSlotArray::RecomputeGlobalStride() {
	// global_stride = LARGE_CONSTANT / Σ(priorities)
	// Must be called under registration_lock or when priorities change
	double total_priority = ComputeTotalPriority();
	if (total_priority > 0.0) {
		global_stride.store(LARGE_CONSTANT / total_priority, std::memory_order_release);
	} else {
		global_stride.store(0.0, std::memory_order_release);
	}
}

void SchedulerSlotArray::IncrementGlobalPass() {
	// Paper: "After every scheduled time slice, the global pass gets incremented by the global stride."
	double stride = global_stride.load(std::memory_order_acquire);
	if (stride > 0.0) {
		double old_pass = global_pass.load(std::memory_order_acquire);
		global_pass.store(old_pass + stride, std::memory_order_release);
	}
}

double SchedulerSlotArray::GetGlobalPass() const {
	return global_pass.load(std::memory_order_acquire);
}

//===--------------------------------------------------------------------===//
// Worker Registration & Change/Return Masks
//===--------------------------------------------------------------------===//

void SchedulerSlotArray::RegisterWorker(ThreadLocalSchedulerState *worker_state) {
	lock_guard<mutex> lock(worker_registry_lock);
	registered_workers.push_back(worker_state);
}

void SchedulerSlotArray::DeregisterWorker(ThreadLocalSchedulerState *worker_state) {
	lock_guard<mutex> lock(worker_registry_lock);
	registered_workers.erase(std::remove(registered_workers.begin(), registered_workers.end(), worker_state),
	                         registered_workers.end());
}

void SchedulerSlotArray::PushChangeToWorkers(idx_t slot_index) {
	lock_guard<mutex> lock(worker_registry_lock);
	if (slot_index < 64) {
		uint64_t bit = 1ULL << slot_index;
		for (auto *worker : registered_workers) {
			worker->change_mask_low.fetch_or(bit, std::memory_order_release);
		}
	} else {
		uint64_t bit = 1ULL << (slot_index - 64);
		for (auto *worker : registered_workers) {
			worker->change_mask_high.fetch_or(bit, std::memory_order_release);
		}
	}
}

void SchedulerSlotArray::PushFinalizationToWorkers(idx_t slot_index) {
	lock_guard<mutex> lock(worker_registry_lock);
	if (slot_index < 64) {
		uint64_t bit = 1ULL << slot_index;
		for (auto *worker : registered_workers) {
			worker->finalization_mask_low.fetch_or(bit, std::memory_order_release);
		}
	} else {
		uint64_t bit = 1ULL << (slot_index - 64);
		for (auto *worker : registered_workers) {
			worker->finalization_mask_high.fetch_or(bit, std::memory_order_release);
		}
	}
}

void SchedulerSlotArray::PushReturnToWorkers(idx_t slot_index) {
	lock_guard<mutex> lock(worker_registry_lock);
	if (slot_index < 64) {
		uint64_t bit = 1ULL << slot_index;
		for (auto *worker : registered_workers) {
			worker->return_mask_low.fetch_or(bit, std::memory_order_release);
		}
	} else {
		uint64_t bit = 1ULL << (slot_index - 64);
		for (auto *worker : registered_workers) {
			worker->return_mask_high.fetch_or(bit, std::memory_order_release);
		}
	}
}

} // namespace duckdb
