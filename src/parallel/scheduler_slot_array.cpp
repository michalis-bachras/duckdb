#include "duckdb/parallel/scheduler_slot_array.hpp"
#include "duckdb/parallel/thread_local_scheduler_state.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {

// Out-of-class definitions for static constexpr members (required for ODR-use in C++11/14)
constexpr double SchedulerSlotArray::LARGE_CONSTANT;
constexpr double SchedulerSlotArray::REFERENCE_DURATION_MS;
constexpr double SchedulerSlotArray::INITIAL_PRIORITY;
constexpr double SchedulerSlotArray::MIN_PRIORITY;
constexpr int SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;
constexpr double SchedulerSlotArray::DEFAULT_DECAY_LAMBDA;
constexpr double SchedulerSlotArray::TRACKING_DURATION_MS;
constexpr double SchedulerSlotArray::REFRESH_DURATION_MS;

SchedulerSlotArray::SchedulerSlotArray() : active_count(0), sequence_number(0) {
	active_slots.reset();
}

idx_t SchedulerSlotArray::RegisterQuery(Executor &executor) {
	lock_guard<mutex> lock(registration_lock);
	return RegisterQueryInternal(executor);
}

idx_t SchedulerSlotArray::RegisterQueryInternal(Executor &executor) {
	// Must be called under registration_lock

	// Find first free slot
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (!active_slots.test(i)) {
			// Found a free slot - initialize with constants
			auto &slot = slots[i];
			slot.executor.store(&executor, std::memory_order_release);
			slot.active_pipeline.store(nullptr, std::memory_order_release);
			slot.quanta_count.store(0, std::memory_order_release);
			slot.start_time_ns.store(0, std::memory_order_release);
			slot.total_elapsed_us.store(0, std::memory_order_release);
			slot.arrival_time_ms = NowMs();

			// Reset snapshot for this slot if tracking is active
			// (prevents stale snapshot from a previous query in this slot)
			if (tracking_active.load(std::memory_order_acquire)) {
				quanta_at_window_start[i] = 0;
				elapsed_at_window_start[i] = 0;
			}

			active_slots.set(i);
			active_count.fetch_add(1, std::memory_order_release);
			sequence_number.fetch_add(1, std::memory_order_release);

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

	// Reset the executor's slot index (bidirectional cleanup)
	Executor *exec = slot.executor.load(std::memory_order_acquire);
	if (exec) {
		exec->SetSchedulerSlotIndex(DConstants::INVALID_INDEX);
	}

	// Mark slot as inactive — workers discover lazily via nullptr check
	slot.executor.store(nullptr, std::memory_order_release);
	slot.active_pipeline.store(nullptr, std::memory_order_release);

	active_slots.reset(slot_index);
	active_count.fetch_sub(1, std::memory_order_release);
	sequence_number.fetch_add(1, std::memory_order_release);

	// Record trace entry if tracking is active (in-window portion only)
	if (tracking_active.load(std::memory_order_acquire)) {
		int total_quanta = slot.quanta_count.load(std::memory_order_acquire);
		int snapshot_quanta = quanta_at_window_start[slot_index];
		int window_quanta = total_quanta - snapshot_quanta;

		if (window_quanta > 0) {
			uint64_t total_elapsed = slot.total_elapsed_us.load(std::memory_order_acquire);
			uint64_t snapshot_elapsed = elapsed_at_window_start[slot_index];
			uint64_t window_elapsed = total_elapsed - snapshot_elapsed;

			QueryTraceEntry entry;
			// Pre-existing query → entered window at t=0; new query → relative arrival
			if (snapshot_quanta > 0) {
				entry.arrival_time_ms = 0.0;
			} else {
				entry.arrival_time_ms = slot.arrival_time_ms - tracking_start_time_ms;
			}
			entry.window_quanta = window_quanta;
			entry.avg_quantum_ms = (static_cast<double>(window_elapsed) / 1000.0) / static_cast<double>(window_quanta);
			// In-window wall time: from when query entered window to now
			double window_entry_time = (snapshot_quanta > 0) ? tracking_start_time_ms : slot.arrival_time_ms;
			entry.in_window_wall_time_ms = NowMs() - window_entry_time;
			tracked_workload.push_back(entry);
		}
	}

	// Wake the front waiter in the queue (if any) — a slot is now free
	if (!wait_queue.empty()) {
		wait_queue.front()->cv.notify_one();
	}
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

void SchedulerSlotArray::AccumulateElapsedTime(idx_t slot_index, uint64_t elapsed_us) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	slots[slot_index].total_elapsed_us.fetch_add(elapsed_us, std::memory_order_relaxed);
}

void SchedulerSlotArray::IncrementQuantaCount(idx_t slot_index) {
	if (slot_index >= SCHEDULER_MAX_SLOTS) {
		return;
	}
	slots[slot_index].quanta_count.fetch_add(1, std::memory_order_relaxed);
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

//===--------------------------------------------------------------------===//
// Global Wait Queue
//===--------------------------------------------------------------------===//

idx_t SchedulerSlotArray::WaitForSlot(Executor &executor, ClientContext &context) {
	std::unique_lock<mutex> lock(registration_lock);

	// Create per-waiter entry with its own CV (targeted wake-ups)
	auto entry = make_uniq<WaitQueueEntry>();
	entry->executor = &executor;
	auto &my_cv = entry->cv;
	wait_queue.push_back(std::move(entry));

	// Sleep until: (front of queue AND slot available) OR interrupted
	// Predicate guards against spurious wakeups — only the front waiter with a free slot proceeds
	my_cv.wait(lock, [&]() {
		return (!wait_queue.empty() && wait_queue.front()->executor == &executor && HasFreeSlot()) ||
		       context.interrupted;
	});

	// Remove self from queue (works for both front and non-front positions)
	RemoveFromQueue(executor);

	// Handle interruption: clean up and pass the free slot to the next waiter
	if (context.interrupted) {
		if (!wait_queue.empty() && HasFreeSlot()) {
			wait_queue.front()->cv.notify_one();
		}
		throw InterruptException();
	}

	// Normal: we are front, slot is free — claim it (already under registration_lock)
	return RegisterQueryInternal(executor);
}

void SchedulerSlotArray::InterruptWaiting(Executor &executor) {
	lock_guard<mutex> lock(registration_lock);
	for (auto &entry : wait_queue) {
		if (entry->executor == &executor) {
			entry->cv.notify_one();
			return;
		}
	}
}

bool SchedulerSlotArray::HasFreeSlot() const {
	return active_count.load(std::memory_order_acquire) < SCHEDULER_MAX_SLOTS;
}

void SchedulerSlotArray::RemoveFromQueue(Executor &executor) {
	for (auto it = wait_queue.begin(); it != wait_queue.end(); ++it) {
		if ((*it)->executor == &executor) {
			wait_queue.erase(it);
			return;
		}
	}
}

void SchedulerSlotArray::StartTrackingWindow() {
	lock_guard<mutex> lock(registration_lock);
	tracked_workload.clear();
	tracking_start_time_ms = NowMs();

	// Snapshot per-slot state for delta computation
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (active_slots.test(i)) {
			quanta_at_window_start[i] = slots[i].quanta_count.load(std::memory_order_acquire);
			elapsed_at_window_start[i] = slots[i].total_elapsed_us.load(std::memory_order_acquire);
		} else {
			quanta_at_window_start[i] = 0;
			elapsed_at_window_start[i] = 0;
		}
	}

	tracking_active.store(true, std::memory_order_release);
}

void SchedulerSlotArray::StopTrackingWindow() {
	lock_guard<mutex> lock(registration_lock);
	double window_end_time = NowMs();

	// Record still-active queries — they create contention and experience slowdown
	for (idx_t i = 0; i < SCHEDULER_MAX_SLOTS; i++) {
		if (!active_slots.test(i)) {
			continue;
		}
		if (slots[i].executor.load(std::memory_order_acquire) == nullptr) {
			continue;
		}

		int current_quanta = slots[i].quanta_count.load(std::memory_order_acquire);
		int snapshot_quanta = quanta_at_window_start[i];
		int window_quanta = current_quanta - snapshot_quanta;

		if (window_quanta <= 0) {
			continue;
		}

		uint64_t current_elapsed = slots[i].total_elapsed_us.load(std::memory_order_acquire);
		uint64_t snapshot_elapsed = elapsed_at_window_start[i];
		uint64_t window_elapsed = current_elapsed - snapshot_elapsed;

		QueryTraceEntry entry;
		if (snapshot_quanta > 0) {
			entry.arrival_time_ms = 0.0;
		} else {
			entry.arrival_time_ms = slots[i].arrival_time_ms - tracking_start_time_ms;
		}
		entry.window_quanta = window_quanta;
		entry.avg_quantum_ms = (static_cast<double>(window_elapsed) / 1000.0) / static_cast<double>(window_quanta);
		double window_entry_time = (snapshot_quanta > 0) ? tracking_start_time_ms : slots[i].arrival_time_ms;
		entry.in_window_wall_time_ms = window_end_time - window_entry_time;
		tracked_workload.push_back(entry);
	}

	// Disable tracking AFTER recording — prevents race with DeregisterQuery
	tracking_active.store(false, std::memory_order_release);
}

bool SchedulerSlotArray::IsTrackingActive() const {
	return tracking_active.load(std::memory_order_acquire);
}

const vector<QueryTraceEntry> &SchedulerSlotArray::GetTrackedWorkload() const {
	return tracked_workload;
}

double SchedulerSlotArray::NowMs() {
	auto now = std::chrono::steady_clock::now();
	return std::chrono::duration<double, std::milli>(now.time_since_epoch()).count();
}

void SchedulerSlotArray::SetDecayParameters(int new_d_start, double new_lambda) {
	decay_start_quanta.store(new_d_start, std::memory_order_relaxed);
	decay_lambda.store(new_lambda, std::memory_order_relaxed);
}

} // namespace duckdb
