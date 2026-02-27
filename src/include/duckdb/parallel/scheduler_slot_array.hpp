//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/scheduler_slot_array.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

#include <array>
#include <bitset>
#include <condition_variable>
#include <deque>

namespace duckdb {

class ClientContext;
class Executor;
class Pipeline;
struct ThreadLocalSchedulerState;

//! Maximum number of concurrent queries the scheduler can track
static constexpr idx_t SCHEDULER_MAX_SLOTS = 128;

//! Entry in the global wait queue. Each waiting query gets its own
//! condition variable to allow targeted wake-ups.
struct WaitQueueEntry {
	Executor *executor;
	std::condition_variable cv;
};

//! SchedulerSlot represents a single slot in the global slot array.
//! Each slot corresponds to one active query (Resource Group in paper terminology).
struct SchedulerSlot {
	//! The executor (query) occupying this slot. nullptr if slot is free.
	atomic<Executor *> executor;
	//! The currently active pipeline for this query. nullptr if no active pipeline.
	atomic<Pipeline *> active_pipeline;
	//! Current priority (higher = more resources, decays over time)
	//! Initialized to INITIAL_PRIORITY when registered, decays via Formula 2
	atomic<double> priority;
	//! Stride value = LARGE_CONSTANT / priority
	atomic<double> stride;
	//! Pass value (accumulated stride, for scheduling decisions)
	atomic<double> pass;
	//! Number of quanta consumed (for priority decay)
	atomic<int> decay_count;
	//! Query start time in nanoseconds (for latency tracking)
	atomic<int64_t> start_time_ns;

	//! Default constructor - slot is empty/unused
	//! Actual values are set by RegisterQuery()
	SchedulerSlot()
	    : executor(nullptr), active_pipeline(nullptr), priority(0.0), stride(0.0), pass(0.0), decay_count(0),
	      start_time_ns(0) {
	}
};

//! SchedulerSlotArray manages the global slot array for stride scheduling.
//! Based on Section 2.3 of "Self-Tuning Query Scheduling for Analytical Workloads".
//!
//! Key properties:
//! - Fixed size (128 slots max)
//! - Lock-free reads for scheduling decisions
//! - Change/return mask notifications to worker threads
//! - Registration/deregistration requires mutex
//! - Sequence number for thread-local cache invalidation
class SchedulerSlotArray {
public:
	//! Stride scheduling constant (determines granularity)
	static constexpr double LARGE_CONSTANT = 1000.0;

	//! Initial priority for all queries (p0 from paper, Section 2.3)
	static constexpr double INITIAL_PRIORITY = 10000.0;

	//! Minimum priority bound (p_min from paper)
	static constexpr double MIN_PRIORITY = 100.0;

	//! Priority decay constants from Section 2.3 of the paper (Formula 2)
	//! d_start: Number of quanta before decay starts
	static constexpr int DECAY_START_QUANTA = 10;
	//! λ (lambda): Decay factor applied each quantum after d_start
	//! Formula 2: p_{i+1} = max(p_min, λ * p_i) for i >= d_start
	static constexpr double DECAY_LAMBDA = 0.9;

	SchedulerSlotArray();

	//===--------------------------------------------------------------------===//
	// Query Registration (requires lock)
	//===--------------------------------------------------------------------===//

	//! Register a query and get a slot index.
	//! Returns INVALID_INDEX if all slots are full (query must wait).
	//! Priority is initialized to INITIAL_PRIORITY.
	//! @param executor The query's executor
	//! @return Slot index, or INVALID_INDEX if no slots available
	idx_t RegisterQuery(Executor &executor);

	//! Deregister a query and free its slot.
	//! If queries are waiting in the global wait queue, wakes the front waiter.
	void DeregisterQuery(idx_t slot_index);

	//===--------------------------------------------------------------------===//
	// Global Wait Queue (stride-only)
	//===--------------------------------------------------------------------===//

	//! Block the calling thread until a slot becomes available, then register.
	//! Throws InterruptException if the query is interrupted while waiting.
	//! @param executor The query's executor
	//! @param context The client context (checked for interrupts)
	//! @return Valid slot index (never INVALID_INDEX on success)
	idx_t WaitForSlot(Executor &executor, ClientContext &context);

	//! Signal a specific waiter in the wait queue to wake up.
	//! Called from the interrupt callback to wake a query waiting for a slot.
	void InterruptWaiting(Executor &executor);

	//===--------------------------------------------------------------------===//
	// Pipeline Management (lock-free)
	//===--------------------------------------------------------------------===//

	//! Set the active pipeline for a slot.
	void SetActivePipeline(idx_t slot_index, Pipeline *pipeline);

	//! Get the active pipeline for a slot.
	Pipeline *GetActivePipeline(idx_t slot_index) const;

	//===--------------------------------------------------------------------===//
	// Priority and Pass Updates (lock-free)
	//===--------------------------------------------------------------------===//

	//! Update priority after consuming CPU time.
	//! @param slot_index Slot to update
	//! @param new_priority New priority value
	void UpdatePriority(idx_t slot_index, double new_priority);

	//! Update pass value after task execution.
	void UpdatePass(idx_t slot_index, double pass_increment);

	//! Increment decay count and apply priority decay if threshold reached.
	//! @param slot_index Slot to update
	void IncrementDecayCountAndApply(idx_t slot_index);

	//! Get current values (lock-free reads)
	double GetPriority(idx_t slot_index) const;
	double GetStride(idx_t slot_index) const;
	double GetPass(idx_t slot_index) const;
	int GetDecayCount(idx_t slot_index) const;
	Executor *GetExecutor(idx_t slot_index) const;

	//! Get the current global pass value
	double GetGlobalPass() const;

	//! Increment global pass by global stride.
	//! Called from ExecuteForever after each stride task execution.
	void IncrementGlobalPass();

	//===--------------------------------------------------------------------===//
	// Scheduling Queries (lock-free)
	//===--------------------------------------------------------------------===//

	//! Get the number of active slots.
	idx_t GetActiveSlotCount() const;

	//! Get indices of all active slots.
	void GetActiveSlots(vector<idx_t> &out_indices) const;

	//! Get the sequence number (incremented on registration/deregistration).
	//! Used for thread-local cache invalidation.
	uint64_t GetSequenceNumber() const;

	//! Find the slot with minimum pass value (for stride scheduling).
	//! Returns -1 if no active slots.
	idx_t FindMinPassSlot() const;

	//===--------------------------------------------------------------------===//
	// Worker Registration & Change/Return Masks
	//===--------------------------------------------------------------------===//

	//! Register a worker thread's local state.
	//! Must be called at the start of ExecuteForever.
	void RegisterWorker(ThreadLocalSchedulerState *worker_state);

	//! Deregister a worker thread's local state.
	//! Must be called at the end of ExecuteForever.
	void DeregisterWorker(ThreadLocalSchedulerState *worker_state);

	//! Push a change notification for a slot to all registered workers.
	//! Called when an initial task set of a new query is registered to a slot.
	//! Workers: activate slot locally, read priority/stride from global.
	void PushChangeToWorkers(idx_t slot_index);

	//! Push a finalization notification for a slot to all registered workers.
	//! Called when a pipeline enters its finalization phase.
	//! Workers: set local pass[slot] = 0 to prioritize finalization tasks.
	void PushFinalizationToWorkers(idx_t slot_index);

	//! Push a return notification for a slot to all registered workers.
	//! Called when a new pipeline/task set is inserted into an existing
	//! resource group's slot (i.e., the previous pipeline finished and a new one starts).
	//! Workers: set local pass[slot] = global_pass, retain priority/stride.
	void PushReturnToWorkers(idx_t slot_index);

	//! Compute total priority across all active slots.
	double ComputeTotalPriority() const;

private:
	//! The slot array
	std::array<SchedulerSlot, SCHEDULER_MAX_SLOTS> slots;
	//! Bitset tracking which slots are active
	std::bitset<SCHEDULER_MAX_SLOTS> active_slots;
	//! Number of active slots
	atomic<idx_t> active_count;
	//! Sequence number for cache invalidation
	atomic<uint64_t> sequence_number;
	//! Mutex for registration/deregistration
	mutex registration_lock;
	//! Global pass value (for initializing new queries and new task sets)
	atomic<double> global_pass;
	//! Global stride = LARGE_CONSTANT / Σ(priorities)
	//! Recomputed on register/deregister/decay
	atomic<double> global_stride;

	//! Recompute global stride from current active priorities
	void RecomputeGlobalStride();

	//! Registry of worker thread states for push-based mask updates.
	//! Protected by worker_registry_lock.
	vector<ThreadLocalSchedulerState *> registered_workers;
	mutex worker_registry_lock;

	//===--------------------------------------------------------------------===//
	// Global Wait Queue (private)
	//===--------------------------------------------------------------------===//

	//! FIFO queue of queries waiting for a free slot.
	//! Protected by registration_lock.
	std::deque<unique_ptr<WaitQueueEntry>> wait_queue;

	//! Register a query into a free slot (caller already holds registration_lock).
	idx_t RegisterQueryInternal(Executor &executor);

	//! Check if any slot is free.
	bool HasFreeSlot() const;

	//! Remove an executor from the wait queue.
	void RemoveFromQueue(Executor &executor);
};

} // namespace duckdb
