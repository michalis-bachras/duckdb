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
#include <chrono>
#include <condition_variable>
#include <deque>

namespace duckdb {

class ClientContext;
class Executor;
class Pipeline;
struct ThreadLocalSchedulerState;

//! Maximum number of concurrent queries the scheduler can track
static constexpr idx_t SCHEDULER_MAX_SLOTS = 128;

//! Number of chunks a worker processes per scheduling quantum in stride mode.
//! Controls the tradeoff between scheduling overhead and fairness granularity.
//! Only used when SchedulerType::STRIDE is active.
static constexpr idx_t STRIDE_QUANTUM_CHUNKS = 50;

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

	//! Accumulated work time (microseconds) across all workers.
	//! Reset on RegisterQuery, accumulated via fetch_add per quantum.
	atomic<uint64_t> total_elapsed_us;
	//! Wall-clock time when this query was registered (milliseconds since epoch).
	//! Written once at registration, read at deregistration.
	double arrival_time_ms;

	//! Default constructor - slot is empty/unused
	//! Actual values are set by RegisterQuery()
	SchedulerSlot()
	    : executor(nullptr), active_pipeline(nullptr), priority(0.0), stride(0.0), pass(0.0), decay_count(0),
	      start_time_ns(0), total_elapsed_us(0), arrival_time_ms(0.0) {
	}
};

//! Entry recording a query's in-window execution during a tracking window.
//! Used by the workload simulator to evaluate candidate scheduling parameters.
//! All fields measure only the portion of execution within the tracking window.
struct QueryTraceEntry {
	//! When the query entered the window (ms, relative to window start).
	//! 0.0 for queries already running when the window opened.
	double arrival_time_ms;
	//! Quanta consumed DURING the window only (delta from snapshot).
	int window_quanta;
	//! Average work time per quantum (ms), computed from in-window data only.
	double avg_quantum_ms;
	//! Wall-clock time the query was active in the window (ms).
	//! From window entry to completion or window end, whichever comes first.
	double in_window_wall_time_ms;
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

	//! Accumulate wall-clock elapsed time for a quantum (lock-free atomic add).
	//! Called from ExecuteForever after timing each quantum.
	void AccumulateElapsedTime(idx_t slot_index, uint64_t elapsed_us);

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
	// Self-Tuning: Tracking Window & Data Collection
	//===--------------------------------------------------------------------===//

	//! Start a tracking window. Clears previous tracked workload.
	void StartTrackingWindow();

	//! Stop the tracking window. No more entries will be appended.
	void StopTrackingWindow();

	//! Check if tracking is currently active.
	bool IsTrackingActive() const;

	//! Get the tracked workload (read-only, only valid after StopTrackingWindow).
	const vector<QueryTraceEntry> &GetTrackedWorkload() const;

	//! Helper: current wall-clock time in milliseconds (steady_clock).
	static double NowMs();

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
	// Self-Tuning: Tracking State (private)
	//===--------------------------------------------------------------------===//

	//! Whether the tracking window is currently open.
	atomic<bool> tracking_active {false};
	//! Wall-clock time when the current tracking window started (ms).
	double tracking_start_time_ms {0.0};
	//! Collected query traces during the tracking window.
	//! Writes are serialized under registration_lock (in DeregisterQuery and StopTrackingWindow).
	//! Reads happen after StopTrackingWindow() (no concurrent writes).
	vector<QueryTraceEntry> tracked_workload;

	//! Per-slot snapshot of decay_count at tracking window start.
	//! Used to compute in-window quanta delta: window_quanta = current - snapshot.
	std::array<int, SCHEDULER_MAX_SLOTS> quanta_at_window_start {};
	//! Per-slot snapshot of total_elapsed_us at tracking window start.
	//! Used to compute in-window elapsed time delta.
	std::array<uint64_t, SCHEDULER_MAX_SLOTS> elapsed_at_window_start {};

	//===--------------------------------------------------------------------===//
	// Self-Tuning: Timing Constants
	//===--------------------------------------------------------------------===//

	//! Tracking window duration (t_t from paper Section 4)
	static constexpr double T_TRACKING_MS = 20000.0;
	//! Refresh period — interval between tracking windows (t_r from paper Section 4)
	static constexpr double T_REFRESH_MS = 60000.0;

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
