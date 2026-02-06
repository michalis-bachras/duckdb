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

namespace duckdb {

class Executor;
class Pipeline;

//! Maximum number of concurrent queries the scheduler can track
static constexpr idx_t SCHEDULER_MAX_SLOTS = 128;

//! SchedulerSlot represents a single slot in the global slot array.
//! Each slot corresponds to one active query (Resource Group in paper terminology).
struct SchedulerSlot {
	//! The executor (query) occupying this slot. nullptr if slot is free.
	atomic<Executor *> executor;
	//! The currently active pipeline for this query. nullptr if no active pipeline.
	atomic<Pipeline *> active_pipeline;
	//! Current priority (higher = more resources)
	atomic<double> priority;
	//! Stride value = LARGE_CONSTANT / priority
	atomic<double> stride;
	//! Pass value (accumulated stride, for scheduling decisions)
	atomic<double> pass;
	//! Number of quanta consumed (for priority decay)
	atomic<int> decay_count;
	//! Query start time in nanoseconds (for latency tracking)
	atomic<int64_t> start_time_ns;

	SchedulerSlot() : executor(nullptr), active_pipeline(nullptr), priority(1.0), 
	                  stride(1000.0), pass(0.0), decay_count(0), start_time_ns(0) {
	}
};

//! SchedulerSlotArray manages the global slot array for stride scheduling.
//! Based on Section 2.3 of "Self-Tuning Query Scheduling for Analytical Workloads".
//!
//! Key properties:
//! - Fixed size (128 slots max)
//! - Lock-free reads for scheduling decisions
//! - Registration/deregistration requires mutex
//! - Sequence number for thread-local cache invalidation
class SchedulerSlotArray {
public:
	//! Stride scheduling constant (determines granularity)
	static constexpr double LARGE_CONSTANT = 1000.0;

	SchedulerSlotArray();

	//===--------------------------------------------------------------------===//
	// Query Registration (requires lock)
	//===--------------------------------------------------------------------===//

	//! Register a query and get a slot index.
	//! Returns -1 if all slots are full (query must wait).
	//! @param executor The query's executor
	//! @param initial_priority Initial priority for this query (default 1.0)
	//! @return Slot index, or -1 if no slots available
	idx_t RegisterQuery(Executor &executor, double initial_priority = 1.0);

	//! Deregister a query and free its slot.
	void DeregisterQuery(idx_t slot_index);

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

	//! Increment decay count.
	void IncrementDecayCount(idx_t slot_index);

	//! Get current values (lock-free reads)
	double GetPriority(idx_t slot_index) const;
	double GetStride(idx_t slot_index) const;
	double GetPass(idx_t slot_index) const;
	int GetDecayCount(idx_t slot_index) const;
	Executor *GetExecutor(idx_t slot_index) const;

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
	//! Global pass value (for initializing new queries)
	atomic<double> global_pass;
};

} // namespace duckdb
