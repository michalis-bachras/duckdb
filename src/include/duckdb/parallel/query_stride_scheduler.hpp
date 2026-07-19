//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_stride_scheduler.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/parallel/event.hpp"

#include <array>
#include <bitset>

namespace duckdb {

class DatabaseInstance;
struct ProducerToken;
class Task;
struct QueryStrideSchedulerState;

static constexpr idx_t QUERY_STRIDE_MAX_SLOTS = 128;
static constexpr idx_t QUERY_STRIDE_QUANTUM_CHUNKS = 50;

enum class QueryStrideDequeueResult : uint8_t { NOT_ACTIVE = 0, NO_TASK, TASK_FOUND };

struct QueryStrideTaskSelection {
	idx_t slot = DConstants::INVALID_INDEX;
	uint64_t generation = 0;
};

struct QueryStrideSchedulerSnapshot {
	idx_t slot = 0;
	uint64_t generation = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double initial_priority = 0;
	double minimum_priority = 0;
	bool static_priority = false;
	bool task_set_active = false;
	bool lifecycle_serial = false;
	idx_t eligible_workers = 0;
	uint64_t task_set_generation = 0;
	uint64_t quanta = 0;
	uint64_t worker_time_us = 0;
};

enum class QueryStrideOptimizerPhase : uint8_t { IDLE = 0, TRACKING = 1, OPTIMIZING = 2 };

struct QueryStrideTuningSnapshot {
	string phase;
	int decay_start = 0;
	double decay_lambda = 0;
	double last_objective = 0;
	uint64_t optimization_count = 0;
	idx_t tracked_query_count = 0;
	uint64_t registered_queries = 0;
	uint64_t unregistered_queries = 0;
	uint64_t activated_task_sets = 0;
	uint64_t finished_task_sets = 0;
	uint64_t change_task_sets = 0;
	uint64_t return_task_sets = 0;
	uint64_t finalization_task_sets = 0;
	uint64_t worker_lazy_deactivations = 0;
};

//! Per-worker STRIDE state. Other workers only write the three notification masks.
struct QueryStrideWorkerState {
	atomic<uint64_t> change_mask_low {0};
	atomic<uint64_t> change_mask_high {0};
	atomic<uint64_t> finalization_mask_low {0};
	atomic<uint64_t> finalization_mask_high {0};
	atomic<uint64_t> return_mask_low {0};
	atomic<uint64_t> return_mask_high {0};

	std::bitset<QUERY_STRIDE_MAX_SLOTS> active_slots;
	std::array<uint64_t, QUERY_STRIDE_MAX_SLOTS> generations {};
	std::array<double, QUERY_STRIDE_MAX_SLOTS> priorities {};
	std::array<double, QUERY_STRIDE_MAX_SLOTS> minimum_priorities {};
	std::array<double, QUERY_STRIDE_MAX_SLOTS> pass_values {};
	std::array<double, QUERY_STRIDE_MAX_SLOTS> strides {};
	std::array<uint64_t, QUERY_STRIDE_MAX_SLOTS> cpu_time_accum_us {};
	std::array<int, QUERY_STRIDE_MAX_SLOTS> decay_steps {};
	std::array<bool, QUERY_STRIDE_MAX_SLOTS> static_priorities {};
	std::array<idx_t, QUERY_STRIDE_MAX_SLOTS> sorted_slots {};
	idx_t sorted_count = 0;
	double local_global_pass = 0;
	double local_global_stride = 0;
	bool needs_full_refresh = true;

	QueryStrideWorkerState();

private:
	friend class QueryStrideScheduler;
	void FullRefresh(QueryStrideScheduler &scheduler);
	void PullUpdates(QueryStrideScheduler &scheduler);
	void ActivateNewSlot(idx_t slot, uint64_t generation, double initial_priority, double minimum_priority,
	                     bool static_priority);
	void DeactivateTaskSet(idx_t slot);
	void ResetSlot(idx_t slot);
	void UpdateAfterQuantum(QueryStrideScheduler &scheduler, idx_t slot, uint64_t generation, uint64_t elapsed_us);
	void RecomputeGlobalStride();
	void RebuildSortedSlots();
};

//! Database-scoped self-tuning STRIDE scheduler for tagged analytical queries.
class QueryStrideScheduler {
public:
	static constexpr double LARGE_CONSTANT = 1000.0;
	static constexpr double REFERENCE_DURATION_MS = 2.0;
	static constexpr double INITIAL_PRIORITY = 10000.0;
	static constexpr double MINIMUM_PRIORITY = 100.0;
	static constexpr int DEFAULT_DECAY_START = 10;
	static constexpr double DEFAULT_DECAY_LAMBDA = 0.9;
	static constexpr double TRACKING_DURATION_MS = 20000.0;
	static constexpr double REFRESH_DURATION_MS = 60000.0;

	explicit QueryStrideScheduler(DatabaseInstance &db);
	~QueryStrideScheduler();

	QueryStrideScheduler(const QueryStrideScheduler &) = delete;
	QueryStrideScheduler &operator=(const QueryStrideScheduler &) = delete;

	bool Enabled() const;
	void RegisterQuery(const QueryRequestMetadata &metadata, ProducerToken &producer, double user_priority);
	void UnregisterQuery(uint64_t db_query_id);
	void OnEventScheduled(uint64_t db_query_id, Event &event);
	void OnEventFinished(uint64_t db_query_id, Event &event);

	void RegisterWorker(QueryStrideWorkerState &worker);
	void DeregisterWorker(QueryStrideWorkerState &worker);
	QueryStrideDequeueResult TryDequeueTask(idx_t worker_id, QueryStrideWorkerState &worker, shared_ptr<Task> &task,
	                                      QueryStrideTaskSelection &selection);
	void OnQuantumCompleted(QueryStrideWorkerState &worker, const QueryStrideTaskSelection &selection,
	                       uint64_t elapsed_us);

	idx_t ActiveQueryCount() const;
	idx_t EligibleWorkerCount(idx_t worker_count) const;
	static idx_t ComputeEligibleWorkerCount(idx_t active_query_count, idx_t worker_count);
	vector<QueryStrideSchedulerSnapshot> GetSnapshot() const;
	QueryStrideTuningSnapshot GetTuningSnapshot() const;

	atomic<int> decay_start {DEFAULT_DECAY_START};
	atomic<double> decay_lambda {DEFAULT_DECAY_LAMBDA};

private:
	friend struct QueryStrideWorkerState;
	bool LoadSlotConfiguration(idx_t slot, uint64_t &generation, double &initial_priority,
	                           double &minimum_priority, bool &static_priority, bool &lifecycle_serial) const;
	bool IsWorkerEligible(idx_t slot, idx_t worker_id, idx_t worker_count) const;
	void PushChange(idx_t slot);
	void PushFinalization(idx_t slot);
	void PushReturn(idx_t slot);
	void MaybeAdvanceOptimizer();

private:
	DatabaseInstance &db;
	unique_ptr<QueryStrideSchedulerState> state;
};

} // namespace duckdb
