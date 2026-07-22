//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_sla_scheduler.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/task.hpp"

namespace duckdb {

class DatabaseInstance;
class Event;
class QueryHardwareManager;
struct ProducerToken;
struct QuerySLASchedulerState;

enum class QuerySLADequeueResult : uint8_t { NOT_ACTIVE = 0, TASK_FOUND, NO_TASK };

struct QuerySLAWorkerSelection {
	QueryRequestHardwareConfiguration hardware;
	uint64_t plan_generation = 0;
	bool assigned = false;
	bool mandatory = false;
	bool optional = false;
	bool liveness = false;
	bool exploration = false;
};

struct QuerySLASchedulerSnapshot {
	uint64_t epoch_generation = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_generation = 0;
	string event_kind;
	bool model_valid = false;
	string model_error;
	idx_t demand_cap = 0;
	idx_t remaining_work_units = 0;
	double selected_throughput = 0;
	bool throughput_is_live = false;
	idx_t mandatory_workers = 0;
	idx_t optional_workers = 0;
	idx_t residual_workers = 0;
	idx_t assigned_workers = 0;
	double next_mandatory_gain = 0;
	double next_optional_gain = 0;
};

struct QuerySLASchedulerEpochSnapshot {
	string scheduler_policy;
	uint64_t epoch_generation = 0;
	uint64_t epoch_timestamp_ns = 0;
	uint64_t epoch_compute_ns = 0;
	uint64_t epoch_wall_ns = 0;
	uint64_t epoch_thread_cpu_ns = 0;
	uint64_t capture_lock_wait_ns = 0;
	uint64_t capture_lock_hold_ns = 0;
	uint64_t work_snapshot_ns = 0;
	uint64_t suffix_prepare_ns = 0;
	uint64_t model_build_ns = 0;
	uint64_t allocation_ns = 0;
	uint64_t publish_lock_wait_ns = 0;
	uint64_t publish_lock_hold_ns = 0;
	uint64_t hardware_apply_ns = 0;
	uint64_t trace_build_ns = 0;
	uint64_t trace_lock_wait_ns = 0;
	uint64_t trace_lock_hold_ns = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	uint64_t deadline_ns = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	uint64_t pipeline_generation = 0;
	string event_kind;
	bool model_valid = false;
	string model_error;
	idx_t demand_cap = 0;
	idx_t remaining_work_units = 0;
	double selected_throughput = 0;
	bool throughput_is_live = false;
	string historical_throughput_level;
	idx_t historical_throughput_sample_count = 0;
	string continuation_level;
	string continuation_kind;
	idx_t continuation_sample_count = 0;
	double continuation_p90 = 0;
	string suffix_primary_level;
	idx_t suffix_exact_sample_count = 0;
	idx_t suffix_scale_sample_count = 0;
	idx_t suffix_global_sample_count = 0;
	double suffix_exact_weight = 0;
	double suffix_scale_weight = 0;
	double suffix_global_weight = 0;
	idx_t suffix_bucket_count = 0;
	double suffix_mean_ns = 0;
	double suffix_p90_ns = 0;
	idx_t mandatory_workers = 0;
	idx_t optional_workers = 0;
	idx_t residual_workers = 0;
	idx_t liveness_workers = 0;
	idx_t planned_workers = 0;
	idx_t assigned_workers = 0;
	idx_t energy_mandatory_workers = 0;
	idx_t energy_o1_workers = 0;
	idx_t energy_o2_workers = 0;
	idx_t energy_o3_workers = 0;
	idx_t energy_liveness_workers = 0;
	idx_t energy_parked_workers = 0;
	idx_t energy_core_probes = 0;
	idx_t energy_uncore_probes = 0;
	idx_t energy_forced_uncore_probes = 0;
	bool energy_optional_only_socket_available = false;
	idx_t energy_epochs_without_optional_only_socket = 0;
	idx_t energy_profile_fallback_count = 0;
	idx_t energy_immature_pair_rejections = 0;
	idx_t energy_domain_pair_promotions = 0;
	double energy_mandatory_footprint_j = 0;
	double energy_optional_incremental_j = 0;
	double predicted_service_rate = 0;
	double predicted_active_power_w = 0;
	double predicted_epoch_energy_j = 0;
	double predicted_optional_risk = 0;
	double first_mandatory_gain = 0;
	double first_optional_gain = 0;
	double last_mandatory_gain = 0;
	double last_optional_gain = 0;
	double next_mandatory_gain = 0;
	double next_optional_gain = 0;
	double predicted_pipeline_finish_ns = 0;
	double predicted_query_finish_mean_ns = 0;
	double predicted_query_finish_p90_ns = 0;
	double predicted_sla_cost = 0;
};

struct QuerySLAEnergyWorkerEpochSnapshot {
	uint64_t epoch_generation = 0;
	uint64_t epoch_timestamp_ns = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	uint64_t pipeline_generation = 0;
	idx_t worker_id = 0;
	int logical_cpu = -1;
	int socket_id = -1;
	int physical_core_id = -1;
	bool mandatory = false;
	bool optional = false;
	bool liveness = false;
	bool exploration = false;
	QueryRequestHardwareConfiguration normal_hardware;
	QueryRequestHardwareConfiguration execution_hardware;
	QueryRequestHardwareConfiguration applied_hardware;
};

struct QuerySLAExplorationDiagnosticsSnapshot {
	uint64_t core_probe_epochs = 0;
	uint64_t optional_uncore_probe_epochs = 0;
	uint64_t forced_uncore_probe_epochs = 0;
	idx_t epochs_without_optional_only_socket = 0;
};

//! Database-scoped SLA scheduler for tagged analytical queries. One worker becomes the epoch leader through a CAS;
//! other workers consume the immutable allocation and use the mutable dispatcher between epochs.
class QuerySLAScheduler {
public:
	explicit QuerySLAScheduler(DatabaseInstance &db);
	~QuerySLAScheduler();

	QuerySLAScheduler(const QuerySLAScheduler &) = delete;
	QuerySLAScheduler &operator=(const QuerySLAScheduler &) = delete;

	bool Enabled() const;
	idx_t EpochMs() const;
	idx_t ActiveQueryCount() const;

	void RegisterQuery(const QueryRequestMetadata &metadata, ProducerToken &producer, bool debug_trace_enabled);
	void UnregisterQuery(uint64_t db_query_id);
	void OnEventScheduled(uint64_t db_query_id, shared_ptr<Event> event, idx_t remaining_suffix_stages);
	void OnEventFinished(uint64_t db_query_id, Event &event);

	QuerySLADequeueResult TryDequeueTask(idx_t worker_id, shared_ptr<Task> &task,
	                                   QuerySLAWorkerSelection *selection = nullptr);
	void RequestInitialEpoch();
	void RegisterWorker(idx_t worker_id, int logical_cpu);
	void UnregisterWorker(idx_t worker_id);
	void ActivateEnergyPolicy();
	void DeactivateEnergyPolicy();
	QueryHardwareManager &GetHardwareManager();

	vector<QuerySLASchedulerSnapshot> GetSnapshot() const;
	vector<QuerySLASchedulerEpochSnapshot> GetEpochTrace() const;
	vector<QuerySLAEnergyWorkerEpochSnapshot> GetEnergyWorkerEpochTrace() const;
	QuerySLAExplorationDiagnosticsSnapshot GetExplorationDiagnostics() const;
	uint64_t EpochTraceDroppedCount() const;
	uint64_t EnergyWorkerEpochTraceDroppedCount() const;
	void ClearEpochTrace();
	uint64_t EpochRunCount() const;

private:
	void MaybeRunEpoch();
	void RunEpoch(uint64_t now_ns);

private:
	DatabaseInstance &db;
	unique_ptr<QuerySLASchedulerState> state;
};

} // namespace duckdb
