//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/energy_attribution/energy_attribution.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class EnergySegmentScope;
class Event;
class Pipeline;
struct EnergyAttributionSettings;

class EnergyAttributionQueryHandle {
public:
	virtual ~EnergyAttributionQueryHandle() = default;
};

struct EnergyPerfSnapshot {
	uint64_t cycles = 0;
	uint64_t instructions = 0;
	uint64_t ref_cycles = 0;
	uint64_t cache_refs = 0;
	uint64_t cache_misses = 0;
	uint64_t llc_misses = 0;
	uint64_t offcore_responses = 0;
	uint64_t time_enabled = 0;
	uint64_t time_running = 0;
	bool valid = false;
	bool scaled = false;
	string status;
};

enum class EnergySegmentRole : uint8_t {
	ASSIGNED_MANDATORY,
	ASSIGNED_OPTIONAL,
	SAME_QUERY_SUCCESSOR,
	FALLBACK_SIBLING,
	FALLBACK_SOCKET,
	FALLBACK_GLOBAL,
	EXPLORATION_PROBE,
	UNKNOWN
};

enum class EnergySegmentPhase : uint8_t {
	EXECUTE,
	INTERNAL,
	INITIALIZE,
	PREPARE_FINISH,
	FINISH
};

enum class EnergySystemCategory : uint8_t {
	NONE,
	SLA_SCHEDULER,
	STRIDE_SCHEDULER,
	ENERGY_SAMPLER
};

struct EnergySegmentRecord {
	uint64_t segment_id = 0;
	uint64_t plan_version = 0;
	uint64_t worker_id = 0;
	int64_t linux_tid = 0;
	int socket_id = -1;
	int physical_core_id = -1;
	int logical_cpu_id = -1;
	int end_socket_id = -1;
	int end_physical_core_id = -1;
	int end_logical_cpu_id = -1;
	uint64_t query_id = 0;
	uint64_t pipeline_id = 0;
	string pipeline_signature;
	EnergySegmentRole role = EnergySegmentRole::UNKNOWN;
	EnergySegmentPhase phase = EnergySegmentPhase::EXECUTE;
	EnergySystemCategory system_category = EnergySystemCategory::NONE;
	uint64_t lifecycle_group_id = 0;
	uint64_t owner_pipeline_id = 0;
	uint64_t lifecycle_member_count = 0;
	vector<uint64_t> lifecycle_member_pipeline_ids;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	double duration_s = 0;
	uint64_t work_units = 0;
	uint64_t tuples = 0;
	uint64_t chunks = 0;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_occupancy = 1;
	uint64_t cycles = 0;
	uint64_t instructions = 0;
	uint64_t ref_cycles = 0;
	uint64_t cache_refs = 0;
	uint64_t cache_misses = 0;
	uint64_t llc_misses = 0;
	uint64_t offcore_responses = 0;
	bool counters_valid = false;
	bool counters_scaled = false;
	bool hardware_state_stable = false;
	bool migrated = false;
	string counter_status;
};

class EnergyAttributionManager {
public:
	static bool Enabled(const ClientContext &context);
	static void ConfigureDatabaseRuntime(ClientContext &context);
	static void ShutdownDatabaseRuntime(DatabaseInstance &db);
	static void RegisterSchedulerWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid, int logical_cpu);
	static void UnregisterSchedulerWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid);
	static void BeginQuery(ClientContext &context, uint64_t query_id, const string &query);
	static void EndQuery(ClientContext &context);
	static void AttachPipeline(ClientContext &context, Pipeline &pipeline, uint64_t profiler_pipeline_id);
	static bool LifecyclePhasesEnabled(const ClientContext &context);
	static uint64_t GetPipelineAttributionId(Pipeline &pipeline);
};

class EnergySegmentScope {
public:
	explicit EnergySegmentScope(Pipeline &pipeline, int start_cpu_hint = -1);
	EnergySegmentScope(Pipeline &pipeline, Event &event, EnergySegmentPhase phase, int start_cpu_hint = -1);
	EnergySegmentScope(Pipeline &pipeline, EnergySegmentPhase phase, uint64_t lifecycle_group_id,
	                   const vector<uint64_t> &lifecycle_member_pipeline_ids, int start_cpu_hint = -1);
	~EnergySegmentScope();

	EnergySegmentScope(const EnergySegmentScope &) = delete;
	EnergySegmentScope &operator=(const EnergySegmentScope &) = delete;

	void SetWork(uint64_t tuples, uint64_t chunks);
	void SetEndCPUHint(int end_cpu_hint);

private:
	shared_ptr<EnergyAttributionQueryHandle> query_handle;
	void *query_state = nullptr;
	void *segment_buffer = nullptr;
	EnergySegmentRecord record;
	EnergyPerfSnapshot start_perf;
	int end_cpu_hint = -1;
	bool active = false;
	bool perf_started = false;
	bool finished = false;
};

//! Query-independent database work is measured separately and is never charged to a query profile.
class EnergySystemSegmentScope {
public:
	EnergySystemSegmentScope(DatabaseInstance &db, EnergySystemCategory category);
	~EnergySystemSegmentScope();

	EnergySystemSegmentScope(const EnergySystemSegmentScope &) = delete;
	EnergySystemSegmentScope &operator=(const EnergySystemSegmentScope &) = delete;

private:
	EnergySegmentRecord record;
	bool active = false;
};

const char *EnergySegmentRoleToString(EnergySegmentRole role);
const char *EnergySegmentPhaseToString(EnergySegmentPhase phase);
const char *EnergySystemCategoryToString(EnergySystemCategory category);

} // namespace duckdb
