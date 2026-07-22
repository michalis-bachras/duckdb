//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_sla_scheduler.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_sla_scheduler.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/query_sla_energy_planner.hpp"
#include "duckdb/parallel/query_sla_model.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <mutex>
#include <queue>
#include <ctime>
#include <unordered_map>

namespace duckdb {

struct QuerySLADispatchEntry {
	QuerySLADispatchEntry(double gain_p, uint64_t db_query_id_p, uint64_t version_p, idx_t workers_p)
	    : gain(gain_p), db_query_id(db_query_id_p), version(version_p), workers(workers_p) {
	}

	double gain;
	uint64_t db_query_id;
	uint64_t version;
	idx_t workers;

	bool operator<(const QuerySLADispatchEntry &other) const {
		if (gain != other.gain) {
			return gain < other.gain;
		}
		return db_query_id > other.db_query_id;
	}
};

struct QuerySLAResidualDispatchEntry {
	QuerySLAResidualDispatchEntry(double mandatory_gain_p, double optional_gain_p, uint64_t db_query_id_p,
	                              uint64_t version_p, idx_t workers_p)
	    : mandatory_gain(mandatory_gain_p), optional_gain(optional_gain_p), db_query_id(db_query_id_p),
	      version(version_p), workers(workers_p) {
	}

	double mandatory_gain;
	double optional_gain;
	uint64_t db_query_id;
	uint64_t version;
	idx_t workers;

	bool operator<(const QuerySLAResidualDispatchEntry &other) const {
		if (mandatory_gain != other.mandatory_gain) {
			return mandatory_gain < other.mandatory_gain;
		}
		if (optional_gain != other.optional_gain) {
			return optional_gain < other.optional_gain;
		}
		return db_query_id > other.db_query_id;
	}
};

namespace {

static uint64_t SLATimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static uint64_t SLAThreadCpuNs() {
#ifdef __linux__
	struct timespec ts;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
		return 0;
	}
	return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
#else
	return 0;
#endif
}

static uint64_t SLADurationNs(uint64_t end_ns, uint64_t start_ns) {
	return end_ns >= start_ns ? end_ns - start_ns : 0;
}

static const char *SLAEventKindName(QueryActivationEventKind kind) {
	switch (kind) {
	case QueryActivationEventKind::INITIALIZE:
		return "initialize";
	case QueryActivationEventKind::PIPELINE:
		return "pipeline";
	case QueryActivationEventKind::PREPARE_FINISH:
		return "prepare_finish";
	case QueryActivationEventKind::FINISH:
		return "finish";
	case QueryActivationEventKind::COMPLETE:
		return "complete";
	case QueryActivationEventKind::INTERNAL:
		return "internal";
	default:
		return "unregistered";
	}
}

struct QueryCapture {
	QueryRequestMetadata metadata;
	ProducerToken *producer = nullptr;
	shared_ptr<Event> event;
	uint64_t generation = 0;
	idx_t remaining_suffix_stages = 1;
	bool debug_trace_enabled = false;
};

static const char *SLASuffixLevelName(DownstreamSuffixProfileLevel level) {
	switch (level) {
	case DownstreamSuffixProfileLevel::EXACT:
		return "exact";
	case DownstreamSuffixProfileLevel::SCALE_FACTOR:
		return "scale_factor";
	case DownstreamSuffixProfileLevel::GLOBAL:
		return "global";
	default:
		return "none";
	}
}

struct EpochTraceInput {
	idx_t capture_index = 0;
	bool published = false;
	idx_t assigned_workers = 0;
	QueryActivationEventKind event_kind = QueryActivationEventKind::UNREGISTERED;
	bool model_valid = false;
	string model_error;
	idx_t demand_cap = 0;
	idx_t remaining_work_units = 0;
	double selected_throughput = 0;
	bool throughput_is_live = false;
	double predicted_sla_cost = 0;
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
};

} // namespace

struct QuerySLASchedulerState {
	static constexpr idx_t MAX_EPOCH_TRACE_ROWS = 250000;
	static constexpr idx_t MAX_ENERGY_WORKER_EPOCH_TRACE_ROWS = 500000;

	struct WorkerAssignment {
		uint64_t db_query_id = 0;
		uint64_t pipeline_generation = 0;
		QueryRequestHardwareConfiguration normal_hardware;
		QueryRequestHardwareConfiguration execution_hardware;
		uint64_t plan_generation = 0;
		bool mandatory = false;
		bool optional = false;
		bool liveness = false;
		bool exploration = false;
	};

	struct QueryState {
		QueryRequestMetadata metadata;
		ProducerToken *producer = nullptr;
		shared_ptr<Event> event;
		uint64_t pipeline_generation = 0;
		idx_t remaining_suffix_stages = 1;
		QuerySLAModel model;
		idx_t mandatory_workers = 0;
		idx_t optional_workers = 0;
		idx_t residual_workers = 0;
		idx_t liveness_workers = 0;
		idx_t assigned_workers = 0;
		vector<QuerySLAEnergyWorkerAssignment> energy_workers;
		bool energy_limited_plan = false;
		bool residual_dispatch_enabled = true;
		bool topology_plan_published = false;
		uint64_t dispatch_version = 0;
		bool continuation_pending = false;
		bool debug_trace_enabled = false;
	};

	mutable mutex lock;
	std::unordered_map<uint64_t, QueryState> queries;
	vector<WorkerAssignment> worker_assignments;
	std::priority_queue<QuerySLADispatchEntry> primary_mandatory;
	std::priority_queue<QuerySLADispatchEntry> primary_optional;
	std::priority_queue<QuerySLADispatchEntry> dynamic_mandatory;
	std::priority_queue<QuerySLADispatchEntry> dynamic_optional;
	std::priority_queue<QuerySLAResidualDispatchEntry> dynamic_residual;
	atomic<uint64_t> next_epoch_ns {0};
	atomic<bool> epoch_leader {false};
	atomic<uint64_t> epoch_generation {0};
	atomic<uint64_t> epoch_run_count {0};
	atomic<uint64_t> debug_trace_query_count {0};
	mutable mutex epoch_trace_lock;
	std::deque<QuerySLASchedulerEpochSnapshot> epoch_trace;
	uint64_t epoch_trace_dropped_count = 0;
	std::deque<QuerySLAEnergyWorkerEpochSnapshot> energy_worker_epoch_trace;
	uint64_t energy_worker_epoch_trace_dropped_count = 0;
	uint64_t exploration_core_probe_epochs = 0;
	uint64_t exploration_optional_uncore_probe_epochs = 0;
	uint64_t exploration_forced_uncore_probe_epochs = 0;
	idx_t epochs_without_optional_only_socket = 0;
	unique_ptr<QueryHardwareManager> hardware_manager;
};

static idx_t PlannedWorkers(const QuerySLASchedulerState::QueryState &query) {
	return query.mandatory_workers + query.optional_workers + query.residual_workers + query.liveness_workers;
}

static QuerySLAEnergyWorkerAssignment EnergyAssignmentAt(const QuerySLASchedulerState::QueryState &query,
	                                                      idx_t worker_index) {
	if (worker_index < query.energy_workers.size()) {
		return query.energy_workers[worker_index];
	}
	return QuerySLAEnergyWorkerAssignment();
}

static void SetWorkerSelection(const QuerySLASchedulerState::WorkerAssignment &assignment,
	                           QuerySLAWorkerSelection *selection) {
	if (!selection) {
		return;
	}
	selection->hardware = assignment.execution_hardware.IsValid() ? assignment.execution_hardware
	                                                           : assignment.normal_hardware;
	selection->plan_generation = assignment.plan_generation;
	selection->assigned = assignment.db_query_id != 0;
	selection->mandatory = assignment.mandatory;
	selection->optional = assignment.optional;
	selection->liveness = assignment.liveness;
	selection->exploration = assignment.exploration;
}

static void RefreshDispatchEntries(QuerySLASchedulerState &state, QuerySLASchedulerState::QueryState &query) {
	query.dispatch_version++;
	if (!query.event || !query.model.valid || query.model.demand_cap == 0) {
		return;
	}
	auto planned = MinValue<idx_t>(PlannedWorkers(query), query.model.demand_cap);
	if (query.assigned_workers < planned) {
		auto workers = query.assigned_workers;
		if (workers < query.mandatory_workers) {
			auto gain = workers < query.model.mandatory_gain.size() ? query.model.mandatory_gain[workers] : 0;
			state.primary_mandatory.emplace(gain, query.metadata.db_query_id, query.dispatch_version, workers);
		} else if (!query.energy_limited_plan || (!query.topology_plan_published && query.liveness_workers > 0)) {
			auto gain = workers < query.model.optional_gain.size() ? query.model.optional_gain[workers] : 0;
			state.primary_optional.emplace(gain, query.metadata.db_query_id, query.dispatch_version, workers);
		}
	}
	if (query.energy_limited_plan) {
		if (query.assigned_workers >= planned && planned < query.model.demand_cap &&
		    planned < query.model.mandatory_gain.size() && query.model.mandatory_gain[planned] > 0) {
			state.dynamic_mandatory.emplace(query.model.mandatory_gain[planned], query.metadata.db_query_id,
			                                query.dispatch_version, planned);
		}
		return;
	}
	if (planned >= query.model.demand_cap) {
		return;
	}
	if (planned < query.model.mandatory_gain.size() && query.model.mandatory_gain[planned] > 0) {
		state.dynamic_mandatory.emplace(query.model.mandatory_gain[planned], query.metadata.db_query_id,
		                                query.dispatch_version, planned);
	}
	if (planned < query.model.optional_gain.size() && query.model.optional_gain[planned] > 0) {
		state.dynamic_optional.emplace(query.model.optional_gain[planned], query.metadata.db_query_id,
		                               query.dispatch_version, planned);
	}
	if (query.residual_dispatch_enabled && planned < query.model.mandatory_gain.size() &&
	    planned < query.model.optional_gain.size()) {
		state.dynamic_residual.emplace(query.model.mandatory_gain[planned], query.model.optional_gain[planned],
		                               query.metadata.db_query_id, query.dispatch_version, planned);
	}
}

static void ResetDispatchEntries(QuerySLASchedulerState &state) {
	state.primary_mandatory = std::priority_queue<QuerySLADispatchEntry>();
	state.primary_optional = std::priority_queue<QuerySLADispatchEntry>();
	state.dynamic_mandatory = std::priority_queue<QuerySLADispatchEntry>();
	state.dynamic_optional = std::priority_queue<QuerySLADispatchEntry>();
	state.dynamic_residual = std::priority_queue<QuerySLAResidualDispatchEntry>();
	for (auto &entry : state.queries) {
		RefreshDispatchEntries(state, entry.second);
	}
}

QuerySLAScheduler::QuerySLAScheduler(DatabaseInstance &db_p)
    : db(db_p), state(make_uniq<QuerySLASchedulerState>()) {
	state->hardware_manager = make_uniq<QueryHardwareManager>(db);
}

QuerySLAScheduler::~QuerySLAScheduler() {
	state->hardware_manager->Deactivate();
}

bool QuerySLAScheduler::Enabled() const {
	return IsQuerySLAPolicy(db.GetQuerySchedulerPolicy());
}

idx_t QuerySLAScheduler::EpochMs() const {
	return db.config.options.query_sla_scheduler_epoch_ms;
}

idx_t QuerySLAScheduler::ActiveQueryCount() const {
	lock_guard<mutex> guard(state->lock);
	return state->queries.size();
}

void QuerySLAScheduler::RegisterQuery(const QueryRequestMetadata &metadata, ProducerToken &producer,
                                      bool debug_trace_enabled) {
	if (!Enabled() || !metadata.valid) {
		return;
	}
	lock_guard<mutex> guard(state->lock);
	auto &query = state->queries[metadata.db_query_id];
	if (!query.debug_trace_enabled && debug_trace_enabled) {
		state->debug_trace_query_count++;
	} else if (query.debug_trace_enabled && !debug_trace_enabled) {
		state->debug_trace_query_count--;
	}
	query.metadata = metadata;
	query.producer = &producer;
	query.residual_dispatch_enabled =
	    db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA && db.config.options.query_sla_residual_workers_enabled;
	query.debug_trace_enabled = debug_trace_enabled;
	state->next_epoch_ns.store(0);
}

void QuerySLAScheduler::UnregisterQuery(uint64_t db_query_id) {
	lock_guard<mutex> guard(state->lock);
	auto entry = state->queries.find(db_query_id);
	if (entry == state->queries.end()) {
		return;
	}
	for (auto &assignment : state->worker_assignments) {
		if (assignment.db_query_id == db_query_id) {
			assignment = QuerySLASchedulerState::WorkerAssignment();
		}
	}
	if (entry->second.debug_trace_enabled) {
		state->debug_trace_query_count--;
	}
	state->queries.erase(entry);
}

void QuerySLAScheduler::OnEventScheduled(uint64_t db_query_id, shared_ptr<Event> event,
	                                     idx_t remaining_suffix_stages) {
	if (!Enabled()) {
		return;
	}
	QueryCapture capture;
	bool has_predecessor = false;
	{
		lock_guard<mutex> guard(state->lock);
		auto entry = state->queries.find(db_query_id);
		if (entry == state->queries.end()) {
			throw InternalException("SLA scheduler received an event for an unregistered query");
		}
		auto &query = entry->second;
		has_predecessor = query.event != nullptr || query.continuation_pending;
		query.continuation_pending = false;
		query.event = event;
		query.pipeline_generation++;
		query.assigned_workers = 0;
		query.remaining_suffix_stages = MaxValue<idx_t>(idx_t(1), remaining_suffix_stages);
		capture.metadata = query.metadata;
		capture.producer = query.producer;
		capture.event = event;
		capture.generation = query.pipeline_generation;
		capture.remaining_suffix_stages = query.remaining_suffix_stages;
		capture.debug_trace_enabled = query.debug_trace_enabled;
	}

	PipelineWorkSnapshot work;
	if (!event->GetPipelineWorkSnapshot(work)) {
		throw InternalException("SLA scheduler could not read active pipeline work state");
	}
	if (event->GetQueryActivationKind() == QueryActivationEventKind::PIPELINE && !work.profile_identity.valid) {
		throw InvalidInputException("SLA scheduler requires a stable pipeline profile identity");
	}
	QuerySLAModelInput model_input;
	model_input.metadata = capture.metadata;
	model_input.work = work;
	model_input.event_kind = event->GetQueryActivationKind();
	model_input.generation = capture.generation;
	auto model = BuildProvisionalQuerySLAModel(model_input);
	if (!model.valid) {
		lock_guard<mutex> guard(state->lock);
		auto entry = state->queries.find(db_query_id);
		if (entry != state->queries.end() && entry->second.pipeline_generation == capture.generation) {
			entry->second.model = model;
		}
		throw InvalidInputException("SLA scheduler cannot model query %llu pipeline %llu: %s",
		                            static_cast<unsigned long long>(db_query_id),
		                            static_cast<unsigned long long>(work.pipeline_id), model.error);
	}
	lock_guard<mutex> guard(state->lock);
	auto entry = state->queries.find(db_query_id);
	if (entry == state->queries.end() || entry->second.pipeline_generation != capture.generation) {
		return;
	}
	auto &query = entry->second;
	query.model = std::move(model);
	auto planned = MinValue<idx_t>(has_predecessor ? idx_t(1) : idx_t(0), query.model.demand_cap);
	query.mandatory_workers = 0;
	while (query.mandatory_workers < planned && query.mandatory_workers < query.model.mandatory_gain.size() &&
	       query.model.mandatory_gain[query.mandatory_workers] > 0) {
		query.mandatory_workers++;
	}
	query.optional_workers = planned - query.mandatory_workers;
	query.residual_workers = 0;
	query.liveness_workers = 0;
	query.energy_workers.clear();
	query.energy_limited_plan = db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY;
	query.residual_dispatch_enabled =
	    !query.energy_limited_plan && db.config.options.query_sla_residual_workers_enabled;
	query.topology_plan_published = false;
	if (query.energy_limited_plan && planned > 0) {
		QuerySLAEnergyWorkerAssignment worker;
		worker.hardware = state->hardware_manager->ReferenceConfiguration();
		worker.mandatory = query.mandatory_workers > 0;
		worker.liveness = !worker.mandatory;
		query.optional_workers = 0;
		query.liveness_workers = worker.liveness ? 1 : 0;
		query.energy_workers.push_back(worker);
	}
	RefreshDispatchEntries(*state, query);
}

void QuerySLAScheduler::OnEventFinished(uint64_t db_query_id, Event &event) {
	if (!Enabled()) {
		return;
	}
	lock_guard<mutex> guard(state->lock);
	auto entry = state->queries.find(db_query_id);
	if (entry == state->queries.end() || entry->second.event.get() != &event) {
		return;
	}
	entry->second.event.reset();
	entry->second.model = QuerySLAModel();
	entry->second.mandatory_workers = 0;
	entry->second.optional_workers = 0;
	entry->second.residual_workers = 0;
	entry->second.liveness_workers = 0;
	entry->second.assigned_workers = 0;
	entry->second.energy_workers.clear();
	entry->second.energy_limited_plan = false;
	entry->second.topology_plan_published = false;
	entry->second.pipeline_generation++;
	entry->second.continuation_pending = true;
	RefreshDispatchEntries(*state, entry->second);
}

void QuerySLAScheduler::RequestInitialEpoch() {
	if (Enabled()) {
		state->next_epoch_ns.store(0);
	}
}

void QuerySLAScheduler::RegisterWorker(idx_t worker_id, int logical_cpu) {
	state->hardware_manager->RegisterWorker(worker_id, logical_cpu);
}

void QuerySLAScheduler::UnregisterWorker(idx_t worker_id) {
	state->hardware_manager->UnregisterWorker(worker_id);
}

void QuerySLAScheduler::ActivateEnergyPolicy() {
	state->hardware_manager->Activate();
}

void QuerySLAScheduler::DeactivateEnergyPolicy() {
	state->hardware_manager->Deactivate();
}

QueryHardwareManager &QuerySLAScheduler::GetHardwareManager() {
	return *state->hardware_manager;
}

void QuerySLAScheduler::MaybeRunEpoch() {
	if (!Enabled()) {
		return;
	}
	auto now_ns = SLATimestampNs();
	auto next_ns = state->next_epoch_ns.load();
	if (next_ns != 0 && now_ns < next_ns) {
		return;
	}
	bool expected = false;
	if (!state->epoch_leader.compare_exchange_strong(expected, true)) {
		return;
	}
	try {
		RunEpoch(now_ns);
	} catch (...) {
		state->next_epoch_ns.store(now_ns + EpochMs() * 1000000ULL);
		state->epoch_leader.store(false);
		throw;
	}
	state->next_epoch_ns.store(now_ns + EpochMs() * 1000000ULL);
	state->epoch_run_count++;
	state->epoch_leader.store(false);
}

void QuerySLAScheduler::RunEpoch(uint64_t now_ns) {
	EnergySystemSegmentScope energy_scope(db, EnergySystemCategory::SLA_SCHEDULER);
	const auto collect_diagnostics = state->debug_trace_query_count.load() > 0;
	const auto thread_cpu_start_ns = collect_diagnostics ? SLAThreadCpuNs() : 0;
	const auto capture_wait_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	uint64_t capture_lock_wait_ns = 0;
	uint64_t capture_lock_hold_ns = 0;
	vector<QueryCapture> captures;
	idx_t epochs_without_optional_only_socket = 0;
	{
		unique_lock<mutex> guard(state->lock);
		const auto capture_lock_acquired_ns = collect_diagnostics ? SLATimestampNs() : 0;
		capture_lock_wait_ns = SLADurationNs(capture_lock_acquired_ns, capture_wait_start_ns);
		captures.reserve(state->queries.size());
		epochs_without_optional_only_socket = state->epochs_without_optional_only_socket;
		for (const auto &entry : state->queries) {
			if (!entry.second.event) {
				continue;
			}
			QueryCapture capture;
			capture.metadata = entry.second.metadata;
			capture.producer = entry.second.producer;
			capture.event = entry.second.event;
			capture.generation = entry.second.pipeline_generation;
			capture.remaining_suffix_stages = entry.second.remaining_suffix_stages;
			capture.debug_trace_enabled = entry.second.debug_trace_enabled;
			captures.push_back(std::move(capture));
		}
		if (collect_diagnostics) {
			capture_lock_hold_ns = SLADurationNs(SLATimestampNs(), capture_lock_acquired_ns);
		}
	}
	if (captures.empty()) {
		return;
	}

	const auto work_snapshot_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	vector<PipelineWorkSnapshot> work(captures.size());
	vector<DownstreamSuffixEpochRequest> suffix_requests;
	vector<idx_t> suffix_indexes(captures.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < captures.size(); i++) {
		if (!captures[i].event->GetPipelineWorkSnapshot(work[i])) {
			continue;
		}
		auto event_kind = captures[i].event->GetQueryActivationKind();
		if ((event_kind != QueryActivationEventKind::PIPELINE && event_kind != QueryActivationEventKind::INTERNAL) ||
		    !work[i].profile_identity.valid) {
			continue;
		}
		suffix_indexes[i] = suffix_requests.size();
		suffix_requests.emplace_back(captures[i].metadata.template_id, captures[i].metadata.scale_factor,
		                             work[i].profile_identity.pipeline_id,
		                             work[i].profile_identity.pipeline_signature_hash,
		                             captures[i].remaining_suffix_stages);
	}
	const auto work_snapshot_ns = collect_diagnostics ? SLADurationNs(SLATimestampNs(), work_snapshot_start_ns) : 0;
	const auto suffix_prepare_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	auto suffix_estimates = db.GetQueryRequestProfileStore().PrepareDownstreamSuffixEpoch(suffix_requests);
	const auto suffix_prepare_ns =
	    collect_diagnostics ? SLADurationNs(SLATimestampNs(), suffix_prepare_start_ns) : 0;
	const auto model_build_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	vector<DownstreamSuffixEstimate> resolved_suffixes(captures.size());
	vector<QuerySLAModelInput> model_inputs;
	model_inputs.reserve(captures.size());
	vector<QuerySLAModel> models;
	models.reserve(captures.size());
	for (idx_t i = 0; i < captures.size(); i++) {
		DownstreamSuffixEstimate suffix;
		auto event_kind = captures[i].event->GetQueryActivationKind();
		if (event_kind == QueryActivationEventKind::PIPELINE || event_kind == QueryActivationEventKind::INTERNAL) {
			if (suffix_indexes[i] != DConstants::INVALID_INDEX && suffix_indexes[i] < suffix_estimates.size()) {
					suffix = suffix_estimates[suffix_indexes[i]];
				}
			} else {
				suffix = QuerySLALifecycleSuffixPointMass();
			}
		resolved_suffixes[i] = suffix;
		QuerySLAModelInput model_input;
		model_input.metadata = captures[i].metadata;
		model_input.work = work[i];
		model_input.suffix = suffix;
		model_input.event_kind = event_kind;
		model_input.generation = captures[i].generation;
		model_input.now_ns = now_ns;
		model_input.epoch_ns = EpochMs() * 1000000ULL;
		model_inputs.push_back(model_input);
		models.push_back(BuildQuerySLAModel(model_input));
	}
	const auto model_build_ns = collect_diagnostics ? SLADurationNs(SLATimestampNs(), model_build_start_ns) : 0;

	const auto allocation_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	auto worker_budget = NumericCast<idx_t>(db.GetScheduler().NumberOfThreads());
	const auto energy_policy = db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY;
	vector<idx_t> mandatory(captures.size(), 0);
	vector<idx_t> optional(captures.size(), 0);
	vector<idx_t> residual(captures.size(), 0);
	vector<idx_t> liveness(captures.size(), 0);
	vector<vector<QuerySLAEnergyWorkerAssignment>> energy_workers(captures.size());
	QuerySLAEnergyEpochPlan energy_epoch_plan;
	vector<QueryHardwareWorkerTopology> energy_topology;
	auto published_generation = state->epoch_generation.load() + 1;
	if (energy_policy) {
		QuerySLAEnergyPlannerInput planner_input;
		planner_input.queries.reserve(captures.size());
		auto reference_hardware = state->hardware_manager->ReferenceConfiguration();
		for (idx_t i = 0; i < captures.size(); i++) {
			QuerySLAEnergyModelInput input;
			input.sla_input = model_inputs[i];
			input.reference_model = models[i];
			input.reference_hardware = reference_hardware;
			if (work[i].profile_identity.valid) {
				input.hardware_profiles = db.GetQueryRequestProfileStore().GetPipelineHardwareProfiles(
				    captures[i].metadata.template_id, captures[i].metadata.scale_factor, work[i].profile_identity);
			}
			planner_input.queries.push_back(std::move(input));
		}
		energy_topology = state->hardware_manager->GetWorkerTopology();
		planner_input.topology = energy_topology;
		planner_input.power_model = state->hardware_manager->GetPowerModelSnapshot();
		planner_input.core_frequency_levels_khz = state->hardware_manager->CoreFrequencyLevelsKHz();
		planner_input.uncore_frequency_levels_khz = state->hardware_manager->UncoreFrequencyLevelsKHz();
		planner_input.worker_budget = worker_budget;
		planner_input.epoch_ns = EpochMs() * 1000000ULL;
		planner_input.epoch_generation = published_generation;
		planner_input.energy_lambda = db.config.options.query_sla_energy_lambda;
		planner_input.exploration_enabled = db.config.options.query_sla_energy_exploration_enabled;
		planner_input.exploration_seed = db.config.options.query_sla_energy_exploration_seed;
		planner_input.epochs_without_optional_only_socket = epochs_without_optional_only_socket;
		energy_epoch_plan = PlanQuerySLAEnergyEpoch(planner_input);
		if (!energy_epoch_plan.valid) {
			throw InvalidInputException("SLA-energy epoch planning failed: %s", energy_epoch_plan.error);
		}
		for (idx_t i = 0; i < energy_epoch_plan.queries.size(); i++) {
			mandatory[i] = energy_epoch_plan.queries[i].mandatory_workers;
			optional[i] = energy_epoch_plan.queries[i].optional_workers;
			liveness[i] = energy_epoch_plan.queries[i].liveness_workers;
			energy_workers[i] = energy_epoch_plan.queries[i].workers;
		}
	} else {
		auto allocation = AllocateQuerySLAWorkers(models, worker_budget,
		                                          db.config.options.query_sla_residual_workers_enabled);
		mandatory = std::move(allocation.mandatory);
		optional = std::move(allocation.optional);
		residual = std::move(allocation.residual);
	}
	const auto allocation_ns = collect_diagnostics ? SLADurationNs(SLATimestampNs(), allocation_start_ns) : 0;

	auto epoch_compute_ns = SLATimestampNs() - now_ns;
	vector<EpochTraceInput> trace_inputs;
	vector<idx_t> trace_input_indexes(captures.size(), DConstants::INVALID_INDEX);
	if (collect_diagnostics) {
		trace_inputs.reserve(captures.size());
		for (idx_t i = 0; i < captures.size(); i++) {
			if (!captures[i].debug_trace_enabled) {
				continue;
			}
			trace_input_indexes[i] = trace_inputs.size();
			EpochTraceInput input;
			input.capture_index = i;
			input.event_kind = models[i].event_kind;
			input.model_valid = models[i].valid;
			input.model_error = models[i].error;
			input.demand_cap = models[i].demand_cap;
			input.remaining_work_units = models[i].remaining_work_units;
			input.selected_throughput = models[i].throughput;
			input.throughput_is_live = models[i].throughput_is_live;
			auto planned_workers = mandatory[i] + optional[i] + residual[i] + liveness[i];
			if (models[i].valid && planned_workers < models[i].mandatory_cost.size()) {
				input.predicted_sla_cost = models[i].mandatory_cost[planned_workers];
			}
			if (energy_policy && i < energy_epoch_plan.queries.size()) {
				const auto &query_plan = energy_epoch_plan.queries[i];
				input.predicted_sla_cost = query_plan.predicted_sla_cost;
				input.predicted_service_rate = query_plan.aggregate_service_rate;
				input.predicted_active_power_w = query_plan.aggregate_active_power_w;
				input.predicted_epoch_energy_j = query_plan.predicted_epoch_energy_j;
				input.predicted_optional_risk = query_plan.predicted_optional_risk;
			}
			if (models[i].valid) {
				if (!models[i].mandatory_gain.empty()) {
					input.first_mandatory_gain = models[i].mandatory_gain[0];
				}
				if (!models[i].optional_gain.empty()) {
					input.first_optional_gain = models[i].optional_gain[0];
				}
				if (mandatory[i] > 0 && mandatory[i] <= models[i].mandatory_gain.size()) {
					input.last_mandatory_gain = models[i].mandatory_gain[mandatory[i] - 1];
				}
				if ((optional[i] > 0 || residual[i] > 0) && planned_workers > 0 &&
				    planned_workers <= models[i].optional_gain.size()) {
					input.last_optional_gain = models[i].optional_gain[planned_workers - 1];
				}
				if (planned_workers < models[i].mandatory_gain.size()) {
					input.next_mandatory_gain = models[i].mandatory_gain[planned_workers];
				}
				if (planned_workers < models[i].optional_gain.size()) {
					input.next_optional_gain = models[i].optional_gain[planned_workers];
				}
			}
			trace_inputs.push_back(std::move(input));
		}
	}
	std::shared_ptr<QueryHardwareTargetPlan> hardware_plan;
	if (energy_policy) {
		hardware_plan = std::make_shared<QueryHardwareTargetPlan>();
		hardware_plan->generation = published_generation;
		for (const auto &topology : energy_topology) {
			QueryHardwareWorkerTarget target;
			target.worker_id = topology.worker_id;
			hardware_plan->workers.push_back(target);
		}
	}
	const auto publish_wait_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	unique_lock<mutex> guard(state->lock);
	const auto publish_lock_acquired_ns = collect_diagnostics ? SLATimestampNs() : 0;
	const auto publish_lock_wait_ns = SLADurationNs(publish_lock_acquired_ns, publish_wait_start_ns);
	if (energy_policy) {
		for (auto &assignment : state->worker_assignments) {
			assignment = QuerySLASchedulerState::WorkerAssignment();
		}
		for (auto &entry : state->queries) {
			entry.second.assigned_workers = 0;
			entry.second.topology_plan_published = false;
		}
	}
	for (idx_t i = 0; i < captures.size(); i++) {
		auto entry = state->queries.find(captures[i].metadata.db_query_id);
		if (entry == state->queries.end() || entry->second.pipeline_generation != captures[i].generation) {
			continue;
		}
		entry->second.model = std::move(models[i]);
		entry->second.mandatory_workers = mandatory[i];
		entry->second.optional_workers = optional[i];
		entry->second.residual_workers = residual[i];
		entry->second.liveness_workers = liveness[i];
		entry->second.energy_workers = std::move(energy_workers[i]);
		entry->second.energy_limited_plan = energy_policy;
		entry->second.residual_dispatch_enabled =
		    !energy_policy && db.config.options.query_sla_residual_workers_enabled;
		entry->second.topology_plan_published = energy_policy;
		if (trace_input_indexes[i] != DConstants::INVALID_INDEX) {
			auto &trace_input = trace_inputs[trace_input_indexes[i]];
			trace_input.published = true;
			trace_input.assigned_workers = entry->second.assigned_workers;
		}
	}
	if (energy_policy) {
		for (const auto &placed : energy_epoch_plan.workers) {
			if (placed.query_index >= captures.size()) {
				continue;
			}
			auto query_entry = state->queries.find(captures[placed.query_index].metadata.db_query_id);
			if (query_entry == state->queries.end() ||
			    query_entry->second.pipeline_generation != captures[placed.query_index].generation) {
				continue;
			}
			if (placed.worker_id >= state->worker_assignments.size()) {
				state->worker_assignments.resize(placed.worker_id + 1);
			}
			auto &assignment = state->worker_assignments[placed.worker_id];
			assignment.db_query_id = query_entry->second.metadata.db_query_id;
			assignment.pipeline_generation = query_entry->second.pipeline_generation;
			assignment.normal_hardware = placed.normal_hardware;
			assignment.execution_hardware = placed.execution_hardware;
			assignment.plan_generation = published_generation;
			assignment.mandatory = placed.mandatory;
			assignment.optional = placed.optional;
			assignment.liveness = placed.liveness;
			assignment.exploration = placed.exploration;
			query_entry->second.assigned_workers++;
			for (auto &target : hardware_plan->workers) {
				if (target.worker_id != placed.worker_id) {
					continue;
				}
				target.hardware = assignment.execution_hardware.IsValid()
				                      ? assignment.execution_hardware
				                      : state->hardware_manager->ReferenceConfiguration();
				target.assigned = true;
				break;
			}
		}
		if (!db.config.options.query_sla_energy_exploration_enabled) {
			state->epochs_without_optional_only_socket = 0;
		} else if (energy_epoch_plan.diagnostics.optional_only_socket_available) {
			state->epochs_without_optional_only_socket = 0;
		} else if (!energy_epoch_plan.workers.empty()) {
			auto missed_epochs = state->epochs_without_optional_only_socket + 1;
			state->epochs_without_optional_only_socket = missed_epochs >= 4 ? 0 : missed_epochs;
		}
		state->exploration_core_probe_epochs += energy_epoch_plan.diagnostics.core_probes;
		state->exploration_forced_uncore_probe_epochs += energy_epoch_plan.diagnostics.forced_uncore_probes;
		state->exploration_optional_uncore_probe_epochs +=
		    energy_epoch_plan.diagnostics.uncore_probes - energy_epoch_plan.diagnostics.forced_uncore_probes;
		for (idx_t i = 0; i < captures.size(); i++) {
			if (trace_input_indexes[i] == DConstants::INVALID_INDEX) {
				continue;
			}
			auto query_entry = state->queries.find(captures[i].metadata.db_query_id);
			if (query_entry != state->queries.end() &&
			    query_entry->second.pipeline_generation == captures[i].generation) {
				trace_inputs[trace_input_indexes[i]].assigned_workers = query_entry->second.assigned_workers;
			}
		}
	}
	ResetDispatchEntries(*state);
	QuerySLAExplorationDiagnosticsSnapshot exploration_diagnostics;
	exploration_diagnostics.core_probe_epochs = state->exploration_core_probe_epochs;
	exploration_diagnostics.optional_uncore_probe_epochs = state->exploration_optional_uncore_probe_epochs;
	exploration_diagnostics.forced_uncore_probe_epochs = state->exploration_forced_uncore_probe_epochs;
	exploration_diagnostics.epochs_without_optional_only_socket = state->epochs_without_optional_only_socket;
	state->epoch_generation++;
	const auto publish_complete_ns = collect_diagnostics ? SLATimestampNs() : 0;
	const auto thread_cpu_complete_ns = collect_diagnostics ? SLAThreadCpuNs() : 0;
	const auto publish_lock_hold_ns =
	    collect_diagnostics ? SLADurationNs(publish_complete_ns, publish_lock_acquired_ns) : 0;
	guard.unlock();
	const auto hardware_apply_start_ns = collect_diagnostics && hardware_plan ? SLATimestampNs() : 0;
	if (hardware_plan) {
		state->hardware_manager->PublishTargets(std::move(hardware_plan));
	}
	const auto hardware_apply_ns = hardware_apply_start_ns > 0
	                                   ? SLADurationNs(SLATimestampNs(), hardware_apply_start_ns)
	                                   : 0;

	if (!collect_diagnostics || trace_inputs.empty()) {
		return;
	}
	const auto epoch_wall_ns = SLADurationNs(publish_complete_ns, now_ns);
	const auto epoch_thread_cpu_ns = SLADurationNs(thread_cpu_complete_ns, thread_cpu_start_ns);
	const auto trace_build_start_ns = SLATimestampNs();
	vector<QuerySLASchedulerEpochSnapshot> trace_rows;
	vector<QuerySLAEnergyWorkerEpochSnapshot> energy_worker_trace_rows;
	trace_rows.reserve(trace_inputs.size());
	if (energy_policy) {
		std::unordered_map<idx_t, QueryHardwareWorkerTopology> topology_by_worker;
		for (const auto &topology : energy_topology) {
			topology_by_worker[topology.worker_id] = topology;
		}
		energy_worker_trace_rows.reserve(energy_epoch_plan.workers.size());
		for (const auto &worker : energy_epoch_plan.workers) {
			if (worker.query_index >= captures.size() || !captures[worker.query_index].debug_trace_enabled) {
				continue;
			}
			auto topology = topology_by_worker.find(worker.worker_id);
			if (topology == topology_by_worker.end()) {
				continue;
			}
			QuerySLAEnergyWorkerEpochSnapshot row;
			row.epoch_generation = published_generation;
			row.epoch_timestamp_ns = now_ns;
			row.db_query_id = captures[worker.query_index].metadata.db_query_id;
			row.request_id = captures[worker.query_index].metadata.request_id;
			row.template_id = captures[worker.query_index].metadata.template_id;
			row.scale_factor = captures[worker.query_index].metadata.scale_factor;
			row.pipeline_id = work[worker.query_index].pipeline_id;
			row.pipeline_signature_hash = work[worker.query_index].profile_identity.pipeline_signature_hash;
			row.pipeline_generation = captures[worker.query_index].generation;
			row.worker_id = worker.worker_id;
			row.logical_cpu = topology->second.logical_cpu;
			row.socket_id = topology->second.socket_id;
			row.physical_core_id = topology->second.physical_core_id;
			row.mandatory = worker.mandatory;
			row.optional = worker.optional;
			row.liveness = worker.liveness;
			row.exploration = worker.exploration;
			row.normal_hardware = worker.normal_hardware;
			row.execution_hardware = worker.execution_hardware;
			row.applied_hardware = state->hardware_manager->GetAppliedConfigurationForCPU(row.logical_cpu);
			energy_worker_trace_rows.push_back(std::move(row));
		}
	}
	for (const auto &input : trace_inputs) {
		if (!input.published) {
			continue;
		}
		auto i = input.capture_index;
		const auto &suffix = resolved_suffixes[i];
		QuerySLASchedulerEpochSnapshot trace;
		trace.scheduler_policy = energy_policy ? "sla_energy" : "sla";
		trace.epoch_generation = published_generation;
		trace.epoch_timestamp_ns = now_ns;
		trace.epoch_compute_ns = epoch_compute_ns;
		trace.epoch_wall_ns = epoch_wall_ns;
		trace.epoch_thread_cpu_ns = epoch_thread_cpu_ns;
		trace.capture_lock_wait_ns = capture_lock_wait_ns;
		trace.capture_lock_hold_ns = capture_lock_hold_ns;
		trace.work_snapshot_ns = work_snapshot_ns;
		trace.suffix_prepare_ns = suffix_prepare_ns;
		trace.model_build_ns = model_build_ns;
		trace.allocation_ns = allocation_ns;
		trace.publish_lock_wait_ns = publish_lock_wait_ns;
		trace.publish_lock_hold_ns = publish_lock_hold_ns;
		trace.hardware_apply_ns = hardware_apply_ns;
		trace.db_query_id = captures[i].metadata.db_query_id;
		trace.request_id = captures[i].metadata.request_id;
		trace.template_id = captures[i].metadata.template_id;
		trace.scale_factor = captures[i].metadata.scale_factor;
		trace.deadline_ns = captures[i].metadata.deadline_ns;
		trace.pipeline_id = work[i].pipeline_id;
		trace.pipeline_signature_hash = work[i].profile_identity.pipeline_signature_hash;
		trace.pipeline_generation = captures[i].generation;
		trace.event_kind = SLAEventKindName(input.event_kind);
		trace.model_valid = input.model_valid;
		trace.model_error = input.model_error;
		trace.demand_cap = input.demand_cap;
		trace.remaining_work_units = input.remaining_work_units;
		trace.selected_throughput = input.selected_throughput;
		trace.throughput_is_live = input.throughput_is_live;
		trace.historical_throughput_level =
		    PipelineThroughputEstimateLevelToString(work[i].historical_throughput_estimate.level);
		trace.historical_throughput_sample_count = work[i].historical_throughput_estimate.sample_count;
		trace.continuation_level = ContinuationEstimateLevelToString(work[i].continuation_estimate.level);
		trace.continuation_kind = ContinuationEstimateKindToString(work[i].continuation_estimate.kind);
		trace.continuation_sample_count = work[i].continuation_estimate.sample_count;
		trace.continuation_p90 = work[i].continuation_estimate.p90;
		trace.suffix_primary_level = SLASuffixLevelName(suffix.primary_level);
		trace.suffix_exact_sample_count = suffix.exact_sample_count;
		trace.suffix_scale_sample_count = suffix.scale_sample_count;
		trace.suffix_global_sample_count = suffix.global_sample_count;
		trace.suffix_exact_weight = suffix.exact_weight;
		trace.suffix_scale_weight = suffix.scale_weight;
		trace.suffix_global_weight = suffix.global_weight;
		trace.suffix_bucket_count = suffix.bucket_count;
		trace.suffix_mean_ns = QuerySLASuffixMeanNs(suffix);
		trace.suffix_p90_ns = QuerySLASuffixQuantileNs(suffix, 0.90);
		trace.mandatory_workers = mandatory[i];
		trace.optional_workers = optional[i];
		trace.residual_workers = residual[i];
		trace.liveness_workers = liveness[i];
		trace.planned_workers = mandatory[i] + optional[i] + residual[i] + liveness[i];
		trace.assigned_workers = input.assigned_workers;
		if (energy_policy) {
			const auto &diagnostics = energy_epoch_plan.diagnostics;
			trace.energy_mandatory_workers = diagnostics.mandatory_workers;
			trace.energy_o1_workers = diagnostics.o1_workers;
			trace.energy_o2_workers = diagnostics.o2_workers;
			trace.energy_o3_workers = diagnostics.o3_workers;
			trace.energy_liveness_workers = diagnostics.liveness_workers;
			trace.energy_parked_workers = diagnostics.parked_workers;
			trace.energy_core_probes = diagnostics.core_probes;
			trace.energy_uncore_probes = diagnostics.uncore_probes;
			trace.energy_forced_uncore_probes = diagnostics.forced_uncore_probes;
			trace.energy_optional_only_socket_available = diagnostics.optional_only_socket_available;
			trace.energy_epochs_without_optional_only_socket =
			    exploration_diagnostics.epochs_without_optional_only_socket;
			trace.energy_profile_fallback_count = diagnostics.profile_fallback_count;
			trace.energy_immature_pair_rejections = diagnostics.immature_pair_rejections;
			trace.energy_domain_pair_promotions = diagnostics.domain_pair_promotions;
			trace.energy_mandatory_footprint_j = diagnostics.mandatory_footprint_energy_j;
			trace.energy_optional_incremental_j = diagnostics.optional_incremental_energy_j;
			trace.predicted_service_rate = input.predicted_service_rate;
			trace.predicted_active_power_w = input.predicted_active_power_w;
			trace.predicted_epoch_energy_j = input.predicted_epoch_energy_j;
			trace.predicted_optional_risk = input.predicted_optional_risk;
		}
		trace.first_mandatory_gain = input.first_mandatory_gain;
		trace.first_optional_gain = input.first_optional_gain;
		trace.last_mandatory_gain = input.last_mandatory_gain;
		trace.last_optional_gain = input.last_optional_gain;
		trace.next_mandatory_gain = input.next_mandatory_gain;
		trace.next_optional_gain = input.next_optional_gain;
		if (input.model_valid && trace.planned_workers <= input.demand_cap) {
			auto service_rate = energy_policy ? input.predicted_service_rate
			                                  : static_cast<double>(trace.planned_workers) * input.selected_throughput;
			trace.predicted_pipeline_finish_ns = QuerySLACurrentPipelineFinishNs(
			    work[i], input.event_kind, service_rate, trace.planned_workers > 0, now_ns,
			    EpochMs() * 1000000ULL);
			if (trace.predicted_pipeline_finish_ns > 0) {
				trace.predicted_query_finish_mean_ns = trace.predicted_pipeline_finish_ns + trace.suffix_mean_ns;
				trace.predicted_query_finish_p90_ns = trace.predicted_pipeline_finish_ns + trace.suffix_p90_ns;
			}
			trace.predicted_sla_cost = input.predicted_sla_cost;
		}
		trace_rows.push_back(std::move(trace));
	}
	const auto trace_build_ns = SLADurationNs(SLATimestampNs(), trace_build_start_ns);
	for (auto &trace : trace_rows) {
		trace.trace_build_ns = trace_build_ns;
	}
	if (trace_rows.empty()) {
		return;
	}

	const auto trace_lock_wait_start_ns = SLATimestampNs();
	unique_lock<mutex> trace_guard(state->epoch_trace_lock);
	const auto trace_lock_acquired_ns = SLATimestampNs();
	const auto trace_lock_wait_ns = SLADurationNs(trace_lock_acquired_ns, trace_lock_wait_start_ns);
	for (auto &trace : trace_rows) {
		trace.trace_lock_wait_ns = trace_lock_wait_ns;
		if (state->epoch_trace.size() >= QuerySLASchedulerState::MAX_EPOCH_TRACE_ROWS) {
			state->epoch_trace.pop_front();
			state->epoch_trace_dropped_count++;
		}
		state->epoch_trace.push_back(std::move(trace));
	}
	for (auto &trace : energy_worker_trace_rows) {
		if (state->energy_worker_epoch_trace.size() >= QuerySLASchedulerState::MAX_ENERGY_WORKER_EPOCH_TRACE_ROWS) {
			state->energy_worker_epoch_trace.pop_front();
			state->energy_worker_epoch_trace_dropped_count++;
		}
		state->energy_worker_epoch_trace.push_back(std::move(trace));
	}
	const auto trace_lock_hold_ns = SLADurationNs(SLATimestampNs(), trace_lock_acquired_ns);
	for (auto trace = state->epoch_trace.rbegin(); trace != state->epoch_trace.rend(); trace++) {
		if (trace->epoch_generation != published_generation) {
			break;
		}
		trace->trace_lock_hold_ns = trace_lock_hold_ns;
	}
}

QuerySLADequeueResult QuerySLAScheduler::TryDequeueTask(idx_t worker_id, shared_ptr<Task> &task,
	                                                   QuerySLAWorkerSelection *selection) {
	if (selection) {
		*selection = QuerySLAWorkerSelection();
	}
	if (!Enabled()) {
		return QuerySLADequeueResult::NOT_ACTIVE;
	}
	MaybeRunEpoch();
	lock_guard<mutex> guard(state->lock);
	if (state->queries.empty()) {
		return QuerySLADequeueResult::NOT_ACTIVE;
	}
	if (worker_id >= state->worker_assignments.size()) {
		state->worker_assignments.resize(worker_id + 1);
	}
	auto &assignment = state->worker_assignments[worker_id];
	if (assignment.db_query_id != 0) {
		auto entry = state->queries.find(assignment.db_query_id);
		if (entry != state->queries.end() && entry->second.pipeline_generation == assignment.pipeline_generation &&
		    entry->second.event && entry->second.model.valid) {
			auto planned = PlannedWorkers(entry->second);
			if (entry->second.assigned_workers <= planned && entry->second.producer &&
			    db.GetScheduler().GetTaskFromProducer(*entry->second.producer, task)) {
				SetWorkerSelection(assignment, selection);
				return QuerySLADequeueResult::TASK_FOUND;
			}
		}
		if (entry != state->queries.end() && entry->second.pipeline_generation == assignment.pipeline_generation &&
		    entry->second.assigned_workers > 0) {
			entry->second.assigned_workers--;
			RefreshDispatchEntries(*state, entry->second);
		}
		assignment = QuerySLASchedulerState::WorkerAssignment();
	}
	auto assign_worker = [&](QuerySLASchedulerState::QueryState &query, bool dispatched_mandatory) {
		auto worker_index = query.assigned_workers > 0 ? query.assigned_workers - 1 : 0;
		auto selected = EnergyAssignmentAt(query, worker_index);
		if (query.energy_limited_plan && !selected.hardware.IsValid()) {
			selected.hardware = state->hardware_manager->ReferenceConfiguration();
			selected.mandatory = dispatched_mandatory;
			selected.liveness = !dispatched_mandatory;
		}
		assignment.db_query_id = query.metadata.db_query_id;
		assignment.pipeline_generation = query.pipeline_generation;
		assignment.normal_hardware = selected.hardware;
		assignment.execution_hardware = selected.hardware;
		assignment.plan_generation = state->epoch_generation.load();
		assignment.mandatory = selected.mandatory;
		assignment.optional = selected.optional;
		assignment.liveness = selected.liveness;
		assignment.exploration = selected.exploration;
		SetWorkerSelection(assignment, selection);
	};

	auto try_dispatch = [&](std::priority_queue<QuerySLADispatchEntry> &heap, bool primary, bool mandatory) {
		vector<QuerySLADispatchEntry> unavailable;
		while (!heap.empty()) {
			auto candidate = heap.top();
			heap.pop();
			auto entry = state->queries.find(candidate.db_query_id);
			if (entry == state->queries.end()) {
				continue;
			}
			auto &query = entry->second;
			auto planned = MinValue<idx_t>(PlannedWorkers(query), query.model.demand_cap);
			if (candidate.version != query.dispatch_version || !query.event || !query.model.valid ||
			    candidate.workers != (primary ? query.assigned_workers : planned)) {
				continue;
			}
			if (primary) {
				if (query.assigned_workers >= planned || mandatory != (query.assigned_workers < query.mandatory_workers)) {
					continue;
				}
			} else {
				if (query.assigned_workers < planned || planned >= query.model.demand_cap) {
					continue;
				}
				auto &gains = mandatory ? query.model.mandatory_gain : query.model.optional_gain;
				if (planned >= gains.size() || gains[planned] <= 0) {
					continue;
				}
			}
			if (!query.producer || !db.GetScheduler().GetTaskFromProducer(*query.producer, task)) {
				unavailable.push_back(candidate);
				continue;
			}
			if (!primary) {
				if (mandatory) {
					query.mandatory_workers++;
				} else {
					query.optional_workers++;
				}
			}
			query.assigned_workers++;
			assign_worker(query, mandatory);
			RefreshDispatchEntries(*state, query);
			for (auto &skipped : unavailable) {
				heap.push(std::move(skipped));
			}
			return true;
		}
		for (auto &skipped : unavailable) {
			heap.push(std::move(skipped));
		}
		return false;
	};
	auto try_residual_dispatch = [&]() {
		vector<QuerySLAResidualDispatchEntry> unavailable;
		while (!state->dynamic_residual.empty()) {
			auto candidate = state->dynamic_residual.top();
			state->dynamic_residual.pop();
			auto entry = state->queries.find(candidate.db_query_id);
			if (entry == state->queries.end()) {
				continue;
			}
			auto &query = entry->second;
			auto planned = MinValue<idx_t>(PlannedWorkers(query), query.model.demand_cap);
			if (candidate.version != query.dispatch_version || !query.event || !query.model.valid ||
			    query.assigned_workers < planned || planned >= query.model.demand_cap || candidate.workers != planned ||
			    planned >= query.model.mandatory_gain.size() || planned >= query.model.optional_gain.size()) {
				continue;
			}
			if (!query.producer || !db.GetScheduler().GetTaskFromProducer(*query.producer, task)) {
				unavailable.push_back(candidate);
				continue;
			}
			query.residual_workers++;
			query.assigned_workers++;
			assign_worker(query, false);
			RefreshDispatchEntries(*state, query);
			for (auto &skipped : unavailable) {
				state->dynamic_residual.push(std::move(skipped));
			}
			return true;
		}
		for (auto &skipped : unavailable) {
			state->dynamic_residual.push(std::move(skipped));
		}
		return false;
	};
	if (try_dispatch(state->primary_mandatory, true, true) ||
	    try_dispatch(state->primary_optional, true, false) ||
	    try_dispatch(state->dynamic_mandatory, false, true) ||
	    try_dispatch(state->dynamic_optional, false, false) || try_residual_dispatch()) {
		return QuerySLADequeueResult::TASK_FOUND;
	}
	return QuerySLADequeueResult::NO_TASK;
}

vector<QuerySLASchedulerSnapshot> QuerySLAScheduler::GetSnapshot() const {
	lock_guard<mutex> guard(state->lock);
	vector<QuerySLASchedulerSnapshot> result;
	result.reserve(state->queries.size());
	for (const auto &entry : state->queries) {
		const auto &query = entry.second;
		QuerySLASchedulerSnapshot snapshot;
		snapshot.epoch_generation = state->epoch_generation.load();
		snapshot.db_query_id = query.metadata.db_query_id;
		snapshot.request_id = query.metadata.request_id;
		snapshot.pipeline_id = query.model.pipeline_id;
		snapshot.pipeline_generation = query.pipeline_generation;
		snapshot.event_kind = SLAEventKindName(query.model.event_kind);
		snapshot.model_valid = query.model.valid;
		snapshot.model_error = query.model.error;
		snapshot.demand_cap = query.model.demand_cap;
		snapshot.remaining_work_units = query.model.remaining_work_units;
		snapshot.selected_throughput = query.model.throughput;
		snapshot.throughput_is_live = query.model.throughput_is_live;
		snapshot.mandatory_workers = query.mandatory_workers;
		snapshot.optional_workers = query.optional_workers;
		snapshot.residual_workers = query.residual_workers;
		snapshot.assigned_workers = query.assigned_workers;
		if (query.assigned_workers < query.model.mandatory_gain.size()) {
			snapshot.next_mandatory_gain = query.model.mandatory_gain[query.assigned_workers];
			snapshot.next_optional_gain = query.model.optional_gain[query.assigned_workers];
		}
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QuerySLASchedulerSnapshot &left,
	                                           const QuerySLASchedulerSnapshot &right) {
		return left.db_query_id < right.db_query_id;
	});
	return result;
}

vector<QuerySLASchedulerEpochSnapshot> QuerySLAScheduler::GetEpochTrace() const {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	return vector<QuerySLASchedulerEpochSnapshot>(state->epoch_trace.begin(), state->epoch_trace.end());
}

vector<QuerySLAEnergyWorkerEpochSnapshot> QuerySLAScheduler::GetEnergyWorkerEpochTrace() const {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	return vector<QuerySLAEnergyWorkerEpochSnapshot>(state->energy_worker_epoch_trace.begin(),
	                                                 state->energy_worker_epoch_trace.end());
}

QuerySLAExplorationDiagnosticsSnapshot QuerySLAScheduler::GetExplorationDiagnostics() const {
	lock_guard<mutex> guard(state->lock);
	QuerySLAExplorationDiagnosticsSnapshot result;
	result.core_probe_epochs = state->exploration_core_probe_epochs;
	result.optional_uncore_probe_epochs = state->exploration_optional_uncore_probe_epochs;
	result.forced_uncore_probe_epochs = state->exploration_forced_uncore_probe_epochs;
	result.epochs_without_optional_only_socket = state->epochs_without_optional_only_socket;
	return result;
}

uint64_t QuerySLAScheduler::EpochTraceDroppedCount() const {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	return state->epoch_trace_dropped_count;
}

uint64_t QuerySLAScheduler::EnergyWorkerEpochTraceDroppedCount() const {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	return state->energy_worker_epoch_trace_dropped_count;
}

void QuerySLAScheduler::ClearEpochTrace() {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	state->epoch_trace.clear();
	state->epoch_trace_dropped_count = 0;
	state->energy_worker_epoch_trace.clear();
	state->energy_worker_epoch_trace_dropped_count = 0;
}

uint64_t QuerySLAScheduler::EpochRunCount() const {
	return state->epoch_run_count.load();
}

} // namespace duckdb
