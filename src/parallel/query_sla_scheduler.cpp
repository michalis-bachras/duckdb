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
#include <condition_variable>
#include <deque>
#include <memory>
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

static bool UsesPipelineSuffix(QueryActivationEventKind kind) {
	return kind == QueryActivationEventKind::PIPELINE || kind == QueryActivationEventKind::INTERNAL ||
	       kind == QueryActivationEventKind::PREPARE_FINISH || kind == QueryActivationEventKind::FINISH;
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
	DownstreamSuffixEstimate activation_suffix;
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
	double mandatory_target_sla_cost = 0;
	double mandatory_target_optional_risk = 0;
	double mandatory_reference_safe_throughput = 0;
	double mandatory_preferred_normalized_edp = 0;
	uint64_t mandatory_profile_version = 0;
	double first_mandatory_gain = 0;
	double first_optional_gain = 0;
	double last_mandatory_gain = 0;
	double last_optional_gain = 0;
	double next_mandatory_gain = 0;
	double next_optional_gain = 0;
};

struct ActivatedPipelinePlan {
	DownstreamSuffixEstimate suffix;
	QuerySLAModel model;
	vector<QuerySLAEnergyWorkerAssignment> dynamic_mandatory_workers;
	vector<QuerySLAEnergyWorkerAssignment> dynamic_optional_workers;
	bool energy_policy = false;
	bool claimable = false;
};

static ActivatedPipelinePlan BuildActivatedPipelinePlan(DatabaseInstance &db, QueryHardwareManager &hardware_manager,
	                                                     const QueryCapture &capture, uint64_t now_ns,
	                                                     uint64_t epoch_ns) {
	ActivatedPipelinePlan result;
	PipelineWorkSnapshot work;
	if (!capture.event->GetPipelineWorkSnapshot(work)) {
		throw InternalException("SLA scheduler could not read active pipeline work state");
	}
	auto event_kind = capture.event->GetQueryActivationKind();
	if (event_kind == QueryActivationEventKind::PIPELINE && !work.profile_identity.valid) {
		throw InvalidInputException("SLA scheduler requires a stable pipeline profile identity");
	}

	DownstreamSuffixEstimate suffix;
	if (UsesPipelineSuffix(event_kind)) {
		if (!work.profile_identity.valid) {
			throw InvalidInputException("SLA scheduler requires a stable profile identity for modeled work");
		}
  DownstreamSuffixEpochRequest request(capture.metadata.template_id, capture.metadata.scale_factor,
      work.profile_identity.pipeline_id, work.profile_identity.pipeline_signature_hash,
      capture.remaining_suffix_stages, work.profile_identity.pipeline_structure_hash);
  auto model = ResolveQuerySLASuffixModel(db.config.options.query_sla_suffix_model, db.GetQuerySchedulerPolicy());
  auto &store = db.GetQueryRequestProfileStore();
  if (event_kind == QueryActivationEventKind::PREPARE_FINISH || event_kind == QueryActivationEventKind::FINISH) {
   auto issued = store.ReadIssuedDownstreamSuffixForecast(capture.metadata.db_query_id, request);
   if (!issued.valid || issued.model != model) {
    throw InvalidInputException("Finishing event has no matching owning-pipeline suffix forecast");
   }
   suffix = model == QuerySLASuffixModel::POINT ? issued : store.PrepareDownstreamSuffixEpoch({request})[0];
  } else {
   suffix = store.PrepareDownstreamSuffixActivation(capture.metadata.db_query_id, request, model);
  }
	} else {
		suffix = QuerySLALifecycleSuffixPointMass();
	}

	QuerySLAModelInput model_input;
	model_input.metadata = capture.metadata;
	model_input.work = work;
	model_input.suffix = suffix;
	model_input.event_kind = event_kind;
	model_input.generation = capture.generation;
	model_input.now_ns = now_ns;
	model_input.epoch_ns = epoch_ns;
	result.suffix = suffix;
	result.model = BuildQuerySLAModel(model_input);
	if (!result.model.valid) {
		throw InvalidInputException("SLA scheduler cannot model query %llu pipeline %llu: %s",
		                            static_cast<unsigned long long>(capture.metadata.db_query_id),
		                            static_cast<unsigned long long>(work.pipeline_id), result.model.error);
	}

	result.energy_policy = db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY;
	result.claimable =
	    (!result.model.mandatory_gain.empty() && result.model.mandatory_gain[0] > 0) ||
	    (!result.model.optional_gain.empty() && result.model.optional_gain[0] > 0);
	if (!result.energy_policy) {
		return result;
	}

	QuerySLAEnergyModelInput energy_input;
	energy_input.sla_input = model_input;
	energy_input.reference_model = result.model;
	energy_input.reference_hardware = hardware_manager.ReferenceConfiguration();
	if (work.profile_identity.valid) {
		auto &profile_store = db.GetQueryRequestProfileStore();
		bool stable_snapshot = false;
		for (idx_t attempt = 0; attempt < 3; attempt++) {
			auto generation = profile_store.GetPipelineHardwareProfileGeneration();
			auto profiles = profile_store.GetPipelineHardwareProfiles(
			    capture.metadata.template_id, capture.metadata.scale_factor, work.profile_identity);
			if (profile_store.GetPipelineHardwareProfileGeneration() != generation) {
				continue;
			}
			energy_input.hardware_profiles = std::move(profiles);
			stable_snapshot = true;
			break;
		}
		if (!stable_snapshot) {
			// Reference-only modeling is safe until the next regular epoch observes a stable profile snapshot.
			energy_input.hardware_profiles.clear();
		}
	}
	auto reference_model = result.model;
	if (!reference_model.valid) {
		throw InvalidInputException("SLA-energy scheduler cannot build a successor reference model for query %llu",
		                            static_cast<unsigned long long>(capture.metadata.db_query_id));
	}
	energy_input.reference_model = reference_model;
	double service_rate = 0;
	idx_t worker_count = 0;
	auto current = QuerySLAEvaluateServiceRate(energy_input.sla_input, service_rate, worker_count);
	double optional_risk = current.valid ? current.optional_risk : 0;
	while (worker_count < reference_model.demand_cap) {
		auto assignment =
		    SelectQuerySLAMandatoryAddition(energy_input, reference_model, service_rate, worker_count);
		if (!assignment.valid || assignment.mandatory_gain <= 0) {
			break;
		}
		result.dynamic_mandatory_workers.push_back(assignment);
		service_rate += assignment.safe_throughput;
		worker_count++;
		current = QuerySLAEvaluateServiceRate(energy_input.sla_input, service_rate, worker_count);
		if (current.valid) {
			optional_risk = current.optional_risk;
		}
	}
	vector<QueryRequestHardwareConfiguration> hardware_pairs;
	for (auto uncore_khz : hardware_manager.UncoreFrequencyLevelsKHz()) {
		for (auto core_khz : hardware_manager.CoreFrequencyLevelsKHz()) {
			QueryRequestHardwareConfiguration hardware;
			hardware.core_frequency_khz = core_khz;
			hardware.uncore_frequency_khz = uncore_khz;
			hardware_pairs.push_back(hardware);
		}
	}
	while (worker_count < reference_model.demand_cap) {
		auto assignment = SelectQuerySLAOptionalAddition(
		    energy_input, service_rate, optional_risk, worker_count, energy_input.reference_hardware,
		    hardware_pairs, QuerySLAEnergyOptionalStage::DYNAMIC);
		if (!assignment.valid || assignment.optional_gain <= 0) {
			break;
		}
		result.dynamic_optional_workers.push_back(assignment);
		service_rate += assignment.safe_throughput;
		worker_count++;
		optional_risk = assignment.optional_fragility_after;
	}
	return result;
}

} // namespace

struct QuerySLASchedulerState {
	static constexpr idx_t MAX_EPOCH_TRACE_ROWS = 250000;
	static constexpr idx_t MAX_ENERGY_WORKER_EPOCH_TRACE_ROWS = 500000;

	struct WorkerAssignment {
		uint64_t db_query_id = 0;
		uint64_t request_id = 0;
		uint64_t pipeline_generation = 0;
		idx_t pipeline_id = 0;
		QueryRequestHardwareConfiguration normal_hardware;
		QueryRequestHardwareConfiguration execution_hardware;
		uint64_t plan_generation = 0;
		uint64_t lease_generation = 0;
		idx_t worker_ordinal = DConstants::INVALID_INDEX;
		idx_t class_ordinal = DConstants::INVALID_INDEX;
		bool mandatory = false;
		bool optional = false;
		bool exploration = false;
		bool debug_trace_enabled = false;
		bool hardware_transition_hold = false;
	};

	struct QueryState {
		DownstreamSuffixEstimate activation_suffix;
		QueryRequestMetadata metadata;
		ProducerToken *producer = nullptr;
		shared_ptr<Event> event;
		uint64_t pipeline_generation = 0;
		idx_t remaining_suffix_stages = 1;
		QuerySLAModel model;
		idx_t mandatory_workers = 0;
		idx_t optional_workers = 0;
		idx_t residual_workers = 0;
		idx_t assigned_workers = 0;
		idx_t assigned_mandatory_workers = 0;
		idx_t assigned_optional_workers = 0;
		idx_t assigned_residual_workers = 0;
		vector<QuerySLAEnergyWorkerAssignment> energy_workers;
		vector<QuerySLAEnergyWorkerAssignment> dynamic_mandatory_workers;
		vector<QuerySLAEnergyWorkerAssignment> dynamic_optional_workers;
		bool energy_limited_plan = false;
		bool residual_dispatch_enabled = true;
		uint64_t dispatch_version = 0;
		bool debug_trace_enabled = false;
	};

	mutable mutex lock;
	std::condition_variable hardware_plan_cv;
	bool hardware_plan_publication_in_progress = false;
	std::unordered_map<uint64_t, QueryState> queries;
	//! Active task leases. Entries exist only while the corresponding worker owns a task.
	vector<WorkerAssignment> worker_assignments;
	//! Topology-aware SLA-energy preferences. Hints never reserve a worker or count as an active task lease.
	vector<WorkerAssignment> worker_hints;
	//! Hardware reservations survive tasks and successor pipelines until scheduling or final query teardown replaces them.
	vector<WorkerAssignment> worker_hardware_reservations;
	uint64_t next_lease_generation = 0;
	std::priority_queue<QuerySLADispatchEntry> primary_mandatory;
	std::priority_queue<QuerySLADispatchEntry> primary_optional;
	std::priority_queue<QuerySLADispatchEntry> dynamic_mandatory;
	std::priority_queue<QuerySLADispatchEntry> dynamic_optional;
	std::priority_queue<QuerySLAResidualDispatchEntry> dynamic_residual;
	atomic<uint64_t> next_epoch_ns {0};
	atomic<bool> epoch_leader {false};
	atomic<bool> epoch_sentinel {false};
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
	return query.mandatory_workers + query.optional_workers + query.residual_workers;
}

static void ReleaseAssignedWorker(QuerySLASchedulerState::QueryState &query,
	                              const QuerySLASchedulerState::WorkerAssignment &assignment) {
	if (query.assigned_workers > 0) {
		query.assigned_workers--;
	}
	if (assignment.mandatory) {
		if (query.assigned_mandatory_workers > 0) {
			query.assigned_mandatory_workers--;
		}
	} else if (assignment.optional) {
		if (query.assigned_optional_workers > 0) {
			query.assigned_optional_workers--;
		}
	} else if (query.assigned_residual_workers > 0) {
		query.assigned_residual_workers--;
	}
}

static idx_t NextClassOrdinal(const QuerySLASchedulerState &state,
	                          const QuerySLASchedulerState::QueryState &query, bool mandatory, bool optional) {
	idx_t class_ordinal = 0;
	while (true) {
		bool occupied = false;
		for (const auto &assignment : state.worker_assignments) {
			if (assignment.db_query_id != query.metadata.db_query_id ||
			    assignment.pipeline_generation != query.pipeline_generation ||
			    assignment.mandatory != mandatory || assignment.optional != optional ||
			    assignment.class_ordinal != class_ordinal) {
				continue;
			}
			occupied = true;
			break;
		}
		if (!occupied) {
			return class_ordinal;
		}
		class_ordinal++;
	}
}

static bool AssignmentClassIsPlanned(const QuerySLASchedulerState::QueryState &query,
	                                 const QuerySLASchedulerState::WorkerAssignment &assignment) {
	if (assignment.mandatory) {
		return assignment.class_ordinal < query.mandatory_workers;
	}
	if (assignment.optional) {
		return assignment.class_ordinal < query.optional_workers;
	}
	return assignment.class_ordinal < query.residual_workers;
}

static bool SameAssignmentClass(const QuerySLASchedulerState::WorkerAssignment &left,
	                            const QuerySLASchedulerState::WorkerAssignment &right) {
	return !left.hardware_transition_hold && !right.hardware_transition_hold && left.db_query_id != 0 &&
	       left.db_query_id == right.db_query_id &&
	       left.pipeline_generation == right.pipeline_generation && left.class_ordinal == right.class_ordinal &&
	       left.mandatory == right.mandatory && left.optional == right.optional;
}

static void HoldHardwareReservationsForQuery(vector<QuerySLASchedulerState::WorkerAssignment> &records,
	                                         uint64_t db_query_id) {
	for (auto &record : records) {
		if (record.db_query_id == db_query_id) {
			record.hardware_transition_hold = true;
		}
	}
}

static void InstallHardwareReservation(QuerySLASchedulerState &state, idx_t worker_id,
	                                   const QuerySLASchedulerState::WorkerAssignment &assignment) {
	if (worker_id >= state.worker_hardware_reservations.size()) {
		state.worker_hardware_reservations.resize(worker_id + 1);
	}
	for (idx_t other_worker = 0; other_worker < state.worker_hardware_reservations.size(); other_worker++) {
		if (other_worker != worker_id &&
		    SameAssignmentClass(state.worker_hardware_reservations[other_worker], assignment)) {
			state.worker_hardware_reservations[other_worker] =
			    QuerySLASchedulerState::WorkerAssignment();
		}
	}
	state.worker_hardware_reservations[worker_id] = assignment;
}

static std::shared_ptr<QueryHardwareTargetPlan>
BuildHardwareTargetPlan(const QuerySLASchedulerState &state, QueryHardwareManager &hardware_manager,
	                    uint64_t generation) {
	auto result = std::make_shared<QueryHardwareTargetPlan>();
	result->generation = generation;
	result->workers.reserve(state.worker_hardware_reservations.size());
	for (idx_t worker_id = 0; worker_id < state.worker_hardware_reservations.size(); worker_id++) {
		const auto &reservation = state.worker_hardware_reservations[worker_id];
		QueryHardwareWorkerTarget target;
		target.worker_id = worker_id;
		target.assigned = reservation.db_query_id != 0;
		if (target.assigned) {
			target.hardware = reservation.execution_hardware.IsValid() ? reservation.execution_hardware
			                                                        : reservation.normal_hardware;
			if (!target.hardware.IsValid()) {
				target.hardware = hardware_manager.ReferenceConfiguration();
			}
		}
		result->workers.push_back(std::move(target));
	}
	return result;
}

static QuerySLAEnergyWorkerAssignment EnergyAssignmentAt(const QuerySLASchedulerState::QueryState &query,
	                                                      idx_t worker_index, bool dispatched_mandatory) {
	if (worker_index < query.energy_workers.size()) {
		return query.energy_workers[worker_index];
	}
	if (dispatched_mandatory) {
		for (const auto &assignment : query.dynamic_mandatory_workers) {
			if (assignment.worker_ordinal == worker_index) {
				return assignment;
			}
		}
	} else {
		for (const auto &assignment : query.dynamic_optional_workers) {
			if (assignment.worker_ordinal == worker_index) {
				return assignment;
			}
		}
	}
	return QuerySLAEnergyWorkerAssignment();
}

static void SetWorkerSelection(const QuerySLASchedulerState::WorkerAssignment &assignment,
	                           QuerySchedulerPolicy policy, QuerySLAWorkerSelection *selection) {
	if (!selection) {
		return;
	}
	selection->hardware = assignment.execution_hardware.IsValid() ? assignment.execution_hardware
	                                                           : assignment.normal_hardware;
	selection->policy = policy;
	selection->plan_generation = assignment.plan_generation;
	selection->lease_generation = assignment.lease_generation;
	selection->db_query_id = assignment.db_query_id;
	selection->request_id = assignment.request_id;
	selection->pipeline_generation = assignment.pipeline_generation;
	selection->pipeline_id = assignment.pipeline_id;
	selection->worker_ordinal = assignment.worker_ordinal;
	selection->class_ordinal = assignment.class_ordinal;
	selection->assigned = assignment.db_query_id != 0;
	selection->mandatory = assignment.mandatory;
	selection->optional = assignment.optional;
	selection->exploration = assignment.exploration;
	selection->debug_trace_enabled = assignment.debug_trace_enabled;
}

static void ClearWorkerRecordsForQuery(vector<QuerySLASchedulerState::WorkerAssignment> &records,
	                                   uint64_t db_query_id) {
	for (auto &record : records) {
		if (record.db_query_id == db_query_id) {
			record = QuerySLASchedulerState::WorkerAssignment();
		}
	}
}

static QuerySLASchedulerState::WorkerAssignment
ResolveEnergyLease(const QuerySLASchedulerState &state, const QuerySLASchedulerState::QueryState &query,
	               idx_t worker_id, idx_t worker_ordinal, idx_t class_ordinal, bool mandatory,
	               uint64_t plan_generation, uint64_t lease_generation) {
	QuerySLASchedulerState::WorkerAssignment result;
	result.db_query_id = query.metadata.db_query_id;
	result.request_id = query.metadata.request_id;
	result.pipeline_generation = query.pipeline_generation;
	result.pipeline_id = query.model.pipeline_id;
	result.plan_generation = plan_generation;
	result.lease_generation = lease_generation;
	result.worker_ordinal = worker_ordinal;
	result.class_ordinal = class_ordinal;
	result.mandatory = mandatory;
	result.optional = !mandatory;
	result.debug_trace_enabled = query.debug_trace_enabled;

	if (worker_id < state.worker_hints.size()) {
		const auto &hint = state.worker_hints[worker_id];
		if (hint.db_query_id == result.db_query_id && hint.pipeline_generation == result.pipeline_generation &&
		    hint.class_ordinal == class_ordinal && hint.mandatory == mandatory) {
			result.normal_hardware = hint.normal_hardware;
			result.execution_hardware = hint.execution_hardware;
			result.exploration = hint.exploration;
			return result;
		}
	}

	auto selected = EnergyAssignmentAt(query, worker_ordinal, mandatory);
	result.normal_hardware = selected.hardware;
	result.execution_hardware = selected.hardware;
	result.exploration = selected.exploration;
	return result;
}

static void RefreshDispatchEntries(QuerySLASchedulerState &state, QuerySLASchedulerState::QueryState &query) {
	query.dispatch_version++;
	if (!query.event || !query.model.valid || query.model.demand_cap == 0) {
		return;
	}
	auto planned = MinValue<idx_t>(PlannedWorkers(query), query.model.demand_cap);
	if (query.assigned_workers < planned) {
		auto workers = query.assigned_workers;
		if (query.assigned_mandatory_workers < query.mandatory_workers) {
			auto gain = workers < query.model.mandatory_gain.size() ? query.model.mandatory_gain[workers] : 0;
			state.primary_mandatory.emplace(gain, query.metadata.db_query_id, query.dispatch_version, workers);
		} else if (query.assigned_optional_workers < query.optional_workers) {
			auto gain = workers < query.model.optional_gain.size() ? query.model.optional_gain[workers] : 0;
			if (gain > 0) {
				state.primary_optional.emplace(gain, query.metadata.db_query_id, query.dispatch_version, workers);
			}
		}
	}
	if (planned >= query.model.demand_cap) {
		return;
	}
	auto primary_filled = query.assigned_mandatory_workers >= query.mandatory_workers &&
	                      query.assigned_optional_workers >= query.optional_workers &&
	                      query.assigned_residual_workers >= query.residual_workers &&
	                      query.assigned_workers == planned;
	if (primary_filled && planned < query.model.mandatory_gain.size() && query.model.mandatory_gain[planned] > 0) {
		state.dynamic_mandatory.emplace(query.model.mandatory_gain[planned], query.metadata.db_query_id,
		                                query.dispatch_version, planned);
	}
	if (primary_filled && planned < query.model.optional_gain.size() && query.model.optional_gain[planned] > 0) {
		state.dynamic_optional.emplace(query.model.optional_gain[planned], query.metadata.db_query_id,
		                               query.dispatch_version, planned);
	}
	if (primary_filled && query.residual_dispatch_enabled && planned < query.model.mandatory_gain.size() &&
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
	bool wake_workers = false;
	{
		unique_lock<mutex> guard(state->lock);
		state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
		auto entry = state->queries.find(db_query_id);
		if (entry == state->queries.end()) {
			return;
		}
		ClearWorkerRecordsForQuery(state->worker_assignments, db_query_id);
		ClearWorkerRecordsForQuery(state->worker_hints, db_query_id);
		ClearWorkerRecordsForQuery(state->worker_hardware_reservations, db_query_id);
		if (entry->second.debug_trace_enabled) {
			state->debug_trace_query_count--;
		}
		state->queries.erase(entry);
		wake_workers = db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY;
	}
	if (wake_workers) {
		db.GetScheduler().Signal(NumericCast<idx_t>(db.GetScheduler().NumberOfThreads()));
	}
}

void QuerySLAScheduler::OnEventScheduled(uint64_t db_query_id, shared_ptr<Event> event,
	                                     idx_t remaining_suffix_stages) {
	if (!Enabled()) {
		return;
	}
	QueryCapture capture;
	{
		unique_lock<mutex> guard(state->lock);
		state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
		auto entry = state->queries.find(db_query_id);
		if (entry == state->queries.end()) {
			throw InternalException("SLA scheduler received an event for an unregistered query");
		}
		auto &query = entry->second;
		ClearWorkerRecordsForQuery(state->worker_assignments, db_query_id);
		ClearWorkerRecordsForQuery(state->worker_hints, db_query_id);
		HoldHardwareReservationsForQuery(state->worker_hardware_reservations, db_query_id);
		query.event = event;
		query.pipeline_generation++;
		query.assigned_workers = 0;
		query.assigned_mandatory_workers = 0;
		query.assigned_optional_workers = 0;
		query.assigned_residual_workers = 0;
		query.remaining_suffix_stages = MaxValue<idx_t>(idx_t(1), remaining_suffix_stages);
		capture.metadata = query.metadata;
		capture.producer = query.producer;
		capture.event = event;
		capture.generation = query.pipeline_generation;
		capture.remaining_suffix_stages = query.remaining_suffix_stages;
		capture.debug_trace_enabled = query.debug_trace_enabled;
	}

	auto activated = BuildActivatedPipelinePlan(db, *state->hardware_manager, capture, SLATimestampNs(),
	                                            EpochMs() * 1000000ULL);
	bool signal_worker = false;
	{
		unique_lock<mutex> guard(state->lock);
		state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
		auto entry = state->queries.find(db_query_id);
		if (entry == state->queries.end() || entry->second.pipeline_generation != capture.generation ||
		    entry->second.event.get() != event.get()) {
			return;
		}
		auto &query = entry->second;
		query.activation_suffix = activated.suffix;
		query.model = std::move(activated.model);
		query.mandatory_workers = 0;
		query.optional_workers = 0;
		query.residual_workers = 0;
		query.assigned_workers = 0;
		query.assigned_mandatory_workers = 0;
		query.assigned_optional_workers = 0;
		query.assigned_residual_workers = 0;
		query.energy_workers.clear();
		query.dynamic_mandatory_workers = std::move(activated.dynamic_mandatory_workers);
		query.dynamic_optional_workers = std::move(activated.dynamic_optional_workers);
		query.energy_limited_plan = activated.energy_policy;
		query.residual_dispatch_enabled =
		    !query.energy_limited_plan && db.config.options.query_sla_residual_workers_enabled;
		RefreshDispatchEntries(*state, query);
		signal_worker = activated.claimable;
	}
	if (db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY) {
		db.GetScheduler().Signal(NumericCast<idx_t>(db.GetScheduler().NumberOfThreads()));
	} else if (signal_worker) {
		db.GetScheduler().Signal(1);
	}
}

void QuerySLAScheduler::OnEventFinished(uint64_t db_query_id, Event &event) {
	if (!Enabled()) {
		return;
	}
	{
		unique_lock<mutex> guard(state->lock);
		state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
		auto entry = state->queries.find(db_query_id);
		if (entry == state->queries.end() || entry->second.event.get() != &event) {
			return;
		}
		entry->second.event.reset();
		entry->second.model = QuerySLAModel();
		entry->second.mandatory_workers = 0;
		entry->second.optional_workers = 0;
		entry->second.residual_workers = 0;
		entry->second.assigned_workers = 0;
		entry->second.assigned_mandatory_workers = 0;
		entry->second.assigned_optional_workers = 0;
		entry->second.assigned_residual_workers = 0;
		entry->second.energy_workers.clear();
		entry->second.dynamic_mandatory_workers.clear();
		entry->second.dynamic_optional_workers.clear();
		entry->second.energy_limited_plan = false;
		entry->second.pipeline_generation++;
		ClearWorkerRecordsForQuery(state->worker_assignments, db_query_id);
		ClearWorkerRecordsForQuery(state->worker_hints, db_query_id);
		HoldHardwareReservationsForQuery(state->worker_hardware_reservations, db_query_id);
		RefreshDispatchEntries(*state, entry->second);
	}
}

void QuerySLAScheduler::RequestInitialEpoch() {
	if (Enabled()) {
		state->next_epoch_ns.store(0);
		db.GetScheduler().Signal(1);
	}
}

bool QuerySLAScheduler::TryAcquireEpochSentinel(uint64_t &deadline_ns) {
	if (!Enabled()) {
		return false;
	}
	{
		lock_guard<mutex> guard(state->lock);
		bool has_active_event = false;
		for (const auto &entry : state->queries) {
			if (entry.second.event) {
				has_active_event = true;
				break;
			}
		}
		if (!has_active_event) {
			return false;
		}
	}
	bool expected = false;
	if (!state->epoch_sentinel.compare_exchange_strong(expected, true)) {
		return false;
	}
	deadline_ns = state->next_epoch_ns.load();
	return true;
}

void QuerySLAScheduler::ReleaseEpochSentinel() {
	state->epoch_sentinel.store(false);
}

void QuerySLAScheduler::RegisterWorker(idx_t worker_id, int logical_cpu) {
	{
		lock_guard<mutex> guard(state->lock);
		if (worker_id >= state->worker_hardware_reservations.size()) {
			state->worker_hardware_reservations.resize(worker_id + 1);
		}
	}
	state->hardware_manager->RegisterWorker(worker_id, logical_cpu);
}

void QuerySLAScheduler::UnregisterWorker(idx_t worker_id) {
	{
		lock_guard<mutex> guard(state->lock);
		if (worker_id < state->worker_hardware_reservations.size()) {
			state->worker_hardware_reservations[worker_id] = QuerySLASchedulerState::WorkerAssignment();
		}
	}
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
	idx_t wake_count = DConstants::INVALID_INDEX;
	try {
		wake_count = RunEpoch(now_ns);
	} catch (...) {
		state->next_epoch_ns.store(now_ns + EpochMs() * 1000000ULL);
		state->epoch_leader.store(false);
		throw;
	}
	state->next_epoch_ns.store(now_ns + EpochMs() * 1000000ULL);
	state->epoch_run_count++;
	state->epoch_leader.store(false);
	if (wake_count == DConstants::INVALID_INDEX || wake_count == 0) {
		return;
	}
	db.GetScheduler().Signal(MinValue<idx_t>(wake_count, NumericCast<idx_t>(db.GetScheduler().NumberOfThreads())));
}

idx_t QuerySLAScheduler::RunEpoch(uint64_t now_ns) {
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
			capture.activation_suffix = entry.second.activation_suffix;
			capture.debug_trace_enabled = entry.second.debug_trace_enabled;
			captures.push_back(std::move(capture));
		}
		if (collect_diagnostics) {
			capture_lock_hold_ns = SLADurationNs(SLATimestampNs(), capture_lock_acquired_ns);
		}
	}
	if (captures.empty()) {
		return 0;
	}

	const auto work_snapshot_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	vector<PipelineWorkSnapshot> work(captures.size());
	auto suffix_model = ResolveQuerySLASuffixModel(db.config.options.query_sla_suffix_model, db.GetQuerySchedulerPolicy());
	vector<DownstreamSuffixEpochRequest> suffix_requests;
	vector<idx_t> suffix_indexes(captures.size(), DConstants::INVALID_INDEX);
	for (idx_t i = 0; i < captures.size(); i++) {
		if (!captures[i].event->GetPipelineWorkSnapshot(work[i])) {
			continue;
		}
		auto event_kind = captures[i].event->GetQueryActivationKind();
		if (suffix_model == QuerySLASuffixModel::POINT || !UsesPipelineSuffix(event_kind) ||
		    !work[i].profile_identity.valid) {
			continue;
		}
		suffix_indexes[i] = suffix_requests.size();
		suffix_requests.emplace_back(captures[i].metadata.template_id, captures[i].metadata.scale_factor,
		                             work[i].profile_identity.pipeline_id,
		                             work[i].profile_identity.pipeline_signature_hash,
		                             captures[i].remaining_suffix_stages, work[i].profile_identity.pipeline_structure_hash);
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
		if (UsesPipelineSuffix(event_kind)) {
			if (suffix_model == QuerySLASuffixModel::POINT) {
				suffix = captures[i].activation_suffix;
			} else if (suffix_indexes[i] != DConstants::INVALID_INDEX && suffix_indexes[i] < suffix_estimates.size()) {
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
	auto logical_allocation =
	    AllocateQuerySLAWorkers(models, worker_budget,
	                            !energy_policy && db.config.options.query_sla_residual_workers_enabled);
	vector<idx_t> mandatory(captures.size(), 0);
	vector<idx_t> optional(captures.size(), 0);
	vector<idx_t> residual(captures.size(), 0);
	vector<vector<QuerySLAEnergyWorkerAssignment>> energy_workers(captures.size());
	vector<vector<QuerySLAEnergyWorkerAssignment>> dynamic_mandatory_workers(captures.size());
	vector<vector<QuerySLAEnergyWorkerAssignment>> dynamic_optional_workers(captures.size());
	QuerySLAEnergyEpochPlan energy_epoch_plan;
	vector<QueryHardwareWorkerTopology> energy_topology;
	auto published_generation = state->epoch_generation.load() + 1;
	if (energy_policy) {
		QuerySLAEnergyPlannerInput planner_input;
		auto reference_hardware = state->hardware_manager->ReferenceConfiguration();
		energy_topology = state->hardware_manager->GetWorkerTopology();
		planner_input.topology = energy_topology;
		planner_input.core_frequency_levels_khz = state->hardware_manager->CoreFrequencyLevelsKHz();
		planner_input.uncore_frequency_levels_khz = state->hardware_manager->UncoreFrequencyLevelsKHz();
		planner_input.worker_budget = worker_budget;
		planner_input.epoch_ns = EpochMs() * 1000000ULL;
		planner_input.epoch_generation = published_generation;
		planner_input.logical_allocation = logical_allocation;
		planner_input.exploration_enabled = db.config.options.query_sla_energy_exploration_enabled;
		planner_input.exploration_seed = db.config.options.query_sla_energy_exploration_seed;
		planner_input.epochs_without_optional_only_socket = epochs_without_optional_only_socket;
		auto &profile_store = db.GetQueryRequestProfileStore();
		bool stable_profile_snapshot = false;
		for (idx_t attempt = 0; attempt < 3; attempt++) {
			auto profile_generation = profile_store.GetPipelineHardwareProfileGeneration();
			planner_input.queries.clear();
			planner_input.queries.reserve(captures.size());
			for (idx_t i = 0; i < captures.size(); i++) {
				QuerySLAEnergyModelInput input;
				input.sla_input = model_inputs[i];
				input.reference_model = models[i];
				input.reference_hardware = reference_hardware;
				if (work[i].profile_identity.valid) {
					input.hardware_profiles = profile_store.GetPipelineHardwareProfiles(
					    captures[i].metadata.template_id, captures[i].metadata.scale_factor, work[i].profile_identity);
				}
				planner_input.queries.push_back(std::move(input));
			}
			if (profile_store.GetPipelineHardwareProfileGeneration() != profile_generation) {
				continue;
			}
			energy_epoch_plan = PlanQuerySLAEnergyEpoch(planner_input);
			if (profile_store.GetPipelineHardwareProfileGeneration() == profile_generation) {
				stable_profile_snapshot = true;
				break;
			}
		}
		if (!stable_profile_snapshot || !energy_epoch_plan.valid) {
			// Hardware profiling and placement are secondary to the logical SLA allocation. Retry with only the
			// calibrated reference pair so an unstable or unusable hardware profile cannot veto runnable work.
			for (auto &query : planner_input.queries) {
				query.hardware_profiles.clear();
			}
			planner_input.exploration_enabled = false;
			energy_epoch_plan = PlanQuerySLAEnergyEpoch(planner_input);
		}
		if (!energy_epoch_plan.valid) {
			throw InternalException("SLA-energy reference placement failed after logical allocation: %s",
			                        energy_epoch_plan.error);
		}
		mandatory = logical_allocation.mandatory;
		optional = logical_allocation.optional;
		for (idx_t i = 0; i < energy_epoch_plan.queries.size(); i++) {
			energy_workers[i] = energy_epoch_plan.queries[i].workers;
			dynamic_mandatory_workers[i] = energy_epoch_plan.queries[i].dynamic_mandatory_workers;
			dynamic_optional_workers[i] = energy_epoch_plan.queries[i].dynamic_optional_workers;
		}
	} else {
		mandatory = logical_allocation.mandatory;
		optional = logical_allocation.optional;
		residual = logical_allocation.residual;
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
			auto planned_workers = mandatory[i] + optional[i] + residual[i];
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
				input.mandatory_target_sla_cost = query_plan.mandatory_target_sla_cost;
				input.mandatory_target_optional_risk = query_plan.mandatory_target_optional_risk;
				input.mandatory_reference_safe_throughput = query_plan.mandatory_reference_safe_throughput;
				input.mandatory_preferred_normalized_edp = query_plan.mandatory_preferred_normalized_edp;
				input.mandatory_profile_version = query_plan.mandatory_profile_version;
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
	const auto publish_wait_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	unique_lock<mutex> guard(state->lock);
	const auto publish_lock_acquired_ns = collect_diagnostics ? SLATimestampNs() : 0;
	const auto publish_lock_wait_ns = SLADurationNs(publish_lock_acquired_ns, publish_wait_start_ns);
	if (energy_policy) {
		state->worker_hints.clear();
		for (const auto &topology : energy_topology) {
			if (topology.worker_id >= state->worker_hints.size()) {
				state->worker_hints.resize(topology.worker_id + 1);
			}
		}
	} else {
		state->worker_hints.clear();
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
		entry->second.energy_workers = std::move(energy_workers[i]);
		entry->second.dynamic_mandatory_workers = std::move(dynamic_mandatory_workers[i]);
		entry->second.dynamic_optional_workers = std::move(dynamic_optional_workers[i]);
		entry->second.energy_limited_plan = energy_policy;
		entry->second.residual_dispatch_enabled =
		    !energy_policy && db.config.options.query_sla_residual_workers_enabled;
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
			if (placed.worker_id >= state->worker_hints.size()) {
				state->worker_hints.resize(placed.worker_id + 1);
			}
			auto &hint = state->worker_hints[placed.worker_id];
			hint.db_query_id = query_entry->second.metadata.db_query_id;
			hint.request_id = query_entry->second.metadata.request_id;
			hint.pipeline_generation = query_entry->second.pipeline_generation;
			hint.pipeline_id = query_entry->second.model.pipeline_id;
			hint.normal_hardware = placed.normal_hardware;
			hint.execution_hardware = placed.execution_hardware;
			hint.plan_generation = published_generation;
			hint.worker_ordinal = placed.worker_ordinal;
			hint.class_ordinal =
			    placed.mandatory
			        ? placed.worker_ordinal
			        : placed.worker_ordinal >= mandatory[placed.query_index]
			              ? placed.worker_ordinal - mandatory[placed.query_index]
			              : DConstants::INVALID_INDEX;
			hint.mandatory = placed.mandatory;
			hint.optional = placed.optional;
			hint.exploration = placed.exploration;
			hint.debug_trace_enabled = query_entry->second.debug_trace_enabled;
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
		for (idx_t worker_id = 0; worker_id < state->worker_assignments.size(); worker_id++) {
			auto &assignment = state->worker_assignments[worker_id];
			if (assignment.db_query_id == 0) {
				continue;
			}
			auto query_entry = state->queries.find(assignment.db_query_id);
			if (query_entry == state->queries.end() ||
			    query_entry->second.pipeline_generation != assignment.pipeline_generation ||
			    !query_entry->second.event || !query_entry->second.model.valid) {
				continue;
			}
			auto worker_ordinal = assignment.mandatory
			                          ? assignment.class_ordinal
			                          : query_entry->second.mandatory_workers + assignment.class_ordinal;
			auto refreshed = ResolveEnergyLease(*state, query_entry->second, worker_id, worker_ordinal,
			                                    assignment.class_ordinal, assignment.mandatory,
			                                    published_generation, assignment.lease_generation);
			if (!refreshed.execution_hardware.IsValid()) {
				refreshed.normal_hardware = state->hardware_manager->ReferenceConfiguration();
				refreshed.execution_hardware = refreshed.normal_hardware;
			}
			assignment = std::move(refreshed);
		}
		auto reservation_count = MaxValue<idx_t>(state->worker_assignments.size(), state->worker_hints.size());
		state->worker_hardware_reservations.assign(reservation_count,
		                                           QuerySLASchedulerState::WorkerAssignment());
		for (idx_t worker_id = 0; worker_id < state->worker_assignments.size(); worker_id++) {
			const auto &assignment = state->worker_assignments[worker_id];
			if (assignment.db_query_id != 0) {
				InstallHardwareReservation(*state, worker_id, assignment);
			}
		}
		for (idx_t worker_id = 0; worker_id < state->worker_hints.size(); worker_id++) {
			const auto &hint = state->worker_hints[worker_id];
			if (hint.db_query_id == 0 ||
			    (worker_id < state->worker_assignments.size() &&
			     state->worker_assignments[worker_id].db_query_id != 0)) {
				continue;
			}
			bool class_reserved = false;
			for (const auto &reservation : state->worker_hardware_reservations) {
				if (SameAssignmentClass(reservation, hint)) {
					class_reserved = true;
					break;
				}
			}
			if (!class_reserved) {
				InstallHardwareReservation(*state, worker_id, hint);
			}
		}
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
	idx_t published_worker_count = 0;
	for (const auto &entry : state->queries) {
		if (entry.second.event && entry.second.model.valid) {
			published_worker_count += MinValue<idx_t>(PlannedWorkers(entry.second), entry.second.model.demand_cap);
		}
	}
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
	auto hardware_target_plan =
	    energy_policy ? BuildHardwareTargetPlan(*state, *state->hardware_manager, published_generation) : nullptr;
	state->hardware_plan_publication_in_progress = hardware_target_plan != nullptr;
	guard.unlock();
	const auto hardware_apply_start_ns = collect_diagnostics && hardware_target_plan ? SLATimestampNs() : 0;
	try {
		if (hardware_target_plan) {
			state->hardware_manager->PublishTargets(std::move(hardware_target_plan));
		}
	} catch (...) {
		{
			lock_guard<mutex> publication_guard(state->lock);
			state->hardware_plan_publication_in_progress = false;
		}
		state->hardware_plan_cv.notify_all();
		throw;
	}
	if (energy_policy) {
		{
			lock_guard<mutex> publication_guard(state->lock);
			state->hardware_plan_publication_in_progress = false;
		}
		state->hardware_plan_cv.notify_all();
	}
	const auto hardware_apply_ns =
	    hardware_apply_start_ns == 0 ? 0 : SLADurationNs(SLATimestampNs(), hardware_apply_start_ns);

	if (!collect_diagnostics || trace_inputs.empty()) {
		return published_worker_count;
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
			row.exploration = worker.exploration;
			row.optional_stage = QuerySLAEnergyOptionalStageToString(worker.optional_stage);
			row.preferred_hardware = worker.preferred_hardware;
			row.normal_hardware = worker.normal_hardware;
			row.execution_hardware = worker.execution_hardware;
			row.applied_hardware = state->hardware_manager->GetAppliedConfigurationForCPU(row.logical_cpu);
			row.preferred_safe_throughput = worker.preferred_safe_throughput;
			row.preferred_normalized_edp = worker.preferred_normalized_edp;
			row.normal_safe_throughput = worker.normal_safe_throughput;
			row.normal_active_power_w = worker.normal_active_power_w;
			row.normal_normalized_edp = worker.normal_normalized_edp;
			row.optional_fragility_before = worker.optional_fragility_before;
			row.optional_fragility_target = worker.optional_fragility_target;
			row.optional_fragility_after = worker.optional_fragility_after;
			row.optional_gain = worker.optional_gain;
			row.profile_version = worker.profile_version;
			row.normal_profile_version = worker.normal_profile_version;
			row.worker_ordinal = worker.worker_ordinal;
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
		trace.suffix_model = QuerySLASuffixModelName(suffix.model);
		trace.suffix_generation = suffix.generation;
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
		trace.planned_workers = mandatory[i] + optional[i] + residual[i];
		trace.assigned_workers = input.assigned_workers;
		trace.reference_mandatory_workers = logical_allocation.mandatory[i];
		trace.reference_optional_workers = logical_allocation.optional[i];
		trace.energy_logical_plan_match =
		    !energy_policy ||
		    (mandatory[i] == logical_allocation.mandatory[i] && optional[i] == logical_allocation.optional[i]);
		if (energy_policy) {
			const auto &diagnostics = energy_epoch_plan.diagnostics;
			trace.energy_mandatory_workers = diagnostics.mandatory_workers;
			trace.energy_o1_workers = diagnostics.o1_workers;
			trace.energy_o2_workers = diagnostics.o2_workers;
			trace.energy_o3_workers = diagnostics.o3_workers;
			trace.energy_reference_fallback_workers = diagnostics.reference_fallback_workers;
			trace.energy_parked_workers = diagnostics.parked_workers;
			trace.energy_core_probes = diagnostics.core_probes;
			trace.energy_uncore_probes = diagnostics.uncore_probes;
			trace.energy_forced_uncore_probes = diagnostics.forced_uncore_probes;
			trace.energy_optional_only_socket_available = diagnostics.optional_only_socket_available;
			trace.energy_epochs_without_optional_only_socket =
			    exploration_diagnostics.epochs_without_optional_only_socket;
			trace.energy_profile_fallback_count = diagnostics.profile_fallback_count;
			trace.energy_immature_pair_rejections = diagnostics.immature_pair_rejections;
			trace.energy_worker_frequency_adjustments = diagnostics.worker_frequency_adjustments;
			trace.energy_mandatory_target_sla_cost = input.mandatory_target_sla_cost;
			trace.energy_mandatory_target_optional_risk = input.mandatory_target_optional_risk;
			trace.energy_mandatory_reference_safe_throughput = input.mandatory_reference_safe_throughput;
			trace.energy_mandatory_preferred_normalized_edp = input.mandatory_preferred_normalized_edp;
			trace.energy_mandatory_profile_version = input.mandatory_profile_version;
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
		return published_worker_count;
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
	return published_worker_count;
}

QuerySLADequeueResult QuerySLAScheduler::TryDequeueTask(idx_t worker_id, shared_ptr<Task> &task,
	                                                   QuerySLAWorkerSelection *selection) {
	if (selection) {
		*selection = QuerySLAWorkerSelection();
	}
	if (!Enabled()) {
		return QuerySLADequeueResult::NOT_ACTIVE;
	}
	auto policy = db.GetQuerySchedulerPolicy();
	MaybeRunEpoch();
	unique_lock<mutex> guard(state->lock);
	state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
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
		    entry->second.assigned_workers > 0) {
			ReleaseAssignedWorker(entry->second, assignment);
			RefreshDispatchEntries(*state, entry->second);
		}
		assignment = QuerySLASchedulerState::WorkerAssignment();
	}
	auto assign_worker = [&](QuerySLASchedulerState::QueryState &query, bool dispatched_mandatory,
	                         bool dispatched_optional, idx_t worker_ordinal, idx_t class_ordinal) {
		auto lease_generation = ++state->next_lease_generation;
		if (query.energy_limited_plan) {
			assignment = ResolveEnergyLease(*state, query, worker_id, worker_ordinal, class_ordinal, dispatched_mandatory,
			                                state->epoch_generation.load(), lease_generation);
			if (!assignment.execution_hardware.IsValid()) {
				assignment.normal_hardware = state->hardware_manager->ReferenceConfiguration();
				assignment.execution_hardware = assignment.normal_hardware;
			}
		} else {
			assignment = QuerySLASchedulerState::WorkerAssignment();
			assignment.db_query_id = query.metadata.db_query_id;
			assignment.request_id = query.metadata.request_id;
			assignment.pipeline_generation = query.pipeline_generation;
			assignment.pipeline_id = query.model.pipeline_id;
			assignment.plan_generation = state->epoch_generation.load();
			assignment.lease_generation = lease_generation;
			assignment.worker_ordinal = worker_ordinal;
			assignment.class_ordinal = class_ordinal;
			assignment.mandatory = dispatched_mandatory;
			assignment.optional = dispatched_optional;
			assignment.debug_trace_enabled = query.debug_trace_enabled;
		}
		if (query.energy_limited_plan) {
			InstallHardwareReservation(*state, worker_id, assignment);
		}
		SetWorkerSelection(assignment, policy, selection);
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
				auto class_underfilled = mandatory
				                             ? query.assigned_mandatory_workers < query.mandatory_workers
				                             : query.assigned_mandatory_workers >= query.mandatory_workers &&
				                                   query.assigned_optional_workers < query.optional_workers;
				if (query.assigned_workers >= planned || !class_underfilled) {
					continue;
				}
			} else {
				auto primary_filled = query.assigned_mandatory_workers >= query.mandatory_workers &&
				                      query.assigned_optional_workers >= query.optional_workers &&
				                      query.assigned_residual_workers >= query.residual_workers;
				if (!primary_filled || query.assigned_workers != planned || planned >= query.model.demand_cap) {
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
			auto worker_ordinal = query.assigned_workers;
			if (!primary) {
				if (mandatory) {
					query.mandatory_workers++;
				} else {
					query.optional_workers++;
				}
			}
			auto class_ordinal = NextClassOrdinal(*state, query, mandatory, !mandatory);
			query.assigned_workers++;
			if (mandatory) {
				query.assigned_mandatory_workers++;
			} else {
				query.assigned_optional_workers++;
			}
			assign_worker(query, mandatory, !mandatory, worker_ordinal, class_ordinal);
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
			auto primary_filled = query.assigned_mandatory_workers >= query.mandatory_workers &&
			                      query.assigned_optional_workers >= query.optional_workers &&
			                      query.assigned_residual_workers >= query.residual_workers;
			if (candidate.version != query.dispatch_version || !query.event || !query.model.valid ||
			    !primary_filled || query.assigned_workers != planned || planned >= query.model.demand_cap ||
			    candidate.workers != planned || planned >= query.model.mandatory_gain.size() ||
			    planned >= query.model.optional_gain.size()) {
				continue;
			}
			if (!query.producer || !db.GetScheduler().GetTaskFromProducer(*query.producer, task)) {
				unavailable.push_back(candidate);
				continue;
			}
			query.residual_workers++;
			auto worker_ordinal = query.assigned_workers;
			auto class_ordinal = NextClassOrdinal(*state, query, false, false);
			query.assigned_workers++;
			query.assigned_residual_workers++;
			assign_worker(query, false, false, worker_ordinal, class_ordinal);
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
	    try_dispatch(state->dynamic_mandatory, false, true) ||
	    try_dispatch(state->primary_optional, true, false) ||
	    try_dispatch(state->dynamic_optional, false, false) || try_residual_dispatch()) {
		return QuerySLADequeueResult::TASK_FOUND;
	}
	return QuerySLADequeueResult::NO_TASK;
}

QuerySLAContinuationResult QuerySLAScheduler::CheckTaskContinuation(idx_t worker_id,
	                                                                 const QuerySLAWorkerSelection &previous,
	                                                                 QuerySLAWorkerSelection &current) {
	current = QuerySLAWorkerSelection();
	if (!Enabled()) {
		return QuerySLAContinuationResult::YIELD;
	}
	auto policy = db.GetQuerySchedulerPolicy();
	if (!previous.assigned || previous.policy != policy) {
		return QuerySLAContinuationResult::YIELD;
	}
	MaybeRunEpoch();
	unique_lock<mutex> guard(state->lock);
	state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
	if (worker_id >= state->worker_assignments.size()) {
		return QuerySLAContinuationResult::YIELD;
	}
	const auto &assignment = state->worker_assignments[worker_id];
	if (assignment.db_query_id == 0 || assignment.db_query_id != previous.db_query_id ||
	    assignment.pipeline_generation != previous.pipeline_generation ||
	    assignment.lease_generation != previous.lease_generation) {
		return QuerySLAContinuationResult::YIELD;
	}
	auto entry = state->queries.find(assignment.db_query_id);
	if (entry == state->queries.end() || entry->second.pipeline_generation != assignment.pipeline_generation ||
	    !entry->second.event || !entry->second.model.valid) {
		return QuerySLAContinuationResult::YIELD;
	}
	auto lease_in_plan = assignment.mandatory
	                         ? assignment.class_ordinal < entry->second.mandatory_workers
	                         : assignment.optional
	                               ? assignment.class_ordinal < entry->second.optional_workers
	                               : assignment.class_ordinal < entry->second.residual_workers;
	if (entry->second.assigned_workers > PlannedWorkers(entry->second) || !lease_in_plan) {
		return QuerySLAContinuationResult::YIELD;
	}
	SetWorkerSelection(assignment, policy, &current);
	return QuerySLAContinuationResult::CONTINUE;
}

void QuerySLAScheduler::ReleaseTaskAssignment(idx_t worker_id, const QuerySLAWorkerSelection &previous) {
	if (!previous.assigned || !IsQuerySLAPolicy(previous.policy)) {
		return;
	}
	bool wake_worker = false;
	{
		lock_guard<mutex> guard(state->lock);
		if (worker_id >= state->worker_assignments.size()) {
			return;
		}
		auto &assignment = state->worker_assignments[worker_id];
		if (assignment.db_query_id != previous.db_query_id ||
		    assignment.pipeline_generation != previous.pipeline_generation ||
		    assignment.lease_generation != previous.lease_generation) {
			return;
		}
		auto entry = state->queries.find(assignment.db_query_id);
		if (entry != state->queries.end() &&
		    entry->second.pipeline_generation == assignment.pipeline_generation) {
			auto &query = entry->second;
			ReleaseAssignedWorker(query, assignment);
			RefreshDispatchEntries(*state, query);
			auto planned = MinValue<idx_t>(PlannedWorkers(query), query.model.demand_cap);
			wake_worker = query.event && query.model.valid && query.assigned_workers < planned;
		}
		assignment = QuerySLASchedulerState::WorkerAssignment();
	}
	if (wake_worker) {
		db.GetScheduler().Signal(1);
	}
}

bool QuerySLAScheduler::GetWorkerHardwareReservation(idx_t worker_id, QuerySLAWorkerSelection &reservation) {
	reservation = QuerySLAWorkerSelection();
	if (!Enabled() || db.GetQuerySchedulerPolicy() != QuerySchedulerPolicy::SLA_ENERGY) {
		return false;
	}
	unique_lock<mutex> guard(state->lock);
	state->hardware_plan_cv.wait(guard, [&]() { return !state->hardware_plan_publication_in_progress; });
	if (worker_id >= state->worker_hardware_reservations.size()) {
		return false;
	}
	auto &current = state->worker_hardware_reservations[worker_id];
	if (current.db_query_id == 0) {
		return false;
	}
	auto query = state->queries.find(current.db_query_id);
	if (query == state->queries.end()) {
		current = QuerySLASchedulerState::WorkerAssignment();
		return false;
	}
	if (!current.hardware_transition_hold &&
	    (query->second.pipeline_generation != current.pipeline_generation || !query->second.event ||
	     !query->second.model.valid || !AssignmentClassIsPlanned(query->second, current))) {
		current = QuerySLASchedulerState::WorkerAssignment();
		return false;
	}
	SetWorkerSelection(current, QuerySchedulerPolicy::SLA_ENERGY, &reservation);
	return reservation.assigned;
}

vector<QuerySLASchedulerSnapshot> QuerySLAScheduler::GetSnapshot() const {
	lock_guard<mutex> guard(state->lock);
	vector<QuerySLASchedulerSnapshot> result;
	result.reserve(state->queries.size());
	auto snapshot_timestamp_ns = SLATimestampNs();
	auto epoch_run_count = state->epoch_run_count.load();
	auto epoch_generation = state->epoch_generation.load();
	auto next_epoch_ns = state->next_epoch_ns.load();
	auto epoch_leader_active = state->epoch_leader.load();
	auto epoch_sentinel_active = state->epoch_sentinel.load();
	for (const auto &entry : state->queries) {
		const auto &query = entry.second;
		QuerySLASchedulerSnapshot snapshot;
		snapshot.snapshot_timestamp_ns = snapshot_timestamp_ns;
		snapshot.epoch_run_count = epoch_run_count;
		snapshot.epoch_generation = epoch_generation;
		snapshot.next_epoch_ns = next_epoch_ns;
		snapshot.epoch_leader_active = epoch_leader_active;
		snapshot.epoch_sentinel_active = epoch_sentinel_active;
		snapshot.db_query_id = query.metadata.db_query_id;
		snapshot.request_id = query.metadata.request_id;
		snapshot.deadline_ns = query.metadata.deadline_ns;
		snapshot.deadline_slack_ms =
		    (static_cast<double>(query.metadata.deadline_ns) - static_cast<double>(snapshot_timestamp_ns)) / 1000000.0;
		snapshot.pipeline_id = query.model.pipeline_id;
		snapshot.pipeline_generation = query.pipeline_generation;
		snapshot.event_kind = SLAEventKindName(query.model.event_kind);
		snapshot.event_present = query.event != nullptr;
		if (query.event) {
			snapshot.event_finished = query.event->IsFinished();
			snapshot.scheduled_tasks = query.event->GetTotalTasks();
			snapshot.finished_tasks = MinValue<idx_t>(query.event->GetFinishedTasks(), snapshot.scheduled_tasks);
			snapshot.blocked_tasks =
			    MinValue<idx_t>(query.event->GetBlockedTasks(), snapshot.scheduled_tasks - snapshot.finished_tasks);
			snapshot.remaining_tasks = snapshot.scheduled_tasks - snapshot.finished_tasks;
			PipelineWorkSnapshot work;
			snapshot.event_work_snapshot_valid = query.event->GetPipelineWorkSnapshot(work);
			if (snapshot.event_work_snapshot_valid) {
				snapshot.pipeline_id = work.pipeline_id;
				snapshot.parallelism_valid = work.parallelism_valid;
				snapshot.preferred_parallelism = work.preferred_parallelism;
				snapshot.event_remaining_work_units = work.remaining_chunks_equiv;
			}
		}
		snapshot.producer_present = query.producer != nullptr;
		if (query.producer) {
			snapshot.producer_queued_tasks = db.GetScheduler().GetTaskCountForProducer(*query.producer);
		}
		snapshot.model_valid = query.model.valid;
		snapshot.model_error = query.model.error;
		snapshot.demand_cap = query.model.demand_cap;
		snapshot.remaining_work_units = query.model.remaining_work_units;
		snapshot.selected_throughput = query.model.throughput;
		snapshot.throughput_is_live = query.model.throughput_is_live;
		snapshot.mandatory_workers = query.mandatory_workers;
		snapshot.optional_workers = query.optional_workers;
		snapshot.residual_workers = query.residual_workers;
		snapshot.planned_workers = PlannedWorkers(query);
		snapshot.assigned_workers = query.assigned_workers;
		snapshot.mandatory_gain_count = query.model.mandatory_gain.size();
		snapshot.optional_gain_count = query.model.optional_gain.size();
		if (!query.model.mandatory_cost.empty()) {
			snapshot.mandatory_cost_zero_workers = query.model.mandatory_cost[0];
		}
		if (query.model.mandatory_cost.size() > 1) {
			snapshot.mandatory_cost_one_worker = query.model.mandatory_cost[1];
		}
		if (!query.model.mandatory_gain.empty()) {
			snapshot.first_mandatory_gain = query.model.mandatory_gain[0];
		}
		if (!query.model.optional_gain.empty()) {
			snapshot.first_optional_gain = query.model.optional_gain[0];
		}
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
