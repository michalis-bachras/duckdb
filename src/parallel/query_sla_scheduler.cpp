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
#include "duckdb/parallel/task_scheduler.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <mutex>
#include <queue>
#include <ctime>
#include <unordered_map>

namespace duckdb {

struct QuerySLAModel {
	bool valid = false;
	string error;
	uint64_t db_query_id = 0;
	uint64_t generation = 0;
	idx_t pipeline_id = 0;
	QueryActivationEventKind event_kind = QueryActivationEventKind::UNREGISTERED;
	idx_t demand_cap = 0;
	idx_t remaining_work_units = 0;
	double throughput = 0;
	bool throughput_is_live = false;
	vector<double> mandatory_cost;
	vector<double> optional_risk;
	vector<double> mandatory_gain;
	vector<double> optional_gain;
};

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

static double SuffixMeanNs(const DownstreamSuffixEstimate &suffix) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		result += bucket.probability * (bucket.lower_ns + bucket.upper_ns) / 2.0;
	}
	return result;
}

static double SuffixCDF(const DownstreamSuffixEstimate &suffix, double value_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		if (bucket.lower_ns == bucket.upper_ns) {
			result += value_ns >= bucket.upper_ns ? bucket.probability : 0.0;
		} else if (value_ns >= bucket.upper_ns) {
			result += bucket.probability;
		} else if (value_ns > bucket.lower_ns) {
			result += bucket.probability * (value_ns - bucket.lower_ns) / (bucket.upper_ns - bucket.lower_ns);
		}
	}
	return result;
}

static double SuffixQuantileNs(const DownstreamSuffixEstimate &suffix, double quantile) {
	if (!suffix.valid || suffix.bucket_count == 0) {
		return 0;
	}
	double lower = suffix.buckets[0].lower_ns;
	double upper = suffix.buckets[0].upper_ns;
	for (idx_t i = 1; i < suffix.bucket_count; i++) {
		lower = MinValue<double>(lower, suffix.buckets[i].lower_ns);
		upper = MaxValue<double>(upper, suffix.buckets[i].upper_ns);
	}
	for (idx_t iteration = 0; iteration < 48; iteration++) {
		auto midpoint = (lower + upper) / 2.0;
		if (SuffixCDF(suffix, midpoint) >= quantile) {
			upper = midpoint;
		} else {
			lower = midpoint;
		}
	}
	return upper;
}

static double CurrentPipelineFinishNs(const PipelineWorkSnapshot &work, QueryActivationEventKind event_kind,
	                                  idx_t workers, uint64_t now_ns, uint64_t epoch_ns) {
	auto lifecycle_tail_ns = work.lifecycle_tail_estimate.valid ? work.lifecycle_tail_estimate.p90_ns : 0.0;
	if (event_kind != QueryActivationEventKind::PIPELINE || work.remaining_chunks_equiv == 0) {
		return static_cast<double>(now_ns) + (workers == 0 ? static_cast<double>(epoch_ns) : 0.0) +
		       lifecycle_tail_ns;
	}
	if (!work.selected_throughput_valid || work.selected_single_worker_chunks_per_s <= 0) {
		return 0;
	}
	auto epoch_seconds = static_cast<double>(epoch_ns) / 1000000000.0;
	auto progress = static_cast<double>(workers) * work.selected_single_worker_chunks_per_s * epoch_seconds;
	if (workers > 0 && progress >= static_cast<double>(work.remaining_chunks_equiv)) {
		auto active_ns = static_cast<double>(work.remaining_chunks_equiv) * 1000000000.0 /
		                 (static_cast<double>(workers) * work.selected_single_worker_chunks_per_s);
		return static_cast<double>(now_ns) + active_ns + lifecycle_tail_ns;
	}
	auto remaining_after_epoch = MaxValue<double>(0.0, static_cast<double>(work.remaining_chunks_equiv) - progress);
	if (!work.continuation_estimate.valid || work.continuation_estimate.p90 <= 0) {
		return 0;
	}
	auto continuation_ns = work.continuation_estimate.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT
	                           ? remaining_after_epoch * work.continuation_estimate.p90
	                           : work.continuation_estimate.p90;
	return static_cast<double>(now_ns) + static_cast<double>(epoch_ns) + continuation_ns;
}

static double ExpectedTardiness(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		double value;
		if (bucket.lower_ns == bucket.upper_ns) {
			value = MaxValue<double>(0.0, bucket.lower_ns - downstream_budget_ns);
		} else if (downstream_budget_ns >= bucket.upper_ns) {
			value = 0;
		} else if (downstream_budget_ns > bucket.lower_ns) {
			auto late_width = bucket.upper_ns - downstream_budget_ns;
			value = (late_width * late_width) / (2.0 * (bucket.upper_ns - bucket.lower_ns));
		} else {
			value = (bucket.lower_ns + bucket.upper_ns) / 2.0 - downstream_budget_ns;
		}
		result += bucket.probability * value;
	}
	return result;
}

static double ExpectedFragility(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns,
	                            double epoch_ns) {
	double result = 0;
	for (idx_t i = 0; i < suffix.bucket_count; i++) {
		const auto &bucket = suffix.buckets[i];
		double value;
		if (bucket.lower_ns == bucket.upper_ns) {
			auto slack = MaxValue<double>(0.0, downstream_budget_ns - bucket.lower_ns);
			value = epoch_ns / (slack + epoch_ns);
		} else if (downstream_budget_ns <= bucket.lower_ns) {
			value = 1.0;
		} else if (downstream_budget_ns < bucket.upper_ns) {
			auto numerator = epoch_ns * std::log((downstream_budget_ns + epoch_ns - bucket.lower_ns) / epoch_ns) +
			                 bucket.upper_ns - downstream_budget_ns;
			value = numerator / (bucket.upper_ns - bucket.lower_ns);
		} else {
			auto numerator = epoch_ns *
			                 std::log((downstream_budget_ns + epoch_ns - bucket.lower_ns) /
			                          (downstream_budget_ns + epoch_ns - bucket.upper_ns));
			value = numerator / (bucket.upper_ns - bucket.lower_ns);
		}
		result += bucket.probability * value;
	}
	return result;
}

static DownstreamSuffixEstimate LifecycleSuffixPointMass() {
	DownstreamSuffixEstimate result;
	result.valid = true;
	result.bucket_count = 1;
	result.buckets[0].probability = 1.0;
	return result;
}

static QuerySLAModel BuildProvisionalQueryModel(const QueryCapture &capture, const PipelineWorkSnapshot &work) {
	QuerySLAModel model;
	model.db_query_id = capture.metadata.db_query_id;
	model.generation = capture.generation;
	model.pipeline_id = work.pipeline_id;
	model.event_kind = capture.event->GetQueryActivationKind();
	model.demand_cap = work.parallelism_valid ? work.preferred_parallelism : 0;
	model.remaining_work_units = work.remaining_chunks_equiv;
	model.throughput = work.selected_single_worker_chunks_per_s;
	model.throughput_is_live = work.selected_throughput_is_live;
	if (model.event_kind != QueryActivationEventKind::PIPELINE) {
		model.demand_cap = MinValue<idx_t>(idx_t(1), model.demand_cap);
	}
	if (model.demand_cap == 0) {
		model.valid = true;
		return model;
	}
	if (model.event_kind == QueryActivationEventKind::PIPELINE && work.remaining_chunks_equiv > 0 &&
	    !work.selected_throughput_valid) {
		model.error = "active data pipeline has no compatible historical or live throughput profile";
		return model;
	}
	if (model.event_kind == QueryActivationEventKind::PIPELINE && work.remaining_chunks_equiv > 0 &&
	    !work.continuation_estimate.valid) {
		model.error = "active data pipeline has no compatible continuation profile";
		return model;
	}
	model.valid = true;
	return model;
}

static QuerySLAModel BuildQueryModel(const QueryCapture &capture, const PipelineWorkSnapshot &work,
                                     const DownstreamSuffixEstimate &suffix, uint64_t now_ns, uint64_t epoch_ns) {
	auto model = BuildProvisionalQueryModel(capture, work);
	if (!model.valid || model.demand_cap == 0) {
		return model;
	}
	if (!suffix.valid || suffix.bucket_count == 0) {
		model.error = "active data pipeline has no trained downstream-suffix histogram";
		return model;
	}

	model.mandatory_cost.resize(model.demand_cap + 1);
	model.optional_risk.resize(model.demand_cap + 1);
	model.mandatory_gain.resize(model.demand_cap);
	model.optional_gain.resize(model.demand_cap);
	for (idx_t workers = 0; workers <= model.demand_cap; workers++) {
		auto current_finish_ns = CurrentPipelineFinishNs(work, model.event_kind, workers, now_ns, epoch_ns);
		if (current_finish_ns <= 0) {
			model.error = "active pipeline completion model is unavailable";
			return model;
		}
		auto downstream_budget_ns = static_cast<double>(capture.metadata.deadline_ns) - current_finish_ns;
		auto penalty_weight_per_ns = capture.metadata.sla_penalty_per_s / 1000000000.0;
		model.mandatory_cost[workers] = penalty_weight_per_ns * ExpectedTardiness(suffix, downstream_budget_ns);
		model.optional_risk[workers] =
		    capture.metadata.sla_penalty_per_s * ExpectedFragility(suffix, downstream_budget_ns,
		                                                              static_cast<double>(epoch_ns));
	}
	for (idx_t workers = 0; workers < model.demand_cap; workers++) {
		model.mandatory_gain[workers] = model.mandatory_cost[workers] - model.mandatory_cost[workers + 1];
		model.optional_gain[workers] = model.optional_risk[workers] - model.optional_risk[workers + 1];
	}
	model.valid = true;
	return model;
}

struct EpochHeapEntry {
	EpochHeapEntry(double gain_p, idx_t query_index_p, idx_t workers_p)
	    : gain(gain_p), query_index(query_index_p), workers(workers_p) {
	}

	double gain = 0;
	idx_t query_index = 0;
	idx_t workers = 0;

	bool operator<(const EpochHeapEntry &other) const {
		if (gain != other.gain) {
			return gain < other.gain;
		}
		return query_index > other.query_index;
	}
};

struct ResidualEpochHeapEntry {
	ResidualEpochHeapEntry(double mandatory_gain_p, double optional_gain_p, idx_t query_index_p, idx_t workers_p)
	    : mandatory_gain(mandatory_gain_p), optional_gain(optional_gain_p), query_index(query_index_p),
	      workers(workers_p) {
	}

	double mandatory_gain = 0;
	double optional_gain = 0;
	idx_t query_index = 0;
	idx_t workers = 0;

	bool operator<(const ResidualEpochHeapEntry &other) const {
		if (mandatory_gain != other.mandatory_gain) {
			return mandatory_gain < other.mandatory_gain;
		}
		if (optional_gain != other.optional_gain) {
			return optional_gain < other.optional_gain;
		}
		return query_index > other.query_index;
	}
};

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
};

} // namespace

struct QuerySLASchedulerState {
	static constexpr idx_t MAX_EPOCH_TRACE_ROWS = 250000;

	struct WorkerAssignment {
		uint64_t db_query_id = 0;
		uint64_t pipeline_generation = 0;
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
		idx_t assigned_workers = 0;
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
};

static idx_t PlannedWorkers(const QuerySLASchedulerState::QueryState &query) {
	return query.mandatory_workers + query.optional_workers;
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
		} else {
			auto gain = workers < query.model.optional_gain.size() ? query.model.optional_gain[workers] : 0;
			state.primary_optional.emplace(gain, query.metadata.db_query_id, query.dispatch_version, workers);
		}
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
	if (planned < query.model.mandatory_gain.size() && planned < query.model.optional_gain.size()) {
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
}

QuerySLAScheduler::~QuerySLAScheduler() {
}

bool QuerySLAScheduler::Enabled() const {
	return db.config.options.query_sla_scheduler_enabled;
}

idx_t QuerySLAScheduler::EpochMs() const {
	return db.config.options.query_sla_scheduler_epoch_ms;
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
	auto model = BuildProvisionalQueryModel(capture, work);
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
	entry->second.assigned_workers = 0;
	entry->second.pipeline_generation++;
	entry->second.continuation_pending = true;
	RefreshDispatchEntries(*state, entry->second);
}

void QuerySLAScheduler::RequestInitialEpoch() {
	if (Enabled()) {
		state->next_epoch_ns.store(0);
	}
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
	const auto collect_diagnostics = state->debug_trace_query_count.load() > 0;
	const auto thread_cpu_start_ns = collect_diagnostics ? SLAThreadCpuNs() : 0;
	const auto capture_wait_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	uint64_t capture_lock_wait_ns = 0;
	uint64_t capture_lock_hold_ns = 0;
	vector<QueryCapture> captures;
	{
		unique_lock<mutex> guard(state->lock);
		const auto capture_lock_acquired_ns = collect_diagnostics ? SLATimestampNs() : 0;
		capture_lock_wait_ns = SLADurationNs(capture_lock_acquired_ns, capture_wait_start_ns);
		captures.reserve(state->queries.size());
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
		if (captures[i].event->GetQueryActivationKind() != QueryActivationEventKind::PIPELINE ||
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
	vector<QuerySLAModel> models;
	models.reserve(captures.size());
	for (idx_t i = 0; i < captures.size(); i++) {
		DownstreamSuffixEstimate suffix;
		if (captures[i].event->GetQueryActivationKind() == QueryActivationEventKind::PIPELINE) {
			if (suffix_indexes[i] != DConstants::INVALID_INDEX && suffix_indexes[i] < suffix_estimates.size()) {
					suffix = suffix_estimates[suffix_indexes[i]];
				}
			} else {
				suffix = LifecycleSuffixPointMass();
			}
		resolved_suffixes[i] = suffix;
		models.push_back(BuildQueryModel(captures[i], work[i], suffix, now_ns, EpochMs() * 1000000ULL));
	}
	const auto model_build_ns = collect_diagnostics ? SLADurationNs(SLATimestampNs(), model_build_start_ns) : 0;

	const auto allocation_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	auto worker_budget = NumericCast<idx_t>(db.GetScheduler().NumberOfThreads());
	vector<idx_t> mandatory(models.size(), 0);
	vector<idx_t> optional(models.size(), 0);
	std::priority_queue<EpochHeapEntry> mandatory_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		if (models[i].valid && models[i].demand_cap > 0 && !models[i].mandatory_gain.empty()) {
			mandatory_heap.push({models[i].mandatory_gain[0], i, 0});
		}
	}
	while (worker_budget > 0 && !mandatory_heap.empty()) {
		auto top = mandatory_heap.top();
		mandatory_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		mandatory[top.query_index]++;
		worker_budget--;
		auto next = mandatory[top.query_index];
		if (next < models[top.query_index].demand_cap) {
			mandatory_heap.push({models[top.query_index].mandatory_gain[next], top.query_index, next});
		}
	}
	std::priority_queue<EpochHeapEntry> optional_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		auto workers = mandatory[i];
		if (models[i].valid && workers < models[i].demand_cap) {
			optional_heap.push({models[i].optional_gain[workers], i, workers});
		}
	}
	while (worker_budget > 0 && !optional_heap.empty()) {
		auto top = optional_heap.top();
		optional_heap.pop();
		if (top.gain <= 0) {
			break;
		}
		optional[top.query_index]++;
		worker_budget--;
		auto workers = mandatory[top.query_index] + optional[top.query_index];
		if (workers < models[top.query_index].demand_cap) {
			optional_heap.push({models[top.query_index].optional_gain[workers], top.query_index, workers});
		}
	}

	// SLA-only scheduling is work-conserving. Estimates can be flat or slightly non-monotonic at the boundary
	// between the current-epoch throughput model and the historical continuation model. Such an estimate must not
	// strand runnable work. After all positive mandatory and fragility gains are assigned, use the best remaining
	// modeled gain as a deterministic residual ordering and fill capacity up to each pipeline's useful demand cap.
	std::priority_queue<ResidualEpochHeapEntry> residual_heap;
	for (idx_t i = 0; i < models.size(); i++) {
		auto workers = mandatory[i] + optional[i];
		if (!models[i].valid || workers >= models[i].demand_cap ||
		    workers >= models[i].mandatory_gain.size() || workers >= models[i].optional_gain.size()) {
			continue;
		}
		residual_heap.push({models[i].mandatory_gain[workers], models[i].optional_gain[workers], i, workers});
	}
	while (worker_budget > 0 && !residual_heap.empty()) {
		auto top = residual_heap.top();
		residual_heap.pop();
		optional[top.query_index]++;
		worker_budget--;
		auto workers = mandatory[top.query_index] + optional[top.query_index];
		if (workers < models[top.query_index].demand_cap && workers < models[top.query_index].mandatory_gain.size() &&
		    workers < models[top.query_index].optional_gain.size()) {
			residual_heap.push({models[top.query_index].mandatory_gain[workers],
			                    models[top.query_index].optional_gain[workers], top.query_index, workers});
		}
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
			auto planned_workers = mandatory[i] + optional[i];
			if (models[i].valid && planned_workers < models[i].mandatory_cost.size()) {
				input.predicted_sla_cost = models[i].mandatory_cost[planned_workers];
			}
			trace_inputs.push_back(std::move(input));
		}
	}
	auto published_generation = state->epoch_generation.load() + 1;
	const auto publish_wait_start_ns = collect_diagnostics ? SLATimestampNs() : 0;
	unique_lock<mutex> guard(state->lock);
	const auto publish_lock_acquired_ns = collect_diagnostics ? SLATimestampNs() : 0;
	const auto publish_lock_wait_ns = SLADurationNs(publish_lock_acquired_ns, publish_wait_start_ns);
	for (idx_t i = 0; i < captures.size(); i++) {
		auto entry = state->queries.find(captures[i].metadata.db_query_id);
		if (entry == state->queries.end() || entry->second.pipeline_generation != captures[i].generation) {
			continue;
		}
		entry->second.model = std::move(models[i]);
		entry->second.mandatory_workers = mandatory[i];
		entry->second.optional_workers = optional[i];
		if (trace_input_indexes[i] != DConstants::INVALID_INDEX) {
			auto &trace_input = trace_inputs[trace_input_indexes[i]];
			trace_input.published = true;
			trace_input.assigned_workers = entry->second.assigned_workers;
		}
	}
	ResetDispatchEntries(*state);
	state->epoch_generation++;
	const auto publish_complete_ns = collect_diagnostics ? SLATimestampNs() : 0;
	const auto thread_cpu_complete_ns = collect_diagnostics ? SLAThreadCpuNs() : 0;
	const auto publish_lock_hold_ns =
	    collect_diagnostics ? SLADurationNs(publish_complete_ns, publish_lock_acquired_ns) : 0;
	guard.unlock();

	if (!collect_diagnostics || trace_inputs.empty()) {
		return;
	}
	const auto epoch_wall_ns = SLADurationNs(publish_complete_ns, now_ns);
	const auto epoch_thread_cpu_ns = SLADurationNs(thread_cpu_complete_ns, thread_cpu_start_ns);
	const auto trace_build_start_ns = SLATimestampNs();
	vector<QuerySLASchedulerEpochSnapshot> trace_rows;
	trace_rows.reserve(trace_inputs.size());
	for (const auto &input : trace_inputs) {
		if (!input.published) {
			continue;
		}
		auto i = input.capture_index;
		const auto &suffix = resolved_suffixes[i];
		QuerySLASchedulerEpochSnapshot trace;
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
		trace.suffix_mean_ns = SuffixMeanNs(suffix);
		trace.suffix_p90_ns = SuffixQuantileNs(suffix, 0.90);
		trace.mandatory_workers = mandatory[i];
		trace.optional_workers = optional[i];
		trace.planned_workers = mandatory[i] + optional[i];
		trace.assigned_workers = input.assigned_workers;
		if (input.model_valid && trace.planned_workers <= input.demand_cap) {
			trace.predicted_pipeline_finish_ns = CurrentPipelineFinishNs(
			    work[i], input.event_kind, trace.planned_workers, now_ns, EpochMs() * 1000000ULL);
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
	const auto trace_lock_hold_ns = SLADurationNs(SLATimestampNs(), trace_lock_acquired_ns);
	for (auto trace = state->epoch_trace.rbegin(); trace != state->epoch_trace.rend(); trace++) {
		if (trace->epoch_generation != published_generation) {
			break;
		}
		trace->trace_lock_hold_ns = trace_lock_hold_ns;
	}
}

QuerySLADequeueResult QuerySLAScheduler::TryDequeueTask(idx_t worker_id, shared_ptr<Task> &task) {
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
			assignment.db_query_id = query.metadata.db_query_id;
			assignment.pipeline_generation = query.pipeline_generation;
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
			query.optional_workers++;
			query.assigned_workers++;
			assignment.db_query_id = query.metadata.db_query_id;
			assignment.pipeline_generation = query.pipeline_generation;
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

uint64_t QuerySLAScheduler::EpochTraceDroppedCount() const {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	return state->epoch_trace_dropped_count;
}

void QuerySLAScheduler::ClearEpochTrace() {
	lock_guard<mutex> guard(state->epoch_trace_lock);
	state->epoch_trace.clear();
	state->epoch_trace_dropped_count = 0;
}

uint64_t QuerySLAScheduler::EpochRunCount() const {
	return state->epoch_run_count.load();
}

} // namespace duckdb
