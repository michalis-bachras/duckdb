//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_pipeline_debug.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_pipeline_debug.hpp"

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>

namespace duckdb {

namespace {

struct QueryPipelineDebugStore {
	mutex lock;
	vector<QueryPipelineDebugEventSnapshot> events;
};

static QueryPipelineDebugStore &GetPipelineDebugStore() {
	static QueryPipelineDebugStore store;
	return store;
}

static uint64_t TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static const char *EventKindName(QueryActivationEventKind kind) {
	switch (kind) {
	case QueryActivationEventKind::UNREGISTERED:
		return "unregistered";
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
		return "unknown";
	}
}

static void AppendOperatorType(string &result, const string &operator_type) {
	if (!result.empty()) {
		result += ">";
	}
	result += operator_type;
}

static void PopulatePipelineInfo(QueryPipelineDebugEventSnapshot &snapshot, Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	if (source) {
		snapshot.source_type = PhysicalOperatorToString(source->type);
		AppendOperatorType(snapshot.operator_type_sequence, snapshot.source_type);
	}
	for (auto &op_ref : pipeline.GetIntermediateOperators()) {
		auto &op = op_ref.get();
		AppendOperatorType(snapshot.operator_type_sequence, PhysicalOperatorToString(op.type));
	}
	auto sink = pipeline.GetSink();
	if (sink) {
		snapshot.sink_type = PhysicalOperatorToString(sink->type);
		AppendOperatorType(snapshot.operator_type_sequence, snapshot.sink_type);
	}
}

static bool PopulateMetadata(ClientContext &context, QueryPipelineDebugEventSnapshot &snapshot) {
	if (!QueryPipelineDebug::Enabled(context)) {
		return false;
	}
	QueryRequestMetadata metadata;
	if (!QueryRequestMetadataManager::TryGetActive(context, metadata)) {
		return false;
	}
	snapshot.db_query_id = metadata.db_query_id;
	snapshot.request_id = metadata.request_id;
	snapshot.template_id = metadata.template_id;
	snapshot.scale_factor = metadata.scale_factor;
	return true;
}

static void PopulateEventInfo(QueryPipelineDebugEventSnapshot &snapshot, Event &event) {
	if (event.HasQueryActivationInfo()) {
		snapshot.activation_group_id = event.GetQueryActivationGroupId();
		snapshot.pipeline_id = event.GetQueryActivationPipelineId();
		snapshot.event_kind = EventKindName(event.GetQueryActivationKind());
	} else {
		snapshot.event_kind = "unregistered";
	}
	snapshot.timestamp_ns = TimestampNs();
	snapshot.total_tasks = event.GetTotalTasks();
	snapshot.finished_tasks = event.GetFinishedTasks();
}

static void AppendDebugEvent(QueryPipelineDebugEventSnapshot snapshot) {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.push_back(std::move(snapshot));
}

} // namespace

bool QueryPipelineDebug::Enabled(ClientContext &context) {
	return ClientConfig::GetConfig(context).query_activation_debug_enabled;
}

void QueryPipelineDebug::RecordPipelineSchedule(Pipeline &pipeline, Event &event, bool parallel,
                                                const string &parallel_blocker, idx_t effective_max_threads,
                                                idx_t source_max_threads, idx_t scheduler_threads) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(pipeline.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = parallel ? "scheduled_parallel" : "scheduled_sequential";
	snapshot.parallel = parallel;
	snapshot.parallel_blocker = parallel_blocker;
	snapshot.effective_max_threads = effective_max_threads;
	snapshot.source_max_threads = source_max_threads;
	snapshot.scheduler_threads = scheduler_threads;
	if (!snapshot.pipeline_id) {
		snapshot.pipeline_id = pipeline.GetProfilerPipelineId();
	}
	PopulatePipelineInfo(snapshot, pipeline);
	AppendDebugEvent(std::move(snapshot));
}

void QueryPipelineDebug::RecordLifecycleSchedule(Pipeline &pipeline, Event &event, const string &state) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(pipeline.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = state;
	snapshot.parallel = false;
	snapshot.parallel_blocker = "lifecycle_single_task";
	snapshot.effective_max_threads = event.GetTotalTasks();
	snapshot.source_max_threads = 0;
	snapshot.scheduler_threads =
	    NumericCast<idx_t>(TaskScheduler::GetScheduler(pipeline.GetClientContext()).NumberOfThreads());
	if (!snapshot.pipeline_id) {
		snapshot.pipeline_id = pipeline.GetProfilerPipelineId();
	}
	PopulatePipelineInfo(snapshot, pipeline);
	AppendDebugEvent(std::move(snapshot));
}

void QueryPipelineDebug::RecordEventFinished(Event &event) {
	QueryPipelineDebugEventSnapshot snapshot;
	if (!PopulateMetadata(event.GetClientContext(), snapshot)) {
		return;
	}
	PopulateEventInfo(snapshot, event);
	snapshot.event_state = "finished";
	snapshot.parallel = false;
	AppendDebugEvent(std::move(snapshot));
}

vector<QueryPipelineDebugEventSnapshot> QueryPipelineDebug::GetDebugSnapshot() {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	return store.events;
}

void QueryPipelineDebug::ClearDebugSnapshot() {
	auto &store = GetPipelineDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.clear();
}

} // namespace duckdb
