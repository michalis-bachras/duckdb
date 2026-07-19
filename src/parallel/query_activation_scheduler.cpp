//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_activation_scheduler.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_activation_scheduler.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include "duckdb/parallel/query_stride_scheduler.hpp"
#include "duckdb/main/client_config.hpp"

#include <chrono>

namespace duckdb {

namespace {

struct QueryActivationDebugStore {
	mutex lock;
	vector<QueryActivationEventSnapshot> events;
};

static QueryActivationDebugStore &GetDebugStore() {
	static QueryActivationDebugStore store;
	return store;
}

} // namespace

QueryActivationScheduler::QueryActivationScheduler(Executor &executor_p, QueryRequestMetadata metadata_p,
                                                   bool debug_enabled_p)
    : executor(executor_p), metadata(std::move(metadata_p)), debug_enabled(debug_enabled_p),
      scheduler_policy(DatabaseInstance::GetDatabase(executor.context).GetQuerySchedulerPolicy()) {
	auto &db = DatabaseInstance::GetDatabase(executor.context);
	if (scheduler_policy == QuerySchedulerPolicy::SLA) {
		db.GetQuerySLAScheduler().RegisterQuery(metadata, executor.GetToken(), debug_enabled);
	} else if (scheduler_policy == QuerySchedulerPolicy::STRIDE) {
		db.GetQueryStrideScheduler().RegisterQuery(metadata, executor.GetToken(),
		                                             ClientConfig::GetConfig(executor.context).stride_user_priority);
	}
}

QueryActivationScheduler::~QueryActivationScheduler() {
	auto &db = DatabaseInstance::GetDatabase(executor.context);
	if (scheduler_policy == QuerySchedulerPolicy::SLA) {
		db.GetQuerySLAScheduler().UnregisterQuery(metadata.db_query_id);
	} else if (scheduler_policy == QuerySchedulerPolicy::STRIDE) {
		db.GetQueryStrideScheduler().UnregisterQuery(metadata.db_query_id);
	}
}

uint64_t QueryActivationScheduler::TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

const char *QueryActivationScheduler::EventKindName(QueryActivationEventKind kind) {
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

bool QueryActivationScheduler::IsPolicyEvent(QueryActivationEventKind kind) {
	return kind == QueryActivationEventKind::PIPELINE;
}

bool QueryActivationScheduler::IsContinuationEvent(QueryActivationEventKind kind) {
	return kind == QueryActivationEventKind::INITIALIZE || kind == QueryActivationEventKind::PREPARE_FINISH ||
	       kind == QueryActivationEventKind::FINISH || kind == QueryActivationEventKind::COMPLETE ||
	       kind == QueryActivationEventKind::INTERNAL;
}

void QueryActivationScheduler::LogEventLocked(const Event &event, const string &state) {
	if (!debug_enabled) {
		return;
	}
	QueryActivationEventSnapshot snapshot;
	snapshot.db_query_id = metadata.db_query_id;
	snapshot.request_id = metadata.request_id;
	snapshot.template_id = metadata.template_id;
	snapshot.scale_factor = metadata.scale_factor;
	snapshot.activation_group_id = event.GetQueryActivationGroupId();
	snapshot.pipeline_id = event.GetQueryActivationPipelineId();
	snapshot.event_kind = EventKindName(event.GetQueryActivationKind());
	snapshot.event_state = state;
	snapshot.timestamp_ns = TimestampNs();

	auto &store = GetDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.push_back(std::move(snapshot));
}

bool QueryActivationScheduler::CanScheduleNewGroupLocked() const {
	return active_event_count == 0 && active_group_id == 0;
}

void QueryActivationScheduler::MarkScheduledLocked(Event &event) {
	auto group_id = event.GetQueryActivationGroupId();
	if (active_group_id == 0) {
		active_group_id = group_id;
	}
	D_ASSERT(active_group_id == group_id);
	active_event = event.shared_from_this();
	active_event_count++;
}

void QueryActivationScheduler::MarkFinishedLocked(Event &event) {
	if (event.GetQueryActivationGroupId() != active_group_id) {
		return;
	}
	if (active_event_count > 0) {
		active_event_count--;
	}
	auto current_event = active_event.lock();
	if (current_event && current_event.get() == &event) {
		active_event.reset();
	}
}

void QueryActivationScheduler::DeferEventLocked(shared_ptr<Event> event) {
	auto group_id = event->GetQueryActivationGroupId();
	auto entry = deferred_events_by_group.find(group_id);
	if (entry == deferred_events_by_group.end()) {
		deferred_group_order.push_back(group_id);
		entry = deferred_events_by_group.emplace(group_id, std::deque<shared_ptr<Event>>()).first;
	}
	entry->second.push_back(std::move(event));
}

shared_ptr<Event> QueryActivationScheduler::PopDeferredEventForActiveGroupLocked() {
	if (active_group_id == 0) {
		return nullptr;
	}
	auto entry = deferred_events_by_group.find(active_group_id);
	if (entry == deferred_events_by_group.end() || entry->second.empty()) {
		return nullptr;
	}
	auto result = std::move(entry->second.front());
	entry->second.pop_front();
	if (entry->second.empty()) {
		deferred_events_by_group.erase(entry);
	}
	return result;
}

shared_ptr<Event> QueryActivationScheduler::PopFirstDeferredEventLocked() {
	while (!deferred_group_order.empty()) {
		auto group_id = deferred_group_order.front();
		deferred_group_order.pop_front();

		auto entry = deferred_events_by_group.find(group_id);
		if (entry == deferred_events_by_group.end() || entry->second.empty()) {
			continue;
		}
		auto result = std::move(entry->second.front());
		entry->second.pop_front();
		if (entry->second.empty()) {
			deferred_events_by_group.erase(entry);
		}
		return result;
	}
	return nullptr;
}

shared_ptr<Event> QueryActivationScheduler::PopReadyEventLocked() {
	if (active_event_count != 0) {
		return nullptr;
	}
	shared_ptr<Event> next_event;
	if (active_group_id != 0) {
		next_event = PopDeferredEventForActiveGroupLocked();
		if (!next_event) {
			active_group_id = 0;
		}
	}
	if (!next_event && CanScheduleNewGroupLocked()) {
		next_event = PopFirstDeferredEventLocked();
	}
	if (!next_event) {
		return nullptr;
	}
	D_ASSERT(IsPolicyEvent(next_event->GetQueryActivationKind()) ||
	         IsContinuationEvent(next_event->GetQueryActivationKind()));
	MarkScheduledLocked(*next_event);
	LogEventLocked(*next_event, "scheduled");
	return next_event;
}

shared_ptr<Event> QueryActivationScheduler::OnEventReady(shared_ptr<Event> event) {
	if (!event || !event->HasQueryActivationInfo()) {
		return std::move(event);
	}
	lock_guard<mutex> guard(scheduler_lock);
	LogEventLocked(*event, "ready");

	if (!CanScheduleNewGroupLocked() && event->GetQueryActivationGroupId() != active_group_id) {
		LogEventLocked(*event, "deferred");
		DeferEventLocked(std::move(event));
		return nullptr;
	}

	if (active_event_count > 0) {
		LogEventLocked(*event, "deferred");
		DeferEventLocked(std::move(event));
		return nullptr;
	}

	if (active_group_id != 0 && event->GetQueryActivationGroupId() != active_group_id) {
		LogEventLocked(*event, "deferred");
		DeferEventLocked(std::move(event));
		return nullptr;
	}

	MarkScheduledLocked(*event);
	LogEventLocked(*event, "scheduled");
	return std::move(event);
}

void QueryActivationScheduler::OnEventTasksScheduled(Event &event) {
	idx_t remaining_suffix_stages;
	{
		lock_guard<mutex> guard(scheduler_lock);
		auto pipeline_count = executor.GetPhysicalPipelineCount();
		remaining_suffix_stages = completed_pipeline_count < pipeline_count ? pipeline_count - completed_pipeline_count : 1;
	}
	auto &db = DatabaseInstance::GetDatabase(executor.context);
	if (scheduler_policy == QuerySchedulerPolicy::SLA) {
		db.GetQuerySLAScheduler().OnEventScheduled(metadata.db_query_id, event.shared_from_this(), remaining_suffix_stages);
	} else if (scheduler_policy == QuerySchedulerPolicy::STRIDE) {
		db.GetQueryStrideScheduler().OnEventScheduled(metadata.db_query_id, event);
	}
}

shared_ptr<Event> QueryActivationScheduler::OnEventFinished(Event &event) {
	if (!event.HasQueryActivationInfo()) {
		return nullptr;
	}
	auto &db = DatabaseInstance::GetDatabase(executor.context);
	if (scheduler_policy == QuerySchedulerPolicy::SLA) {
		db.GetQuerySLAScheduler().OnEventFinished(metadata.db_query_id, event);
	} else if (scheduler_policy == QuerySchedulerPolicy::STRIDE) {
		db.GetQueryStrideScheduler().OnEventFinished(metadata.db_query_id, event);
	}
	lock_guard<mutex> guard(scheduler_lock);
	LogEventLocked(event, "finished");

	MarkFinishedLocked(event);
	if (event.GetQueryActivationKind() == QueryActivationEventKind::PIPELINE) {
		completed_pipeline_count++;
	}
	return PopReadyEventLocked();
}

vector<QueryActivationEventSnapshot> QueryActivationScheduler::GetDebugSnapshot() {
	auto &store = GetDebugStore();
	lock_guard<mutex> guard(store.lock);
	return store.events;
}

void QueryActivationScheduler::ClearDebugSnapshot() {
	auto &store = GetDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.clear();
}

bool QueryActivationScheduler::GetActivePipelineWorkSnapshot(PipelineWorkSnapshot &snapshot) {
	lock_guard<mutex> guard(scheduler_lock);
	auto event = active_event.lock();
	if (!event) {
		return false;
	}
	return event->GetPipelineWorkSnapshot(snapshot);
}

} // namespace duckdb
