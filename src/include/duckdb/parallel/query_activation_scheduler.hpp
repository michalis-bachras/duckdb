//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_activation_scheduler.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/parallel/event.hpp"

#include <deque>
#include <unordered_map>

namespace duckdb {

class Executor;

struct QueryActivationEventSnapshot {
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t activation_group_id = 0;
	idx_t pipeline_id = 0;
	string event_kind;
	string event_state;
	uint64_t timestamp_ns = 0;
};

class QueryActivationScheduler {
public:
	QueryActivationScheduler(Executor &executor, QueryRequestMetadata metadata, bool debug_enabled);
	~QueryActivationScheduler();

	shared_ptr<Event> OnEventReady(shared_ptr<Event> event);
	shared_ptr<Event> OnEventFinished(Event &event);

	static vector<QueryActivationEventSnapshot> GetDebugSnapshot();
	static void ClearDebugSnapshot();

private:
	static uint64_t TimestampNs();
	static const char *EventKindName(QueryActivationEventKind kind);
	static bool IsPolicyEvent(QueryActivationEventKind kind);
	static bool IsContinuationEvent(QueryActivationEventKind kind);

	void LogEventLocked(const Event &event, const string &state);
	void DeferEventLocked(shared_ptr<Event> event);
	shared_ptr<Event> PopReadyEventLocked();
	shared_ptr<Event> PopFirstDeferredEventLocked();
	shared_ptr<Event> PopDeferredEventForActiveGroupLocked();
	bool CanScheduleNewGroupLocked() const;
	void MarkScheduledLocked(Event &event);
	void MarkFinishedLocked(Event &event);

private:
	QueryRequestMetadata metadata;
	bool debug_enabled;
	mutex scheduler_lock;
	idx_t active_group_id = 0;
	idx_t active_event_count = 0;
	std::deque<idx_t> deferred_group_order;
	std::unordered_map<idx_t, std::deque<shared_ptr<Event>>> deferred_events_by_group;
};

} // namespace duckdb
