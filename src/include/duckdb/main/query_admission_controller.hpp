//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_admission_controller.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/pending_query_notification.hpp"

#include <condition_variable>
#include <deque>
#include <unordered_map>

namespace duckdb {

class ClientContext;
class DatabaseInstance;

struct QueryAdmissionSnapshot {
	string state;
	idx_t slot_id = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double sla_tag = 0;
	uint64_t deadline_ns = 0;
	uint64_t queued_ns = 0;
	uint64_t admitted_ns = 0;
};

struct QueryAdmissionEventSnapshot {
	string event_state;
	uint64_t timestamp_ns = 0;
	idx_t ticket = 0;
	idx_t slot_id = 0;
	idx_t max_active = 0;
	idx_t active_count = 0;
	idx_t waiting_count = 0;
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double sla_tag = 0;
	uint64_t deadline_ns = 0;
	uint64_t queued_ns = 0;
	uint64_t admitted_ns = 0;
	uint64_t released_ns = 0;
};

class QueryAdmissionController;

class QueryAdmissionHandle {
public:
	QueryAdmissionHandle(QueryAdmissionController &controller, const QueryRequestMetadata &metadata, idx_t max_active,
	                     idx_t ticket, uint64_t queued_ns, bool debug_enabled,
	                     shared_ptr<PendingQueryNotification> notification);
	~QueryAdmissionHandle();

	QueryAdmissionHandle(const QueryAdmissionHandle &) = delete;
	QueryAdmissionHandle &operator=(const QueryAdmissionHandle &) = delete;

	QueryAdmissionHandle(QueryAdmissionHandle &&other) = delete;
	QueryAdmissionHandle &operator=(QueryAdmissionHandle &&other) = delete;

	idx_t SlotId() const {
		return slot_id;
	}
	bool IsAdmitted() const {
		return admitted;
	}
	void Wait();

private:
	QueryAdmissionController *controller;
	QueryRequestMetadata metadata;
	idx_t max_active;
	idx_t ticket;
	idx_t slot_id = 0;
	uint64_t queued_ns;
	uint64_t admitted_ns = 0;
	bool admitted = false;
	bool cancelled = false;
	bool debug_enabled;
	shared_ptr<PendingQueryNotification> notification;
	std::condition_variable cv;

	friend class QueryAdmissionController;
};

class QueryAdmissionController {
	friend class QueryAdmissionHandle;

public:
	explicit QueryAdmissionController(DatabaseInstance &db);

	unique_ptr<QueryAdmissionHandle> EnqueueOrAcquire(ClientContext &context, const QueryRequestMetadata &metadata,
	                                                  idx_t max_active,
	                                                  shared_ptr<PendingQueryNotification> notification = nullptr);
	unique_ptr<QueryAdmissionHandle> Acquire(ClientContext &context, const QueryRequestMetadata &metadata,
	                                        idx_t max_active,
	                                        shared_ptr<PendingQueryNotification> notification = nullptr);

	vector<QueryAdmissionSnapshot> GetSnapshot() const;
	static vector<QueryAdmissionEventSnapshot> GetEventSnapshot();

private:
	struct AdmissionNotification {
		shared_ptr<PendingQueryNotification> notification;
		PendingQueryEventType type;
	};

	struct ActiveEntry {
		QueryRequestMetadata metadata;
		idx_t slot_id;
		idx_t ticket;
		idx_t max_active;
		uint64_t queued_ns;
		uint64_t admitted_ns;
		bool debug_enabled;
	};

private:
	static uint64_t TimestampNs();
	void AdmitRequest(QueryAdmissionHandle &request, vector<AdmissionNotification> &notifications);
	void CancelOrRelease(QueryAdmissionHandle &request);
	void ReleaseLocked(idx_t slot_id, vector<AdmissionNotification> &notifications);
	void TryAdmitWaiters(vector<AdmissionNotification> &notifications);
	void Wait(QueryAdmissionHandle &request);
	static void PublishNotifications(vector<AdmissionNotification> &notifications);
	void LogEventLocked(const QueryRequestMetadata &metadata, const string &event_state, idx_t ticket, idx_t slot_id,
	                    idx_t max_active, uint64_t queued_ns, uint64_t admitted_ns, uint64_t released_ns,
	                    bool debug_enabled) const;

private:
	DatabaseInstance &db;
	mutable mutex admission_lock;
	std::deque<QueryAdmissionHandle *> waiters;
	std::unordered_map<idx_t, ActiveEntry> active;
	idx_t next_ticket = 1;
	idx_t next_slot_id = 1;
};

} // namespace duckdb
