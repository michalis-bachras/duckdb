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
	QueryAdmissionHandle(QueryAdmissionController &controller, idx_t slot_id);
	~QueryAdmissionHandle();

	QueryAdmissionHandle(const QueryAdmissionHandle &) = delete;
	QueryAdmissionHandle &operator=(const QueryAdmissionHandle &) = delete;

	QueryAdmissionHandle(QueryAdmissionHandle &&other) noexcept;
	QueryAdmissionHandle &operator=(QueryAdmissionHandle &&other) noexcept;

	idx_t SlotId() const {
		return slot_id;
	}

private:
	QueryAdmissionController *controller;
	idx_t slot_id;
};

class QueryAdmissionController {
public:
	explicit QueryAdmissionController(DatabaseInstance &db);

	unique_ptr<QueryAdmissionHandle> Acquire(ClientContext &context, const QueryRequestMetadata &metadata,
	                                        idx_t max_active);
	void Release(idx_t slot_id);

	vector<QueryAdmissionSnapshot> GetSnapshot() const;
	static vector<QueryAdmissionEventSnapshot> GetEventSnapshot();

private:
	struct Waiter {
		explicit Waiter(const QueryRequestMetadata &metadata_p, idx_t max_active_p, idx_t ticket_p,
		                uint64_t queued_ns_p, bool debug_enabled_p)
		    : metadata(metadata_p), max_active(max_active_p), ticket(ticket_p), queued_ns(queued_ns_p),
		      debug_enabled(debug_enabled_p) {
		}

		QueryRequestMetadata metadata;
		idx_t max_active;
		idx_t ticket;
		idx_t slot_id = 0;
		uint64_t queued_ns;
		uint64_t admitted_ns = 0;
		bool admitted = false;
		bool debug_enabled;
		std::condition_variable cv;
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
	bool CanAdmit(const Waiter &waiter) const;
	void AdmitWaiter(Waiter &waiter);
	void LogEventLocked(const QueryRequestMetadata &metadata, const string &event_state, idx_t ticket, idx_t slot_id,
	                    idx_t max_active, uint64_t queued_ns, uint64_t admitted_ns, uint64_t released_ns,
	                    bool debug_enabled) const;

private:
	DatabaseInstance &db;
	mutable mutex admission_lock;
	std::deque<Waiter *> waiters;
	std::unordered_map<idx_t, ActiveEntry> active;
	idx_t next_ticket = 1;
	idx_t next_slot_id = 1;
};

} // namespace duckdb
