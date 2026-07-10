//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_admission_controller.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/query_admission_controller.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>

namespace duckdb {

namespace {

struct QueryAdmissionDebugStore {
	mutex lock;
	vector<QueryAdmissionEventSnapshot> events;
};

static QueryAdmissionDebugStore &GetAdmissionDebugStore() {
	static QueryAdmissionDebugStore store;
	return store;
}

} // namespace

QueryAdmissionHandle::QueryAdmissionHandle(QueryAdmissionController &controller_p, const QueryRequestMetadata &metadata_p,
                                           idx_t max_active_p, idx_t ticket_p, uint64_t queued_ns_p,
                                           bool debug_enabled_p)
    : controller(&controller_p), metadata(metadata_p), max_active(max_active_p), ticket(ticket_p), queued_ns(queued_ns_p),
      debug_enabled(debug_enabled_p) {
}

QueryAdmissionHandle::~QueryAdmissionHandle() {
	if (controller) {
		controller->CancelOrRelease(*this);
	}
}

void QueryAdmissionHandle::Wait() {
	if (!controller) {
		return;
	}
	controller->Wait(*this);
}

QueryAdmissionController::QueryAdmissionController(DatabaseInstance &db_p) : db(db_p) {
}

uint64_t QueryAdmissionController::TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

void QueryAdmissionController::AdmitRequest(QueryAdmissionHandle &request) {
	auto slot_id = next_slot_id++;
	auto admitted_ns = TimestampNs();
	active[slot_id] = ActiveEntry {request.metadata, slot_id, request.ticket, request.max_active, request.queued_ns,
	                               admitted_ns, request.debug_enabled};
	request.slot_id = slot_id;
	request.admitted_ns = admitted_ns;
	request.admitted = true;
	LogEventLocked(request.metadata, "admitted", request.ticket, request.slot_id, request.max_active, request.queued_ns,
	               request.admitted_ns, 0, request.debug_enabled);
	request.cv.notify_all();
}

void QueryAdmissionController::LogEventLocked(const QueryRequestMetadata &metadata, const string &event_state,
                                              idx_t ticket, idx_t slot_id, idx_t max_active, uint64_t queued_ns,
                                              uint64_t admitted_ns, uint64_t released_ns,
                                              bool debug_enabled) const {
	if (!debug_enabled) {
		return;
	}
	QueryAdmissionEventSnapshot snapshot;
	snapshot.event_state = event_state;
	snapshot.timestamp_ns = TimestampNs();
	snapshot.ticket = ticket;
	snapshot.slot_id = slot_id;
	snapshot.max_active = max_active;
	snapshot.active_count = active.size();
	snapshot.waiting_count = waiters.size();
	snapshot.db_query_id = metadata.db_query_id;
	snapshot.request_id = metadata.request_id;
	snapshot.template_id = metadata.template_id;
	snapshot.scale_factor = metadata.scale_factor;
	snapshot.sla_tag = metadata.sla_tag;
	snapshot.deadline_ns = metadata.deadline_ns;
	snapshot.queued_ns = queued_ns;
	snapshot.admitted_ns = admitted_ns;
	snapshot.released_ns = released_ns;

	auto &store = GetAdmissionDebugStore();
	lock_guard<mutex> guard(store.lock);
	store.events.push_back(std::move(snapshot));
}

unique_ptr<QueryAdmissionHandle> QueryAdmissionController::EnqueueOrAcquire(ClientContext &context,
                                                                            const QueryRequestMetadata &metadata,
                                                                            idx_t max_active) {
	if (!metadata.valid || max_active == 0) {
		return nullptr;
	}

	unique_lock<mutex> guard(admission_lock);
	auto debug_enabled = context.config.query_activation_debug_enabled;
	auto request = make_uniq<QueryAdmissionHandle>(*this, metadata, max_active, next_ticket++, TimestampNs(), debug_enabled);
	if (waiters.empty() && active.size() < max_active) {
		AdmitRequest(*request);
		return request;
	}

	waiters.push_back(request.get());
	LogEventLocked(request->metadata, "queued", request->ticket, 0, request->max_active, request->queued_ns, 0, 0,
	               request->debug_enabled);
	return request;
}

unique_ptr<QueryAdmissionHandle> QueryAdmissionController::Acquire(ClientContext &context,
                                                                   const QueryRequestMetadata &metadata,
                                                                   idx_t max_active) {
	auto request = EnqueueOrAcquire(context, metadata, max_active);
	if (request) {
		request->Wait();
	}
	return request;
}

void QueryAdmissionController::ReleaseLocked(idx_t slot_id) {
	auto entry = active.find(slot_id);
	if (entry == active.end()) {
		return;
	}
	auto active_entry = entry->second;
	active.erase(entry);
	auto released_ns = TimestampNs();
	LogEventLocked(active_entry.metadata, "released", active_entry.ticket, active_entry.slot_id,
	               active_entry.max_active, active_entry.queued_ns, active_entry.admitted_ns, released_ns,
	               active_entry.debug_enabled);
	TryAdmitWaiters();
}

void QueryAdmissionController::TryAdmitWaiters() {
	while (!waiters.empty()) {
		auto *request = waiters.front();
		if (active.size() >= request->max_active) {
			break;
		}
		waiters.pop_front();
		AdmitRequest(*request);
	}
}

void QueryAdmissionController::CancelOrRelease(QueryAdmissionHandle &request) {
	unique_lock<mutex> guard(admission_lock);
	if (request.cancelled) {
		return;
	}
	request.cancelled = true;
	if (request.admitted) {
		auto slot_id = request.slot_id;
		request.controller = nullptr;
		request.cv.notify_all();
		ReleaseLocked(slot_id);
		return;
	}
	for (auto entry = waiters.begin(); entry != waiters.end(); entry++) {
		if (*entry == &request) {
			waiters.erase(entry);
			LogEventLocked(request.metadata, "cancelled", request.ticket, 0, request.max_active, request.queued_ns, 0, 0,
			               request.debug_enabled);
			break;
		}
	}
	request.controller = nullptr;
	request.cv.notify_all();
	TryAdmitWaiters();
}

void QueryAdmissionController::Wait(QueryAdmissionHandle &request) {
	unique_lock<mutex> guard(admission_lock);
	while (!request.admitted && !request.cancelled) {
		request.cv.wait(guard);
	}
}

vector<QueryAdmissionSnapshot> QueryAdmissionController::GetSnapshot() const {
	lock_guard<mutex> guard(admission_lock);
	vector<QueryAdmissionSnapshot> result;
	result.reserve(active.size() + waiters.size());
	for (auto &entry : active) {
		const auto &active_entry = entry.second;
		const auto &metadata = active_entry.metadata;
		QueryAdmissionSnapshot snapshot;
		snapshot.state = "active";
		snapshot.slot_id = active_entry.slot_id;
		snapshot.db_query_id = metadata.db_query_id;
		snapshot.request_id = metadata.request_id;
		snapshot.template_id = metadata.template_id;
		snapshot.scale_factor = metadata.scale_factor;
		snapshot.sla_tag = metadata.sla_tag;
		snapshot.deadline_ns = metadata.deadline_ns;
		snapshot.queued_ns = active_entry.queued_ns;
		snapshot.admitted_ns = active_entry.admitted_ns;
		result.push_back(std::move(snapshot));
	}
	for (auto *request : waiters) {
		const auto &metadata = request->metadata;
		QueryAdmissionSnapshot snapshot;
		snapshot.state = "waiting";
		snapshot.slot_id = 0;
		snapshot.db_query_id = metadata.db_query_id;
		snapshot.request_id = metadata.request_id;
		snapshot.template_id = metadata.template_id;
		snapshot.scale_factor = metadata.scale_factor;
		snapshot.sla_tag = metadata.sla_tag;
		snapshot.deadline_ns = metadata.deadline_ns;
		snapshot.queued_ns = request->queued_ns;
		snapshot.admitted_ns = 0;
		result.push_back(std::move(snapshot));
	}
	return result;
}

vector<QueryAdmissionEventSnapshot> QueryAdmissionController::GetEventSnapshot() {
	auto &store = GetAdmissionDebugStore();
	lock_guard<mutex> guard(store.lock);
	return store.events;
}

} // namespace duckdb
