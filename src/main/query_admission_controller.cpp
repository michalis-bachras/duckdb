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
#include <limits>

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
                                           bool debug_enabled_p, shared_ptr<PendingQueryNotification> notification_p)
    : controller(&controller_p), metadata(metadata_p), max_active(max_active_p), ticket(ticket_p), queued_ns(queued_ns_p),
      debug_enabled(debug_enabled_p), notification(std::move(notification_p)) {
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

QueryAdmissionController::QueryAdmissionController(DatabaseInstance &db_p)
    : db(db_p), policy(db_p.config.options.query_admission_policy) {
}

void QueryAdmissionController::SetPolicy(QueryAdmissionPolicy new_policy) {
	lock_guard<mutex> guard(admission_lock);
	if (new_policy != policy && (!active.empty() || !waiters.empty())) {
		throw InvalidInputException("query_admission_policy cannot change while requests are admitted or waiting");
	}
	policy = new_policy;
}

QueryAdmissionPolicy QueryAdmissionController::GetPolicy() const {
	lock_guard<mutex> guard(admission_lock);
	return policy;
}

QueryAdmissionController::WaiterKey QueryAdmissionController::WaitingKey(const QueryAdmissionHandle &request) const {
	uint64_t deadline = 0;
	if (policy == QueryAdmissionPolicy::EDF) {
		deadline = request.metadata.deadline_ns ? request.metadata.deadline_ns : std::numeric_limits<uint64_t>::max();
	} else if (policy == QueryAdmissionPolicy::SJF) {
		deadline = request.metadata.estimated_service_ns ? request.metadata.estimated_service_ns
		                                                 : std::numeric_limits<uint64_t>::max();
	}
	return {deadline, request.ticket};
}

uint64_t QueryAdmissionController::TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

void QueryAdmissionController::AdmitRequest(QueryAdmissionHandle &request,
                                            vector<AdmissionNotification> &notifications) {
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
	if (request.notification) {
		notifications.push_back({request.notification, PendingQueryEventType::ADMITTED});
	}
}

void QueryAdmissionController::PublishNotifications(vector<AdmissionNotification> &notifications) {
	for (auto &entry : notifications) {
		if (entry.notification) {
			entry.notification->Publish(entry.type);
		}
	}
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
                                                                            idx_t max_active,
                                                                            shared_ptr<PendingQueryNotification> notification) {
	if (!metadata.valid || max_active == 0) {
		return nullptr;
	}

	vector<AdmissionNotification> notifications;
	unique_ptr<QueryAdmissionHandle> request;
	{
		unique_lock<mutex> guard(admission_lock);
		auto debug_enabled = context.config.query_activation_debug_enabled;
		request = make_uniq<QueryAdmissionHandle>(*this, metadata, max_active, next_ticket++, TimestampNs(),
		                                           debug_enabled, std::move(notification));
		if (waiters.empty() && active.size() < max_active) {
			AdmitRequest(*request, notifications);
		} else {
			waiters.emplace(WaitingKey(*request), request.get());
			LogEventLocked(request->metadata, "queued", request->ticket, 0, request->max_active, request->queued_ns, 0,
			               0, request->debug_enabled);
			if (request->notification) {
				notifications.push_back({request->notification, PendingQueryEventType::ADMISSION_QUEUED});
			}
			// A new EDF head may be eligible even when the previous head's local cap blocked admission.
			TryAdmitWaiters(notifications);
		}
	}
	PublishNotifications(notifications);
	return request;
}

unique_ptr<QueryAdmissionHandle> QueryAdmissionController::Acquire(ClientContext &context,
                                                                   const QueryRequestMetadata &metadata,
                                                                   idx_t max_active,
                                                                   shared_ptr<PendingQueryNotification> notification) {
	auto request = EnqueueOrAcquire(context, metadata, max_active, std::move(notification));
	if (request) {
		request->Wait();
	}
	return request;
}

void QueryAdmissionController::ReleaseLocked(idx_t slot_id, vector<AdmissionNotification> &notifications) {
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
	TryAdmitWaiters(notifications);
}

void QueryAdmissionController::TryAdmitWaiters(vector<AdmissionNotification> &notifications) {
	while (!waiters.empty()) {
		auto entry = waiters.begin();
		auto *request = entry->second;
		if (active.size() >= request->max_active) {
			break;
		}
		waiters.erase(entry);
		AdmitRequest(*request, notifications);
	}
}

void QueryAdmissionController::CancelOrRelease(QueryAdmissionHandle &request) {
	vector<AdmissionNotification> notifications;
	{
		unique_lock<mutex> guard(admission_lock);
		if (request.cancelled) {
			return;
		}
		request.cancelled = true;
		if (request.admitted) {
			auto slot_id = request.slot_id;
			request.controller = nullptr;
			request.cv.notify_all();
			if (request.notification) {
				notifications.push_back({request.notification, PendingQueryEventType::ADMISSION_RELEASED});
			}
			ReleaseLocked(slot_id, notifications);
		} else {
			auto entry = waiters.find(WaitingKey(request));
			if (entry != waiters.end()) {
				waiters.erase(entry);
				LogEventLocked(request.metadata, "cancelled", request.ticket, 0, request.max_active,
				               request.queued_ns, 0, 0, request.debug_enabled);
				if (request.notification) {
					notifications.push_back({request.notification, PendingQueryEventType::CANCELLED});
				}
			}
			request.controller = nullptr;
			request.cv.notify_all();
			TryAdmitWaiters(notifications);
		}
	}
	PublishNotifications(notifications);
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
	for (const auto &entry : waiters) {
		auto *request = entry.second;
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
