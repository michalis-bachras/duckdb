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

QueryAdmissionHandle::QueryAdmissionHandle(QueryAdmissionController &controller_p, idx_t slot_id_p)
    : controller(&controller_p), slot_id(slot_id_p) {
}

QueryAdmissionHandle::~QueryAdmissionHandle() {
	if (controller) {
		controller->Release(slot_id);
	}
}

QueryAdmissionHandle::QueryAdmissionHandle(QueryAdmissionHandle &&other) noexcept
    : controller(other.controller), slot_id(other.slot_id) {
	other.controller = nullptr;
	other.slot_id = 0;
}

QueryAdmissionHandle &QueryAdmissionHandle::operator=(QueryAdmissionHandle &&other) noexcept {
	if (this == &other) {
		return *this;
	}
	if (controller) {
		controller->Release(slot_id);
	}
	controller = other.controller;
	slot_id = other.slot_id;
	other.controller = nullptr;
	other.slot_id = 0;
	return *this;
}

QueryAdmissionController::QueryAdmissionController(DatabaseInstance &db_p) : db(db_p) {
}

uint64_t QueryAdmissionController::TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

bool QueryAdmissionController::CanAdmit(const Waiter &waiter) const {
	return !waiters.empty() && waiters.front() == &waiter && active.size() < waiter.max_active;
}

void QueryAdmissionController::AdmitWaiter(Waiter &waiter) {
	auto slot_id = next_slot_id++;
	auto admitted_ns = TimestampNs();
	active[slot_id] =
	    ActiveEntry {waiter.metadata, slot_id, waiter.ticket, waiter.max_active, waiter.queued_ns, admitted_ns,
	                 waiter.debug_enabled};
	waiter.slot_id = slot_id;
	waiter.admitted_ns = admitted_ns;
	waiter.admitted = true;
	waiters.pop_front();
	LogEventLocked(waiter.metadata, "admitted", waiter.ticket, waiter.slot_id, waiter.max_active, waiter.queued_ns,
	               waiter.admitted_ns, 0, waiter.debug_enabled);
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

unique_ptr<QueryAdmissionHandle> QueryAdmissionController::Acquire(ClientContext &context,
                                                                   const QueryRequestMetadata &metadata,
                                                                   idx_t max_active) {
	if (!metadata.valid || max_active == 0) {
		return nullptr;
	}

	unique_lock<mutex> guard(admission_lock);
	auto debug_enabled = context.config.query_activation_debug_enabled;
	if (!debug_enabled && waiters.empty() && active.size() < max_active) {
		auto ticket = next_ticket++;
		auto slot_id = next_slot_id++;
		auto admitted_ns = TimestampNs();
		active.emplace(slot_id, ActiveEntry {metadata, slot_id, ticket, max_active, admitted_ns, admitted_ns,
		                                     debug_enabled});
		return make_uniq<QueryAdmissionHandle>(*this, slot_id);
	}

	Waiter waiter(metadata, max_active, next_ticket++, TimestampNs(), debug_enabled);
	waiters.push_back(&waiter);
	auto remove_waiter = [&]() {
		if (waiter.admitted) {
			return;
		}
		for (auto entry = waiters.begin(); entry != waiters.end(); entry++) {
			if (*entry == &waiter) {
				waiters.erase(entry);
				break;
			}
		}
		for (auto *pending : waiters) {
			pending->cv.notify_one();
			break;
		}
	};
	struct WaiterCleanup {
		decltype(remove_waiter) &cleanup;
		~WaiterCleanup() {
			cleanup();
		}
	} waiter_cleanup {remove_waiter};
	LogEventLocked(waiter.metadata, "queued", waiter.ticket, 0, waiter.max_active, waiter.queued_ns, 0, 0,
	               waiter.debug_enabled);
	while (!waiter.admitted) {
		if (CanAdmit(waiter)) {
			AdmitWaiter(waiter);
			break;
		}
		waiter.cv.wait(guard);
	}
	for (auto *pending : waiters) {
		pending->cv.notify_one();
		break;
	}
	return make_uniq<QueryAdmissionHandle>(*this, waiter.slot_id);
}

void QueryAdmissionController::Release(idx_t slot_id) {
	lock_guard<mutex> guard(admission_lock);
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
	for (auto *waiter : waiters) {
		waiter->cv.notify_one();
		break;
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
	for (auto *waiter : waiters) {
		const auto &metadata = waiter->metadata;
		QueryAdmissionSnapshot snapshot;
		snapshot.state = "waiting";
		snapshot.slot_id = 0;
		snapshot.db_query_id = metadata.db_query_id;
		snapshot.request_id = metadata.request_id;
		snapshot.template_id = metadata.template_id;
		snapshot.scale_factor = metadata.scale_factor;
		snapshot.sla_tag = metadata.sla_tag;
		snapshot.deadline_ns = metadata.deadline_ns;
		snapshot.queued_ns = waiter->queued_ns;
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
