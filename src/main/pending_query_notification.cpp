//===----------------------------------------------------------------------===//
//                         DuckDB
//
// pending_query_notification.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/pending_query_notification.hpp"

#include <chrono>

namespace duckdb {

PendingQueryNotificationQueue::PendingQueryNotificationQueue() {
}

PendingQueryNotificationQueue::~PendingQueryNotificationQueue() {
}

bool PendingQueryNotificationQueue::Push(PendingQueryEvent event) {
	{
		lock_guard<mutex> guard(queue_lock);
		if (closed) {
			return false;
		}
		events.push_back(std::move(event));
		high_water_mark = MaxValue<idx_t>(high_water_mark, events.size());
	}
	queue_cv.notify_one();
	return true;
}

bool PendingQueryNotificationQueue::Wait(PendingQueryEvent &event) {
	unique_lock<mutex> guard(queue_lock);
	while (events.empty() && !closed) {
		queue_cv.wait(guard);
	}
	if (events.empty()) {
		return false;
	}
	event = std::move(events.front());
	events.pop_front();
	return true;
}

bool PendingQueryNotificationQueue::TryPop(PendingQueryEvent &event) {
	lock_guard<mutex> guard(queue_lock);
	if (events.empty()) {
		return false;
	}
	event = std::move(events.front());
	events.pop_front();
	return true;
}

void PendingQueryNotificationQueue::Close() {
	{
		lock_guard<mutex> guard(queue_lock);
		closed = true;
	}
	queue_cv.notify_all();
}

idx_t PendingQueryNotificationQueue::HighWaterMark() const {
	lock_guard<mutex> guard(queue_lock);
	return high_water_mark;
}

PendingQueryNotification::PendingQueryNotification(uint64_t request_token_p,
                                                   shared_ptr<PendingQueryNotificationQueue> queue_p)
    : request_token(request_token_p), queue(std::move(queue_p)) {
}

uint64_t PendingQueryNotification::TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

bool PendingQueryNotification::IsTerminal(PendingQueryEventType type) {
	return type == PendingQueryEventType::RESULT_READY || type == PendingQueryEventType::ERROR ||
	       type == PendingQueryEventType::CANCELLED;
}

bool PendingQueryNotification::Publish(PendingQueryEventType type, const string &error) {
	{
		lock_guard<mutex> guard(state_lock);
		auto bit = uint64_t(1) << static_cast<uint8_t>(type);
		if ((published_mask & bit) != 0 || terminal_published) {
			return false;
		}
		published_mask |= bit;
		if (IsTerminal(type)) {
			terminal_published = true;
		}
	}
	if (!queue) {
		return false;
	}
	PendingQueryEvent event;
	event.request_token = request_token;
	event.type = type;
	event.timestamp_ns = TimestampNs();
	event.error = error;
	return queue->Push(std::move(event));
}

} // namespace duckdb
