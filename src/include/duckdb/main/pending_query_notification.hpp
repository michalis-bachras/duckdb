//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/pending_query_notification.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>
#include <deque>

namespace duckdb {

enum class PendingQueryEventType : uint8_t {
	ADMISSION_QUEUED = 0,
	ADMITTED,
	EXECUTION_STARTED,
	EXECUTION_READY,
	EXECUTION_ERROR,
	ADMISSION_RELEASED,
	RESULT_READY,
	ERROR,
	CANCELLED
};

struct PendingQueryEvent {
	uint64_t request_token = 0;
	PendingQueryEventType type = PendingQueryEventType::ERROR;
	uint64_t timestamp_ns = 0;
	string error;
};

//! Shared blocking event queue used by native clients that multiplex many pending queries.
class PendingQueryNotificationQueue {
public:
	PendingQueryNotificationQueue();
	~PendingQueryNotificationQueue();

	PendingQueryNotificationQueue(const PendingQueryNotificationQueue &) = delete;
	PendingQueryNotificationQueue &operator=(const PendingQueryNotificationQueue &) = delete;

	//! Wait for and remove the next event. Returns false after Close once the queue is empty.
	bool Wait(PendingQueryEvent &event);
	//! Remove the next event without blocking.
	bool TryPop(PendingQueryEvent &event);
	void Close();
	idx_t HighWaterMark() const;

private:
	friend class PendingQueryNotification;
	bool Push(PendingQueryEvent event);

private:
	mutable mutex queue_lock;
	std::condition_variable queue_cv;
	std::deque<PendingQueryEvent> events;
	bool closed = false;
	idx_t high_water_mark = 0;
};

//! Per-request notification state. Each transition and terminal outcome is published at most once.
class PendingQueryNotification {
public:
	PendingQueryNotification(uint64_t request_token, shared_ptr<PendingQueryNotificationQueue> queue);

	bool Publish(PendingQueryEventType type, const string &error = string());
	uint64_t RequestToken() const {
		return request_token;
	}

	static uint64_t TimestampNs();
	static bool IsTerminal(PendingQueryEventType type);

private:
	uint64_t request_token;
	shared_ptr<PendingQueryNotificationQueue> queue;
	mutex state_lock;
	uint64_t published_mask = 0;
	bool terminal_published = false;
};

} // namespace duckdb
