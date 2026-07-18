#include "duckdb/parallel/event.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

Event::Event(Executor &executor_p)
    : executor(executor_p), finished_tasks(0), total_tasks(0), blocked_tasks(0), finished_dependencies(0),
      total_dependencies(0),
      finished(false), activation_registered(false), activation_group_id(0), activation_pipeline_id(0),
      activation_kind(QueryActivationEventKind::UNREGISTERED), energy_lifecycle_registered(false),
      energy_lifecycle_group_id(0) {
}

void Event::CompleteDependency() {
	idx_t current_finished = ++finished_dependencies;
	D_ASSERT(current_finished <= total_dependencies);
	if (current_finished == total_dependencies) {
		// all dependencies have been completed: schedule the event
		D_ASSERT(total_tasks == 0);
		executor.ScheduleEvent(shared_from_this());
	}
}

void Event::Finish() {
	D_ASSERT(!finished);
	FinishEvent();
	finished = true;
	QueryPipelineDebug::RecordEventFinished(*this);
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for (auto &parent_entry : parents) {
		auto parent = parent_entry.lock();
		if (!parent) { // LCOV_EXCL_START
			continue;
		} // LCOV_EXCL_STOP
		// mark a dependency as completed for each of the parents
		parent->CompleteDependency();
	}
	FinalizeFinish();
	executor.NotifyEventFinished(*this);
}

void Event::AddDependency(Event &event) {
	total_dependencies++;
	event.parents.push_back(weak_ptr<Event>(shared_from_this()));
#ifdef DEBUG
	event.parents_raw.push_back(*this);
#endif
}

const vector<reference<Event>> &Event::GetParentsVerification() const {
	D_ASSERT(parents.size() == parents_raw.size());
	return parents_raw;
}

void Event::FinishTask() {
	D_ASSERT(finished_tasks.load() < total_tasks.load());
	idx_t current_tasks = total_tasks;
	idx_t current_finished = ++finished_tasks;
	D_ASSERT(current_finished <= current_tasks);
	if (current_finished == current_tasks) {
		Finish();
	}
}

void Event::MarkTaskBlocked() {
	auto blocked = ++blocked_tasks;
	D_ASSERT(blocked <= total_tasks.load() - finished_tasks.load());
}

void Event::MarkTaskUnblocked() {
	auto blocked = blocked_tasks.load();
	while (blocked > 0 && !blocked_tasks.compare_exchange_weak(blocked, blocked - 1)) {
	}
}

ClientContext &Event::GetClientContext() {
	return executor.context;
}

void Event::InsertEvent(shared_ptr<Event> replacement_event) {
	replacement_event->parents = std::move(parents);
#ifdef DEBUG
	replacement_event->parents_raw = std::move(parents_raw);
#endif
	if (activation_registered) {
		replacement_event->SetQueryActivationInfo(activation_group_id, QueryActivationEventKind::INTERNAL,
		                                          activation_pipeline_id);
	}
	replacement_event->AddDependency(*this);
	executor.AddEvent(std::move(replacement_event));
}

void Event::SetQueryActivationInfo(idx_t group_id, QueryActivationEventKind kind, idx_t pipeline_id) {
	activation_registered = true;
	activation_group_id = group_id;
	activation_pipeline_id = pipeline_id;
	activation_kind = kind;
}

void Event::SetEnergyLifecycleInfo(idx_t group_id, vector<uint64_t> member_pipeline_ids) {
	energy_lifecycle_registered = true;
	energy_lifecycle_group_id = group_id;
	energy_lifecycle_member_pipeline_ids = std::move(member_pipeline_ids);
}

void Event::SetTasks(vector<shared_ptr<Task>> tasks) {
	auto &ts = TaskScheduler::GetScheduler(executor.context);
	D_ASSERT(total_tasks == 0);
	D_ASSERT(!tasks.empty());
	this->total_tasks = tasks.size();
	try {
		executor.NotifyEventTasksScheduled(*this);
	} catch (...) {
		this->total_tasks = 0;
		throw;
	}
	ts.ScheduleTasks(executor.GetToken(), tasks);
}

} // namespace duckdb
