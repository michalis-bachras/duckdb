#include "duckdb/parallel/event.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

Event::Event(Executor &executor_p)
    : executor(executor_p), finished_tasks(0), total_tasks(0), blocked_tasks(0), finished_dependencies(0),
      total_dependencies(0), finished(false), activation_registered(false), activation_group_id(0),
      activation_pipeline_id(0), activation_kind(QueryActivationEventKind::UNREGISTERED),
      energy_lifecycle_registered(false), energy_lifecycle_group_id(0), internal_event_type(InternalEventType::NONE),
      internal_event_position(0), internal_total_work(0), internal_completed_work(0), internal_worker_time_ns(0),
      internal_work_scalable(false), internal_work_tracking_enabled(false) {
}

const char *InternalEventTypeToString(InternalEventType type) {
	switch (type) {
	case InternalEventType::NONE:
		return "none";
	case InternalEventType::HASH_JOIN_TABLE_INIT:
		return "hash_join_table_init";
	case InternalEventType::HASH_JOIN_FINALIZE:
		return "hash_join_finalize";
	case InternalEventType::HASH_JOIN_REPARTITION:
		return "hash_join_repartition";
	case InternalEventType::HASH_AGGREGATE_FINALIZE:
		return "hash_aggregate_finalize";
	case InternalEventType::HASH_AGGREGATE_DISTINCT_FINALIZE:
		return "hash_aggregate_distinct_finalize";
	case InternalEventType::UNGROUPED_AGGREGATE_DISTINCT_FINALIZE:
		return "ungrouped_aggregate_distinct_finalize";
	case InternalEventType::RANGE_JOIN_MATERIALIZE:
		return "range_join_materialize";
	case InternalEventType::ARROW_MERGE:
		return "arrow_merge";
	case InternalEventType::COPY_REMAINING_BATCHES:
		return "copy_remaining_batches";
	default:
		return "unknown";
	}
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
	if (activation_kind == QueryActivationEventKind::INTERNAL && internal_event_type != InternalEventType::NONE) {
		auto pipeline = GetOwningPipeline();
		if (pipeline) {
			if (internal_work_tracking_enabled) {
				QueryProfiler::Get(GetClientContext())
				    .RecordInternalEventFinish(pipeline->GetProfilerPipelineId(), internal_event_position,
				                               InternalEventTypeToString(internal_event_type));
			}
			// Sink finalization can replace the finish event with a chain of internal events. Keep the pipeline
			// lifecycle boundary at the latest completed internal event instead of the earlier finish task.
			pipeline->RecordProfilerFinishDone();
		}
	}
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

void Event::ConfigureInternalWork(InternalEventType type, idx_t total_units, string native_unit, bool scalable) {
	D_ASSERT(type != InternalEventType::NONE);
	D_ASSERT(total_tasks.load() == 0);
	internal_event_type = type;
	internal_total_work = total_units;
	internal_native_unit = std::move(native_unit);
	internal_work_scalable = scalable;
	auto pipeline = GetOwningPipeline();
	if (!pipeline) {
		return;
	}
	PipelineWorkSnapshot owner;
	pipeline->GetWorkSnapshot(owner);
	QueryRequestMetadata metadata;
	if (!owner.profile_identity.valid || !QueryRequestMetadataManager::TryGetActive(GetClientContext(), metadata)) {
		return;
	}
	internal_work_tracking_enabled = true;
	QueryProfiler::Get(GetClientContext())
	    .RecordInternalEventStart(pipeline->GetProfilerPipelineId(), owner.profile_identity.pipeline_signature_hash,
	                              internal_event_position, InternalEventTypeToString(type), internal_native_unit,
	                              total_units);
	auto estimate = DatabaseInstance::GetDatabase(GetClientContext())
	                    .GetQueryRequestProfileStore()
	                    .ResolveInternalEvent(metadata.template_id, metadata.scale_factor, owner.profile_identity,
	                                          internal_event_position, InternalEventTypeToString(type),
	                                          internal_native_unit);
	internal_historical_throughput = estimate.throughput;
	internal_continuation_estimate = estimate.continuation;
	internal_tail_estimate = estimate.tail_to_pipeline_end;
}

void Event::ReportInternalWork(idx_t completed_units) {
	if (!internal_work_tracking_enabled || completed_units == 0) {
		return;
	}
	auto previous = internal_completed_work.fetch_add(completed_units);
	D_ASSERT(internal_total_work == 0 || previous + completed_units <= internal_total_work);
	auto pipeline = GetOwningPipeline();
	if (pipeline) {
		QueryProfiler::Get(GetClientContext())
		    .RecordInternalEventWork(pipeline->GetProfilerPipelineId(), internal_event_position,
		                             InternalEventTypeToString(internal_event_type), completed_units);
	}
}

void Event::ReportInternalWorkerTime(uint64_t duration_ns) {
	if (!internal_work_tracking_enabled || duration_ns == 0) {
		return;
	}
	internal_worker_time_ns.fetch_add(duration_ns);
	auto pipeline = GetOwningPipeline();
	if (pipeline) {
		QueryProfiler::Get(GetClientContext())
		    .RecordInternalEventWorkerTime(pipeline->GetProfilerPipelineId(), internal_event_position,
		                                   InternalEventTypeToString(internal_event_type), duration_ns);
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
	if (energy_lifecycle_registered) {
		replacement_event->SetEnergyLifecycleInfo(energy_lifecycle_group_id, energy_lifecycle_member_pipeline_ids);
	}
	replacement_event->internal_event_position = internal_event_position + 1;
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
