//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/executor.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Executor;
class Task;
struct PipelineWorkSnapshot;

enum class QueryActivationEventKind : uint8_t {
	UNREGISTERED = 0,
	INITIALIZE,
	PIPELINE,
	PREPARE_FINISH,
	FINISH,
	COMPLETE,
	INTERNAL
};

class Event : public enable_shared_from_this<Event> {
public:
	explicit Event(Executor &executor);
	virtual ~Event() = default;

public:
	virtual void Schedule() = 0;
	//! Called right after the event is finished
	virtual void FinishEvent() {
	}
	//! Called after the event is entirely finished
	virtual void FinalizeFinish() {
	}

	void FinishTask();
	void Finish();

	void AddDependency(Event &event);
	bool HasDependencies() const {
		return total_dependencies != 0;
	}
	const vector<reference<Event>> &GetParentsVerification() const;

	void CompleteDependency();

	void SetTasks(vector<shared_ptr<Task>> tasks);

	void InsertEvent(shared_ptr<Event> replacement_event);

	bool IsFinished() const {
		return finished;
	}
	idx_t GetTotalTasks() const {
		return total_tasks.load();
	}
	idx_t GetFinishedTasks() const {
		return finished_tasks.load();
	}
	idx_t GetBlockedTasks() const {
		return blocked_tasks.load();
	}
	void MarkTaskBlocked();
	void MarkTaskUnblocked();

	void SetQueryActivationInfo(idx_t group_id, QueryActivationEventKind kind, idx_t pipeline_id = 0);
	bool HasQueryActivationInfo() const {
		return activation_registered;
	}
	idx_t GetQueryActivationGroupId() const {
		return activation_group_id;
	}
	idx_t GetQueryActivationPipelineId() const {
		return activation_pipeline_id;
	}
	QueryActivationEventKind GetQueryActivationKind() const {
		return activation_kind;
	}

	void SetEnergyLifecycleInfo(idx_t group_id, vector<uint64_t> member_pipeline_ids);
	bool HasEnergyLifecycleInfo() const {
		return energy_lifecycle_registered;
	}
	idx_t GetEnergyLifecycleGroupId() const {
		return energy_lifecycle_group_id;
	}
	const vector<uint64_t> &GetEnergyLifecycleMemberPipelineIds() const {
		return energy_lifecycle_member_pipeline_ids;
	}

	virtual void PrintPipeline() {
	}
	virtual bool GetPipelineWorkSnapshot(PipelineWorkSnapshot &) const {
		return false;
	}

	ClientContext &GetClientContext();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	Executor &executor;
	//! The current threads working on the event
	atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the event
	atomic<idx_t> total_tasks;
	//! Tasks currently blocked on asynchronous work and therefore unable to use a worker.
	atomic<idx_t> blocked_tasks;

	//! The amount of completed dependencies
	//! The event can only be started after the dependencies have finished executing
	atomic<idx_t> finished_dependencies;
	//! The total amount of dependencies
	idx_t total_dependencies;

	//! The events that depend on this event to run
	vector<weak_ptr<Event>> parents;
	//! Raw pointers to the parents (used for verification only)
	vector<reference<Event>> parents_raw;

	//! Whether or not the event is finished executing
	atomic<bool> finished;

	//! Optional prototype query activation scheduler metadata.
	bool activation_registered;
	idx_t activation_group_id;
	idx_t activation_pipeline_id;
	QueryActivationEventKind activation_kind;

	//! Optional lifecycle attribution group metadata.
	bool energy_lifecycle_registered;
	idx_t energy_lifecycle_group_id;
	vector<uint64_t> energy_lifecycle_member_pipeline_ids;
};

} // namespace duckdb
