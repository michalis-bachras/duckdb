#pragma once

#include "duckdb/parallel/query_stride_scheduler.hpp"
#include <array>
#include <condition_variable>
#include <functional>
#include <mutex>

namespace duckdb {
namespace scheduler_workload {

struct PhasePairTarget {
	PhasePairTarget(uint64_t request,idx_t pipeline,string phase_p,vector<idx_t> workers_p,idx_t lifecycle)
	    : request_id(request),pipeline_id(pipeline),phase(std::move(phase_p)),workers(std::move(workers_p)),lifecycle_worker(lifecycle) {}
	PhasePairTarget() = default;
	uint64_t request_id = 0;
	idx_t pipeline_id = 0;
	string phase;
	vector<idx_t> workers;
	idx_t lifecycle_worker = DConstants::INVALID_INDEX;
};

//! Experiment protocol lives in the harness, not in the database scheduler.
//! Atomics publish dispatch decisions; the mutex is used only at event boundaries
//! and by the control thread waiting for readiness/completion.
class PhasePairController final : public QueryStrideTaskSetControl {
public:
	explicit PhasePairController(vector<PhasePairTarget> targets);
	void OnReady(const QueryStrideTaskSetInfo &info) override;
	void OnFinished(const QueryStrideTaskSetInfo &info) override;
	void OnUnregistered(uint64_t request_id, uint64_t generation) noexcept override;
	bool IsWorkerEligible(uint64_t request_id, uint64_t generation, uint64_t task_generation,
	                      idx_t worker, bool serial) const noexcept override;
	void Prepare(idx_t role);
	void Release(const vector<idx_t> &roles);
	void Drain(idx_t role);
	void Cancel() noexcept;
	void WaitReady(idx_t role);
	void WaitFinished(const vector<idx_t> &roles);
	void WaitUnregistered(idx_t role);
	void WriteTrace(const string &path);
	QueryStrideTaskSetInfo TargetInfo(idx_t role) const;

private:
	struct Role {
		PhasePairTarget target;
		atomic<uint64_t> generation {0}, target_generation {0};
		atomic<bool> preparing {false}, released {false}, draining {false}, finished {false}, unregistered {false};
		QueryStrideTaskSetInfo info;
	};
	struct Boundary { QueryStrideTaskSetInfo info; string action; uint64_t timestamp_ns; };
	idx_t Find(uint64_t request) const noexcept;
	void Wait(const std::function<bool()> &predicate);
	void Record(const QueryStrideTaskSetInfo &info, const string &action);
	std::array<Role,2> roles;
	mutable std::mutex lock;
	std::condition_variable changed;
	vector<Boundary> trace;
	string failure;
	atomic<bool> cancelled {false};
};

} // namespace scheduler_workload
} // namespace duckdb
