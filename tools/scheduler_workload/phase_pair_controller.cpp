#include "phase_pair_controller.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <set>

namespace duckdb {
namespace scheduler_workload {

static uint64_t NowNs() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(
	    std::chrono::steady_clock::now().time_since_epoch()).count();
}

PhasePairController::PhasePairController(vector<PhasePairTarget> targets) {
	if (targets.size()!=2 || targets[0].request_id==targets[1].request_id)
		throw InvalidInputException("Phase pair requires two distinct requests");
	std::set<idx_t> assigned;
	for (idx_t i=0;i<2;i++) {
		auto &t=targets[i];
		if (t.workers.empty() || std::find(t.workers.begin(),t.workers.end(),t.lifecycle_worker)==t.workers.end())
			throw InvalidInputException("Phase pair requires workers and a lifecycle worker in that set");
		for (auto worker:t.workers) if (!assigned.insert(worker).second)
			throw InvalidInputException("Phase pair worker sets must be distinct and disjoint");
		roles[i].target=std::move(t);
	}
}

idx_t PhasePairController::Find(uint64_t request) const noexcept {
	for (idx_t i=0;i<2;i++) if (roles[i].target.request_id==request) return i;
	return DConstants::INVALID_INDEX;
}

void PhasePairController::Record(const QueryStrideTaskSetInfo &info,const string &action) {
	trace.push_back({info,action,NowNs()});
}

void PhasePairController::OnReady(const QueryStrideTaskSetInfo &info) {
	std::lock_guard<std::mutex> guard(lock);
	auto i=Find(info.request_id);
	if (i==DConstants::INVALID_INDEX) { failure="Unexpected request in phase pair";changed.notify_all();return; }
	auto &r=roles[i];
	if (r.generation.load() && r.generation.load()!=info.query_generation) failure="Query generation changed";
	r.generation.store(info.query_generation);
	Record(info,"ready");
	const bool matches=info.pipeline_id==r.target.pipeline_id &&
	    (r.target.phase=="pipeline" ? info.kind==QueryActivationEventKind::PIPELINE :
	     r.target.phase==InternalEventTypeToString(info.internal_type));
	if (matches) {
		if (r.target_generation.load()) failure="Target phase matched more than one task set";
		else {r.info=info;r.target_generation.store(info.task_set_generation,std::memory_order_release);Record(info,"target_ready");}
	}
	changed.notify_all();
}

void PhasePairController::OnFinished(const QueryStrideTaskSetInfo &info) {
	std::lock_guard<std::mutex> guard(lock);
	const auto i=Find(info.request_id);if (i==DConstants::INVALID_INDEX) return;
	auto &r=roles[i];Record(info,"finished");
	if (r.generation.load()==info.query_generation && r.target_generation.load()==info.task_set_generation) {
		r.finished.store(true);Record(info,"target_finished");
	}
	changed.notify_all();
}

void PhasePairController::OnUnregistered(uint64_t request,uint64_t generation) noexcept {
	std::lock_guard<std::mutex> guard(lock);
	const auto i=Find(request);if (i==DConstants::INVALID_INDEX) return;
	auto &r=roles[i];
	if (!cancelled.load() && (!r.finished.load() || r.generation.load()!=generation))
		failure="Query ended before its selected target completed";
	r.unregistered.store(true);changed.notify_all();
}

bool PhasePairController::IsWorkerEligible(uint64_t request,uint64_t generation,uint64_t task_generation,
                                          idx_t worker,bool serial) const noexcept {
	const auto i=Find(request);if (i==DConstants::INVALID_INDEX) return false;
	const auto &r=roles[i];
	if (r.generation.load()!=generation) return false;
	if (std::find(r.target.workers.begin(),r.target.workers.end(),worker)==r.target.workers.end()) return false;
	// Preserve one-worker lifecycle execution, but pin it independently of STRIDE
	// registration-slot reuse. Spare workers cannot be borrowed by the other query.
	if (serial && worker!=r.target.lifecycle_worker) return false;
	if (cancelled.load() || r.draining.load()) return true;
	const auto target=r.target_generation.load(std::memory_order_acquire);
	if (!target) return r.preparing.load();
	return task_generation==target && r.released.load();
}

void PhasePairController::Prepare(idx_t role) {roles.at(role).preparing.store(true);}

void PhasePairController::Release(const vector<idx_t> &selected) {
	std::lock_guard<std::mutex> guard(lock);
	for (auto i:selected) {
		auto &r=roles.at(i);
		if (!r.target_generation.load() || r.finished.load()) throw InvalidInputException("Target is not ready for release");
		Record(r.info,"release");
	}
	// Publish both permissions under one control operation. Start skew is still
	// measured from actual dispatches; a barrier does not guarantee simultaneity.
	for (auto i:selected) roles[i].released.store(true);
}

void PhasePairController::Drain(idx_t role) {roles.at(role).draining.store(true);}
void PhasePairController::Cancel() noexcept {cancelled.store(true);changed.notify_all();}

void PhasePairController::Wait(const std::function<bool()> &predicate) {
	std::unique_lock<std::mutex> guard(lock);
	if (!changed.wait_for(guard,std::chrono::seconds(480),[&](){return cancelled.load() || !failure.empty() || predicate();}))
		throw InvalidInputException("Timed out waiting for phase-pair state transition");
	if (cancelled.load()) throw InvalidInputException("Phase pair cancelled");
	if (!failure.empty()) throw InvalidInputException(failure);
}
void PhasePairController::WaitReady(idx_t role) {Wait([&](){return roles.at(role).target_generation.load()!=0;});}
void PhasePairController::WaitFinished(const vector<idx_t> &selected) {
	Wait([&](){for (auto i:selected) if (!roles.at(i).finished.load()) return false;return true;});
}
void PhasePairController::WaitUnregistered(idx_t role) {Wait([&](){return roles.at(role).unregistered.load();});}

QueryStrideTaskSetInfo PhasePairController::TargetInfo(idx_t role) const {
	std::lock_guard<std::mutex> guard(lock);return roles.at(role).info;
}

void PhasePairController::WriteTrace(const string &path) {
	std::lock_guard<std::mutex> guard(lock);
	std::ofstream out(path);if (!out) throw IOException("Cannot write phase-pair trace");
	out<<"timestamp_ns,request_id,db_query_id,query_generation,task_set_generation,pipeline_id,internal_position,total_work_units,kind,internal_type,action\n";
	for (auto &r:trace) out<<r.timestamp_ns<<','<<r.info.request_id<<','<<r.info.db_query_id<<','
	    <<r.info.query_generation<<','<<r.info.task_set_generation<<','<<r.info.pipeline_id<<','
	    <<r.info.internal_position<<','<<r.info.total_work_units<<','<<int(r.info.kind)<<','
	    <<InternalEventTypeToString(r.info.internal_type)<<','<<r.action<<'\n';
}

} // namespace scheduler_workload
} // namespace duckdb
