#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/query_admission_controller.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <map>
#include <stdexcept>
#include <thread>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace duckdb {
namespace scheduler_workload {

// Experimental observer, identical in hardware-control and shadow ablations.
// No hardware writes and no SQL are performed on a worker or scheduler lock.
class LiveSLAObserver {
	DatabaseInstance &db;
	string path;
	int cpu;
	std::atomic<bool> stopping {false};
	std::thread thread;
	string error;
	static uint64_t Now() {
		return std::chrono::duration_cast<std::chrono::nanoseconds>(
		    std::chrono::steady_clock::now().time_since_epoch()).count();
	}
	void Publish(bool idle = false) {
		if (db.GetQuerySchedulerPolicy()==QuerySchedulerPolicy::DEFAULT) {
			const auto count=idle ? 0 : db.GetQueryAdmissionController().GetSnapshot().size();
			std::ofstream out(path+".tmp");
			if (!out) throw std::runtime_error("Cannot create live admission activity snapshot");
			out << "{\"sample_ns\":" << Now() << ",\"registered_queries\":" << count
			    << ",\"allocation_policy\":\"admission_" << QueryAdmissionPolicyName(db.GetQueryAdmissionController().GetPolicy())
			    << "\",\"active\":[]}\n";
			out.close();
			if (!out || std::rename((path+".tmp").c_str(),path.c_str())!=0)
				throw std::runtime_error("Cannot publish live admission activity snapshot");
			return;
		}
		auto active = idle ? vector<QuerySLASchedulerSnapshot>() : db.GetQuerySLAScheduler().GetSnapshot();
		auto epochs = active.empty() ? vector<QuerySLASchedulerEpochSnapshot>() : db.GetQuerySLAScheduler().GetLatestEpochTrace();
		std::map<uint64_t, QuerySLASchedulerEpochSnapshot> latest;
		for (auto it = epochs.rbegin(); it != epochs.rend(); ++it) {
			if (!latest.count(it->db_query_id)) latest.emplace(it->db_query_id, *it);
		}
		std::ofstream out(path + ".tmp");
		if (!out) throw std::runtime_error("Cannot create live scheduler snapshot");
		auto allocation_env = std::getenv("OLAP_SLA_ALLOCATION");
		string allocation = allocation_env ? allocation_env : "legacy";
		if (allocation != "legacy" && allocation != "edf_backfill" && allocation != "triage_backfill" && allocation != "srpt_backfill" &&
		    allocation != "psjf_backfill" && allocation != "stable_srpt_backfill" && allocation != "moore_backfill" &&
		    allocation != "bounded_dp_backfill" && allocation != "sjf_backfill") allocation = "unknown";
		out << std::setprecision(17) << "{\"sample_ns\":" << Now()
		    << ",\"registered_queries\":" << active.size() << ",\"allocation_policy\":\"" << allocation << "\",\"active\":[";
		bool first = true;
		for (auto &q : active) {
			if (!q.event_present || q.event_finished) continue;
			auto found = latest.find(q.db_query_id);
			bool matched = found != latest.end() && found->second.pipeline_generation == q.pipeline_generation &&
			               found->second.pipeline_id == q.pipeline_id && found->second.model_valid && q.model_valid &&
			               found->second.epoch_generation == q.epoch_generation &&
			               found->second.planned_workers == q.planned_workers;
			if (!first) out << ',';
			first = false;
			out << "{\"request_id\":" << q.request_id << ",\"db_query_id\":" << q.db_query_id
			    << ",\"deadline_ns\":" << q.deadline_ns << ",\"pipeline_id\":" << q.pipeline_id
			    << ",\"pipeline_generation\":" << q.pipeline_generation
			    << ",\"planned_workers\":" << q.planned_workers << ",\"assigned_workers\":" << q.assigned_workers
			    << ",\"is_committed_primary\":" << (q.is_committed_primary ? "true" : "false")
			    << ",\"prior_service_ns\":" << q.prior_service_ns
			    << ",\"remaining_work_units\":" << q.event_remaining_work_units
			    << ",\"model_valid\":" << (matched ? "true" : "false")
			    << ",\"full_capacity_remaining_valid\":" << (matched && q.full_capacity_remaining_valid ? "true" : "false")
			    << ",\"full_capacity_remaining_ns\":" << (matched ? q.full_capacity_remaining_ns : 0)
			    << ",\"full_capacity_workers\":" << q.full_capacity_workers
			    << ",\"epoch_ns\":" << (matched ? found->second.epoch_timestamp_ns : 0)
			    << ",\"template_id\":" << (matched ? found->second.template_id : 0)
			    << ",\"predicted_finish_ns\":" << (matched ? found->second.predicted_query_finish_p90_ns : 0) << '}';
		}
		out << "]}\n";
		out.close();
		if (!out || std::rename((path + ".tmp").c_str(), path.c_str()) != 0)
			throw std::runtime_error("Cannot publish live scheduler snapshot");
	}

public:
	LiveSLAObserver(DatabaseInstance &db_p, string path_p, int cpu_p) : db(db_p), path(std::move(path_p)), cpu(cpu_p) {
		if (path.empty()) return;
		thread = std::thread([&]() {
			try {
#ifdef __linux__
				if (cpu >= 0) {
					cpu_set_t cpus; CPU_ZERO(&cpus); CPU_SET(cpu, &cpus);
					if (pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus) != 0)
						throw std::runtime_error("Cannot pin live scheduler observer");
				}
#endif
				while (!stopping.load()) {
					Publish();
					std::this_thread::sleep_for(std::chrono::milliseconds(100));
				}
				Publish(true);
			} catch (const std::exception &e) { error = e.what(); }
		});
	}
	void Stop() { stopping.store(true); if (thread.joinable()) thread.join(); }
	const string &Error() const { return error; }
	~LiveSLAObserver() { Stop(); }
};
} // namespace scheduler_workload
} // namespace duckdb
