#include "scheduler_workload.hpp"
#include "package_energy.hpp"
#include "phase_pair_controller.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/pending_query_notification.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/main/query_profiler.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <ctime>
#include <random>
#include <set>
#include <numeric>
#include <iomanip>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>
#ifdef __linux__
#include <sched.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace scheduler_workload {

namespace {

static uint64_t TimestampNs() {
	return PendingQueryNotification::TimestampNs();
}

static uint64_t ThreadCpuNs() {
 timespec value;
 if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) != 0) { return 0; }
 return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}

static string QuoteSQLString(const string &value) {
	string result = "'";
	for (const auto c : value) {
		if (c == '\'') {
			result += "''";
		} else {
			result.push_back(c);
		}
	}
	result.push_back('\'');
	return result;
}

static string QueryPath(const WorkloadConfig &config, uint64_t template_id) {
	return config.queries_dir + "/tpch-q" + std::to_string(template_id) + ".sql";
}

static string ReadTextFile(const string &path) {
	std::ifstream input(path.c_str(), std::ios::binary);
	if (!input) {
		throw IOException("Could not open query file: %s", path);
	}
	return string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}

static string ResultFingerprint(QueryResult &result) {
    vector<string> rows;
    uint64_t numeric_count=0;
    long double numeric_abs=0,probe1=0,probe2=0,squares=0;
    auto hash_text=[](const string &text) {
        uint64_t value=14695981039346656037ULL;
        for(auto c:text){value^=static_cast<unsigned char>(c);value*=1099511628211ULL;}
        return value;
    };
    auto mix=[](uint64_t value) {
        value=(value^(value>>30))*0xbf58476d1ce4e5b9ULL;
        value=(value^(value>>27))*0x94d049bb133111ebULL;
        return value^(value>>31);
    };
    while (auto chunk = result.Fetch()) {
        if (!chunk->size()) break;
        for (idx_t row=0; row<chunk->size(); row++) {
            std::ostringstream canonical;
            vector<std::pair<idx_t,double>> numeric;
            for (idx_t col=0; col<chunk->ColumnCount(); col++) {
                auto value = chunk->GetValue(col,row);
                auto type = value.type().id(); string text;
                if (!value.IsNull() && (type == LogicalTypeId::FLOAT || type == LogicalTypeId::DOUBLE)) {
                    auto number=value.GetValue<double>();
                    if (std::isfinite(number)) {numeric.emplace_back(col,number);text="<numeric>";}
                    else text=value.ToString();
                } else { text=value.ToString(); }
                canonical << int(type) << ':' << text.size() << ':' << text << ';';
            }
            auto exact=canonical.str();auto key=hash_text(exact);
            for(const auto &entry:numeric) {
                long double number=entry.second;
                auto seed=key ^ (uint64_t(entry.first+1)*0x9e3779b97f4a7c15ULL);
                auto w1=static_cast<long double>(mix(seed)&0xffffffffULL)/2147483647.5L-1;
                auto w2=static_cast<long double>(mix(seed^0xd1b54a32d192ed03ULL)&0xffffffffULL)/2147483647.5L-1;
                numeric_count++;numeric_abs+=std::abs(number);probe1+=number*w1;probe2+=number*w2;squares+=number*number;
            }
            rows.push_back(std::move(exact));
        }
    }
    std::sort(rows.begin(),rows.end()); uint64_t hash=14695981039346656037ULL;
    for (const auto &row:rows) { for (auto c:row) { hash ^= static_cast<unsigned char>(c); hash *= 1099511628211ULL; } hash ^= 255; hash *= 1099511628211ULL; }
    std::ostringstream fingerprint;
    fingerprint << "v2:" << rows.size() << ':' << hash << ':' << numeric_count << ':' << std::scientific << std::setprecision(20)
                << numeric_abs << ':' << probe1 << ':' << probe2 << ':' << squares;
    return fingerprint.str();
}

static void CheckResult(unique_ptr<MaterializedQueryResult> result, const string &operation) {
	if (!result || result->HasError()) {
		throw InvalidInputException("%s failed: %s", operation, result ? result->GetError() : "no result");
	}
}

static void ExecuteSetting(Connection &connection, const string &query) {
	CheckResult(connection.Query(query), query);
}

static void ConfigureEnergyAttribution(Connection &root, const WorkloadConfig &config) {
	if (!config.energy_attribution_enabled) {
		return;
	}
	auto output_dir = config.output_dir + "/energy_attribution";
	ExecuteSetting(root, "SET energy_attribution_output_dir=" + QuoteSQLString(output_dir));
	ExecuteSetting(root, "SET energy_attribution_base_power_path=" +
	                         QuoteSQLString(config.energy_power_model_path));
	ExecuteSetting(root, string("SET energy_attribution_export_enable=") +
	                         (config.energy_attribution_export ? "true" : "false"));
	ExecuteSetting(root, string("SET energy_attribution_debug_export_enable=") +
	                         (config.energy_attribution_debug_export ? "true" : "false"));
	ExecuteSetting(root, "SET energy_attribution_period_ms=" +
	                         std::to_string(config.energy_attribution_period_ms));
	ExecuteSetting(root, "SET energy_attribution_idle_period_ms=" +
	                         std::to_string(config.energy_attribution_idle_period_ms));
	// Enable last so the runtime starts once with its complete immutable configuration.
	ExecuteSetting(root, "SET energy_attribution_enable=true");
}

static void FinalizeEnergyAttribution(DuckDB &db, Connection &root, const WorkloadConfig &config) {
	if (!config.energy_attribution_enabled) {
		return;
	}
	EnergyAttributionManager::ShutdownDatabaseRuntime(*db.instance);
	// Prevent diagnostic COPY queries below from starting a second periodic runtime.
	ClientConfig::GetConfig(*root.context).energy_attribution.enabled = false;
}

static void ExportRelation(Connection &root, const string &query, const string &path, const string &operation) {
	CheckResult(root.Query("COPY (" + query + ") TO " + QuoteSQLString(path) + " (HEADER, DELIMITER ',')"),
	            operation);
}

static void ConfigureDatabase(Connection &root, const WorkloadConfig &config, QuerySchedulerPolicy policy) {
	auto &architecture_options = DatabaseInstance::GetDatabase(*root.context).config.options;
	architecture_options.query_sla_total_package = config.architecture == "total_package";
	architecture_options.query_sla_total_reference_only = config.energy_controller == "reference" || config.energy_mode == "observe";
	architecture_options.query_sla_total_lowest_safe = config.energy_controller == "lowest_safe";
	architecture_options.query_sla_total_shadow = config.energy_mode != "enforce";
	architecture_options.query_sla_total_models_validated = config.models_validated;
	architecture_options.query_profile_fixed_core_khz = config.profile_core_khz;
	architecture_options.query_sla_total_isolated_profiles_path = config.isolated_profiles_path;
	ExecuteSetting(root, "SET threads=" + std::to_string(config.threads));
	if (!config.memory_limit.empty()) {
		ExecuteSetting(root, "SET memory_limit=" + QuoteSQLString(config.memory_limit));
	}
	ExecuteSetting(root, "SET external_threads=0");
	ExecuteSetting(root, string("SET scheduler_process_partial=") +
	                         (config.scheduler_process_partial ? "true" : "false"));
	ExecuteSetting(root, "SET GLOBAL query_admission_policy=" + QuoteSQLString(QueryAdmissionPolicyName(config.admission_policy)));
	ExecuteSetting(root, "SET query_sla_suffix_model=" + QuoteSQLString(config.architecture == "total_package" ? "histogram" : QuerySLASuffixModelName(config.sla_suffix_model)));
	ExecuteSetting(root, "SET query_worker_only_execution_enable=true");
	ExecuteSetting(root, string("SET query_sla_residual_policy=") +
	                         QuoteSQLString(config.sla_residual_workers_enabled ? "work_conserving" : "park"));
	if (IsQuerySLAPolicy(policy)) {
		ExecuteSetting(root, "SET query_sla_scheduler_epoch_ms=" + std::to_string(config.sla_epoch_ms));
	}
	auto pin_threads =
	    policy == QuerySchedulerPolicy::SLA_ENERGY || config.energy_attribution_enabled ? "on" : config.pin_threads;
	ExecuteSetting(root, "SET pin_threads=" + QuoteSQLString(pin_threads));
	if (config.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY) {
		ExecuteSetting(root, string("SET query_sla_energy_hardware_control_enable=") +
		                         (config.sla_energy_hardware_control ? "true" : "false"));
		ExecuteSetting(root, string("SET query_sla_energy_exploration_enable=") +
		                         (config.sla_energy_exploration ? "true" : "false"));
		ExecuteSetting(root, "SET query_sla_energy_exploration_seed=" +
		                         std::to_string(config.sla_energy_exploration_seed));
		ExecuteSetting(root, "SET query_sla_energy_power_model_path=" +
		                         QuoteSQLString(config.energy_power_model_path));
	}
	ExecuteSetting(root, "SET scheduler_policy='" + SchedulerName(policy) + "'");
	for (const auto &database : config.databases) {
		ExecuteSetting(root, "ATTACH " + QuoteSQLString(database.path) + " AS sf" +
		                         std::to_string(database.scale_factor) + " (READ_ONLY)");
	}
}

static void ConfigureRequestConnection(Connection &connection, const WorkloadConfig &config, uint64_t scale_factor,
	                                   bool nonblocking, bool scheduler_trace) {
	ExecuteSetting(connection, "SET query_request_profiling_enable=true");
    if (config.profile_timing_export) {
        ExecuteSetting(connection,"PRAGMA custom_profiling_settings='{\"DEFAULT\":\"false\",\"PIPELINE_INFO\":\"true\",\"PIPELINE_THROUGHPUT\":\"true\"}'");
        ExecuteSetting(connection,"SET enable_profiling='no_output'");
    }
    if (config.profile_base_power_w > 0) {
        if (config.threads != 1 || !config.profile_core_khz) throw InvalidInputException("Isolated calibration requires one worker and a verified fixed clock");
        ExecuteSetting(connection,"PRAGMA custom_profiling_settings='{\"DEFAULT\":\"false\",\"PIPELINE_INFO\":\"true\",\"PIPELINE_DVFS_METRICS\":\"true\",\"PIPELINE_THROUGHPUT\":\"true\"}'");
        ExecuteSetting(connection,"SET enable_profiling='no_output'");
    }
	ExecuteSetting(connection, "SET query_activation_scheduler_enable=true");
	ExecuteSetting(connection, string("SET query_activation_debug_enable=") + (scheduler_trace ? "true" : "false"));
	ExecuteSetting(connection, "SET query_admission_max_active=" + std::to_string(config.admission_cap));
	ExecuteSetting(connection, string("SET query_admission_nonblocking_enable=") + (nonblocking ? "true" : "false"));
	ExecuteSetting(connection, "USE sf" + std::to_string(scale_factor));
}

static void ImportProfiles(DuckDB &db, const WorkloadConfig &config) {
	if (!config.profile_snapshot_in.empty()) {
		db.instance->GetQueryRequestProfileStore().ImportSnapshot(db.instance->GetFileSystem(),
		                                                          config.profile_snapshot_in);
	}
	if (!config.hardware_profile_snapshot_in.empty()) {
		db.instance->GetQueryRequestProfileStore().ImportHardwareProfiles(db.instance->GetFileSystem(),
		                                                                 config.hardware_profile_snapshot_in);
	}
}

static void ExportProfiles(DuckDB &db, const WorkloadConfig &config) {
 if (!config.output_dir.empty()) {
  db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
  auto &store = db.instance->GetQueryRequestProfileStore();
  std::ofstream models(config.output_dir + "/query_request_suffix_models.csv");
  std::ofstream runtime(config.output_dir + "/query_request_suffix_model_runtime.csv");
  std::ofstream metadata(config.output_dir + "/run_metadata.json");
  if (!models || !runtime || !metadata) { throw IOException("Could not write suffix diagnostics"); }
  models << std::setprecision(17) << "template_id,scale_factor,pipeline_id,pipeline_structure_hash,observation_count,generation,running_mean_ns,latest_ns,prediction_ns,histogram_sample_count\n";
  for (const auto &row : store.GetSuffixModelProfilesSnapshot()) {
   models << row.template_id << ',' << row.scale_factor << ',' << row.pipeline_id << ',' << row.pipeline_structure_hash << ','
          << row.observation_count << ',' << row.generation << ',' << row.running_mean_ns << ',' << row.latest_ns << ','
          << row.prediction_ns << ',' << row.histogram_sample_count << '\n';
  }
  std::ofstream pipeline_reference(config.output_dir+"/pipeline_reference_profiles.csv");
  pipeline_reference << std::setprecision(17) << "template_id,scale_factor,pipeline_id,pipeline_signature_hash,sample_count,work_units,parallelism_cap,throughput_per_worker,continuation_p95_ns_per_unit,lifecycle_mean_ns\n";
  for (const auto &row:store.GetPipelineProfilesSnapshot()) {
      const auto &p=row.estimate;
      pipeline_reference << row.template_id << ',' << row.scale_factor << ',' << p.pipeline_id << ',' << p.pipeline_signature_hash << ','
                         << p.sample_count << ',' << p.mean_planned_input_chunks_equiv << ',' << p.mean_source_max_threads << ','
                         << p.mean_single_worker_chunks_per_s << ',' << p.p95_effective_ns_per_work_unit << ',' << MaxValue<double>(0,p.mean_lifecycle_runtime_ns-p.mean_task_runtime_ns) << '\n';
  }
  auto r = store.GetSuffixModelRuntimeSnapshot();
  runtime << "issued_count,resolved_count,discarded_count,outstanding_count,missing_key_count,duplicate_feedback_count,unmatched_feedback_count,bootstrap_target\n"
          << r.issued_count << ',' << r.resolved_count << ',' << r.discarded_count << ',' << r.outstanding_count << ','
          << r.missing_key_count << ',' << r.duplicate_feedback_count << ',' << r.unmatched_feedback_count << ',' << r.bootstrap_target << '\n';
  metadata << "{\n  \"snapshot_version\": 12,\n  \"scheduler\": \"" << SchedulerName(config.scheduler_policy)
           << "\",\n  \"admission_policy\": \"" << QueryAdmissionPolicyName(config.admission_policy)
           << "\",\n  \"admission_cap\": " << config.admission_cap
           << ",\n  \"suffix_model_requested\": \"" << QuerySLASuffixModelName(config.sla_suffix_model)
           << "\",\n  \"suffix_model\": \"" << QuerySLASuffixModelName(ResolveQuerySLASuffixModel(db.instance->config.options.query_sla_suffix_model, db.instance->GetQuerySchedulerPolicy()))
           << "\",\n  \"bootstrap_target\": " << r.bootstrap_target << ",\n  \"seed\": " << config.seed << "\n}\n";
 }

	if (!config.profile_snapshot_out.empty()) {
		db.instance->GetQueryRequestProfileStore().ExportSnapshot(db.instance->GetFileSystem(),
		                                                          config.profile_snapshot_out);
	}
}

static QueryRequestMetadata BuildMetadata(uint64_t request_id, uint64_t template_id, uint64_t scale_factor,
	                                      double sla_tag, double penalty, uint64_t deadline_ns) {
	QueryRequestMetadata metadata;
	metadata.valid = true;
	metadata.parse_status = "typed";
	metadata.request_id = request_id;
	metadata.template_id = template_id;
	metadata.scale_factor = scale_factor;
	metadata.sla_tag = sla_tag;
	metadata.sla_penalty_per_s = penalty;
	metadata.deadline_ns = deadline_ns;
	return metadata;
}

template <class T>
class BlockingQueue {
public:
	void Push(T value) {
		{
			std::lock_guard<std::mutex> guard(lock);
			values.push_back(std::move(value));
		}
		cv.notify_one();
	}

	bool Pop(T &value) {
		std::unique_lock<std::mutex> guard(lock);
		while (values.empty() && !closed) {
			cv.wait(guard);
		}
		if (values.empty()) {
			return false;
		}
		value = std::move(values.front());
		values.pop_front();
		return true;
	}

	void Close() {
		{
			std::lock_guard<std::mutex> guard(lock);
			closed = true;
		}
		cv.notify_all();
	}

private:
	std::mutex lock;
	std::condition_variable cv;
	std::deque<T> values;
	bool closed = false;
};

struct RequestState {
	ScheduleEntry schedule;
	unique_ptr<Connection> connection;
	unique_ptr<PreparedStatement> prepared;
	unique_ptr<PendingQueryResult> pending;
	unique_ptr<QueryResult> completed_result;
	shared_ptr<PendingQueryNotification> notification;
	std::mutex lock;
	std::condition_variable submitted_cv;
	bool submission_done = false;
	bool admission_queued = false;
	bool finalizing = false;
	bool done = false;
	RequestResult result;
};

static bool HasSubmittedIncompleteRequest(vector<unique_ptr<RequestState>> &requests) {
	for (auto &request : requests) {
		lock_guard<mutex> guard(request->lock);
		if (request->submission_done && !request->done) {
			return true;
		}
	}
	return false;
}

static void PrintNoProgressDiagnostics(DuckDB &db, vector<unique_ptr<RequestState>> &requests, idx_t done_count,
	                                   idx_t current_waiters, idx_t notification_high_water, double stalled_s) {
	auto &task_scheduler = db.instance->GetScheduler();
	auto &sla_scheduler = db.instance->GetQuerySLAScheduler();
	auto snapshots = sla_scheduler.GetSnapshot();
	std::cerr << "scheduler_watchdog_summary"
	          << " stalled_s=" << stalled_s << " done=" << done_count << '/' << requests.size()
	          << " global_queued_tasks=" << task_scheduler.GetNumberOfTasks()
	          << " producer_count=" << task_scheduler.GetProducerCount()
	          << " active_queries=" << snapshots.size() << " epoch_run_count=" << sla_scheduler.EpochRunCount()
	          << " admission_waiters=" << current_waiters
	          << " notification_high_water=" << notification_high_water << '\n';
	for (const auto &snapshot : snapshots) {
		std::cerr << "scheduler_watchdog_query"
		          << " db_query_id=" << snapshot.db_query_id << " request_id=" << snapshot.request_id
		          << " pipeline_id=" << snapshot.pipeline_id
		          << " pipeline_generation=" << snapshot.pipeline_generation << " event_kind=" << snapshot.event_kind
		          << " event_present=" << snapshot.event_present << " event_finished=" << snapshot.event_finished
		          << " event_work_valid=" << snapshot.event_work_snapshot_valid
		          << " scheduled_tasks=" << snapshot.scheduled_tasks << " finished_tasks=" << snapshot.finished_tasks
		          << " blocked_tasks=" << snapshot.blocked_tasks << " remaining_tasks=" << snapshot.remaining_tasks
		          << " parallelism_valid=" << snapshot.parallelism_valid
		          << " preferred_parallelism=" << snapshot.preferred_parallelism
		          << " event_remaining_work=" << snapshot.event_remaining_work_units
		          << " producer_present=" << snapshot.producer_present
		          << " producer_queued_tasks=" << snapshot.producer_queued_tasks
		          << " model_valid=" << snapshot.model_valid << " model_error=" << snapshot.model_error
		          << " demand_cap=" << snapshot.demand_cap
		          << " model_remaining_work=" << snapshot.remaining_work_units
		          << " mandatory_workers=" << snapshot.mandatory_workers
		          << " optional_workers=" << snapshot.optional_workers
		          << " residual_workers=" << snapshot.residual_workers
		          << " planned_workers=" << snapshot.planned_workers
		          << " assigned_workers=" << snapshot.assigned_workers
		          << " mandatory_gain_count=" << snapshot.mandatory_gain_count
		          << " optional_gain_count=" << snapshot.optional_gain_count
		          << " cost_0=" << snapshot.mandatory_cost_zero_workers
		          << " cost_1=" << snapshot.mandatory_cost_one_worker
		          << " first_mandatory_gain=" << snapshot.first_mandatory_gain
		          << " first_optional_gain=" << snapshot.first_optional_gain
		          << " next_mandatory_gain=" << snapshot.next_mandatory_gain
		          << " next_optional_gain=" << snapshot.next_optional_gain
		          << " deadline_slack_ms=" << snapshot.deadline_slack_ms
		          << " epoch_generation=" << snapshot.epoch_generation
		          << " next_epoch_ns=" << snapshot.next_epoch_ns
		          << " epoch_leader=" << snapshot.epoch_leader_active
		          << " epoch_sentinel=" << snapshot.epoch_sentinel_active << '\n';
	}
	for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
		auto &request = *requests[request_idx];
		lock_guard<mutex> guard(request.lock);
		if (request.done || (!request.submission_done && request.result.submit_begin_ns == 0)) {
			continue;
		}
		std::cerr << "scheduler_watchdog_request"
		          << " token=" << request_idx << " request_id=" << request.schedule.request_id
		          << " template_id=" << request.schedule.template_id << " scale_factor=" << request.schedule.scale_factor
		          << " submission_done=" << request.submission_done
		          << " admission_queued=" << request.admission_queued << " pending_present=" << (request.pending != nullptr)
		          << " finalizing=" << request.finalizing << " done=" << request.done
		          << " submit_begin_ns=" << request.result.submit_begin_ns
		          << " submit_end_ns=" << request.result.submit_end_ns
		          << " admitted_ns=" << request.result.admitted_ns
		          << " execution_started_ns=" << request.result.execution_started_ns
		          << " execution_ready_ns=" << request.result.execution_ready_ns
		          << " result_ready_ns=" << request.result.result_ready_ns
		          << " admission_released_ns=" << request.result.admission_released_ns << '\n';
	}
	std::cerr.flush();
}

static double Percentile(vector<double> values, double quantile) {
	if (values.empty()) {
		return 0;
	}
	std::sort(values.begin(), values.end());
	if (values.size() == 1) {
		return values[0];
	}
	auto position = quantile * static_cast<double>(values.size() - 1);
	auto lower = static_cast<idx_t>(std::floor(position));
	auto upper = static_cast<idx_t>(std::ceil(position));
	auto fraction = position - static_cast<double>(lower);
	return values[lower] * (1.0 - fraction) + values[upper] * fraction;
}

static RunSummary SummarizeInternal(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
	                        const vector<RequestResult> &results, uint64_t run_start_ns, idx_t queue_high_water,
	                        idx_t max_waiters) {
	RunSummary summary;
	summary.scheduler = SchedulerName(config.scheduler_policy);
	summary.admission_policy = QueryAdmissionPolicyName(config.admission_policy);
	summary.admission_cap = config.admission_cap;
	summary.schedule_hash = ScheduleHash(schedule);
	summary.run_start_ns = run_start_ns;
	summary.request_count = schedule.size();
	summary.max_admission_waiters = max_waiters;
	summary.notification_queue_high_water_mark = queue_high_water;
	vector<double> response_s;
	vector<double> arrival_to_admission_s;
	vector<double> arrival_dispatch_lag_ms;
	vector<double> driver_handoff_lag_ms;
	vector<double> submitter_queue_lag_ms;
	vector<double> database_registration_latency_ms;
	vector<double> database_registration_cpu_ms;
	vector<double> submission_lag_ms;
	vector<double> admission_resume_lag_ms;
	vector<double> admission_notification_dispatch_lag_ms;
	double response_total = 0;
	double arrival_lag_total = 0;
	double submission_lag_total = 0;
	uint64_t final_ns = run_start_ns;
	for (const auto &result : results) {
		if (result.status == "ok") {
			summary.completed_count++;
		} else {
			summary.error_count++;
		}
		if (result.admitted_ns > 0 && result.admitted_ns > result.submit_end_ns) {
			summary.queued_count++;
		}
		if (result.admitted_ns > 0) {
			auto scheduled_ns = run_start_ns + result.request.scheduled_offset_ns;
			arrival_to_admission_s.push_back(result.admitted_ns > scheduled_ns
			                                     ? static_cast<double>(result.admitted_ns - scheduled_ns) / 1e9
			                                     : 0);
			summary.late_at_admission_count += result.admitted_ns > run_start_ns + result.request.deadline_offset_ns;
		}
		if (result.result_ready_ns == 0) {
			continue;
		}
		auto scheduled_ns = run_start_ns + result.request.scheduled_offset_ns;
		auto response = static_cast<double>(result.result_ready_ns - scheduled_ns) / 1000000000.0;
		auto arrival_lag = result.arrival_ns > scheduled_ns
		                       ? static_cast<double>(result.arrival_ns - scheduled_ns) / 1000000.0
		                       : 0;
		auto submission_lag = result.submit_end_ns > scheduled_ns
		                          ? static_cast<double>(result.submit_end_ns - scheduled_ns) / 1000000.0
		                          : 0;
  auto elapsed_ms = [](uint64_t end, uint64_t start) { return end > start ? static_cast<double>(end - start) / 1000000.0 : 0.0; };
  driver_handoff_lag_ms.push_back(elapsed_ms(result.submit_begin_ns, scheduled_ns));
  submitter_queue_lag_ms.push_back(elapsed_ms(result.submit_begin_ns, result.arrival_ns));
  database_registration_latency_ms.push_back(elapsed_ms(result.submit_end_ns, result.submit_begin_ns));
  database_registration_cpu_ms.push_back(static_cast<double>(result.database_registration_cpu_ns) / 1000000.0);
		response_s.push_back(response);
		arrival_dispatch_lag_ms.push_back(arrival_lag);
		submission_lag_ms.push_back(submission_lag);
        if (result.admitted_ns > 0 && result.admission_dispatched_ns >= result.admitted_ns) {
            admission_notification_dispatch_lag_ms.push_back((result.admission_dispatched_ns-result.admitted_ns)/1e6);
        }
		if (result.admitted_ns > 0 && result.execution_started_ns >= result.admitted_ns) {
			admission_resume_lag_ms.push_back(
			    static_cast<double>(result.execution_started_ns - result.admitted_ns) / 1000000.0);
		}
		response_total += response;
		arrival_lag_total += arrival_lag;
		submission_lag_total += submission_lag;
		final_ns = MaxValue<uint64_t>(final_ns, result.result_ready_ns);
		auto deadline_ns = run_start_ns + result.request.deadline_offset_ns;
		if (result.result_ready_ns > deadline_ns) {
			summary.sla_miss_count++;
			summary.total_sla_cost += result.request.sla_penalty_per_s *
			                          static_cast<double>(result.result_ready_ns - deadline_ns) / 1000000000.0;
		}
	}
	summary.makespan_s = static_cast<double>(final_ns - run_start_ns) / 1000000000.0;
	summary.throughput_qps = summary.makespan_s > 0 ? summary.completed_count / summary.makespan_s : 0;
	summary.mean_response_s = response_s.empty() ? 0 : response_total / response_s.size();
	summary.p50_response_s = Percentile(response_s, 0.50);
	summary.p90_response_s = Percentile(response_s, 0.90);
	summary.p95_response_s = Percentile(response_s, 0.95);
	summary.p99_response_s = Percentile(response_s, 0.99);
	summary.mean_arrival_to_admission_s = arrival_to_admission_s.empty()
	                                          ? 0
	                                          : std::accumulate(arrival_to_admission_s.begin(), arrival_to_admission_s.end(), 0.0) /
	                                                arrival_to_admission_s.size();
	summary.p95_arrival_to_admission_s = Percentile(arrival_to_admission_s, 0.95);
	summary.mean_arrival_dispatch_lag_ms =
	    arrival_dispatch_lag_ms.empty() ? 0 : arrival_lag_total / arrival_dispatch_lag_ms.size();
	summary.p99_arrival_dispatch_lag_ms = Percentile(arrival_dispatch_lag_ms, 0.99);
	summary.mean_submission_lag_ms =
	    submission_lag_ms.empty() ? 0 : submission_lag_total / submission_lag_ms.size();
	summary.p99_submission_lag_ms = Percentile(submission_lag_ms, 0.99);
	summary.mean_driver_handoff_lag_ms = driver_handoff_lag_ms.empty() ? 0 : std::accumulate(driver_handoff_lag_ms.begin(), driver_handoff_lag_ms.end(), 0.0) / driver_handoff_lag_ms.size();
	summary.p99_driver_handoff_lag_ms = Percentile(driver_handoff_lag_ms, 0.99);
	summary.mean_submitter_queue_lag_ms = submitter_queue_lag_ms.empty() ? 0 : std::accumulate(submitter_queue_lag_ms.begin(), submitter_queue_lag_ms.end(), 0.0) / submitter_queue_lag_ms.size();
	summary.p99_submitter_queue_lag_ms = Percentile(submitter_queue_lag_ms, 0.99);
	summary.mean_database_registration_latency_ms = database_registration_latency_ms.empty() ? 0 : std::accumulate(database_registration_latency_ms.begin(), database_registration_latency_ms.end(), 0.0) / database_registration_latency_ms.size();
	summary.p99_database_registration_latency_ms = Percentile(database_registration_latency_ms, 0.99);
	summary.mean_database_registration_cpu_ms = database_registration_cpu_ms.empty() ? 0 : std::accumulate(database_registration_cpu_ms.begin(), database_registration_cpu_ms.end(), 0.0) / database_registration_cpu_ms.size();
	summary.p99_database_registration_cpu_ms = Percentile(database_registration_cpu_ms, 0.99);

	summary.p99_admission_resume_lag_ms = Percentile(admission_resume_lag_ms, 0.99);
	summary.p99_admission_notification_dispatch_lag_ms = Percentile(admission_notification_dispatch_lag_ms, 0.99);
	if (summary.completed_count != summary.request_count) {
		summary.validity_errors.push_back("not_all_requests_completed_successfully");
	}
	if (summary.p99_arrival_dispatch_lag_ms > config.max_driver_lag_ms) {
		summary.validity_errors.push_back("p99_arrival_dispatch_lag_exceeded_limit");
	}
	if (summary.p99_driver_handoff_lag_ms > config.max_driver_lag_ms) {
		summary.validity_errors.push_back("p99_driver_handoff_lag_exceeded_limit");
	}
    if (config.package_energy && summary.p99_admission_notification_dispatch_lag_ms > config.max_driver_lag_ms) {
        summary.validity_errors.push_back("p99_admission_notification_dispatch_lag_exceeded_limit");
    }
	if (!config.package_energy && summary.p99_admission_resume_lag_ms > config.max_driver_lag_ms) {
		summary.validity_errors.push_back("p99_admission_resume_lag_exceeded_limit");
	}
	summary.valid = summary.validity_errors.empty();
	return summary;
}

static unique_ptr<QueryResult> ExecutePrepared(PreparedStatement &prepared, const QueryRequestMetadata &metadata) {
	case_insensitive_map_t<BoundParameterData> values;
	PendingQueryParameters parameters;
	parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	parameters.request_metadata = metadata;
	parameters.has_request_metadata = true;
	auto pending = prepared.PendingQuery(values, std::move(parameters));
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->Execute();
}

} // namespace

bool ResultFingerprintsMatch(const string &a,const string &b) {
    if(a==b)return true;
    vector<string> x,y;string part;std::stringstream left(a),right(b);
    while(std::getline(left,part,':'))x.push_back(part);
    while(std::getline(right,part,':'))y.push_back(part);
    if(x.size()!=8 || y.size()!=8 || x[0]!="v2" || y[0]!="v2")return false;
    for(idx_t i=0;i<4;i++)if(x[i]!=y[i])return false;
    try {
        auto scale=MaxValue<long double>(1,std::max(std::stold(x[4]),std::stold(y[4])));
        for(idx_t i=4;i<8;i++) {
            auto u=std::stold(x[i]),v=std::stold(y[i]);
            auto bound=i==7?MaxValue<long double>(1,std::max(std::abs(u),std::abs(v))):scale;
            if(!std::isfinite(u)||!std::isfinite(v)||std::abs(u-v)>1e-12L*bound)return false;
        }
        return true;
    } catch(...) {return false;}
}

void RunSteady(const WorkloadConfig &config) {
    if (config.databases.size()!=1 || config.query_templates.size()!=1 || config.threads!=1)
        throw InvalidInputException("steady requires one database, one query and one worker");
    DuckDB db;Connection root(db);ConfigureDatabase(root,config,QuerySchedulerPolicy::DEFAULT);
    Connection connection(db);ConfigureRequestConnection(connection,config,config.databases[0].scale_factor,false,false);
    auto prepared=connection.Prepare(ReadTextFile(QueryPath(config,config.query_templates[0])));
    if(prepared->HasError()) throw InvalidInputException("steady prepare failed: %s",prepared->GetError());
    unique_ptr<QueryResult> last_result;
    auto execute=[&](bool check) {
        auto start=TimestampNs();
        auto metadata=BuildMetadata(1,config.query_templates[0],config.databases[0].scale_factor,3,1,start+86400000000000ULL);
        auto result=ExecutePrepared(*prepared,metadata);
        if(!result || result->HasError()) throw InvalidInputException("steady execution failed");
        auto elapsed=(TimestampNs()-start)/1e9;
        string signature;
        if(check)signature=ResultFingerprint(*result);
        else last_result=std::move(result);
        return std::make_pair(signature,elapsed);
    };
    auto reference=execute(true).first;
    std::cout << "READY " << reference << std::endl;
    char command;std::cin.get(command);
    if(command!='G') return;
    std::cout << "RUNNING" << std::endl;
    auto start=TimestampNs();idx_t completed=0;double service=0;
    do {
        auto r=execute(false);
        service+=r.second;completed++;
    } while((TimestampNs()-start)/1e9<config.duration_s);
    auto elapsed=(TimestampNs()-start)/1e9;
    if(!last_result || !ResultFingerprintsMatch(ResultFingerprint(*last_result),reference))throw InvalidInputException("steady result fingerprint changed");
    std::cout << std::setprecision(17) << "{\"completed\":" << completed << ",\"elapsed_s\":" << elapsed
              << ",\"mean_query_s\":" << service/completed << ",\"fingerprint\":\"" << reference << "\",\"correct\":true}" << std::endl;
}

vector<CalibrationEntry> Calibrate(const WorkloadConfig &config) {
	DuckDB db;
	Connection root(db);
	ConfigureDatabase(root, config, QuerySchedulerPolicy::DEFAULT);
	ConfigureEnergyAttribution(root, config);
	vector<CalibrationEntry> calibration;
	uint64_t request_id = 1;
	for (const auto &database : config.databases) {
		for (const auto template_id : config.query_templates) {
			Connection connection(db);
			ConfigureRequestConnection(connection, config, database.scale_factor, false, false);
			auto prepared = connection.Prepare(ReadTextFile(QueryPath(config, template_id)));
			if (prepared->HasError()) {
				throw InvalidInputException("Could not prepare Q%llu SF%llu: %s", template_id,
				                            database.scale_factor, prepared->GetError());
			}
			double total_s = 0;
			for (idx_t warmup=0;warmup<config.warmup_repetitions;warmup++) {
				auto result=prepared->Execute();
				if(!result || result->HasError())throw InvalidInputException("Calibration warmup failed");
			}
			for (idx_t repetition = 0; repetition < config.repetitions; repetition++) {
				auto start_ns = TimestampNs();
				auto metadata = BuildMetadata(request_id++, template_id, database.scale_factor, config.sla_tag,
				                              config.sla_penalty_per_s, start_ns + 86400ULL * 1000000000ULL);
				auto result = ExecutePrepared(*prepared, metadata);
				auto end_ns = TimestampNs();
				if (!result || result->HasError()) {
					throw InvalidInputException("Calibration Q%llu SF%llu failed: %s", template_id,
					                            database.scale_factor, result ? result->GetError() : "no result");
				}
				total_s += static_cast<double>(end_ns - start_ns) / 1000000000.0;
			}
			CalibrationEntry entry;
			entry.template_id = template_id;
			entry.scale_factor = database.scale_factor;
			entry.sample_count = config.repetitions;
			entry.mean_runtime_s = total_s / config.repetitions;
			calibration.push_back(entry);
		}
	}
	return calibration;
}

RunSummary SummarizeRun(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
 const vector<RequestResult> &results, uint64_t run_start_ns, idx_t queue_high_water, idx_t max_waiters) {
 return SummarizeInternal(config, schedule, results, run_start_ns, queue_high_water, max_waiters);
}

void TrainProfiles(const WorkloadConfig &config) {
 DuckDB db;
 Connection root(db);
 ConfigureDatabase(root, config, config.scheduler_policy);
 ConfigureEnergyAttribution(root, config);
 if(config.warmup_repetitions) {
     for(idx_t repetition=0;repetition<config.warmup_repetitions;repetition++) {
         for(const auto &database:config.databases) {
             Connection warm(db);ConfigureRequestConnection(warm,config,database.scale_factor,false,false);
             for(auto query:config.query_templates) {
                 auto result=warm.Query(ReadTextFile(QueryPath(config,query)));
                 if(!result || result->HasError())throw InvalidInputException("Profile warmup failed");
             }
         }
     }
     db.instance->GetQueryRequestProfileStore().Clear();
 }
 ImportProfiles(db, config);
 std::ofstream isolated;
 std::ofstream query_timings, pipeline_timings, task_pmu;
 if (config.profile_timing_export) {
     db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
     query_timings.open(config.output_dir+"/query_timing_samples.csv");
     pipeline_timings.open(config.output_dir+"/pipeline_timing_samples.csv");
     if (!query_timings || !pipeline_timings) throw IOException("Could not open timing exports");
     query_timings << std::setprecision(17) << "request_id,repetition,template_id,scale_factor,phase,start_ns,end_ns,duration_s,fingerprint,correct\n";
     pipeline_timings << "request_id,repetition,template_id,scale_factor,pipeline_id,pipeline_signature_hash,pipeline_structure_hash,source_type,sink_type,start_ns,tasks_done_ns,finish_done_ns,wall_ns,worker_ns,worker_tasks,planned_work_units,source_tuples,source_chunks,pipeline_input_tuples,pipeline_input_chunks,valid_timing\n";
     task_pmu.open(config.output_dir+"/pipeline_task_pmu.csv");
     if (!task_pmu) throw IOException("Could not open task PMU export");
     task_pmu << "request_id,repetition,template_id,scale_factor,pipeline_id,task_id,thread_id,start_cpu,end_cpu,start_ns,end_ns,pmu_group,valid,error,time_enabled_ns,time_running_ns,cycles,instructions,event2,event3,event4,phase,task_type,event_position,activation_group_id,ownership,shared_pipeline_ids,outcome\n";
     query_timings.flush();pipeline_timings.flush();
 }
 if (config.profile_base_power_w > 0) {
     db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
     isolated.open(config.output_dir+"/isolated_pipeline_samples.csv");
     isolated << std::setprecision(17) << "template_id,scale_factor,pipeline_id,pipeline_signature_hash,core_khz,uncore_khz,work_units,worker_ns,wall_ns,package_energy_j,matched_baseline_w,valid\n";
 }
 auto &store = db.instance->GetQueryRequestProfileStore();
 if (config.sla_suffix_bootstrap) { store.SetSuffixBootstrapMode(true, config.sla_suffix_bootstrap_observations); }
 vector<std::pair<idx_t, uint64_t>> tasks;
 for (idx_t i = 0; i < config.databases.size(); i++) {
  for (auto template_id : config.query_templates) { tasks.emplace_back(i, template_id); }
 }
 std::mt19937_64 random(config.seed);
 uint64_t request_id = 1;
 for (idx_t repetition = 0; repetition < config.repetitions; repetition++) {
  std::shuffle(tasks.begin(), tasks.end(), random);
  for (const auto &task : tasks) {
   const auto &database = config.databases[task.first];
   auto template_id = task.second;
   Connection connection(db);
   ConfigureRequestConnection(connection, config, database.scale_factor, false, false);
   auto prepared = connection.Prepare(ReadTextFile(QueryPath(config, template_id)));
   if (prepared->HasError()) { throw InvalidInputException("Could not prepare profile training query: %s", prepared->GetError()); }
   string reference;
   for (idx_t warmup=0;warmup<config.profile_query_warmups;warmup++) {
       auto warm_start=TimestampNs();auto warm_id=request_id++;
       std::cout << "profile_warmup_started Q" << template_id << " SF" << database.scale_factor << std::endl;
       auto warm_metadata=BuildMetadata(warm_id,template_id,database.scale_factor,config.sla_tag,config.sla_penalty_per_s,warm_start+86400ULL*1000000000ULL);
       auto warm_result=ExecutePrepared(*prepared,warm_metadata);auto warm_end=TimestampNs();
       if (!warm_result || warm_result->HasError()) throw InvalidInputException("Per-query profile warmup failed");
       auto fingerprint=ResultFingerprint(*warm_result);
       if (!reference.empty() && !ResultFingerprintsMatch(reference,fingerprint)) throw InvalidInputException("Warmup fingerprint changed");
       reference=fingerprint;
       query_timings << warm_id << ',' << repetition << ',' << template_id << ',' << database.scale_factor << ",warmup," << warm_start << ',' << warm_end << ',' << (warm_end-warm_start)/1e9 << ',' << fingerprint << ",true\n";
       query_timings.flush();
   }
   if (config.profile_execution_gate) {
       std::cout << "profile_execution_ready Q" << template_id << std::endl;
       string command;
       if (!(std::cin >> command) || command!="G") throw InvalidInputException("Expected G at profile execution gate");
   }
   auto timing_start=TimestampNs();auto timing_id=request_id;
   if (config.profile_timing_export) std::cout << "profile_measurement_started Q" << template_id << " SF" << database.scale_factor << std::endl;
   auto metadata = BuildMetadata(request_id++, template_id, database.scale_factor, config.sla_tag,
                                 config.sla_penalty_per_s, TimestampNs() + 86400ULL * 1000000000ULL);
   auto result = ExecutePrepared(*prepared, metadata);
   auto timing_end=TimestampNs();
   if (!result || result->HasError()) { throw InvalidInputException("Profile training failed: %s", result ? result->GetError() : "no result"); }
   if (config.profile_timing_export) {
       auto fingerprint=ResultFingerprint(*result);
       bool correct=!reference.empty() && ResultFingerprintsMatch(reference,fingerprint);
       if (!reference.empty() && !correct) throw InvalidInputException("Measured result differs from warmup");
       query_timings << timing_id << ',' << repetition << ',' << template_id << ',' << database.scale_factor << ",measured," << timing_start << ',' << timing_end << ',' << (timing_end-timing_start)/1e9 << ',' << fingerprint << ',' << (correct?"true":"unchecked") << '\n';
       query_timings.flush();
       for (const auto &p : QueryProfiler::Get(*connection.context).GetPipelineProfilesSnapshot()) {
           auto wall=p.tasks_done_ns>=p.start_ns?p.tasks_done_ns-p.start_ns:0;
           bool valid=p.start_ns>0 && p.tasks_done_ns>=p.start_ns && wall>0;
           pipeline_timings << timing_id << ',' << repetition << ',' << template_id << ',' << database.scale_factor << ',' << p.pipeline_id << ',' << p.pipeline_signature_hash << ',' << p.pipeline_structure_hash << ',' << p.source_type << ',' << p.sink_type << ',' << p.start_ns << ',' << p.tasks_done_ns << ',' << p.finish_done_ns << ',' << wall << ',' << p.worker_task_duration_ns << ',' << p.worker_task_count << ',' << p.planned_input_chunks_equiv << ',' << p.source_tuples_touched << ',' << p.source_chunks_touched << ',' << p.pipeline_input_tuples << ',' << p.pipeline_input_chunks << ',' << (valid?"true":"false") << '\n';
       }
       pipeline_timings.flush();
       for (const auto &task : QueryProfiler::Get(*connection.context).GetPipelinePMUSegmentsSnapshot()) {
           if (task.pmu.group.empty()) continue;
           const auto &pmu=task.pmu;
           task_pmu << timing_id << ',' << repetition << ',' << template_id << ',' << database.scale_factor << ','
                    << task.pipeline_id << ',' << task.task_id << ',' << task.thread_id << ',' << task.start_cpu << ','
                    << task.end_cpu << ',' << task.start_ns << ',' << task.end_ns << ',' << pmu.group << ','
                    << (pmu.valid?"true":"false") << ',' << pmu.error << ',' << pmu.time_enabled << ',' << pmu.time_running;
           for (idx_t i=0;i<5;i++) {
               task_pmu << ',';
               if (i<pmu.values.size()) task_pmu << pmu.values[i];
           }
           task_pmu << ',' << task.phase << ',' << task.task_type << ',' << task.event_position << ','
                    << task.activation_group_id << ',' << task.ownership << ',' << task.shared_pipeline_ids << ',' << task.outcome << '\n';
       }
       task_pmu.flush();
       std::cout << "profile_measurement_completed Q" << template_id << " seconds=" << (timing_end-timing_start)/1e9 << std::endl;
       if (config.profile_execution_gate) {
           std::cout << "profile_execution_finished Q" << template_id << std::endl;
           string command;
           if (!(std::cin >> command) || command!="C") throw InvalidInputException("Expected C after profile execution");
       }
   }
   if (isolated.is_open()) {
       for (const auto &p : QueryProfiler::Get(*connection.context).GetPipelineProfilesSnapshot()) {
           auto wall_ns=p.tasks_done_ns>p.start_ns?p.tasks_done_ns-p.start_ns:0;
           bool valid=p.rapl_supported && wall_ns>0 && wall_ns<200000000000ULL && p.worker_task_duration_ns>0 && p.planned_input_chunks_equiv>0;
           isolated << template_id << ',' << database.scale_factor << ',' << p.pipeline_id << ',' << p.pipeline_signature_hash << ','
                    << config.profile_core_khz << ",2800000," << p.planned_input_chunks_equiv << ',' << p.worker_task_duration_ns << ',' << wall_ns << ','
                    << p.cpu_package_j << ',' << config.profile_base_power_w << ',' << valid << '\n';
       }
   }
  }
 }
 if (config.sla_suffix_bootstrap && !store.SuffixBootstrapComplete()) { throw InvalidInputException("Incomplete exact-key suffix bootstrap; no snapshot exported"); }
 store.SetSuffixBootstrapMode(false);
 FinalizeEnergyAttribution(db, root, config);
 ExportProfiles(db, config);
 if (!config.output_dir.empty()) {
  ExportRelation(root, "SELECT * FROM duckdb_debug_query_request_pipeline_hardware_profiles()",
                 config.output_dir + "/query_request_pipeline_hardware_profiles.csv", "export training hardware profiles");
 }
}

static RunSummary ExecuteRequestBatch(DuckDB &db, Connection &root, const WorkloadConfig &config,
    const vector<ScheduleEntry> &schedule, vector<RequestResult> &results, bool sequential=false,
    std::map<uint64_t,unique_ptr<Connection>> *retained=nullptr,
    std::shared_ptr<PhasePairController> phase_control=nullptr) {
	auto effective_schedule=schedule;
	std::unordered_map<uint64_t, string> query_sql;
	for (const auto template_id : config.query_templates) {
		query_sql[template_id] = ReadTextFile(QueryPath(config, template_id));
	}
	std::unordered_map<uint64_t, bool> available_scales;
	for (const auto &database : config.databases) {
		available_scales[database.scale_factor] = true;
	}

	auto notification_queue = make_shared_ptr<PendingQueryNotificationQueue>();
	vector<unique_ptr<RequestState>> requests;
	requests.reserve(schedule.size());
	std::unordered_map<uint64_t, idx_t> token_to_request;
	for (idx_t i = 0; i < schedule.size(); i++) {
		const auto &entry = schedule[i];
		if (!available_scales[entry.scale_factor] || query_sql.find(entry.template_id) == query_sql.end()) {
			throw InvalidInputException("Schedule references unavailable Q%llu SF%llu", entry.template_id,
			                            entry.scale_factor);
		}
		auto state = make_uniq<RequestState>();
		state->schedule = entry;
		state->result.request = entry;
		state->connection = retained ? std::move(retained->at(entry.template_id)) : make_uniq<Connection>(db);
		auto scheduler_trace = std::find(config.scheduler_trace_templates.begin(),
		                                 config.scheduler_trace_templates.end(), entry.template_id) !=
		                       config.scheduler_trace_templates.end();
		ConfigureRequestConnection(*state->connection, config, entry.scale_factor, true, scheduler_trace);
		state->prepared = state->connection->Prepare(query_sql[entry.template_id]);
		if (state->prepared->HasError()) {
			throw InvalidInputException("Could not prepare request %llu: %s", entry.request_id,
			                            state->prepared->GetError());
		}
		state->notification = make_shared_ptr<PendingQueryNotification>(i, notification_queue);
		token_to_request[i] = i;
		requests.push_back(std::move(state));
	}
	if (config.command==WorkloadCommand::PAIR_PROFILES && config.profile_execution_gate && !phase_control) {
		std::cout << "pair_trial_ready " << config.output_dir << std::endl;
		string command;
		if (!(std::cin>>command) || command!="G") throw InvalidInputException("Expected G before pair trial");
	}
	// Keep database setup and request preparation outside the measured workload.
	ConfigureEnergyAttribution(root, config);

	BlockingQueue<idx_t> submissions;
	std::atomic<idx_t> done_count(0);
	std::atomic<idx_t> current_waiters(0);
	std::atomic<idx_t> max_waiters(0);
	std::mutex done_lock;
	std::condition_variable done_cv;
	PackageEnergyMeter package_meter(config.package_energy);
	auto run_start_ns = TimestampNs() + 100000000ULL;

	vector<std::thread> submitters;
	for (idx_t thread_idx = 0; thread_idx < config.submitter_threads; thread_idx++) {
		submitters.emplace_back([&]() {
			idx_t request_idx;
			while (submissions.Pop(request_idx)) {
				auto &request = *requests[request_idx];
				std::unique_lock<std::mutex> guard(request.lock);
				request.result.submit_begin_ns = TimestampNs();
				case_insensitive_map_t<BoundParameterData> values;
				PendingQueryParameters parameters;
				parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
				parameters.request_metadata = BuildMetadata(
				    request.schedule.request_id, request.schedule.template_id, request.schedule.scale_factor,
				    request.schedule.sla_tag, request.schedule.sla_penalty_per_s,
				    run_start_ns + request.schedule.deadline_offset_ns);
				parameters.has_request_metadata = true;
				if (phase_control) parameters.request_metadata.stride_static_priority=1;
				parameters.notification = request.notification;
				auto registration_cpu_start = ThreadCpuNs();
				request.pending = request.prepared->PendingQuery(values, std::move(parameters));
				request.result.database_registration_cpu_ns = ThreadCpuNs() - registration_cpu_start;
				request.result.submit_end_ns = TimestampNs();
				request.submission_done = true;
				guard.unlock();
				request.submitted_cv.notify_all();
			}
		});
	}

	vector<std::thread> completers;
	for (idx_t thread_idx = 0; thread_idx < config.completion_threads; thread_idx++) {
		completers.emplace_back([&]() {
			PendingQueryEvent event;
			while (notification_queue->Wait(event)) {
				auto request_entry = token_to_request.find(event.request_token);
				if (request_entry == token_to_request.end()) {
					continue;
				}
				auto &request = *requests[request_entry->second];
				std::unique_lock<std::mutex> guard(request.lock);
				while (!request.submission_done) {
					request.submitted_cv.wait(guard);
				}
				switch (event.type) {
				case PendingQueryEventType::ADMISSION_QUEUED: {
					request.admission_queued = true;
					auto waiting = ++current_waiters;
					auto observed = max_waiters.load();
					while (waiting > observed && !max_waiters.compare_exchange_weak(observed, waiting)) {
					}
					break;
				}
				case PendingQueryEventType::ADMITTED:
					request.result.admitted_ns = event.timestamp_ns;
					request.result.admission_dispatched_ns = TimestampNs();
					if (request.admission_queued) {
						request.admission_queued = false;
						--current_waiters;
						if (request.pending && !request.pending->HasError()) {
							request.pending->CheckPulse();
						}
					}
					break;
				case PendingQueryEventType::EXECUTION_STARTED:
					request.result.execution_started_ns = event.timestamp_ns;
					break;
				case PendingQueryEventType::EXECUTION_READY:
				case PendingQueryEventType::EXECUTION_ERROR:
					request.result.execution_ready_ns = event.timestamp_ns;
					if (!request.finalizing && request.pending) {
						request.finalizing = true;
						auto query_result = request.pending->Execute();
						if (!query_result || query_result->HasError()) {
							request.result.error = query_result ? query_result->GetError() : "no query result";
						} else { request.completed_result = std::move(query_result); }
					}
					break;
				case PendingQueryEventType::ADMISSION_RELEASED:
					request.result.admission_released_ns = event.timestamp_ns;
					break;
				case PendingQueryEventType::RESULT_READY:
				case PendingQueryEventType::ERROR:
				case PendingQueryEventType::CANCELLED:
					if (request.admission_queued) {
						request.admission_queued = false;
						--current_waiters;
					}
					request.result.result_ready_ns = config.package_energy ? TimestampNs() : event.timestamp_ns;
					if (!event.error.empty()) {
						request.result.error = event.error;
					}
					request.result.status = event.type == PendingQueryEventType::RESULT_READY ? "ok" : "error";
					if (!request.done) {
						std::lock_guard<std::mutex> boundary_guard(done_lock);
						if (config.package_energy && done_count.load()+1 == requests.size()) package_meter.Stop();
						if (config.package_energy) request.result.result_ready_ns = TimestampNs();
						request.done = true;
						done_count++;
						done_cv.notify_all();
					}
					break;
				}
			}
		});
	}

	std::thread arrivals([&]() {
		if (config.package_energy) {
			std::this_thread::sleep_until(std::chrono::steady_clock::time_point(std::chrono::nanoseconds(run_start_ns)));
			run_start_ns = package_meter.Start();
		}
		for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
			if (sequential && request_idx>0) {
				std::unique_lock<std::mutex> guard(done_lock);
				done_cv.wait(guard,[&](){return requests[request_idx-1]->done;});
				auto offset=TimestampNs()-run_start_ns;
				effective_schedule[request_idx].scheduled_offset_ns=offset;
				requests[request_idx]->schedule.scheduled_offset_ns=offset;
				requests[request_idx]->result.request.scheduled_offset_ns=offset;
			}
			auto target = std::chrono::steady_clock::time_point(
			    std::chrono::nanoseconds(run_start_ns + requests[request_idx]->schedule.scheduled_offset_ns));
			std::this_thread::sleep_until(target);
			requests[request_idx]->result.arrival_ns = TimestampNs();
			submissions.Push(request_idx);
		}
		submissions.Close();
	});

	std::atomic<bool> watchdog_stop(false);
	std::exception_ptr phase_error;
	std::thread phase_coordinator;
	if (phase_control) {
		phase_coordinator=std::thread([&]() {
			try {
				auto signal_workers=[&]() {db.instance->GetScheduler().Signal(config.threads);};
				// Preparation is identical even in a solo arm: the other query's state
				// remains resident, but none of its target work executes in that window.
				for (idx_t role=0;role<2;role++) {
					phase_control->Prepare(role);signal_workers();phase_control->WaitReady(role);
				}
				for (auto &r:requests) QueryProfiler::Get(*r->connection->context).GetPipelinePMUSegmentsSnapshot();
				phase_control->WriteTrace(config.output_dir+"/phase_control.csv");
				std::cout<<"phase_targets_ready "<<config.output_dir<<std::endl;
				string command;
				if (config.profile_execution_gate && (!(std::cin>>command) || command!="G"))
					throw InvalidInputException("Expected G before phase targets");
				vector<idx_t> selected=config.pair_condition=="a_only" ? vector<idx_t>{0} :
				    (config.pair_condition=="b_only" ? vector<idx_t>{1} : vector<idx_t>{0,1});
				phase_control->Release(selected);signal_workers();phase_control->WaitFinished(selected);
				// Completion is published from inside the last task. Wait for its PMU
				// scope to close before draining; otherwise successor work can leak in.
				for (auto &r:requests) QueryProfiler::Get(*r->connection->context).GetPipelinePMUSegmentsSnapshot();
				phase_control->WriteTrace(config.output_dir+"/phase_control.csv");
				std::cout<<"phase_window_finished "<<config.output_dir<<std::endl;
				if (config.profile_execution_gate && (!(std::cin>>command) || command!="C"))
					throw InvalidInputException("Expected C before draining phase pair");
				for (idx_t role=0;role<2;role++) {
					phase_control->Drain(role);signal_workers();phase_control->WaitUnregistered(role);
				}
				phase_control->WriteTrace(config.output_dir+"/phase_control.csv");
			} catch (...) {
				phase_error=std::current_exception();phase_control->Cancel();
				db.instance->GetScheduler().Signal(config.threads);
				for (auto &r:requests) r->connection->Interrupt();
			}
		});
	}
	std::thread watchdog;
	if (config.no_progress_diagnostic_s > 0) {
		watchdog = std::thread([&]() {
			auto last_done = done_count.load();
			while (!watchdog_stop.load()) {
				std::unique_lock<std::mutex> guard(done_lock);
				auto progress = done_cv.wait_for(
				    guard, std::chrono::duration<double>(config.no_progress_diagnostic_s),
				    [&]() { return watchdog_stop.load() || done_count.load() != last_done; });
				if (watchdog_stop.load()) {
					break;
				}
				if (progress) {
					last_done = done_count.load();
					continue;
				}
				auto observed_done = done_count.load();
				guard.unlock();
				if (!HasSubmittedIncompleteRequest(requests)) {
					continue;
				}
				PrintNoProgressDiagnostics(db, requests, observed_done, current_waiters.load(),
				                           notification_queue->HighWaterMark(), config.no_progress_diagnostic_s);
			}
		});
	}

	{
		std::unique_lock<std::mutex> guard(done_lock);
		while (done_count.load() < requests.size()) {
			done_cv.wait(guard);
		}
	}
	watchdog_stop.store(true);
	done_cv.notify_all();
	if (watchdog.joinable()) {
		watchdog.join();
	}
	arrivals.join();
	for (auto &thread : submitters) {
		thread.join();
	}
	notification_queue->Close();
	for (auto &thread : completers) {
		thread.join();
	}
	if (phase_coordinator.joinable()) phase_coordinator.join();
	if (phase_error) std::rethrow_exception(phase_error);

	results.clear();
	results.reserve(requests.size());
	for (auto &request : requests) {
		// Correctness checking consumes materialized results after the final
		// terminal-result energy boundary; it must not distort latency or energy.
		if (request->completed_result) request->result.result_fingerprint = ResultFingerprint(*request->completed_result);
		results.push_back(std::move(request->result));
	}
	if (config.command==WorkloadCommand::PAIR_PROFILES) {
		db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
		std::ofstream query(config.output_dir+"/query_timing_samples.csv");
		std::ofstream pipes(config.output_dir+"/pipeline_timing_samples.csv");
		std::ofstream tasks(config.output_dir+"/pipeline_task_pmu.csv");
		if (!query || !pipes || !tasks) throw IOException("Could not open pair profiling exports");
		query << std::setprecision(17) << "request_id,repetition,template_id,scale_factor,phase,start_ns,end_ns,duration_s,fingerprint,correct,submit_begin_ns,admitted_ns,result_ready_ns\n";
		pipes << "request_id,repetition,template_id,scale_factor,pipeline_id,pipeline_signature_hash,pipeline_structure_hash,source_type,sink_type,start_ns,tasks_done_ns,finish_done_ns,wall_ns,worker_ns,worker_tasks,planned_work_units,source_tuples,source_chunks,pipeline_input_tuples,pipeline_input_chunks,valid_timing\n";
		tasks << "request_id,repetition,template_id,scale_factor,pipeline_id,task_id,thread_id,start_cpu,end_cpu,start_ns,end_ns,pmu_group,valid,error,time_enabled_ns,time_running_ns,cycles,instructions,event2,event3,event4,phase,task_type,event_position,activation_group_id,ownership,shared_pipeline_ids,outcome\n";
		for (idx_t i=0;i<requests.size();i++) {
			auto &r=results[i];auto &context=*requests[i]->connection->context;
			auto begin=r.execution_started_ns,end=r.execution_ready_ns;
			query << r.request.request_id << ",1," << r.request.template_id << ',' << r.request.scale_factor
			      << ",measured," << begin << ',' << end << ',' << (end-begin)/1e9 << ',' << r.result_fingerprint
			      << ",unchecked," << r.submit_begin_ns << ',' << r.admitted_ns << ',' << r.result_ready_ns << '\n';
			for (const auto &p:QueryProfiler::Get(context).GetPipelineProfilesSnapshot()) {
				auto wall=p.tasks_done_ns>=p.start_ns?p.tasks_done_ns-p.start_ns:0;
				pipes << r.request.request_id << ",1," << r.request.template_id << ',' << r.request.scale_factor << ','
				      << p.pipeline_id << ',' << p.pipeline_signature_hash << ',' << p.pipeline_structure_hash << ',' << p.source_type << ','
				      << p.sink_type << ',' << p.start_ns << ',' << p.tasks_done_ns << ',' << p.finish_done_ns << ',' << wall << ','
				      << p.worker_task_duration_ns << ',' << p.worker_task_count << ',' << p.planned_input_chunks_equiv << ','
				      << p.source_tuples_touched << ',' << p.source_chunks_touched << ',' << p.pipeline_input_tuples << ','
				      << p.pipeline_input_chunks << ',' << (p.start_ns && wall?"true":"false") << '\n';
			}
			for (const auto &t:QueryProfiler::Get(context).GetPipelinePMUSegmentsSnapshot()) {
				const auto &pmu=t.pmu;
				tasks << r.request.request_id << ",1," << r.request.template_id << ',' << r.request.scale_factor << ','
				      << t.pipeline_id << ',' << t.task_id << ',' << t.thread_id << ',' << t.start_cpu << ',' << t.end_cpu << ','
				      << t.start_ns << ',' << t.end_ns << ',' << pmu.group << ',' << (pmu.valid?"true":"false") << ','
				      << pmu.error << ',' << pmu.time_enabled << ',' << pmu.time_running;
				for (idx_t k=0;k<5;k++) {tasks << ',';if (k<pmu.values.size()) tasks << pmu.values[k];}
				tasks << ',' << t.phase << ',' << t.task_type << ',' << t.event_position << ',' << t.activation_group_id
				      << ',' << t.ownership << ',' << t.shared_pipeline_ids << ',' << t.outcome << '\n';
			}
			(*retained)[r.request.template_id]=std::move(requests[i]->connection);
		}
		return SummarizeInternal(config,effective_schedule,results,run_start_ns,notification_queue->HighWaterMark(),max_waiters.load());
	}
	auto summary = SummarizeInternal(config, effective_schedule, results, run_start_ns, notification_queue->HighWaterMark(),
	                         max_waiters.load());
 auto suffix_runtime = db.instance->GetQueryRequestProfileStore().GetSuffixModelRuntimeSnapshot();
 if (IsQuerySLAPolicy(config.scheduler_policy) && (suffix_runtime.outstanding_count || suffix_runtime.missing_key_count ||
     suffix_runtime.duplicate_feedback_count || suffix_runtime.unmatched_feedback_count ||
     suffix_runtime.issued_count != suffix_runtime.resolved_count)) {
  summary.validity_errors.push_back("invalid_suffix_forecast_feedback");
  summary.valid = false;
 }
	FinalizeEnergyAttribution(db, root, config);
	db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
	package_meter.Write(config.output_dir);
    if (config.architecture == "total_package") {
        std::ofstream output(config.output_dir + "/total_package_candidates.csv");
        output << std::setprecision(17) << "epoch_generation,candidate_id,reference_work_s,total_package_energy_j,reference_efficiency,candidate_efficiency,modeled_cost,selected,shadow_selected,greedy_allocation_cost,exact_allocation_cost,non_diminishing_queries,reason\n";
        for (const auto &row : db.instance->GetQuerySLAScheduler().GetTotalPackageTrace()) {
            output << row.epoch_generation << ',' << row.candidate_id << ',' << row.reference_work_s << ',' << row.package_energy_j << ',' << row.reference_efficiency << ',' << row.candidate_efficiency << ',' << row.modeled_cost << ',' << row.selected << ',' << row.shadow_selected << ',' << row.greedy_allocation_cost << ',' << row.exact_allocation_cost << ',' << row.non_diminishing_queries << ',' << row.reason << '\n';
        }
    }
	if (!config.scheduler_trace_templates.empty()) {
		auto trace_path = config.output_dir + "/query_sla_scheduler_epochs.csv";
		ExportRelation(root, "SELECT * FROM duckdb_debug_query_sla_scheduler_epochs()", trace_path,
		               "export SLA scheduler epoch trace");
	}
	if (config.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY) {
		ExportRelation(root, "SELECT * FROM duckdb_debug_query_request_pipeline_hardware_profiles()",
		               config.output_dir + "/query_request_pipeline_hardware_profiles.csv",
		               "export pipeline hardware profiles");
		ExportRelation(root, "SELECT * FROM duckdb_debug_query_sla_energy_hardware()",
		               config.output_dir + "/query_sla_energy_hardware.csv", "export SLA-energy hardware state");
			if (!config.scheduler_trace_templates.empty()) {
				ExportRelation(root, "SELECT * FROM duckdb_debug_query_sla_energy_worker_epochs()",
				               config.output_dir + "/query_sla_energy_worker_epochs.csv",
				               "export SLA-energy worker trace");
				ExportRelation(root, "SELECT * FROM duckdb_debug_query_sla_energy_hardware_transitions()",
				               config.output_dir + "/query_sla_energy_hardware_transitions.csv",
				               "export SLA-energy hardware transition trace");
			}
	}
	ExportProfiles(db, config);
	if (config.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY) {
		auto &scheduler = db.instance->GetQuerySLAScheduler();
		scheduler.DeactivateEnergyPolicy();
		auto restoration = scheduler.GetHardwareManager().GetSnapshot();
		if (config.sla_energy_hardware_control && restoration.restoration_failure_count > 0) {
			summary.validity_errors.push_back("hardware_restoration_verification_failed");
			summary.valid = false;
		}
		ExportRelation(root, "SELECT * FROM duckdb_debug_query_sla_energy_hardware()",
		               config.output_dir + "/query_sla_energy_hardware_restoration.csv",
		               "export post-restoration SLA-energy hardware state");
	}
	return summary;
}

RunSummary RunWorkload(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
                       vector<RequestResult> &results) {
	DuckDB db;Connection root(db);
	ConfigureDatabase(root,config,config.warmup_repetitions?QuerySchedulerPolicy::DEFAULT:config.scheduler_policy);
	for (idx_t repetition=0;repetition<config.warmup_repetitions;repetition++) {
		for (const auto &database:config.databases) {
			Connection warm(db);ConfigureRequestConnection(warm,config,database.scale_factor,false,false);
			for (auto query:config.query_templates) {
				auto prepared=warm.Prepare(ReadTextFile(QueryPath(config,query)));
				if (prepared->HasError()) throw InvalidInputException("Warmup prepare failed");
				auto metadata=BuildMetadata(1,query,database.scale_factor,3,1,TimestampNs()+86400000000000ULL);
				auto result=ExecutePrepared(*prepared,metadata);
				if (!result || result->HasError()) throw InvalidInputException("Warmup query failed");
				(void)ResultFingerprint(*result);
			}
		}
	}
	ImportProfiles(db,config);
	if (config.warmup_repetitions) ExecuteSetting(root,"SET scheduler_policy='"+SchedulerName(config.scheduler_policy)+"'");
	return ExecuteRequestBatch(db,root,config,schedule,results);
}

void RunPairProfiles(const WorkloadConfig &config) {
	DuckDB db;Connection root(db);
	ConfigureDatabase(root,config,config.scheduler_policy);
	ExecuteSetting(root,"SET max_temp_directory_size='0B'");
	auto partial_setting=root.Query("SELECT current_setting('scheduler_process_partial')");
	if (partial_setting->HasError()) throw InvalidInputException("Could not verify partial-processing setting");
	const auto partial_enabled=partial_setting->GetValue(0,0).GetValue<bool>();
	if (partial_enabled!=config.scheduler_process_partial) throw InvalidInputException("Partial-processing setting mismatch");
	const auto applied_policy=db.instance->GetQuerySchedulerPolicy();
	if (applied_policy!=config.scheduler_policy) throw InvalidInputException("Pair scheduler policy mismatch");
#ifdef __linux__
	if (config.pair_driver_cpu>=0) {
		if (config.pair_driver_cpu>=CPU_SETSIZE) throw InvalidInputException("Invalid pair driver CPU");
		cpu_set_t cpus;CPU_ZERO(&cpus);CPU_SET(config.pair_driver_cpu,&cpus);
		if (sched_setaffinity(0,sizeof(cpus),&cpus)!=0) throw IOException("Could not pin pair control thread");
	}
#endif
	std::map<uint64_t,unique_ptr<Connection>> connections;
	for (auto query:config.query_templates) connections[query]=make_uniq<Connection>(db);
	db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
	std::ofstream session(config.output_dir+"/pair_session.csv");
	if (!session) throw IOException("Could not open pair session export");
	session << "condition,trial,request_id,template_id,database_instance,connection_instance,worker_pool_size,scheduler_process_partial,scheduler\n";
	std::map<uint64_t,string> fingerprints;
	std::mt19937_64 rng(config.seed);uint64_t next_id=1;idx_t trial=0;
	vector<string> conditions={"sequential","concurrent"};
	std::shuffle(conditions.begin(),conditions.end(),rng);
	struct PairTrial {string condition; vector<WorkloadConfig::PhasePair> targets;string arm;};
	vector<PairTrial> order={{"warmup",{},""}};
	for (idx_t repetition=0;repetition<config.repetitions;repetition++) {
		if (config.phase_pairs.empty()) {
			for (auto &condition:conditions) order.push_back({condition,{},""});
		} else for (auto &pair:config.phase_pairs) {
			vector<string> arms={"a_only","b_only","both"};std::shuffle(arms.begin(),arms.end(),rng);
			for (auto &arm:arms) order.push_back({pair.name+"__"+arm,{pair},arm});
		}
	}
	for (const auto &item:order) {
		const auto &condition=item.condition;
		auto queries=config.query_templates;
		if (condition!="concurrent" && item.targets.empty()) std::shuffle(queries.begin(),queries.end(),rng);
		vector<ScheduleEntry> schedule;
		for (auto query:queries) {
			ScheduleEntry entry;entry.request_id=next_id++;entry.template_id=query;
			entry.scale_factor=config.databases[0].scale_factor;entry.deadline_offset_ns=86400000000000ULL;
			entry.sla_tag=config.sla_tag;entry.sla_penalty_per_s=config.sla_penalty_per_s;schedule.push_back(entry);
		}
		WorkloadConfig batch=config;
		batch.phase_pairs=item.targets;batch.pair_condition=item.arm;
		batch.output_dir=config.output_dir+"/"+std::to_string(++trial)+"_"+condition;
		db.instance->GetFileSystem().CreateDirectoriesRecursive(batch.output_dir);
		std::shared_ptr<PhasePairController> control;
		if (!item.targets.empty()) {
			auto topology=db.instance->GetQuerySLAScheduler().GetHardwareManager().GetWorkerTopology();
			auto map_workers=[&](const vector<idx_t> &cpus) {
				vector<idx_t> workers;
				for (auto cpu:cpus) {
					auto found=std::find_if(topology.begin(),topology.end(),[&](const QueryHardwareWorkerTopology &t){return t.logical_cpu==int(cpu);});
					if (found==topology.end()) throw InvalidInputException("Requested CPU is not a registered worker");
					workers.push_back(found->worker_id);
				}
				return workers;
			};
			auto a=map_workers(config.pair_cpus_a),b=map_workers(config.pair_cpus_b);
			auto &target=item.targets[0];
			control=std::make_shared<PhasePairController>(vector<PhasePairTarget>{
			    {schedule[0].request_id,target.pipeline_a,target.phase_a,a,a[0]},
			    {schedule[1].request_id,target.pipeline_b,target.phase_b,b,b[0]}});
			std::ofstream mapping(batch.output_dir+"/worker_assignments.csv");
			mapping<<"role,request_id,worker_id,logical_cpu,lifecycle\n";
			for (idx_t role=0;role<2;role++) {
				auto &workers=role ? b : a;auto &cpus=role ? config.pair_cpus_b : config.pair_cpus_a;
				for (idx_t i=0;i<workers.size();i++) mapping<<role<<','<<schedule[role].request_id<<','<<workers[i]<<','<<cpus[i]<<','<<(i==0 ? "true" : "false")<<'\n';
			}
			db.instance->GetQueryStrideScheduler().SetTaskSetControl(control);
		}
		vector<RequestResult> results;
		RunSummary summary;
		try {
			summary=ExecuteRequestBatch(db,root,batch,schedule,results,item.targets.empty() && condition!="concurrent",&connections,control);
		} catch (...) {
			if (control) {control->Cancel();db.instance->GetQueryStrideScheduler().SetTaskSetControl(nullptr);}
			throw;
		}
		if (control) db.instance->GetQueryStrideScheduler().SetTaskSetControl(nullptr);
		WriteRunOutputs(batch,summary,results);
		for (const auto &r:results) {
			auto query=r.request.template_id;
			if (condition=="warmup") fingerprints[query]=r.result_fingerprint;
			else if (!ResultFingerprintsMatch(fingerprints.at(query),r.result_fingerprint)) {
				throw InvalidInputException("Pair result differs from warmup");
			}
			session << condition << ',' << trial << ',' << r.request.request_id << ',' << query << ','
			        << reinterpret_cast<uintptr_t>(db.instance.get()) << ','
			        << reinterpret_cast<uintptr_t>(connections.at(query).get()) << ',' << config.threads << ','
			        << (partial_enabled ? "true" : "false") << ',' << SchedulerName(applied_policy) << '\n';
		}
		session.flush();
		if (!summary.valid) throw InvalidInputException("Pair request batch failed validation");
		if (condition=="warmup" && !config.phase_pairs.empty()) {
			// Resolve selectors before staging any measured query. A missing or
			// ambiguous target is an immediate configuration error, not a gate timeout.
			for (auto &pair:config.phase_pairs) for (idx_t role=0;role<2;role++) {
				auto &context=*connections.at(config.query_templates[role])->context;
				auto segments=QueryProfiler::Get(context).GetPipelinePMUSegmentsSnapshot();
				auto pid=role ? pair.pipeline_b : pair.pipeline_a;auto phase=role ? pair.phase_b : pair.phase_a;
				std::set<idx_t> positions;bool found=false;
				for (auto &segment:segments) if (segment.pipeline_id==pid && segment.phase==phase) {
					found=true;positions.insert(segment.event_position);
				}
				if (!found || positions.size()!=1) throw InvalidInputException("Phase-pair target is absent or ambiguous in warmup");
			}
		}
		std::cout << "pair_trial_finished " << condition << std::endl;
		if (config.profile_execution_gate) {
			string command;
			if (!(std::cin>>command) || command!="C") throw InvalidInputException("Expected C after pair trial");
		}
	}
}

} // namespace scheduler_workload
} // namespace duckdb
