#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/pending_query_notification.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {
namespace scheduler_workload {

namespace {

static uint64_t TimestampNs() {
	return PendingQueryNotification::TimestampNs();
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
	ExecuteSetting(root, "SET energy_attribution_rapl_enable=true");
	ExecuteSetting(root, "SET energy_attribution_perf_counters_enable=true");
	ExecuteSetting(root, string("SET energy_attribution_export_enable=") +
	                         (config.energy_attribution_export ? "true" : "false"));
	ExecuteSetting(root, string("SET energy_attribution_debug_export_enable=") +
	                         (config.energy_attribution_debug_export ? "true" : "false"));
	ExecuteSetting(root, "SET energy_attribution_closed_segments_export_enable=false");
	ExecuteSetting(root, "SET energy_attribution_overhead_detail_enable=false");
	ExecuteSetting(root, "SET energy_attribution_pipeline_signatures_enable=false");
	ExecuteSetting(root, "SET energy_attribution_metadata_cache_enable=true");
	ExecuteSetting(root, "SET energy_attribution_lifecycle_phases_enable=true");
	ExecuteSetting(root, "SET energy_attribution_migration_check_enable=true");
	ExecuteSetting(root, "SET energy_attribution_profile_update_enable=true");
	ExecuteSetting(root, "SET energy_attribution_period_ms=" +
	                         std::to_string(config.energy_attribution_period_ms));
	ExecuteSetting(root, "SET energy_attribution_periodic_enable=true");
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
	ExecuteSetting(root, "SET threads=" + std::to_string(config.threads));
	ExecuteSetting(root, "SET external_threads=0");
	ExecuteSetting(root, "SET query_worker_only_execution_enable=true");
	if (policy == QuerySchedulerPolicy::SLA_ENERGY || config.energy_attribution_enabled) {
		ExecuteSetting(root, "SET pin_threads='on'");
	}
	if (policy == QuerySchedulerPolicy::SLA_ENERGY) {
		ExecuteSetting(root, string("SET query_sla_energy_hardware_control_enable=") +
		                         (config.sla_energy_hardware_control ? "true" : "false"));
		ExecuteSetting(root, "SET query_sla_energy_lambda=" + std::to_string(config.sla_energy_lambda));
		ExecuteSetting(root, string("SET query_sla_energy_exploration_enable=") +
		                         (config.sla_energy_exploration ? "true" : "false"));
		ExecuteSetting(root, "SET query_sla_energy_exploration_seed=" +
		                         std::to_string(config.sla_energy_exploration_seed));
		ExecuteSetting(root, "SET query_sla_energy_power_model_path=" +
		                         QuoteSQLString(config.energy_power_model_path));
	}
	ConfigureEnergyAttribution(root, config);
	ExecuteSetting(root, "SET scheduler_policy='" + SchedulerName(policy) + "'");
	for (const auto &database : config.databases) {
		ExecuteSetting(root, "ATTACH " + QuoteSQLString(database.path) + " AS sf" +
		                         std::to_string(database.scale_factor) + " (READ_ONLY)");
	}
}

static void ConfigureRequestConnection(Connection &connection, const WorkloadConfig &config, uint64_t scale_factor,
	                                   bool nonblocking, bool scheduler_trace) {
	ExecuteSetting(connection, "SET query_request_profiling_enable=true");
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
}

static void ExportProfiles(DuckDB &db, const WorkloadConfig &config) {
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
	shared_ptr<PendingQueryNotification> notification;
	std::mutex lock;
	std::condition_variable submitted_cv;
	bool submission_done = false;
	bool admission_queued = false;
	bool finalizing = false;
	bool done = false;
	RequestResult result;
};

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

static RunSummary Summarize(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
	                        const vector<RequestResult> &results, uint64_t run_start_ns, idx_t queue_high_water,
	                        idx_t max_waiters) {
	RunSummary summary;
	summary.scheduler = SchedulerName(config.scheduler_policy);
	summary.schedule_hash = ScheduleHash(schedule);
	summary.run_start_ns = run_start_ns;
	summary.request_count = schedule.size();
	summary.max_admission_waiters = max_waiters;
	summary.notification_queue_high_water_mark = queue_high_water;
	vector<double> response_s;
	vector<double> arrival_dispatch_lag_ms;
	vector<double> submission_lag_ms;
	vector<double> admission_resume_lag_ms;
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
		response_s.push_back(response);
		arrival_dispatch_lag_ms.push_back(arrival_lag);
		submission_lag_ms.push_back(submission_lag);
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
	summary.p99_response_s = Percentile(response_s, 0.99);
	summary.mean_arrival_dispatch_lag_ms =
	    arrival_dispatch_lag_ms.empty() ? 0 : arrival_lag_total / arrival_dispatch_lag_ms.size();
	summary.p99_arrival_dispatch_lag_ms = Percentile(arrival_dispatch_lag_ms, 0.99);
	summary.mean_submission_lag_ms =
	    submission_lag_ms.empty() ? 0 : submission_lag_total / submission_lag_ms.size();
	summary.p99_submission_lag_ms = Percentile(submission_lag_ms, 0.99);
	summary.p99_admission_resume_lag_ms = Percentile(admission_resume_lag_ms, 0.99);
	if (summary.completed_count != summary.request_count) {
		summary.validity_errors.push_back("not_all_requests_completed_successfully");
	}
	if (summary.p99_arrival_dispatch_lag_ms > config.max_driver_lag_ms) {
		summary.validity_errors.push_back("p99_arrival_dispatch_lag_exceeded_limit");
	}
	if (summary.p99_submission_lag_ms > config.max_driver_lag_ms) {
		summary.validity_errors.push_back("p99_database_submission_lag_exceeded_limit");
	}
	if (summary.p99_admission_resume_lag_ms > config.max_driver_lag_ms) {
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

vector<CalibrationEntry> Calibrate(const WorkloadConfig &config) {
	DuckDB db;
	Connection root(db);
	ConfigureDatabase(root, config, QuerySchedulerPolicy::DEFAULT);
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

void TrainProfiles(const WorkloadConfig &config) {
	DuckDB db;
	Connection root(db);
	ConfigureDatabase(root, config, QuerySchedulerPolicy::DEFAULT);
	ImportProfiles(db, config);
	uint64_t request_id = 1;
	for (idx_t repetition = 0; repetition < config.repetitions; repetition++) {
		for (const auto &database : config.databases) {
			for (const auto template_id : config.query_templates) {
				Connection connection(db);
				ConfigureRequestConnection(connection, config, database.scale_factor, false, false);
				auto prepared = connection.Prepare(ReadTextFile(QueryPath(config, template_id)));
				if (prepared->HasError()) {
					throw InvalidInputException("Could not prepare Q%llu SF%llu: %s", template_id,
					                            database.scale_factor, prepared->GetError());
				}
				auto now = TimestampNs();
				auto metadata = BuildMetadata(request_id++, template_id, database.scale_factor, config.sla_tag,
				                              config.sla_penalty_per_s, now + 86400ULL * 1000000000ULL);
				auto result = ExecutePrepared(*prepared, metadata);
				if (!result || result->HasError()) {
					throw InvalidInputException("Profile training Q%llu SF%llu failed: %s", template_id,
					                            database.scale_factor, result ? result->GetError() : "no result");
				}
			}
		}
	}
	ExportProfiles(db, config);
}

RunSummary RunWorkload(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
	                   vector<RequestResult> &results) {
	DuckDB db;
	Connection root(db);
	ConfigureDatabase(root, config, config.scheduler_policy);
	ImportProfiles(db, config);

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
		state->connection = make_uniq<Connection>(db);
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

	BlockingQueue<idx_t> submissions;
	std::atomic<idx_t> done_count(0);
	std::atomic<idx_t> current_waiters(0);
	std::atomic<idx_t> max_waiters(0);
	std::mutex done_lock;
	std::condition_variable done_cv;
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
				parameters.notification = request.notification;
				request.pending = request.prepared->PendingQuery(values, std::move(parameters));
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
						}
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
					request.result.result_ready_ns = event.timestamp_ns;
					if (!event.error.empty()) {
						request.result.error = event.error;
					}
					request.result.status = event.type == PendingQueryEventType::RESULT_READY ? "ok" : "error";
					if (!request.done) {
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
		for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
			auto target = std::chrono::steady_clock::time_point(
			    std::chrono::nanoseconds(run_start_ns + requests[request_idx]->schedule.scheduled_offset_ns));
			std::this_thread::sleep_until(target);
			requests[request_idx]->result.arrival_ns = TimestampNs();
			submissions.Push(request_idx);
		}
		submissions.Close();
	});

	{
		std::unique_lock<std::mutex> guard(done_lock);
		while (done_count.load() < requests.size()) {
			done_cv.wait(guard);
		}
	}
	arrivals.join();
	for (auto &thread : submitters) {
		thread.join();
	}
	notification_queue->Close();
	for (auto &thread : completers) {
		thread.join();
	}

	results.clear();
	results.reserve(requests.size());
	for (auto &request : requests) {
		results.push_back(std::move(request->result));
	}
	auto summary = Summarize(config, schedule, results, run_start_ns, notification_queue->HighWaterMark(),
	                         max_waiters.load());
	FinalizeEnergyAttribution(db, root, config);
	db.instance->GetFileSystem().CreateDirectoriesRecursive(config.output_dir);
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

} // namespace scheduler_workload
} // namespace duckdb
