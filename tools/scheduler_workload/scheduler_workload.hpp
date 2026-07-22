//===----------------------------------------------------------------------===//
//                         DuckDB
//
// scheduler_workload.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/query_scheduler_policy.hpp"

#include <map>

namespace duckdb {
namespace scheduler_workload {

enum class WorkloadCommand : uint8_t { INVALID = 0, CALIBRATE, TRAIN_PROFILES, GENERATE, RUN };

struct ScaleDatabase {
	uint64_t scale_factor = 0;
	string path;
	double weight = 1;
};

struct CalibrationEntry {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t sample_count = 0;
	double mean_runtime_s = 0;
};

struct ScheduleEntry {
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	uint64_t scheduled_offset_ns = 0;
	uint64_t deadline_offset_ns = 0;
	double sla_tag = 0;
	double sla_penalty_per_s = 0;
};

struct WorkloadConfig {
	WorkloadCommand command = WorkloadCommand::INVALID;
	vector<ScaleDatabase> databases;
	vector<uint64_t> query_templates;
	vector<uint64_t> scheduler_trace_templates;
	string queries_dir;
	string calibration_path;
	string schedule_path;
	string output_path;
	string output_dir;
	string profile_snapshot_in;
	string profile_snapshot_out;
	QuerySchedulerPolicy scheduler_policy = QuerySchedulerPolicy::DEFAULT;
	idx_t threads = 1;
	idx_t submitter_threads = 2;
	idx_t completion_threads = 2;
	idx_t admission_cap = 128;
	idx_t repetitions = 3;
	uint64_t seed = 1;
	double duration_s = 300;
	double load_factor = 0.95;
	double sla_tag = 30;
	double sla_penalty_per_s = 1;
	double max_driver_lag_ms = 10;
	bool sla_residual_workers_enabled = true;
	double sla_energy_lambda = 1;
	uint64_t sla_energy_exploration_seed = 1;
	idx_t energy_attribution_period_ms = 100;
	string energy_power_model_path;
	bool sla_energy_exploration = false;
	bool sla_energy_hardware_control = true;
	bool energy_attribution_enabled = false;
	bool energy_attribution_export = true;
	bool energy_attribution_debug_export = false;
	bool fail_on_invalid = true;
};

struct RequestResult {
	ScheduleEntry request;
	uint64_t arrival_ns = 0;
	uint64_t submit_begin_ns = 0;
	uint64_t submit_end_ns = 0;
	uint64_t admitted_ns = 0;
	uint64_t execution_started_ns = 0;
	uint64_t execution_ready_ns = 0;
	uint64_t result_ready_ns = 0;
	uint64_t admission_released_ns = 0;
	string status;
	string error;
};

struct RunSummary {
	string scheduler;
	string schedule_hash;
	uint64_t run_start_ns = 0;
	idx_t request_count = 0;
	idx_t completed_count = 0;
	idx_t error_count = 0;
	idx_t queued_count = 0;
	idx_t max_admission_waiters = 0;
	idx_t notification_queue_high_water_mark = 0;
	double makespan_s = 0;
	double throughput_qps = 0;
	double mean_response_s = 0;
	double p50_response_s = 0;
	double p90_response_s = 0;
	double p99_response_s = 0;
	double mean_arrival_dispatch_lag_ms = 0;
	double p99_arrival_dispatch_lag_ms = 0;
	double mean_submission_lag_ms = 0;
	double p99_submission_lag_ms = 0;
	double p99_admission_resume_lag_ms = 0;
	double total_sla_cost = 0;
	idx_t sla_miss_count = 0;
	bool valid = false;
	vector<string> validity_errors;
};

WorkloadConfig ParseConfig(int argc, char **argv);
string Usage();
string SchedulerName(QuerySchedulerPolicy policy);

vector<CalibrationEntry> ReadCalibration(const string &path);
void WriteCalibration(const string &path, const vector<CalibrationEntry> &entries);
double WeightedMeanDuration(const WorkloadConfig &config, const vector<CalibrationEntry> &calibration);
vector<ScheduleEntry> GenerateSchedule(const WorkloadConfig &config, const vector<CalibrationEntry> &calibration);
vector<ScheduleEntry> ReadSchedule(const string &path);
void WriteSchedule(const string &path, const vector<ScheduleEntry> &schedule);
string ScheduleHash(const vector<ScheduleEntry> &schedule);

vector<CalibrationEntry> Calibrate(const WorkloadConfig &config);
void TrainProfiles(const WorkloadConfig &config);
RunSummary RunWorkload(const WorkloadConfig &config, const vector<ScheduleEntry> &schedule,
                       vector<RequestResult> &results);
void WriteRunOutputs(const WorkloadConfig &config, const RunSummary &summary,
                     const vector<RequestResult> &results);

} // namespace scheduler_workload
} // namespace duckdb
