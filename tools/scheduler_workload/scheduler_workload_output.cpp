#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

#include <fstream>
#include <iomanip>

namespace duckdb {
namespace scheduler_workload {

namespace {

static string JSONString(const string &value) {
	string result = "\"";
	for (const auto c : value) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result.push_back(c);
			break;
		}
	}
	result.push_back('"');
	return result;
}

static string CSVString(const string &value) {
	string result = "\"";
	for (const auto c : value) {
		if (c == '"') {
			result += "\"\"";
		} else {
			result.push_back(c);
		}
	}
	result.push_back('"');
	return result;
}

} // namespace

void WriteRunOutputs(const WorkloadConfig &config, const RunSummary &summary,
	                 const vector<RequestResult> &results) {
	auto fs = FileSystem::CreateLocal();
	fs->CreateDirectoriesRecursive(config.output_dir);
	{
		std::ofstream output((config.output_dir + "/query_timeline.csv").c_str(), std::ios::trunc);
		if (!output) {
			throw IOException("Could not create query timeline in %s", config.output_dir);
		}
		output << "request_id,template_id,scale_factor,scheduled_offset_ns,deadline_offset_ns,sla_tag,"
		          "sla_penalty_per_s,arrival_ns,"
		          "submit_begin_ns,submit_end_ns,admitted_ns,execution_started_ns,execution_ready_ns,"
		          "admission_released_ns,result_ready_ns,status,error\n";
		for (const auto &result : results) {
			output << result.request.request_id << ',' << result.request.template_id << ','
			       << result.request.scale_factor << ',' << result.request.scheduled_offset_ns << ','
			       << result.request.deadline_offset_ns << ',' << result.request.sla_tag << ','
			       << result.request.sla_penalty_per_s << ',' << result.arrival_ns << ','
			       << result.submit_begin_ns << ',' << result.submit_end_ns << ',' << result.admitted_ns << ','
			       << result.execution_started_ns << ',' << result.execution_ready_ns << ','
			       << result.admission_released_ns << ',' << result.result_ready_ns << ','
			       << CSVString(result.status) << ',' << CSVString(result.error) << '\n';
		}
	}
	{
		std::ofstream output((config.output_dir + "/workload_summary.json").c_str(), std::ios::trunc);
		if (!output) {
			throw IOException("Could not create workload summary in %s", config.output_dir);
		}
		output << std::setprecision(17);
		output << "{\n";
		output << "  \"format\": \"scheduler_workload_summary_v1\",\n";
		output << "  \"scheduler\": " << JSONString(summary.scheduler) << ",\n";
		output << "  \"schedule_hash\": " << JSONString(summary.schedule_hash) << ",\n";
		output << "  \"run_start_ns\": " << summary.run_start_ns << ",\n";
		output << "  \"request_count\": " << summary.request_count << ",\n";
		output << "  \"completed_count\": " << summary.completed_count << ",\n";
		output << "  \"error_count\": " << summary.error_count << ",\n";
		output << "  \"queued_count\": " << summary.queued_count << ",\n";
		output << "  \"max_admission_waiters\": " << summary.max_admission_waiters << ",\n";
		output << "  \"notification_queue_high_water_mark\": "
		       << summary.notification_queue_high_water_mark << ",\n";
		output << "  \"makespan_s\": " << summary.makespan_s << ",\n";
		output << "  \"throughput_qps\": " << summary.throughput_qps << ",\n";
		output << "  \"mean_response_s\": " << summary.mean_response_s << ",\n";
		output << "  \"p50_response_s\": " << summary.p50_response_s << ",\n";
		output << "  \"p90_response_s\": " << summary.p90_response_s << ",\n";
		output << "  \"p99_response_s\": " << summary.p99_response_s << ",\n";
		output << "  \"mean_arrival_dispatch_lag_ms\": " << summary.mean_arrival_dispatch_lag_ms << ",\n";
		output << "  \"p99_arrival_dispatch_lag_ms\": " << summary.p99_arrival_dispatch_lag_ms << ",\n";
		output << "  \"mean_submission_lag_ms\": " << summary.mean_submission_lag_ms << ",\n";
		output << "  \"p99_submission_lag_ms\": " << summary.p99_submission_lag_ms << ",\n";
		output << "  \"p99_admission_resume_lag_ms\": " << summary.p99_admission_resume_lag_ms << ",\n";
		output << "  \"sla_miss_count\": " << summary.sla_miss_count << ",\n";
		output << "  \"total_sla_cost\": " << summary.total_sla_cost << ",\n";
		output << "  \"valid\": " << (summary.valid ? "true" : "false") << ",\n";
		output << "  \"validity_errors\": [";
		for (idx_t i = 0; i < summary.validity_errors.size(); i++) {
			if (i > 0) {
				output << ", ";
			}
			output << JSONString(summary.validity_errors[i]);
		}
		output << "]\n}\n";
	}
}

} // namespace scheduler_workload
} // namespace duckdb
