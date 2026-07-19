#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"

#include <iostream>

using namespace duckdb;
using namespace duckdb::scheduler_workload;

int main(int argc, char **argv) {
	try {
		if (argc == 2 && (string(argv[1]) == "--help" || string(argv[1]) == "-h")) {
			std::cout << Usage();
			return 0;
		}
		auto config = ParseConfig(argc, argv);
		switch (config.command) {
		case WorkloadCommand::CALIBRATE: {
			auto calibration = Calibrate(config);
			WriteCalibration(config.output_path, calibration);
			std::cout << "calibration: " << config.output_path << "\n";
			std::cout << "weighted_mean_duration_s: " << WeightedMeanDuration(config, calibration) << "\n";
			return 0;
		}
		case WorkloadCommand::TRAIN_PROFILES:
			TrainProfiles(config);
			std::cout << "profile_snapshot: " << config.profile_snapshot_out << "\n";
			return 0;
		case WorkloadCommand::GENERATE: {
			auto calibration = ReadCalibration(config.calibration_path);
			auto schedule = GenerateSchedule(config, calibration);
			WriteSchedule(config.output_path, schedule);
			std::cout << "schedule: " << config.output_path << "\n";
			std::cout << "schedule_hash: " << ScheduleHash(schedule) << "\n";
			std::cout << "request_count: " << schedule.size() << "\n";
			std::cout << "weighted_mean_duration_s: " << WeightedMeanDuration(config, calibration) << "\n";
			std::cout << "arrival_rate_qps: "
			          << config.load_factor / WeightedMeanDuration(config, calibration) << "\n";
			return 0;
		}
		case WorkloadCommand::RUN: {
			auto schedule = ReadSchedule(config.schedule_path);
			vector<RequestResult> results;
			auto summary = RunWorkload(config, schedule, results);
			WriteRunOutputs(config, summary, results);
			std::cout << "schedule_hash: " << summary.schedule_hash << "\n";
			std::cout << "completed: " << summary.completed_count << '/' << summary.request_count << "\n";
			std::cout << "makespan_s: " << summary.makespan_s << "\n";
			std::cout << "throughput_qps: " << summary.throughput_qps << "\n";
			std::cout << "total_sla_cost: " << summary.total_sla_cost << "\n";
			std::cout << "p99_submission_lag_ms: " << summary.p99_submission_lag_ms << "\n";
			std::cout << "p99_admission_resume_lag_ms: " << summary.p99_admission_resume_lag_ms << "\n";
			std::cout << "valid: " << (summary.valid ? "true" : "false") << "\n";
			return !summary.valid && config.fail_on_invalid ? 2 : 0;
		}
		default:
			throw InvalidInputException("No workload command selected");
		}
	} catch (std::exception &ex) {
		std::cerr << "scheduler_workload: " << ex.what() << '\n';
		return 1;
	}
}
