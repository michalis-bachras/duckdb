#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"

#include <cmath>
#include <cstdio>
#include <iostream>

using namespace duckdb;
using namespace duckdb::scheduler_workload;

static void Require(bool condition, const string &message) {
	if (!condition) {
		throw InternalException("scheduler_workload test failed: %s", message);
	}
}

int main() {
	try {
		WorkloadConfig config;
		config.command = WorkloadCommand::GENERATE;
		config.seed = 42;
		config.duration_s = 1000;
		config.load_factor = 0.95;
		config.sla_tag = 30;
		config.query_templates = {1, 2, 3, 4};
		ScaleDatabase sf10;
		sf10.scale_factor = 10;
		sf10.weight = 3;
		ScaleDatabase sf100;
		sf100.scale_factor = 100;
		sf100.weight = 1;
		config.databases = {sf10, sf100};
		vector<CalibrationEntry> calibration;
		for (const auto &database : config.databases) {
			for (const auto query : config.query_templates) {
				CalibrationEntry entry;
				entry.template_id = query;
				entry.scale_factor = database.scale_factor;
				entry.sample_count = 3;
				entry.mean_runtime_s = database.scale_factor == 10 ? 0.1 : 0.9;
				calibration.push_back(entry);
			}
		}
		auto first = GenerateSchedule(config, calibration);
		auto second = GenerateSchedule(config, calibration);
		Require(ScheduleHash(first) == ScheduleHash(second), "same seed must produce the same schedule");
		Require(first.size() == second.size() && first.size() > 1000, "deterministic schedule size");
		idx_t sf10_count = 0;
		for (const auto &entry : first) {
			sf10_count += entry.scale_factor == 10 ? 1 : 0;
			Require(entry.deadline_offset_ns > entry.scheduled_offset_ns, "deadline follows arrival");
			Require(entry.sla_tag == config.sla_tag, "SLA tag is part of the immutable schedule");
		}
		auto sf10_fraction = static_cast<double>(sf10_count) / first.size();
		Require(std::fabs(sf10_fraction - 0.75) < 0.03, "3:1 scale-factor distribution");
		auto mean_duration = WeightedMeanDuration(config, calibration);
		Require(std::fabs(mean_duration - 0.3) < 1e-12, "weighted mean duration");
		auto realized_lambda = static_cast<double>(first.size()) / config.duration_s;
		auto configured_lambda = config.load_factor / mean_duration;
		Require(std::fabs(realized_lambda - configured_lambda) / configured_lambda < 0.05,
		        "exponential arrivals converge to configured lambda");

		const string path = "/tmp/duckdb_scheduler_workload_schedule_test.csv";
		WriteSchedule(path, first);
		auto round_trip = ReadSchedule(path);
		std::remove(path.c_str());
		Require(ScheduleHash(first) == ScheduleHash(round_trip), "schedule CSV round trip");
		std::cout << "scheduler_workload tests passed\n";
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
