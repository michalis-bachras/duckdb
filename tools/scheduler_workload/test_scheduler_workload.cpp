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

		const char *arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla_energy",
		    "--energy-power-model", "/tmp/power.csv", "--energy-hardware-control", "off",
		    "--energy-exploration", "on", "--energy-exploration-seed", "17",
		    "--energy-attribution-period-ms", "50",
		    "--energy-attribution-export", "off", "--energy-attribution-debug-export", "on"};
		auto parsed = ParseConfig(sizeof(arguments) / sizeof(arguments[0]), const_cast<char **>(arguments));
		Require(parsed.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY, "SLA-energy scheduler parsing");
		Require(!parsed.sla_energy_hardware_control, "dry-run hardware mode parsing");
		Require(parsed.sla_energy_exploration, "exploration mode parsing");
		Require(parsed.sla_energy_exploration_seed == 17, "exploration seed parsing");
		Require(!parsed.energy_attribution_enabled, "attribution is explicit and defaults off");
		Require(parsed.energy_attribution_period_ms == 50, "attribution period parsing");
		Require(!parsed.energy_attribution_export, "attribution export parsing");
		Require(parsed.energy_attribution_debug_export, "attribution debug export parsing");

		const char *sla_attribution_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla",
		    "--energy-power-model", "/tmp/power.csv", "--energy-attribution", "on"};
		auto sla_attribution = ParseConfig(sizeof(sla_attribution_arguments) / sizeof(sla_attribution_arguments[0]),
		                                   const_cast<char **>(sla_attribution_arguments));
		Require(sla_attribution.scheduler_policy == QuerySchedulerPolicy::SLA,
		        "SLA baseline scheduler parsing");
		Require(sla_attribution.energy_attribution_enabled, "policy-independent attribution parsing");
		Require(sla_attribution.energy_power_model_path == "/tmp/power.csv", "attribution power model parsing");

		const char *unsafe_dry_run_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla_energy",
		    "--energy-power-model", "/tmp/power.csv", "--energy-hardware-control", "off",
		    "--energy-attribution", "on"};
		bool rejected_unsafe_dry_run = false;
		try {
			ParseConfig(sizeof(unsafe_dry_run_arguments) / sizeof(unsafe_dry_run_arguments[0]),
			            const_cast<char **>(unsafe_dry_run_arguments));
		} catch (InvalidInputException &) {
			rejected_unsafe_dry_run = true;
		}
		Require(rejected_unsafe_dry_run, "attribution rejects logical-only SLA-energy hardware targets");
		std::cout << "scheduler_workload tests passed\n";
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
