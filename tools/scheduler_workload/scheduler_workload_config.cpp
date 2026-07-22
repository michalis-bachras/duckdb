#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cmath>
#include <cstdlib>
#include <set>
#include <sstream>
#include <thread>

namespace duckdb {
namespace scheduler_workload {

namespace {

static vector<string> Split(const string &value) {
	vector<string> result;
	std::stringstream stream(value);
	string item;
	while (std::getline(stream, item, ',')) {
		StringUtil::Trim(item);
		if (!item.empty()) {
			result.push_back(item);
		}
	}
	return result;
}

static uint64_t ParseUint(const string &value, const string &name) {
	char *end = nullptr;
	auto parsed = std::strtoull(value.c_str(), &end, 10);
	if (!end || *end != '\0') {
		throw InvalidInputException("Invalid unsigned integer for --%s: %s", name, value);
	}
	return parsed;
}

static double ParseDouble(const string &value, const string &name) {
	char *end = nullptr;
	auto parsed = std::strtod(value.c_str(), &end);
	if (!end || *end != '\0' || !std::isfinite(parsed)) {
		throw InvalidInputException("Invalid number for --%s: %s", name, value);
	}
	return parsed;
}

static bool ParseBool(const string &value, const string &name) {
	auto lower = StringUtil::Lower(value);
	if (lower == "on" || lower == "true" || lower == "1") {
		return true;
	}
	if (lower == "off" || lower == "false" || lower == "0") {
		return false;
	}
	throw InvalidInputException("--%s must be on or off", name);
}

static QuerySchedulerPolicy ParseScheduler(const string &value) {
	auto lower = StringUtil::Lower(value);
	if (lower == "default") {
		return QuerySchedulerPolicy::DEFAULT;
	}
	if (lower == "stride") {
		return QuerySchedulerPolicy::STRIDE;
	}
	if (lower == "sla") {
		return QuerySchedulerPolicy::SLA;
	}
	if (lower == "sla_energy") {
		return QuerySchedulerPolicy::SLA_ENERGY;
	}
	throw InvalidInputException("--scheduler must be default, stride, sla, or sla_energy");
}

static WorkloadCommand ParseCommand(const string &value) {
	if (value == "calibrate") {
		return WorkloadCommand::CALIBRATE;
	}
	if (value == "train-profiles") {
		return WorkloadCommand::TRAIN_PROFILES;
	}
	if (value == "generate") {
		return WorkloadCommand::GENERATE;
	}
	if (value == "run") {
		return WorkloadCommand::RUN;
	}
	throw InvalidInputException("Unknown scheduler_workload command: %s", value);
}

static void ValidateConfig(WorkloadConfig &config) {
	if (config.query_templates.empty()) {
		for (uint64_t query = 1; query <= 22; query++) {
			config.query_templates.push_back(query);
		}
	}
	std::set<uint64_t> queries(config.query_templates.begin(), config.query_templates.end());
	if (queries.size() != config.query_templates.size()) {
		throw InvalidInputException("--queries contains duplicate templates");
	}
	std::set<uint64_t> trace_queries(config.scheduler_trace_templates.begin(),
	                                 config.scheduler_trace_templates.end());
	if (trace_queries.size() != config.scheduler_trace_templates.size()) {
		throw InvalidInputException("--scheduler-trace-queries contains duplicate templates");
	}
	for (const auto query : config.scheduler_trace_templates) {
		if (query < 1 || query > 22) {
			throw InvalidInputException("Scheduler trace query templates must be in [1, 22]");
		}
	}
	if (!config.scheduler_trace_templates.empty() &&
	    (config.command != WorkloadCommand::RUN || !IsQuerySLAPolicy(config.scheduler_policy))) {
		throw InvalidInputException("--scheduler-trace-queries is only valid for an SLA-policy run");
	}
	std::set<uint64_t> scale_factors;
	for (const auto &database : config.databases) {
		if (database.scale_factor == 0 || !scale_factors.insert(database.scale_factor).second) {
			throw InvalidInputException("Scale factors must be positive and unique");
		}
	}
	if (config.threads == 0 || config.submitter_threads == 0 || config.completion_threads == 0) {
		throw InvalidInputException("Worker, submitter, and completion thread counts must be positive");
	}
	if (config.admission_cap == 0) {
		throw InvalidInputException("--admission-cap must be positive");
	}
	if (config.repetitions == 0) {
		throw InvalidInputException("--repetitions must be positive");
	}
	if (config.sla_tag <= 0 || config.sla_penalty_per_s < 0 || config.max_driver_lag_ms < 0) {
		throw InvalidInputException("SLA tag must be positive; penalty and driver-lag limit must be non-negative");
	}
	if (config.command == WorkloadCommand::CALIBRATE || config.command == WorkloadCommand::TRAIN_PROFILES ||
	    config.command == WorkloadCommand::RUN) {
		if (config.databases.empty() || config.queries_dir.empty()) {
			throw InvalidInputException("This command requires --database SF=PATH and --queries-dir PATH");
		}
	}
	if (config.command == WorkloadCommand::CALIBRATE && config.output_path.empty()) {
		throw InvalidInputException("calibrate requires --output PATH");
	}
	if (config.command == WorkloadCommand::TRAIN_PROFILES && config.profile_snapshot_out.empty()) {
		throw InvalidInputException("train-profiles requires --profile-output PATH");
	}
	if (config.command == WorkloadCommand::GENERATE) {
		if (config.calibration_path.empty() || config.output_path.empty() || config.databases.empty()) {
			throw InvalidInputException("generate requires --calibration PATH, --database SF=PATH, and --output PATH");
		}
		if (config.duration_s <= 0 || config.load_factor <= 0) {
			throw InvalidInputException("--duration-s and --load-factor must be positive");
		}
	}
	if (config.command != WorkloadCommand::GENERATE) {
		for (const auto &database : config.databases) {
			if (database.path.empty()) {
				throw InvalidInputException("--scale-factors is only valid for generate; use --database SF=PATH here");
			}
		}
	}
	if (config.command == WorkloadCommand::RUN &&
	    (config.schedule_path.empty() || config.output_dir.empty())) {
		throw InvalidInputException("run requires --schedule PATH and --output-dir PATH");
	}
	if (config.scheduler_policy == QuerySchedulerPolicy::STRIDE && config.admission_cap != 128) {
		throw InvalidInputException("STRIDE requires --admission-cap 128");
	}
	if (config.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY) {
		if (config.energy_power_model_path.empty()) {
			throw InvalidInputException("SLA-energy runs require --energy-power-model");
		}
		if (!std::isfinite(config.sla_energy_lambda) || config.sla_energy_lambda < 0) {
			throw InvalidInputException("--energy-lambda must be finite and non-negative");
		}
		if (config.energy_attribution_enabled && !config.sla_energy_hardware_control) {
			throw InvalidInputException(
			    "SLA-energy attribution requires real hardware control; disable attribution for dry-run targets");
		}
	}
	if (config.energy_attribution_enabled) {
		if (config.energy_power_model_path.empty()) {
			throw InvalidInputException("Energy attribution requires --energy-power-model");
		}
		if (config.energy_attribution_period_ms == 0) {
			throw InvalidInputException("--energy-attribution-period-ms must be positive");
		}
	}
}

} // namespace

string SchedulerName(QuerySchedulerPolicy policy) {
	switch (policy) {
	case QuerySchedulerPolicy::SLA:
		return "sla";
	case QuerySchedulerPolicy::SLA_ENERGY:
		return "sla_energy";
	case QuerySchedulerPolicy::STRIDE:
		return "stride";
	default:
		return "default";
	}
}

string Usage() {
	return R"USAGE(scheduler_workload COMMAND [OPTIONS]

Commands:
  calibrate       Measure isolated template runtimes under the common execution environment.
  train-profiles  Execute every selected template under DEFAULT and export a scheduler profile snapshot.
  generate        Generate a deterministic paper-style Poisson schedule using lambda = alpha / mean_duration.
	  run             Execute one saved schedule with DEFAULT, STRIDE, SLA, or SLA-energy.

Core options:
  --database SF=PATH          Repeat for each scale factor (for example 10=db/sf10.duckdb).
  --scale-factors SF1,SF2     Scale factors for generate, where database paths are not needed.
  --scale-weights W1,W2       Selection weights in --database order (default: equal).
  --queries-dir PATH          Directory containing tpch-q1.sql through tpch-q22.sql.
  --queries all|Q1,Q2,...     Query templates (default: all).
  --threads N                 DuckDB hardware worker threads (default: hardware concurrency).
  --admission-cap N           Database admission cap (default: 128).
  --repetitions N             Calibration/training repetitions (default: 3).

Schedule options:
  --calibration PATH          Calibration CSV produced by calibrate.
  --load-factor ALPHA         Target offered load alpha (default: 0.95).
  --duration-s SECONDS        Arrival-generation duration (default: 300).
  --seed N                    Deterministic schedule seed.
  --sla-tag X                 Deadline multiplier over isolated mean (default: 30).
  --output PATH               Calibration or schedule output path.

Run options:
  --schedule PATH             Saved schedule CSV.
	  --scheduler default|stride|sla|sla_energy
  --profile-input PATH        Optional scheduler profile snapshot.
  --profile-output PATH       Export updated profile state after training/run.
  --output-dir PATH           Run timeline and summary directory.
  --submitter-threads N       Fixed native submission pool size (default: 2).
  --completion-threads N      Fixed event/completion pool size (default: 2).
  --max-driver-lag-ms X       p99 arrival, DB submission, and admission-resume gate (default: 10).
	  --scheduler-trace-queries Q2,Q9,...
	                              Enable debug epoch tracing only for these templates.
	  --sla-residual-policy work_conserving|park
	                              SLA-only zero-gain residual-worker policy (default: work_conserving).
	  --energy-power-model PATH  Calibrated socket/core power table for SLA-energy and attribution.
	  --energy-lambda X          Energy weight for optional-worker utility (default: 1).
	  --energy-exploration on|off
	                              Enable epoch-scoped warmup exploration (default: off).
	  --energy-exploration-seed N
	                              Deterministic warmup exploration seed (default: 1).
	  --energy-hardware-control on|off
	                              Direct MSR control or dry-run publication (default: on).
	  --energy-attribution on|off
	                              Enable periodic RAPL attribution independently of policy (default: off).
	  --energy-attribution-period-ms N
	                              Periodic RAPL attribution interval (default: 100).
	  --energy-attribution-export on|off
	                              Export aggregate attribution diagnostics (default: on).
	  --energy-attribution-debug-export on|off
	                              Export per-window attribution diagnostics (default: off).
  --allow-invalid             Write diagnostics but return success for invalid runs.
)USAGE";
}

WorkloadConfig ParseConfig(int argc, char **argv) {
	if (argc < 2) {
		throw InvalidInputException("%s", Usage());
	}
	WorkloadConfig config;
	config.command = ParseCommand(argv[1]);
	config.threads = MaxValue<idx_t>(1, std::thread::hardware_concurrency());
	vector<double> scale_weights;
	for (int i = 2; i < argc; i++) {
		string argument = argv[i];
		if (argument == "--help" || argument == "-h") {
			throw InvalidInputException("%s", Usage());
		}
		if (!StringUtil::StartsWith(argument, "--")) {
			throw InvalidInputException("Unexpected positional argument: %s", argument);
		}
		auto name = argument.substr(2);
		string value;
		auto equals = name.find('=');
		if (equals != string::npos) {
			value = name.substr(equals + 1);
			name.resize(equals);
		} else if (name != "allow-invalid") {
			if (++i >= argc) {
				throw InvalidInputException("Missing value for --%s", name);
			}
			value = argv[i];
		}

		if (name == "database") {
			auto separator = value.find('=');
			if (separator == string::npos) {
				throw InvalidInputException("--database must use SF=PATH");
			}
			ScaleDatabase database;
			auto scale = value.substr(0, separator);
			if (StringUtil::StartsWith(StringUtil::Lower(scale), "sf")) {
				scale = scale.substr(2);
			}
			database.scale_factor = ParseUint(scale, name);
			database.path = value.substr(separator + 1);
			config.databases.push_back(std::move(database));
		} else if (name == "scale-factors") {
			for (const auto &item : Split(value)) {
				ScaleDatabase database;
				database.scale_factor = ParseUint(item, name);
				config.databases.push_back(std::move(database));
			}
		} else if (name == "scale-weights") {
			for (const auto &item : Split(value)) {
				scale_weights.push_back(ParseDouble(item, name));
			}
		} else if (name == "queries") {
			if (StringUtil::Lower(value) != "all") {
				for (auto item : Split(value)) {
					if (!item.empty() && (item[0] == 'q' || item[0] == 'Q')) {
						item = item.substr(1);
					}
					auto query = ParseUint(item, name);
					if (query < 1 || query > 22) {
						throw InvalidInputException("TPC-H query template must be in [1, 22]");
					}
					config.query_templates.push_back(query);
				}
			}
		} else if (name == "scheduler-trace-queries") {
			for (auto item : Split(value)) {
				if (!item.empty() && (item[0] == 'q' || item[0] == 'Q')) {
					item = item.substr(1);
				}
				config.scheduler_trace_templates.push_back(ParseUint(item, name));
			}
		} else if (name == "queries-dir") {
			config.queries_dir = value;
		} else if (name == "calibration") {
			config.calibration_path = value;
		} else if (name == "schedule") {
			config.schedule_path = value;
		} else if (name == "output") {
			config.output_path = value;
		} else if (name == "output-dir") {
			config.output_dir = value;
		} else if (name == "profile-input") {
			config.profile_snapshot_in = value;
		} else if (name == "profile-output") {
			config.profile_snapshot_out = value;
		} else if (name == "scheduler") {
			config.scheduler_policy = ParseScheduler(value);
		} else if (name == "threads") {
			config.threads = ParseUint(value, name);
		} else if (name == "submitter-threads") {
			config.submitter_threads = ParseUint(value, name);
		} else if (name == "completion-threads") {
			config.completion_threads = ParseUint(value, name);
		} else if (name == "admission-cap") {
			config.admission_cap = ParseUint(value, name);
		} else if (name == "repetitions") {
			config.repetitions = ParseUint(value, name);
		} else if (name == "seed") {
			config.seed = ParseUint(value, name);
		} else if (name == "duration-s") {
			config.duration_s = ParseDouble(value, name);
		} else if (name == "load-factor") {
			config.load_factor = ParseDouble(value, name);
		} else if (name == "sla-tag") {
			config.sla_tag = ParseDouble(value, name);
		} else if (name == "sla-penalty-per-s") {
			config.sla_penalty_per_s = ParseDouble(value, name);
		} else if (name == "max-driver-lag-ms") {
			config.max_driver_lag_ms = ParseDouble(value, name);
		} else if (name == "sla-residual-policy") {
			auto policy = StringUtil::Lower(value);
			if (policy == "work_conserving") {
				config.sla_residual_workers_enabled = true;
			} else if (policy == "park") {
				config.sla_residual_workers_enabled = false;
			} else {
				throw InvalidInputException("--sla-residual-policy must be one of: work_conserving, park");
			}
		} else if (name == "energy-power-model") {
			config.energy_power_model_path = value;
		} else if (name == "energy-lambda") {
			config.sla_energy_lambda = ParseDouble(value, name);
		} else if (name == "energy-exploration") {
			config.sla_energy_exploration = ParseBool(value, name);
		} else if (name == "energy-exploration-seed") {
			config.sla_energy_exploration_seed = ParseUint(value, name);
		} else if (name == "energy-hardware-control") {
			config.sla_energy_hardware_control = ParseBool(value, name);
		} else if (name == "energy-attribution") {
			config.energy_attribution_enabled = ParseBool(value, name);
		} else if (name == "energy-attribution-period-ms") {
			config.energy_attribution_period_ms = ParseUint(value, name);
		} else if (name == "energy-attribution-export") {
			config.energy_attribution_export = ParseBool(value, name);
		} else if (name == "energy-attribution-debug-export") {
			config.energy_attribution_debug_export = ParseBool(value, name);
		} else if (name == "allow-invalid") {
			config.fail_on_invalid = false;
		} else {
			throw InvalidInputException("Unknown option --%s", name);
		}
	}
	if (!scale_weights.empty()) {
		if (scale_weights.size() != config.databases.size()) {
			throw InvalidInputException("--scale-weights count must match --database count");
		}
		for (idx_t i = 0; i < scale_weights.size(); i++) {
			if (scale_weights[i] <= 0) {
				throw InvalidInputException("Scale weights must be positive");
			}
			config.databases[i].weight = scale_weights[i];
		}
	}
	ValidateConfig(config);
	return config;
}

} // namespace scheduler_workload
} // namespace duckdb
