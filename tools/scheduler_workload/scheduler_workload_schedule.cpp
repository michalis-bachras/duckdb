#include "scheduler_workload.hpp"

#include "duckdb/common/exception.hpp"
#include "mbedtls_wrapper.hpp"

#include <cmath>
#include <fstream>
#include <iomanip>
#include <random>
#include <set>
#include <sstream>

namespace duckdb {
namespace scheduler_workload {

namespace {

typedef std::pair<uint64_t, uint64_t> CalibrationKey;

static vector<string> SplitCSV(const string &line) {
	vector<string> result;
	std::stringstream stream(line);
	string value;
	while (std::getline(stream, value, ',')) {
		result.push_back(value);
	}
	return result;
}

static uint64_t ParseUint(const string &value, const char *field) {
	char *end = nullptr;
	auto result = std::strtoull(value.c_str(), &end, 10);
	if (!end || *end != '\0') {
		throw InvalidInputException("Invalid %s in CSV: %s", field, value);
	}
	return result;
}

static double ParseDouble(const string &value, const char *field) {
	char *end = nullptr;
	auto result = std::strtod(value.c_str(), &end);
	if (!end || *end != '\0' || !std::isfinite(result)) {
		throw InvalidInputException("Invalid %s in CSV: %s", field, value);
	}
	return result;
}

static std::map<CalibrationKey, CalibrationEntry> CalibrationMap(const vector<CalibrationEntry> &entries) {
	std::map<CalibrationKey, CalibrationEntry> result;
	for (const auto &entry : entries) {
		CalibrationKey key(entry.template_id, entry.scale_factor);
		if (entry.template_id == 0 || entry.scale_factor == 0 || entry.sample_count == 0 ||
		    !std::isfinite(entry.mean_runtime_s) || entry.mean_runtime_s <= 0 ||
		    !result.emplace(key, entry).second) {
			throw InvalidInputException("Invalid or duplicate calibration entry for Q%llu SF%llu",
			                            entry.template_id, entry.scale_factor);
		}
	}
	return result;
}

static string CanonicalSchedule(const vector<ScheduleEntry> &schedule) {
	std::ostringstream out;
	out << "scheduler_workload_schedule_v1\n";
	for (const auto &entry : schedule) {
		out << entry.request_id << ',' << entry.template_id << ',' << entry.scale_factor << ','
		    << entry.scheduled_offset_ns << ',' << entry.deadline_offset_ns << ',' << std::setprecision(17)
		    << entry.sla_tag << ',' << entry.sla_penalty_per_s << '\n';
	}
	return out.str();
}

} // namespace

vector<CalibrationEntry> ReadCalibration(const string &path) {
	std::ifstream input(path.c_str());
	if (!input) {
		throw IOException("Could not open calibration file: %s", path);
	}
	string line;
	if (!std::getline(input, line) || line != "template_id,scale_factor,sample_count,mean_runtime_s") {
		throw InvalidInputException("Unsupported calibration CSV header in %s", path);
	}
	vector<CalibrationEntry> result;
	while (std::getline(input, line)) {
		if (line.empty()) {
			continue;
		}
		auto fields = SplitCSV(line);
		if (fields.size() != 4) {
			throw InvalidInputException("Malformed calibration CSV row: %s", line);
		}
		CalibrationEntry entry;
		entry.template_id = ParseUint(fields[0], "template_id");
		entry.scale_factor = ParseUint(fields[1], "scale_factor");
		entry.sample_count = ParseUint(fields[2], "sample_count");
		entry.mean_runtime_s = ParseDouble(fields[3], "mean_runtime_s");
		result.push_back(entry);
	}
	CalibrationMap(result);
	return result;
}

void WriteCalibration(const string &path, const vector<CalibrationEntry> &entries) {
	std::ofstream output(path.c_str(), std::ios::trunc);
	if (!output) {
		throw IOException("Could not create calibration file: %s", path);
	}
	output << "template_id,scale_factor,sample_count,mean_runtime_s\n";
	output << std::setprecision(17);
	for (const auto &entry : entries) {
		output << entry.template_id << ',' << entry.scale_factor << ',' << entry.sample_count << ','
		       << entry.mean_runtime_s << '\n';
	}
}

double WeightedMeanDuration(const WorkloadConfig &config, const vector<CalibrationEntry> &calibration) {
	auto entries = CalibrationMap(calibration);
	double total_weight = 0;
	double weighted_duration = 0;
	for (const auto &database : config.databases) {
		for (const auto query : config.query_templates) {
			auto entry = entries.find(CalibrationKey(query, database.scale_factor));
			if (entry == entries.end()) {
				throw InvalidInputException("Calibration is missing Q%llu SF%llu", query, database.scale_factor);
			}
			weighted_duration += database.weight * entry->second.mean_runtime_s;
			total_weight += database.weight;
		}
	}
	if (total_weight <= 0) {
		throw InvalidInputException("Cannot calculate weighted mean duration from an empty workload");
	}
	return weighted_duration / total_weight;
}

vector<ScheduleEntry> GenerateSchedule(const WorkloadConfig &config,
                                       const vector<CalibrationEntry> &calibration) {
	auto calibration_map = CalibrationMap(calibration);
	auto mean_duration_s = WeightedMeanDuration(config, calibration);
	auto lambda = config.load_factor / mean_duration_s;
	if (!std::isfinite(lambda) || lambda <= 0) {
		throw InvalidInputException("Calculated arrival rate is invalid");
	}
	vector<double> scale_weights;
	for (const auto &database : config.databases) {
		scale_weights.push_back(database.weight);
	}
	std::mt19937_64 random(config.seed);
	std::discrete_distribution<idx_t> choose_scale(scale_weights.begin(), scale_weights.end());
	std::uniform_int_distribution<idx_t> choose_query(0, config.query_templates.size() - 1);
	std::exponential_distribution<double> interarrival(lambda);

	vector<ScheduleEntry> result;
	double scheduled_s = 0;
	uint64_t request_id = 0;
	while (scheduled_s < config.duration_s) {
		auto &database = config.databases[choose_scale(random)];
		auto query = config.query_templates[choose_query(random)];
		auto calibration_entry = calibration_map.find(CalibrationKey(query, database.scale_factor));
		D_ASSERT(calibration_entry != calibration_map.end());
		ScheduleEntry entry;
		entry.request_id = request_id++;
		entry.template_id = query;
		entry.scale_factor = database.scale_factor;
		entry.scheduled_offset_ns = static_cast<uint64_t>(std::llround(scheduled_s * 1000000000.0));
		auto deadline_duration_s = config.sla_tag * calibration_entry->second.mean_runtime_s;
		entry.deadline_offset_ns = entry.scheduled_offset_ns +
		                           static_cast<uint64_t>(std::llround(deadline_duration_s * 1000000000.0));
		entry.sla_tag = config.sla_tag;
		entry.sla_penalty_per_s = config.sla_penalty_per_s;
		result.push_back(entry);
		scheduled_s += interarrival(random);
	}
	return result;
}

void WriteSchedule(const string &path, const vector<ScheduleEntry> &schedule) {
	std::ofstream output(path.c_str(), std::ios::trunc);
	if (!output) {
		throw IOException("Could not create schedule file: %s", path);
	}
	output << "request_id,template_id,scale_factor,scheduled_offset_ns,deadline_offset_ns,sla_tag,"
	          "sla_penalty_per_s\n";
	output << std::setprecision(17);
	for (const auto &entry : schedule) {
		output << entry.request_id << ',' << entry.template_id << ',' << entry.scale_factor << ','
		       << entry.scheduled_offset_ns << ',' << entry.deadline_offset_ns << ',' << entry.sla_tag << ','
		       << entry.sla_penalty_per_s << '\n';
	}
}

vector<ScheduleEntry> ReadSchedule(const string &path) {
	std::ifstream input(path.c_str());
	if (!input) {
		throw IOException("Could not open schedule file: %s", path);
	}
	string line;
	if (!std::getline(input, line) ||
	    line != "request_id,template_id,scale_factor,scheduled_offset_ns,deadline_offset_ns,sla_tag,"
	            "sla_penalty_per_s") {
		throw InvalidInputException("Unsupported schedule CSV header in %s", path);
	}
	vector<ScheduleEntry> result;
	std::set<uint64_t> request_ids;
	uint64_t previous_offset = 0;
	while (std::getline(input, line)) {
		if (line.empty()) {
			continue;
		}
		auto fields = SplitCSV(line);
		if (fields.size() != 7) {
			throw InvalidInputException("Malformed schedule CSV row: %s", line);
		}
		ScheduleEntry entry;
		entry.request_id = ParseUint(fields[0], "request_id");
		entry.template_id = ParseUint(fields[1], "template_id");
		entry.scale_factor = ParseUint(fields[2], "scale_factor");
		entry.scheduled_offset_ns = ParseUint(fields[3], "scheduled_offset_ns");
		entry.deadline_offset_ns = ParseUint(fields[4], "deadline_offset_ns");
		entry.sla_tag = ParseDouble(fields[5], "sla_tag");
		entry.sla_penalty_per_s = ParseDouble(fields[6], "sla_penalty_per_s");
		if (entry.template_id < 1 || entry.template_id > 22 || entry.scale_factor == 0 ||
		    entry.deadline_offset_ns <= entry.scheduled_offset_ns || entry.sla_tag <= 0 ||
		    entry.sla_penalty_per_s < 0 ||
		    (!result.empty() && entry.scheduled_offset_ns < previous_offset) ||
		    !request_ids.insert(entry.request_id).second) {
			throw InvalidInputException("Invalid schedule row: %s", line);
		}
		previous_offset = entry.scheduled_offset_ns;
		result.push_back(entry);
	}
	if (result.empty()) {
		throw InvalidInputException("Schedule contains no requests");
	}
	return result;
}

string ScheduleHash(const vector<ScheduleEntry> &schedule) {
	auto binary = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(CanonicalSchedule(schedule));
	static const char *digits = "0123456789abcdef";
	string result;
	result.reserve(binary.size() * 2);
	for (const auto value : binary) {
		auto byte = static_cast<uint8_t>(value);
		result.push_back(digits[byte >> 4]);
		result.push_back(digits[byte & 0xf]);
	}
	return result;
}

} // namespace scheduler_workload
} // namespace duckdb
