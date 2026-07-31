//===----------------------------------------------------------------------===//
//                         DuckDB
//
// energy_attribution_base_power.cpp
//
//===----------------------------------------------------------------------===//

#include "energy_attribution_base_power.hpp"

#include "duckdb/common/string_util.hpp"

#include <cmath>
#include <fstream>
#include <limits>
#include <mutex>
#include <unordered_map>

namespace duckdb {
namespace energy_attribution {

namespace {

mutex base_power_cache_lock;
unordered_map<string, vector<BasePowerEntry>> base_power_cache;

static string TrimCopy(string input) {
	StringUtil::Trim(input);
	return input;
}

static vector<string> SplitCSVLine(const string &line) {
	vector<string> result;
	string current;
	bool in_quotes = false;
	for (idx_t i = 0; i < line.size(); i++) {
		auto c = line[i];
		if (c == '"') {
			if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
				current += '"';
				i++;
			} else {
				in_quotes = !in_quotes;
			}
		} else if (c == ',' && !in_quotes) {
			result.push_back(TrimCopy(current));
			current.clear();
		} else {
			current += c;
		}
	}
	result.push_back(TrimCopy(current));
	return result;
}

static double ParseDoubleOrZero(const string &value) {
	if (value.empty()) {
		return 0;
	}
	try {
		return std::stod(value);
	} catch (...) {
		return 0;
	}
}

static int ParseIntOrDefault(const string &value, int default_value) {
	if (value.empty()) {
		return default_value;
	}
	try {
		return NumericCast<int>(StringUtil::ToSigned(value));
	} catch (...) {
		return default_value;
	}
}

static bool LoadBasePowerCSV(const string &path, vector<BasePowerEntry> &entries) {
	if (path.empty()) {
		return false;
	}
	std::ifstream input(path.c_str());
	if (!input.good()) {
		return false;
	}
	string line;
	bool first = true;
	while (std::getline(input, line)) {
		line = TrimCopy(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		auto fields = SplitCSVLine(line);
		if (first && !fields.empty() && StringUtil::Lower(fields[0]) == "socket_id") {
			first = false;
			continue;
		}
		first = false;
		if (fields.size() < 4) {
			continue;
		}
		BasePowerEntry entry;
		entry.socket_id = ParseIntOrDefault(fields[0], -1);
		if (fields.size() >= 5) {
			entry.core_freq_hz = ParseDoubleOrZero(fields[1]);
			entry.uncore_freq_hz = ParseDoubleOrZero(fields[2]);
			entry.smt_state = ParseIntOrDefault(fields[3], 0);
			entry.base_power_w = ParseDoubleOrZero(fields[4]);
		} else {
			entry.uncore_freq_hz = ParseDoubleOrZero(fields[1]);
			entry.base_power_w = ParseDoubleOrZero(fields[2]);
		}
		if (entry.socket_id >= 0 && entry.base_power_w >= 0) {
			entries.push_back(entry);
		}
	}
	return !entries.empty();
}

static double FrequencyDistance(double lhs, double rhs) {
	if (lhs <= 0 || rhs <= 0) {
		return 0;
	}
	return std::abs(lhs - rhs) / std::max(lhs, rhs);
}

} // namespace

bool LoadBasePowerCSVCached(const string &path, vector<BasePowerEntry> &entries) {
	entries.clear();
	if (path.empty()) {
		return false;
	}
	{
		lock_guard<mutex> guard(base_power_cache_lock);
		auto entry = base_power_cache.find(path);
		if (entry != base_power_cache.end()) {
			entries = entry->second;
			return !entries.empty();
		}
	}
	vector<BasePowerEntry> loaded;
	if (!LoadBasePowerCSV(path, loaded)) {
		return false;
	}
	{
		lock_guard<mutex> guard(base_power_cache_lock);
		base_power_cache[path] = loaded;
	}
	entries = std::move(loaded);
	return !entries.empty();
}

bool LookupBasePower(const vector<BasePowerEntry> &entries, int socket_id, double core_freq_hz,
                     double uncore_freq_hz, int smt_state, double &base_power_w) {
	double best_score = std::numeric_limits<double>::max();
	bool found = false;
	for (const auto &entry : entries) {
		if (entry.socket_id != socket_id) {
			continue;
		}
		double score = FrequencyDistance(entry.core_freq_hz, core_freq_hz) +
		               FrequencyDistance(entry.uncore_freq_hz, uncore_freq_hz);
		if (entry.smt_state > 0 && smt_state > 0 && entry.smt_state != smt_state) {
			score += 1.0;
		}
		if (score < best_score) {
			best_score = score;
			base_power_w = entry.base_power_w;
			found = true;
		}
	}
	return found;
}

} // namespace energy_attribution
} // namespace duckdb
