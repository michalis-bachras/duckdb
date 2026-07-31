//===----------------------------------------------------------------------===//
//                         DuckDB
//
// energy_attribution_base_power.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
namespace energy_attribution {

struct BasePowerEntry {
	int socket_id = -1;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_state = 0;
	double base_power_w = 0;
};

bool LoadBasePowerCSVCached(const string &path, vector<BasePowerEntry> &entries);
bool LookupBasePower(const vector<BasePowerEntry> &entries, int socket_id, double core_freq_hz,
                     double uncore_freq_hz, int smt_state, double &base_power_w);

} // namespace energy_attribution
} // namespace duckdb
