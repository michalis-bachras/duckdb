//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/query_admission_policy.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Database-scoped ordering of requests waiting for an admission slot.
enum class QueryAdmissionPolicy : uint8_t { FIFO = 0, EDF = 1, SJF = 2 };

inline const char *QueryAdmissionPolicyName(QueryAdmissionPolicy policy) {
	switch (policy) {
	case QueryAdmissionPolicy::EDF: return "edf";
	case QueryAdmissionPolicy::SJF: return "sjf";
	default: return "fifo";
	}
}

} // namespace duckdb
