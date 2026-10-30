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
enum class QueryAdmissionPolicy : uint8_t { FIFO = 0, EDF = 1 };

inline const char *QueryAdmissionPolicyName(QueryAdmissionPolicy policy) {
	return policy == QueryAdmissionPolicy::EDF ? "edf" : "fifo";
}

} // namespace duckdb
