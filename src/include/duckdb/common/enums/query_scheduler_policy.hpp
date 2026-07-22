//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/query_scheduler_policy.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Database-scoped worker scheduling policy for tagged analytical queries.
enum class QuerySchedulerPolicy : uint8_t { DEFAULT = 0, SLA = 1, STRIDE = 2, SLA_ENERGY = 3 };

inline bool IsQuerySLAPolicy(QuerySchedulerPolicy policy) {
	return policy == QuerySchedulerPolicy::SLA || policy == QuerySchedulerPolicy::SLA_ENERGY;
}

} // namespace duckdb
