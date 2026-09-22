// Shared downstream predictor selection for SLA and SLA-energy.
#pragma once

#include "duckdb/common/enums/query_scheduler_policy.hpp"

namespace duckdb {

enum class QuerySLASuffixModel : uint8_t { MODEL_DEFAULT = 0, HISTOGRAM, POINT };
enum class QuerySLASuffixUpdateMode : uint8_t { NONE = 0, HISTOGRAM, POINT, BOOTSTRAP_ALL };

inline QuerySLASuffixModel ResolveQuerySLASuffixModel(QuerySLASuffixModel model, QuerySchedulerPolicy policy) {
	if (model != QuerySLASuffixModel::MODEL_DEFAULT) {
		return model;
	}
	return policy == QuerySchedulerPolicy::SLA_ENERGY ? QuerySLASuffixModel::POINT : QuerySLASuffixModel::HISTOGRAM;
}

inline const char *QuerySLASuffixModelName(QuerySLASuffixModel model) {
	switch (model) {
	case QuerySLASuffixModel::POINT:
		return "point";
	case QuerySLASuffixModel::HISTOGRAM:
		return "histogram";
	default:
		return "model_default";
	}
}

} // namespace duckdb
