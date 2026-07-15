//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_request_metadata.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;

struct QueryRequestMetadata {
	bool valid = false;
	string parse_status = "missing";
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double sla_tag = 0;
	double sla_penalty_per_s = 0;
	uint64_t deadline_ns = 0;
	uint64_t query_start_ns = 0;
};

class QueryRequestMetadataManager {
public:
	static bool NeedsMetadata(ClientContext &context);
	static bool ProfilingEnabled(ClientContext &context);
	static void BeginQuery(ClientContext &context, uint64_t db_query_id, const string &query);
	static void RefreshQueryStart(ClientContext &context);
	static void EndQuery(ClientContext &context, bool success);
	static bool HasActiveMetadata(ClientContext &context);
	static bool TryGetActive(ClientContext &context, QueryRequestMetadata &metadata);
};

} // namespace duckdb
