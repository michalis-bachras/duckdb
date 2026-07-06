//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_pipeline_debug.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class Event;
class Pipeline;

struct QueryPipelineDebugEventSnapshot {
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t activation_group_id = 0;
	idx_t pipeline_id = 0;
	string event_kind;
	string event_state;
	uint64_t timestamp_ns = 0;
	idx_t total_tasks = 0;
	idx_t finished_tasks = 0;
	idx_t effective_max_threads = 0;
	idx_t source_max_threads = 0;
	idx_t scheduler_threads = 0;
	bool parallel = false;
	string parallel_blocker;
	string source_type;
	string sink_type;
	string operator_type_sequence;
};

class QueryPipelineDebug {
public:
	static bool Enabled(ClientContext &context);
	static void RecordPipelineSchedule(Pipeline &pipeline, Event &event, bool parallel, const string &parallel_blocker,
	                                   idx_t effective_max_threads, idx_t source_max_threads,
	                                   idx_t scheduler_threads);
	static void RecordLifecycleSchedule(Pipeline &pipeline, Event &event, const string &state);
	static void RecordEventFinished(Event &event);

	static vector<QueryPipelineDebugEventSnapshot> GetDebugSnapshot();
	static void ClearDebugSnapshot();
};

} // namespace duckdb
