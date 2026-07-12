//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_request_profile_store.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_metadata.hpp"

namespace duckdb {

struct QueryRequestProfileEstimate {
	bool valid = false;
	idx_t sample_count = 0;
	double mean_runtime_ns = 0;
	double p50_runtime_ns = 0;
	double p90_runtime_ns = 0;
	double p99_runtime_ns = 0;
	double last_runtime_ns = 0;
	double mean_lateness_ns = 0;
	double mean_sla_cost = 0;
};

struct QueryRequestPipelineProfileEstimate {
	bool valid = false;
	idx_t sample_count = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	string pipeline_signature;
	string operator_type_sequence;
	string source_type;
	string sink_type;
	string source_input_kind;
	string source_input_confidence;
	string planned_input_native_unit;
	double mean_task_runtime_ns = 0;
	double p90_task_runtime_ns = 0;
	double mean_lifecycle_runtime_ns = 0;
	double mean_downstream_suffix_ns = 0;
	double mean_task_count = 0;
	double mean_source_max_threads = 0;
	double mean_planned_input_rows = 0;
	double mean_planned_input_chunks_equiv = 0;
};

struct QueryRequestProfileSnapshot {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	QueryRequestProfileEstimate estimate;
};

struct QueryRequestPipelineProfileSnapshot {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	QueryRequestPipelineProfileEstimate estimate;
};

struct QueryRequestSampleSnapshot {
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double sla_tag = 0;
	double sla_penalty_per_s = 0;
	uint64_t query_start_ns = 0;
	uint64_t query_end_ns = 0;
	uint64_t runtime_ns = 0;
	uint64_t deadline_ns = 0;
	uint64_t lateness_ns = 0;
	double sla_cost = 0;
	bool deadline_met = true;
};

struct QueryRequestPipelineInstanceSnapshot {
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	string pipeline_signature;
	string operator_type_sequence;
	string source_type;
	string sink_type;
	string source_input_kind;
	string source_input_confidence;
	string planned_input_native_unit;
	idx_t task_count = 0;
	idx_t source_max_threads = 0;
	idx_t planned_input_rows = 0;
	idx_t planned_input_chunks_equiv = 0;
	idx_t source_estimated_cardinality = 0;
	idx_t sink_estimated_cardinality = 0;
	uint64_t start_ns = 0;
	uint64_t tasks_done_ns = 0;
	uint64_t finish_done_ns = 0;
	uint64_t task_runtime_ns = 0;
	uint64_t lifecycle_runtime_ns = 0;
	uint64_t downstream_suffix_ns = 0;
};

class QueryRequestProfileStore {
public:
	static QueryRequestProfileStore &Get();

	void RecordQueryCompletion(const QueryRequestMetadata &metadata, uint64_t query_end_ns,
	                           const vector<PipelineProfilingInfo> &pipeline_profiles);
	bool TryGetQueryEstimate(uint64_t template_id, uint64_t scale_factor, QueryRequestProfileEstimate &estimate) const;
	bool TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor, idx_t pipeline_id,
	                            uint64_t pipeline_signature_hash, QueryRequestPipelineProfileEstimate &estimate) const;
	vector<QueryRequestProfileSnapshot> GetQueryProfilesSnapshot() const;
	vector<QueryRequestPipelineProfileSnapshot> GetPipelineProfilesSnapshot() const;
	vector<QueryRequestSampleSnapshot> GetQuerySamplesSnapshot() const;
	vector<QueryRequestPipelineInstanceSnapshot> GetPipelineInstancesSnapshot() const;
	idx_t QueryProfileCount() const;
	idx_t PipelineProfileCount() const;
	void Clear();
};

} // namespace duckdb
