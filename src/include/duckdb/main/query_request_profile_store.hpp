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

class QueryRequestProfileStore {
public:
	static QueryRequestProfileStore &Get();

	void RecordQueryCompletion(const QueryRequestMetadata &metadata, uint64_t query_end_ns,
	                           const vector<PipelineProfilingInfo> &pipeline_profiles);
	bool TryGetQueryEstimate(uint64_t template_id, uint64_t scale_factor, QueryRequestProfileEstimate &estimate) const;
	bool TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor, uint64_t pipeline_signature_hash,
	                            QueryRequestPipelineProfileEstimate &estimate) const;
	idx_t QueryProfileCount() const;
	idx_t PipelineProfileCount() const;
	void Clear();
};

} // namespace duckdb
