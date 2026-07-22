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

#include <array>

namespace duckdb {

struct QueryRequestProfileStoreState;
class FileSystem;

static constexpr idx_t DOWNSTREAM_SUFFIX_MAX_BUCKETS = 8;
static constexpr idx_t DOWNSTREAM_SUFFIX_MAX_RESOLVED_BUCKETS = 16;
static constexpr idx_t PIPELINE_HARDWARE_PROFILE_MIN_STABLE_SAMPLES = 8;

struct QueryRequestHardwareConfiguration {
	uint32_t core_frequency_khz = 0;
	uint32_t uncore_frequency_khz = 0;

	bool IsValid() const {
		return core_frequency_khz > 0 && uncore_frequency_khz > 0;
	}

	bool operator==(const QueryRequestHardwareConfiguration &other) const {
		return core_frequency_khz == other.core_frequency_khz &&
		       uncore_frequency_khz == other.uncore_frequency_khz;
	}
};

struct QueryRequestPipelineHardwareProfileEstimate {
	bool valid = false;
	bool mature = false;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	QueryRequestHardwareConfiguration hardware;
	idx_t throughput_sample_count = 0;
	idx_t power_sample_count = 0;
	double mean_work_units_per_s = 0;
	double ewma_work_units_per_s = 0;
	double p10_work_units_per_s = 0;
	double safe_work_units_per_s = 0;
	double mean_active_power_w = 0;
	double ewma_active_power_w = 0;
	double mean_charged_power_w = 0;
	double ewma_charged_power_w = 0;
	double safe_throughput_per_active_watt = 0;
	double mean_throughput_per_active_watt = 0;
	idx_t rejected_unstable_samples = 0;
};

enum class DownstreamSuffixProfileLevel : uint8_t { NONE = 0, EXACT = 1, SCALE_FACTOR = 2, GLOBAL = 3 };

struct DownstreamSuffixBucket {
	double lower_ns = 0;
	double upper_ns = 0;
	double probability = 0;
};

struct DownstreamSuffixHistogram {
	idx_t sample_count = 0;
	idx_t bucket_count = 0;
	uint64_t generation = 0;
	std::array<DownstreamSuffixBucket, DOWNSTREAM_SUFFIX_MAX_BUCKETS> buckets {};
};

struct DownstreamSuffixEstimate {
	bool valid = false;
	DownstreamSuffixProfileLevel primary_level = DownstreamSuffixProfileLevel::NONE;
	idx_t exact_sample_count = 0;
	idx_t scale_sample_count = 0;
	idx_t global_sample_count = 0;
	double exact_weight = 0;
	double scale_weight = 0;
	double global_weight = 0;
	idx_t bucket_count = 0;
	std::array<DownstreamSuffixBucket, DOWNSTREAM_SUFFIX_MAX_RESOLVED_BUCKETS> buckets {};
};

struct DownstreamSuffixEpochRequest {
	DownstreamSuffixEpochRequest() = default;
	DownstreamSuffixEpochRequest(uint64_t template_id_p, uint64_t scale_factor_p, idx_t pipeline_id_p,
	                             uint64_t pipeline_signature_hash_p, idx_t remaining_suffix_stages_p)
	    : template_id(template_id_p), scale_factor(scale_factor_p), pipeline_id(pipeline_id_p),
	      pipeline_signature_hash(pipeline_signature_hash_p), remaining_suffix_stages(remaining_suffix_stages_p) {
	}

	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	idx_t remaining_suffix_stages = 0;
};

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
	string source_work_class;
	string source_input_kind;
	string source_input_confidence;
	string planned_input_native_unit;
	double mean_task_runtime_ns = 0;
	double p90_task_runtime_ns = 0;
	double mean_lifecycle_runtime_ns = 0;
	double mean_finish_tail_ns = 0;
	double p90_finish_tail_ns = 0;
	double mean_downstream_suffix_ns = 0;
	idx_t downstream_suffix_sample_count = 0;
	double mean_task_count = 0;
	double mean_source_max_threads = 0;
	double mean_planned_input_rows = 0;
	double mean_planned_input_chunks_equiv = 0;
	idx_t throughput_sample_count = 0;
	double mean_single_worker_chunks_per_s = 0;
	double ewma_single_worker_chunks_per_s = 0;
	idx_t continuation_sample_count = 0;
	double mean_effective_ns_per_work_unit = 0;
	double p50_effective_ns_per_work_unit = 0;
	double p90_effective_ns_per_work_unit = 0;
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
	string source_work_class;
	string source_input_kind;
	string source_input_confidence;
	string planned_input_native_unit;
	idx_t task_count = 0;
	idx_t source_max_threads = 0;
	idx_t planned_input_rows = 0;
	idx_t planned_input_chunks_equiv = 0;
	idx_t worker_task_count = 0;
	uint64_t worker_task_duration_ns = 0;
	double single_worker_chunks_per_s = 0;
	idx_t source_estimated_cardinality = 0;
	idx_t sink_estimated_cardinality = 0;
	uint64_t start_ns = 0;
	uint64_t tasks_done_ns = 0;
	uint64_t finish_done_ns = 0;
	uint64_t task_runtime_ns = 0;
	uint64_t lifecycle_runtime_ns = 0;
	uint64_t finish_tail_ns = 0;
	uint64_t downstream_suffix_ns = 0;
	idx_t pipeline_completion_ordinal = 0;
	idx_t total_pipeline_count = 0;
	idx_t remaining_suffix_stages = 0;
	double normalized_downstream_suffix_ns = 0;
	double effective_ns_per_work_unit = 0;
	bool throughput_valid = false;
	bool continuation_valid = false;
};

struct QueryRequestContinuationProfileSnapshot {
	ContinuationEstimateLevel level = ContinuationEstimateLevel::NONE;
	SourceWorkClass source_work_class;
	PhysicalOperatorType sink_type = PhysicalOperatorType::INVALID;
	ContinuationEstimateKind kind = ContinuationEstimateKind::INVALID;
	idx_t sample_count = 0;
	double mean = 0;
	double p50 = 0;
	double p90 = 0;
	idx_t native_unit_mismatch_count = 0;
};

struct QueryRequestThroughputProfileSnapshot {
	PipelineThroughputEstimateLevel level = PipelineThroughputEstimateLevel::NONE;
	SourceWorkClass source_work_class;
	PhysicalOperatorType sink_type = PhysicalOperatorType::INVALID;
	SourceThroughputKind work_kind = SourceThroughputKind::UNKNOWN;
	string native_unit;
	idx_t sample_count = 0;
	double mean_work_units_per_s = 0;
	double ewma_work_units_per_s = 0;
};

struct QueryRequestDownstreamSuffixProfileSnapshot {
	DownstreamSuffixProfileLevel level = DownstreamSuffixProfileLevel::NONE;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	string value_unit;
	DownstreamSuffixHistogram histogram;
};

struct InternalEventModelEstimate {
	PipelineThroughputEstimate throughput;
	PipelineContinuationEstimate continuation;
	PipelineLifecycleTailEstimate tail_to_pipeline_end;
};

struct QueryRequestInternalEventProfileSnapshot {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	idx_t event_position = 0;
	string event_type;
	string native_unit;
	idx_t sample_count = 0;
	double mean_work_units_per_s = 0;
	double ewma_work_units_per_s = 0;
	double mean_ns_per_work_unit = 0;
	double p50_ns_per_work_unit = 0;
	double p90_ns_per_work_unit = 0;
	double mean_tail_ns = 0;
	double p90_tail_ns = 0;
};

struct QueryRequestInternalEventInstanceSnapshot : public InternalEventProfilingInfo {
	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	double work_units_per_s = 0;
	double ns_per_work_unit = 0;
	uint64_t tail_to_pipeline_end_ns = 0;
};

class QueryRequestProfileStore {
public:
	QueryRequestProfileStore();
	~QueryRequestProfileStore();

	QueryRequestProfileStore(const QueryRequestProfileStore &) = delete;
	QueryRequestProfileStore &operator=(const QueryRequestProfileStore &) = delete;

	void RecordQueryCompletion(const QueryRequestMetadata &metadata, uint64_t query_end_ns,
	                           const vector<PipelineProfilingInfo> &pipeline_profiles,
	                           const vector<InternalEventProfilingInfo> &internal_event_profiles = {});
	bool TryGetQueryEstimate(uint64_t template_id, uint64_t scale_factor, QueryRequestProfileEstimate &estimate) const;
	bool TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor, idx_t pipeline_id,
	                            uint64_t pipeline_signature_hash, QueryRequestPipelineProfileEstimate &estimate) const;
	PipelineContinuationEstimate ResolvePipelineContinuation(uint64_t template_id, uint64_t scale_factor,
	                                                         const PipelineProfileIdentity &identity,
	                                                         idx_t planned_work_units) const;
	PipelineThroughputEstimate ResolvePipelineThroughput(uint64_t template_id, uint64_t scale_factor,
	                                                     const PipelineProfileIdentity &identity) const;
	PipelineLifecycleTailEstimate ResolvePipelineLifecycleTail(uint64_t template_id, uint64_t scale_factor,
	                                                           const PipelineProfileIdentity &identity) const;
	InternalEventModelEstimate ResolveInternalEvent(uint64_t template_id, uint64_t scale_factor,
	                                                const PipelineProfileIdentity &pipeline_identity,
	                                                idx_t event_position, const string &event_type,
	                                                const string &native_unit) const;
	void RecordPipelineHardwareThroughput(uint64_t template_id, uint64_t scale_factor,
	                                      const PipelineProfileIdentity &pipeline_identity,
	                                      const QueryRequestHardwareConfiguration &hardware, double work_units,
	                                      uint64_t worker_duration_ns, bool hardware_state_stable);
	void RecordPipelineHardwareEnergy(uint64_t template_id, uint64_t scale_factor,
	                                  const PipelineProfileIdentity &pipeline_identity,
	                                  const QueryRequestHardwareConfiguration &hardware, uint64_t duration_ns,
	                                  double attributed_active_energy_j, double attributed_base_energy_j,
	                                  bool hardware_state_stable);
	vector<QueryRequestPipelineHardwareProfileEstimate>
	GetPipelineHardwareProfiles(uint64_t template_id, uint64_t scale_factor,
	                            const PipelineProfileIdentity &pipeline_identity) const;
	vector<QueryRequestPipelineHardwareProfileEstimate> GetPipelineHardwareProfilesSnapshot() const;
	vector<DownstreamSuffixEstimate>
	PrepareDownstreamSuffixEpoch(const vector<DownstreamSuffixEpochRequest> &requests) const;
	vector<QueryRequestProfileSnapshot> GetQueryProfilesSnapshot() const;
	vector<QueryRequestPipelineProfileSnapshot> GetPipelineProfilesSnapshot() const;
	vector<QueryRequestSampleSnapshot> GetQuerySamplesSnapshot() const;
	vector<QueryRequestPipelineInstanceSnapshot> GetPipelineInstancesSnapshot() const;
	vector<QueryRequestContinuationProfileSnapshot> GetContinuationProfilesSnapshot() const;
	vector<QueryRequestThroughputProfileSnapshot> GetThroughputProfilesSnapshot() const;
	vector<QueryRequestDownstreamSuffixProfileSnapshot> GetDownstreamSuffixProfilesSnapshot() const;
	vector<QueryRequestInternalEventProfileSnapshot> GetInternalEventProfilesSnapshot() const;
	vector<QueryRequestInternalEventInstanceSnapshot> GetInternalEventInstancesSnapshot() const;
	idx_t QueryProfileCount() const;
	idx_t PipelineProfileCount() const;
	//! Persist only bounded scheduler model state. Diagnostic instance/sample rows are intentionally excluded.
	void ExportSnapshot(FileSystem &fs, const string &path) const;
	//! Replace the current store atomically after fully validating and rebuilding derived state.
	void ImportSnapshot(FileSystem &fs, const string &path);
	void Clear();

private:
	unique_ptr<QueryRequestProfileStoreState> state;
};

} // namespace duckdb
