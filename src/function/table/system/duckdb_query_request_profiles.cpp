//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_query_request_profiles.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_admission_controller.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/query_activation_scheduler.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include "duckdb/parallel/query_stride_scheduler.hpp"

namespace duckdb {

struct DuckDBQueryRequestProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestPipelineProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestPipelineProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestPipelineHardwareProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestPipelineHardwareProfileEstimate> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestSamplesData : public GlobalTableFunctionState {
	vector<QueryRequestSampleSnapshot> samples;
	idx_t offset = 0;
};

struct DuckDBQueryRequestPipelineInstancesData : public GlobalTableFunctionState {
	vector<QueryRequestPipelineInstanceSnapshot> instances;
	idx_t offset = 0;
};

struct DuckDBQueryRequestInternalEventProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestInternalEventProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestInternalEventInstancesData : public GlobalTableFunctionState {
	vector<QueryRequestInternalEventInstanceSnapshot> instances;
	idx_t offset = 0;
};

struct DuckDBQueryRequestContinuationProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestContinuationProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestThroughputProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestThroughputProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestDownstreamSuffixProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestDownstreamSuffixProfileSnapshot> profiles;
	idx_t profile_offset = 0;
	idx_t bucket_offset = 0;
};

struct DuckDBQueryAdmissionData : public GlobalTableFunctionState {
	vector<QueryAdmissionSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQueryAdmissionEventsData : public GlobalTableFunctionState {
	vector<QueryAdmissionEventSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQueryActivationEventsData : public GlobalTableFunctionState {
	vector<QueryActivationEventSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQueryPipelineEventsData : public GlobalTableFunctionState {
	vector<QueryPipelineDebugEventSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQuerySLASchedulerData : public GlobalTableFunctionState {
	vector<QuerySLASchedulerSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQuerySLASchedulerEpochsData : public GlobalTableFunctionState {
	vector<QuerySLASchedulerEpochSnapshot> snapshots;
	uint64_t dropped_count = 0;
	idx_t offset = 0;
};

struct DuckDBQuerySLAEnergyWorkerEpochsData : public GlobalTableFunctionState {
	vector<QuerySLAEnergyWorkerEpochSnapshot> snapshots;
	uint64_t dropped_count = 0;
	idx_t offset = 0;
};

struct DuckDBQuerySLAEnergyHardwareData : public GlobalTableFunctionState {
	QueryHardwareManagerSnapshot snapshot;
	QuerySLAExplorationDiagnosticsSnapshot exploration;
	bool emitted = false;
};

struct DuckDBQueryStrideSchedulerData : public GlobalTableFunctionState {
	vector<QueryStrideSchedulerSnapshot> snapshots;
	idx_t offset = 0;
};

struct DuckDBQueryStrideTuningData : public GlobalTableFunctionState {
	QueryStrideTuningSnapshot snapshot;
	bool emitted = false;
};

static unique_ptr<FunctionData> DuckDBQueryRequestProfilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p50_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p90_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p99_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("last_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_lateness_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_sla_cost");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestProfilesInit(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestProfilesData>();
	result->profiles = DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetQueryProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestProfilesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		const auto &estimate = profile.estimate;
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(profile.template_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(estimate.sample_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p50_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p90_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p99_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.last_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_lateness_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_sla_cost));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestPipelineProfilesBind(ClientContext &context,
                                                                       TableFunctionBindInput &input,
                                                                       vector<LogicalType> &return_types,
                                                                       vector<string> &names) {
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_signature_hash");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_signature");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("operator_type_sequence");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_work_class");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_input_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_input_confidence");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("planned_input_native_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("mean_task_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p90_task_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_lifecycle_runtime_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_finish_tail_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p90_finish_tail_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_downstream_suffix_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("downstream_suffix_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_task_count");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_source_max_threads");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_planned_input_rows");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_planned_input_chunks_equiv");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("throughput_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("ewma_single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("continuation_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_effective_ns_per_work_unit");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p50_effective_ns_per_work_unit");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p90_effective_ns_per_work_unit");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestPipelineProfilesInit(ClientContext &context,
                                                                                   TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestPipelineProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetPipelineProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestPipelineProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
                                                       DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestPipelineProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		const auto &estimate = profile.estimate;
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(profile.template_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(estimate.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(estimate.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UBIGINT(estimate.sample_count));
		output.SetValue(col++, count, Value(estimate.pipeline_signature));
		output.SetValue(col++, count, Value(estimate.operator_type_sequence));
		output.SetValue(col++, count, Value(estimate.source_type));
		output.SetValue(col++, count, Value(estimate.sink_type));
		output.SetValue(col++, count, Value(estimate.source_work_class));
		output.SetValue(col++, count, Value(estimate.source_input_kind));
		output.SetValue(col++, count, Value(estimate.source_input_confidence));
		output.SetValue(col++, count, Value(estimate.planned_input_native_unit));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_task_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p90_task_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_lifecycle_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_finish_tail_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p90_finish_tail_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_downstream_suffix_ns));
		output.SetValue(col++, count, Value::UBIGINT(estimate.downstream_suffix_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_task_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_source_max_threads));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_planned_input_rows));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_planned_input_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(estimate.throughput_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_single_worker_chunks_per_s));
		output.SetValue(col++, count, Value::DOUBLE(estimate.ewma_single_worker_chunks_per_s));
		output.SetValue(col++, count, Value::UBIGINT(estimate.continuation_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_effective_ns_per_work_unit));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p50_effective_ns_per_work_unit));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p90_effective_ns_per_work_unit));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestPipelineHardwareProfilesBind(
    ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	for (auto name : {"template_id", "scale_factor", "pipeline_id", "pipeline_signature_hash"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	names.emplace_back("core_frequency_khz");
	return_types.emplace_back(LogicalType::UINTEGER);
	names.emplace_back("uncore_frequency_khz");
	return_types.emplace_back(LogicalType::UINTEGER);
	for (auto name : {"valid", "mature"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::BOOLEAN);
	}
	for (auto name : {"throughput_sample_count", "power_sample_count"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	for (auto name : {"mean_work_units_per_s", "ewma_work_units_per_s", "p10_work_units_per_s",
	                  "safe_work_units_per_s", "mean_active_power_w", "ewma_active_power_w",
	                  "mean_charged_power_w", "ewma_charged_power_w", "safe_throughput_per_active_watt",
	                  "mean_throughput_per_active_watt"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::DOUBLE);
	}
	names.emplace_back("rejected_unstable_samples");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestPipelineHardwareProfilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestPipelineHardwareProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetPipelineHardwareProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestPipelineHardwareProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
                                                               DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestPipelineHardwareProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(profile.template_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UINTEGER(profile.hardware.core_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(profile.hardware.uncore_frequency_khz));
		output.SetValue(col++, count, Value::BOOLEAN(profile.valid));
		output.SetValue(col++, count, Value::BOOLEAN(profile.mature));
		output.SetValue(col++, count, Value::UBIGINT(profile.throughput_sample_count));
		output.SetValue(col++, count, Value::UBIGINT(profile.power_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.ewma_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.p10_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.safe_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_active_power_w));
		output.SetValue(col++, count, Value::DOUBLE(profile.ewma_active_power_w));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_charged_power_w));
		output.SetValue(col++, count, Value::DOUBLE(profile.ewma_charged_power_w));
		output.SetValue(col++, count, Value::DOUBLE(profile.safe_throughput_per_active_watt));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_throughput_per_active_watt));
		output.SetValue(col++, count, Value::UBIGINT(profile.rejected_unstable_samples));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestSamplesBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sla_tag");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("sla_penalty_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("query_start_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("query_end_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("runtime_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("deadline_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("lateness_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sla_cost");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("deadline_met");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestSamplesInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestSamplesData>();
	result->samples = DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetQuerySamplesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestSamplesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestSamplesData>();
	idx_t count = 0;
	while (data.offset < data.samples.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &sample = data.samples[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(sample.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(sample.request_id));
		output.SetValue(col++, count, Value::UBIGINT(sample.template_id));
		output.SetValue(col++, count, Value::UBIGINT(sample.scale_factor));
		output.SetValue(col++, count, Value::DOUBLE(sample.sla_tag));
		output.SetValue(col++, count, Value::DOUBLE(sample.sla_penalty_per_s));
		output.SetValue(col++, count, Value::UBIGINT(sample.query_start_ns));
		output.SetValue(col++, count, Value::UBIGINT(sample.query_end_ns));
		output.SetValue(col++, count, Value::UBIGINT(sample.runtime_ns));
		output.SetValue(col++, count, Value::UBIGINT(sample.deadline_ns));
		output.SetValue(col++, count, Value::UBIGINT(sample.lateness_ns));
		output.SetValue(col++, count, Value::DOUBLE(sample.sla_cost));
		output.SetValue(col++, count, Value::BOOLEAN(sample.deadline_met));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestPipelineInstancesBind(ClientContext &context,
                                                                        TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types,
                                                                        vector<string> &names) {
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_signature_hash");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_signature");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("operator_type_sequence");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_work_class");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_input_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_input_confidence");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("planned_input_native_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("task_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_max_threads");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("planned_input_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("planned_input_chunks_equiv");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("worker_task_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("worker_task_duration_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("throughput_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("source_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sink_estimated_cardinality");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("start_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("tasks_done_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("finish_done_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("task_runtime_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("lifecycle_runtime_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("finish_tail_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("downstream_suffix_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_completion_ordinal");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("total_pipeline_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_suffix_stages");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("normalized_downstream_suffix_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("effective_ns_per_work_unit");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("continuation_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestPipelineInstancesInit(ClientContext &context,
                                                                                    TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestPipelineInstancesData>();
	result->instances =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetPipelineInstancesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestPipelineInstancesFunction(ClientContext &context, TableFunctionInput &data_p,
                                                        DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestPipelineInstancesData>();
	idx_t count = 0;
	while (data.offset < data.instances.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &instance = data.instances[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(instance.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.request_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.template_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(instance.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.pipeline_signature_hash));
		output.SetValue(col++, count, Value(instance.pipeline_signature));
		output.SetValue(col++, count, Value(instance.operator_type_sequence));
		output.SetValue(col++, count, Value(instance.source_type));
		output.SetValue(col++, count, Value(instance.sink_type));
		output.SetValue(col++, count, Value(instance.source_work_class));
		output.SetValue(col++, count, Value(instance.source_input_kind));
		output.SetValue(col++, count, Value(instance.source_input_confidence));
		output.SetValue(col++, count, Value(instance.planned_input_native_unit));
		output.SetValue(col++, count, Value::UBIGINT(instance.task_count));
		output.SetValue(col++, count, Value::UBIGINT(instance.source_max_threads));
		output.SetValue(col++, count, Value::UBIGINT(instance.planned_input_rows));
		output.SetValue(col++, count, Value::UBIGINT(instance.planned_input_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(instance.worker_task_count));
		output.SetValue(col++, count, Value::UBIGINT(instance.worker_task_duration_ns));
		output.SetValue(col++, count, Value::DOUBLE(instance.single_worker_chunks_per_s));
		output.SetValue(col++, count, Value::BOOLEAN(instance.throughput_valid));
		output.SetValue(col++, count, Value::UBIGINT(instance.source_estimated_cardinality));
		output.SetValue(col++, count, Value::UBIGINT(instance.sink_estimated_cardinality));
		output.SetValue(col++, count, Value::UBIGINT(instance.start_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.tasks_done_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.finish_done_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.task_runtime_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.lifecycle_runtime_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.finish_tail_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.downstream_suffix_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.pipeline_completion_ordinal));
		output.SetValue(col++, count, Value::UBIGINT(instance.total_pipeline_count));
		output.SetValue(col++, count, Value::UBIGINT(instance.remaining_suffix_stages));
		output.SetValue(col++, count, Value::DOUBLE(instance.normalized_downstream_suffix_ns));
		output.SetValue(col++, count, Value::DOUBLE(instance.effective_ns_per_work_unit));
		output.SetValue(col++, count, Value::BOOLEAN(instance.continuation_valid));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestInternalEventProfilesBind(
    ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	const char *columns[] = {"template_id", "scale_factor", "pipeline_id", "pipeline_signature_hash",
	                         "event_position", "event_type", "native_unit", "sample_count"};
	for (idx_t i = 0; i < 8; i++) {
		names.emplace_back(columns[i]);
		return_types.emplace_back(i == 5 || i == 6 ? LogicalType::VARCHAR : LogicalType::UBIGINT);
	}
	const char *metrics[] = {"mean_work_units_per_s", "ewma_work_units_per_s", "mean_ns_per_work_unit",
	                         "p50_ns_per_work_unit", "p90_ns_per_work_unit", "mean_tail_ns", "p90_tail_ns"};
	for (idx_t i = 0; i < 7; i++) {
		names.emplace_back(metrics[i]);
		return_types.emplace_back(LogicalType::DOUBLE);
	}
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestInternalEventProfilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestInternalEventProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetInternalEventProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestInternalEventProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
	                                                          DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestInternalEventProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(profile.template_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UBIGINT(profile.event_position));
		output.SetValue(col++, count, Value(profile.event_type));
		output.SetValue(col++, count, Value(profile.native_unit));
		output.SetValue(col++, count, Value::UBIGINT(profile.sample_count));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.ewma_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_ns_per_work_unit));
		output.SetValue(col++, count, Value::DOUBLE(profile.p50_ns_per_work_unit));
		output.SetValue(col++, count, Value::DOUBLE(profile.p90_ns_per_work_unit));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_tail_ns));
		output.SetValue(col++, count, Value::DOUBLE(profile.p90_tail_ns));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestInternalEventInstancesBind(
    ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	const char *integer_columns[] = {"db_query_id", "request_id", "template_id", "scale_factor", "pipeline_id",
	                                 "pipeline_signature_hash", "event_position"};
	for (idx_t i = 0; i < 7; i++) {
		names.emplace_back(integer_columns[i]);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	names.emplace_back("event_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("native_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	const char *work_columns[] = {"total_work_units", "completed_work_units", "worker_time_ns", "start_ns", "finish_ns"};
	for (idx_t i = 0; i < 5; i++) {
		names.emplace_back(work_columns[i]);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	names.emplace_back("work_units_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("ns_per_work_unit");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("tail_to_pipeline_end_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestInternalEventInstancesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestInternalEventInstancesData>();
	result->instances =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetInternalEventInstancesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestInternalEventInstancesFunction(ClientContext &context, TableFunctionInput &data_p,
	                                                           DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestInternalEventInstancesData>();
	idx_t count = 0;
	while (data.offset < data.instances.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &instance = data.instances[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(instance.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.request_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.template_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(instance.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(instance.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UBIGINT(instance.event_position));
		output.SetValue(col++, count, Value(instance.event_type));
		output.SetValue(col++, count, Value(instance.native_unit));
		output.SetValue(col++, count, Value::UBIGINT(instance.total_work_units));
		output.SetValue(col++, count, Value::UBIGINT(instance.completed_work_units));
		output.SetValue(col++, count, Value::UBIGINT(instance.worker_time_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.start_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.finish_ns));
		output.SetValue(col++, count, Value::DOUBLE(instance.work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(instance.ns_per_work_unit));
		output.SetValue(col++, count, Value::UBIGINT(instance.tail_to_pipeline_end_ns));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestContinuationProfilesBind(ClientContext &context,
                                                                           TableFunctionBindInput &input,
                                                                           vector<LogicalType> &return_types,
                                                                           vector<string> &names) {
	names.emplace_back("profile_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_work_class");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("estimate_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p50");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("p90");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("native_unit_mismatch_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestContinuationProfilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestContinuationProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetContinuationProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestContinuationProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
                                                           DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestContinuationProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(ContinuationEstimateLevelToString(profile.level)));
		output.SetValue(col++, count, Value(SourceWorkClassToString(profile.source_work_class)));
		output.SetValue(col++, count, Value(PhysicalOperatorToString(profile.sink_type)));
		output.SetValue(col++, count, Value(ContinuationEstimateKindToString(profile.kind)));
		output.SetValue(col++, count, Value::UBIGINT(profile.sample_count));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean));
		output.SetValue(col++, count, Value::DOUBLE(profile.p50));
		output.SetValue(col++, count, Value::DOUBLE(profile.p90));
		output.SetValue(col++, count, Value::UBIGINT(profile.native_unit_mismatch_count));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryRequestThroughputProfilesBind(ClientContext &context,
	                                                                      TableFunctionBindInput &input,
	                                                                      vector<LogicalType> &return_types,
	                                                                      vector<string> &names) {
	names.emplace_back("profile_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_work_class");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("work_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("native_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_work_units_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("ewma_work_units_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestThroughputProfilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestThroughputProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetThroughputProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestThroughputProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
	                                                       DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestThroughputProfilesData>();
	idx_t count = 0;
	while (data.offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(PipelineThroughputEstimateLevelToString(profile.level)));
		output.SetValue(col++, count, Value(SourceWorkClassToString(profile.source_work_class)));
		output.SetValue(col++, count, Value(PhysicalOperatorToString(profile.sink_type)));
		output.SetValue(col++, count, Value(SourceThroughputKindToString(profile.work_kind)));
		output.SetValue(col++, count, Value(profile.native_unit));
		output.SetValue(col++, count, Value::UBIGINT(profile.sample_count));
		output.SetValue(col++, count, Value::DOUBLE(profile.mean_work_units_per_s));
		output.SetValue(col++, count, Value::DOUBLE(profile.ewma_work_units_per_s));
		count++;
	}
	output.SetCardinality(count);
}

static const char *DownstreamSuffixProfileLevelName(DownstreamSuffixProfileLevel level) {
	switch (level) {
	case DownstreamSuffixProfileLevel::EXACT:
		return "exact";
	case DownstreamSuffixProfileLevel::SCALE_FACTOR:
		return "scale_factor";
	case DownstreamSuffixProfileLevel::GLOBAL:
		return "global";
	default:
		return "none";
	}
}

static unique_ptr<FunctionData> DuckDBQueryRequestDownstreamSuffixProfilesBind(
    ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("profile_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_signature_hash");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("value_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("generation");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("bucket_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("lower");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("upper");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("probability");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("point_mass");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState>
DuckDBQueryRequestDownstreamSuffixProfilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestDownstreamSuffixProfilesData>();
	result->profiles =
	    DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().GetDownstreamSuffixProfilesSnapshot();
	return std::move(result);
}

static void DuckDBQueryRequestDownstreamSuffixProfilesFunction(ClientContext &context, TableFunctionInput &data_p,
	                                                           DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryRequestDownstreamSuffixProfilesData>();
	idx_t count = 0;
	while (data.profile_offset < data.profiles.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &profile = data.profiles[data.profile_offset];
		if (data.bucket_offset >= profile.histogram.bucket_count) {
			data.profile_offset++;
			data.bucket_offset = 0;
			continue;
		}
		const auto &bucket = profile.histogram.buckets[data.bucket_offset];
		idx_t col = 0;
		output.SetValue(col++, count, Value(DownstreamSuffixProfileLevelName(profile.level)));
		output.SetValue(col++, count, Value::UBIGINT(profile.template_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(profile.pipeline_signature_hash));
		output.SetValue(col++, count, Value(profile.value_unit));
		output.SetValue(col++, count, Value::UBIGINT(profile.histogram.sample_count));
		output.SetValue(col++, count, Value::UBIGINT(profile.histogram.generation));
		output.SetValue(col++, count, Value::UBIGINT(data.bucket_offset));
		output.SetValue(col++, count, Value::DOUBLE(bucket.lower_ns));
		output.SetValue(col++, count, Value::DOUBLE(bucket.upper_ns));
		output.SetValue(col++, count, Value::DOUBLE(bucket.probability));
		output.SetValue(col++, count, Value::BOOLEAN(bucket.lower_ns == bucket.upper_ns));
		data.bucket_offset++;
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryAdmissionBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("slot_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sla_tag");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("deadline_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("queued_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("admitted_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryAdmissionInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryAdmissionData>();
	result->snapshots = DatabaseInstance::GetDatabase(context).GetQueryAdmissionController().GetSnapshot();
	return std::move(result);
}

static void DuckDBQueryAdmissionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryAdmissionData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(snapshot.state));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.slot_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.template_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scale_factor));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.sla_tag));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.deadline_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.queued_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.admitted_ns));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryAdmissionEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	names.emplace_back("event_state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("timestamp_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("ticket");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("slot_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("max_active");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("active_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("waiting_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("sla_tag");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("deadline_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("queued_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("admitted_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("released_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryAdmissionEventsInit(ClientContext &context,
                                                                           TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryAdmissionEventsData>();
	result->snapshots = QueryAdmissionController::GetEventSnapshot();
	return std::move(result);
}

static void DuckDBQueryAdmissionEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryAdmissionEventsData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(snapshot.event_state));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.timestamp_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.ticket));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.slot_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.max_active));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.active_count));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.waiting_count));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.template_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scale_factor));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.sla_tag));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.deadline_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.queued_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.admitted_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.released_ns));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryActivationEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("activation_group_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("event_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("event_state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("timestamp_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryActivationEventsInit(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryActivationEventsData>();
	result->snapshots = QueryActivationScheduler::GetDebugSnapshot();
	return std::move(result);
}

static void DuckDBQueryActivationEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryActivationEventsData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.template_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.activation_group_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_id));
		output.SetValue(col++, count, Value(snapshot.event_kind));
		output.SetValue(col++, count, Value(snapshot.event_state));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.timestamp_ns));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQuerySLASchedulerBind(ClientContext &context, TableFunctionBindInput &input,
	                                                        vector<LogicalType> &return_types,
	                                                        vector<string> &names) {
	names.emplace_back("epoch_generation");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_generation");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("event_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("model_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("model_error");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("demand_cap");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_work_units");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("selected_throughput");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("throughput_is_live");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("mandatory_workers");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("optional_workers");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("residual_workers");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("assigned_workers");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("next_mandatory_gain");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("next_optional_gain");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQuerySLASchedulerInit(ClientContext &context,
	                                                                    TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQuerySLASchedulerData>();
	result->snapshots = DatabaseInstance::GetDatabase(context).GetQuerySLAScheduler().GetSnapshot();
	return std::move(result);
}

static void DuckDBQuerySLASchedulerFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQuerySLASchedulerData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_generation));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_generation));
		output.SetValue(col++, count, Value(snapshot.event_kind));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.model_valid));
		output.SetValue(col++, count, Value(snapshot.model_error));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.demand_cap));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.remaining_work_units));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.selected_throughput));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.throughput_is_live));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.mandatory_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.optional_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.residual_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.assigned_workers));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.next_mandatory_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.next_optional_gain));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQuerySLASchedulerEpochsBind(ClientContext &context,
	                                                              TableFunctionBindInput &input,
	                                                              vector<LogicalType> &return_types,
	                                                              vector<string> &names) {
	#define SLA_EPOCH_COLUMN(name, type) \
	names.emplace_back(name);              \
	return_types.emplace_back(type)
	SLA_EPOCH_COLUMN("scheduler_policy", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("epoch_generation", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("epoch_timestamp_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("epoch_compute_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("epoch_wall_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("epoch_thread_cpu_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("capture_lock_wait_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("capture_lock_hold_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("work_snapshot_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("suffix_prepare_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("model_build_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("allocation_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("publish_lock_wait_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("publish_lock_hold_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("hardware_apply_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("trace_build_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("trace_lock_wait_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("trace_lock_hold_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("db_query_id", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("request_id", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("template_id", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("scale_factor", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("deadline_ns", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("pipeline_id", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("pipeline_signature_hash", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("pipeline_generation", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("event_kind", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("model_valid", LogicalType::BOOLEAN);
	SLA_EPOCH_COLUMN("model_error", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("demand_cap", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("remaining_work_units", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("selected_throughput", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("throughput_is_live", LogicalType::BOOLEAN);
	SLA_EPOCH_COLUMN("historical_throughput_level", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("historical_throughput_sample_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("continuation_level", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("continuation_kind", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("continuation_sample_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("continuation_p90", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("suffix_primary_level", LogicalType::VARCHAR);
	SLA_EPOCH_COLUMN("suffix_exact_sample_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("suffix_scale_sample_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("suffix_global_sample_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("suffix_exact_weight", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("suffix_scale_weight", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("suffix_global_weight", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("suffix_bucket_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("suffix_mean_ns", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("suffix_p90_ns", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("mandatory_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("optional_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("residual_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("liveness_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("planned_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("assigned_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_mandatory_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_o1_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_o2_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_o3_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_liveness_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_parked_workers", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_core_probes", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_uncore_probes", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_forced_uncore_probes", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_optional_only_socket_available", LogicalType::BOOLEAN);
	SLA_EPOCH_COLUMN("energy_epochs_without_optional_only_socket", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_profile_fallback_count", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_immature_pair_rejections", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_domain_pair_promotions", LogicalType::UBIGINT);
	SLA_EPOCH_COLUMN("energy_mandatory_footprint_j", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("energy_optional_incremental_j", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_service_rate", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_active_power_w", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_epoch_energy_j", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_optional_risk", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("first_mandatory_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("first_optional_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("last_mandatory_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("last_optional_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("next_mandatory_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("next_optional_gain", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_pipeline_finish_ns", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_query_finish_mean_ns", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_query_finish_p90_ns", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("predicted_sla_cost", LogicalType::DOUBLE);
	SLA_EPOCH_COLUMN("trace_dropped_count", LogicalType::UBIGINT);
#undef SLA_EPOCH_COLUMN
	return nullptr;
}

static unique_ptr<FunctionData> DuckDBQueryStrideSchedulerBind(ClientContext &context, TableFunctionBindInput &input,
	                                                            vector<LogicalType> &return_types,
	                                                            vector<string> &names) {
	for (auto name : {"slot", "generation", "db_query_id", "request_id", "template_id", "scale_factor"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	names.emplace_back("initial_priority");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("minimum_priority");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("static_priority");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("task_set_active");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("lifecycle_serial");
	return_types.emplace_back(LogicalType::BOOLEAN);
	for (auto name : {"eligible_workers", "task_set_generation", "quanta", "worker_time_us"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryStrideSchedulerInit(ClientContext &context,
	                                                                       TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryStrideSchedulerData>();
	result->snapshots = DatabaseInstance::GetDatabase(context).GetQueryStrideScheduler().GetSnapshot();
	return std::move(result);
}

static void DuckDBQueryStrideSchedulerFunction(ClientContext &context, TableFunctionInput &data_p,
	                                             DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryStrideSchedulerData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = data.snapshots[data.offset++];
		idx_t column = 0;
		output.SetValue(column++, count, Value::UBIGINT(row.slot));
		output.SetValue(column++, count, Value::UBIGINT(row.generation));
		output.SetValue(column++, count, Value::UBIGINT(row.db_query_id));
		output.SetValue(column++, count, Value::UBIGINT(row.request_id));
		output.SetValue(column++, count, Value::UBIGINT(row.template_id));
		output.SetValue(column++, count, Value::UBIGINT(row.scale_factor));
		output.SetValue(column++, count, Value::DOUBLE(row.initial_priority));
		output.SetValue(column++, count, Value::DOUBLE(row.minimum_priority));
		output.SetValue(column++, count, Value::BOOLEAN(row.static_priority));
		output.SetValue(column++, count, Value::BOOLEAN(row.task_set_active));
		output.SetValue(column++, count, Value::BOOLEAN(row.lifecycle_serial));
		output.SetValue(column++, count, Value::UBIGINT(row.eligible_workers));
		output.SetValue(column++, count, Value::UBIGINT(row.task_set_generation));
		output.SetValue(column++, count, Value::UBIGINT(row.quanta));
		output.SetValue(column++, count, Value::UBIGINT(row.worker_time_us));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQueryStrideTuningBind(ClientContext &context, TableFunctionBindInput &input,
	                                                         vector<LogicalType> &return_types,
	                                                         vector<string> &names) {
	names = {"phase",
	         "decay_start",
	         "decay_lambda",
	         "last_objective",
	         "optimization_count",
	         "tracked_query_count",
	         "registered_queries",
	         "unregistered_queries",
	         "activated_task_sets",
	         "finished_task_sets",
	         "change_task_sets",
	         "return_task_sets",
	         "finalization_task_sets",
	         "worker_lazy_deactivations"};
	return_types = {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::DOUBLE,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT};
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryStrideTuningInit(ClientContext &context,
	                                                                    TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryStrideTuningData>();
	result->snapshot = DatabaseInstance::GetDatabase(context).GetQueryStrideScheduler().GetTuningSnapshot();
	return std::move(result);
}

static void DuckDBQueryStrideTuningFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryStrideTuningData>();
	if (data.emitted) {
		output.SetCardinality(0);
		return;
	}
	auto &row = data.snapshot;
	output.SetValue(0, 0, Value(row.phase));
	output.SetValue(1, 0, Value::INTEGER(row.decay_start));
	output.SetValue(2, 0, Value::DOUBLE(row.decay_lambda));
	output.SetValue(3, 0, Value::DOUBLE(row.last_objective));
	output.SetValue(4, 0, Value::UBIGINT(row.optimization_count));
	output.SetValue(5, 0, Value::UBIGINT(row.tracked_query_count));
	output.SetValue(6, 0, Value::UBIGINT(row.registered_queries));
	output.SetValue(7, 0, Value::UBIGINT(row.unregistered_queries));
	output.SetValue(8, 0, Value::UBIGINT(row.activated_task_sets));
	output.SetValue(9, 0, Value::UBIGINT(row.finished_task_sets));
	output.SetValue(10, 0, Value::UBIGINT(row.change_task_sets));
	output.SetValue(11, 0, Value::UBIGINT(row.return_task_sets));
	output.SetValue(12, 0, Value::UBIGINT(row.finalization_task_sets));
	output.SetValue(13, 0, Value::UBIGINT(row.worker_lazy_deactivations));
	output.SetCardinality(1);
	data.emitted = true;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQuerySLASchedulerEpochsInit(ClientContext &context,
	                                                                         TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQuerySLASchedulerEpochsData>();
	auto &scheduler = DatabaseInstance::GetDatabase(context).GetQuerySLAScheduler();
	result->snapshots = scheduler.GetEpochTrace();
	result->dropped_count = scheduler.EpochTraceDroppedCount();
	return std::move(result);
}

static void DuckDBQuerySLASchedulerEpochsFunction(ClientContext &context, TableFunctionInput &data_p,
	                                               DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQuerySLASchedulerEpochsData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value(snapshot.scheduler_policy));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_generation));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_timestamp_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_compute_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_wall_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.epoch_thread_cpu_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.capture_lock_wait_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.capture_lock_hold_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.work_snapshot_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.suffix_prepare_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.model_build_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.allocation_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.publish_lock_wait_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.publish_lock_hold_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.hardware_apply_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.trace_build_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.trace_lock_wait_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.trace_lock_hold_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.template_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.deadline_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_generation));
		output.SetValue(col++, count, Value(snapshot.event_kind));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.model_valid));
		output.SetValue(col++, count, Value(snapshot.model_error));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.demand_cap));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.remaining_work_units));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.selected_throughput));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.throughput_is_live));
		output.SetValue(col++, count, Value(snapshot.historical_throughput_level));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.historical_throughput_sample_count));
		output.SetValue(col++, count, Value(snapshot.continuation_level));
		output.SetValue(col++, count, Value(snapshot.continuation_kind));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.continuation_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.continuation_p90));
		output.SetValue(col++, count, Value(snapshot.suffix_primary_level));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.suffix_exact_sample_count));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.suffix_scale_sample_count));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.suffix_global_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.suffix_exact_weight));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.suffix_scale_weight));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.suffix_global_weight));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.suffix_bucket_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.suffix_mean_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.suffix_p90_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.mandatory_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.optional_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.residual_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.liveness_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.planned_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.assigned_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_mandatory_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_o1_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_o2_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_o3_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_liveness_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_parked_workers));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_core_probes));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_uncore_probes));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_forced_uncore_probes));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.energy_optional_only_socket_available));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_epochs_without_optional_only_socket));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_profile_fallback_count));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_immature_pair_rejections));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.energy_domain_pair_promotions));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.energy_mandatory_footprint_j));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.energy_optional_incremental_j));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_service_rate));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_active_power_w));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_epoch_energy_j));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_optional_risk));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.first_mandatory_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.first_optional_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.last_mandatory_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.last_optional_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.next_mandatory_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.next_optional_gain));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_pipeline_finish_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_query_finish_mean_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_query_finish_p90_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.predicted_sla_cost));
		output.SetValue(col++, count, Value::UBIGINT(data.dropped_count));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQuerySLAEnergyWorkerEpochsBind(ClientContext &context,
	                                                                 TableFunctionBindInput &input,
	                                                                 vector<LogicalType> &return_types,
	                                                                 vector<string> &names) {
	for (auto name : {"epoch_generation", "epoch_timestamp_ns", "db_query_id", "request_id", "template_id",
	                  "scale_factor", "pipeline_id", "pipeline_signature_hash", "pipeline_generation", "worker_id"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UBIGINT);
	}
	for (auto name : {"logical_cpu", "socket_id", "physical_core_id"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::INTEGER);
	}
	for (auto name : {"mandatory", "optional", "liveness", "exploration"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::BOOLEAN);
	}
	for (auto name : {"normal_core_khz", "normal_uncore_khz", "execution_core_khz", "execution_uncore_khz",
	                  "applied_core_khz", "applied_uncore_khz"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::UINTEGER);
	}
	names.emplace_back("trace_dropped_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQuerySLAEnergyWorkerEpochsInit(ClientContext &context,
	                                                                             TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQuerySLAEnergyWorkerEpochsData>();
	auto &scheduler = DatabaseInstance::GetDatabase(context).GetQuerySLAScheduler();
	result->snapshots = scheduler.GetEnergyWorkerEpochTrace();
	result->dropped_count = scheduler.EnergyWorkerEpochTraceDroppedCount();
	return std::move(result);
}

static void DuckDBQuerySLAEnergyWorkerEpochsFunction(ClientContext &context, TableFunctionInput &data_p,
	                                                  DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQuerySLAEnergyWorkerEpochsData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.snapshots[data.offset++];
		idx_t col = 0;
		for (auto value : {row.epoch_generation, row.epoch_timestamp_ns, row.db_query_id, row.request_id,
		                   row.template_id, row.scale_factor, static_cast<uint64_t>(row.pipeline_id),
		                   row.pipeline_signature_hash, row.pipeline_generation, static_cast<uint64_t>(row.worker_id)}) {
			output.SetValue(col++, count, Value::UBIGINT(value));
		}
		output.SetValue(col++, count, Value::INTEGER(row.logical_cpu));
		output.SetValue(col++, count, Value::INTEGER(row.socket_id));
		output.SetValue(col++, count, Value::INTEGER(row.physical_core_id));
		output.SetValue(col++, count, Value::BOOLEAN(row.mandatory));
		output.SetValue(col++, count, Value::BOOLEAN(row.optional));
		output.SetValue(col++, count, Value::BOOLEAN(row.liveness));
		output.SetValue(col++, count, Value::BOOLEAN(row.exploration));
		output.SetValue(col++, count, Value::UINTEGER(row.normal_hardware.core_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(row.normal_hardware.uncore_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(row.execution_hardware.core_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(row.execution_hardware.uncore_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(row.applied_hardware.core_frequency_khz));
		output.SetValue(col++, count, Value::UINTEGER(row.applied_hardware.uncore_frequency_khz));
		output.SetValue(col++, count, Value::UBIGINT(data.dropped_count));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> DuckDBQuerySLAEnergyHardwareBind(ClientContext &context,
	                                                             TableFunctionBindInput &input,
	                                                             vector<LogicalType> &return_types,
	                                                             vector<string> &names) {
	names = {"active", "hardware_control_enabled", "published_generation", "applied_generation",
	         "registered_workers", "physical_core_domains", "socket_domains", "msr_write_count",
	         "verification_failure_count", "restoration_attempt_count", "restoration_verified_count",
	         "restoration_failure_count", "exploration_core_probe_epochs", "exploration_optional_uncore_probe_epochs",
	         "exploration_forced_uncore_probe_epochs", "epochs_without_optional_only_socket", "power_model_loaded",
	         "status", "restoration_status"};
	return_types = {LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQuerySLAEnergyHardwareInit(ClientContext &context,
	                                                                         TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQuerySLAEnergyHardwareData>();
	auto &scheduler = DatabaseInstance::GetDatabase(context).GetQuerySLAScheduler();
	result->snapshot = scheduler.GetHardwareManager().GetSnapshot();
	result->exploration = scheduler.GetExplorationDiagnostics();
	return std::move(result);
}

static void DuckDBQuerySLAEnergyHardwareFunction(ClientContext &context, TableFunctionInput &data_p,
	                                              DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQuerySLAEnergyHardwareData>();
	if (data.emitted) {
		output.SetCardinality(0);
		return;
	}
	const auto &row = data.snapshot;
	idx_t col = 0;
	output.SetValue(col++, 0, Value::BOOLEAN(row.active));
	output.SetValue(col++, 0, Value::BOOLEAN(row.hardware_control_enabled));
	output.SetValue(col++, 0, Value::UBIGINT(row.published_generation));
	output.SetValue(col++, 0, Value::UBIGINT(row.applied_generation));
	output.SetValue(col++, 0, Value::UBIGINT(row.registered_workers));
	output.SetValue(col++, 0, Value::UBIGINT(row.physical_core_domains));
	output.SetValue(col++, 0, Value::UBIGINT(row.socket_domains));
	output.SetValue(col++, 0, Value::UBIGINT(row.msr_write_count));
	output.SetValue(col++, 0, Value::UBIGINT(row.verification_failure_count));
	output.SetValue(col++, 0, Value::UBIGINT(row.restoration_attempt_count));
	output.SetValue(col++, 0, Value::UBIGINT(row.restoration_verified_count));
	output.SetValue(col++, 0, Value::UBIGINT(row.restoration_failure_count));
	output.SetValue(col++, 0, Value::UBIGINT(data.exploration.core_probe_epochs));
	output.SetValue(col++, 0, Value::UBIGINT(data.exploration.optional_uncore_probe_epochs));
	output.SetValue(col++, 0, Value::UBIGINT(data.exploration.forced_uncore_probe_epochs));
	output.SetValue(col++, 0, Value::UBIGINT(data.exploration.epochs_without_optional_only_socket));
	output.SetValue(col++, 0, Value::BOOLEAN(row.power_model_loaded));
	output.SetValue(col++, 0, Value(row.status));
	output.SetValue(col++, 0, Value(row.restoration_status));
	output.SetCardinality(1);
	data.emitted = true;
}

static unique_ptr<FunctionData> DuckDBQueryPipelineEventsBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("db_query_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("template_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("activation_group_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("pipeline_id");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("event_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("event_state");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("timestamp_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("total_tasks");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("finished_tasks");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_tasks");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("preferred_parallelism");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("effective_max_threads");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("source_max_threads");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("scheduler_threads");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("parallel");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("parallel_blocker");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("work_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("parallelism_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("scalable");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("source_input_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_input_confidence");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("total_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("total_chunks_equiv");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("total_native_units");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("native_unit");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("completed_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("completed_chunks_equiv");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("completed_native_units");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("remaining_chunks_equiv");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("throughput_completed_chunks_equiv");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("throughput_worker_time_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("throughput_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("selected_throughput_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("selected_throughput_is_live");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("selected_single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("historical_throughput_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("historical_throughput_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("historical_throughput_mean");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("historical_throughput_ewma");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("lifecycle_tail_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("lifecycle_tail_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("lifecycle_tail_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("lifecycle_tail_mean_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("lifecycle_tail_p50_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("lifecycle_tail_p90_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("source_work_class");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("continuation_valid");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("continuation_level");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("continuation_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("continuation_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("continuation_mean");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("continuation_p50");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("continuation_p90");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("source_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sink_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("operator_type_sequence");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryPipelineEventsInit(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryPipelineEventsData>();
	result->snapshots = QueryPipelineDebug::GetDebugSnapshot();
	return std::move(result);
}

static void DuckDBQueryPipelineEventsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueryPipelineEventsData>();
	idx_t count = 0;
	while (data.offset < data.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &snapshot = data.snapshots[data.offset++];
		idx_t col = 0;
		output.SetValue(col++, count, Value::UBIGINT(snapshot.db_query_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.request_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.template_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scale_factor));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.activation_group_id));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.pipeline_id));
		output.SetValue(col++, count, Value(snapshot.event_kind));
		output.SetValue(col++, count, Value(snapshot.event_state));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.timestamp_ns));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.total_tasks));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.finished_tasks));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.remaining_tasks));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.preferred_parallelism));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.effective_max_threads));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.source_max_threads));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.scheduler_threads));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.parallel));
		output.SetValue(col++, count, Value(snapshot.parallel_blocker));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.work_valid));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.parallelism_valid));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.scalable));
		output.SetValue(col++, count, Value(snapshot.source_input_kind));
		output.SetValue(col++, count, Value(snapshot.source_input_confidence));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.total_rows));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.total_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.total_native_units));
		output.SetValue(col++, count, Value(snapshot.native_unit));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.completed_rows));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.completed_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.completed_native_units));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.remaining_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.throughput_completed_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.throughput_worker_time_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.single_worker_chunks_per_s));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.throughput_valid));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.selected_throughput_valid));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.selected_throughput_is_live));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.selected_single_worker_chunks_per_s));
		output.SetValue(col++, count, Value(snapshot.historical_throughput_level));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.historical_throughput_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.historical_throughput_mean));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.historical_throughput_ewma));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.lifecycle_tail_valid));
		output.SetValue(col++, count, Value(snapshot.lifecycle_tail_level));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.lifecycle_tail_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.lifecycle_tail_mean_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.lifecycle_tail_p50_ns));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.lifecycle_tail_p90_ns));
		output.SetValue(col++, count, Value(snapshot.source_work_class));
		output.SetValue(col++, count, Value::BOOLEAN(snapshot.continuation_valid));
		output.SetValue(col++, count, Value(snapshot.continuation_level));
		output.SetValue(col++, count, Value(snapshot.continuation_kind));
		output.SetValue(col++, count, Value::UBIGINT(snapshot.continuation_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.continuation_mean));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.continuation_p50));
		output.SetValue(col++, count, Value::DOUBLE(snapshot.continuation_p90));
		output.SetValue(col++, count, Value(snapshot.source_type));
		output.SetValue(col++, count, Value(snapshot.sink_type));
		output.SetValue(col++, count, Value(snapshot.operator_type_sequence));
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBQueryRequestProfilesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_debug_query_request_profiles", {}, DuckDBQueryRequestProfilesFunction,
	                              DuckDBQueryRequestProfilesBind, DuckDBQueryRequestProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_pipeline_profiles", {},
	                              DuckDBQueryRequestPipelineProfilesFunction, DuckDBQueryRequestPipelineProfilesBind,
	                              DuckDBQueryRequestPipelineProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_pipeline_hardware_profiles", {},
	                              DuckDBQueryRequestPipelineHardwareProfilesFunction,
	                              DuckDBQueryRequestPipelineHardwareProfilesBind,
	                              DuckDBQueryRequestPipelineHardwareProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_samples", {}, DuckDBQueryRequestSamplesFunction,
	                              DuckDBQueryRequestSamplesBind, DuckDBQueryRequestSamplesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_pipeline_instances", {},
	                              DuckDBQueryRequestPipelineInstancesFunction,
	                              DuckDBQueryRequestPipelineInstancesBind,
	                              DuckDBQueryRequestPipelineInstancesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_internal_event_profiles", {},
	                              DuckDBQueryRequestInternalEventProfilesFunction,
	                              DuckDBQueryRequestInternalEventProfilesBind,
	                              DuckDBQueryRequestInternalEventProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_internal_event_instances", {},
	                              DuckDBQueryRequestInternalEventInstancesFunction,
	                              DuckDBQueryRequestInternalEventInstancesBind,
	                              DuckDBQueryRequestInternalEventInstancesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_continuation_profiles", {},
	                              DuckDBQueryRequestContinuationProfilesFunction,
	                              DuckDBQueryRequestContinuationProfilesBind,
	                              DuckDBQueryRequestContinuationProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_throughput_profiles", {},
	                              DuckDBQueryRequestThroughputProfilesFunction,
	                              DuckDBQueryRequestThroughputProfilesBind,
	                              DuckDBQueryRequestThroughputProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_downstream_suffix_profiles", {},
	                              DuckDBQueryRequestDownstreamSuffixProfilesFunction,
	                              DuckDBQueryRequestDownstreamSuffixProfilesBind,
	                              DuckDBQueryRequestDownstreamSuffixProfilesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_admission", {}, DuckDBQueryAdmissionFunction,
	                              DuckDBQueryAdmissionBind, DuckDBQueryAdmissionInit));
	set.AddFunction(TableFunction("duckdb_debug_query_admission_events", {}, DuckDBQueryAdmissionEventsFunction,
	                              DuckDBQueryAdmissionEventsBind, DuckDBQueryAdmissionEventsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_activation_events", {}, DuckDBQueryActivationEventsFunction,
	                              DuckDBQueryActivationEventsBind, DuckDBQueryActivationEventsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_sla_scheduler", {}, DuckDBQuerySLASchedulerFunction,
	                              DuckDBQuerySLASchedulerBind, DuckDBQuerySLASchedulerInit));
	set.AddFunction(TableFunction("duckdb_debug_query_sla_scheduler_epochs", {},
	                              DuckDBQuerySLASchedulerEpochsFunction, DuckDBQuerySLASchedulerEpochsBind,
	                              DuckDBQuerySLASchedulerEpochsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_sla_energy_worker_epochs", {},
	                              DuckDBQuerySLAEnergyWorkerEpochsFunction, DuckDBQuerySLAEnergyWorkerEpochsBind,
	                              DuckDBQuerySLAEnergyWorkerEpochsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_sla_energy_hardware", {},
	                              DuckDBQuerySLAEnergyHardwareFunction, DuckDBQuerySLAEnergyHardwareBind,
	                              DuckDBQuerySLAEnergyHardwareInit));
	set.AddFunction(TableFunction("duckdb_debug_query_stride_scheduler", {}, DuckDBQueryStrideSchedulerFunction,
	                              DuckDBQueryStrideSchedulerBind, DuckDBQueryStrideSchedulerInit));
	set.AddFunction(TableFunction("duckdb_debug_query_stride_tuning", {}, DuckDBQueryStrideTuningFunction,
	                              DuckDBQueryStrideTuningBind, DuckDBQueryStrideTuningInit));
	set.AddFunction(TableFunction("duckdb_debug_query_pipeline_events", {}, DuckDBQueryPipelineEventsFunction,
	                              DuckDBQueryPipelineEventsBind, DuckDBQueryPipelineEventsInit));
}

} // namespace duckdb
