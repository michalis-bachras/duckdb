//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_query_request_profiles.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

namespace duckdb {

struct DuckDBQueryRequestProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestProfileSnapshot> profiles;
	idx_t offset = 0;
};

struct DuckDBQueryRequestPipelineProfilesData : public GlobalTableFunctionState {
	vector<QueryRequestPipelineProfileSnapshot> profiles;
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
	result->profiles = QueryRequestProfileStore::Get().GetQueryProfilesSnapshot();
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
	names.emplace_back("mean_downstream_suffix_ns");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_task_count");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_source_max_threads");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_planned_input_rows");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("mean_planned_input_chunks_equiv");
	return_types.emplace_back(LogicalType::DOUBLE);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestPipelineProfilesInit(ClientContext &context,
                                                                                   TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestPipelineProfilesData>();
	result->profiles = QueryRequestProfileStore::Get().GetPipelineProfilesSnapshot();
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
		output.SetValue(col++, count, Value::UBIGINT(estimate.pipeline_signature_hash));
		output.SetValue(col++, count, Value::UBIGINT(estimate.sample_count));
		output.SetValue(col++, count, Value(estimate.pipeline_signature));
		output.SetValue(col++, count, Value(estimate.operator_type_sequence));
		output.SetValue(col++, count, Value(estimate.source_type));
		output.SetValue(col++, count, Value(estimate.sink_type));
		output.SetValue(col++, count, Value(estimate.source_input_kind));
		output.SetValue(col++, count, Value(estimate.source_input_confidence));
		output.SetValue(col++, count, Value(estimate.planned_input_native_unit));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_task_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.p90_task_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_lifecycle_runtime_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_downstream_suffix_ns));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_task_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_source_max_threads));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_planned_input_rows));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_planned_input_chunks_equiv));
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
	result->samples = QueryRequestProfileStore::Get().GetQuerySamplesSnapshot();
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
	names.emplace_back("downstream_suffix_ns");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBQueryRequestPipelineInstancesInit(ClientContext &context,
                                                                                    TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueryRequestPipelineInstancesData>();
	result->instances = QueryRequestProfileStore::Get().GetPipelineInstancesSnapshot();
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
		output.SetValue(col++, count, Value(instance.source_input_kind));
		output.SetValue(col++, count, Value(instance.source_input_confidence));
		output.SetValue(col++, count, Value(instance.planned_input_native_unit));
		output.SetValue(col++, count, Value::UBIGINT(instance.task_count));
		output.SetValue(col++, count, Value::UBIGINT(instance.source_max_threads));
		output.SetValue(col++, count, Value::UBIGINT(instance.planned_input_rows));
		output.SetValue(col++, count, Value::UBIGINT(instance.planned_input_chunks_equiv));
		output.SetValue(col++, count, Value::UBIGINT(instance.source_estimated_cardinality));
		output.SetValue(col++, count, Value::UBIGINT(instance.sink_estimated_cardinality));
		output.SetValue(col++, count, Value::UBIGINT(instance.start_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.tasks_done_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.finish_done_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.task_runtime_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.lifecycle_runtime_ns));
		output.SetValue(col++, count, Value::UBIGINT(instance.downstream_suffix_ns));
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
	set.AddFunction(TableFunction("duckdb_debug_query_request_samples", {}, DuckDBQueryRequestSamplesFunction,
	                              DuckDBQueryRequestSamplesBind, DuckDBQueryRequestSamplesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_pipeline_instances", {},
	                              DuckDBQueryRequestPipelineInstancesFunction,
	                              DuckDBQueryRequestPipelineInstancesBind,
	                              DuckDBQueryRequestPipelineInstancesInit));
}

} // namespace duckdb
