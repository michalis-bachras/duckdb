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
#include "duckdb/parallel/query_pipeline_debug.hpp"

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
	names.emplace_back("throughput_sample_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("mean_single_worker_chunks_per_s");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("ewma_single_worker_chunks_per_s");
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
		output.SetValue(col++, count, Value::UBIGINT(estimate.pipeline_id));
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
		output.SetValue(col++, count, Value::UBIGINT(estimate.throughput_sample_count));
		output.SetValue(col++, count, Value::DOUBLE(estimate.mean_single_worker_chunks_per_s));
		output.SetValue(col++, count, Value::DOUBLE(estimate.ewma_single_worker_chunks_per_s));
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
		output.SetValue(col++, count, Value::UBIGINT(instance.downstream_suffix_ns));
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
	set.AddFunction(TableFunction("duckdb_debug_query_request_samples", {}, DuckDBQueryRequestSamplesFunction,
	                              DuckDBQueryRequestSamplesBind, DuckDBQueryRequestSamplesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_request_pipeline_instances", {},
	                              DuckDBQueryRequestPipelineInstancesFunction,
	                              DuckDBQueryRequestPipelineInstancesBind,
	                              DuckDBQueryRequestPipelineInstancesInit));
	set.AddFunction(TableFunction("duckdb_debug_query_admission", {}, DuckDBQueryAdmissionFunction,
	                              DuckDBQueryAdmissionBind, DuckDBQueryAdmissionInit));
	set.AddFunction(TableFunction("duckdb_debug_query_admission_events", {}, DuckDBQueryAdmissionEventsFunction,
	                              DuckDBQueryAdmissionEventsBind, DuckDBQueryAdmissionEventsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_activation_events", {}, DuckDBQueryActivationEventsFunction,
	                              DuckDBQueryActivationEventsBind, DuckDBQueryActivationEventsInit));
	set.AddFunction(TableFunction("duckdb_debug_query_pipeline_events", {}, DuckDBQueryPipelineEventsFunction,
	                              DuckDBQueryPipelineEventsBind, DuckDBQueryPipelineEventsInit));
}

} // namespace duckdb
