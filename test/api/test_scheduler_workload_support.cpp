#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/pending_query_notification.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

#include <chrono>

using namespace duckdb;

namespace {

static uint64_t CurrentNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static QueryRequestMetadata Metadata(uint64_t request_id, uint64_t template_id = 6) {
	QueryRequestMetadata metadata;
	metadata.valid = true;
	metadata.request_id = request_id;
	metadata.template_id = template_id;
	metadata.scale_factor = 10;
	metadata.sla_tag = 30;
	metadata.sla_penalty_per_s = 1;
	metadata.deadline_ns = CurrentNs() + 60000000000ULL;
	return metadata;
}

static unique_ptr<PendingQueryResult> TypedPending(PreparedStatement &prepared, const QueryRequestMetadata &metadata,
	                                               shared_ptr<PendingQueryNotification> notification) {
	case_insensitive_map_t<BoundParameterData> values;
	PendingQueryParameters parameters;
	parameters.query_parameters.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	parameters.request_metadata = metadata;
	parameters.has_request_metadata = true;
	parameters.notification = std::move(notification);
	return prepared.PendingQuery(values, std::move(parameters));
}

static void Configure(Connection &connection, idx_t admission_cap) {
	REQUIRE_NO_FAIL(connection.Query("SET query_request_profiling_enable=true"));
	REQUIRE_NO_FAIL(connection.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(connection.Query("SET query_activation_debug_enable=false"));
	REQUIRE_NO_FAIL(connection.Query("SET query_admission_nonblocking_enable=true"));
	REQUIRE_NO_FAIL(connection.Query("SET query_admission_max_active=" + std::to_string(admission_cap)));
}

static vector<PendingQueryEvent> Drain(PendingQueryNotificationQueue &queue) {
	vector<PendingQueryEvent> result;
	PendingQueryEvent event;
	while (queue.TryPop(event)) {
		result.push_back(std::move(event));
	}
	return result;
}

static bool HasEvent(const vector<PendingQueryEvent> &events, uint64_t token, PendingQueryEventType type) {
	for (const auto &event : events) {
		if (event.request_token == token && event.type == type) {
			return true;
		}
	}
	return false;
}

} // namespace

TEST_CASE("Pending query notifications are ordered and published once", "[api][scheduler_workload]") {
	auto queue = make_shared_ptr<PendingQueryNotificationQueue>();
	PendingQueryNotification notification(42, queue);
	REQUIRE(notification.Publish(PendingQueryEventType::ADMITTED));
	REQUIRE(!notification.Publish(PendingQueryEventType::ADMITTED));
	REQUIRE(notification.Publish(PendingQueryEventType::RESULT_READY));
	REQUIRE(!notification.Publish(PendingQueryEventType::ERROR, "late error"));
	auto events = Drain(*queue);
	REQUIRE(events.size() == 2);
	REQUIRE(events[0].request_token == 42);
	REQUIRE(events[0].type == PendingQueryEventType::ADMITTED);
	REQUIRE(events[1].type == PendingQueryEventType::RESULT_READY);
}

TEST_CASE("Typed pending queries publish complete materialized lifecycle", "[api][scheduler_workload]") {
	DuckDB db;
	Connection root(db);
	REQUIRE_NO_FAIL(root.Query("SET threads=2"));
	REQUIRE_NO_FAIL(root.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(root.Query("SET query_worker_only_execution_enable=true"));
	Connection connection(db);
	Configure(connection, 128);
	auto prepared = connection.Prepare("SELECT SUM(i) FROM range(1000000) t(i)");
	REQUIRE(!prepared->HasError());
	auto queue = make_shared_ptr<PendingQueryNotificationQueue>();
	auto pending = TypedPending(*prepared, Metadata(1), make_shared_ptr<PendingQueryNotification>(1, queue));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE_NO_FAIL(*result);
	auto events = Drain(*queue);
	REQUIRE(HasEvent(events, 1, PendingQueryEventType::ADMITTED));
	REQUIRE(HasEvent(events, 1, PendingQueryEventType::EXECUTION_STARTED));
	REQUIRE(HasEvent(events, 1, PendingQueryEventType::EXECUTION_READY));
	REQUIRE(HasEvent(events, 1, PendingQueryEventType::ADMISSION_RELEASED));
	REQUIRE(HasEvent(events, 1, PendingQueryEventType::RESULT_READY));
}

TEST_CASE("Worker execution errors publish execution and terminal notifications", "[api][scheduler_workload]") {
	DuckDB db;
	Connection root(db);
	REQUIRE_NO_FAIL(root.Query("SET threads=2"));
	REQUIRE_NO_FAIL(root.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(root.Query("SET query_worker_only_execution_enable=true"));
	Connection connection(db);
	Configure(connection, 128);
	auto prepared = connection.Prepare("SELECT CAST(i AS TINYINT) FROM range(1000000) t(i)");
	REQUIRE(!prepared->HasError());
	auto queue = make_shared_ptr<PendingQueryNotificationQueue>();
	auto pending = TypedPending(*prepared, Metadata(3), make_shared_ptr<PendingQueryNotification>(3, queue));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->HasError());
	auto events = Drain(*queue);
	REQUIRE(HasEvent(events, 3, PendingQueryEventType::EXECUTION_ERROR));
	REQUIRE(HasEvent(events, 3, PendingQueryEventType::ERROR));
}

TEST_CASE("Nonblocking admission wakes a queued pending query", "[api][scheduler_workload]") {
	DuckDB db;
	Connection root(db);
	REQUIRE_NO_FAIL(root.Query("SET threads=2"));
	REQUIRE_NO_FAIL(root.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(root.Query("SET query_worker_only_execution_enable=true"));
	Connection first(db);
	Connection second(db);
	Configure(first, 1);
	Configure(second, 1);
	auto first_prepared = first.Prepare("SELECT SUM(i) FROM range(1000000) t(i)");
	auto second_prepared = second.Prepare("SELECT SUM(i) FROM range(1000000) t(i)");
	auto queue = make_shared_ptr<PendingQueryNotificationQueue>();
	auto first_pending =
	    TypedPending(*first_prepared, Metadata(1), make_shared_ptr<PendingQueryNotification>(1, queue));
	auto second_pending =
	    TypedPending(*second_prepared, Metadata(2), make_shared_ptr<PendingQueryNotification>(2, queue));
	REQUIRE(second_pending->CheckPulse() == PendingExecutionResult::WAITING_FOR_ADMISSION);
	REQUIRE_NO_FAIL(*first_pending->Execute());
	REQUIRE(second_pending->CheckPulse() != PendingExecutionResult::WAITING_FOR_ADMISSION);
	REQUIRE_NO_FAIL(*second_pending->Execute());
	auto events = Drain(*queue);
	REQUIRE(HasEvent(events, 2, PendingQueryEventType::ADMISSION_QUEUED));
	REQUIRE(HasEvent(events, 2, PendingQueryEventType::ADMITTED));
	REQUIRE(HasEvent(events, 2, PendingQueryEventType::RESULT_READY));
}

TEST_CASE("Destroying a queued client publishes cancellation", "[api][scheduler_workload]") {
	DuckDB db;
	Connection root(db);
	REQUIRE_NO_FAIL(root.Query("SET threads=2"));
	REQUIRE_NO_FAIL(root.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(root.Query("SET query_worker_only_execution_enable=true"));
	Connection first(db);
	Configure(first, 1);
	auto first_prepared = first.Prepare("SELECT SUM(i) FROM range(1000000) t(i)");
	auto queue = make_shared_ptr<PendingQueryNotificationQueue>();
	auto first_pending =
	    TypedPending(*first_prepared, Metadata(1), make_shared_ptr<PendingQueryNotification>(1, queue));
	{
		auto second = make_uniq<Connection>(db);
		Configure(*second, 1);
		auto second_prepared = second->Prepare("SELECT SUM(i) FROM range(1000000) t(i)");
		auto second_pending =
		    TypedPending(*second_prepared, Metadata(2), make_shared_ptr<PendingQueryNotification>(2, queue));
		REQUIRE(second_pending->CheckPulse() == PendingExecutionResult::WAITING_FOR_ADMISSION);
		second_pending.reset();
		second_prepared.reset();
		second.reset();
	}
	REQUIRE_NO_FAIL(*first_pending->Execute());
	auto events = Drain(*queue);
	REQUIRE(HasEvent(events, 2, PendingQueryEventType::ADMISSION_QUEUED));
	REQUIRE(HasEvent(events, 2, PendingQueryEventType::CANCELLED));
}

TEST_CASE("Scheduler profile snapshots round trip bounded model state", "[api][scheduler_workload]") {
	DuckDB db;
	auto &store = db.instance->GetQueryRequestProfileStore();
	QueryRequestMetadata metadata = Metadata(1);
	metadata.db_query_id = 10;
	metadata.query_start_ns = 1000;
	PipelineProfilingInfo pipeline;
	pipeline.pipeline_id = 7;
	pipeline.pipeline_signature_hash = 99;
	pipeline.pipeline_signature = "test-signature";
	pipeline.operator_type_sequence = "TABLE_SCAN>PROJECTION";
	pipeline.source_type = "TABLE_SCAN";
	pipeline.sink_type = "RESULT_COLLECTOR";
	pipeline.source_work_class.source_type = PhysicalOperatorType::TABLE_SCAN;
	pipeline.source_work_class.work_kind = SourceThroughputKind::TABLE_SCAN_ROWS;
	pipeline.source_input_kind = "table_scan_rows";
	pipeline.source_input_confidence = "exact";
	pipeline.planned_input_native_unit = "row";
	pipeline.source_max_threads = 4;
	pipeline.planned_input_rows = 4096;
	pipeline.planned_input_chunks_equiv = 2;
	pipeline.task_count = 2;
	pipeline.worker_task_count = 2;
	pipeline.worker_task_duration_ns = 1000;
	pipeline.start_ns = 1100;
	pipeline.tasks_done_ns = 1800;
	pipeline.finish_done_ns = 1900;
	InternalEventProfilingInfo internal_event;
	internal_event.pipeline_id = 7;
	internal_event.pipeline_signature_hash = 99;
	internal_event.event_position = 1;
	internal_event.event_type = "hash_join_finalize";
	internal_event.native_unit = "hash_table_chunk";
	internal_event.total_work_units = 100;
	internal_event.completed_work_units = 100;
	internal_event.worker_time_ns = 200;
	internal_event.start_ns = 1200;
	internal_event.finish_ns = 1600;
	store.RecordQueryCompletion(metadata, 2000, {pipeline}, {internal_event});
	PipelineProfileIdentity hardware_identity;
	hardware_identity.pipeline_id = 7;
	hardware_identity.pipeline_signature_hash = 99;
	hardware_identity.valid = true;
	QueryRequestHardwareConfiguration hardware;
	hardware.core_frequency_khz = 3000000;
	hardware.uncore_frequency_khz = 2800000;
	store.RecordPipelineHardwareThroughput(6, 10, hardware_identity, hardware, 2, 1000, true);
	store.RecordPipelineHardwareEnergy(6, 10, hardware_identity, hardware, 1000, 0.001, 0.0001, true);
	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.PipelineProfileCount() == 1);
	REQUIRE(store.GetInternalEventProfilesSnapshot().size() == 1);
	REQUIRE(store.GetPipelineHardwareProfilesSnapshot().size() == 1);
	auto hardware_profile = store.GetPipelineHardwareProfilesSnapshot()[0];
	REQUIRE(hardware_profile.mean_active_power_w > 0);
	REQUIRE(hardware_profile.mean_charged_power_w > hardware_profile.mean_active_power_w);
	REQUIRE(store.GetInternalEventInstancesSnapshot().size() == 1);
	auto path = TestCreatePath("scheduler_profile_snapshot.bin");
	store.ExportSnapshot(db.instance->GetFileSystem(), path);
	store.Clear();
	REQUIRE(store.QueryProfileCount() == 0);
	REQUIRE(store.GetInternalEventProfilesSnapshot().empty());
	store.ImportSnapshot(db.instance->GetFileSystem(), path);
	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.PipelineProfileCount() == 1);
	REQUIRE(store.GetInternalEventProfilesSnapshot().size() == 1);
	REQUIRE(store.GetQuerySamplesSnapshot().empty());
	REQUIRE(store.GetPipelineInstancesSnapshot().empty());
	REQUIRE(store.GetInternalEventInstancesSnapshot().empty());
	REQUIRE(store.GetPipelineHardwareProfilesSnapshot().size() == 1);
	QueryRequestPipelineProfileEstimate estimate;
	REQUIRE(store.TryGetPipelineEstimate(6, 10, 7, 99, estimate));
	REQUIRE(estimate.throughput_sample_count == 1);
	REQUIRE(estimate.continuation_sample_count == 1);
	PipelineProfileIdentity identity;
	identity.pipeline_id = 7;
	identity.pipeline_signature_hash = 99;
	identity.valid = true;
	auto internal_estimate =
	    store.ResolveInternalEvent(6, 10, identity, 1, "hash_join_finalize", "hash_table_chunk");
	REQUIRE(internal_estimate.throughput.valid);
	REQUIRE(internal_estimate.throughput.sample_count == 1);
	REQUIRE(internal_estimate.throughput.mean_work_units_per_s == 500000000.0);
	REQUIRE(internal_estimate.continuation.valid);
	REQUIRE(internal_estimate.continuation.sample_count == 1);
	REQUIRE(internal_estimate.continuation.p90 == 4.0);
	REQUIRE(internal_estimate.tail_to_pipeline_end.valid);
	REQUIRE(internal_estimate.tail_to_pipeline_end.sample_count == 1);
	REQUIRE(internal_estimate.tail_to_pipeline_end.p90_ns == 300.0);
	auto scale_estimate =
	    store.ResolveInternalEvent(999, 10, identity, 1, "hash_join_finalize", "hash_table_chunk");
	REQUIRE(scale_estimate.throughput.level == PipelineThroughputEstimateLevel::SCALE_FACTOR);
	REQUIRE(scale_estimate.continuation.level == ContinuationEstimateLevel::SCALE_FACTOR);
	REQUIRE(scale_estimate.tail_to_pipeline_end.level == PipelineLifecycleTailEstimateLevel::SCALE_FACTOR);
	auto global_estimate =
	    store.ResolveInternalEvent(999, 30, identity, 1, "hash_join_finalize", "hash_table_chunk");
	REQUIRE(global_estimate.throughput.level == PipelineThroughputEstimateLevel::GLOBAL_COMPATIBLE);
	REQUIRE(global_estimate.continuation.level == ContinuationEstimateLevel::GLOBAL_RAW);
	REQUIRE(global_estimate.tail_to_pipeline_end.level == PipelineLifecycleTailEstimateLevel::GLOBAL);
	db.instance->GetFileSystem().RemoveFile(path);
}
