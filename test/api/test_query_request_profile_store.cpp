#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <mutex>
#include <thread>

using namespace duckdb;
using namespace std;

namespace {

static uint64_t CurrentNs() {
	return static_cast<uint64_t>(
	    chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count());
}

static uint64_t StableStringHash64(const string &value) {
	uint64_t hash = 1469598103934665603ULL;
	for (idx_t i = 0; i < value.size(); i++) {
		hash ^= static_cast<unsigned char>(value[i]);
		hash *= 1099511628211ULL;
	}
	return hash;
}

static string PipelineSignature(const PipelineProfilingInfo &profile) {
	return StringUtil::Format(
	    "%s|source=%s|sink=%s|input=%s|confidence=%s|native=%s|source_card=%llu|sink_card=%llu",
	    profile.operator_type_sequence, profile.source_type, profile.sink_type, profile.source_input_kind,
	    profile.source_input_confidence, profile.planned_input_native_unit,
	    static_cast<unsigned long long>(profile.source_estimated_cardinality),
	    static_cast<unsigned long long>(profile.sink_estimated_cardinality));
}

static QueryRequestMetadata Metadata(uint64_t request_id, uint64_t template_id, uint64_t scale_factor,
                                     uint64_t start_ns, uint64_t deadline_ns) {
	QueryRequestMetadata metadata;
	metadata.valid = true;
	metadata.parse_status = "ok";
	metadata.db_query_id = request_id + 1000;
	metadata.request_id = request_id;
	metadata.template_id = template_id;
	metadata.scale_factor = scale_factor;
	metadata.sla_tag = 30;
	metadata.sla_penalty_per_s = 2;
	metadata.deadline_ns = deadline_ns;
	metadata.query_start_ns = start_ns;
	return metadata;
}

static PipelineProfilingInfo PipelineProfile(uint64_t start_ns) {
	PipelineProfilingInfo profile;
	profile.pipeline_id = 7;
	profile.operator_type_sequence = "TABLE_SCAN>PROJECTION>HASH_GROUP_BY";
	profile.source_type = "TABLE_SCAN";
	profile.sink_type = "HASH_GROUP_BY";
	profile.source_input_kind = "scan_rows";
	profile.source_input_confidence = "exact";
	profile.planned_input_native_unit = "row";
	profile.source_estimated_cardinality = 1000;
	profile.sink_estimated_cardinality = 10;
	profile.source_max_threads = 4;
	profile.planned_input_rows = 1000;
	profile.planned_input_chunks_equiv = 8;
	profile.task_count = 6;
	profile.start_ns = start_ns;
	profile.tasks_done_ns = start_ns + 700;
	profile.finish_done_ns = start_ns + 900;
	return profile;
}

static string MetadataQuery(uint64_t request_id, uint64_t template_id, uint64_t scale_factor, const string &body) {
	auto deadline_ns = CurrentNs() + 60000000000ULL;
	return StringUtil::Format(
	    "/* duckdb_sla_v1 request_id=%llu template_id=%llu scale_factor=%llu sla_tag=30 penalty_per_s=1.0 "
	    "deadline_ns=%llu */ %s",
	    static_cast<unsigned long long>(request_id), static_cast<unsigned long long>(template_id),
	    static_cast<unsigned long long>(scale_factor), static_cast<unsigned long long>(deadline_ns), body);
}

} // namespace

TEST_CASE("Query request profile store aggregates direct observations", "[api]") {
	auto &store = QueryRequestProfileStore::Get();
	store.Clear();

	auto pipeline = PipelineProfile(2000);
	auto signature_hash = StableStringHash64(PipelineSignature(pipeline));
	duckdb::vector<PipelineProfilingInfo> pipelines_1;
	pipelines_1.push_back(pipeline);
	duckdb::vector<PipelineProfilingInfo> pipelines_2;
	pipelines_2.push_back(PipelineProfile(22000));

	store.RecordQueryCompletion(Metadata(1, 101, 10, 1000, 9000), 11000, pipelines_1);
	store.RecordQueryCompletion(Metadata(2, 101, 10, 21000, 29000), 31000, pipelines_2);

	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.PipelineProfileCount() == 1);
	auto query_samples = store.GetQuerySamplesSnapshot();
	REQUIRE(query_samples.size() == 2);
	REQUIRE(query_samples[0].db_query_id == 1001);
	REQUIRE(query_samples[0].request_id == 1);
	REQUIRE(query_samples[0].template_id == 101);
	REQUIRE(query_samples[0].scale_factor == 10);
	REQUIRE(query_samples[0].runtime_ns == 10000);
	REQUIRE(query_samples[0].lateness_ns == 2000);
	REQUIRE(query_samples[0].sla_cost > 0);

	auto pipeline_instances = store.GetPipelineInstancesSnapshot();
	REQUIRE(pipeline_instances.size() == 2);
	REQUIRE(pipeline_instances[0].db_query_id == 1001);
	REQUIRE(pipeline_instances[0].request_id == 1);
	REQUIRE(pipeline_instances[0].pipeline_id == 7);
	REQUIRE(pipeline_instances[0].pipeline_signature_hash == signature_hash);
	REQUIRE(pipeline_instances[0].operator_type_sequence == "TABLE_SCAN>PROJECTION>HASH_GROUP_BY");
	REQUIRE(pipeline_instances[0].task_runtime_ns == 700);
	REQUIRE(pipeline_instances[0].lifecycle_runtime_ns == 900);
	REQUIRE(pipeline_instances[0].downstream_suffix_ns == 8300);

	QueryRequestProfileEstimate query_estimate;
	REQUIRE(store.TryGetQueryEstimate(101, 10, query_estimate));
	REQUIRE(query_estimate.valid);
	REQUIRE(query_estimate.sample_count == 2);
	REQUIRE(query_estimate.mean_runtime_ns == 10000);
	REQUIRE(query_estimate.p50_runtime_ns == 10000);
	REQUIRE(query_estimate.last_runtime_ns == 10000);
	REQUIRE(query_estimate.mean_lateness_ns == 2000);
	REQUIRE(query_estimate.mean_sla_cost > 0);

	QueryRequestPipelineProfileEstimate pipeline_estimate;
	REQUIRE(store.TryGetPipelineEstimate(101, 10, 7, signature_hash, pipeline_estimate));
	REQUIRE(pipeline_estimate.valid);
	REQUIRE(pipeline_estimate.sample_count == 2);
	REQUIRE(pipeline_estimate.pipeline_id == 7);
	REQUIRE(pipeline_estimate.pipeline_signature_hash == signature_hash);
	REQUIRE(pipeline_estimate.operator_type_sequence == "TABLE_SCAN>PROJECTION>HASH_GROUP_BY");
	REQUIRE(pipeline_estimate.source_type == "TABLE_SCAN");
	REQUIRE(pipeline_estimate.sink_type == "HASH_GROUP_BY");
	REQUIRE(pipeline_estimate.mean_task_count == 6);
	REQUIRE(pipeline_estimate.mean_source_max_threads == 4);
	REQUIRE(pipeline_estimate.mean_planned_input_rows == 1000);
	REQUIRE(pipeline_estimate.mean_task_runtime_ns == 700);
	REQUIRE(pipeline_estimate.mean_lifecycle_runtime_ns == 900);
	REQUIRE(pipeline_estimate.mean_downstream_suffix_ns == 8300);

	store.Clear();
	for (idx_t i = 0; i < 105; i++) {
		auto start_ns = 100000 + i * 1000;
		store.RecordQueryCompletion(Metadata(i, 202, 10, start_ns, start_ns + 1000), start_ns + 2000, {});
	}
	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.TryGetQueryEstimate(202, 10, query_estimate));
	REQUIRE(query_estimate.sample_count == 100);

	store.Clear();
}

TEST_CASE("Query request profile store separates repeated local pipeline signatures by pipeline id", "[api]") {
	auto &store = QueryRequestProfileStore::Get();
	store.Clear();

	auto first_pipeline = PipelineProfile(2000);
	auto second_pipeline = PipelineProfile(3000);
	second_pipeline.pipeline_id = 17;
	auto signature_hash = StableStringHash64(PipelineSignature(first_pipeline));
	REQUIRE(StableStringHash64(PipelineSignature(second_pipeline)) == signature_hash);

	duckdb::vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(first_pipeline);
	pipelines.push_back(second_pipeline);

	store.RecordQueryCompletion(Metadata(1, 101, 10, 1000, 9000), 11000, pipelines);

	REQUIRE(store.PipelineProfileCount() == 2);

	QueryRequestPipelineProfileEstimate first_estimate;
	REQUIRE(store.TryGetPipelineEstimate(101, 10, 7, signature_hash, first_estimate));
	REQUIRE(first_estimate.valid);
	REQUIRE(first_estimate.pipeline_id == 7);

	QueryRequestPipelineProfileEstimate second_estimate;
	REQUIRE(store.TryGetPipelineEstimate(101, 10, 17, signature_hash, second_estimate));
	REQUIRE(second_estimate.valid);
	REQUIRE(second_estimate.pipeline_id == 17);

	store.Clear();
}

TEST_CASE("Query request profile store is disabled by default for metadata comments", "[api]") {
	auto &store = QueryRequestProfileStore::Get();
	store.Clear();

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE metadata_disabled_t AS SELECT i FROM range(1000) tbl(i)"));

	auto result = con.Query(MetadataQuery(1, 6, 10, "SELECT count(*) FROM metadata_disabled_t"));
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(store.QueryProfileCount() == 0);
	REQUIRE(store.PipelineProfileCount() == 0);

	auto query_profiles = con.Query("SELECT count(*) FROM duckdb_debug_query_request_profiles()");
	REQUIRE(CHECK_COLUMN(query_profiles, 0, {0}));
	auto pipeline_profiles = con.Query("SELECT count(*) FROM duckdb_debug_query_request_pipeline_profiles()");
	REQUIRE(CHECK_COLUMN(pipeline_profiles, 0, {0}));
}

TEST_CASE("Query request profile store records repeated metadata queries", "[api]") {
	auto &store = QueryRequestProfileStore::Get();
	store.Clear();

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE metadata_profile_t AS SELECT i, i % 7 AS g FROM range(10000) tbl(i)"));

	REQUIRE_NO_FAIL(con.Query("SELECT count(*) FROM metadata_profile_t"));
	REQUIRE(store.QueryProfileCount() == 0);
	REQUIRE(store.PipelineProfileCount() == 0);
	REQUIRE_NO_FAIL(con.Query("SET query_request_profiling_enable=true"));

	string body = "SELECT g, count(*), sum(i) FROM metadata_profile_t GROUP BY g ORDER BY g";
	for (idx_t i = 0; i < 3; i++) {
		auto result = con.Query(MetadataQuery(i, 6, 10, body));
		REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5, 6}));
	}

	QueryRequestProfileEstimate query_estimate;
	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.TryGetQueryEstimate(6, 10, query_estimate));
	REQUIRE(query_estimate.valid);
	REQUIRE(query_estimate.sample_count == 3);
	REQUIRE(query_estimate.mean_runtime_ns > 0);
	REQUIRE(query_estimate.p90_runtime_ns > 0);
	REQUIRE(store.PipelineProfileCount() > 0);

	auto query_profiles =
	    con.Query("SELECT template_id, scale_factor, sample_count FROM duckdb_debug_query_request_profiles()");
	REQUIRE(CHECK_COLUMN(query_profiles, 0, {Value::UBIGINT(6)}));
	REQUIRE(CHECK_COLUMN(query_profiles, 1, {Value::UBIGINT(10)}));
	REQUIRE(CHECK_COLUMN(query_profiles, 2, {Value::UBIGINT(3)}));
	auto pipeline_profiles = con.Query("SELECT count(*) FROM duckdb_debug_query_request_pipeline_profiles()");
	REQUIRE(CHECK_COLUMN(pipeline_profiles, 0, {Value::BIGINT(static_cast<int64_t>(store.PipelineProfileCount()))}));
	auto query_samples =
	    con.Query("SELECT template_id, scale_factor, count(*) FROM duckdb_debug_query_request_samples() "
	              "GROUP BY template_id, scale_factor");
	REQUIRE(CHECK_COLUMN(query_samples, 0, {Value::UBIGINT(6)}));
	REQUIRE(CHECK_COLUMN(query_samples, 1, {Value::UBIGINT(10)}));
	REQUIRE(CHECK_COLUMN(query_samples, 2, {Value::BIGINT(3)}));
	auto pipeline_instances =
	    con.Query("SELECT count(*), count(DISTINCT db_query_id), count(DISTINCT pipeline_id) "
	              "FROM duckdb_debug_query_request_pipeline_instances()");
	REQUIRE(CHECK_COLUMN(pipeline_instances, 0, {Value::BIGINT(static_cast<int64_t>(
	                                                 store.GetPipelineInstancesSnapshot().size()))}));
	REQUIRE(CHECK_COLUMN(pipeline_instances, 1, {Value::BIGINT(3)}));
	REQUIRE(CHECK_COLUMN(pipeline_instances, 2, {Value::BIGINT(static_cast<int64_t>(store.PipelineProfileCount()))}));

	store.Clear();
}

TEST_CASE("Query request profile store records concurrent metadata queries", "[api]") {
	auto &store = QueryRequestProfileStore::Get();
	store.Clear();

	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE concurrent_metadata_profile_t AS "
	                            "SELECT i, i % 5 AS g FROM range(20000) tbl(i)"));

	static const idx_t THREAD_COUNT = 4;
	static const idx_t QUERY_REPETITIONS = 3;
	std::vector<thread> threads;
	mutex error_lock;
	std::vector<string> errors;

	for (idx_t thread_idx = 0; thread_idx < THREAD_COUNT; thread_idx++) {
		threads.emplace_back([&db, &error_lock, &errors, thread_idx]() {
			Connection con(db);
			auto set_result = con.Query("SET query_request_profiling_enable=true");
			if (set_result->HasError()) {
				lock_guard<mutex> guard(error_lock);
				errors.push_back(set_result->GetError());
				return;
			}
			auto template_id = thread_idx % 2 == 0 ? 11 : 12;
			string body = "SELECT g, count(*), sum(i) FROM concurrent_metadata_profile_t GROUP BY g ORDER BY g";
			for (idx_t repetition = 0; repetition < QUERY_REPETITIONS; repetition++) {
				auto request_id = thread_idx * 100 + repetition;
				auto result = con.Query(MetadataQuery(request_id, template_id, 10, body));
				if (result->HasError()) {
					lock_guard<mutex> guard(error_lock);
					errors.push_back(result->GetError());
				}
			}
		});
	}

	for (auto &thread : threads) {
		thread.join();
	}

	REQUIRE(errors.empty());

	QueryRequestProfileEstimate template_11;
	QueryRequestProfileEstimate template_12;
	REQUIRE(store.QueryProfileCount() == 2);
	REQUIRE(store.TryGetQueryEstimate(11, 10, template_11));
	REQUIRE(store.TryGetQueryEstimate(12, 10, template_12));
	REQUIRE(template_11.sample_count == 6);
	REQUIRE(template_12.sample_count == 6);
	REQUIRE(template_11.mean_runtime_ns > 0);
	REQUIRE(template_12.mean_runtime_ns > 0);
	REQUIRE(store.PipelineProfileCount() >= 2);

	store.Clear();
}
