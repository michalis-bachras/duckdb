#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/query_pipeline_debug.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
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
	profile.source_operator_type = PhysicalOperatorType::TABLE_SCAN;
	profile.sink_operator_type = PhysicalOperatorType::HASH_GROUP_BY;
	profile.source_work_class.source_type = PhysicalOperatorType::TABLE_SCAN;
	profile.source_work_class.work_kind = SourceThroughputKind::TABLE_SCAN_ROWS;
	profile.source_type = "TABLE_SCAN";
	profile.sink_type = "HASH_GROUP_BY";
	profile.source_input_kind = SourceThroughputKindToString(SourceThroughputKind::TABLE_SCAN_ROWS);
	profile.source_input_confidence = "exact";
	profile.planned_input_native_unit = "row";
	profile.source_estimated_cardinality = 1000;
	profile.sink_estimated_cardinality = 10;
	profile.source_max_threads = 4;
	profile.planned_input_rows = 1000;
	profile.planned_input_chunks_equiv = 8;
	profile.task_count = 6;
	profile.worker_task_count = 6;
	profile.worker_task_duration_ns = 4000;
	profile.start_ns = start_ns;
	profile.tasks_done_ns = start_ns + 700;
	profile.finish_done_ns = start_ns + 900;
	profile.pipeline_signature = PipelineSignature(profile);
	profile.pipeline_signature_hash = StableStringHash64(profile.pipeline_signature);
	return profile;
}

static PipelineProfileIdentity PipelineIdentity(const PipelineProfilingInfo &profile) {
	PipelineProfileIdentity identity;
	identity.pipeline_id = profile.pipeline_id;
	identity.pipeline_signature_hash = profile.pipeline_signature_hash;
	identity.source_work_class = profile.source_work_class;
	identity.sink_type = profile.sink_operator_type;
	identity.planned_input_native_unit = profile.planned_input_native_unit;
	identity.valid = identity.pipeline_id != 0 && identity.pipeline_signature_hash != 0;
	return identity;
}

static DownstreamSuffixEstimate PrepareDownstreamSuffix(QueryRequestProfileStore &store, uint64_t template_id,
                                                        uint64_t scale_factor, idx_t pipeline_id,
                                                        uint64_t pipeline_signature_hash,
                                                        idx_t remaining_suffix_stages) {
	DownstreamSuffixEpochRequest request;
	request.template_id = template_id;
	request.scale_factor = scale_factor;
	request.pipeline_id = pipeline_id;
	request.pipeline_signature_hash = pipeline_signature_hash;
	request.remaining_suffix_stages = remaining_suffix_stages;
	auto estimates = store.PrepareDownstreamSuffixEpoch({request});
	REQUIRE(estimates.size() == 1);
	return estimates[0];
}

static string MetadataQuery(uint64_t request_id, uint64_t template_id, uint64_t scale_factor, double penalty_per_s,
                            const string &body) {
	auto deadline_ns = CurrentNs() + 60000000000ULL;
	return StringUtil::Format(
	    "/* duckdb_sla_v1 request_id=%llu template_id=%llu scale_factor=%llu sla_tag=30 penalty_per_s=%.17g "
	    "deadline_ns=%llu */ %s",
	    static_cast<unsigned long long>(request_id), static_cast<unsigned long long>(template_id),
	    static_cast<unsigned long long>(scale_factor), penalty_per_s, static_cast<unsigned long long>(deadline_ns), body);
}

static string MetadataQuery(uint64_t request_id, uint64_t template_id, uint64_t scale_factor, const string &body) {
	return MetadataQuery(request_id, template_id, scale_factor, 1.0, body);
}

} // namespace

TEST_CASE("Query request profile store aggregates direct observations", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto pipeline = PipelineProfile(2000);
	auto signature_hash = StableStringHash64(PipelineSignature(pipeline));
	duckdb::vector<PipelineProfilingInfo> pipelines_1;
	pipelines_1.push_back(pipeline);
	duckdb::vector<PipelineProfilingInfo> pipelines_2;
	auto second_pipeline = PipelineProfile(22000);
	second_pipeline.worker_task_duration_ns = 8000;
	pipelines_2.push_back(second_pipeline);

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
	REQUIRE(pipeline_instances[0].source_work_class == "TABLE_SCAN::table_scan_rows");
	REQUIRE(pipeline_instances[0].worker_task_count == 6);
	REQUIRE(pipeline_instances[0].worker_task_duration_ns == 4000);
	REQUIRE(pipeline_instances[0].throughput_valid);
	REQUIRE(pipeline_instances[0].single_worker_chunks_per_s == 2000000.0);
	REQUIRE(pipeline_instances[0].task_runtime_ns == 700);
	REQUIRE(pipeline_instances[0].lifecycle_runtime_ns == 900);
	REQUIRE(pipeline_instances[0].downstream_suffix_ns == 8100);
	REQUIRE(pipeline_instances[0].pipeline_completion_ordinal == 1);
	REQUIRE(pipeline_instances[0].total_pipeline_count == 1);
	REQUIRE(pipeline_instances[0].remaining_suffix_stages == 1);
	REQUIRE(pipeline_instances[0].normalized_downstream_suffix_ns == 8100.0);
	REQUIRE(pipeline_instances[0].continuation_valid);
	REQUIRE(pipeline_instances[0].effective_ns_per_work_unit == 112.5);

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
	REQUIRE(pipeline_estimate.source_work_class == "TABLE_SCAN::table_scan_rows");
	REQUIRE(pipeline_estimate.mean_task_count == 6);
	REQUIRE(pipeline_estimate.mean_source_max_threads == 4);
	REQUIRE(pipeline_estimate.mean_planned_input_rows == 1000);
	REQUIRE(pipeline_estimate.mean_task_runtime_ns == 700);
	REQUIRE(pipeline_estimate.mean_lifecycle_runtime_ns == 900);
	REQUIRE(pipeline_estimate.mean_downstream_suffix_ns == 8100);
	REQUIRE(pipeline_estimate.downstream_suffix_sample_count == 2);
	REQUIRE(pipeline_estimate.throughput_sample_count == 2);
	REQUIRE(pipeline_estimate.mean_single_worker_chunks_per_s == 1500000.0);
	REQUIRE(pipeline_estimate.ewma_single_worker_chunks_per_s == 1300000.0);
	REQUIRE(pipeline_estimate.continuation_sample_count == 2);
	REQUIRE(pipeline_estimate.mean_effective_ns_per_work_unit == 112.5);
	REQUIRE(pipeline_estimate.p50_effective_ns_per_work_unit == 112.5);
	REQUIRE(pipeline_estimate.p90_effective_ns_per_work_unit == 112.5);

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

TEST_CASE("Query request profile store tracks bounded continuation percentiles", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto signature_hash = StableStringHash64(PipelineSignature(PipelineProfile(1000)));
	QueryRequestPipelineProfileEstimate pipeline_estimate;
	for (idx_t i = 0; i < 10; i++) {
		auto start_ns = 100000 + i * 1000000;
		auto pipeline = PipelineProfile(start_ns);
		pipeline.finish_done_ns = start_ns + (i + 1) * 8000;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(i, 303, 10, start_ns, start_ns + 100000000), start_ns + 200000000,
		                            pipelines);
	}
	REQUIRE(store.TryGetPipelineEstimate(303, 10, 7, signature_hash, pipeline_estimate));
	REQUIRE(pipeline_estimate.continuation_sample_count == 10);
	REQUIRE(pipeline_estimate.mean_effective_ns_per_work_unit == 5500.0);
	REQUIRE(pipeline_estimate.p50_effective_ns_per_work_unit == 5000.0);
	REQUIRE(pipeline_estimate.p90_effective_ns_per_work_unit == 9000.0);

	for (idx_t i = 10; i < 105; i++) {
		auto start_ns = 100000 + i * 1000000;
		auto pipeline = PipelineProfile(start_ns);
		pipeline.finish_done_ns = start_ns + (i + 1) * 8000;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(i, 303, 10, start_ns, start_ns + 100000000), start_ns + 200000000,
		                            pipelines);
	}
	REQUIRE(store.TryGetPipelineEstimate(303, 10, 7, signature_hash, pipeline_estimate));
	REQUIRE(pipeline_estimate.continuation_sample_count == 100);
	REQUIRE(pipeline_estimate.mean_effective_ns_per_work_unit == 55500.0);
	REQUIRE(pipeline_estimate.p50_effective_ns_per_work_unit == 55000.0);
	REQUIRE(pipeline_estimate.p90_effective_ns_per_work_unit == 95000.0);

	store.Clear();
}

TEST_CASE("Query request profile store records lifecycle suffix progress", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();
	auto make_pipeline = [](idx_t pipeline_id, uint64_t finish_ns) {
		auto pipeline = PipelineProfile(1000);
		pipeline.pipeline_id = pipeline_id;
		pipeline.tasks_done_ns = finish_ns - 100;
		pipeline.finish_done_ns = finish_ns;
		pipeline.pipeline_signature = PipelineSignature(pipeline);
		pipeline.pipeline_signature_hash = StableStringHash64(pipeline.pipeline_signature);
		return pipeline;
	};
	duckdb::vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(make_pipeline(30, 8000));
	pipelines.push_back(make_pipeline(10, 2000));
	pipelines.push_back(make_pipeline(20, 5000));
	store.RecordQueryCompletion(Metadata(1, 404, 10, 500, 20000), 10000, pipelines);

	auto instances = store.GetPipelineInstancesSnapshot();
	REQUIRE(instances.size() == 3);
	REQUIRE(instances[0].pipeline_id == 10);
	REQUIRE(instances[0].pipeline_completion_ordinal == 1);
	REQUIRE(instances[0].total_pipeline_count == 3);
	REQUIRE(instances[0].remaining_suffix_stages == 3);
	REQUIRE(instances[0].downstream_suffix_ns == 8000);
	REQUIRE(instances[0].normalized_downstream_suffix_ns == Approx(8000.0 / 3.0));
	REQUIRE(instances[1].pipeline_id == 20);
	REQUIRE(instances[1].pipeline_completion_ordinal == 2);
	REQUIRE(instances[1].remaining_suffix_stages == 2);
	REQUIRE(instances[1].downstream_suffix_ns == 5000);
	REQUIRE(instances[1].normalized_downstream_suffix_ns == 2500.0);
	REQUIRE(instances[2].pipeline_id == 30);
	REQUIRE(instances[2].pipeline_completion_ordinal == 3);
	REQUIRE(instances[2].remaining_suffix_stages == 1);
	REQUIRE(instances[2].downstream_suffix_ns == 2000);
	REQUIRE(instances[2].normalized_downstream_suffix_ns == 2000.0);
	store.Clear();
}

TEST_CASE("Query request downstream suffix histograms preserve sparse point masses", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();
	uint64_t signature_hash = 0;
	for (idx_t i = 0; i < 9; i++) {
		auto start_ns = 100000 + i * 10000;
		auto pipeline = PipelineProfile(start_ns);
		signature_hash = pipeline.pipeline_signature_hash;
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 100;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(i, 505, 10, start_ns - 100, query_end_ns + 10000), query_end_ns,
		                            pipelines);
		if (i == 3 || i == 7) {
			auto snapshots = store.GetDownstreamSuffixProfilesSnapshot();
			bool found = false;
			for (const auto &snapshot : snapshots) {
				if (snapshot.level != DownstreamSuffixProfileLevel::EXACT || snapshot.template_id != 505 ||
				    snapshot.pipeline_signature_hash != signature_hash) {
					continue;
				}
				found = true;
				REQUIRE(snapshot.histogram.sample_count == i + 1);
				REQUIRE(snapshot.histogram.bucket_count == i + 1);
				for (idx_t bucket_idx = 0; bucket_idx < snapshot.histogram.bucket_count; bucket_idx++) {
					REQUIRE(snapshot.histogram.buckets[bucket_idx].lower_ns ==
					        snapshot.histogram.buckets[bucket_idx].upper_ns);
				}
			}
			REQUIRE(found);
		}
	}
	auto snapshots = store.GetDownstreamSuffixProfilesSnapshot();
	bool found_nine = false;
	for (const auto &snapshot : snapshots) {
		if (snapshot.level != DownstreamSuffixProfileLevel::EXACT || snapshot.template_id != 505 ||
		    snapshot.pipeline_signature_hash != signature_hash) {
			continue;
		}
		found_nine = true;
		REQUIRE(snapshot.histogram.sample_count == 9);
		REQUIRE(snapshot.histogram.bucket_count == 8);
		double probability = 0;
		for (idx_t bucket_idx = 0; bucket_idx < snapshot.histogram.bucket_count; bucket_idx++) {
			const auto &bucket = snapshot.histogram.buckets[bucket_idx];
			REQUIRE(bucket.lower_ns <= bucket.upper_ns);
			probability += bucket.probability;
		}
		REQUIRE(probability == Approx(1.0));
	}
	REQUIRE(found_nine);

	for (idx_t i = 9; i < 105; i++) {
		auto start_ns = 100000 + i * 10000;
		auto pipeline = PipelineProfile(start_ns);
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 100;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(i, 505, 10, start_ns - 100, query_end_ns + 10000), query_end_ns,
		                            pipelines);
	}
	snapshots = store.GetDownstreamSuffixProfilesSnapshot();
	for (const auto &snapshot : snapshots) {
		if (snapshot.level == DownstreamSuffixProfileLevel::EXACT && snapshot.template_id == 505 &&
		    snapshot.pipeline_signature_hash == signature_hash) {
			REQUIRE(snapshot.histogram.sample_count == 100);
			REQUIRE(snapshot.histogram.bucket_count <= 8);
		}
	}
	store.Clear();
}

TEST_CASE("Query request downstream suffix epoch preparation blends exact scale and global profiles", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();
	for (idx_t i = 0; i < 4; i++) {
		auto start_ns = 10000 + i * 10000;
		auto pipeline = PipelineProfile(start_ns);
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 100;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(i, 606, 30, start_ns - 100, query_end_ns + 10000), query_end_ns,
		                            pipelines);
	}
	auto global = PrepareDownstreamSuffix(store, 999, 100, 77, 12345, 2);
	REQUIRE(global.valid);
	REQUIRE(global.primary_level == DownstreamSuffixProfileLevel::GLOBAL);
	REQUIRE(global.global_weight == 1.0);
	REQUIRE(global.bucket_count == 4);
	REQUIRE(global.buckets[0].lower_ns == 200.0);
	REQUIRE(global.buckets[3].upper_ns == 800.0);

	uint64_t scale_signature = 0;
	for (idx_t i = 0; i < 2; i++) {
		auto start_ns = 100000 + i * 10000;
		auto pipeline = PipelineProfile(start_ns);
		scale_signature = pipeline.pipeline_signature_hash;
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 1000;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(10 + i, 707, 10, start_ns - 100, query_end_ns + 10000), query_end_ns,
		                            pipelines);
	}
	auto scale_global = PrepareDownstreamSuffix(store, 999, 10, 77, 67890, 1);
	REQUIRE(scale_global.valid);
	REQUIRE(scale_global.primary_level == DownstreamSuffixProfileLevel::SCALE_FACTOR);
	REQUIRE(scale_global.scale_weight == 0.5);
	REQUIRE(scale_global.global_weight == 0.5);

	auto exact = PrepareDownstreamSuffix(store, 707, 10, 7, scale_signature, 1);
	REQUIRE(exact.valid);
	REQUIRE(exact.primary_level == DownstreamSuffixProfileLevel::EXACT);
	REQUIRE(exact.exact_weight == 0.5);
	REQUIRE(exact.scale_weight == 0.25);
	REQUIRE(exact.global_weight == 0.25);
	for (idx_t i = 2; i < 4; i++) {
		auto start_ns = 100000 + i * 10000;
		auto pipeline = PipelineProfile(start_ns);
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 1000;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(10 + i, 707, 10, start_ns - 100, query_end_ns + 10000), query_end_ns,
		                            pipelines);
	}
	exact = PrepareDownstreamSuffix(store, 707, 10, 7, scale_signature, 1);
	REQUIRE(exact.valid);
	REQUIRE(exact.exact_sample_count == 4);
	REQUIRE(exact.exact_weight == 1.0);
	REQUIRE(exact.scale_weight == 0.0);
	REQUIRE(exact.global_weight == 0.0);

	store.Clear();
	auto missing = PrepareDownstreamSuffix(store, 999, 100, 77, 12345, 1);
	REQUIRE_FALSE(missing.valid);
}

TEST_CASE("Query request downstream suffix epoch preparation is ordered and deduplicated", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	uint64_t request_id = 1;
	uint64_t signature_10 = 0;
	for (idx_t i = 0; i < 4; i++) {
		auto start_ns = 10000 + request_id * 10000;
		auto pipeline = PipelineProfile(start_ns);
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 100;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(request_id++, 801, 10, start_ns - 100, query_end_ns + 10000),
		                            query_end_ns, pipelines);
		signature_10 = pipeline.pipeline_signature_hash;
	}
	for (idx_t i = 0; i < 4; i++) {
		auto start_ns = 10000 + request_id * 10000;
		auto pipeline = PipelineProfile(start_ns);
		auto query_end_ns = pipeline.finish_done_ns + (i + 1) * 1000;
		duckdb::vector<PipelineProfilingInfo> pipelines;
		pipelines.push_back(pipeline);
		store.RecordQueryCompletion(Metadata(request_id++, 802, 30, start_ns - 100, query_end_ns + 10000),
		                            query_end_ns, pipelines);
	}

	DownstreamSuffixEpochRequest exact_10 {801, 10, 7, signature_10, 1};
	DownstreamSuffixEpochRequest duplicate_10 {801, 10, 7, signature_10, 1};
	DownstreamSuffixEpochRequest scale_30 {999, 30, 77, 54321, 2};
	DownstreamSuffixEpochRequest invalid_pipeline {801, 10, 0, signature_10, 1};
	DownstreamSuffixEpochRequest invalid_signature {801, 10, 7, 0, 1};
	DownstreamSuffixEpochRequest invalid_stages {801, 10, 7, signature_10, 0};
	auto estimates = store.PrepareDownstreamSuffixEpoch(
	    {exact_10, duplicate_10, scale_30, invalid_pipeline, invalid_signature, invalid_stages});
	REQUIRE(estimates.size() == 6);
	REQUIRE(estimates[0].valid);
	REQUIRE(estimates[0].primary_level == DownstreamSuffixProfileLevel::EXACT);
	REQUIRE(estimates[0].exact_sample_count == 4);
	REQUIRE(estimates[0].scale_sample_count == 4);
	REQUIRE(estimates[0].global_sample_count == 8);
	REQUIRE(estimates[0].exact_weight == 1.0);
	REQUIRE(estimates[1].valid);
	REQUIRE(estimates[1].bucket_count == estimates[0].bucket_count);
	REQUIRE(estimates[1].exact_sample_count == estimates[0].exact_sample_count);
	for (idx_t bucket_idx = 0; bucket_idx < estimates[0].bucket_count; bucket_idx++) {
		REQUIRE(estimates[1].buckets[bucket_idx].lower_ns == estimates[0].buckets[bucket_idx].lower_ns);
		REQUIRE(estimates[1].buckets[bucket_idx].upper_ns == estimates[0].buckets[bucket_idx].upper_ns);
		REQUIRE(estimates[1].buckets[bucket_idx].probability == estimates[0].buckets[bucket_idx].probability);
	}
	REQUIRE(estimates[2].valid);
	REQUIRE(estimates[2].primary_level == DownstreamSuffixProfileLevel::SCALE_FACTOR);
	REQUIRE(estimates[2].scale_sample_count == 4);
	REQUIRE(estimates[2].scale_weight == 1.0);
	REQUIRE_FALSE(estimates[3].valid);
	REQUIRE_FALSE(estimates[4].valid);
	REQUIRE_FALSE(estimates[5].valid);
	REQUIRE(store.PrepareDownstreamSuffixEpoch({}).empty());

	auto fallback_update = PipelineProfile(200000);
	auto fallback_update_end = fallback_update.finish_done_ns + 500;
	duckdb::vector<PipelineProfilingInfo> fallback_update_pipelines {fallback_update};
	store.RecordQueryCompletion(Metadata(request_id, 803, 10, fallback_update.start_ns - 100,
	                                     fallback_update_end + 10000),
	                            fallback_update_end, fallback_update_pipelines);
	auto refreshed = store.PrepareDownstreamSuffixEpoch({exact_10});
	REQUIRE(refreshed.size() == 1);
	REQUIRE(refreshed[0].exact_weight == 1.0);
	REQUIRE(refreshed[0].scale_sample_count == 5);
	REQUIRE(refreshed[0].global_sample_count == 9);
	store.Clear();
}

TEST_CASE("Query request downstream suffix epoch preparation leaves inactive profiles available", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();
	uint64_t active_signature = 0;
	uint64_t inactive_signature = 0;
	for (idx_t i = 0; i < 4; i++) {
		auto active = PipelineProfile(10000 + i * 20000);
		auto active_end = active.finish_done_ns + 100 + i;
		duckdb::vector<PipelineProfilingInfo> active_pipelines {active};
		store.RecordQueryCompletion(Metadata(i, 901, 10, active.start_ns - 100, active_end + 10000), active_end,
		                            active_pipelines);
		active_signature = active.pipeline_signature_hash;

		auto inactive = PipelineProfile(20000 + i * 20000);
		auto inactive_end = inactive.finish_done_ns + 1000 + i;
		duckdb::vector<PipelineProfilingInfo> inactive_pipelines {inactive};
		store.RecordQueryCompletion(Metadata(100 + i, 902, 30, inactive.start_ns - 100, inactive_end + 10000),
		                            inactive_end, inactive_pipelines);
		inactive_signature = inactive.pipeline_signature_hash;
	}

	auto active = PrepareDownstreamSuffix(store, 901, 10, 7, active_signature, 1);
	REQUIRE(active.valid);
	REQUIRE(active.exact_sample_count == 4);
	auto inactive = PrepareDownstreamSuffix(store, 902, 30, 7, inactive_signature, 1);
	REQUIRE(inactive.valid);
	REQUIRE(inactive.exact_sample_count == 4);
	REQUIRE(inactive.buckets[0].lower_ns >= 1000.0);
	store.Clear();
}

TEST_CASE("Query request downstream suffix epoch preparation tolerates concurrent completions", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();
	auto pipeline = PipelineProfile(1000);
	DownstreamSuffixEpochRequest request {1001, 10, pipeline.pipeline_id, pipeline.pipeline_signature_hash, 1};
	std::thread writer([&]() {
		for (idx_t i = 0; i < 250; i++) {
			auto sample = PipelineProfile(10000 + i * 10000);
			auto query_end_ns = sample.finish_done_ns + 100 + i;
			duckdb::vector<PipelineProfilingInfo> pipelines {sample};
			store.RecordQueryCompletion(Metadata(i, 1001, 10, sample.start_ns - 100, query_end_ns + 10000),
			                            query_end_ns, pipelines);
		}
	});
	for (idx_t i = 0; i < 250; i++) {
		auto estimates = store.PrepareDownstreamSuffixEpoch({request});
		REQUIRE(estimates.size() == 1);
		if (estimates[0].valid) {
			double probability = 0;
			for (idx_t bucket_idx = 0; bucket_idx < estimates[0].bucket_count; bucket_idx++) {
				probability += estimates[0].buckets[bucket_idx].probability;
			}
			REQUIRE(probability == Approx(1.0));
		}
	}
	writer.join();
	auto final_estimates = store.PrepareDownstreamSuffixEpoch({request});
	REQUIRE(final_estimates.size() == 1);
	REQUIRE(final_estimates[0].valid);
	REQUIRE(final_estimates[0].exact_sample_count == 100);
	REQUIRE(final_estimates[0].bucket_count == 8);
	store.Clear();
}

TEST_CASE("Query request continuation resolver uses hierarchical fallbacks", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto pipeline = PipelineProfile(1000);
	duckdb::vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(pipeline);
	store.RecordQueryCompletion(Metadata(1, 101, 10, 500, 100000), 5000, pipelines);

	auto identity = PipelineIdentity(pipeline);
	auto exact = store.ResolvePipelineContinuation(101, 10, identity, 8);
	REQUIRE(exact.valid);
	REQUIRE(exact.level == ContinuationEstimateLevel::EXACT);
	REQUIRE(exact.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT);
	REQUIRE(exact.sample_count == 1);
	REQUIRE(exact.p90 == 112.5);

	auto source_sink = store.ResolvePipelineContinuation(202, 30, identity, 8000);
	REQUIRE(source_sink.valid);
	REQUIRE(source_sink.level == ContinuationEstimateLevel::SOURCE_SINK);
	REQUIRE(source_sink.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT);
	REQUIRE(source_sink.p90 == exact.p90);

	auto different_sink = identity;
	different_sink.sink_type = PhysicalOperatorType::ORDER_BY;
	auto source = store.ResolvePipelineContinuation(202, 30, different_sink, 8);
	REQUIRE(source.valid);
	REQUIRE(source.level == ContinuationEstimateLevel::SOURCE);
	REQUIRE(source.kind == ContinuationEstimateKind::NS_PER_WORK_UNIT);
	REQUIRE(source.p90 == exact.p90);

	auto different_source = identity;
	different_source.source_work_class.source_type = PhysicalOperatorType::HASH_JOIN;
	different_source.source_work_class.work_kind = SourceThroughputKind::HASH_JOIN_BUILD_ROWS;
	auto global = store.ResolvePipelineContinuation(202, 30, different_source, 8);
	REQUIRE(global.valid);
	REQUIRE(global.level == ContinuationEstimateLevel::GLOBAL_RAW);
	REQUIRE(global.kind == ContinuationEstimateKind::RAW_LATENCY_NS);
	REQUIRE(global.p90 == 900.0);

	auto zero_work = store.ResolvePipelineContinuation(101, 10, identity, 0);
	REQUIRE(zero_work.valid);
	REQUIRE(zero_work.level == ContinuationEstimateLevel::EXACT);
	REQUIRE(zero_work.kind == ContinuationEstimateKind::RAW_LATENCY_NS);
	REQUIRE(zero_work.p90 == 900.0);

	store.Clear();
	auto empty = store.ResolvePipelineContinuation(101, 10, identity, 8);
	REQUIRE_FALSE(empty.valid);
	REQUIRE(empty.level == ContinuationEstimateLevel::NONE);
	REQUIRE(empty.kind == ContinuationEstimateKind::INVALID);
}

TEST_CASE("Query request continuation uses task completion without an independent finish event", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto pipeline = PipelineProfile(1000);
	pipeline.source_operator_type = PhysicalOperatorType::HASH_JOIN;
	pipeline.sink_operator_type = PhysicalOperatorType::HASH_JOIN;
	pipeline.source_work_class.source_type = PhysicalOperatorType::HASH_JOIN;
	pipeline.source_work_class.work_kind = SourceThroughputKind::NO_SOURCE_SCAN;
	pipeline.source_type = "HASH_JOIN";
	pipeline.sink_type = "HASH_JOIN";
	pipeline.source_input_kind = SourceThroughputKindToString(SourceThroughputKind::NO_SOURCE_SCAN);
	pipeline.planned_input_native_unit = "none";
	pipeline.planned_input_rows = 0;
	pipeline.planned_input_chunks_equiv = 0;
	pipeline.finish_done_ns = 0;
	pipeline.pipeline_signature = PipelineSignature(pipeline);
	pipeline.pipeline_signature_hash = StableStringHash64(pipeline.pipeline_signature);

	duckdb::vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(pipeline);
	store.RecordQueryCompletion(Metadata(1, 101, 10, 500, 100000), 5000, pipelines);

	auto instances = store.GetPipelineInstancesSnapshot();
	REQUIRE(instances.size() == 1);
	REQUIRE(instances[0].tasks_done_ns == 1700);
	REQUIRE(instances[0].finish_done_ns == 0);
	REQUIRE(instances[0].lifecycle_runtime_ns == 700);
	REQUIRE_FALSE(instances[0].continuation_valid);

	QueryRequestPipelineProfileEstimate profile;
	REQUIRE(store.TryGetPipelineEstimate(101, 10, pipeline.pipeline_id, pipeline.pipeline_signature_hash, profile));
	REQUIRE(profile.mean_lifecycle_runtime_ns == 700.0);
	REQUIRE(profile.continuation_sample_count == 0);

	auto estimate = store.ResolvePipelineContinuation(101, 10, PipelineIdentity(pipeline), 0);
	REQUIRE(estimate.valid);
	REQUIRE(estimate.level == ContinuationEstimateLevel::EXACT);
	REQUIRE(estimate.kind == ContinuationEstimateKind::RAW_LATENCY_NS);
	REQUIRE(estimate.sample_count == 1);
	REQUIRE(estimate.mean == 700.0);
	REQUIRE(estimate.p50 == 700.0);
	REQUIRE(estimate.p90 == 700.0);

	store.Clear();
}

TEST_CASE("Query request continuation fallbacks reject incompatible native units", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto rows_pipeline = PipelineProfile(1000);
	duckdb::vector<PipelineProfilingInfo> rows_pipelines;
	rows_pipelines.push_back(rows_pipeline);
	store.RecordQueryCompletion(Metadata(1, 101, 10, 500, 100000), 5000, rows_pipelines);

	auto partition_pipeline = PipelineProfile(3000);
	partition_pipeline.planned_input_native_unit = "partition";
	partition_pipeline.pipeline_signature = PipelineSignature(partition_pipeline);
	partition_pipeline.pipeline_signature_hash = StableStringHash64(partition_pipeline.pipeline_signature);
	duckdb::vector<PipelineProfilingInfo> partition_pipelines;
	partition_pipelines.push_back(partition_pipeline);
	store.RecordQueryCompletion(Metadata(2, 202, 30, 2500, 100000), 7000, partition_pipelines);

	auto identity = PipelineIdentity(rows_pipeline);
	identity.planned_input_native_unit = "partition";
	auto fallback = store.ResolvePipelineContinuation(303, 100, identity, 8);
	REQUIRE(fallback.valid);
	REQUIRE(fallback.level == ContinuationEstimateLevel::GLOBAL_RAW);
	REQUIRE(fallback.kind == ContinuationEstimateKind::RAW_LATENCY_NS);

	auto profiles = store.GetContinuationProfilesSnapshot();
	idx_t mismatches = 0;
	for (const auto &profile : profiles) {
		mismatches += profile.native_unit_mismatch_count;
	}
	REQUIRE(mismatches == 2);

	store.Clear();
}

TEST_CASE("Query request throughput resolver uses compatible hierarchical fallbacks", "[api]") {
	QueryRequestProfileStore store;
	store.Clear();

	auto first = PipelineProfile(1000);
	auto second = PipelineProfile(3000);
	second.worker_task_duration_ns = 8000;
	duckdb::vector<PipelineProfilingInfo> first_observation {first};
	duckdb::vector<PipelineProfilingInfo> second_observation {second};
	store.RecordQueryCompletion(Metadata(1, 101, 10, 500, 100000), 5000, first_observation);
	store.RecordQueryCompletion(Metadata(2, 101, 10, 2500, 100000), 7000, second_observation);

	auto identity = PipelineIdentity(first);
	auto exact = store.ResolvePipelineThroughput(101, 10, identity);
	REQUIRE(exact.valid);
	REQUIRE(exact.level == PipelineThroughputEstimateLevel::EXACT);
	REQUIRE(exact.sample_count == 2);
	REQUIRE(exact.mean_work_units_per_s == 1500000.0);
	REQUIRE(exact.ewma_work_units_per_s == 1300000.0);

	auto source_sink = store.ResolvePipelineThroughput(202, 30, identity);
	REQUIRE(source_sink.valid);
	REQUIRE(source_sink.level == PipelineThroughputEstimateLevel::SOURCE_SINK);
	REQUIRE(source_sink.sample_count == 2);
	REQUIRE(source_sink.ewma_work_units_per_s == exact.ewma_work_units_per_s);

	auto different_sink = identity;
	different_sink.sink_type = PhysicalOperatorType::ORDER_BY;
	auto source = store.ResolvePipelineThroughput(202, 30, different_sink);
	REQUIRE(source.valid);
	REQUIRE(source.level == PipelineThroughputEstimateLevel::SOURCE);
	REQUIRE(source.ewma_work_units_per_s == exact.ewma_work_units_per_s);

	auto different_source = different_sink;
	different_source.source_work_class.source_type = PhysicalOperatorType::FILTER;
	auto global = store.ResolvePipelineThroughput(202, 30, different_source);
	REQUIRE(global.valid);
	REQUIRE(global.level == PipelineThroughputEstimateLevel::GLOBAL_COMPATIBLE);
	REQUIRE(global.ewma_work_units_per_s == exact.ewma_work_units_per_s);

	auto incompatible_unit = different_source;
	incompatible_unit.planned_input_native_unit = "partition";
	auto incompatible = store.ResolvePipelineThroughput(202, 30, incompatible_unit);
	REQUIRE_FALSE(incompatible.valid);
	REQUIRE(incompatible.level == PipelineThroughputEstimateLevel::NONE);

	auto snapshots = store.GetThroughputProfilesSnapshot();
	REQUIRE(snapshots.size() == 3);
	for (const auto &snapshot : snapshots) {
		REQUIRE(snapshot.sample_count == 2);
		REQUIRE(snapshot.native_unit == "row");
		REQUIRE(snapshot.ewma_work_units_per_s == 1300000.0);
	}

	auto exact_tail = store.ResolvePipelineLifecycleTail(101, 10, identity);
	REQUIRE(exact_tail.valid);
	REQUIRE(exact_tail.level == PipelineLifecycleTailEstimateLevel::EXACT);
	REQUIRE(exact_tail.sample_count == 2);
	REQUIRE(exact_tail.mean_ns == 200.0);
	REQUIRE(exact_tail.p90_ns == 200.0);
	auto global_tail = store.ResolvePipelineLifecycleTail(202, 30, identity);
	REQUIRE(global_tail.valid);
	REQUIRE(global_tail.level == PipelineLifecycleTailEstimateLevel::GLOBAL);
	REQUIRE(global_tail.sample_count == 2);
	REQUIRE(global_tail.p90_ns == 200.0);

	store.Clear();
	REQUIRE(store.GetThroughputProfilesSnapshot().empty());
	REQUIRE_FALSE(store.ResolvePipelineThroughput(101, 10, identity).valid);
	REQUIRE_FALSE(store.ResolvePipelineLifecycleTail(101, 10, identity).valid);
}

TEST_CASE("Query request profile store separates repeated local pipeline signatures by pipeline id", "[api]") {
	QueryRequestProfileStore store;
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
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
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

TEST_CASE("Query request profile stores are isolated by database instance", "[api]") {
	DuckDB first_db(nullptr);
	DuckDB second_db(nullptr);
	auto &first_store = first_db.instance->GetQueryRequestProfileStore();
	auto &second_store = second_db.instance->GetQueryRequestProfileStore();

	auto pipeline = PipelineProfile(2000);
	duckdb::vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(pipeline);
	first_store.RecordQueryCompletion(Metadata(1, 101, 10, 1000, 9000), 11000, pipelines);

	REQUIRE(first_store.QueryProfileCount() == 1);
	REQUIRE(first_store.PipelineProfileCount() == 1);
	REQUIRE(second_store.QueryProfileCount() == 0);
	REQUIRE(second_store.PipelineProfileCount() == 0);

	Connection first_connection(first_db);
	Connection second_connection(second_db);
	auto first_count = first_connection.Query("SELECT count(*) FROM duckdb_debug_query_request_profiles()");
	auto second_count = second_connection.Query("SELECT count(*) FROM duckdb_debug_query_request_profiles()");
	REQUIRE(CHECK_COLUMN(first_count, 0, {1}));
	REQUIRE(CHECK_COLUMN(second_count, 0, {0}));

	second_store.Clear();
	REQUIRE(first_store.QueryProfileCount() == 1);
}

TEST_CASE("Query request profile store records repeated metadata queries", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
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
	auto failed_result = con.Query(MetadataQuery(99, 6, 10, "SELECT * FROM metadata_profile_missing"));
	REQUIRE(failed_result->HasError());

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
	auto suffix_levels = con.Query(
	    "SELECT profile_level FROM duckdb_debug_query_request_downstream_suffix_profiles() "
	    "GROUP BY profile_level ORDER BY profile_level");
	REQUIRE(CHECK_COLUMN(suffix_levels, 0, {"exact", "global", "scale_factor"}));
	auto exact_suffix_samples = con.Query(
	    "SELECT min(sample_count), max(sample_count) "
	    "FROM duckdb_debug_query_request_downstream_suffix_profiles() WHERE profile_level='exact'");
	REQUIRE(CHECK_COLUMN(exact_suffix_samples, 0, {Value::UBIGINT(3)}));
	REQUIRE(CHECK_COLUMN(exact_suffix_samples, 1, {Value::UBIGINT(3)}));
	auto invalid_suffix_probabilities = con.Query(
	    "SELECT count(*) FROM ("
	    "SELECT profile_level, template_id, scale_factor, pipeline_id, pipeline_signature_hash "
	    "FROM duckdb_debug_query_request_downstream_suffix_profiles() "
	    "GROUP BY ALL HAVING abs(sum(probability) - 1.0) > 1e-9)");
	REQUIRE(CHECK_COLUMN(invalid_suffix_probabilities, 0, {Value::BIGINT(0)}));

	store.Clear();
}

TEST_CASE("Query continuation profiles train in activation scheduler mode", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
	QueryPipelineDebug::ClearDebugSnapshot();
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_debug_enable=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE continuation_scheduler_t AS "
	                          "SELECT i, i % 7 AS g FROM range(200000) tbl(i)"));

	string body = "SELECT g, count(*), sum(i) FROM continuation_scheduler_t GROUP BY g ORDER BY g";
	REQUIRE_NO_FAIL(con.Query(MetadataQuery(9001, 6, 10, body)));
	REQUIRE_NO_FAIL(con.Query(MetadataQuery(9002, 6, 10, body)));

	REQUIRE(store.QueryProfileCount() == 1);
	REQUIRE(store.PipelineProfileCount() > 0);
	auto exact_events = con.Query(
	    "SELECT count(*) FROM duckdb_debug_query_pipeline_events() "
	    "WHERE request_id=9002 AND event_kind='pipeline' AND continuation_valid "
	    "AND continuation_level='exact' AND continuation_kind='ns_per_work_unit'");
	REQUIRE(!exact_events->HasError());
	REQUIRE(exact_events->GetValue(0, 0).GetValue<int64_t>() > 0);

	auto cohorts = con.Query(
	    "SELECT count(*) FROM duckdb_debug_query_request_continuation_profiles() "
	    "WHERE profile_level IN ('source_sink', 'source', 'global_raw')");
	REQUIRE(!cohorts->HasError());
	REQUIRE(cohorts->GetValue(0, 0).GetValue<int64_t>() > 0);
	auto historical_throughput = con.Query(
	    "SELECT count(*) FROM duckdb_debug_query_pipeline_events() "
	    "WHERE request_id=9002 AND event_kind='pipeline' AND selected_throughput_valid "
	    "AND historical_throughput_level='exact' AND historical_throughput_sample_count > 0");
	REQUIRE(!historical_throughput->HasError());
	REQUIRE(historical_throughput->GetValue(0, 0).GetValue<int64_t>() > 0);
	auto throughput_cohorts = con.Query(
	    "SELECT count(*) FROM duckdb_debug_query_request_throughput_profiles() "
	    "WHERE profile_level IN ('source_sink', 'source', 'global_compatible')");
	REQUIRE(!throughput_cohorts->HasError());
	REQUIRE(throughput_cohorts->GetValue(0, 0).GetValue<int64_t>() > 0);

	store.Clear();
	QueryPipelineDebug::ClearDebugSnapshot();
}

TEST_CASE("SLA scheduler preserves worker-only database invariants", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_FAIL(con.Query("SET scheduler_policy='sla'"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=1"));
	REQUIRE_FAIL(con.Query("SET scheduler_policy='sla'"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='sla'"));
	REQUIRE_FAIL(con.Query("SET external_threads=1"));
	REQUIRE_FAIL(con.Query("SET query_worker_only_execution_enable=false"));
	REQUIRE_FAIL(con.Query("RESET query_worker_only_execution_enable"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='default'"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=false"));
}

TEST_CASE("SLA scheduler rejects an untrained tagged data pipeline", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE sla_untrained_t AS SELECT i FROM range(100000) tbl(i)"));
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='sla'"));
	REQUIRE_FAIL(con.Query(MetadataQuery(9101, 91, 10, "SELECT sum(i) FROM sla_untrained_t")));
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}

TEST_CASE("SLA scheduler executes a trained tagged query without debug mode", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_debug_enable=false"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE sla_trained_t AS "
	                          "SELECT i, i % 101 AS g FROM range(1000000) tbl(i)"));

	const string body = "SELECT g, count(*), sum(i) FROM sla_trained_t GROUP BY g ORDER BY g";
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_NO_FAIL(con.Query(MetadataQuery(9200 + i, 92, 10, body)));
	}
	REQUIRE(store.PipelineProfileCount() > 0);
	REQUIRE_NO_FAIL(con.Query("SET query_sla_scheduler_epoch_ms=10"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='sla'"));
	auto &scheduler = db.instance->GetQuerySLAScheduler();
	scheduler.ClearEpochTrace();
	auto epochs_before = scheduler.EpochRunCount();
	REQUIRE_NO_FAIL(con.Query(MetadataQuery(9210, 92, 10, body)));
	REQUIRE(scheduler.EpochRunCount() > epochs_before);
	REQUIRE(scheduler.GetSnapshot().empty());
	REQUIRE(scheduler.GetEpochTrace().empty());
	auto debug_rows = con.Query("SELECT count(*) FROM duckdb_debug_query_sla_scheduler()");
	REQUIRE(!debug_rows->HasError());
	REQUIRE(debug_rows->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("SLA scheduler records bounded epoch predictions only for debug queries", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_debug_enable=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE sla_trace_t AS "
	                          "SELECT i, i % 1009 AS g FROM range(2000000) tbl(i)"));

	const string body = "SELECT g, count(*), sum(i) FROM sla_trace_t GROUP BY g ORDER BY g";
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_NO_FAIL(con.Query(MetadataQuery(9250 + i, 95, 10, body)));
	}
	REQUIRE_NO_FAIL(con.Query("SET query_sla_scheduler_epoch_ms=5"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='sla'"));
	auto &scheduler = db.instance->GetQuerySLAScheduler();
	scheduler.ClearEpochTrace();
	REQUIRE_NO_FAIL(con.Query(MetadataQuery(9260, 95, 10, body)));

	auto trace = scheduler.GetEpochTrace();
	REQUIRE(!trace.empty());
	REQUIRE(scheduler.EpochTraceDroppedCount() == 0);
	bool saw_valid_pipeline = false;
	for (const auto &row : trace) {
		REQUIRE(row.epoch_wall_ns > 0);
#ifdef __linux__
		REQUIRE(row.epoch_thread_cpu_ns > 0);
		REQUIRE(row.epoch_thread_cpu_ns <= row.epoch_wall_ns);
#else
		REQUIRE(row.epoch_thread_cpu_ns == 0);
#endif
		REQUIRE(row.capture_lock_wait_ns <= row.epoch_wall_ns);
		REQUIRE(row.capture_lock_hold_ns <= row.epoch_wall_ns);
		REQUIRE(row.work_snapshot_ns <= row.epoch_wall_ns);
		REQUIRE(row.suffix_prepare_ns <= row.epoch_wall_ns);
		REQUIRE(row.model_build_ns <= row.epoch_wall_ns);
		REQUIRE(row.allocation_ns <= row.epoch_wall_ns);
		REQUIRE(row.publish_lock_wait_ns <= row.epoch_wall_ns);
		REQUIRE(row.publish_lock_hold_ns <= row.epoch_wall_ns);
		REQUIRE(row.trace_build_ns > 0);
		REQUIRE(row.trace_lock_hold_ns > 0);
		REQUIRE(row.planned_workers <= row.demand_cap);
		if (row.event_kind != "pipeline" || !row.model_valid || row.remaining_work_units == 0) {
			continue;
		}
		saw_valid_pipeline = true;
		REQUIRE(row.pipeline_signature_hash != 0);
		REQUIRE(row.selected_throughput > 0);
		REQUIRE(row.first_mandatory_gain >= -1e-9);
		REQUIRE(row.last_mandatory_gain >= -1e-9);
		REQUIRE(row.next_mandatory_gain >= -1e-9);
		REQUIRE(row.historical_throughput_sample_count >= 4);
		REQUIRE(row.continuation_sample_count >= 4);
		REQUIRE(row.suffix_bucket_count > 0);
		REQUIRE(row.suffix_exact_sample_count >= 4);
		REQUIRE(row.predicted_pipeline_finish_ns > row.epoch_timestamp_ns);
		REQUIRE(row.predicted_query_finish_mean_ns >= row.predicted_pipeline_finish_ns);
		REQUIRE(row.predicted_query_finish_p90_ns >= row.predicted_pipeline_finish_ns);
		REQUIRE(row.predicted_sla_cost >= 0);
	}
	REQUIRE(saw_valid_pipeline);
	auto exported = con.Query(
	    "SELECT count(*) FROM duckdb_debug_query_sla_scheduler_epochs() "
	    "WHERE request_id=9260 AND event_kind='pipeline' AND model_valid AND predicted_pipeline_finish_ns>0 "
	    "AND epoch_wall_ns>0 "
	    "AND capture_lock_wait_ns<=epoch_wall_ns AND publish_lock_wait_ns<=epoch_wall_ns "
	    "AND trace_build_ns>0 AND trace_lock_hold_ns>0");
	REQUIRE(!exported->HasError());
	REQUIRE(exported->GetValue(0, 0).GetValue<int64_t>() > 0);
}

TEST_CASE("SLA scheduler remains work-conserving when modeled SLA gains are zero", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET query_activation_debug_enable=true"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE sla_zero_gain_t AS "
	                          "SELECT i, i % 1009 AS g FROM range(2000000) tbl(i)"));

	const string body = "SELECT g, count(*), sum(i) FROM sla_zero_gain_t GROUP BY g ORDER BY g";
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_NO_FAIL(con.Query(MetadataQuery(9270 + i, 96, 10, body)));
	}
	REQUIRE_NO_FAIL(con.Query("SET query_sla_scheduler_epoch_ms=5"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='sla'"));
	auto &scheduler = db.instance->GetQuerySLAScheduler();
	scheduler.ClearEpochTrace();
	REQUIRE_NO_FAIL(con.Query(MetadataQuery(9280, 96, 10, 0.0, body)));

	auto trace = scheduler.GetEpochTrace();
	bool saw_work_conserving_pipeline = false;
	for (const auto &row : trace) {
		REQUIRE(row.planned_workers <= row.demand_cap);
		if (row.request_id == 9280 && row.event_kind == "pipeline" && row.model_valid &&
		    row.remaining_work_units > 0 && row.demand_cap > 0 && row.planned_workers > 0) {
			saw_work_conserving_pipeline = true;
		}
	}
	REQUIRE(saw_work_conserving_pipeline);
	REQUIRE(scheduler.GetSnapshot().empty());
}

TEST_CASE("SLA scheduler coordinates concurrent trained queries within demand caps", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET threads=4"));
	REQUIRE_NO_FAIL(setup.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(setup.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(setup.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(setup.Query("SET query_activation_debug_enable=true"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE sla_concurrent_t AS "
	                            "SELECT i, i % 1009 AS g FROM range(3000000) tbl(i)"));
	const string body = "SELECT g, count(*), sum(i) FROM sla_concurrent_t GROUP BY g ORDER BY g";
	for (idx_t i = 0; i < 4; i++) {
		REQUIRE_NO_FAIL(setup.Query(MetadataQuery(9300 + i, 93, 10, body)));
		REQUIRE_NO_FAIL(setup.Query(MetadataQuery(9400 + i, 94, 10, body)));
	}
	REQUIRE_NO_FAIL(setup.Query("SET query_sla_scheduler_epoch_ms=5"));
	REQUIRE_NO_FAIL(setup.Query("SET scheduler_policy='sla'"));

	Connection first(db);
	Connection second(db);
	REQUIRE_NO_FAIL(first.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(second.Query("SET query_activation_scheduler_enable=true"));
	REQUIRE_NO_FAIL(first.Query("SET query_activation_debug_enable=true"));
	REQUIRE_NO_FAIL(second.Query("SET query_activation_debug_enable=true"));
	atomic<bool> first_done(false);
	atomic<bool> second_done(false);
	string first_error;
	string second_error;
	thread first_thread([&]() {
		auto result = first.Query(MetadataQuery(9310, 93, 10, body));
		if (result->HasError()) {
			first_error = result->GetError();
		}
		first_done = true;
	});
	thread second_thread([&]() {
		auto result = second.Query(MetadataQuery(9410, 94, 10, body));
		if (result->HasError()) {
			second_error = result->GetError();
		}
		second_done = true;
	});
	atomic<idx_t> trace_reads(0);
	thread trace_reader([&]() {
		while (!first_done.load() || !second_done.load()) {
			auto trace = db.instance->GetQuerySLAScheduler().GetEpochTrace();
			(void)db.instance->GetQuerySLAScheduler().EpochTraceDroppedCount();
			if ((trace_reads.fetch_add(1) % 8) == 7) {
				db.instance->GetQuerySLAScheduler().ClearEpochTrace();
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	});

	bool saw_two_queries = false;
	bool saw_valid_data_models = false;
	bool per_query_bounds_valid = true;
	bool global_budget_valid = true;
	string first_bound_violation;
	while (!first_done.load() || !second_done.load()) {
		auto snapshots = db.instance->GetQuerySLAScheduler().GetSnapshot();
		if (snapshots.size() >= 2) {
			saw_two_queries = true;
		}
		idx_t total_assigned = 0;
		idx_t valid_data_models = 0;
		for (const auto &snapshot : snapshots) {
			total_assigned += snapshot.assigned_workers;
			if (snapshot.assigned_workers > snapshot.demand_cap) {
				per_query_bounds_valid = false;
				if (first_bound_violation.empty()) {
					first_bound_violation = StringUtil::Format(
					    "query=%llu event=%s generation=%llu assigned=%llu demand=%llu",
					    static_cast<unsigned long long>(snapshot.db_query_id), snapshot.event_kind,
					    static_cast<unsigned long long>(snapshot.pipeline_generation),
					    static_cast<unsigned long long>(snapshot.assigned_workers),
					    static_cast<unsigned long long>(snapshot.demand_cap));
				}
			}
			if (snapshot.event_kind == "pipeline" && snapshot.model_valid && snapshot.remaining_work_units > 0 &&
			    snapshot.selected_throughput > 0) {
				valid_data_models++;
			}
		}
		if (valid_data_models >= 2) {
			saw_valid_data_models = true;
		}
		if (total_assigned > 4) {
			global_budget_valid = false;
			if (first_bound_violation.empty()) {
				first_bound_violation =
				    StringUtil::Format("total_assigned=%llu", static_cast<unsigned long long>(total_assigned));
			}
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	first_thread.join();
	second_thread.join();
	trace_reader.join();

	REQUIRE(first_error.empty());
	REQUIRE(second_error.empty());
	REQUIRE(trace_reads.load() > 0);
	REQUIRE(saw_two_queries);
	REQUIRE(saw_valid_data_models);
	INFO(first_bound_violation);
	REQUIRE(per_query_bounds_valid);
	REQUIRE(global_budget_valid);
	REQUIRE(db.instance->GetQuerySLAScheduler().GetSnapshot().empty());
}

TEST_CASE("Query request profile store records concurrent metadata queries", "[api]") {
	DuckDB db(nullptr);
	auto &store = db.instance->GetQueryRequestProfileStore();
	store.Clear();
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
