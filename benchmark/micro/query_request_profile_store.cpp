#include "benchmark_runner.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

#include <atomic>
#include <thread>

using namespace duckdb;

namespace {

struct ResolverKey {
	uint64_t template_id;
	uint64_t scale_factor;
	idx_t pipeline_id;
	uint64_t pipeline_signature_hash;
	idx_t remaining_suffix_stages;
};

enum class SuffixPreparationMode : uint8_t {
	CACHED,
	DIRTY_EXACT,
	SCALE_FALLBACK,
	GLOBAL_FALLBACK,
	CACHED_EPOCH_BATCH,
	EPOCH_BATCH,
	EPOCH_BATCH_MULTI_SCALE,
	CONCURRENT_UPDATES
};

struct SuffixPreparationState : public BenchmarkState {
	vector<ResolverKey> keys;
	idx_t repetitions = 1;
	uint64_t checksum = 0;
};

static QueryRequestMetadata BenchmarkMetadata(uint64_t request_id, uint64_t template_id, uint64_t scale_factor,
	                                           uint64_t start_ns, uint64_t query_end_ns) {
	QueryRequestMetadata metadata;
	metadata.valid = true;
	metadata.parse_status = "ok";
	metadata.db_query_id = request_id + 1;
	metadata.request_id = request_id + 1;
	metadata.template_id = template_id;
	metadata.scale_factor = scale_factor;
	metadata.sla_tag = 30;
	metadata.sla_penalty_per_s = 1;
	metadata.deadline_ns = query_end_ns + 1000000;
	metadata.query_start_ns = start_ns;
	return metadata;
}

static PipelineProfilingInfo BenchmarkPipeline(const ResolverKey &key, uint64_t start_ns, uint64_t finish_ns) {
	PipelineProfilingInfo profile;
	profile.pipeline_id = key.pipeline_id;
	profile.pipeline_signature_hash = key.pipeline_signature_hash;
	profile.pipeline_signature = "TABLE_SCAN>PROJECTION>HASH_GROUP_BY";
	profile.operator_type_sequence = profile.pipeline_signature;
	profile.source_type = "TABLE_SCAN";
	profile.sink_type = "HASH_GROUP_BY";
	profile.source_input_kind = "table_scan_rows";
	profile.source_input_confidence = "exact";
	profile.planned_input_native_unit = "row";
	profile.planned_input_rows = 16384;
	profile.planned_input_chunks_equiv = 8;
	profile.source_max_threads = 4;
	profile.task_count = 4;
	profile.worker_task_count = 4;
	profile.worker_task_duration_ns = 4000;
	profile.start_ns = start_ns + 100;
	profile.tasks_done_ns = finish_ns;
	profile.finish_done_ns = finish_ns;
	return profile;
}

static void RecordObservation(const ResolverKey &key, uint64_t request_id, idx_t sample_index) {
	auto start_ns = 1000000ULL + request_id * 2000000ULL;
	auto finish_ns = start_ns + 100000ULL;
	auto suffix_ns = 100000ULL + static_cast<uint64_t>((sample_index * 7919 + key.template_id) % 1000000);
	auto query_end_ns = finish_ns + suffix_ns;
	vector<PipelineProfilingInfo> pipelines;
	pipelines.push_back(BenchmarkPipeline(key, start_ns, finish_ns));
	QueryRequestProfileStore::Get().RecordQueryCompletion(
	    BenchmarkMetadata(request_id, key.template_id, key.scale_factor, start_ns, query_end_ns), query_end_ns,
	    pipelines);
}

static void SeedProfile(const ResolverKey &key, idx_t sample_count, uint64_t &request_id) {
	for (idx_t sample = 0; sample < sample_count; sample++) {
		RecordObservation(key, request_id++, sample);
	}
}

static uint64_t PrepareAndChecksum(const vector<ResolverKey> &keys) {
	vector<DownstreamSuffixEpochRequest> requests;
	requests.reserve(keys.size());
	for (const auto &key : keys) {
		requests.push_back({key.template_id, key.scale_factor, key.pipeline_id, key.pipeline_signature_hash,
		                    key.remaining_suffix_stages});
	}
	auto estimates = QueryRequestProfileStore::Get().PrepareDownstreamSuffixEpoch(requests);
	uint64_t checksum = estimates.size();
	for (const auto &estimate : estimates) {
		checksum += static_cast<uint64_t>(estimate.valid) + estimate.bucket_count + estimate.exact_sample_count +
		            estimate.scale_sample_count + estimate.global_sample_count;
	}
	return checksum;
}

class SuffixPreparationBenchmark : public Benchmark {
public:
	SuffixPreparationBenchmark(string name_p, SuffixPreparationMode mode_p, idx_t sample_count_p = 100,
	                        idx_t profile_count_p = 1, idx_t run_count_p = 25)
	    : Benchmark(true, std::move(name_p), "[query_request_profile_store]"), mode(mode_p),
	      sample_count(sample_count_p), profile_count(profile_count_p), run_count(run_count_p) {
	}

	unique_ptr<BenchmarkState> Initialize(BenchmarkConfiguration &config) override {
		auto state = make_uniq<SuffixPreparationState>();
		auto &store = QueryRequestProfileStore::Get();
		store.Clear();
		uint64_t request_id = 1;
		ResolverKey fallback_seed {900001, 10, 1, 900001, 1};
		SeedProfile(fallback_seed, 100, request_id);

		switch (mode) {
		case SuffixPreparationMode::CACHED: {
			ResolverKey key {100001, 10, 1, 100001, 1};
			SeedProfile(key, 100, request_id);
			state->keys.push_back(key);
			state->checksum += PrepareAndChecksum(state->keys);
			state->repetitions = 100000;
			break;
		}
		case SuffixPreparationMode::DIRTY_EXACT: {
			for (idx_t profile_idx = 0; profile_idx < profile_count; profile_idx++) {
				ResolverKey key {200001 + profile_idx, 10, 1, 200001 + profile_idx, 1};
				SeedProfile(key, sample_count, request_id);
				state->keys.push_back(key);
			}
			break;
		}
		case SuffixPreparationMode::SCALE_FALLBACK:
			for (idx_t profile_idx = 0; profile_idx < profile_count; profile_idx++) {
				auto scale_factor = 1000 + profile_idx;
				ResolverKey seed {300001 + profile_idx, scale_factor, 1, 300001 + profile_idx, 1};
				SeedProfile(seed, sample_count, request_id);
				state->keys.push_back({400001 + profile_idx, scale_factor, 1, 400001 + profile_idx, 3});
			}
			break;
		case SuffixPreparationMode::EPOCH_BATCH_MULTI_SCALE:
			for (idx_t profile_idx = 0; profile_idx < profile_count; profile_idx++) {
				uint64_t scale_factor = profile_idx % 2 == 0 ? 10 : 30;
				ResolverKey key {550000 + profile_idx, scale_factor, 1, 550000 + profile_idx, 1};
				SeedProfile(key, sample_count, request_id);
				state->keys.push_back(key);
			}
			break;
		case SuffixPreparationMode::GLOBAL_FALLBACK:
			state->keys.push_back({400001, 999, 1, 400001, 3});
			break;
		case SuffixPreparationMode::CACHED_EPOCH_BATCH:
			for (idx_t profile_idx = 0; profile_idx < profile_count; profile_idx++) {
				ResolverKey key {450000 + profile_idx, 10, 1, 450000 + profile_idx, 1};
				SeedProfile(key, sample_count, request_id);
				state->keys.push_back(key);
			}
			state->checksum += PrepareAndChecksum(state->keys);
			break;
		case SuffixPreparationMode::EPOCH_BATCH:
			for (idx_t profile_idx = 0; profile_idx < profile_count; profile_idx++) {
				ResolverKey key {500000 + profile_idx, 10, 1, 500000 + profile_idx, 1};
				SeedProfile(key, sample_count, request_id);
				state->keys.push_back(key);
			}
			break;
		case SuffixPreparationMode::CONCURRENT_UPDATES: {
			ResolverKey key {600001, 10, 1, 600001, 1};
			SeedProfile(key, 100, request_id);
			state->keys.push_back(key);
			state->repetitions = 10000;
			break;
		}
		}
		return std::move(state);
	}

	void Run(BenchmarkState *state_p) override {
		auto &state = *static_cast<SuffixPreparationState *>(state_p);
		if (mode == SuffixPreparationMode::CONCURRENT_UPDATES) {
			std::atomic<bool> start(false);
			std::thread writer([&]() {
				while (!start.load()) {
					std::this_thread::yield();
				}
				for (idx_t sample = 0; sample < 1000; sample++) {
					RecordObservation(state.keys[0], 1000000 + sample, sample);
				}
			});
			start.store(true);
			for (idx_t repetition = 0; repetition < state.repetitions; repetition++) {
				state.checksum += PrepareAndChecksum(state.keys);
			}
			writer.join();
			return;
		}
		for (idx_t repetition = 0; repetition < state.repetitions; repetition++) {
			state.checksum += PrepareAndChecksum(state.keys);
		}
	}

	void Cleanup(BenchmarkState *state) override {
		QueryRequestProfileStore::Get().Clear();
	}

	string Verify(BenchmarkState *state_p) override {
		auto &state = *static_cast<SuffixPreparationState *>(state_p);
		return state.checksum == 0 ? "preparation returned no profile data" : string();
	}

	void Interrupt(BenchmarkState *state) override {
	}

	string BenchmarkInfo() override {
		return "Direct production-path downstream suffix epoch preparation";
	}

	string GetLogOutput(BenchmarkState *state) override {
		return string();
	}

	bool RequireReinit() override {
		return true;
	}

	size_t NRuns() override {
		return run_count;
	}

private:
	SuffixPreparationMode mode;
	idx_t sample_count;
	idx_t profile_count;
	idx_t run_count;
};

static SuffixPreparationBenchmark suffix_cached("SuffixPrepareCached100K", SuffixPreparationMode::CACHED, 100, 1, 10);
static SuffixPreparationBenchmark suffix_dirty_1("SuffixPrepareDirty001x064", SuffixPreparationMode::DIRTY_EXACT, 1,
	                                         64, 10);
static SuffixPreparationBenchmark suffix_dirty_4("SuffixPrepareDirty004x064", SuffixPreparationMode::DIRTY_EXACT, 4,
	                                         64, 10);
static SuffixPreparationBenchmark suffix_dirty_8("SuffixPrepareDirty008x064", SuffixPreparationMode::DIRTY_EXACT, 8,
	                                         64, 10);
static SuffixPreparationBenchmark suffix_dirty_9("SuffixPrepareDirty009x064", SuffixPreparationMode::DIRTY_EXACT, 9,
	                                         64, 10);
static SuffixPreparationBenchmark suffix_dirty_50("SuffixPrepareDirty050x064", SuffixPreparationMode::DIRTY_EXACT, 50,
	                                          64, 10);
static SuffixPreparationBenchmark suffix_dirty_100("SuffixPrepareDirty100x064", SuffixPreparationMode::DIRTY_EXACT,
	                                           100, 64, 10);
static SuffixPreparationBenchmark suffix_scale_fallback("SuffixPrepareScaleFallback064",
	                                                    SuffixPreparationMode::SCALE_FALLBACK, 100, 64, 10);
static SuffixPreparationBenchmark suffix_global_fallback("SuffixPrepareGlobalFallback",
	                                                     SuffixPreparationMode::GLOBAL_FALLBACK);
static SuffixPreparationBenchmark suffix_cached_epoch_16("SuffixPrepareCachedEpoch016",
	                                                     SuffixPreparationMode::CACHED_EPOCH_BATCH, 100, 16, 25);
static SuffixPreparationBenchmark suffix_cached_epoch_64("SuffixPrepareCachedEpoch064",
	                                                     SuffixPreparationMode::CACHED_EPOCH_BATCH, 100, 64, 25);
static SuffixPreparationBenchmark suffix_cached_epoch_128("SuffixPrepareCachedEpoch128",
	                                                      SuffixPreparationMode::CACHED_EPOCH_BATCH, 100, 128, 25);
static SuffixPreparationBenchmark suffix_epoch_16("SuffixPrepareEpoch016", SuffixPreparationMode::EPOCH_BATCH, 100, 16,
	                                          10);
static SuffixPreparationBenchmark suffix_epoch_64("SuffixPrepareEpoch064", SuffixPreparationMode::EPOCH_BATCH, 100, 64,
	                                          10);
static SuffixPreparationBenchmark suffix_epoch_128("SuffixPrepareEpoch128", SuffixPreparationMode::EPOCH_BATCH, 100,
	                                           128, 10);
static SuffixPreparationBenchmark suffix_epoch_128_multi_scale("SuffixPrepareEpoch128x2Scales",
	                                                          SuffixPreparationMode::EPOCH_BATCH_MULTI_SCALE, 100,
	                                                          128, 10);
static SuffixPreparationBenchmark suffix_concurrent("SuffixPrepareConcurrentUpdates",
	                                                SuffixPreparationMode::CONCURRENT_UPDATES, 100, 1, 10);

} // namespace
