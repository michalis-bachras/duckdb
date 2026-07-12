//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_request_profile_store.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/query_request_profile_store.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cmath>
#include <deque>
#include <mutex>
#include <unordered_map>

namespace duckdb {

namespace {

static const idx_t PROFILE_SAMPLE_LIMIT = 100;
static constexpr double THROUGHPUT_EWMA_ALPHA = 0.7;

struct BoundedSamples {
	std::deque<double> values;

	void Add(double value) {
		if (!std::isfinite(value)) {
			return;
		}
		if (values.size() >= PROFILE_SAMPLE_LIMIT) {
			values.pop_front();
		}
		values.push_back(value);
	}

	idx_t Count() const {
		return values.size();
	}

	double Mean() const {
		if (values.empty()) {
			return 0;
		}
		double total = 0;
		for (idx_t i = 0; i < values.size(); i++) {
			total += values[i];
		}
		return total / static_cast<double>(values.size());
	}

	double Last() const {
		return values.empty() ? 0 : values.back();
	}

	double Percentile(double quantile) const {
		if (values.empty()) {
			return 0;
		}
		vector<double> sorted(values.begin(), values.end());
		std::sort(sorted.begin(), sorted.end());
		if (sorted.size() == 1) {
			return sorted[0];
		}
		auto position = quantile * static_cast<double>(sorted.size() - 1);
		auto lower_index = static_cast<idx_t>(std::floor(position));
		auto upper_index = static_cast<idx_t>(std::ceil(position));
		if (lower_index == upper_index) {
			return sorted[lower_index];
		}
		auto fraction = position - static_cast<double>(lower_index);
		return sorted[lower_index] * (1.0 - fraction) + sorted[upper_index] * fraction;
	}
};

struct QueryProfileKey {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;

	bool operator==(const QueryProfileKey &other) const {
		return template_id == other.template_id && scale_factor == other.scale_factor;
	}
};

struct QueryProfileKeyHash {
	size_t operator()(const QueryProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.template_id);
		result ^= std::hash<uint64_t> {}(key.scale_factor) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct PipelineProfileKey {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;

	bool operator==(const PipelineProfileKey &other) const {
		return template_id == other.template_id && scale_factor == other.scale_factor &&
		       pipeline_id == other.pipeline_id && pipeline_signature_hash == other.pipeline_signature_hash;
	}
};

struct PipelineProfileKeyHash {
	size_t operator()(const PipelineProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.template_id);
		result ^= std::hash<uint64_t> {}(key.scale_factor) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<idx_t> {}(key.pipeline_id) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<uint64_t> {}(key.pipeline_signature_hash) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		return result;
	}
};

struct QueryProfileAggregate {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	BoundedSamples runtime_ns;
	BoundedSamples lateness_ns;
	BoundedSamples sla_cost;
};

struct PipelineProfileAggregate {
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
	BoundedSamples source_max_threads;
	BoundedSamples planned_input_rows;
	BoundedSamples planned_input_chunks_equiv;
	BoundedSamples task_count;
	BoundedSamples task_runtime_ns;
	BoundedSamples lifecycle_runtime_ns;
	BoundedSamples downstream_suffix_ns;
	BoundedSamples single_worker_chunks_per_s;
	double ewma_single_worker_chunks_per_s = 0;
};

static std::mutex g_profile_store_lock;
static std::unordered_map<QueryProfileKey, QueryProfileAggregate, QueryProfileKeyHash> g_query_profiles;
static std::unordered_map<PipelineProfileKey, PipelineProfileAggregate, PipelineProfileKeyHash> g_pipeline_profiles;
static vector<QueryRequestSampleSnapshot> g_query_samples;
static vector<QueryRequestPipelineInstanceSnapshot> g_pipeline_instances;

static uint64_t DurationNs(uint64_t end_ns, uint64_t start_ns) {
	if (end_ns <= start_ns) {
		return 0;
	}
	return end_ns - start_ns;
}

static uint64_t StableStringHash64(const string &value) {
	uint64_t hash = 1469598103934665603ULL;
	for (idx_t i = 0; i < value.size(); i++) {
		hash ^= static_cast<unsigned char>(value[i]);
		hash *= 1099511628211ULL;
	}
	return hash;
}

static double SingleWorkerChunksPerSecond(idx_t chunks_equiv, uint64_t worker_time_ns) {
	if (chunks_equiv == 0 || worker_time_ns == 0) {
		return 0;
	}
	return (static_cast<double>(chunks_equiv) * 1000000000.0) / static_cast<double>(worker_time_ns);
}

static string BuildPipelineSignature(const PipelineProfilingInfo &profile) {
	return StringUtil::Format(
	    "%s|source=%s|sink=%s|input=%s|confidence=%s|native=%s|source_card=%llu|sink_card=%llu",
	    profile.operator_type_sequence, profile.source_type, profile.sink_type, profile.source_input_kind,
	    profile.source_input_confidence, profile.planned_input_native_unit,
	    static_cast<unsigned long long>(profile.source_estimated_cardinality),
	    static_cast<unsigned long long>(profile.sink_estimated_cardinality));
}

static void PopulateQueryEstimate(const QueryProfileAggregate &profile, QueryRequestProfileEstimate &estimate) {
	estimate.valid = true;
	estimate.sample_count = profile.runtime_ns.Count();
	estimate.mean_runtime_ns = profile.runtime_ns.Mean();
	estimate.p50_runtime_ns = profile.runtime_ns.Percentile(0.5);
	estimate.p90_runtime_ns = profile.runtime_ns.Percentile(0.9);
	estimate.p99_runtime_ns = profile.runtime_ns.Percentile(0.99);
	estimate.last_runtime_ns = profile.runtime_ns.Last();
	estimate.mean_lateness_ns = profile.lateness_ns.Mean();
	estimate.mean_sla_cost = profile.sla_cost.Mean();
}

static void PopulatePipelineEstimate(const PipelineProfileAggregate &profile,
                                     QueryRequestPipelineProfileEstimate &estimate) {
	estimate.valid = true;
	estimate.sample_count = profile.task_count.Count();
	estimate.pipeline_id = profile.pipeline_id;
	estimate.pipeline_signature_hash = profile.pipeline_signature_hash;
	estimate.pipeline_signature = profile.pipeline_signature;
	estimate.operator_type_sequence = profile.operator_type_sequence;
	estimate.source_type = profile.source_type;
	estimate.sink_type = profile.sink_type;
	estimate.source_input_kind = profile.source_input_kind;
	estimate.source_input_confidence = profile.source_input_confidence;
	estimate.planned_input_native_unit = profile.planned_input_native_unit;
	estimate.mean_task_runtime_ns = profile.task_runtime_ns.Mean();
	estimate.p90_task_runtime_ns = profile.task_runtime_ns.Percentile(0.9);
	estimate.mean_lifecycle_runtime_ns = profile.lifecycle_runtime_ns.Mean();
	estimate.mean_downstream_suffix_ns = profile.downstream_suffix_ns.Mean();
	estimate.mean_task_count = profile.task_count.Mean();
	estimate.mean_source_max_threads = profile.source_max_threads.Mean();
	estimate.mean_planned_input_rows = profile.planned_input_rows.Mean();
	estimate.mean_planned_input_chunks_equiv = profile.planned_input_chunks_equiv.Mean();
	estimate.throughput_sample_count = profile.single_worker_chunks_per_s.Count();
	estimate.mean_single_worker_chunks_per_s = profile.single_worker_chunks_per_s.Mean();
	estimate.ewma_single_worker_chunks_per_s = profile.ewma_single_worker_chunks_per_s;
}

} // namespace

QueryRequestProfileStore &QueryRequestProfileStore::Get() {
	static QueryRequestProfileStore store;
	return store;
}

void QueryRequestProfileStore::RecordQueryCompletion(const QueryRequestMetadata &metadata, uint64_t query_end_ns,
                                                     const vector<PipelineProfilingInfo> &pipeline_profiles) {
	if (!metadata.valid || metadata.query_start_ns == 0 || query_end_ns <= metadata.query_start_ns) {
		return;
	}
	auto runtime_ns = DurationNs(query_end_ns, metadata.query_start_ns);
	auto lateness_ns = metadata.deadline_ns > 0 ? DurationNs(query_end_ns, metadata.deadline_ns) : 0;
	auto sla_cost =
	    (static_cast<double>(lateness_ns) / 1000000000.0) * static_cast<double>(metadata.sla_penalty_per_s);

	lock_guard<std::mutex> guard(g_profile_store_lock);
	QueryRequestSampleSnapshot query_sample;
	query_sample.db_query_id = metadata.db_query_id;
	query_sample.request_id = metadata.request_id;
	query_sample.template_id = metadata.template_id;
	query_sample.scale_factor = metadata.scale_factor;
	query_sample.sla_tag = metadata.sla_tag;
	query_sample.sla_penalty_per_s = metadata.sla_penalty_per_s;
	query_sample.query_start_ns = metadata.query_start_ns;
	query_sample.query_end_ns = query_end_ns;
	query_sample.runtime_ns = runtime_ns;
	query_sample.deadline_ns = metadata.deadline_ns;
	query_sample.lateness_ns = lateness_ns;
	query_sample.sla_cost = sla_cost;
	query_sample.deadline_met = lateness_ns == 0;
	g_query_samples.push_back(std::move(query_sample));

	QueryProfileKey query_key;
	query_key.template_id = metadata.template_id;
	query_key.scale_factor = metadata.scale_factor;
	auto &query_profile = g_query_profiles[query_key];
	query_profile.template_id = metadata.template_id;
	query_profile.scale_factor = metadata.scale_factor;
	query_profile.runtime_ns.Add(static_cast<double>(runtime_ns));
	query_profile.lateness_ns.Add(static_cast<double>(lateness_ns));
	query_profile.sla_cost.Add(sla_cost);

	for (idx_t i = 0; i < pipeline_profiles.size(); i++) {
		const auto &profile = pipeline_profiles[i];
		auto signature = BuildPipelineSignature(profile);
		auto signature_hash = StableStringHash64(signature);
		PipelineProfileKey pipeline_key;
		pipeline_key.template_id = metadata.template_id;
		pipeline_key.scale_factor = metadata.scale_factor;
		pipeline_key.pipeline_id = profile.pipeline_id;
		pipeline_key.pipeline_signature_hash = signature_hash;
		auto &pipeline_profile = g_pipeline_profiles[pipeline_key];
		pipeline_profile.template_id = metadata.template_id;
		pipeline_profile.scale_factor = metadata.scale_factor;
		pipeline_profile.pipeline_id = profile.pipeline_id;
		pipeline_profile.pipeline_signature_hash = signature_hash;
		pipeline_profile.pipeline_signature = signature;
		pipeline_profile.operator_type_sequence = profile.operator_type_sequence;
		pipeline_profile.source_type = profile.source_type;
		pipeline_profile.sink_type = profile.sink_type;
		pipeline_profile.source_input_kind = profile.source_input_kind;
		pipeline_profile.source_input_confidence = profile.source_input_confidence;
		pipeline_profile.planned_input_native_unit = profile.planned_input_native_unit;
		pipeline_profile.source_max_threads.Add(static_cast<double>(profile.source_max_threads));
		pipeline_profile.planned_input_rows.Add(static_cast<double>(profile.planned_input_rows));
		// Store planned work in the same unit family as live remaining work. Scheduler throughput uses summed worker
		// task time below; wall-clock pipeline runtime would overstate single-worker throughput under parallelism.
		pipeline_profile.planned_input_chunks_equiv.Add(static_cast<double>(profile.planned_input_chunks_equiv));
		pipeline_profile.task_count.Add(static_cast<double>(profile.task_count));
		QueryRequestPipelineInstanceSnapshot pipeline_instance;
		pipeline_instance.db_query_id = metadata.db_query_id;
		pipeline_instance.request_id = metadata.request_id;
		pipeline_instance.template_id = metadata.template_id;
		pipeline_instance.scale_factor = metadata.scale_factor;
		pipeline_instance.pipeline_id = profile.pipeline_id;
		pipeline_instance.pipeline_signature_hash = signature_hash;
		pipeline_instance.pipeline_signature = signature;
		pipeline_instance.operator_type_sequence = profile.operator_type_sequence;
		pipeline_instance.source_type = profile.source_type;
		pipeline_instance.sink_type = profile.sink_type;
		pipeline_instance.source_input_kind = profile.source_input_kind;
		pipeline_instance.source_input_confidence = profile.source_input_confidence;
		pipeline_instance.planned_input_native_unit = profile.planned_input_native_unit;
		pipeline_instance.task_count = profile.task_count;
		pipeline_instance.source_max_threads = profile.source_max_threads;
		pipeline_instance.planned_input_rows = profile.planned_input_rows;
		pipeline_instance.planned_input_chunks_equiv = profile.planned_input_chunks_equiv;
		pipeline_instance.worker_task_count = profile.worker_task_count;
		pipeline_instance.worker_task_duration_ns = profile.worker_task_duration_ns;
		auto single_worker_chunks_per_s =
		    SingleWorkerChunksPerSecond(profile.planned_input_chunks_equiv, profile.worker_task_duration_ns);
		if (single_worker_chunks_per_s > 0) {
			pipeline_profile.single_worker_chunks_per_s.Add(single_worker_chunks_per_s);
			pipeline_profile.ewma_single_worker_chunks_per_s =
			    pipeline_profile.single_worker_chunks_per_s.Count() == 1
			        ? single_worker_chunks_per_s
			        : THROUGHPUT_EWMA_ALPHA * single_worker_chunks_per_s +
			              (1.0 - THROUGHPUT_EWMA_ALPHA) * pipeline_profile.ewma_single_worker_chunks_per_s;
			pipeline_instance.single_worker_chunks_per_s = single_worker_chunks_per_s;
			pipeline_instance.throughput_valid = true;
		}
		pipeline_instance.source_estimated_cardinality = profile.source_estimated_cardinality;
		pipeline_instance.sink_estimated_cardinality = profile.sink_estimated_cardinality;
		pipeline_instance.start_ns = profile.start_ns;
		pipeline_instance.tasks_done_ns = profile.tasks_done_ns;
		pipeline_instance.finish_done_ns = profile.finish_done_ns;
		if (profile.start_ns > 0 && profile.tasks_done_ns >= profile.start_ns) {
			auto task_runtime_ns = profile.tasks_done_ns - profile.start_ns;
			auto downstream_suffix_ns = DurationNs(query_end_ns, profile.tasks_done_ns);
			pipeline_profile.task_runtime_ns.Add(static_cast<double>(task_runtime_ns));
			pipeline_profile.downstream_suffix_ns.Add(static_cast<double>(downstream_suffix_ns));
			pipeline_instance.task_runtime_ns = task_runtime_ns;
			pipeline_instance.downstream_suffix_ns = downstream_suffix_ns;
		}
		if (profile.start_ns > 0 && profile.finish_done_ns >= profile.start_ns) {
			auto lifecycle_runtime_ns = profile.finish_done_ns - profile.start_ns;
			pipeline_profile.lifecycle_runtime_ns.Add(static_cast<double>(lifecycle_runtime_ns));
			pipeline_instance.lifecycle_runtime_ns = lifecycle_runtime_ns;
		}
		g_pipeline_instances.push_back(std::move(pipeline_instance));
	}
}

bool QueryRequestProfileStore::TryGetQueryEstimate(uint64_t template_id, uint64_t scale_factor,
                                                   QueryRequestProfileEstimate &estimate) const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	QueryProfileKey key;
	key.template_id = template_id;
	key.scale_factor = scale_factor;
	auto entry = g_query_profiles.find(key);
	if (entry == g_query_profiles.end()) {
		return false;
	}
	PopulateQueryEstimate(entry->second, estimate);
	return true;
}

bool QueryRequestProfileStore::TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor, idx_t pipeline_id,
                                                      uint64_t pipeline_signature_hash,
                                                      QueryRequestPipelineProfileEstimate &estimate) const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	PipelineProfileKey key;
	key.template_id = template_id;
	key.scale_factor = scale_factor;
	key.pipeline_id = pipeline_id;
	key.pipeline_signature_hash = pipeline_signature_hash;
	auto entry = g_pipeline_profiles.find(key);
	if (entry == g_pipeline_profiles.end()) {
		return false;
	}
	PopulatePipelineEstimate(entry->second, estimate);
	return true;
}

vector<QueryRequestProfileSnapshot> QueryRequestProfileStore::GetQueryProfilesSnapshot() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	vector<QueryRequestProfileSnapshot> result;
	result.reserve(g_query_profiles.size());
	for (const auto &entry : g_query_profiles) {
		QueryRequestProfileSnapshot snapshot;
		snapshot.template_id = entry.second.template_id;
		snapshot.scale_factor = entry.second.scale_factor;
		PopulateQueryEstimate(entry.second, snapshot.estimate);
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestProfileSnapshot &left,
	                                           const QueryRequestProfileSnapshot &right) {
		if (left.template_id != right.template_id) {
			return left.template_id < right.template_id;
		}
		return left.scale_factor < right.scale_factor;
	});
	return result;
}

vector<QueryRequestPipelineProfileSnapshot> QueryRequestProfileStore::GetPipelineProfilesSnapshot() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	vector<QueryRequestPipelineProfileSnapshot> result;
	result.reserve(g_pipeline_profiles.size());
	for (const auto &entry : g_pipeline_profiles) {
		QueryRequestPipelineProfileSnapshot snapshot;
		snapshot.template_id = entry.second.template_id;
		snapshot.scale_factor = entry.second.scale_factor;
		PopulatePipelineEstimate(entry.second, snapshot.estimate);
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestPipelineProfileSnapshot &left,
	                                           const QueryRequestPipelineProfileSnapshot &right) {
		if (left.template_id != right.template_id) {
			return left.template_id < right.template_id;
		}
		if (left.scale_factor != right.scale_factor) {
			return left.scale_factor < right.scale_factor;
		}
		if (left.estimate.pipeline_id != right.estimate.pipeline_id) {
			return left.estimate.pipeline_id < right.estimate.pipeline_id;
		}
		return left.estimate.pipeline_signature_hash < right.estimate.pipeline_signature_hash;
	});
	return result;
}

vector<QueryRequestSampleSnapshot> QueryRequestProfileStore::GetQuerySamplesSnapshot() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	auto result = g_query_samples;
	std::sort(result.begin(), result.end(), [](const QueryRequestSampleSnapshot &left,
	                                           const QueryRequestSampleSnapshot &right) {
		if (left.db_query_id != right.db_query_id) {
			return left.db_query_id < right.db_query_id;
		}
		return left.request_id < right.request_id;
	});
	return result;
}

vector<QueryRequestPipelineInstanceSnapshot> QueryRequestProfileStore::GetPipelineInstancesSnapshot() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	auto result = g_pipeline_instances;
	std::sort(result.begin(), result.end(), [](const QueryRequestPipelineInstanceSnapshot &left,
	                                           const QueryRequestPipelineInstanceSnapshot &right) {
		if (left.db_query_id != right.db_query_id) {
			return left.db_query_id < right.db_query_id;
		}
		if (left.pipeline_id != right.pipeline_id) {
			return left.pipeline_id < right.pipeline_id;
		}
		return left.pipeline_signature_hash < right.pipeline_signature_hash;
	});
	return result;
}

idx_t QueryRequestProfileStore::QueryProfileCount() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	return g_query_profiles.size();
}

idx_t QueryRequestProfileStore::PipelineProfileCount() const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	return g_pipeline_profiles.size();
}

void QueryRequestProfileStore::Clear() {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	g_query_profiles.clear();
	g_pipeline_profiles.clear();
	g_query_samples.clear();
	g_pipeline_instances.clear();
}

} // namespace duckdb
