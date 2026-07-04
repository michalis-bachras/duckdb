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
	uint64_t pipeline_signature_hash = 0;

	bool operator==(const PipelineProfileKey &other) const {
		return template_id == other.template_id && scale_factor == other.scale_factor &&
		       pipeline_signature_hash == other.pipeline_signature_hash;
	}
};

struct PipelineProfileKeyHash {
	size_t operator()(const PipelineProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.template_id);
		result ^= std::hash<uint64_t> {}(key.scale_factor) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
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
};

static std::mutex g_profile_store_lock;
static std::unordered_map<QueryProfileKey, QueryProfileAggregate, QueryProfileKeyHash> g_query_profiles;
static std::unordered_map<PipelineProfileKey, PipelineProfileAggregate, PipelineProfileKeyHash> g_pipeline_profiles;

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
		pipeline_key.pipeline_signature_hash = signature_hash;
		auto &pipeline_profile = g_pipeline_profiles[pipeline_key];
		pipeline_profile.template_id = metadata.template_id;
		pipeline_profile.scale_factor = metadata.scale_factor;
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
		pipeline_profile.planned_input_chunks_equiv.Add(static_cast<double>(profile.planned_input_chunks_equiv));
		pipeline_profile.task_count.Add(static_cast<double>(profile.task_count));
		if (profile.start_ns > 0 && profile.tasks_done_ns >= profile.start_ns) {
			pipeline_profile.task_runtime_ns.Add(static_cast<double>(profile.tasks_done_ns - profile.start_ns));
			pipeline_profile.downstream_suffix_ns.Add(
			    static_cast<double>(DurationNs(query_end_ns, profile.tasks_done_ns)));
		}
		if (profile.start_ns > 0 && profile.finish_done_ns >= profile.start_ns) {
			pipeline_profile.lifecycle_runtime_ns.Add(static_cast<double>(profile.finish_done_ns - profile.start_ns));
		}
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

bool QueryRequestProfileStore::TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor,
                                                      uint64_t pipeline_signature_hash,
                                                      QueryRequestPipelineProfileEstimate &estimate) const {
	lock_guard<std::mutex> guard(g_profile_store_lock);
	PipelineProfileKey key;
	key.template_id = template_id;
	key.scale_factor = scale_factor;
	key.pipeline_signature_hash = pipeline_signature_hash;
	auto entry = g_pipeline_profiles.find(key);
	if (entry == g_pipeline_profiles.end()) {
		return false;
	}
	PopulatePipelineEstimate(entry->second, estimate);
	return true;
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
}

} // namespace duckdb
