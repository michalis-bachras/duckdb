//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_request_profile_store.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/query_request_profile_store.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <deque>
#include <mutex>
#include <set>
#include <unordered_map>

namespace duckdb {

namespace query_request_profile_store_internal {

static const idx_t PROFILE_SAMPLE_LIMIT = 100;
static const idx_t DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES = 4;
static constexpr double THROUGHPUT_EWMA_ALPHA = 0.7;
static constexpr double CONTINUATION_MEDIAN_QUANTILE = 0.50;
static constexpr double CONTINUATION_TAIL_QUANTILE = 0.90;
static constexpr uint32_t PROFILE_SNAPSHOT_VERSION = 2;
static const char *PROFILE_SNAPSHOT_MAGIC = "DUCKDB_QUERY_REQUEST_PROFILE";

class ProfileSnapshotWriter {
public:
	ProfileSnapshotWriter(FileSystem &fs, const string &path)
	    : writer(fs, path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
	}

	template <class T>
	void Write(const T &value) {
		writer.WriteData(const_data_ptr_cast(&value), sizeof(T));
	}

	void WriteString(const string &value) {
		Write<uint64_t>(value.size());
		if (!value.empty()) {
			writer.WriteData(const_data_ptr_cast(value.data()), value.size());
		}
	}

	void Flush() {
		writer.Flush();
	}

private:
	BufferedFileWriter writer;
};

class ProfileSnapshotReader {
public:
	ProfileSnapshotReader(FileSystem &fs, const string &path) : reader(fs, path.c_str()) {
	}

	template <class T>
	T Read() {
		T value;
		reader.ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}

	uint64_t ReadCount(uint64_t maximum, const char *name) {
		auto count = Read<uint64_t>();
		if (count > maximum) {
			throw SerializationException("Invalid %s count in query profile snapshot", name);
		}
		return count;
	}

	string ReadString() {
		auto size = ReadCount(1ULL << 24, "string");
		string result(size, '\0');
		if (size > 0) {
			reader.ReadData(data_ptr_cast(&result[0]), size);
		}
		return result;
	}

	bool Finished() {
		return reader.Finished();
	}

private:
	BufferedFileReader reader;
};

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

struct SuffixSampleWindow {
	std::array<double, PROFILE_SAMPLE_LIMIT> values {};
	idx_t head = 0;
	idx_t count = 0;
	uint64_t generation = 0;

	void Add(double value) {
		if (!std::isfinite(value) || value < 0) {
			return;
		}
		values[head] = value;
		head = (head + 1) % PROFILE_SAMPLE_LIMIT;
		count = MinValue<idx_t>(count + 1, PROFILE_SAMPLE_LIMIT);
		generation++;
	}

	idx_t Count() const {
		return count;
	}

	double Mean() const {
		if (count == 0) {
			return 0;
		}
		double total = 0;
		auto first = (head + PROFILE_SAMPLE_LIMIT - count) % PROFILE_SAMPLE_LIMIT;
		for (idx_t i = 0; i < count; i++) {
			total += values[(first + i) % PROFILE_SAMPLE_LIMIT];
		}
		return total / static_cast<double>(count);
	}

	void CopyTo(std::array<double, PROFILE_SAMPLE_LIMIT> &target) const {
		auto first = (head + PROFILE_SAMPLE_LIMIT - count) % PROFILE_SAMPLE_LIMIT;
		for (idx_t i = 0; i < count; i++) {
			target[i] = values[(first + i) % PROFILE_SAMPLE_LIMIT];
		}
	}
};

struct SuffixHistogramAggregate {
	SuffixSampleWindow samples;
	DownstreamSuffixHistogram cached;
	bool cache_valid = false;
	bool dirty = false;

	void Add(double value) {
		auto previous_generation = samples.generation;
		samples.Add(value);
		if (samples.generation != previous_generation) {
			dirty = true;
		}
	}
};

static void AddHistogramBucket(DownstreamSuffixHistogram &histogram, double lower, double upper,
	                           double probability) {
	if (probability <= 0 || histogram.bucket_count >= DOWNSTREAM_SUFFIX_MAX_BUCKETS) {
		return;
	}
	if (histogram.bucket_count > 0) {
		auto &previous = histogram.buckets[histogram.bucket_count - 1];
		if (previous.lower_ns == lower && previous.upper_ns == upper) {
			previous.probability += probability;
			return;
		}
	}
	auto &bucket = histogram.buckets[histogram.bucket_count++];
	bucket.lower_ns = lower;
	bucket.upper_ns = upper;
	bucket.probability = probability;
}

static DownstreamSuffixHistogram BuildSuffixHistogram(const SuffixSampleWindow &window) {
	DownstreamSuffixHistogram result;
	result.sample_count = window.Count();
	result.generation = window.generation;
	if (result.sample_count == 0) {
		return result;
	}
	std::array<double, PROFILE_SAMPLE_LIMIT> sorted;
	window.CopyTo(sorted);
	std::sort(sorted.begin(), sorted.begin() + result.sample_count);
	if (result.sample_count <= DOWNSTREAM_SUFFIX_MAX_BUCKETS) {
		for (idx_t i = 0; i < result.sample_count; i++) {
			AddHistogramBucket(result, sorted[i], sorted[i], 1.0 / static_cast<double>(result.sample_count));
		}
		return result;
	}
	for (idx_t bucket_idx = 0; bucket_idx < DOWNSTREAM_SUFFIX_MAX_BUCKETS; bucket_idx++) {
		auto begin = (bucket_idx * result.sample_count) / DOWNSTREAM_SUFFIX_MAX_BUCKETS;
		auto end = ((bucket_idx + 1) * result.sample_count) / DOWNSTREAM_SUFFIX_MAX_BUCKETS;
		D_ASSERT(end > begin);
		AddHistogramBucket(result, sorted[begin], sorted[end - 1],
		                   static_cast<double>(end - begin) / static_cast<double>(result.sample_count));
	}
	return result;
}

struct QuantileEntry {
	double value = 0;
	uint64_t id = 0;

	QuantileEntry() = default;
	QuantileEntry(double value_p, uint64_t id_p) : value(value_p), id(id_p) {
	}

	bool operator<(const QuantileEntry &other) const {
		if (value < other.value) {
			return true;
		}
		if (value > other.value) {
			return false;
		}
		return id < other.id;
	}
};

class BoundedQuantileTracker {
public:
	explicit BoundedQuantileTracker(double quantile_p) : quantile(quantile_p) {
	}

	void Add(double value) {
		if (!std::isfinite(value)) {
			return;
		}
		QuantileEntry entry {value, next_id++};
		if (lower.empty() || value <= lower.rbegin()->value) {
			lower.insert(entry);
		} else {
			upper.insert(entry);
		}
		window.push_back(entry);
		if (window.size() > PROFILE_SAMPLE_LIMIT) {
			Remove(window.front());
			window.pop_front();
		}
		Rebalance();
	}

	idx_t Count() const {
		return lower.size() + upper.size();
	}

	double Quantile() const {
		return lower.empty() ? 0 : lower.rbegin()->value;
	}

private:
	idx_t TargetLowerSize() const {
		auto count = Count();
		if (count == 0) {
			return 0;
		}
		auto target = static_cast<idx_t>(std::ceil(quantile * static_cast<double>(count)));
		return MaxValue<idx_t>(idx_t(1), MinValue<idx_t>(target, count));
	}

	void Remove(const QuantileEntry &entry) {
		auto lower_entry = lower.find(entry);
		if (lower_entry != lower.end()) {
			lower.erase(lower_entry);
			return;
		}
		auto upper_entry = upper.find(entry);
		if (upper_entry != upper.end()) {
			upper.erase(upper_entry);
		}
	}

	void MoveLowerToUpper() {
		D_ASSERT(!lower.empty());
		auto entry = std::prev(lower.end());
		upper.insert(*entry);
		lower.erase(entry);
	}

	void MoveUpperToLower() {
		D_ASSERT(!upper.empty());
		auto entry = upper.begin();
		lower.insert(*entry);
		upper.erase(entry);
	}

	void RebalanceOrdering() {
		while (!lower.empty() && !upper.empty() && lower.rbegin()->value > upper.begin()->value) {
			auto lower_entry = std::prev(lower.end());
			auto upper_entry = upper.begin();
			auto lower_value = *lower_entry;
			auto upper_value = *upper_entry;
			lower.erase(lower_entry);
			upper.erase(upper_entry);
			lower.insert(upper_value);
			upper.insert(lower_value);
		}
	}

	void Rebalance() {
		RebalanceOrdering();
		auto target = TargetLowerSize();
		while (lower.size() > target) {
			MoveLowerToUpper();
		}
		while (lower.size() < target && !upper.empty()) {
			MoveUpperToLower();
		}
		RebalanceOrdering();
	}

	std::deque<QuantileEntry> window;
	std::multiset<QuantileEntry> lower;
	std::multiset<QuantileEntry> upper;
	uint64_t next_id = 1;
	double quantile;
};

struct ContinuationAggregate {
	BoundedSamples values;
	BoundedQuantileTracker p50 {CONTINUATION_MEDIAN_QUANTILE};
	BoundedQuantileTracker p90 {CONTINUATION_TAIL_QUANTILE};
	string native_unit;
	idx_t native_unit_mismatch_count = 0;

	bool Add(double value, const string &native_unit_p = string()) {
		if (!native_unit_p.empty()) {
			if (native_unit.empty()) {
				native_unit = native_unit_p;
			} else if (native_unit != native_unit_p) {
				native_unit_mismatch_count++;
				return false;
			}
		}
		values.Add(value);
		p50.Add(value);
		p90.Add(value);
		return true;
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

struct InternalEventProfileKey {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	idx_t event_position = 0;
	string event_type;
	string native_unit;

	bool operator==(const InternalEventProfileKey &other) const {
		return template_id == other.template_id && scale_factor == other.scale_factor &&
		       pipeline_id == other.pipeline_id && pipeline_signature_hash == other.pipeline_signature_hash &&
		       event_position == other.event_position && event_type == other.event_type && native_unit == other.native_unit;
	}
};

struct InternalEventProfileKeyHash {
	size_t operator()(const InternalEventProfileKey &key) const {
		PipelineProfileKey pipeline_key;
		pipeline_key.template_id = key.template_id;
		pipeline_key.scale_factor = key.scale_factor;
		pipeline_key.pipeline_id = key.pipeline_id;
		pipeline_key.pipeline_signature_hash = key.pipeline_signature_hash;
		size_t result = PipelineProfileKeyHash {}(pipeline_key);
		result ^= std::hash<idx_t> {}(key.event_position) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<string> {}(key.event_type) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<string> {}(key.native_unit) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct InternalEventFallbackKey {
	uint64_t scale_factor = 0;
	string event_type;
	string native_unit;

	bool operator==(const InternalEventFallbackKey &other) const {
		return scale_factor == other.scale_factor && event_type == other.event_type && native_unit == other.native_unit;
	}
};

struct InternalEventFallbackKeyHash {
	size_t operator()(const InternalEventFallbackKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.scale_factor);
		result ^= std::hash<string> {}(key.event_type) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<string> {}(key.native_unit) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct SourceWorkClassHash {
	size_t operator()(const SourceWorkClass &work_class) const {
		size_t result = std::hash<uint8_t> {}(static_cast<uint8_t>(work_class.source_type));
		result ^= std::hash<uint8_t> {}(static_cast<uint8_t>(work_class.work_kind)) + 0x9e3779b97f4a7c15ULL +
		          (result << 6) + (result >> 2);
		return result;
	}
};

struct SourceSinkContinuationKey {
	SourceWorkClass source_work_class;
	PhysicalOperatorType sink_type = PhysicalOperatorType::INVALID;

	bool operator==(const SourceSinkContinuationKey &other) const {
		return source_work_class == other.source_work_class && sink_type == other.sink_type;
	}
};

struct SourceSinkContinuationKeyHash {
	size_t operator()(const SourceSinkContinuationKey &key) const {
		size_t result = SourceWorkClassHash {}(key.source_work_class);
		result ^= std::hash<uint8_t> {}(static_cast<uint8_t>(key.sink_type)) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		return result;
	}
};

struct SourceSinkThroughputKey {
	SourceWorkClass source_work_class;
	PhysicalOperatorType sink_type = PhysicalOperatorType::INVALID;
	string native_unit;

	bool operator==(const SourceSinkThroughputKey &other) const {
		return source_work_class == other.source_work_class && sink_type == other.sink_type &&
		       native_unit == other.native_unit;
	}
};

struct SourceSinkThroughputKeyHash {
	size_t operator()(const SourceSinkThroughputKey &key) const {
		size_t result = SourceWorkClassHash {}(key.source_work_class);
		result ^= std::hash<uint8_t> {}(static_cast<uint8_t>(key.sink_type)) + 0x9e3779b97f4a7c15ULL +
		          (result << 6) + (result >> 2);
		result ^= std::hash<string> {}(key.native_unit) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct SourceThroughputKey {
	SourceWorkClass source_work_class;
	string native_unit;

	bool operator==(const SourceThroughputKey &other) const {
		return source_work_class == other.source_work_class && native_unit == other.native_unit;
	}
};

struct SourceThroughputKeyHash {
	size_t operator()(const SourceThroughputKey &key) const {
		size_t result = SourceWorkClassHash {}(key.source_work_class);
		result ^= std::hash<string> {}(key.native_unit) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct CompatibleThroughputKey {
	SourceThroughputKind work_kind = SourceThroughputKind::UNKNOWN;
	string native_unit;

	bool operator==(const CompatibleThroughputKey &other) const {
		return work_kind == other.work_kind && native_unit == other.native_unit;
	}
};

struct CompatibleThroughputKeyHash {
	size_t operator()(const CompatibleThroughputKey &key) const {
		size_t result = std::hash<uint8_t> {}(static_cast<uint8_t>(key.work_kind));
		result ^= std::hash<string> {}(key.native_unit) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

struct ThroughputAggregate {
	BoundedSamples values;
	double ewma = 0;

	void Add(double value) {
		if (!std::isfinite(value) || value <= 0) {
			return;
		}
		auto first = values.Count() == 0;
		values.Add(value);
		ewma = first ? value : THROUGHPUT_EWMA_ALPHA * value + (1.0 - THROUGHPUT_EWMA_ALPHA) * ewma;
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
	SourceWorkClass source_work_class;
	string source_input_kind;
	string source_input_confidence;
	string planned_input_native_unit;
	BoundedSamples source_max_threads;
	BoundedSamples planned_input_rows;
	BoundedSamples planned_input_chunks_equiv;
	BoundedSamples task_count;
	BoundedSamples task_runtime_ns;
	ContinuationAggregate lifecycle_runtime_ns;
	ContinuationAggregate finish_tail_ns;
	SuffixHistogramAggregate downstream_suffix_ns;
	BoundedSamples single_worker_chunks_per_s;
	ContinuationAggregate continuation;
	double ewma_single_worker_chunks_per_s = 0;
};

struct InternalEventProfileAggregate {
	InternalEventProfileKey key;
	ThroughputAggregate throughput;
	ContinuationAggregate continuation;
	ContinuationAggregate tail_to_pipeline_end;
};

} // namespace query_request_profile_store_internal

using namespace query_request_profile_store_internal;

struct QueryRequestProfileStoreState {
	std::mutex lock;
	std::unordered_map<QueryProfileKey, QueryProfileAggregate, QueryProfileKeyHash> query_profiles;
	std::unordered_map<PipelineProfileKey, PipelineProfileAggregate, PipelineProfileKeyHash> pipeline_profiles;
	std::unordered_map<uint64_t, SuffixHistogramAggregate> scale_downstream_suffix_profiles;
	SuffixHistogramAggregate global_downstream_suffix_profile;
	std::unordered_map<SourceSinkContinuationKey, ContinuationAggregate, SourceSinkContinuationKeyHash>
	    source_sink_continuation_profiles;
	std::unordered_map<SourceWorkClass, ContinuationAggregate, SourceWorkClassHash> source_continuation_profiles;
	ContinuationAggregate global_raw_continuation_profile;
	std::unordered_map<SourceSinkThroughputKey, ThroughputAggregate, SourceSinkThroughputKeyHash>
	    source_sink_throughput_profiles;
	std::unordered_map<SourceThroughputKey, ThroughputAggregate, SourceThroughputKeyHash> source_throughput_profiles;
	std::unordered_map<CompatibleThroughputKey, ThroughputAggregate, CompatibleThroughputKeyHash>
	    global_compatible_throughput_profiles;
	std::unordered_map<InternalEventProfileKey, InternalEventProfileAggregate, InternalEventProfileKeyHash>
	    internal_event_profiles;
	std::unordered_map<InternalEventFallbackKey, InternalEventProfileAggregate, InternalEventFallbackKeyHash>
	    scale_internal_event_profiles;
	std::unordered_map<InternalEventFallbackKey, InternalEventProfileAggregate, InternalEventFallbackKeyHash>
	    global_internal_event_profiles;
	ContinuationAggregate global_finish_tail_profile;
	vector<QueryRequestSampleSnapshot> query_samples;
	vector<QueryRequestPipelineInstanceSnapshot> pipeline_instances;
	vector<QueryRequestInternalEventInstanceSnapshot> internal_event_instances;
};

namespace query_request_profile_store_internal {

static constexpr uint64_t PROFILE_SNAPSHOT_MAX_ENTRIES = 1ULL << 20;

struct ProfileSnapshotState {
	std::unordered_map<QueryProfileKey, QueryProfileAggregate, QueryProfileKeyHash> query_profiles;
	std::unordered_map<PipelineProfileKey, PipelineProfileAggregate, PipelineProfileKeyHash> pipeline_profiles;
	std::unordered_map<uint64_t, SuffixHistogramAggregate> scale_downstream_suffix_profiles;
	SuffixHistogramAggregate global_downstream_suffix_profile;
	std::unordered_map<SourceSinkContinuationKey, ContinuationAggregate, SourceSinkContinuationKeyHash>
	    source_sink_continuation_profiles;
	std::unordered_map<SourceWorkClass, ContinuationAggregate, SourceWorkClassHash> source_continuation_profiles;
	ContinuationAggregate global_raw_continuation_profile;
	std::unordered_map<SourceSinkThroughputKey, ThroughputAggregate, SourceSinkThroughputKeyHash>
	    source_sink_throughput_profiles;
	std::unordered_map<SourceThroughputKey, ThroughputAggregate, SourceThroughputKeyHash> source_throughput_profiles;
	std::unordered_map<CompatibleThroughputKey, ThroughputAggregate, CompatibleThroughputKeyHash>
	    global_compatible_throughput_profiles;
	std::unordered_map<InternalEventProfileKey, InternalEventProfileAggregate, InternalEventProfileKeyHash>
	    internal_event_profiles;
	std::unordered_map<InternalEventFallbackKey, InternalEventProfileAggregate, InternalEventFallbackKeyHash>
	    scale_internal_event_profiles;
	std::unordered_map<InternalEventFallbackKey, InternalEventProfileAggregate, InternalEventFallbackKeyHash>
	    global_internal_event_profiles;
	ContinuationAggregate global_finish_tail_profile;
};

static void WriteBoundedSamples(ProfileSnapshotWriter &writer, const BoundedSamples &samples) {
	writer.Write<uint64_t>(samples.values.size());
	for (const auto value : samples.values) {
		writer.Write<double>(value);
	}
}

static BoundedSamples ReadBoundedSamples(ProfileSnapshotReader &reader) {
	BoundedSamples result;
	auto count = reader.ReadCount(PROFILE_SAMPLE_LIMIT, "sample");
	for (idx_t i = 0; i < count; i++) {
		auto value = reader.Read<double>();
		if (!std::isfinite(value)) {
			throw SerializationException("Non-finite sample in query profile snapshot");
		}
		result.Add(value);
	}
	return result;
}

static void WriteContinuation(ProfileSnapshotWriter &writer, const ContinuationAggregate &profile) {
	writer.WriteString(profile.native_unit);
	writer.Write<uint64_t>(profile.native_unit_mismatch_count);
	WriteBoundedSamples(writer, profile.values);
}

static ContinuationAggregate ReadContinuation(ProfileSnapshotReader &reader) {
	ContinuationAggregate result;
	result.native_unit = reader.ReadString();
	result.native_unit_mismatch_count = reader.Read<uint64_t>();
	auto values = ReadBoundedSamples(reader);
	for (const auto value : values.values) {
		result.Add(value);
	}
	return result;
}

static void WriteThroughput(ProfileSnapshotWriter &writer, const ThroughputAggregate &profile) {
	WriteBoundedSamples(writer, profile.values);
	writer.Write<double>(profile.ewma);
}

static ThroughputAggregate ReadThroughput(ProfileSnapshotReader &reader) {
	ThroughputAggregate result;
	result.values = ReadBoundedSamples(reader);
	result.ewma = reader.Read<double>();
	if ((!result.values.values.empty() && (!std::isfinite(result.ewma) || result.ewma <= 0)) ||
	    (result.values.values.empty() && result.ewma != 0)) {
		throw SerializationException("Invalid throughput EWMA in query profile snapshot");
	}
	return result;
}

static void WriteSuffix(ProfileSnapshotWriter &writer, const SuffixHistogramAggregate &profile) {
	writer.Write<uint64_t>(profile.samples.Count());
	std::array<double, PROFILE_SAMPLE_LIMIT> values;
	profile.samples.CopyTo(values);
	for (idx_t i = 0; i < profile.samples.Count(); i++) {
		writer.Write<double>(values[i]);
	}
}

static SuffixHistogramAggregate ReadSuffix(ProfileSnapshotReader &reader) {
	SuffixHistogramAggregate result;
	auto count = reader.ReadCount(PROFILE_SAMPLE_LIMIT, "downstream suffix sample");
	for (idx_t i = 0; i < count; i++) {
		auto value = reader.Read<double>();
		if (!std::isfinite(value) || value < 0) {
			throw SerializationException("Invalid downstream suffix sample in query profile snapshot");
		}
		result.Add(value);
	}
	return result;
}

static void WriteSourceWorkClass(ProfileSnapshotWriter &writer, const SourceWorkClass &work_class) {
	writer.Write<uint8_t>(static_cast<uint8_t>(work_class.source_type));
	writer.Write<uint8_t>(static_cast<uint8_t>(work_class.work_kind));
}

static SourceWorkClass ReadSourceWorkClass(ProfileSnapshotReader &reader) {
	SourceWorkClass result;
	result.source_type = static_cast<PhysicalOperatorType>(reader.Read<uint8_t>());
	result.work_kind = static_cast<SourceThroughputKind>(reader.Read<uint8_t>());
	return result;
}

static void WriteQueryProfile(ProfileSnapshotWriter &writer, const QueryProfileAggregate &profile) {
	writer.Write<uint64_t>(profile.template_id);
	writer.Write<uint64_t>(profile.scale_factor);
	WriteBoundedSamples(writer, profile.runtime_ns);
	WriteBoundedSamples(writer, profile.lateness_ns);
	WriteBoundedSamples(writer, profile.sla_cost);
}

static QueryProfileAggregate ReadQueryProfile(ProfileSnapshotReader &reader) {
	QueryProfileAggregate result;
	result.template_id = reader.Read<uint64_t>();
	result.scale_factor = reader.Read<uint64_t>();
	result.runtime_ns = ReadBoundedSamples(reader);
	result.lateness_ns = ReadBoundedSamples(reader);
	result.sla_cost = ReadBoundedSamples(reader);
	return result;
}

static void WritePipelineProfile(ProfileSnapshotWriter &writer, const PipelineProfileAggregate &profile) {
	writer.Write<uint64_t>(profile.template_id);
	writer.Write<uint64_t>(profile.scale_factor);
	writer.Write<uint64_t>(profile.pipeline_id);
	writer.Write<uint64_t>(profile.pipeline_signature_hash);
	writer.WriteString(profile.pipeline_signature);
	writer.WriteString(profile.operator_type_sequence);
	writer.WriteString(profile.source_type);
	writer.WriteString(profile.sink_type);
	WriteSourceWorkClass(writer, profile.source_work_class);
	writer.WriteString(profile.source_input_kind);
	writer.WriteString(profile.source_input_confidence);
	writer.WriteString(profile.planned_input_native_unit);
	WriteBoundedSamples(writer, profile.source_max_threads);
	WriteBoundedSamples(writer, profile.planned_input_rows);
	WriteBoundedSamples(writer, profile.planned_input_chunks_equiv);
	WriteBoundedSamples(writer, profile.task_count);
	WriteBoundedSamples(writer, profile.task_runtime_ns);
	WriteContinuation(writer, profile.lifecycle_runtime_ns);
	WriteContinuation(writer, profile.finish_tail_ns);
	WriteSuffix(writer, profile.downstream_suffix_ns);
	WriteBoundedSamples(writer, profile.single_worker_chunks_per_s);
	WriteContinuation(writer, profile.continuation);
	writer.Write<double>(profile.ewma_single_worker_chunks_per_s);
}

static PipelineProfileAggregate ReadPipelineProfile(ProfileSnapshotReader &reader) {
	PipelineProfileAggregate result;
	result.template_id = reader.Read<uint64_t>();
	result.scale_factor = reader.Read<uint64_t>();
	result.pipeline_id = reader.Read<uint64_t>();
	result.pipeline_signature_hash = reader.Read<uint64_t>();
	result.pipeline_signature = reader.ReadString();
	result.operator_type_sequence = reader.ReadString();
	result.source_type = reader.ReadString();
	result.sink_type = reader.ReadString();
	result.source_work_class = ReadSourceWorkClass(reader);
	result.source_input_kind = reader.ReadString();
	result.source_input_confidence = reader.ReadString();
	result.planned_input_native_unit = reader.ReadString();
	result.source_max_threads = ReadBoundedSamples(reader);
	result.planned_input_rows = ReadBoundedSamples(reader);
	result.planned_input_chunks_equiv = ReadBoundedSamples(reader);
	result.task_count = ReadBoundedSamples(reader);
	result.task_runtime_ns = ReadBoundedSamples(reader);
	result.lifecycle_runtime_ns = ReadContinuation(reader);
	result.finish_tail_ns = ReadContinuation(reader);
	result.downstream_suffix_ns = ReadSuffix(reader);
	result.single_worker_chunks_per_s = ReadBoundedSamples(reader);
	result.continuation = ReadContinuation(reader);
	result.ewma_single_worker_chunks_per_s = reader.Read<double>();
	if ((!result.single_worker_chunks_per_s.values.empty() &&
	     (!std::isfinite(result.ewma_single_worker_chunks_per_s) || result.ewma_single_worker_chunks_per_s <= 0)) ||
	    (result.single_worker_chunks_per_s.values.empty() && result.ewma_single_worker_chunks_per_s != 0)) {
		throw SerializationException("Invalid pipeline throughput EWMA in query profile snapshot");
	}
	return result;
}

static void WriteInternalEventAggregate(ProfileSnapshotWriter &writer, const InternalEventProfileAggregate &profile) {
	WriteThroughput(writer, profile.throughput);
	WriteContinuation(writer, profile.continuation);
	WriteContinuation(writer, profile.tail_to_pipeline_end);
}

static InternalEventProfileAggregate ReadInternalEventAggregate(ProfileSnapshotReader &reader) {
	InternalEventProfileAggregate result;
	result.throughput = ReadThroughput(reader);
	result.continuation = ReadContinuation(reader);
	result.tail_to_pipeline_end = ReadContinuation(reader);
	return result;
}

static ProfileSnapshotState CaptureSnapshotState(QueryRequestProfileStoreState &state) {
	ProfileSnapshotState result;
	lock_guard<std::mutex> guard(state.lock);
	result.query_profiles = state.query_profiles;
	result.pipeline_profiles = state.pipeline_profiles;
	result.scale_downstream_suffix_profiles = state.scale_downstream_suffix_profiles;
	result.global_downstream_suffix_profile = state.global_downstream_suffix_profile;
	result.source_sink_continuation_profiles = state.source_sink_continuation_profiles;
	result.source_continuation_profiles = state.source_continuation_profiles;
	result.global_raw_continuation_profile = state.global_raw_continuation_profile;
	result.source_sink_throughput_profiles = state.source_sink_throughput_profiles;
	result.source_throughput_profiles = state.source_throughput_profiles;
	result.global_compatible_throughput_profiles = state.global_compatible_throughput_profiles;
	result.internal_event_profiles = state.internal_event_profiles;
	result.scale_internal_event_profiles = state.scale_internal_event_profiles;
	result.global_internal_event_profiles = state.global_internal_event_profiles;
	result.global_finish_tail_profile = state.global_finish_tail_profile;
	return result;
}

static void WriteSnapshotState(ProfileSnapshotWriter &writer, const ProfileSnapshotState &snapshot) {
	writer.Write<uint64_t>(snapshot.query_profiles.size());
	for (const auto &entry : snapshot.query_profiles) {
		WriteQueryProfile(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.pipeline_profiles.size());
	for (const auto &entry : snapshot.pipeline_profiles) {
		WritePipelineProfile(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.scale_downstream_suffix_profiles.size());
	for (const auto &entry : snapshot.scale_downstream_suffix_profiles) {
		writer.Write<uint64_t>(entry.first);
		WriteSuffix(writer, entry.second);
	}
	WriteSuffix(writer, snapshot.global_downstream_suffix_profile);

	writer.Write<uint64_t>(snapshot.source_sink_continuation_profiles.size());
	for (const auto &entry : snapshot.source_sink_continuation_profiles) {
		WriteSourceWorkClass(writer, entry.first.source_work_class);
		writer.Write<uint8_t>(static_cast<uint8_t>(entry.first.sink_type));
		WriteContinuation(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.source_continuation_profiles.size());
	for (const auto &entry : snapshot.source_continuation_profiles) {
		WriteSourceWorkClass(writer, entry.first);
		WriteContinuation(writer, entry.second);
	}
	WriteContinuation(writer, snapshot.global_raw_continuation_profile);

	writer.Write<uint64_t>(snapshot.source_sink_throughput_profiles.size());
	for (const auto &entry : snapshot.source_sink_throughput_profiles) {
		WriteSourceWorkClass(writer, entry.first.source_work_class);
		writer.Write<uint8_t>(static_cast<uint8_t>(entry.first.sink_type));
		writer.WriteString(entry.first.native_unit);
		WriteThroughput(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.source_throughput_profiles.size());
	for (const auto &entry : snapshot.source_throughput_profiles) {
		WriteSourceWorkClass(writer, entry.first.source_work_class);
		writer.WriteString(entry.first.native_unit);
		WriteThroughput(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.global_compatible_throughput_profiles.size());
	for (const auto &entry : snapshot.global_compatible_throughput_profiles) {
		writer.Write<uint8_t>(static_cast<uint8_t>(entry.first.work_kind));
		writer.WriteString(entry.first.native_unit);
		WriteThroughput(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.internal_event_profiles.size());
	for (const auto &entry : snapshot.internal_event_profiles) {
		writer.Write<uint64_t>(entry.first.template_id);
		writer.Write<uint64_t>(entry.first.scale_factor);
		writer.Write<uint64_t>(entry.first.pipeline_id);
		writer.Write<uint64_t>(entry.first.pipeline_signature_hash);
		writer.Write<uint64_t>(entry.first.event_position);
		writer.WriteString(entry.first.event_type);
		writer.WriteString(entry.first.native_unit);
		WriteInternalEventAggregate(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.scale_internal_event_profiles.size());
	for (const auto &entry : snapshot.scale_internal_event_profiles) {
		writer.Write<uint64_t>(entry.first.scale_factor);
		writer.WriteString(entry.first.event_type);
		writer.WriteString(entry.first.native_unit);
		WriteInternalEventAggregate(writer, entry.second);
	}
	writer.Write<uint64_t>(snapshot.global_internal_event_profiles.size());
	for (const auto &entry : snapshot.global_internal_event_profiles) {
		writer.Write<uint64_t>(entry.first.scale_factor);
		writer.WriteString(entry.first.event_type);
		writer.WriteString(entry.first.native_unit);
		WriteInternalEventAggregate(writer, entry.second);
	}
	WriteContinuation(writer, snapshot.global_finish_tail_profile);
}

static ProfileSnapshotState ReadSnapshotState(ProfileSnapshotReader &reader) {
	ProfileSnapshotState result;
	auto query_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "query profile");
	for (idx_t i = 0; i < query_count; i++) {
		auto profile = ReadQueryProfile(reader);
		QueryProfileKey key;
		key.template_id = profile.template_id;
		key.scale_factor = profile.scale_factor;
		if (!result.query_profiles.emplace(key, std::move(profile)).second) {
			throw SerializationException("Duplicate query profile in query profile snapshot");
		}
	}
	auto pipeline_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "pipeline profile");
	for (idx_t i = 0; i < pipeline_count; i++) {
		auto profile = ReadPipelineProfile(reader);
		PipelineProfileKey key;
		key.template_id = profile.template_id;
		key.scale_factor = profile.scale_factor;
		key.pipeline_id = profile.pipeline_id;
		key.pipeline_signature_hash = profile.pipeline_signature_hash;
		if (!result.pipeline_profiles.emplace(key, std::move(profile)).second) {
			throw SerializationException("Duplicate pipeline profile in query profile snapshot");
		}
	}
	auto scale_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "scale suffix profile");
	for (idx_t i = 0; i < scale_count; i++) {
		auto scale_factor = reader.Read<uint64_t>();
		if (!result.scale_downstream_suffix_profiles.emplace(scale_factor, ReadSuffix(reader)).second) {
			throw SerializationException("Duplicate scale suffix profile in query profile snapshot");
		}
	}
	result.global_downstream_suffix_profile = ReadSuffix(reader);

	auto source_sink_continuation_count =
	    reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "source/sink continuation profile");
	for (idx_t i = 0; i < source_sink_continuation_count; i++) {
		SourceSinkContinuationKey key;
		key.source_work_class = ReadSourceWorkClass(reader);
		key.sink_type = static_cast<PhysicalOperatorType>(reader.Read<uint8_t>());
		if (!result.source_sink_continuation_profiles.emplace(key, ReadContinuation(reader)).second) {
			throw SerializationException("Duplicate source/sink continuation profile in query profile snapshot");
		}
	}
	auto source_continuation_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "source continuation profile");
	for (idx_t i = 0; i < source_continuation_count; i++) {
		auto key = ReadSourceWorkClass(reader);
		if (!result.source_continuation_profiles.emplace(key, ReadContinuation(reader)).second) {
			throw SerializationException("Duplicate source continuation profile in query profile snapshot");
		}
	}
	result.global_raw_continuation_profile = ReadContinuation(reader);

	auto source_sink_throughput_count =
	    reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "source/sink throughput profile");
	for (idx_t i = 0; i < source_sink_throughput_count; i++) {
		SourceSinkThroughputKey key;
		key.source_work_class = ReadSourceWorkClass(reader);
		key.sink_type = static_cast<PhysicalOperatorType>(reader.Read<uint8_t>());
		key.native_unit = reader.ReadString();
		if (!result.source_sink_throughput_profiles.emplace(key, ReadThroughput(reader)).second) {
			throw SerializationException("Duplicate source/sink throughput profile in query profile snapshot");
		}
	}
	auto source_throughput_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "source throughput profile");
	for (idx_t i = 0; i < source_throughput_count; i++) {
		SourceThroughputKey key;
		key.source_work_class = ReadSourceWorkClass(reader);
		key.native_unit = reader.ReadString();
		if (!result.source_throughput_profiles.emplace(key, ReadThroughput(reader)).second) {
			throw SerializationException("Duplicate source throughput profile in query profile snapshot");
		}
	}
	auto global_throughput_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "global throughput profile");
	for (idx_t i = 0; i < global_throughput_count; i++) {
		CompatibleThroughputKey key;
		key.work_kind = static_cast<SourceThroughputKind>(reader.Read<uint8_t>());
		key.native_unit = reader.ReadString();
		if (!result.global_compatible_throughput_profiles.emplace(key, ReadThroughput(reader)).second) {
			throw SerializationException("Duplicate global throughput profile in query profile snapshot");
		}
	}
	auto internal_event_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "internal event profile");
	for (idx_t i = 0; i < internal_event_count; i++) {
		InternalEventProfileKey key;
		key.template_id = reader.Read<uint64_t>();
		key.scale_factor = reader.Read<uint64_t>();
		key.pipeline_id = reader.Read<uint64_t>();
		key.pipeline_signature_hash = reader.Read<uint64_t>();
		key.event_position = reader.Read<uint64_t>();
		key.event_type = reader.ReadString();
		key.native_unit = reader.ReadString();
		auto profile = ReadInternalEventAggregate(reader);
		profile.key = key;
		if (!result.internal_event_profiles.emplace(key, std::move(profile)).second) {
			throw SerializationException("Duplicate internal event profile in query profile snapshot");
		}
	}
	auto scale_internal_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "scale internal event profile");
	for (idx_t i = 0; i < scale_internal_count; i++) {
		InternalEventFallbackKey key;
		key.scale_factor = reader.Read<uint64_t>();
		key.event_type = reader.ReadString();
		key.native_unit = reader.ReadString();
		if (!result.scale_internal_event_profiles.emplace(key, ReadInternalEventAggregate(reader)).second) {
			throw SerializationException("Duplicate scale internal event profile in query profile snapshot");
		}
	}
	auto global_internal_count = reader.ReadCount(PROFILE_SNAPSHOT_MAX_ENTRIES, "global internal event profile");
	for (idx_t i = 0; i < global_internal_count; i++) {
		InternalEventFallbackKey key;
		key.scale_factor = reader.Read<uint64_t>();
		key.event_type = reader.ReadString();
		key.native_unit = reader.ReadString();
		if (!result.global_internal_event_profiles.emplace(key, ReadInternalEventAggregate(reader)).second) {
			throw SerializationException("Duplicate global internal event profile in query profile snapshot");
		}
	}
	result.global_finish_tail_profile = ReadContinuation(reader);
	return result;
}

static void InstallSnapshotState(QueryRequestProfileStoreState &state, ProfileSnapshotState snapshot) {
	lock_guard<std::mutex> guard(state.lock);
	state.query_profiles = std::move(snapshot.query_profiles);
	state.pipeline_profiles = std::move(snapshot.pipeline_profiles);
	state.scale_downstream_suffix_profiles = std::move(snapshot.scale_downstream_suffix_profiles);
	state.global_downstream_suffix_profile = std::move(snapshot.global_downstream_suffix_profile);
	state.source_sink_continuation_profiles = std::move(snapshot.source_sink_continuation_profiles);
	state.source_continuation_profiles = std::move(snapshot.source_continuation_profiles);
	state.global_raw_continuation_profile = std::move(snapshot.global_raw_continuation_profile);
	state.source_sink_throughput_profiles = std::move(snapshot.source_sink_throughput_profiles);
	state.source_throughput_profiles = std::move(snapshot.source_throughput_profiles);
	state.global_compatible_throughput_profiles = std::move(snapshot.global_compatible_throughput_profiles);
	state.internal_event_profiles = std::move(snapshot.internal_event_profiles);
	state.scale_internal_event_profiles = std::move(snapshot.scale_internal_event_profiles);
	state.global_internal_event_profiles = std::move(snapshot.global_internal_event_profiles);
	state.global_finish_tail_profile = std::move(snapshot.global_finish_tail_profile);
	state.query_samples.clear();
	state.pipeline_instances.clear();
	state.internal_event_instances.clear();
}

static uint64_t DurationNs(uint64_t end_ns, uint64_t start_ns) {
	if (end_ns <= start_ns) {
		return 0;
	}
	return end_ns - start_ns;
}

static uint64_t PipelineLifecycleEndNs(const PipelineProfilingInfo &profile) {
	return profile.finish_done_ns ? profile.finish_done_ns : profile.tasks_done_ns;
}

struct DerivedSuffixObservation {
	bool valid = false;
	uint64_t suffix_ns = 0;
	idx_t completion_ordinal = 0;
	idx_t total_pipeline_count = 0;
	idx_t remaining_suffix_stages = 0;
	double normalized_suffix_ns = 0;
};

static double SingleWorkerChunksPerSecond(idx_t chunks_equiv, uint64_t worker_time_ns) {
	if (chunks_equiv == 0 || worker_time_ns == 0) {
		return 0;
	}
	return (static_cast<double>(chunks_equiv) * 1000000000.0) / static_cast<double>(worker_time_ns);
}

static double EffectiveNsPerWorkUnit(uint64_t duration_ns, idx_t chunks_equiv) {
	if (chunks_equiv == 0 || duration_ns == 0) {
		return 0;
	}
	return static_cast<double>(duration_ns) / static_cast<double>(chunks_equiv);
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
	estimate.source_work_class = SourceWorkClassToString(profile.source_work_class);
	estimate.source_input_kind = profile.source_input_kind;
	estimate.source_input_confidence = profile.source_input_confidence;
	estimate.planned_input_native_unit = profile.planned_input_native_unit;
	estimate.mean_task_runtime_ns = profile.task_runtime_ns.Mean();
	estimate.p90_task_runtime_ns = profile.task_runtime_ns.Percentile(0.9);
	estimate.mean_lifecycle_runtime_ns = profile.lifecycle_runtime_ns.values.Mean();
	estimate.mean_finish_tail_ns = profile.finish_tail_ns.values.Mean();
	estimate.p90_finish_tail_ns = profile.finish_tail_ns.p90.Quantile();
	estimate.mean_downstream_suffix_ns = profile.downstream_suffix_ns.samples.Mean();
	estimate.downstream_suffix_sample_count = profile.downstream_suffix_ns.samples.Count();
	estimate.mean_task_count = profile.task_count.Mean();
	estimate.mean_source_max_threads = profile.source_max_threads.Mean();
	estimate.mean_planned_input_rows = profile.planned_input_rows.Mean();
	estimate.mean_planned_input_chunks_equiv = profile.planned_input_chunks_equiv.Mean();
	estimate.throughput_sample_count = profile.single_worker_chunks_per_s.Count();
	estimate.mean_single_worker_chunks_per_s = profile.single_worker_chunks_per_s.Mean();
	estimate.ewma_single_worker_chunks_per_s = profile.ewma_single_worker_chunks_per_s;
	estimate.continuation_sample_count = profile.continuation.values.Count();
	estimate.mean_effective_ns_per_work_unit = profile.continuation.values.Mean();
	estimate.p50_effective_ns_per_work_unit = profile.continuation.p50.Quantile();
	estimate.p90_effective_ns_per_work_unit = profile.continuation.p90.Quantile();
}

static PipelineContinuationEstimate BuildContinuationEstimate(const ContinuationAggregate &profile,
                                                              ContinuationEstimateLevel level,
                                                              ContinuationEstimateKind kind) {
	PipelineContinuationEstimate result;
	if (profile.values.Count() == 0) {
		return result;
	}
	result.valid = true;
	result.level = level;
	result.kind = kind;
	result.sample_count = profile.values.Count();
	result.mean = profile.values.Mean();
	result.p50 = profile.p50.Quantile();
	result.p90 = profile.p90.Quantile();
	return result;
}

static PipelineThroughputEstimate BuildThroughputEstimate(const BoundedSamples &values, double ewma,
	                                                       PipelineThroughputEstimateLevel level) {
	PipelineThroughputEstimate result;
	if (values.Count() == 0 || !std::isfinite(ewma) || ewma <= 0) {
		return result;
	}
	result.valid = true;
	result.level = level;
	result.sample_count = values.Count();
	result.mean_work_units_per_s = values.Mean();
	result.ewma_work_units_per_s = ewma;
	return result;
}

static PipelineThroughputEstimate BuildThroughputEstimate(const ThroughputAggregate &profile,
	                                                       PipelineThroughputEstimateLevel level) {
	return BuildThroughputEstimate(profile.values, profile.ewma, level);
}

static PipelineLifecycleTailEstimate BuildLifecycleTailEstimate(const ContinuationAggregate &profile,
	                                                             PipelineLifecycleTailEstimateLevel level) {
	PipelineLifecycleTailEstimate result;
	if (profile.values.Count() == 0) {
		return result;
	}
	result.valid = true;
	result.level = level;
	result.sample_count = profile.values.Count();
	result.mean_ns = profile.values.Mean();
	result.p50_ns = profile.p50.Quantile();
	result.p90_ns = profile.p90.Quantile();
	return result;
}

static string ThroughputNativeUnit(const PipelineProfileIdentity &identity) {
	if (!identity.planned_input_native_unit.empty()) {
		return identity.planned_input_native_unit;
	}
	return SourceThroughputKindToString(identity.source_work_class.work_kind);
}

struct SuffixHistogramRead {
	bool needs_build = false;
	SuffixSampleWindow samples;
	DownstreamSuffixHistogram histogram;
};

static SuffixHistogramRead CaptureSuffixHistogram(const SuffixHistogramAggregate *profile) {
	SuffixHistogramRead result;
	if (!profile || profile->samples.Count() == 0) {
		return result;
	}
	if (profile->cache_valid && !profile->dirty) {
		result.histogram = profile->cached;
		return result;
	}
	result.needs_build = true;
	result.samples = profile->samples;
	return result;
}

static void BuildCapturedSuffixHistogram(SuffixHistogramRead &profile) {
	if (profile.needs_build) {
		profile.histogram = BuildSuffixHistogram(profile.samples);
	}
}

static void PublishCapturedSuffixHistogram(SuffixHistogramAggregate &target, const SuffixHistogramRead &profile) {
	if (!profile.needs_build || target.samples.generation != profile.histogram.generation) {
		return;
	}
	target.cached = profile.histogram;
	target.cache_valid = true;
	target.dirty = false;
}

static void AppendResolvedHistogram(DownstreamSuffixEstimate &estimate, const DownstreamSuffixHistogram &histogram,
	                                double weight, double suffix_stage_scale) {
	if (weight <= 0 || histogram.bucket_count == 0) {
		return;
	}
	for (idx_t i = 0; i < histogram.bucket_count; i++) {
		const auto &source = histogram.buckets[i];
		auto lower = source.lower_ns * suffix_stage_scale;
		auto upper = source.upper_ns * suffix_stage_scale;
		auto probability = source.probability * weight;
		bool merged = false;
		for (idx_t result_idx = 0; result_idx < estimate.bucket_count; result_idx++) {
			auto &existing = estimate.buckets[result_idx];
			if (existing.lower_ns == lower && existing.upper_ns == upper) {
				existing.probability += probability;
				merged = true;
				break;
			}
		}
		if (merged) {
			continue;
		}
		D_ASSERT(estimate.bucket_count < DOWNSTREAM_SUFFIX_MAX_RESOLVED_BUCKETS);
		auto &target = estimate.buckets[estimate.bucket_count++];
		target.lower_ns = lower;
		target.upper_ns = upper;
		target.probability = probability;
	}
}

static void NormalizeResolvedHistogram(DownstreamSuffixEstimate &estimate) {
	double total_probability = 0;
	for (idx_t i = 0; i < estimate.bucket_count; i++) {
		total_probability += estimate.buckets[i].probability;
	}
	if (total_probability <= 0) {
		estimate.bucket_count = 0;
		return;
	}
	for (idx_t i = 0; i < estimate.bucket_count; i++) {
		estimate.buckets[i].probability /= total_probability;
	}
	std::sort(estimate.buckets.begin(), estimate.buckets.begin() + estimate.bucket_count,
	          [](const DownstreamSuffixBucket &left, const DownstreamSuffixBucket &right) {
		          if (left.lower_ns != right.lower_ns) {
			          return left.lower_ns < right.lower_ns;
		          }
		          return left.upper_ns < right.upper_ns;
	          });
}

static DownstreamSuffixEstimate BuildDownstreamSuffixEstimate(const DownstreamSuffixHistogram &exact,
                                                              const DownstreamSuffixHistogram &scale,
                                                              const DownstreamSuffixHistogram &global,
                                                              idx_t remaining_suffix_stages) {
	DownstreamSuffixEstimate result;
	result.exact_sample_count = exact.sample_count;
	result.scale_sample_count = scale.sample_count;
	result.global_sample_count = global.sample_count;
	if (result.exact_sample_count >= DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES) {
		result.exact_weight = 1;
		result.primary_level = DownstreamSuffixProfileLevel::EXACT;
		AppendResolvedHistogram(result, exact, 1, 1);
	} else {
		result.exact_weight = static_cast<double>(result.exact_sample_count) /
		                      static_cast<double>(DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES);
		auto fallback_weight = 1.0 - result.exact_weight;
		double scale_fraction = 0;
		if (result.scale_sample_count >= DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES) {
			scale_fraction = 1;
		} else {
			scale_fraction = static_cast<double>(result.scale_sample_count) /
			                 static_cast<double>(DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES);
			if (result.global_sample_count < DOWNSTREAM_SUFFIX_MIN_EXACT_SAMPLES) {
				return DownstreamSuffixEstimate();
			}
		}
		result.scale_weight = fallback_weight * scale_fraction;
		result.global_weight = fallback_weight * (1.0 - scale_fraction);
		if (result.exact_weight > 0) {
			result.primary_level = DownstreamSuffixProfileLevel::EXACT;
		} else if (result.scale_weight > 0) {
			result.primary_level = DownstreamSuffixProfileLevel::SCALE_FACTOR;
		} else {
			result.primary_level = DownstreamSuffixProfileLevel::GLOBAL;
		}
		AppendResolvedHistogram(result, exact, result.exact_weight, 1);
		AppendResolvedHistogram(result, scale, result.scale_weight,
		                        static_cast<double>(remaining_suffix_stages));
		AppendResolvedHistogram(result, global, result.global_weight,
		                        static_cast<double>(remaining_suffix_stages));
	}
	NormalizeResolvedHistogram(result);
	result.valid = result.bucket_count > 0;
	return result;
}

struct EpochExactSuffixProfile {
	PipelineProfileKey key;
	SuffixHistogramRead read;
};

struct EpochScaleSuffixProfile {
	uint64_t scale_factor = 0;
	SuffixHistogramRead read;
};

} // namespace query_request_profile_store_internal

QueryRequestProfileStore::QueryRequestProfileStore() : state(make_uniq<QueryRequestProfileStoreState>()) {
}

QueryRequestProfileStore::~QueryRequestProfileStore() {
}

void QueryRequestProfileStore::RecordQueryCompletion(const QueryRequestMetadata &metadata, uint64_t query_end_ns,
                                                      const vector<PipelineProfilingInfo> &pipeline_profiles,
                                                      const vector<InternalEventProfilingInfo> &internal_event_profiles) {
	if (!metadata.valid || metadata.query_start_ns == 0 || query_end_ns <= metadata.query_start_ns) {
		return;
	}
	auto runtime_ns = DurationNs(query_end_ns, metadata.query_start_ns);
	auto lateness_ns = metadata.deadline_ns > 0 ? DurationNs(query_end_ns, metadata.deadline_ns) : 0;
	auto sla_cost =
	    (static_cast<double>(lateness_ns) / 1000000000.0) * static_cast<double>(metadata.sla_penalty_per_s);
	vector<DerivedSuffixObservation> suffix_observations(pipeline_profiles.size());
	vector<idx_t> completed_pipeline_indexes;
	completed_pipeline_indexes.reserve(pipeline_profiles.size());
	for (idx_t i = 0; i < pipeline_profiles.size(); i++) {
		const auto &profile = pipeline_profiles[i];
		auto lifecycle_end_ns = PipelineLifecycleEndNs(profile);
		if (profile.pipeline_id == 0 || profile.pipeline_signature_hash == 0 || lifecycle_end_ns == 0 ||
		    lifecycle_end_ns > query_end_ns) {
			continue;
		}
		completed_pipeline_indexes.push_back(i);
	}
	std::sort(completed_pipeline_indexes.begin(), completed_pipeline_indexes.end(),
	          [&](idx_t left, idx_t right) {
		          auto left_end = PipelineLifecycleEndNs(pipeline_profiles[left]);
		          auto right_end = PipelineLifecycleEndNs(pipeline_profiles[right]);
		          if (left_end != right_end) {
			          return left_end < right_end;
		          }
		          return pipeline_profiles[left].pipeline_id < pipeline_profiles[right].pipeline_id;
	          });
	auto total_pipeline_count = completed_pipeline_indexes.size();
	for (idx_t ordinal_index = 0; ordinal_index < total_pipeline_count; ordinal_index++) {
		auto profile_index = completed_pipeline_indexes[ordinal_index];
		auto &observation = suffix_observations[profile_index];
		observation.valid = true;
		observation.suffix_ns = query_end_ns - PipelineLifecycleEndNs(pipeline_profiles[profile_index]);
		observation.completion_ordinal = ordinal_index + 1;
		observation.total_pipeline_count = total_pipeline_count;
		observation.remaining_suffix_stages = total_pipeline_count - observation.completion_ordinal + 1;
		observation.normalized_suffix_ns = static_cast<double>(observation.suffix_ns) /
		                                    static_cast<double>(observation.remaining_suffix_stages);
	}

	lock_guard<std::mutex> guard(state->lock);
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
	state->query_samples.push_back(std::move(query_sample));

	QueryProfileKey query_key;
	query_key.template_id = metadata.template_id;
	query_key.scale_factor = metadata.scale_factor;
	auto &query_profile = state->query_profiles[query_key];
	query_profile.template_id = metadata.template_id;
	query_profile.scale_factor = metadata.scale_factor;
	query_profile.runtime_ns.Add(static_cast<double>(runtime_ns));
	query_profile.lateness_ns.Add(static_cast<double>(lateness_ns));
	query_profile.sla_cost.Add(sla_cost);

	for (idx_t i = 0; i < pipeline_profiles.size(); i++) {
		const auto &profile = pipeline_profiles[i];
		if (profile.pipeline_id == 0 || profile.pipeline_signature_hash == 0) {
			continue;
		}
		PipelineProfileKey pipeline_key;
		pipeline_key.template_id = metadata.template_id;
		pipeline_key.scale_factor = metadata.scale_factor;
		pipeline_key.pipeline_id = profile.pipeline_id;
		pipeline_key.pipeline_signature_hash = profile.pipeline_signature_hash;
		auto &pipeline_profile = state->pipeline_profiles[pipeline_key];
		pipeline_profile.template_id = metadata.template_id;
		pipeline_profile.scale_factor = metadata.scale_factor;
		pipeline_profile.pipeline_id = profile.pipeline_id;
		pipeline_profile.pipeline_signature_hash = profile.pipeline_signature_hash;
		pipeline_profile.pipeline_signature = profile.pipeline_signature;
		pipeline_profile.operator_type_sequence = profile.operator_type_sequence;
		pipeline_profile.source_type = profile.source_type;
		pipeline_profile.sink_type = profile.sink_type;
		pipeline_profile.source_work_class = profile.source_work_class;
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
		pipeline_instance.pipeline_signature_hash = profile.pipeline_signature_hash;
		pipeline_instance.pipeline_signature = profile.pipeline_signature;
		pipeline_instance.operator_type_sequence = profile.operator_type_sequence;
		pipeline_instance.source_type = profile.source_type;
		pipeline_instance.sink_type = profile.sink_type;
		pipeline_instance.source_work_class = SourceWorkClassToString(profile.source_work_class);
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
			if (profile.source_work_class.IsValid()) {
				auto native_unit = profile.planned_input_native_unit.empty()
				                       ? string(SourceThroughputKindToString(profile.source_work_class.work_kind))
				                       : profile.planned_input_native_unit;
				SourceSinkThroughputKey source_sink_key;
				source_sink_key.source_work_class = profile.source_work_class;
				source_sink_key.sink_type = profile.sink_operator_type;
				source_sink_key.native_unit = native_unit;
				state->source_sink_throughput_profiles[source_sink_key].Add(single_worker_chunks_per_s);
				SourceThroughputKey source_key;
				source_key.source_work_class = profile.source_work_class;
				source_key.native_unit = native_unit;
				state->source_throughput_profiles[source_key].Add(single_worker_chunks_per_s);
				CompatibleThroughputKey global_key;
				global_key.work_kind = profile.source_work_class.work_kind;
				global_key.native_unit = native_unit;
				state->global_compatible_throughput_profiles[global_key].Add(single_worker_chunks_per_s);
			}
		}
		pipeline_instance.source_estimated_cardinality = profile.source_estimated_cardinality;
		pipeline_instance.sink_estimated_cardinality = profile.sink_estimated_cardinality;
		pipeline_instance.start_ns = profile.start_ns;
		pipeline_instance.tasks_done_ns = profile.tasks_done_ns;
		pipeline_instance.finish_done_ns = profile.finish_done_ns;
		if (profile.start_ns > 0 && profile.tasks_done_ns >= profile.start_ns) {
			auto task_runtime_ns = profile.tasks_done_ns - profile.start_ns;
			pipeline_profile.task_runtime_ns.Add(static_cast<double>(task_runtime_ns));
			pipeline_instance.task_runtime_ns = task_runtime_ns;
		}
		if (profile.tasks_done_ns > 0 && profile.finish_done_ns >= profile.tasks_done_ns) {
			auto finish_tail_ns = profile.finish_done_ns - profile.tasks_done_ns;
			pipeline_profile.finish_tail_ns.Add(static_cast<double>(finish_tail_ns));
			state->global_finish_tail_profile.Add(static_cast<double>(finish_tail_ns));
			pipeline_instance.finish_tail_ns = finish_tail_ns;
		}
		auto lifecycle_end_ns = PipelineLifecycleEndNs(profile);
		if (profile.start_ns > 0 && lifecycle_end_ns >= profile.start_ns) {
			auto lifecycle_runtime_ns = lifecycle_end_ns - profile.start_ns;
			pipeline_profile.lifecycle_runtime_ns.Add(static_cast<double>(lifecycle_runtime_ns));
			state->global_raw_continuation_profile.Add(static_cast<double>(lifecycle_runtime_ns));
			pipeline_instance.lifecycle_runtime_ns = lifecycle_runtime_ns;
			auto effective_ns_per_work_unit =
			    EffectiveNsPerWorkUnit(lifecycle_runtime_ns, profile.planned_input_chunks_equiv);
			if (effective_ns_per_work_unit > 0) {
				pipeline_profile.continuation.Add(effective_ns_per_work_unit, profile.planned_input_native_unit);
				if (profile.source_work_class.IsValid()) {
					SourceSinkContinuationKey source_sink_key;
					source_sink_key.source_work_class = profile.source_work_class;
					source_sink_key.sink_type = profile.sink_operator_type;
					state->source_sink_continuation_profiles[source_sink_key].Add(
					    effective_ns_per_work_unit, profile.planned_input_native_unit);
					state->source_continuation_profiles[profile.source_work_class].Add(
					    effective_ns_per_work_unit, profile.planned_input_native_unit);
				}
				pipeline_instance.effective_ns_per_work_unit = effective_ns_per_work_unit;
				pipeline_instance.continuation_valid = true;
			}
		}
		const auto &suffix_observation = suffix_observations[i];
		if (suffix_observation.valid) {
			pipeline_profile.downstream_suffix_ns.Add(static_cast<double>(suffix_observation.suffix_ns));
			state->scale_downstream_suffix_profiles[metadata.scale_factor].Add(
			    suffix_observation.normalized_suffix_ns);
			state->global_downstream_suffix_profile.Add(suffix_observation.normalized_suffix_ns);
			pipeline_instance.downstream_suffix_ns = suffix_observation.suffix_ns;
			pipeline_instance.pipeline_completion_ordinal = suffix_observation.completion_ordinal;
			pipeline_instance.total_pipeline_count = suffix_observation.total_pipeline_count;
			pipeline_instance.remaining_suffix_stages = suffix_observation.remaining_suffix_stages;
			pipeline_instance.normalized_downstream_suffix_ns = suffix_observation.normalized_suffix_ns;
		}
		state->pipeline_instances.push_back(std::move(pipeline_instance));
	}

	for (const auto &event : internal_event_profiles) {
		if (event.pipeline_id == 0 || event.pipeline_signature_hash == 0 || event.event_type.empty() ||
		    event.native_unit.empty() || event.total_work_units == 0 || event.finish_ns <= event.start_ns) {
			continue;
		}
		uint64_t pipeline_end_ns = 0;
		for (const auto &pipeline : pipeline_profiles) {
			if (pipeline.pipeline_id == event.pipeline_id &&
			    pipeline.pipeline_signature_hash == event.pipeline_signature_hash) {
				pipeline_end_ns = PipelineLifecycleEndNs(pipeline);
				break;
			}
		}
		InternalEventProfileKey key;
		key.template_id = metadata.template_id;
		key.scale_factor = metadata.scale_factor;
		key.pipeline_id = event.pipeline_id;
		key.pipeline_signature_hash = event.pipeline_signature_hash;
		key.event_position = event.event_position;
		key.event_type = event.event_type;
		key.native_unit = event.native_unit;
		auto &profile = state->internal_event_profiles[key];
		profile.key = key;
		InternalEventFallbackKey scale_key;
		scale_key.scale_factor = metadata.scale_factor;
		scale_key.event_type = event.event_type;
		scale_key.native_unit = event.native_unit;
		InternalEventFallbackKey global_key;
		global_key.event_type = event.event_type;
		global_key.native_unit = event.native_unit;
		auto &scale_profile = state->scale_internal_event_profiles[scale_key];
		auto &global_profile = state->global_internal_event_profiles[global_key];

		double throughput = 0;
		if (event.completed_work_units > 0 && event.worker_time_ns > 0) {
			throughput = static_cast<double>(event.completed_work_units) * 1000000000.0 /
			             static_cast<double>(event.worker_time_ns);
			profile.throughput.Add(throughput);
			scale_profile.throughput.Add(throughput);
			global_profile.throughput.Add(throughput);
		}
		auto wall_ns = event.finish_ns - event.start_ns;
		auto ns_per_unit = static_cast<double>(wall_ns) / static_cast<double>(event.total_work_units);
		profile.continuation.Add(ns_per_unit, event.native_unit);
		scale_profile.continuation.Add(ns_per_unit, event.native_unit);
		global_profile.continuation.Add(ns_per_unit, event.native_unit);
		uint64_t tail_ns = 0;
		if (pipeline_end_ns >= event.finish_ns) {
			tail_ns = pipeline_end_ns - event.finish_ns;
			profile.tail_to_pipeline_end.Add(static_cast<double>(tail_ns));
			scale_profile.tail_to_pipeline_end.Add(static_cast<double>(tail_ns));
			global_profile.tail_to_pipeline_end.Add(static_cast<double>(tail_ns));
		}
		QueryRequestInternalEventInstanceSnapshot instance;
		static_cast<InternalEventProfilingInfo &>(instance) = event;
		instance.db_query_id = metadata.db_query_id;
		instance.request_id = metadata.request_id;
		instance.template_id = metadata.template_id;
		instance.scale_factor = metadata.scale_factor;
		instance.work_units_per_s = throughput;
		instance.ns_per_work_unit = ns_per_unit;
		instance.tail_to_pipeline_end_ns = tail_ns;
		state->internal_event_instances.push_back(std::move(instance));
	}
}

bool QueryRequestProfileStore::TryGetQueryEstimate(uint64_t template_id, uint64_t scale_factor,
                                                   QueryRequestProfileEstimate &estimate) const {
	lock_guard<std::mutex> guard(state->lock);
	QueryProfileKey key;
	key.template_id = template_id;
	key.scale_factor = scale_factor;
	auto entry = state->query_profiles.find(key);
	if (entry == state->query_profiles.end()) {
		return false;
	}
	PopulateQueryEstimate(entry->second, estimate);
	return true;
}

bool QueryRequestProfileStore::TryGetPipelineEstimate(uint64_t template_id, uint64_t scale_factor, idx_t pipeline_id,
                                                      uint64_t pipeline_signature_hash,
                                                      QueryRequestPipelineProfileEstimate &estimate) const {
	lock_guard<std::mutex> guard(state->lock);
	PipelineProfileKey key;
	key.template_id = template_id;
	key.scale_factor = scale_factor;
	key.pipeline_id = pipeline_id;
	key.pipeline_signature_hash = pipeline_signature_hash;
	auto entry = state->pipeline_profiles.find(key);
	if (entry == state->pipeline_profiles.end()) {
		return false;
	}
	PopulatePipelineEstimate(entry->second, estimate);
	return true;
}

PipelineContinuationEstimate QueryRequestProfileStore::ResolvePipelineContinuation(
    uint64_t template_id, uint64_t scale_factor, const PipelineProfileIdentity &identity,
    idx_t planned_work_units) const {
	lock_guard<std::mutex> guard(state->lock);
	if (identity.valid) {
		PipelineProfileKey exact_key;
		exact_key.template_id = template_id;
		exact_key.scale_factor = scale_factor;
		exact_key.pipeline_id = identity.pipeline_id;
		exact_key.pipeline_signature_hash = identity.pipeline_signature_hash;
		auto exact_entry = state->pipeline_profiles.find(exact_key);
		if (exact_entry != state->pipeline_profiles.end()) {
			if (planned_work_units > 0 && exact_entry->second.continuation.values.Count() > 0) {
				return BuildContinuationEstimate(exact_entry->second.continuation, ContinuationEstimateLevel::EXACT,
				                                 ContinuationEstimateKind::NS_PER_WORK_UNIT);
			}
			auto raw_exact = BuildContinuationEstimate(exact_entry->second.lifecycle_runtime_ns,
			                                             ContinuationEstimateLevel::EXACT,
			                                             ContinuationEstimateKind::RAW_LATENCY_NS);
			if (raw_exact.valid) {
				return raw_exact;
			}
		}
	}
	if (planned_work_units > 0 && identity.source_work_class.IsValid()) {
		SourceSinkContinuationKey source_sink_key;
		source_sink_key.source_work_class = identity.source_work_class;
		source_sink_key.sink_type = identity.sink_type;
		auto source_sink_entry = state->source_sink_continuation_profiles.find(source_sink_key);
		if (source_sink_entry != state->source_sink_continuation_profiles.end() &&
		    (identity.planned_input_native_unit.empty() || source_sink_entry->second.native_unit.empty() ||
		     identity.planned_input_native_unit == source_sink_entry->second.native_unit)) {
			return BuildContinuationEstimate(source_sink_entry->second, ContinuationEstimateLevel::SOURCE_SINK,
			                                 ContinuationEstimateKind::NS_PER_WORK_UNIT);
		}
		auto source_entry = state->source_continuation_profiles.find(identity.source_work_class);
		if (source_entry != state->source_continuation_profiles.end() &&
		    (identity.planned_input_native_unit.empty() || source_entry->second.native_unit.empty() ||
		     identity.planned_input_native_unit == source_entry->second.native_unit)) {
			return BuildContinuationEstimate(source_entry->second, ContinuationEstimateLevel::SOURCE,
			                                 ContinuationEstimateKind::NS_PER_WORK_UNIT);
		}
	}
	return BuildContinuationEstimate(state->global_raw_continuation_profile, ContinuationEstimateLevel::GLOBAL_RAW,
	                                 ContinuationEstimateKind::RAW_LATENCY_NS);
}

PipelineThroughputEstimate QueryRequestProfileStore::ResolvePipelineThroughput(
    uint64_t template_id, uint64_t scale_factor, const PipelineProfileIdentity &identity) const {
	lock_guard<std::mutex> guard(state->lock);
	if (identity.valid) {
		PipelineProfileKey exact_key;
		exact_key.template_id = template_id;
		exact_key.scale_factor = scale_factor;
		exact_key.pipeline_id = identity.pipeline_id;
		exact_key.pipeline_signature_hash = identity.pipeline_signature_hash;
		auto exact_entry = state->pipeline_profiles.find(exact_key);
		if (exact_entry != state->pipeline_profiles.end()) {
			auto exact = BuildThroughputEstimate(exact_entry->second.single_worker_chunks_per_s,
			                                     exact_entry->second.ewma_single_worker_chunks_per_s,
			                                     PipelineThroughputEstimateLevel::EXACT);
			if (exact.valid) {
				return exact;
			}
		}
	}
	if (!identity.source_work_class.IsValid()) {
		return PipelineThroughputEstimate();
	}
	auto native_unit = ThroughputNativeUnit(identity);
	SourceSinkThroughputKey source_sink_key;
	source_sink_key.source_work_class = identity.source_work_class;
	source_sink_key.sink_type = identity.sink_type;
	source_sink_key.native_unit = native_unit;
	auto source_sink_entry = state->source_sink_throughput_profiles.find(source_sink_key);
	if (source_sink_entry != state->source_sink_throughput_profiles.end()) {
		return BuildThroughputEstimate(source_sink_entry->second, PipelineThroughputEstimateLevel::SOURCE_SINK);
	}
	SourceThroughputKey source_key;
	source_key.source_work_class = identity.source_work_class;
	source_key.native_unit = native_unit;
	auto source_entry = state->source_throughput_profiles.find(source_key);
	if (source_entry != state->source_throughput_profiles.end()) {
		return BuildThroughputEstimate(source_entry->second, PipelineThroughputEstimateLevel::SOURCE);
	}
	CompatibleThroughputKey global_key;
	global_key.work_kind = identity.source_work_class.work_kind;
	global_key.native_unit = native_unit;
	auto global_entry = state->global_compatible_throughput_profiles.find(global_key);
	if (global_entry != state->global_compatible_throughput_profiles.end()) {
		return BuildThroughputEstimate(global_entry->second,
		                               PipelineThroughputEstimateLevel::GLOBAL_COMPATIBLE);
	}
	return PipelineThroughputEstimate();
}

PipelineLifecycleTailEstimate QueryRequestProfileStore::ResolvePipelineLifecycleTail(
    uint64_t template_id, uint64_t scale_factor, const PipelineProfileIdentity &identity) const {
	lock_guard<std::mutex> guard(state->lock);
	if (identity.valid) {
		PipelineProfileKey exact_key;
		exact_key.template_id = template_id;
		exact_key.scale_factor = scale_factor;
		exact_key.pipeline_id = identity.pipeline_id;
		exact_key.pipeline_signature_hash = identity.pipeline_signature_hash;
		auto exact_entry = state->pipeline_profiles.find(exact_key);
		if (exact_entry != state->pipeline_profiles.end()) {
			auto exact = BuildLifecycleTailEstimate(exact_entry->second.finish_tail_ns,
			                                        PipelineLifecycleTailEstimateLevel::EXACT);
			if (exact.valid) {
				return exact;
			}
		}
	}
	return BuildLifecycleTailEstimate(state->global_finish_tail_profile,
	                                  PipelineLifecycleTailEstimateLevel::GLOBAL);
}

InternalEventModelEstimate QueryRequestProfileStore::ResolveInternalEvent(
    uint64_t template_id, uint64_t scale_factor, const PipelineProfileIdentity &pipeline_identity,
    idx_t event_position, const string &event_type, const string &native_unit) const {
	InternalEventModelEstimate result;
	if (!pipeline_identity.valid || event_type.empty() || native_unit.empty()) {
		return result;
	}
	lock_guard<std::mutex> guard(state->lock);
	auto apply_profile = [&](const InternalEventProfileAggregate &profile, PipelineThroughputEstimateLevel throughput_level,
	                         ContinuationEstimateLevel continuation_level,
	                         PipelineLifecycleTailEstimateLevel tail_level) {
		if (!result.throughput.valid) {
			result.throughput = BuildThroughputEstimate(profile.throughput, throughput_level);
		}
		if (!result.continuation.valid) {
			result.continuation = BuildContinuationEstimate(profile.continuation, continuation_level,
			                                                ContinuationEstimateKind::NS_PER_WORK_UNIT);
		}
		if (!result.tail_to_pipeline_end.valid) {
			result.tail_to_pipeline_end = BuildLifecycleTailEstimate(profile.tail_to_pipeline_end, tail_level);
		}
	};

	InternalEventProfileKey exact_key;
	exact_key.template_id = template_id;
	exact_key.scale_factor = scale_factor;
	exact_key.pipeline_id = pipeline_identity.pipeline_id;
	exact_key.pipeline_signature_hash = pipeline_identity.pipeline_signature_hash;
	exact_key.event_position = event_position;
	exact_key.event_type = event_type;
	exact_key.native_unit = native_unit;
	auto exact = state->internal_event_profiles.find(exact_key);
	if (exact != state->internal_event_profiles.end()) {
		apply_profile(exact->second, PipelineThroughputEstimateLevel::EXACT, ContinuationEstimateLevel::EXACT,
		              PipelineLifecycleTailEstimateLevel::EXACT);
	}

	InternalEventFallbackKey scale_key;
	scale_key.scale_factor = scale_factor;
	scale_key.event_type = event_type;
	scale_key.native_unit = native_unit;
	auto scale = state->scale_internal_event_profiles.find(scale_key);
	if (scale != state->scale_internal_event_profiles.end()) {
		apply_profile(scale->second, PipelineThroughputEstimateLevel::SCALE_FACTOR,
		              ContinuationEstimateLevel::SCALE_FACTOR, PipelineLifecycleTailEstimateLevel::SCALE_FACTOR);
	}
	InternalEventFallbackKey global_key;
	global_key.event_type = event_type;
	global_key.native_unit = native_unit;
	auto global = state->global_internal_event_profiles.find(global_key);
	if (global != state->global_internal_event_profiles.end()) {
		apply_profile(global->second, PipelineThroughputEstimateLevel::GLOBAL_COMPATIBLE,
		              ContinuationEstimateLevel::GLOBAL_RAW, PipelineLifecycleTailEstimateLevel::GLOBAL);
	}
	return result;
}

vector<DownstreamSuffixEstimate> QueryRequestProfileStore::PrepareDownstreamSuffixEpoch(
    const vector<DownstreamSuffixEpochRequest> &requests) const {
	vector<DownstreamSuffixEstimate> results(requests.size());
	if (requests.empty()) {
		return results;
	}

	vector<EpochExactSuffixProfile> exact_profiles;
	vector<EpochScaleSuffixProfile> scale_profiles;
	vector<idx_t> request_exact_indexes(requests.size(), 0);
	vector<idx_t> request_scale_indexes(requests.size(), 0);
	vector<bool> valid_requests(requests.size(), false);
	std::unordered_map<PipelineProfileKey, idx_t, PipelineProfileKeyHash> exact_indexes;
	std::unordered_map<uint64_t, idx_t> scale_indexes;
	exact_indexes.reserve(requests.size());
	scale_indexes.reserve(requests.size());
	for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
		const auto &request = requests[request_idx];
		if (request.pipeline_id == 0 || request.pipeline_signature_hash == 0 ||
		    request.remaining_suffix_stages == 0) {
			continue;
		}
		valid_requests[request_idx] = true;
		PipelineProfileKey exact_key;
		exact_key.template_id = request.template_id;
		exact_key.scale_factor = request.scale_factor;
		exact_key.pipeline_id = request.pipeline_id;
		exact_key.pipeline_signature_hash = request.pipeline_signature_hash;
		auto exact_insert = exact_indexes.emplace(exact_key, exact_profiles.size());
		if (exact_insert.second) {
			EpochExactSuffixProfile profile;
			profile.key = exact_key;
			exact_profiles.push_back(std::move(profile));
		}
		request_exact_indexes[request_idx] = exact_insert.first->second;

		auto scale_insert = scale_indexes.emplace(request.scale_factor, scale_profiles.size());
		if (scale_insert.second) {
			EpochScaleSuffixProfile profile;
			profile.scale_factor = request.scale_factor;
			scale_profiles.push_back(std::move(profile));
		}
		request_scale_indexes[request_idx] = scale_insert.first->second;
	}
	if (exact_profiles.empty()) {
		return results;
	}

	SuffixHistogramRead global;
	{
		lock_guard<std::mutex> guard(state->lock);
		for (auto &profile : exact_profiles) {
			auto entry = state->pipeline_profiles.find(profile.key);
			profile.read = CaptureSuffixHistogram(entry == state->pipeline_profiles.end()
			                                          ? nullptr
			                                          : &entry->second.downstream_suffix_ns);
		}
		for (auto &profile : scale_profiles) {
			auto entry = state->scale_downstream_suffix_profiles.find(profile.scale_factor);
			profile.read = CaptureSuffixHistogram(entry == state->scale_downstream_suffix_profiles.end()
			                                          ? nullptr
			                                          : &entry->second);
		}
		global = CaptureSuffixHistogram(&state->global_downstream_suffix_profile);
	}

	bool has_rebuilt_histogram = global.needs_build;
	BuildCapturedSuffixHistogram(global);
	for (auto &profile : exact_profiles) {
		has_rebuilt_histogram = has_rebuilt_histogram || profile.read.needs_build;
		BuildCapturedSuffixHistogram(profile.read);
	}
	for (auto &profile : scale_profiles) {
		has_rebuilt_histogram = has_rebuilt_histogram || profile.read.needs_build;
		BuildCapturedSuffixHistogram(profile.read);
	}

	// Every result uses the same captured generations, even if a completion appends samples concurrently.
	for (idx_t request_idx = 0; request_idx < requests.size(); request_idx++) {
		if (!valid_requests[request_idx]) {
			continue;
		}
		const auto &exact = exact_profiles[request_exact_indexes[request_idx]].read.histogram;
		const auto &scale = scale_profiles[request_scale_indexes[request_idx]].read.histogram;
		results[request_idx] = BuildDownstreamSuffixEstimate(exact, scale, global.histogram,
		                                                           requests[request_idx].remaining_suffix_stages);
	}

	if (has_rebuilt_histogram) {
		lock_guard<std::mutex> guard(state->lock);
		for (const auto &profile : exact_profiles) {
			auto entry = state->pipeline_profiles.find(profile.key);
			if (entry != state->pipeline_profiles.end()) {
				PublishCapturedSuffixHistogram(entry->second.downstream_suffix_ns, profile.read);
			}
		}
		for (const auto &profile : scale_profiles) {
			auto entry = state->scale_downstream_suffix_profiles.find(profile.scale_factor);
			if (entry != state->scale_downstream_suffix_profiles.end()) {
				PublishCapturedSuffixHistogram(entry->second, profile.read);
			}
		}
		PublishCapturedSuffixHistogram(state->global_downstream_suffix_profile, global);
	}
	return results;
}

vector<QueryRequestProfileSnapshot> QueryRequestProfileStore::GetQueryProfilesSnapshot() const {
	lock_guard<std::mutex> guard(state->lock);
	vector<QueryRequestProfileSnapshot> result;
	result.reserve(state->query_profiles.size());
	for (const auto &entry : state->query_profiles) {
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
	lock_guard<std::mutex> guard(state->lock);
	vector<QueryRequestPipelineProfileSnapshot> result;
	result.reserve(state->pipeline_profiles.size());
	for (const auto &entry : state->pipeline_profiles) {
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
	lock_guard<std::mutex> guard(state->lock);
	auto result = state->query_samples;
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
	lock_guard<std::mutex> guard(state->lock);
	auto result = state->pipeline_instances;
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

vector<QueryRequestContinuationProfileSnapshot> QueryRequestProfileStore::GetContinuationProfilesSnapshot() const {
	lock_guard<std::mutex> guard(state->lock);
	vector<QueryRequestContinuationProfileSnapshot> result;
	result.reserve(state->source_sink_continuation_profiles.size() + state->source_continuation_profiles.size() + 1);
	for (const auto &entry : state->source_sink_continuation_profiles) {
		QueryRequestContinuationProfileSnapshot snapshot;
		snapshot.level = ContinuationEstimateLevel::SOURCE_SINK;
		snapshot.source_work_class = entry.first.source_work_class;
		snapshot.sink_type = entry.first.sink_type;
		snapshot.kind = ContinuationEstimateKind::NS_PER_WORK_UNIT;
		snapshot.sample_count = entry.second.values.Count();
		snapshot.mean = entry.second.values.Mean();
		snapshot.p50 = entry.second.p50.Quantile();
		snapshot.p90 = entry.second.p90.Quantile();
		snapshot.native_unit_mismatch_count = entry.second.native_unit_mismatch_count;
		result.push_back(std::move(snapshot));
	}
	for (const auto &entry : state->source_continuation_profiles) {
		QueryRequestContinuationProfileSnapshot snapshot;
		snapshot.level = ContinuationEstimateLevel::SOURCE;
		snapshot.source_work_class = entry.first;
		snapshot.kind = ContinuationEstimateKind::NS_PER_WORK_UNIT;
		snapshot.sample_count = entry.second.values.Count();
		snapshot.mean = entry.second.values.Mean();
		snapshot.p50 = entry.second.p50.Quantile();
		snapshot.p90 = entry.second.p90.Quantile();
		snapshot.native_unit_mismatch_count = entry.second.native_unit_mismatch_count;
		result.push_back(std::move(snapshot));
	}
	if (state->global_raw_continuation_profile.values.Count() > 0) {
		QueryRequestContinuationProfileSnapshot snapshot;
		snapshot.level = ContinuationEstimateLevel::GLOBAL_RAW;
		snapshot.kind = ContinuationEstimateKind::RAW_LATENCY_NS;
		snapshot.sample_count = state->global_raw_continuation_profile.values.Count();
		snapshot.mean = state->global_raw_continuation_profile.values.Mean();
		snapshot.p50 = state->global_raw_continuation_profile.p50.Quantile();
		snapshot.p90 = state->global_raw_continuation_profile.p90.Quantile();
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestContinuationProfileSnapshot &left,
	                                           const QueryRequestContinuationProfileSnapshot &right) {
		if (left.level != right.level) {
			return static_cast<uint8_t>(left.level) < static_cast<uint8_t>(right.level);
		}
		auto left_class = SourceWorkClassToString(left.source_work_class);
		auto right_class = SourceWorkClassToString(right.source_work_class);
		if (left_class != right_class) {
			return left_class < right_class;
		}
		return static_cast<uint8_t>(left.sink_type) < static_cast<uint8_t>(right.sink_type);
	});
	return result;
}

vector<QueryRequestThroughputProfileSnapshot> QueryRequestProfileStore::GetThroughputProfilesSnapshot() const {
	lock_guard<std::mutex> guard(state->lock);
	vector<QueryRequestThroughputProfileSnapshot> result;
	result.reserve(state->source_sink_throughput_profiles.size() + state->source_throughput_profiles.size() +
	               state->global_compatible_throughput_profiles.size());
	for (const auto &entry : state->source_sink_throughput_profiles) {
		QueryRequestThroughputProfileSnapshot snapshot;
		snapshot.level = PipelineThroughputEstimateLevel::SOURCE_SINK;
		snapshot.source_work_class = entry.first.source_work_class;
		snapshot.sink_type = entry.first.sink_type;
		snapshot.work_kind = entry.first.source_work_class.work_kind;
		snapshot.native_unit = entry.first.native_unit;
		snapshot.sample_count = entry.second.values.Count();
		snapshot.mean_work_units_per_s = entry.second.values.Mean();
		snapshot.ewma_work_units_per_s = entry.second.ewma;
		result.push_back(std::move(snapshot));
	}
	for (const auto &entry : state->source_throughput_profiles) {
		QueryRequestThroughputProfileSnapshot snapshot;
		snapshot.level = PipelineThroughputEstimateLevel::SOURCE;
		snapshot.source_work_class = entry.first.source_work_class;
		snapshot.work_kind = entry.first.source_work_class.work_kind;
		snapshot.native_unit = entry.first.native_unit;
		snapshot.sample_count = entry.second.values.Count();
		snapshot.mean_work_units_per_s = entry.second.values.Mean();
		snapshot.ewma_work_units_per_s = entry.second.ewma;
		result.push_back(std::move(snapshot));
	}
	for (const auto &entry : state->global_compatible_throughput_profiles) {
		QueryRequestThroughputProfileSnapshot snapshot;
		snapshot.level = PipelineThroughputEstimateLevel::GLOBAL_COMPATIBLE;
		snapshot.work_kind = entry.first.work_kind;
		snapshot.native_unit = entry.first.native_unit;
		snapshot.sample_count = entry.second.values.Count();
		snapshot.mean_work_units_per_s = entry.second.values.Mean();
		snapshot.ewma_work_units_per_s = entry.second.ewma;
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestThroughputProfileSnapshot &left,
	                                           const QueryRequestThroughputProfileSnapshot &right) {
		if (left.level != right.level) {
			return static_cast<uint8_t>(left.level) < static_cast<uint8_t>(right.level);
		}
		auto left_class = SourceWorkClassToString(left.source_work_class);
		auto right_class = SourceWorkClassToString(right.source_work_class);
		if (left_class != right_class) {
			return left_class < right_class;
		}
		if (left.sink_type != right.sink_type) {
			return static_cast<uint8_t>(left.sink_type) < static_cast<uint8_t>(right.sink_type);
		}
		if (left.work_kind != right.work_kind) {
			return static_cast<uint8_t>(left.work_kind) < static_cast<uint8_t>(right.work_kind);
		}
		return left.native_unit < right.native_unit;
	});
	return result;
}

vector<QueryRequestDownstreamSuffixProfileSnapshot>
QueryRequestProfileStore::GetDownstreamSuffixProfilesSnapshot() const {
	struct PendingSnapshot {
		QueryRequestDownstreamSuffixProfileSnapshot snapshot;
		SuffixSampleWindow samples;
		bool needs_build = false;
	};
	vector<PendingSnapshot> pending;
	{
		lock_guard<std::mutex> guard(state->lock);
		pending.reserve(state->pipeline_profiles.size() + state->scale_downstream_suffix_profiles.size() + 1);
		for (const auto &entry : state->pipeline_profiles) {
			const auto &profile = entry.second.downstream_suffix_ns;
			if (profile.samples.Count() == 0) {
				continue;
			}
			PendingSnapshot item;
			item.snapshot.level = DownstreamSuffixProfileLevel::EXACT;
			item.snapshot.template_id = entry.second.template_id;
			item.snapshot.scale_factor = entry.second.scale_factor;
			item.snapshot.pipeline_id = entry.second.pipeline_id;
			item.snapshot.pipeline_signature_hash = entry.second.pipeline_signature_hash;
			item.snapshot.value_unit = "ns";
			if (profile.cache_valid && !profile.dirty) {
				item.snapshot.histogram = profile.cached;
			} else {
				item.samples = profile.samples;
				item.needs_build = true;
			}
			pending.push_back(std::move(item));
		}
		for (const auto &entry : state->scale_downstream_suffix_profiles) {
			if (entry.second.samples.Count() == 0) {
				continue;
			}
			PendingSnapshot item;
			item.snapshot.level = DownstreamSuffixProfileLevel::SCALE_FACTOR;
			item.snapshot.scale_factor = entry.first;
			item.snapshot.value_unit = "ns_per_remaining_stage";
			if (entry.second.cache_valid && !entry.second.dirty) {
				item.snapshot.histogram = entry.second.cached;
			} else {
				item.samples = entry.second.samples;
				item.needs_build = true;
			}
			pending.push_back(std::move(item));
		}
		if (state->global_downstream_suffix_profile.samples.Count() > 0) {
			PendingSnapshot item;
			item.snapshot.level = DownstreamSuffixProfileLevel::GLOBAL;
			item.snapshot.value_unit = "ns_per_remaining_stage";
			if (state->global_downstream_suffix_profile.cache_valid && !state->global_downstream_suffix_profile.dirty) {
				item.snapshot.histogram = state->global_downstream_suffix_profile.cached;
			} else {
				item.samples = state->global_downstream_suffix_profile.samples;
				item.needs_build = true;
			}
			pending.push_back(std::move(item));
		}
	}
	vector<QueryRequestDownstreamSuffixProfileSnapshot> result;
	result.reserve(pending.size());
	for (auto &item : pending) {
		if (item.needs_build) {
			item.snapshot.histogram = BuildSuffixHistogram(item.samples);
		}
		result.push_back(std::move(item.snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestDownstreamSuffixProfileSnapshot &left,
	                                           const QueryRequestDownstreamSuffixProfileSnapshot &right) {
		if (left.level != right.level) {
			return static_cast<uint8_t>(left.level) < static_cast<uint8_t>(right.level);
		}
		if (left.template_id != right.template_id) {
			return left.template_id < right.template_id;
		}
		if (left.scale_factor != right.scale_factor) {
			return left.scale_factor < right.scale_factor;
		}
		if (left.pipeline_id != right.pipeline_id) {
			return left.pipeline_id < right.pipeline_id;
		}
		return left.pipeline_signature_hash < right.pipeline_signature_hash;
	});
	return result;
}

vector<QueryRequestInternalEventProfileSnapshot> QueryRequestProfileStore::GetInternalEventProfilesSnapshot() const {
	lock_guard<std::mutex> guard(state->lock);
	vector<QueryRequestInternalEventProfileSnapshot> result;
	result.reserve(state->internal_event_profiles.size());
	for (const auto &entry : state->internal_event_profiles) {
		QueryRequestInternalEventProfileSnapshot snapshot;
		snapshot.template_id = entry.first.template_id;
		snapshot.scale_factor = entry.first.scale_factor;
		snapshot.pipeline_id = entry.first.pipeline_id;
		snapshot.pipeline_signature_hash = entry.first.pipeline_signature_hash;
		snapshot.event_position = entry.first.event_position;
		snapshot.event_type = entry.first.event_type;
		snapshot.native_unit = entry.first.native_unit;
		snapshot.sample_count = entry.second.continuation.values.Count();
		snapshot.mean_work_units_per_s = entry.second.throughput.values.Mean();
		snapshot.ewma_work_units_per_s = entry.second.throughput.ewma;
		snapshot.mean_ns_per_work_unit = entry.second.continuation.values.Mean();
		snapshot.p50_ns_per_work_unit = entry.second.continuation.p50.Quantile();
		snapshot.p90_ns_per_work_unit = entry.second.continuation.p90.Quantile();
		snapshot.mean_tail_ns = entry.second.tail_to_pipeline_end.values.Mean();
		snapshot.p90_tail_ns = entry.second.tail_to_pipeline_end.p90.Quantile();
		result.push_back(std::move(snapshot));
	}
	std::sort(result.begin(), result.end(), [](const QueryRequestInternalEventProfileSnapshot &left,
	                                           const QueryRequestInternalEventProfileSnapshot &right) {
		if (left.template_id != right.template_id) {
			return left.template_id < right.template_id;
		}
		if (left.scale_factor != right.scale_factor) {
			return left.scale_factor < right.scale_factor;
		}
		if (left.pipeline_id != right.pipeline_id) {
			return left.pipeline_id < right.pipeline_id;
		}
		if (left.event_position != right.event_position) {
			return left.event_position < right.event_position;
		}
		return left.event_type < right.event_type;
	});
	return result;
}

vector<QueryRequestInternalEventInstanceSnapshot> QueryRequestProfileStore::GetInternalEventInstancesSnapshot() const {
	lock_guard<std::mutex> guard(state->lock);
	auto result = state->internal_event_instances;
	std::sort(result.begin(), result.end(), [](const QueryRequestInternalEventInstanceSnapshot &left,
	                                           const QueryRequestInternalEventInstanceSnapshot &right) {
		if (left.db_query_id != right.db_query_id) {
			return left.db_query_id < right.db_query_id;
		}
		if (left.pipeline_id != right.pipeline_id) {
			return left.pipeline_id < right.pipeline_id;
		}
		return left.event_position < right.event_position;
	});
	return result;
}

idx_t QueryRequestProfileStore::QueryProfileCount() const {
	lock_guard<std::mutex> guard(state->lock);
	return state->query_profiles.size();
}

idx_t QueryRequestProfileStore::PipelineProfileCount() const {
	lock_guard<std::mutex> guard(state->lock);
	return state->pipeline_profiles.size();
}

void QueryRequestProfileStore::ExportSnapshot(FileSystem &fs, const string &path) const {
	auto snapshot = CaptureSnapshotState(*state);
	ProfileSnapshotWriter writer(fs, path);
	writer.WriteString(PROFILE_SNAPSHOT_MAGIC);
	writer.Write<uint32_t>(PROFILE_SNAPSHOT_VERSION);
	writer.WriteString(DuckDB::SourceID());
	writer.Write<uint64_t>(PROFILE_SAMPLE_LIMIT);
	writer.Write<double>(THROUGHPUT_EWMA_ALPHA);
	WriteSnapshotState(writer, snapshot);
	writer.Flush();
}

void QueryRequestProfileStore::ImportSnapshot(FileSystem &fs, const string &path) {
	ProfileSnapshotReader reader(fs, path);
	if (reader.ReadString() != PROFILE_SNAPSHOT_MAGIC) {
		throw SerializationException("Invalid query profile snapshot magic");
	}
	if (reader.Read<uint32_t>() != PROFILE_SNAPSHOT_VERSION) {
		throw SerializationException("Unsupported query profile snapshot version");
	}
	if (reader.ReadString() != DuckDB::SourceID()) {
		throw SerializationException("Query profile snapshot was created by a different DuckDB build");
	}
	if (reader.Read<uint64_t>() != PROFILE_SAMPLE_LIMIT) {
		throw SerializationException("Query profile snapshot uses a different sample window size");
	}
	if (reader.Read<double>() != THROUGHPUT_EWMA_ALPHA) {
		throw SerializationException("Query profile snapshot uses a different throughput EWMA alpha");
	}
	auto snapshot = ReadSnapshotState(reader);
	if (!reader.Finished()) {
		throw SerializationException("Trailing data in query profile snapshot");
	}
	InstallSnapshotState(*state, std::move(snapshot));
}

void QueryRequestProfileStore::Clear() {
	lock_guard<std::mutex> guard(state->lock);
	state->query_profiles.clear();
	state->pipeline_profiles.clear();
	state->scale_downstream_suffix_profiles.clear();
	state->global_downstream_suffix_profile = SuffixHistogramAggregate();
	state->source_sink_continuation_profiles.clear();
	state->source_continuation_profiles.clear();
	state->global_raw_continuation_profile = ContinuationAggregate();
	state->source_sink_throughput_profiles.clear();
	state->source_throughput_profiles.clear();
	state->global_compatible_throughput_profiles.clear();
	state->internal_event_profiles.clear();
	state->scale_internal_event_profiles.clear();
	state->global_internal_event_profiles.clear();
	state->global_finish_tail_profile = ContinuationAggregate();
	state->query_samples.clear();
	state->pipeline_instances.clear();
	state->internal_event_instances.clear();
}

} // namespace duckdb
