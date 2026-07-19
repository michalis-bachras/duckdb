//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/pipeline_continuation.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/source_throughput.hpp"

namespace duckdb {

struct SourceWorkClass {
	PhysicalOperatorType source_type = PhysicalOperatorType::INVALID;
	SourceThroughputKind work_kind = SourceThroughputKind::UNKNOWN;

	bool IsValid() const {
		return source_type != PhysicalOperatorType::INVALID && work_kind != SourceThroughputKind::UNKNOWN;
	}

	bool operator==(const SourceWorkClass &other) const {
		return source_type == other.source_type && work_kind == other.work_kind;
	}
};

string SourceWorkClassToString(const SourceWorkClass &work_class);

struct PipelineProfileIdentity {
	idx_t pipeline_id = 0;
	uint64_t pipeline_signature_hash = 0;
	SourceWorkClass source_work_class;
	PhysicalOperatorType sink_type = PhysicalOperatorType::INVALID;
	string planned_input_native_unit;
	bool valid = false;
};

enum class PipelineThroughputEstimateLevel : uint8_t {
	NONE = 0,
	EXACT,
	SOURCE_SINK,
	SOURCE,
	GLOBAL_COMPATIBLE,
	SCALE_FACTOR
};

const char *PipelineThroughputEstimateLevelToString(PipelineThroughputEstimateLevel level);

struct PipelineThroughputEstimate {
	bool valid = false;
	PipelineThroughputEstimateLevel level = PipelineThroughputEstimateLevel::NONE;
	idx_t sample_count = 0;
	double mean_work_units_per_s = 0;
	double ewma_work_units_per_s = 0;
};

enum class PipelineLifecycleTailEstimateLevel : uint8_t { NONE = 0, EXACT, GLOBAL, SCALE_FACTOR };

const char *PipelineLifecycleTailEstimateLevelToString(PipelineLifecycleTailEstimateLevel level);

struct PipelineLifecycleTailEstimate {
	bool valid = false;
	PipelineLifecycleTailEstimateLevel level = PipelineLifecycleTailEstimateLevel::NONE;
	idx_t sample_count = 0;
	double mean_ns = 0;
	double p50_ns = 0;
	double p90_ns = 0;
};

enum class ContinuationEstimateLevel : uint8_t {
	NONE = 0,
	EXACT,
	SOURCE_SINK,
	SOURCE,
	GLOBAL_RAW,
	SCALE_FACTOR
};

enum class ContinuationEstimateKind : uint8_t {
	INVALID = 0,
	NS_PER_WORK_UNIT,
	RAW_LATENCY_NS
};

const char *ContinuationEstimateLevelToString(ContinuationEstimateLevel level);
const char *ContinuationEstimateKindToString(ContinuationEstimateKind kind);

struct PipelineContinuationEstimate {
	bool valid = false;
	ContinuationEstimateLevel level = ContinuationEstimateLevel::NONE;
	ContinuationEstimateKind kind = ContinuationEstimateKind::INVALID;
	idx_t sample_count = 0;
	double mean = 0;
	double p50 = 0;
	double p90 = 0;
};

} // namespace duckdb
