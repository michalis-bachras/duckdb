//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/source_throughput.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

enum class SourceThroughputKind : uint8_t {
	UNKNOWN,
	MIXED_SOURCE_TUPLES,
	NO_SOURCE_SCAN,
	SINGLE_AGGREGATE_ROW,
	BASE_TABLE_ROWS,
	INDEX_ROWIDS,
	TABLE_SCAN_ROWS,
	MATERIALIZED_ROWS,
	PERFECT_HASH_GROUPS,
	AGGREGATE_GROUPS,
	PARTITIONED_AGGREGATE_GROUPS,
	WINDOW_ROWS,
	SORTED_ROWS,
	TOP_N_HEAP_ROWS,
	HASH_JOIN_BUILD_ROWS,
	HASH_JOIN_PROBE_ROWS,
	NESTED_LOOP_JOIN_BUILD_ROWS,
	BLOCKWISE_NL_JOIN_BUILD_ROWS,
	PIECEWISE_MERGE_JOIN_BUILD_ROWS,
	IE_JOIN_ROWS,
	ASOF_JOIN_ROWS,
	POSITIONAL_JOIN_ROWS,
	LIMIT_MATERIALIZED_ROWS,
	LIMIT_PERCENT_MATERIALIZED_ROWS,
	RESERVOIR_SAMPLE_ROWS,
	RECURSIVE_CTE_ROWS,
	DUMMY_ROW,
	EMPTY_RESULT
};

const char *SourceThroughputKindToString(SourceThroughputKind kind);
SourceThroughputKind SourceThroughputKindFromString(const string &kind);

struct SourceThroughputCounters {
	string tuple_kind = "unknown";
	string tuple_confidence = "unknown";
	idx_t tuples_touched = 0;
	idx_t chunks_touched = 0;
	idx_t native_units_touched = 0;
	string native_unit;
	bool adaptive_morsel_candidate = false;
	bool reported = false;
	string work_kind = "unknown";
	string work_confidence = "unknown";
	idx_t work_units_touched = 0;
	string work_unit;
	bool work_reported = false;

	static idx_t EstimateStandardChunks(idx_t tuples);

	void AddTuples(idx_t tuples, const string &kind, const string &confidence, bool adaptive_candidate,
	               idx_t chunks = 0, idx_t native_units = 0, const string &native_unit_p = string());
	void AddTuples(idx_t tuples, SourceThroughputKind kind, const string &confidence, bool adaptive_candidate,
	               idx_t chunks = 0, idx_t native_units = 0, const string &native_unit_p = string());
	void AddWorkUnits(idx_t work_units, const string &kind, const string &confidence,
	                  const string &work_unit_p = string());
	void AddWorkUnits(idx_t work_units, SourceThroughputKind kind, const string &confidence,
	                  const string &work_unit_p = string());
};

double SourceTuplesPerTaskSecond(idx_t tuples, uint64_t duration_ns);

} // namespace duckdb
