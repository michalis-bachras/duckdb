//===----------------------------------------------------------------------===//
//                         DuckDB
//
// source_throughput.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/source_throughput.hpp"

namespace duckdb {

static constexpr double SOURCE_THROUGHPUT_EWMA_ALPHA = 0.8;

const char *SourceThroughputKindToString(SourceThroughputKind kind) {
	switch (kind) {
	case SourceThroughputKind::UNKNOWN:
		return "unknown";
	case SourceThroughputKind::MIXED_SOURCE_TUPLES:
		return "mixed_source_tuples";
	case SourceThroughputKind::NO_SOURCE_SCAN:
		return "no_source_scan";
	case SourceThroughputKind::SINGLE_AGGREGATE_ROW:
		return "single_aggregate_row";
	case SourceThroughputKind::BASE_TABLE_ROWS:
		return "base_table_rows";
	case SourceThroughputKind::INDEX_ROWIDS:
		return "index_rowids";
	case SourceThroughputKind::TABLE_SCAN_ROWS:
		return "table_scan_rows";
	case SourceThroughputKind::MATERIALIZED_ROWS:
		return "materialized_rows";
	case SourceThroughputKind::PERFECT_HASH_GROUPS:
		return "perfect_hash_groups";
	case SourceThroughputKind::AGGREGATE_GROUPS:
		return "aggregate_groups";
	case SourceThroughputKind::PARTITIONED_AGGREGATE_GROUPS:
		return "partitioned_aggregate_groups";
	case SourceThroughputKind::WINDOW_ROWS:
		return "window_rows";
	case SourceThroughputKind::SORTED_ROWS:
		return "sorted_rows";
	case SourceThroughputKind::TOP_N_HEAP_ROWS:
		return "top_n_heap_rows";
	case SourceThroughputKind::HASH_JOIN_BUILD_ROWS:
		return "hash_join_build_rows";
	case SourceThroughputKind::HASH_JOIN_PROBE_ROWS:
		return "hash_join_probe_rows";
	case SourceThroughputKind::NESTED_LOOP_JOIN_BUILD_ROWS:
		return "nested_loop_join_build_rows";
	case SourceThroughputKind::BLOCKWISE_NL_JOIN_BUILD_ROWS:
		return "blockwise_nested_loop_join_build_rows";
	case SourceThroughputKind::PIECEWISE_MERGE_JOIN_BUILD_ROWS:
		return "piecewise_merge_join_build_rows";
	case SourceThroughputKind::IE_JOIN_ROWS:
		return "ie_join_rows";
	case SourceThroughputKind::ASOF_JOIN_ROWS:
		return "asof_join_rows";
	case SourceThroughputKind::POSITIONAL_JOIN_ROWS:
		return "positional_join_rows";
	case SourceThroughputKind::LIMIT_MATERIALIZED_ROWS:
		return "limit_materialized_rows";
	case SourceThroughputKind::LIMIT_PERCENT_MATERIALIZED_ROWS:
		return "limit_percent_materialized_rows";
	case SourceThroughputKind::RESERVOIR_SAMPLE_ROWS:
		return "reservoir_sample_rows";
	case SourceThroughputKind::RECURSIVE_CTE_ROWS:
		return "recursive_cte_rows";
	case SourceThroughputKind::DUMMY_ROW:
		return "dummy_row";
	case SourceThroughputKind::EMPTY_RESULT:
		return "empty_result";
	default:
		return "unknown";
	}
}

SourceThroughputKind SourceThroughputKindFromString(const string &kind) {
	if (kind == "mixed_source_tuples") {
		return SourceThroughputKind::MIXED_SOURCE_TUPLES;
	}
	if (kind == "no_source_scan") {
		return SourceThroughputKind::NO_SOURCE_SCAN;
	}
	if (kind == "single_aggregate_row") {
		return SourceThroughputKind::SINGLE_AGGREGATE_ROW;
	}
	if (kind == "base_table_rows") {
		return SourceThroughputKind::BASE_TABLE_ROWS;
	}
	if (kind == "index_rowids") {
		return SourceThroughputKind::INDEX_ROWIDS;
	}
	if (kind == "table_scan_rows") {
		return SourceThroughputKind::TABLE_SCAN_ROWS;
	}
	if (kind == "materialized_rows") {
		return SourceThroughputKind::MATERIALIZED_ROWS;
	}
	if (kind == "perfect_hash_groups") {
		return SourceThroughputKind::PERFECT_HASH_GROUPS;
	}
	if (kind == "aggregate_groups") {
		return SourceThroughputKind::AGGREGATE_GROUPS;
	}
	if (kind == "partitioned_aggregate_groups") {
		return SourceThroughputKind::PARTITIONED_AGGREGATE_GROUPS;
	}
	if (kind == "window_rows") {
		return SourceThroughputKind::WINDOW_ROWS;
	}
	if (kind == "sorted_rows") {
		return SourceThroughputKind::SORTED_ROWS;
	}
	if (kind == "top_n_heap_rows") {
		return SourceThroughputKind::TOP_N_HEAP_ROWS;
	}
	if (kind == "hash_join_build_rows") {
		return SourceThroughputKind::HASH_JOIN_BUILD_ROWS;
	}
	if (kind == "hash_join_probe_rows") {
		return SourceThroughputKind::HASH_JOIN_PROBE_ROWS;
	}
	if (kind == "nested_loop_join_build_rows") {
		return SourceThroughputKind::NESTED_LOOP_JOIN_BUILD_ROWS;
	}
	if (kind == "blockwise_nested_loop_join_build_rows") {
		return SourceThroughputKind::BLOCKWISE_NL_JOIN_BUILD_ROWS;
	}
	if (kind == "piecewise_merge_join_build_rows") {
		return SourceThroughputKind::PIECEWISE_MERGE_JOIN_BUILD_ROWS;
	}
	if (kind == "ie_join_rows") {
		return SourceThroughputKind::IE_JOIN_ROWS;
	}
	if (kind == "asof_join_rows") {
		return SourceThroughputKind::ASOF_JOIN_ROWS;
	}
	if (kind == "positional_join_rows") {
		return SourceThroughputKind::POSITIONAL_JOIN_ROWS;
	}
	if (kind == "limit_materialized_rows") {
		return SourceThroughputKind::LIMIT_MATERIALIZED_ROWS;
	}
	if (kind == "limit_percent_materialized_rows") {
		return SourceThroughputKind::LIMIT_PERCENT_MATERIALIZED_ROWS;
	}
	if (kind == "reservoir_sample_rows") {
		return SourceThroughputKind::RESERVOIR_SAMPLE_ROWS;
	}
	if (kind == "recursive_cte_rows") {
		return SourceThroughputKind::RECURSIVE_CTE_ROWS;
	}
	if (kind == "dummy_row") {
		return SourceThroughputKind::DUMMY_ROW;
	}
	if (kind == "empty_result") {
		return SourceThroughputKind::EMPTY_RESULT;
	}
	return SourceThroughputKind::UNKNOWN;
}

idx_t SourceThroughputCounters::EstimateStandardChunks(idx_t tuples) {
	return tuples == 0 ? 0 : (tuples + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

void SourceThroughputCounters::AddTuples(idx_t tuples, const string &kind, const string &confidence,
                                         bool adaptive_candidate, idx_t chunks, idx_t native_units,
                                         const string &native_unit_p) {
	if (!reported || tuple_kind == "unknown") {
		tuple_kind = kind;
		tuple_confidence = confidence;
		native_unit = native_unit_p;
	} else if (tuple_kind != kind) {
		tuple_kind = SourceThroughputKindToString(SourceThroughputKind::MIXED_SOURCE_TUPLES);
		tuple_confidence = "estimate";
	} else if (tuple_confidence != confidence && tuple_confidence != "estimate") {
		tuple_confidence = "estimate";
	}
	tuples_touched += tuples;
	chunks_touched += chunks ? chunks : EstimateStandardChunks(tuples);
	native_units_touched += native_units;
	adaptive_morsel_candidate = adaptive_morsel_candidate || adaptive_candidate;
	reported = true;
}

void SourceThroughputCounters::AddTuples(idx_t tuples, SourceThroughputKind kind, const string &confidence,
                                         bool adaptive_candidate, idx_t chunks, idx_t native_units,
                                         const string &native_unit_p) {
	AddTuples(tuples, SourceThroughputKindToString(kind), confidence, adaptive_candidate, chunks, native_units,
	          native_unit_p);
}

double SourceTuplesPerTaskSecond(idx_t tuples, uint64_t duration_ns) {
	if (duration_ns == 0) {
		return 0;
	}
	return static_cast<double>(tuples) * 1000000000.0 / static_cast<double>(duration_ns);
}

const SourceThroughputEstimate &SourceThroughputEstimator::Update(const SourceThroughputCounters &counters,
                                                                  uint64_t duration_ns) {
	if (!counters.reported || !counters.adaptive_morsel_candidate || counters.tuples_touched == 0 ||
	    duration_ns == 0) {
		return estimate;
	}
	auto task_throughput = SourceTuplesPerTaskSecond(counters.tuples_touched, duration_ns);
	if (estimate.sample_count == 0) {
		estimate.alpha = SOURCE_THROUGHPUT_EWMA_ALPHA;
		estimate.estimated_tuples_per_task_s = task_throughput;
	} else {
		estimate.estimated_tuples_per_task_s =
		    estimate.alpha * task_throughput + (1 - estimate.alpha) * estimate.estimated_tuples_per_task_s;
	}
	estimate.last_task_tuples_per_s = task_throughput;
	estimate.sample_count++;
	estimate.sample_tuples += counters.tuples_touched;
	estimate.sample_ns += duration_ns;
	return estimate;
}

void SourceThroughputEstimator::Reset() {
	estimate = SourceThroughputEstimate();
}

} // namespace duckdb
