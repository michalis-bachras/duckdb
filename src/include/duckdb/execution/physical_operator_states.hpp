//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/source_throughput.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/execution/partition_info.hpp"

namespace duckdb {
class Event;
class Executor;
class PhysicalOperator;
class Pipeline;
class PipelineBuildState;
class MetaPipeline;
class InterruptState;

struct SourceInputVolume {
	SourceThroughputKind kind = SourceThroughputKind::UNKNOWN;
	string confidence = "unknown";
	idx_t rows = 0;
	idx_t chunks_equiv = 0;
	idx_t native_units = 0;
	string native_unit;
};

// LCOV_EXCL_START
class OperatorState {
public:
	virtual ~OperatorState() {
	}

	virtual void Finalize(const PhysicalOperator &op, ExecutionContext &context) {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual idx_t MaxThreads(idx_t source_max_threads) {
		return source_max_threads;
	}
};

class GlobalSinkState : public StateWithBlockableTasks {
public:
	GlobalSinkState() : state(SinkFinalizeType::READY) {
	}
	virtual ~GlobalSinkState() {
	}

	SinkFinalizeType state;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual idx_t MaxThreads(idx_t source_max_threads) {
		return source_max_threads;
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}

	//! Source partition info
	SourcePartitionInfo partition_info;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class GlobalSourceState : public StateWithBlockableTasks {
public:
	virtual ~GlobalSourceState() {
	}

	virtual idx_t MaxThreads() {
		return 1;
	}

	virtual SourceInputVolume GetSourceInputVolume() const {
		return SourceInputVolume();
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class LocalSourceState {
public:
	virtual ~LocalSourceState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct OperatorSinkInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSourceInput {
	OperatorSourceInput(GlobalSourceState &global_state_p, LocalSourceState &local_state_p,
	                    InterruptState &interrupt_state_p, SourceThroughputCounters *source_throughput_p = nullptr)
	    : global_state(global_state_p), local_state(local_state_p), interrupt_state(interrupt_state_p),
	      source_throughput(source_throughput_p) {
	}

	GlobalSourceState &global_state;
	LocalSourceState &local_state;
	InterruptState &interrupt_state;
	SourceThroughputCounters *source_throughput;

	void ReportSourceTuplesTouched(idx_t tuples, const string &kind, const string &confidence, bool adaptive_candidate,
	                               idx_t chunks = 0, idx_t native_units = 0, const string &native_unit = string()) {
		if (source_throughput) {
			source_throughput->AddTuples(tuples, kind, confidence, adaptive_candidate, chunks, native_units,
			                             native_unit);
		}
	}

	void ReportSourceTuplesTouched(idx_t tuples, SourceThroughputKind kind, const string &confidence,
	                               bool adaptive_candidate, idx_t chunks = 0, idx_t native_units = 0,
	                               const string &native_unit = string()) {
		ReportSourceTuplesTouched(tuples, SourceThroughputKindToString(kind), confidence, adaptive_candidate, chunks,
		                          native_units, native_unit);
	}

	void ReportSourceOutputChunk(const DataChunk &chunk, const string &kind, const string &confidence,
	                             bool adaptive_candidate, const string &native_unit) {
		if (chunk.size() > 0) {
			ReportSourceTuplesTouched(chunk.size(), kind, confidence, adaptive_candidate, 1, chunk.size(), native_unit);
		}
	}

	void ReportSourceOutputChunk(const DataChunk &chunk, SourceThroughputKind kind, const string &confidence,
	                             bool adaptive_candidate, const string &native_unit) {
		ReportSourceOutputChunk(chunk, SourceThroughputKindToString(kind), confidence, adaptive_candidate, native_unit);
	}

	void ReportSourceWorkUnits(idx_t work_units, const string &kind, const string &confidence,
	                           const string &work_unit = string()) {
		if (source_throughput) {
			source_throughput->AddWorkUnits(work_units, kind, confidence, work_unit);
		}
	}

	void ReportSourceWorkUnits(idx_t work_units, SourceThroughputKind kind, const string &confidence,
	                           const string &work_unit = string()) {
		ReportSourceWorkUnits(work_units, SourceThroughputKindToString(kind), confidence, work_unit);
	}

	void ReportSourceControl(const string &kind, const string &confidence, const string &native_unit = "none") {
		ReportSourceTuplesTouched(0, kind, confidence, false, 0, 0, native_unit);
	}

	void ReportSourceControl(SourceThroughputKind kind, const string &confidence, const string &native_unit = "none") {
		ReportSourceControl(SourceThroughputKindToString(kind), confidence, native_unit);
	}
};

struct OperatorSinkCombineInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkFinalizeInput {
	GlobalSinkState &global_state;
	InterruptState &interrupt_state;
};

struct OperatorFinalizeInput {
	GlobalOperatorState &global_state;
	InterruptState &interrupt_state;
};

struct OperatorSinkNextBatchInput {
	GlobalSinkState &global_state;
	LocalSinkState &local_state;
	InterruptState &interrupt_state;
};

// LCOV_EXCL_STOP

} // namespace duckdb
