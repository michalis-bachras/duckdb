//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/scheduler_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! SchedulerType determines which scheduling policy to use for query execution
enum class SchedulerType : uint8_t {
	//! Default FIFO scheduler (current DuckDB behavior)
	DEFAULT = 0,
	//! Stride scheduler with priority decay (based on SIGMOD 2021 paper)
	STRIDE = 1,
	//! Machine learning based scheduler (future)
	ML = 2
};

} // namespace duckdb
