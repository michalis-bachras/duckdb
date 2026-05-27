#include "duckdb/execution/operator/scan/physical_empty_result.hpp"

namespace duckdb {

SourceResultType PhysicalEmptyResult::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	input.ReportSourceControl(SourceThroughputKind::EMPTY_RESULT, "exact");
	return SourceResultType::FINISHED;
}

} // namespace duckdb
