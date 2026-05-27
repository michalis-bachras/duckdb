#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

namespace duckdb {

SourceResultType PhysicalDummyScan::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
	input.ReportSourceTuplesTouched(1, SourceThroughputKind::DUMMY_ROW, "exact", false, 1, 1, "row");

	return SourceResultType::FINISHED;
}

} // namespace duckdb
