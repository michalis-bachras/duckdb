//===----------------------------------------------------------------------===//
//                         DuckDB
//
// End-to-end TPC-H tests for stride scheduler correctness.
//
// Runs TPC-H queries under both DEFAULT and STRIDE scheduling and
// verifies that:
//   1. All queries produce correct results (against known answers)
//   2. Results are identical across scheduler types
//   3. Concurrent TPC-H queries work under STRIDE mode
//
// Uses SF=0.01 for fast execution in CI. The tpch extension must be
// loaded (tests skip gracefully if not available).
//
//===----------------------------------------------------------------------===//

#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/string_util.hpp"

#include <thread>

using namespace duckdb;
using namespace std;

static constexpr double SF = 0.01;

//===----------------------------------------------------------------------===//
// Helper: generate TPC-H data and verify extension is available
//===----------------------------------------------------------------------===//
static bool SetupTPCH(DuckDB &db, Connection &con) {
	if (!db.ExtensionIsLoaded("tpch")) {
		return false;
	}
	REQUIRE_NO_FAIL(con.Query("CALL dbgen(sf=" + to_string(SF) + ")"));
	return true;
}

//===----------------------------------------------------------------------===//
// Helper: run a TPC-H query and compare against reference answer.
// Returns empty string on match, error message on mismatch or failure.
// Thread-safe (does not use compare_csv which writes to a shared temp file).
//===----------------------------------------------------------------------===//
static string RunAndVerifyTPCH(DuckDB &db, int query_num) {
	Connection c(db);
	string query_sql = TpchExtension::GetQuery(query_num);

	// Run the query under DEFAULT to get reference results
	c.Query("SET scheduler_type='default'");
	auto r_ref = c.Query(query_sql);
	if (r_ref->HasError()) {
		return "Q" + to_string(query_num) + " reference error: " + r_ref->GetError();
	}

	// Run under STRIDE
	c.Query("SET scheduler_type='stride'");
	auto r_stride = c.Query(query_sql);
	if (r_stride->HasError()) {
		return "Q" + to_string(query_num) + " STRIDE error: " + r_stride->GetError();
	}

	// Compare row/column counts
	if (r_ref->RowCount() != r_stride->RowCount()) {
		return "Q" + to_string(query_num) + " row count mismatch: " +
		       to_string(r_ref->RowCount()) + " vs " + to_string(r_stride->RowCount());
	}
	if (r_ref->ColumnCount() != r_stride->ColumnCount()) {
		return "Q" + to_string(query_num) + " column count mismatch";
	}

	// Compare every cell
	for (idx_t row = 0; row < r_ref->RowCount(); row++) {
		for (idx_t col = 0; col < r_ref->ColumnCount(); col++) {
			auto v1 = r_ref->GetValue(col, row).ToString();
			auto v2 = r_stride->GetValue(col, row).ToString();
			if (v1 != v2) {
				return "Q" + to_string(query_num) + " mismatch at row " + to_string(row) +
				       " col " + to_string(col) + ": ref=" + v1 + " stride=" + v2;
			}
		}
	}
	return "";
}

//===----------------------------------------------------------------------===//
// Helper: run a TPC-H query under stride and verify it produces results
// (non-empty, no error). Thread-safe.
//===----------------------------------------------------------------------===//
static string RunTPCHUnderStride(DuckDB &db, int query_num) {
	Connection c(db);
	string query_sql = TpchExtension::GetQuery(query_num);
	auto r = c.Query(query_sql);
	if (r->HasError()) {
		return "Q" + to_string(query_num) + " error: " + r->GetError();
	}
	if (r->RowCount() == 0) {
		return "Q" + to_string(query_num) + " returned 0 rows";
	}
	return "";
}

//===----------------------------------------------------------------------===//
// TPCH-S.1: All 22 TPC-H Queries Correct Under STRIDE Scheduling
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.1: All TPC-H queries correct under STRIDE", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	con.Query("SET scheduler_type='stride'");
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	for (idx_t q = 1; q <= 22; q++) {
		string query_sql = TpchExtension::GetQuery(q);
		auto result = con.Query(query_sql);
		string answer = TpchExtension::GetAnswer(SF, q);
		auto cmp = compare_csv(*result, answer, true);
		if (!cmp.empty()) {
			FAIL("TPC-H Q" + to_string(q) + " under STRIDE: " + cmp);
		}
	}

	// All slots cleaned up
	REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
}

//===----------------------------------------------------------------------===//
// TPCH-S.2: All 22 TPC-H Queries Correct Under DEFAULT Scheduling
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.2: All TPC-H queries correct under DEFAULT", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::DEFAULT);

	for (idx_t q = 1; q <= 22; q++) {
		string query_sql = TpchExtension::GetQuery(q);
		auto result = con.Query(query_sql);
		string answer = TpchExtension::GetAnswer(SF, q);
		auto cmp = compare_csv(*result, answer, true);
		if (!cmp.empty()) {
			FAIL("TPC-H Q" + to_string(q) + " under DEFAULT: " + cmp);
		}
	}
}

//===----------------------------------------------------------------------===//
// TPCH-S.3: STRIDE and DEFAULT Produce Identical Results
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.3: STRIDE and DEFAULT produce identical results", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	for (idx_t q = 1; q <= 22; q++) {
		string query_sql = TpchExtension::GetQuery(q);

		// DEFAULT mode
		con.Query("SET scheduler_type='default'");
		auto r_default = con.Query(query_sql);
		REQUIRE(!r_default->HasError());

		// STRIDE mode
		con.Query("SET scheduler_type='stride'");
		auto r_stride = con.Query(query_sql);
		REQUIRE(!r_stride->HasError());

		// Same row count
		REQUIRE(r_default->RowCount() == r_stride->RowCount());

		// Same column count
		REQUIRE(r_default->ColumnCount() == r_stride->ColumnCount());

		// Compare every cell via ToString to handle NULLs
		for (idx_t row = 0; row < r_default->RowCount(); row++) {
			for (idx_t col = 0; col < r_default->ColumnCount(); col++) {
				auto val_def = r_default->GetValue(col, row).ToString();
				auto val_str = r_stride->GetValue(col, row).ToString();
				if (val_def != val_str) {
					FAIL("TPC-H Q" + to_string(q) + " mismatch at row " + to_string(row) +
					     " col " + to_string(col) + ": DEFAULT=" + val_def +
					     " STRIDE=" + val_str);
				}
			}
		}
	}
}

//===----------------------------------------------------------------------===//
// TPCH-S.4: Concurrent TPC-H Queries Under STRIDE
//
// Run multiple TPC-H queries simultaneously under STRIDE scheduling.
// Each thread runs its query under both DEFAULT and STRIDE and compares
// results cell-by-cell (thread-safe, no shared temp files).
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.4: Concurrent TPC-H queries under STRIDE", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	con.Query("SET scheduler_type='stride'");

	int query_nums[] = {1, 6, 14};
	constexpr int N = 3;
	string errors[N];

	std::vector<std::thread> threads;
	for (int i = 0; i < N; i++) {
		threads.emplace_back([&db, &errors, &query_nums, i]() {
			errors[i] = RunAndVerifyTPCH(db, query_nums[i]);
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	for (int i = 0; i < N; i++) {
		if (!errors[i].empty()) {
			FAIL(errors[i]);
		}
	}

	// All slots cleaned up
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
}

//===----------------------------------------------------------------------===//
// TPCH-S.5: Many Concurrent Queries (Stride Fairness)
//
// Run 6 different TPC-H queries simultaneously under STRIDE. Each thread
// verifies its query produces non-empty, error-free results.
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.5: Many concurrent TPC-H queries under STRIDE", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	con.Query("SET scheduler_type='stride'");

	int query_nums[] = {3, 6, 10, 12, 14, 19};
	constexpr int N = 6;
	string errors[N];

	std::vector<std::thread> threads;
	for (int i = 0; i < N; i++) {
		threads.emplace_back([&db, &errors, &query_nums, i]() {
			errors[i] = RunTPCHUnderStride(db, query_nums[i]);
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	for (int i = 0; i < N; i++) {
		if (!errors[i].empty()) {
			FAIL(errors[i]);
		}
	}

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
}

//===----------------------------------------------------------------------===//
// TPCH-S.6: Sequential TPC-H Queries After Scheduler Switch
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.6: TPC-H queries correct after scheduler switch", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	// Run a subset under DEFAULT
	con.Query("SET scheduler_type='default'");
	for (int q : {1, 3, 6, 10, 14}) {
		string query_sql = TpchExtension::GetQuery(q);
		auto r = con.Query(query_sql);
		string answer = TpchExtension::GetAnswer(SF, q);
		auto cmp = compare_csv(*r, answer, true);
		if (!cmp.empty()) {
			FAIL("DEFAULT round, Q" + to_string(q) + ": " + cmp);
		}
	}

	// Switch to STRIDE and run same subset
	con.Query("SET scheduler_type='stride'");
	for (int q : {1, 3, 6, 10, 14}) {
		string query_sql = TpchExtension::GetQuery(q);
		auto r = con.Query(query_sql);
		string answer = TpchExtension::GetAnswer(SF, q);
		auto cmp = compare_csv(*r, answer, true);
		if (!cmp.empty()) {
			FAIL("STRIDE round, Q" + to_string(q) + ": " + cmp);
		}
	}

	// Switch back to DEFAULT
	con.Query("SET scheduler_type='default'");
	for (int q : {1, 3, 6, 10, 14}) {
		string query_sql = TpchExtension::GetQuery(q);
		auto r = con.Query(query_sql);
		string answer = TpchExtension::GetAnswer(SF, q);
		auto cmp = compare_csv(*r, answer, true);
		if (!cmp.empty()) {
			FAIL("DEFAULT round 2, Q" + to_string(q) + ": " + cmp);
		}
	}
}

//===----------------------------------------------------------------------===//
// TPCH-S.7: Mixed Concurrent Workload (Short + Long Queries)
//
// Short queries (Q6, Q14) alongside long queries (Q1, Q5). Each thread
// verifies STRIDE produces the same result as DEFAULT.
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.7: Mixed short+long TPC-H queries under STRIDE", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	con.Query("SET scheduler_type='stride'");

	int query_nums[] = {1, 5, 6, 14};
	constexpr int N = 4;
	string errors[N];

	std::vector<std::thread> threads;
	for (int i = 0; i < N; i++) {
		threads.emplace_back([&db, &errors, &query_nums, i]() {
			errors[i] = RunAndVerifyTPCH(db, query_nums[i]);
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	for (int i = 0; i < N; i++) {
		if (!errors[i].empty()) {
			FAIL(errors[i]);
		}
	}
}

//===----------------------------------------------------------------------===//
// TPCH-S.8: Repeated Concurrent Bursts
//
// Run 3 rounds of concurrent TPC-H queries. Verifies scheduler state
// is properly cleaned up between rounds.
//===----------------------------------------------------------------------===//
TEST_CASE("TPCH-S.8: Repeated concurrent TPC-H bursts under STRIDE", "[stride][tpch][.]") {
	DuckDB db(nullptr);
	Connection con(db);

	if (!SetupTPCH(db, con)) {
		return;
	}

	con.Query("SET scheduler_type='stride'");
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);

	for (int round = 0; round < 3; round++) {
		int queries[] = {1, 6, 14};
		constexpr int N = 3;
		string errors[N];

		std::vector<std::thread> threads;
		for (int i = 0; i < N; i++) {
			threads.emplace_back([&db, &errors, &queries, i]() {
				errors[i] = RunTPCHUnderStride(db, queries[i]);
			});
		}

		for (auto &t : threads) {
			t.join();
		}

		for (int i = 0; i < N; i++) {
			if (!errors[i].empty()) {
				FAIL("Round " + to_string(round) + ": " + errors[i]);
			}
		}

		// Slots cleaned up between rounds
		REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
	}
}
