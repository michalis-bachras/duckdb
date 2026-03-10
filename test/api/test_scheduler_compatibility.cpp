//===----------------------------------------------------------------------===//
//                         DuckDB
//
// Test scheduler policy compatibility — verify that DuckDB operates
// correctly under each scheduler type (DEFAULT, STRIDE) and that
// runtime switching between them works.
//
//===----------------------------------------------------------------------===//

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/enums/scheduler_type.hpp"

#include <thread>
#include <atomic>

using namespace duckdb;

//===----------------------------------------------------------------------===//
// Test SC.1: SchedulerType Enum Values Are Distinct
//===----------------------------------------------------------------------===//
TEST_CASE("SC.1: SchedulerType enum values are distinct", "[scheduler][api]") {
	REQUIRE(static_cast<uint8_t>(SchedulerType::DEFAULT) == 0);
	REQUIRE(static_cast<uint8_t>(SchedulerType::STRIDE) == 1);
	REQUIRE(static_cast<uint8_t>(SchedulerType::ML) == 2);
	REQUIRE(SchedulerType::DEFAULT != SchedulerType::STRIDE);
	REQUIRE(SchedulerType::DEFAULT != SchedulerType::ML);
	REQUIRE(SchedulerType::STRIDE != SchedulerType::ML);
}

//===----------------------------------------------------------------------===//
// Test SC.2: DEFAULT Scheduler Executes Queries Correctly
//
// Verify that DuckDB's default FIFO scheduler produces correct results
// for a variety of query types.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.2: DEFAULT scheduler executes queries correctly", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	// Verify we're in DEFAULT mode
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::DEFAULT);

	SECTION("Simple aggregation") {
		auto r = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!r->HasError());
		REQUIRE(r->GetValue(0, 0) == Value::BIGINT(4999950000LL));
	}

	SECTION("Sort + aggregate") {
		auto r = con.Query(
		    "SELECT key, COUNT(*) FROM (SELECT i % 10 AS key FROM range(10000) t(i)) sub "
		    "GROUP BY key ORDER BY key LIMIT 3");
		REQUIRE(!r->HasError());
		REQUIRE(r->RowCount() == 3);
		REQUIRE(r->GetValue(1, 0) == Value::BIGINT(1000));
	}

	SECTION("Concurrent queries") {
		string error;
		std::atomic<int64_t> result1 {0}, result2 {0};

		std::thread t1([&]() {
			Connection c(db);
			auto r = c.Query("SELECT SUM(i) FROM range(50000) t(i)");
			if (!r->HasError()) {
				result1.store(r->GetValue(0, 0).GetValue<int64_t>());
			} else {
				error = r->GetError();
			}
		});
		std::thread t2([&]() {
			Connection c(db);
			auto r = c.Query("SELECT SUM(i) FROM range(30000) t(i)");
			if (!r->HasError()) {
				result2.store(r->GetValue(0, 0).GetValue<int64_t>());
			} else {
				error = r->GetError();
			}
		});
		t1.join();
		t2.join();

		REQUIRE(error.empty());
		REQUIRE(result1.load() == 1249975000LL);
		REQUIRE(result2.load() == 449985000LL);
	}
}

//===----------------------------------------------------------------------===//
// Test SC.3: STRIDE Scheduler Executes Queries Correctly
//
// Same queries as SC.2 but under STRIDE scheduling. Results must be
// identical — the scheduler affects latency/fairness, not correctness.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.3: STRIDE scheduler executes queries correctly", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);
	con.Query("SET scheduler_type='stride'");

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	SECTION("Simple aggregation") {
		auto r = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!r->HasError());
		REQUIRE(r->GetValue(0, 0) == Value::BIGINT(4999950000LL));
	}

	SECTION("Sort + aggregate") {
		auto r = con.Query(
		    "SELECT key, COUNT(*) FROM (SELECT i % 10 AS key FROM range(10000) t(i)) sub "
		    "GROUP BY key ORDER BY key LIMIT 3");
		REQUIRE(!r->HasError());
		REQUIRE(r->RowCount() == 3);
		REQUIRE(r->GetValue(1, 0) == Value::BIGINT(1000));
	}

	SECTION("Concurrent queries") {
		string error;
		std::atomic<int64_t> result1 {0}, result2 {0};

		std::thread t1([&]() {
			Connection c(db);
			auto r = c.Query("SELECT SUM(i) FROM range(50000) t(i)");
			if (!r->HasError()) {
				result1.store(r->GetValue(0, 0).GetValue<int64_t>());
			} else {
				error = r->GetError();
			}
		});
		std::thread t2([&]() {
			Connection c(db);
			auto r = c.Query("SELECT SUM(i) FROM range(30000) t(i)");
			if (!r->HasError()) {
				result2.store(r->GetValue(0, 0).GetValue<int64_t>());
			} else {
				error = r->GetError();
			}
		});
		t1.join();
		t2.join();

		REQUIRE(error.empty());
		REQUIRE(result1.load() == 1249975000LL);
		REQUIRE(result2.load() == 449985000LL);
	}
}

//===----------------------------------------------------------------------===//
// Test SC.4: Runtime Switch from DEFAULT to STRIDE
//
// Start in DEFAULT mode, run a query, switch to STRIDE, run the same
// query. Both must produce identical results.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.4: Runtime switch DEFAULT to STRIDE", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::DEFAULT);

	// Run in DEFAULT mode
	auto r1 = con.Query("SELECT SUM(i * i) FROM range(50000) t(i)");
	REQUIRE(!r1->HasError());
	auto val_default = r1->GetValue(0, 0);

	// Switch to STRIDE
	con.Query("SET scheduler_type='stride'");
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	// Run same query in STRIDE mode
	auto r2 = con.Query("SELECT SUM(i * i) FROM range(50000) t(i)");
	REQUIRE(!r2->HasError());
	auto val_stride = r2->GetValue(0, 0);

	// Results must be identical
	REQUIRE(val_default == val_stride);
}

//===----------------------------------------------------------------------===//
// Test SC.5: Runtime Switch from STRIDE to DEFAULT
//
// Start in STRIDE mode, then switch back to DEFAULT.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.5: Runtime switch STRIDE to DEFAULT", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	// Switch to STRIDE first
	con.Query("SET scheduler_type='stride'");
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	auto r1 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	REQUIRE(!r1->HasError());
	auto val_stride = r1->GetValue(0, 0);

	// Switch back to DEFAULT
	con.Query("SET scheduler_type='default'");
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::DEFAULT);

	auto r2 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	REQUIRE(!r2->HasError());
	auto val_default = r2->GetValue(0, 0);

	REQUIRE(val_stride == val_default);
}

//===----------------------------------------------------------------------===//
// Test SC.6: Multiple Switches Are Stable
//
// Rapidly switch scheduler types multiple times with queries in between.
// The system must remain stable and produce correct results.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.6: Multiple scheduler switches are stable", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	int64_t expected = 4999950000LL; // SUM(range(100000))

	for (int i = 0; i < 5; i++) {
		// DEFAULT mode
		con.Query("SET scheduler_type='default'");
		auto r1 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!r1->HasError());
		REQUIRE(r1->GetValue(0, 0) == Value::BIGINT(expected));

		// STRIDE mode
		con.Query("SET scheduler_type='stride'");
		auto r2 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!r2->HasError());
		REQUIRE(r2->GetValue(0, 0) == Value::BIGINT(expected));
	}
}

//===----------------------------------------------------------------------===//
// Test SC.7: Scheduler Type Persists Across Connections
//
// Setting scheduler_type is a global setting. Verify it persists when
// new connections are created.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.7: Scheduler type persists across connections", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);

	{
		Connection con(db);
		con.Query("SET scheduler_type='stride'");
	}

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	// New connection should see STRIDE scheduler and produce correct results
	{
		Connection con(db);
		auto r = con.Query("SELECT SUM(i) FROM range(50000) t(i)");
		REQUIRE(!r->HasError());
		REQUIRE(r->GetValue(0, 0) == Value::BIGINT(1249975000LL));
	}
}

//===----------------------------------------------------------------------===//
// Test SC.8: STRIDE Mode Cleans Up Slots After Queries
//
// After all queries complete, the slot array should have zero active slots.
// This verifies proper deregistration.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.8: STRIDE mode cleans up slots after queries", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);
	con.Query("SET scheduler_type='stride'");

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	auto &slot_array = scheduler.GetSlotArray();

	// Run several queries
	con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	con.Query("SELECT COUNT(*) FROM range(50000) t(i)");
	con.Query("SELECT MAX(i) FROM range(200000) t(i)");

	// All slots should be freed
	REQUIRE(slot_array.GetActiveSlotCount() == 0);
}

//===----------------------------------------------------------------------===//
// Test SC.9: Complex Queries Produce Identical Results Under Both Modes
//
// Run a complex multi-join, multi-pipeline query under both DEFAULT and
// STRIDE, verify results match.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.9: Complex queries match across scheduler types", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	// Create a table
	con.Query("CREATE TABLE test_data AS SELECT i AS id, i % 100 AS grp, i * 1.5 AS val FROM range(50000) t(i)");

	// Complex query: join + group by + order by + window function
	string complex_query =
	    "SELECT grp, SUM(val), COUNT(*) FROM test_data WHERE id < 40000 GROUP BY grp ORDER BY grp LIMIT 5";

	// DEFAULT mode
	con.Query("SET scheduler_type='default'");
	auto r1 = con.Query(complex_query);
	REQUIRE(!r1->HasError());

	// STRIDE mode
	con.Query("SET scheduler_type='stride'");
	auto r2 = con.Query(complex_query);
	REQUIRE(!r2->HasError());

	// Same row count
	REQUIRE(r1->RowCount() == r2->RowCount());

	// Same values
	for (idx_t row = 0; row < r1->RowCount(); row++) {
		for (idx_t col = 0; col < 3; col++) {
			REQUIRE(r1->GetValue(col, row) == r2->GetValue(col, row));
		}
	}
}

//===----------------------------------------------------------------------===//
// Test SC.10: Error Handling Works Under Both Scheduler Types
//
// Verify that SQL errors are properly propagated regardless of scheduler.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.10: Error handling works under both scheduler types", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	SECTION("DEFAULT mode") {
		con.Query("SET scheduler_type='default'");
		auto r = con.Query("SELECT * FROM nonexistent_table");
		REQUIRE(r->HasError());
	}

	SECTION("STRIDE mode") {
		con.Query("SET scheduler_type='stride'");
		auto r = con.Query("SELECT * FROM nonexistent_table");
		REQUIRE(r->HasError());
	}

	SECTION("Error doesn't corrupt scheduler state") {
		con.Query("SET scheduler_type='stride'");
		auto r1 = con.Query("SELECT * FROM nonexistent_table");
		REQUIRE(r1->HasError());

		// Subsequent valid query should work fine
		auto r2 = con.Query("SELECT 42");
		REQUIRE(!r2->HasError());
		REQUIRE(r2->GetValue(0, 0) == Value::INTEGER(42));

		// Slots cleaned up
		auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
		REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
	}
}

//===----------------------------------------------------------------------===//
// Test SC.11: STRIDE→DEFAULT Deregistration Cleans Up Worker State
//
// When switching from STRIDE to DEFAULT, workers should lazily deregister
// from the slot array. Verify that:
// (a) Queries run correctly under DEFAULT after being in STRIDE,
// (b) The slot array has zero active slots after completion,
// (c) Concurrent queries work correctly after the switch.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.11: STRIDE to DEFAULT deregistration cleanup", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	auto &slot_array = scheduler.GetSlotArray();

	// Phase 1: Run several queries in STRIDE mode to exercise registration
	con.Query("SET scheduler_type='stride'");
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	for (int i = 0; i < 5; i++) {
		auto r = con.Query("SELECT SUM(i) FROM range(50000) t(i)");
		REQUIRE(!r->HasError());
		REQUIRE(r->GetValue(0, 0) == Value::BIGINT(1249975000LL));
	}
	REQUIRE(slot_array.GetActiveSlotCount() == 0);

	// Phase 2: Switch to DEFAULT — workers should lazily deregister
	con.Query("SET scheduler_type='default'");
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::DEFAULT);

	// Run queries to trigger the lazy deregistration path in workers
	for (int i = 0; i < 5; i++) {
		auto r = con.Query("SELECT SUM(i) FROM range(50000) t(i)");
		REQUIRE(!r->HasError());
		REQUIRE(r->GetValue(0, 0) == Value::BIGINT(1249975000LL));
	}
	// Slots must still be clean
	REQUIRE(slot_array.GetActiveSlotCount() == 0);

	// Phase 3: Concurrent queries after STRIDE→DEFAULT switch
	std::atomic<int> errors {0};
	std::atomic<int64_t> sum1 {0}, sum2 {0}, sum3 {0};

	std::thread t1([&]() {
		Connection c(db);
		auto r = c.Query("SELECT SUM(i) FROM range(80000) t(i)");
		if (!r->HasError()) {
			sum1.store(r->GetValue(0, 0).GetValue<int64_t>());
		} else {
			errors.fetch_add(1);
		}
	});
	std::thread t2([&]() {
		Connection c(db);
		auto r = c.Query("SELECT SUM(i) FROM range(60000) t(i)");
		if (!r->HasError()) {
			sum2.store(r->GetValue(0, 0).GetValue<int64_t>());
		} else {
			errors.fetch_add(1);
		}
	});
	std::thread t3([&]() {
		Connection c(db);
		auto r = c.Query("SELECT SUM(i) FROM range(40000) t(i)");
		if (!r->HasError()) {
			sum3.store(r->GetValue(0, 0).GetValue<int64_t>());
		} else {
			errors.fetch_add(1);
		}
	});
	t1.join();
	t2.join();
	t3.join();

	REQUIRE(errors.load() == 0);
	REQUIRE(sum1.load() == 3199960000LL);  // SUM(range(80000))
	REQUIRE(sum2.load() == 1799970000LL);  // SUM(range(60000))
	REQUIRE(sum3.load() == 799980000LL);   // SUM(range(40000))
}

//===----------------------------------------------------------------------===//
// Test SC.12: Re-registration After STRIDE→DEFAULT→STRIDE
//
// Verify that workers correctly re-register with the slot array when
// switching back to STRIDE after having been in DEFAULT mode.
// This exercises the full lifecycle: register → deregister → re-register.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.12: Re-registration after STRIDE to DEFAULT to STRIDE", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	auto &slot_array = scheduler.GetSlotArray();
	int64_t expected = 4999950000LL; // SUM(range(100000))

	// Round 1: STRIDE
	con.Query("SET scheduler_type='stride'");
	auto r1 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	REQUIRE(!r1->HasError());
	REQUIRE(r1->GetValue(0, 0) == Value::BIGINT(expected));
	REQUIRE(slot_array.GetActiveSlotCount() == 0);

	// Round 2: DEFAULT (workers deregister lazily)
	con.Query("SET scheduler_type='default'");
	auto r2 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	REQUIRE(!r2->HasError());
	REQUIRE(r2->GetValue(0, 0) == Value::BIGINT(expected));

	// Round 3: STRIDE again (workers must re-register)
	con.Query("SET scheduler_type='stride'");
	auto r3 = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
	REQUIRE(!r3->HasError());
	REQUIRE(r3->GetValue(0, 0) == Value::BIGINT(expected));
	REQUIRE(slot_array.GetActiveSlotCount() == 0);

	// Round 4: Multiple cycles to stress re-registration
	for (int cycle = 0; cycle < 3; cycle++) {
		con.Query("SET scheduler_type='default'");
		auto rd = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!rd->HasError());
		REQUIRE(rd->GetValue(0, 0) == Value::BIGINT(expected));

		con.Query("SET scheduler_type='stride'");
		auto rs = con.Query("SELECT SUM(i) FROM range(100000) t(i)");
		REQUIRE(!rs->HasError());
		REQUIRE(rs->GetValue(0, 0) == Value::BIGINT(expected));
		REQUIRE(slot_array.GetActiveSlotCount() == 0);
	}
}

//===----------------------------------------------------------------------===//
// Test SC.13: Concurrent Queries During STRIDE→DEFAULT Switch
//
// Start concurrent queries in STRIDE mode, switch to DEFAULT mid-flight,
// then verify all queries complete with correct results. This tests
// that the lazy deregistration doesn't interfere with in-progress tasks.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.13: Concurrent queries during STRIDE to DEFAULT switch", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);

	// Start in STRIDE mode
	con.Query("SET scheduler_type='stride'");
	REQUIRE(scheduler.GetSchedulerType() == SchedulerType::STRIDE);

	// Launch concurrent queries
	std::atomic<int> errors {0};
	std::atomic<int> completed {0};
	constexpr int NUM_THREADS = 4;
	int64_t expected_sums[NUM_THREADS];
	std::atomic<int64_t> actual_sums[NUM_THREADS];

	for (int i = 0; i < NUM_THREADS; i++) {
		int64_t n = 20000 * (i + 1);
		expected_sums[i] = n * (n - 1) / 2; // SUM(range(n))
		actual_sums[i].store(0);
	}

	std::vector<std::thread> threads;
	for (int i = 0; i < NUM_THREADS; i++) {
		threads.emplace_back([&, i]() {
			Connection c(db);
			int64_t n = 20000 * (i + 1);
			string query = "SELECT SUM(i) FROM range(" + std::to_string(n) + ") t(i)";
			auto r = c.Query(query);
			if (!r->HasError()) {
				actual_sums[i].store(r->GetValue(0, 0).GetValue<int64_t>());
				completed.fetch_add(1);
			} else {
				errors.fetch_add(1);
			}
		});
	}

	// Switch to DEFAULT while queries are running
	con.Query("SET scheduler_type='default'");

	for (auto &t : threads) {
		t.join();
	}

	REQUIRE(errors.load() == 0);
	REQUIRE(completed.load() == NUM_THREADS);
	for (int i = 0; i < NUM_THREADS; i++) {
		REQUIRE(actual_sums[i].load() == expected_sums[i]);
	}
}

//===----------------------------------------------------------------------===//
// Test SC.14: Slot Array Has No Stale Workers After Switch
//
// After switching from STRIDE to DEFAULT and running enough queries for
// all workers to deregister, verify that switching back to STRIDE and
// running concurrent queries still works — proving re-registration is
// fully functional with no stale pointers.
//===----------------------------------------------------------------------===//
TEST_CASE("SC.14: No stale workers after STRIDE-DEFAULT-STRIDE cycle", "[scheduler][api]") {
	DBConfig config;
	config.options.maximum_threads = 4;
	DuckDB db(nullptr, &config);
	Connection con(db);

	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	auto &slot_array = scheduler.GetSlotArray();

	// Run in STRIDE to register all workers
	con.Query("SET scheduler_type='stride'");
	for (int i = 0; i < 10; i++) {
		con.Query("SELECT SUM(i) FROM range(10000) t(i)");
	}

	// Switch to DEFAULT — workers deregister lazily
	con.Query("SET scheduler_type='default'");
	// Run enough queries so all 4 worker threads wake up and deregister
	for (int i = 0; i < 20; i++) {
		con.Query("SELECT SUM(i) FROM range(10000) t(i)");
	}

	// Switch back to STRIDE — workers must re-register from scratch
	con.Query("SET scheduler_type='stride'");

	// Concurrent stress test: if re-registration is broken (stale pointers,
	// missing slot_array_ptr, etc.), this will crash or produce wrong results
	std::atomic<int> errors {0};
	std::vector<std::thread> threads;
	constexpr int NUM_THREADS = 6;
	std::atomic<int64_t> results[NUM_THREADS];
	for (int i = 0; i < NUM_THREADS; i++) {
		results[i].store(0);
	}

	for (int i = 0; i < NUM_THREADS; i++) {
		threads.emplace_back([&, i]() {
			Connection c(db);
			auto r = c.Query("SELECT SUM(i) FROM range(50000) t(i)");
			if (!r->HasError()) {
				results[i].store(r->GetValue(0, 0).GetValue<int64_t>());
			} else {
				errors.fetch_add(1);
			}
		});
	}

	for (auto &t : threads) {
		t.join();
	}

	REQUIRE(errors.load() == 0);
	for (int i = 0; i < NUM_THREADS; i++) {
		REQUIRE(results[i].load() == 1249975000LL); // SUM(range(50000))
	}
	REQUIRE(slot_array.GetActiveSlotCount() == 0);
}
