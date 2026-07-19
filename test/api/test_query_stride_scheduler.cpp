#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/query_stride_optimizer.hpp"
#include "duckdb/parallel/query_stride_scheduler.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <cmath>
#include <thread>

using namespace duckdb;
using namespace std;

namespace {

static uint64_t CurrentNs() {
	return static_cast<uint64_t>(
	    chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch()).count());
}

static string MetadataQuery(uint64_t request_id, uint64_t template_id, const string &body) {
	auto deadline_ns = CurrentNs() + 60000000000ULL;
	return StringUtil::Format(
	    "/* duckdb_sla_v1 request_id=%llu template_id=%llu scale_factor=10 sla_tag=30 penalty_per_s=1 "
	    "deadline_ns=%llu */ %s",
	    static_cast<unsigned long long>(request_id), static_cast<unsigned long long>(template_id),
	    static_cast<unsigned long long>(deadline_ns), body);
}

} // namespace

TEST_CASE("STRIDE scheduler validates policy invariants and priority settings", "[api][stride]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FAIL(con.Query("SET scheduler_policy='invalid'"));
	REQUIRE_FAIL(con.Query("SET scheduler_policy='stride'"));
	REQUIRE_NO_FAIL(con.Query("SET query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='stride'"));
	REQUIRE_FAIL(con.Query("SET external_threads=1"));
	REQUIRE_FAIL(con.Query("SET stride_user_priority=0"));
	REQUIRE_FAIL(con.Query("SET stride_user_priority=-1"));
	REQUIRE_NO_FAIL(con.Query("SET stride_user_priority=2.5"));
	auto priority = con.Query("SELECT current_setting('stride_user_priority')::DOUBLE");
	REQUIRE(CHECK_COLUMN(priority, 0, {Value::DOUBLE(2.5)}));
	REQUIRE_NO_FAIL(con.Query("SET scheduler_policy='default'"));
}

TEST_CASE("STRIDE high-load restriction and tuning are deterministic", "[api][stride]") {
	REQUIRE(QueryStrideScheduler::ComputeEligibleWorkerCount(0, 16) == 16);
	REQUIRE(QueryStrideScheduler::ComputeEligibleWorkerCount(64, 16) == 16);
	REQUIRE(QueryStrideScheduler::ComputeEligibleWorkerCount(96, 16) == 8);
	REQUIRE(QueryStrideScheduler::ComputeEligibleWorkerCount(127, 16) == 1);
	REQUIRE(QueryStrideScheduler::ComputeEligibleWorkerCount(128, 16) == 1);

	duckdb::vector<QueryStrideTraceEntry> workload;
	QueryStrideTraceEntry short_query;
	short_query.window_quanta = 10;
	short_query.average_quantum_ms = 2;
	short_query.in_window_wall_time_ms = 20;
	short_query.initial_priority = QueryStrideScheduler::INITIAL_PRIORITY;
	short_query.minimum_priority = QueryStrideScheduler::MINIMUM_PRIORITY;
	workload.push_back(short_query);
	QueryStrideTraceEntry long_query;
	long_query.arrival_time_ms = 1;
	long_query.window_quanta = 50;
	long_query.average_quantum_ms = 2;
	long_query.in_window_wall_time_ms = 100;
	long_query.initial_priority = QueryStrideScheduler::INITIAL_PRIORITY;
	long_query.minimum_priority = QueryStrideScheduler::MINIMUM_PRIORITY;
	workload.push_back(long_query);
	QueryStrideTraceEntry static_query;
	static_query.arrival_time_ms = 2;
	static_query.window_quanta = 5;
	static_query.average_quantum_ms = 1;
	static_query.in_window_wall_time_ms = 5;
	static_query.initial_priority = 500;
	static_query.minimum_priority = 500;
	static_query.static_priority = true;
	workload.push_back(static_query);

	auto first = QueryStrideOptimizer::Optimize(workload, 4, QueryStrideScheduler::DEFAULT_DECAY_LAMBDA);
	auto second = QueryStrideOptimizer::Optimize(workload, 4, QueryStrideScheduler::DEFAULT_DECAY_LAMBDA);
	REQUIRE(first.decay_start == second.decay_start);
	REQUIRE(first.decay_lambda == second.decay_lambda);
	REQUIRE(first.objective == second.objective);
	REQUIRE(first.decay_start >= 0);
	REQUIRE(first.decay_lambda > 0);
	REQUIRE(first.decay_lambda <= 1);
	REQUIRE(std::isfinite(first.objective));
}

TEST_CASE("STRIDE preserves resource-group state across task-set transitions", "[api][stride]") {
	DuckDB db(nullptr);
	Connection setup(db);
	REQUIRE_NO_FAIL(setup.Query("SET threads=4"));
	REQUIRE_NO_FAIL(setup.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(setup.Query("CREATE TABLE stride_concurrent_t AS "
	                            "SELECT i, i % 1009 AS g FROM range(3000000) tbl(i)"));
	REQUIRE_NO_FAIL(setup.Query("SET GLOBAL query_worker_only_execution_enable=true"));
	REQUIRE_NO_FAIL(setup.Query("SET GLOBAL scheduler_policy='stride'"));

	Connection first(db);
	Connection second(db);
	for (auto connection : {&first, &second}) {
		REQUIRE_NO_FAIL(connection->Query("SET query_activation_scheduler_enable=true"));
		REQUIRE_NO_FAIL(connection->Query("SET query_admission_max_active=128"));
	}
	string first_error;
	string second_error;
	const string body = "SELECT g, count(*), sum(i) FROM stride_concurrent_t GROUP BY g ORDER BY g";
	thread first_thread([&]() {
		auto result = first.Query(MetadataQuery(9501, 95, body));
		if (result->HasError()) {
			first_error = result->GetError();
		}
	});
	thread second_thread([&]() {
		auto result = second.Query(MetadataQuery(9601, 96, body));
		if (result->HasError()) {
			second_error = result->GetError();
		}
	});
	first_thread.join();
	second_thread.join();
	REQUIRE(first_error.empty());
	REQUIRE(second_error.empty());

	auto &scheduler = db.instance->GetQueryStrideScheduler();
	REQUIRE(scheduler.ActiveQueryCount() == 0);
	REQUIRE(scheduler.GetSnapshot().empty());
	auto tuning = scheduler.GetTuningSnapshot();
	REQUIRE(tuning.phase == "tracking");
	REQUIRE(tuning.tracked_query_count == 2);
	REQUIRE(tuning.decay_start == QueryStrideScheduler::DEFAULT_DECAY_START);
	REQUIRE(tuning.decay_lambda == QueryStrideScheduler::DEFAULT_DECAY_LAMBDA);
	REQUIRE(tuning.registered_queries == 2);
	REQUIRE(tuning.unregistered_queries == 2);
	REQUIRE(tuning.change_task_sets == 2);
	REQUIRE(tuning.return_task_sets > 0);
	REQUIRE(tuning.finalization_task_sets > 0);
	REQUIRE(tuning.activated_task_sets == tuning.change_task_sets + tuning.return_task_sets);
	REQUIRE(tuning.finished_task_sets == tuning.activated_task_sets);

	auto counters = setup.Query(
	    "SELECT registered_queries, unregistered_queries, activated_task_sets, finished_task_sets, "
	    "change_task_sets, return_task_sets, finalization_task_sets "
	    "FROM duckdb_debug_query_stride_tuning()");
	REQUIRE(!counters->HasError());
	REQUIRE(counters->RowCount() == 1);
	REQUIRE(counters->GetValue(0, 0).GetValue<uint64_t>() == 2);
	REQUIRE(counters->GetValue(1, 0).GetValue<uint64_t>() == 2);
	REQUIRE(counters->GetValue(2, 0).GetValue<uint64_t>() == tuning.activated_task_sets);
	REQUIRE(counters->GetValue(3, 0).GetValue<uint64_t>() == tuning.finished_task_sets);
}
