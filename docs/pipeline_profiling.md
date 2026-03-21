# Pipeline-Level Profiling for EXPLAIN ANALYZE

## Overview

This document describes the pipeline-level profiling feature added to DuckDB's `EXPLAIN ANALYZE` output. Prior to this change, `EXPLAIN ANALYZE` only showed a tree of **operators** with per-operator timing and cardinality. It provided no visibility into **pipelines** — the execution units that group operators into source → intermediate → sink chains and determine parallelism.

This feature adds a **Pipeline Execution Summary** section to `EXPLAIN ANALYZE` output showing per-pipeline metrics:

- **CPU Time**: Total CPU time spent across all parallel tasks in the pipeline (sum of wall-clock time inside each `PipelineTask::ExecuteTask()` call).
- **Latency**: Wall-clock time from when the pipeline is scheduled to when it finishes (i.e., the elapsed real time).
- **Parallelism**: Number of parallel tasks launched for the pipeline.
- **Operator Composition**: Which operators (source, intermediates, sink) belong to each pipeline.

When a pipeline runs in parallel, CPU Time will typically exceed Latency (e.g., 4 threads each running for 0.2s yields ~0.8s CPU time but ~0.2s latency). For sequential pipelines (parallelism = 1), CPU Time will be slightly less than Latency — the difference represents scheduling overhead (task queuing, thread pickup, event system overhead).

Pipeline metrics are **always collected** whenever profiling is enabled — either via `EXPLAIN ANALYZE` or via `PRAGMA enable_profiling`. No additional configuration is required.

---

## Building DuckDB

### Prerequisites

- A C++11 (or later) compiler (GCC, Clang, or MSVC)
- CMake (version 3.14 or later)
- Python 3 (for code generation scripts)
- Ninja (optional, but recommended for faster builds)

**Important**: If you cloned from a fork, the version may show as `v0.0.1 (Unknown Version)` because git tags are missing. Fix this by fetching tags from the upstream repository:

```bash
git remote add upstream https://github.com/duckdb/duckdb.git  # if not already added
git fetch upstream --tags
```

Then rebuild so CMake picks up the correct version from `git describe --tags`.

### Debug Build (recommended for development)

```bash
cd duckdb

# Using Make (default generator)
make debug

# Or using Ninja for faster builds
GEN=ninja make debug
```

The debug binary will be at `build/debug/duckdb`.

### Release Build

```bash
cd duckdb
make release
# Or with Ninja:
GEN=ninja make release
```

The release binary will be at `build/release/duckdb`.

### Incremental Rebuilds

After making changes, rebuild from the build directory:

```bash
# If using Ninja:
cd build/debug
ninja -j$(nproc)

# If using Make:
cd build/debug
make -j$(nproc)
```

### Running Tests

```bash
cd build/debug

# Run all tests in a specific directory
./test/unittest "test/sql/explain/*"

# Run a specific test file
./test/unittest "test/sql/parallelism/intraquery/test_pipeline_profiling_parallelism.test"

# Run profiling-related tests
./test/unittest "test/sql/pragma/profiling/*"
```

---

## Using EXPLAIN ANALYZE

### Basic Usage

```sql
-- Text output (default)
EXPLAIN ANALYZE SELECT SUM(i) FROM range(1000000) tbl(i) GROUP BY i % 10;

-- JSON output (note: all options go inside a single set of parentheses)
EXPLAIN (ANALYZE, FORMAT JSON) SELECT SUM(i) FROM range(1000000) tbl(i) GROUP BY i % 10;
```

**Important**: The correct syntax is `EXPLAIN (ANALYZE, FORMAT JSON)`, not `EXPLAIN ANALYZE (FORMAT JSON)`. All options (ANALYZE, FORMAT) must be inside the same parentheses.

### Using PRAGMA enable_profiling

An alternative way to get profiling output is via the `PRAGMA enable_profiling` setting. This prints profiling information after every query, rather than requiring `EXPLAIN ANALYZE` on each one.

```sql
-- Enable profiling with text output (default)
PRAGMA enable_profiling;
SELECT SUM(i) FROM range(1000000) tbl(i);
-- Profiling output is printed to console after the query result

-- Enable profiling with JSON output
PRAGMA enable_profiling='json';
SELECT SUM(i) FROM range(1000000) tbl(i);
-- JSON profiling output is printed to console after the query result

-- Switch back to text output
PRAGMA enable_profiling='query_tree';

-- Disable profiling entirely
PRAGMA disable_profiling;
```

Supported format values for `PRAGMA enable_profiling`:
- `'query_tree'` — text tree format (default)
- `'json'` — JSON format
- `'query_tree_optimizer'` — text tree with optimizer phase timings
- `'no_output'` — collect metrics but don't print
- `'html'` — HTML format
- `'graphviz'` — Graphviz DOT format

### Pipeline Metrics Are Always Collected

Pipeline metrics (CPU time, latency, parallelism, operator composition) are collected automatically whenever profiling is enabled. There is no need to set `custom_profiling_settings` — the Pipeline Execution Summary section appears in the output by default.

The `PIPELINE_CPU_TIME`, `PIPELINE_LATENCY`, and `PIPELINE_PARALLELISM` entries in the metric type system (registered in `metric_type.json` with `is_default: false`) exist for integration with the custom profiling settings framework, but pipeline profile collection is gated only by whether the profiler itself is enabled (`QueryProfiler::IsEnabled()`), not by individual metric settings.

### Using with TPC-H Queries

First, load the TPC-H extension and generate data:

```sql
-- Load TPC-H extension
INSTALL tpch;
LOAD tpch;

-- Generate TPC-H data (scale factor 1 = ~1GB)
CALL dbgen(sf=1);

-- Run TPC-H queries with EXPLAIN ANALYZE
-- Q1: Pricing Summary Report
EXPLAIN ANALYZE
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Q3: Shipping Priority (multi-pipeline join query)
EXPLAIN ANALYZE
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;

-- Q9: Product Type Profit Measure (complex multi-join)
EXPLAIN ANALYZE
SELECT
    nation, o_year, sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        extract(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE
        s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
        AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
```

### JSON Output with TPC-H

```sql
-- JSON format for programmatic analysis
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT l_returnflag, l_linestatus, sum(l_quantity)
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus;
```

### Controlling Thread Count

```sql
-- Set number of threads for parallelism
PRAGMA threads=8;

-- Verify parallelism is being used
PRAGMA verify_parallelism;
```

---

## Example Output

### Text Format

```
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
...
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││         Pipeline Execution Summary           ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘

Pipeline 0 (Latency: 0.19s, CPU Time: 0.76s, Parallelism: 4)
  Source:     SEQ_SCAN
  Operators:  PROJECTION
  Sink:       HASH_GROUP_BY

Pipeline 1 (Latency: 0.00s, CPU Time: 0.00s, Parallelism: 1)
  Source:     HASH_GROUP_BY
  Operators:  (none)
  Sink:       RESULT_COLLECTOR
```

### JSON Format

```json
{
  "children": [ ... ],
  "pipelines": [
    {
      "pipeline_id": 0,
      "latency_seconds": 0.191878,
      "cpu_time_seconds": 0.759245,
      "parallelism": 4,
      "source": "SEQ_SCAN",
      "operators": ["PROJECTION"],
      "sink": "HASH_GROUP_BY"
    },
    {
      "pipeline_id": 1,
      "latency_seconds": 0.000748,
      "cpu_time_seconds": 0.000509,
      "parallelism": 1,
      "source": "HASH_GROUP_BY",
      "operators": [],
      "sink": "RESULT_COLLECTOR"
    }
  ]
}
```

### Understanding the Output

- **CPU Time > Latency** (parallel pipelines): Multiple threads contribute CPU time concurrently. For example, with parallelism=16 and latency=0.065s, a CPU time of 0.818s means ~12.6x parallel speedup.
- **CPU Time < Latency** (sequential pipelines): The gap represents scheduling overhead — time between `PipelineEvent::Schedule()` creating tasks and a thread actually picking up the task, plus event system overhead. This is normal and typically very small (microseconds).
- **Pipelines with zero values**: The `RESULT_COLLECTOR` and `EXPLAIN_ANALYZE` pipelines will show zeros because they execute after pipeline profiles are collected — `EXPLAIN ANALYZE` must render the output before those pipelines complete.

---

## Architecture and Design

### DuckDB's Pipeline Execution Model

DuckDB breaks query execution into **pipelines**. Each pipeline is a chain of operators:

```
Source → [Intermediate Operators] → Sink
```

For example, a `SELECT ... FROM t WHERE x > 10 GROUP BY y` query might produce:

- **Pipeline 0**: `SEQ_SCAN` → `FILTER` → `PROJECTION` → `HASH_GROUP_BY` (parallel, builds hash table)
- **Pipeline 1**: `HASH_GROUP_BY` → `RESULT_COLLECTOR` (sequential, scans hash table)

Pipeline boundaries are created at operators that need to materialize (like hash joins, aggregations, sorts). Each pipeline can run with multiple parallel tasks (threads).

### Pipeline Event Lifecycle

Each pipeline goes through a series of events during execution:

```
PipelineInitializeEvent  →  PipelineEvent  →  PipelineFinishEvent  →  PipelineCompleteEvent
     (shared)               (per-pipeline)      (may be shared)          (per-meta-pipeline)
```

Key observations that drove our measurement design:

1. **`PipelineInitializeEvent`** is shared across all pipelines in a `MetaPipeline`. It cannot be used for per-pipeline start times.
2. **`PipelineEvent`** is unique per pipeline — each pipeline gets its own. This is where we mark both the schedule start time and the finish time.
3. **`PipelineFinishEvent`** can be shared. Non-base pipelines may share the base pipeline's `PipelineFinishEvent` (e.g., in UNION queries) or share a finish group's event (e.g., IEJoin). Using it for end-time measurement would incorrectly overwrite another pipeline's latency.

### Where Metrics Are Measured

#### CPU Time

Measured directly inside `PipelineTask::ExecuteTask()` (`src/parallel/pipeline.cpp`). A `Profiler` timer starts before execution and stops on every exit path (NOT_FINISHED, BLOCKED, FINISHED). The elapsed time is atomically added to `PipelineProfile::cpu_time_seconds` via a compare-and-swap loop, since multiple tasks run concurrently for parallel pipelines.

```
Task 1: |---0.15s---|
Task 2:   |---0.20s---|        CPU Time = 0.15 + 0.20 + 0.18 + 0.22 = 0.75s
Task 3:  |--0.18s--|           Latency  = 0.25s (wall clock)
Task 4:    |----0.22s----|     Parallelism = 4
```

#### Latency

Measured between two events in the pipeline lifecycle:

- **Start**: `PipelineEvent::Schedule()` (`src/parallel/pipeline_event.cpp`) — calls `profile->MarkScheduleStart()` right before `pipeline->Schedule(event)`. This captures the moment the pipeline begins scheduling its tasks.
- **End**: `PipelineEvent::FinishEvent()` (`src/parallel/pipeline_event.cpp`) — calls `profile->MarkFinished()` when all tasks in the pipeline have completed. This is called by the event system after all tasks finish.

Both timestamps use `steady_clock` for monotonic, high-resolution timing. Latency is computed as the difference in seconds.

**Why not `PipelineFinishEvent` for end time?** There are three cases for how non-base pipelines relate to `PipelineFinishEvent`:

1. **Finish group** (IEJoin, AsOfJoin): The non-base pipeline shares another pipeline's `PipelineFinishEvent`. Marking end time there would overwrite the other pipeline's latency.
2. **Own finish event**: The non-base pipeline is a group leader and gets its own `PipelineFinishEvent`. This would work but is only one of three cases.
3. **No finish event** (UNION, default): The non-base pipeline shares the base pipeline's `PipelineFinishEvent`. Marking end time there would overwrite the base pipeline's latency.

Since `PipelineEvent` is always unique per pipeline, it is the correct and consistent place for both start and end measurements.

#### Parallelism

Set once when the pipeline schedules its tasks:

- `Pipeline::ScheduleSequentialTask()`: sets `profile->parallelism = 1`
- `Pipeline::LaunchScanTasks()`: sets `profile->parallelism = max_threads` (the actual number of tasks launched)

### Data Flow

The profiling data flows through the system as follows:

```
1. Executor::InitializeInternal()
   └─ Assigns pipeline IDs (0, 1, 2, ...)
   └─ Calls Pipeline::InitializeProfile() for each pipeline
      └─ Creates PipelineProfile with source/operator/sink names

2. During execution (concurrent)
   └─ PipelineEvent::Schedule() → MarkScheduleStart()
   └─ PipelineTask::ExecuteTask() → AddCPUTime() (atomic, per task)
   └─ Pipeline::LaunchScanTasks() / ScheduleSequentialTask() → sets parallelism
   └─ PipelineEvent::FinishEvent() → MarkFinished()

3. After execution
   └─ Executor::ExecuteTask() (when all pipelines done)
      └─ QueryProfiler::CollectPipelineProfiles()
         └─ Snapshots atomic values into non-atomic PipelineProfile copies
      └─ pipelines.clear()

4. Rendering
   └─ QueryProfiler::QueryTreeToStream() → text output with pipeline section
   └─ QueryProfiler::ToJSON() → JSON output with "pipelines" array
```

For `EXPLAIN ANALYZE` specifically, step 3 happens earlier — in `PhysicalExplainAnalyze::Finalize()` — because the profiler renders during query execution, before the normal `EndQuery` path.

### Thread Safety

- `PipelineProfile::cpu_time_seconds` is `atomic<double>`. Multiple tasks call `AddCPUTime()` concurrently using a compare-and-swap loop.
- `latency_seconds` and `parallelism` are not atomic — they are each written exactly once from a single thread (the event system thread).
- `CollectPipelineProfiles()` runs under the executor lock after all tasks have completed, so it safely reads all fields.

---

## Files Modified

| File | Description |
|------|-------------|
| `src/include/duckdb/parallel/pipeline_profile.hpp` | **New file.** Defines `PipelineProfile` struct with atomic CPU time, latency tracking, parallelism, and operator names. |
| `src/include/duckdb/parallel/pipeline.hpp` | Added `pipeline_id`, `profile` (unique_ptr), getters (`GetPipelineId`, `SetPipelineId`, `GetProfile`), and `InitializeProfile()` declaration. |
| `src/parallel/pipeline.cpp` | Instrumented `PipelineTask::ExecuteTask()` with per-task CPU time measurement. Added parallelism recording in `ScheduleSequentialTask()` and `LaunchScanTasks()`. Implemented `Pipeline::InitializeProfile()`. |
| `src/parallel/executor.cpp` | In `InitializeInternal()`: assigns sequential pipeline IDs and initializes profiles when profiling is enabled. In `ExecuteTask()`: collects pipeline profiles into `QueryProfiler` right before `pipelines.clear()`. |
| `src/include/duckdb/execution/executor.hpp` | Added `GetPipelines()` public method returning `const vector<shared_ptr<Pipeline>> &`. |
| `src/parallel/pipeline_event.cpp` | Added `MarkScheduleStart()` call in `Schedule()` and `MarkFinished()` call in `FinishEvent()` — the per-pipeline latency measurement points. |
| `src/parallel/pipeline_finish_event.cpp` | Removed incorrect `MarkFinished()` call (was causing double end-time measurement for base pipelines). |
| `src/include/duckdb/main/query_profiler.hpp` | Added `pipeline_profiles` vector, `CollectPipelineProfiles()` method, and `Pipeline` forward declaration. |
| `src/main/query_profiler.cpp` | Implemented `CollectPipelineProfiles()`. Added pipeline summary rendering in `QueryTreeToStream()` (text) and `ToJSON()` (JSON). |
| `src/execution/operator/helper/physical_explain_analyze.cpp` | Added `CollectPipelineProfiles()` call in `Finalize()` so EXPLAIN ANALYZE captures pipeline data before rendering. |
| `src/common/enums/metric_type.json` | Added `"pipeline"` metric group with `PIPELINE_CPU_TIME`, `PIPELINE_LATENCY`, and `PIPELINE_PARALLELISM`. |
| `src/include/duckdb/common/enums/metric_type.hpp` | Auto-generated: new `MetricType` enum entries and `MetricsUtils` functions for pipeline metrics. |
| `src/common/enums/metric_type.cpp` | Auto-generated: utility function implementations for pipeline metric group. |
| `src/main/profiling_utils.cpp` | Auto-generated: updated `QueryMetrics` collection methods. |
| `test/sql/parallelism/intraquery/test_pipeline_profiling_parallelism.test` | **New test.** Validates that pipeline profiling output contains the `"pipelines"` key and that parallelism > 1 is reported for parallel queries. |
| `test/sql/pragma/profiling/test_all_profiling_settings.test` | Auto-generated update: includes new pipeline metric types. |
| `test/sql/pragma/profiling/test_custom_profiling_using_groups.test` | Auto-generated update: includes new pipeline metric group. |

---

## Metric Type Registration

The three pipeline metrics are registered in the auto-generated metric system via `src/common/enums/metric_type.json`:

| Metric | Type | Description |
|--------|------|-------------|
| `PIPELINE_CPU_TIME` | `double` (seconds) | Sum of wall-clock execution time across all tasks in the pipeline. |
| `PIPELINE_LATENCY` | `double` (seconds) | Wall-clock time from `PipelineEvent::Schedule()` to `PipelineEvent::FinishEvent()`. |
| `PIPELINE_PARALLELISM` | `uint64` (absolute) | Number of parallel tasks launched (equals thread count for parallel pipelines, 1 for sequential). |

These are registered with `is_default: false` in the metric type system for integration with the `custom_profiling_settings` framework. However, pipeline profile collection and rendering is controlled independently — it is always active when the profiler is enabled (`QueryProfiler::IsEnabled()` returns true), which happens either via `EXPLAIN ANALYZE` or `PRAGMA enable_profiling`.
