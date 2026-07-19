# Native Scheduler Workload

`scheduler_workload` is the controlled native driver for comparing DuckDB's `default`, `stride`, and `sla` scheduling policies. It generates the mixed analytical workload described in Section 5.1 of *Self-Tuning Query Scheduling for Analytical Workloads* without Python threads, the Python GIL, pending-state polling, or a cursor-pool limit.

## Build

```bash
cmake -S . -B build/scheduler-workload \
  -DBUILD_SCHEDULER_WORKLOAD=ON \
  -DBUILD_UNITTESTS=ON \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build/scheduler-workload --target scheduler_workload scheduler_workload_tests -j
```

The executable is `build/scheduler-workload/tools/scheduler_workload/scheduler_workload`.

## Experimental Workflow

Calibrate every query template in isolation under the common execution environment:

```bash
scheduler_workload calibrate \
  --database 10=/data/tpch_sf10.duckdb \
  --database 100=/data/tpch_sf100.duckdb \
  --scale-weights 3,1 \
  --queries-dir /repo/workloads/tpch/queries/duckdb \
  --queries all \
  --threads 16 \
  --repetitions 3 \
  --output calibration.csv
```

Train policy-neutral profiles with `DEFAULT` and export the bounded scheduler model:

```bash
scheduler_workload train-profiles \
  --database 10=/data/tpch_sf10.duckdb \
  --database 100=/data/tpch_sf100.duckdb \
  --queries-dir /repo/workloads/tpch/queries/duckdb \
  --queries all \
  --threads 16 \
  --admission-cap 128 \
  --repetitions 3 \
  --profile-output profiles.bin
```

Generate one immutable Poisson schedule. The tool calculates `lambda = alpha / d`, where `d` is the probability-weighted isolated mean duration in `calibration.csv`:

```bash
scheduler_workload generate \
  --scale-factors 10,100 \
  --scale-weights 3,1 \
  --queries all \
  --calibration calibration.csv \
  --load-factor 0.95 \
  --duration-s 300 \
  --seed 301 \
  --sla-tag 30 \
  --output schedule.csv
```

Run that exact schedule once per scheduler:

```bash
for policy in default stride sla; do
  scheduler_workload run \
    --database 10=/data/tpch_sf10.duckdb \
    --database 100=/data/tpch_sf100.duckdb \
    --queries-dir /repo/workloads/tpch/queries/duckdb \
    --queries all \
    --threads 16 \
    --admission-cap 128 \
    --schedule schedule.csv \
    --scheduler "$policy" \
    --profile-input profiles.bin \
    --output-dir "results/$policy"
done
```

Each summary contains the schedule SHA-256. Comparisons are valid only when those hashes match and every run reports `"valid": true`.

## Common Environment

Every measured policy uses:

- the same prepared query text, typed request metadata, databases, deadlines, and arrival schedule;
- worker-only execution, zero external threads, one active pipeline per query, materialized results, and admission inside DuckDB;
- `query_admission_max_active=128` by default;
- query profiling enabled so bounded profiles continue adapting online;
- activation and admission debug tracing disabled;
- OS-controlled core and uncore frequency.

Only `scheduler_policy` changes between runs.

## Event-Driven Execution

Each scheduled request owns a precompiled `ClientContext` and prepared statement. A fixed submission pool starts pending queries. DuckDB emits admission and execution transitions into a blocking native queue. An `ADMITTED` event initializes a query that was waiting in the database admission queue; an `EXECUTION_READY` or `EXECUTION_ERROR` event lets a fixed completion pool finalize the materialized result. No thread polls pending query state.

The measured response time starts at the scheduled arrival, not at admission or execution start. It therefore includes driver lag, admission waiting, execution, lifecycle finalization, and result materialization.

## Validity Gates

The run exits nonzero after writing diagnostics when:

- any request fails or lacks a terminal result;
- p99 arrival-dispatch lag exceeds `--max-driver-lag-ms`;
- p99 database-submission lag exceeds that limit; or
- p99 admitted-to-execution-start lag exceeds that limit.

Use `--allow-invalid` only for diagnostic runs. Invalid runs must not be included in scheduler comparisons.

## Targeted SLA Diagnostics

`--scheduler-trace-queries` enables SLA epoch tracing only for selected templates. It is intended for diagnosing model
coverage, gain ordering, and worker-allocation decisions without enabling activation debug state for every request.

```bash
scheduler_workload run \
  --database 10=/data/tpch_sf10.duckdb \
  --database 100=/data/tpch_sf100.duckdb \
  --queries-dir /repo/workloads/tpch/queries/duckdb \
  --queries all \
  --threads 16 \
  --admission-cap 128 \
  --schedule schedule.csv \
  --scheduler sla \
  --profile-input profiles.bin \
  --scheduler-trace-queries Q2,Q9,Q16,Q18,Q21 \
  --output-dir results/sla_trace
```

The run writes `query_sla_scheduler_epochs.csv` in addition to the normal timeline and summary. The trace includes model
identity and validity, remaining work, demand, selected throughput and continuation levels, suffix-profile coverage,
planned and assigned workers, marginal mandatory and optional gains, predicted completion, and epoch-stage timings.

Tracing is diagnostic instrumentation. Use it to validate a scheduler change on a saved schedule, then replay the same
schedule without `--scheduler-trace-queries` for the reported performance measurement.

## Tests

```bash
build/scheduler-workload/tools/scheduler_workload/scheduler_workload_tests
build/scheduler-workload/test/unittest "[scheduler_workload]"
```

The tests cover deterministic schedules, the 3:1 scale distribution, schedule round trips, typed metadata, publish-once notifications, nonblocking admission wakeup, materialized completion, and profile snapshot round trips.
