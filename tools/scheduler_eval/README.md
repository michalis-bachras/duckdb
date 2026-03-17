# Scheduler Evaluation Client

Compares **STRIDE** vs **DEFAULT** scheduling in DuckDB using the evaluation
methodology from *"Self-Tuning Query Scheduling for Analytical Workloads"*
(SIGMOD '21, Section 5).

## Building

From the DuckDB root directory:

```bash
# Release build (recommended for evaluation)
BUILD_TPCH=1 BUILD_STRIDE_SCHEDULER=1 make release -j$(nproc)

# Binary location
./build/release/tools/scheduler_eval/scheduler_eval
```

For incremental rebuilds after editing `scheduler_eval.cpp`:

```bash
cmake --build build/release --target scheduler_eval -j$(nproc)
```

## Quick Start

```bash
# Full evaluation with defaults (both phases, SF3+SF30, alpha 0.8-1.0)
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb --output ./results

# Phase 1 only (calibration — measures isolation times)
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb --output ./results --phase1

# Phase 2 only (reuse existing database and isolation times)
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb --output ./results --phase2 --skip-dbgen
```

## Command-Line Options

| Option | Default | Description |
|---|---|---|
| `--phase1` | | Run calibration only |
| `--phase2` | | Run evaluation only |
| `--all` | (default) | Run both phases |
| `--db PATH` | `eval_scheduler.duckdb` | Database file path |
| `--output DIR` | `./eval_results` | Output directory for CSV files |
| `--skip-dbgen` | off | Skip TPC-H data generation (reuse existing DB) |
| `--alpha LIST` | `0.8,0.85,0.9,0.95,1.0` | Comma-separated load factors |
| `--queries N` | `200` | Measured queries per experiment run |
| `--warmup N` | `30` | Warmup queries per run (not counted in metrics) |
| `--reps N` | `3` | Repetitions per (scheduler, alpha) combination |
| `--threads N` | system default | DuckDB worker thread count |
| `--calibration-runs N` | `3` | Timed runs per query for isolation time (takes median) |

## Phases

### Phase 1: Sequential Calibration

Measures **isolation times** — the baseline latency of each TPC-H query
running alone with all workers dedicated to it.

For each combination of (scheduler_type, scale_factor, query):
1. One warmup run (discarded)
2. N timed runs (default: 3)
3. Median taken as the isolation time

These isolation times serve two purposes:
- **Denominator for relative slowdown**: `slowdown = wall_time / isolation_time`
- **Mean query duration** for computing arrival rate: `lambda = alpha / mean_dur`

The weighted mean query duration uses the 75/25 sampling ratio:
```
mean_dur = 0.75 * mean(SF3 times) + 0.25 * mean(SF30 times)
```

### Phase 2: Mixed Workload Evaluation

Generates concurrent workloads following the paper's methodology:

1. **Query sampling**: TPC-H Q1-Q22, uniform random selection
2. **Scale factor**: 75% SF3, 25% SF30 (paper Section 5)
3. **Arrivals**: Exponential inter-arrival times with rate `lambda = alpha / mean_query_duration`
4. **Warmup**: First N queries (default: 30) allow the self-tuner to converge
   (`t_t=20s` tracking + `t_r=60s` refresh). Only subsequent queries are measured.
5. **Execution**: Each arrival spawns a thread with its own Connection

The same workload (same random seed) is replayed under both STRIDE and DEFAULT
for a fair comparison.

#### Experiment Matrix

Default configuration runs:
```
2 schedulers x 5 alphas x 3 seeds = 30 experiment runs
```

## Output Files

All CSV files are written to the `--output` directory.

### `isolation_times.csv`

Per-query isolation times from Phase 1.

```
scheduler_type,query_num,scale_factor,duration_sec
DEFAULT,1,3,0.160114
DEFAULT,2,3,0.032186
STRIDE,1,3,0.218032
...
```

### `query_log.csv`

Per-query measurements from Phase 2. One row per measured query (warmup excluded).

```
scheduler_type,alpha,seed,event_id,query_num,scale_factor,arrival_time,wall_time_sec,isolation_time_sec,relative_slowdown
DEFAULT,0.80,42,0,15,3,1.699624,0.056784,0.048745,1.164913
...
```

| Column | Description |
|---|---|
| `event_id` | Sequential ID within measured queries (0-based) |
| `arrival_time` | Scheduled arrival time (seconds from experiment start) |
| `wall_time_sec` | Actual query execution time |
| `isolation_time_sec` | Baseline time (from Phase 1, same scheduler+query+SF) |
| `relative_slowdown` | `wall_time_sec / isolation_time_sec` (1.0 = no contention) |

### `summary.csv`

Aggregated metrics per experiment run.

```
scheduler_type,alpha,seed,mean_relative_slowdown,p50_slowdown,p95_slowdown,p99_slowdown,throughput_qps,total_wall_sec,n_queries
DEFAULT,0.80,42,1.366371,1.295840,1.845180,1.980705,2.2089,4.53,10
...
```

| Column | Description |
|---|---|
| `mean_relative_slowdown` | Paper's cost metric: `(1/N) * sum(wall_time / isolation_time)` |
| `p50/p95/p99_slowdown` | Percentiles of relative slowdown distribution |
| `throughput_qps` | `n_queries / total_wall_sec` |
| `total_wall_sec` | End-to-end wall time for the entire run |

## Interpreting Results

### Key Metric: Mean Relative Slowdown

From the paper's Equation (1): `f_W(P) = (1/|W|) * sum(P_W(q) / L_B(q))`

- A value of **1.0** means queries run as fast as in isolation (no contention impact)
- A value of **2.0** means queries take 2x longer than isolation on average
- **Lower is better**

### What to Expect

| Load Factor (alpha) | Expected Behavior |
|---|---|
| 0.80 | Light load. Few concurrent queries. Both schedulers perform similarly. |
| 0.85-0.90 | Moderate load. STRIDE should start showing fairness benefits for short queries. |
| 0.95 | High load. Temporary oversubscription occurs. STRIDE's decay mechanism should reduce slowdown for short queries. |
| 1.00 | Near-saturation. Maximum scheduling impact. STRIDE's advantage should be most visible in P95/P99 tail latencies. |

The stride scheduler's advantage is most visible:
- At **high load factors** (alpha >= 0.9) where contention is significant
- In **tail latencies** (P95, P99) where short queries benefit from priority decay
- With **mixed workloads** (SF3 + SF30 creates high variance in query duration)

### Console Output

The tool prints a comparison table at the end:

```
=== Aggregated (mean across seeds) ===
Scheduler   Alpha | MeanSlowD P50       P95       P99       | QPS
---------- ------ | --------- --------- --------- --------- | ---------
DEFAULT      0.80 |     1.366     1.296     1.845     1.981 |      2.21
DEFAULT      0.90 |     1.823     1.562     2.901     3.445 |      2.85
STRIDE       0.80 |     1.467     1.297     2.385     2.816 |      1.58
STRIDE       0.90 |     1.654     1.412     2.534     2.901 |      2.12
```

## Example Workflows

### Minimal Test Run (verify setup)

```bash
./build/release/tools/scheduler_eval/scheduler_eval \
  --db /tmp/test_eval.duckdb \
  --output /tmp/test_results \
  --alpha 0.8 \
  --queries 10 \
  --warmup 3 \
  --reps 1 \
  --calibration-runs 1 \
  --threads 4
```

Takes ~10 minutes (mostly SF30 data generation on first run).

### Standard Evaluation (paper methodology)

```bash
# First run: generate data + calibrate + evaluate
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb \
  --output ./results \
  --alpha 0.8,0.85,0.9,0.95,1.0 \
  --queries 200 \
  --warmup 30 \
  --reps 3

# Subsequent runs: skip data generation
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb \
  --output ./results_v2 \
  --skip-dbgen \
  --alpha 0.8,0.85,0.9,0.95,1.0 \
  --queries 200 \
  --warmup 30 \
  --reps 3
```

### Extended Evaluation (more statistical power)

```bash
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb \
  --output ./results_extended \
  --skip-dbgen \
  --alpha 0.8,0.85,0.9,0.95,1.0 \
  --queries 500 \
  --warmup 50 \
  --reps 5 \
  --calibration-runs 5
```

### Single Load Factor Deep Dive

```bash
./build/release/tools/scheduler_eval/scheduler_eval \
  --db eval.duckdb \
  --output ./results_alpha095 \
  --skip-dbgen \
  --alpha 0.95 \
  --queries 300 \
  --warmup 40 \
  --reps 5
```

## Self-Tuning Optimizer Interaction

The STRIDE scheduler's self-tuning optimizer has:
- **Tracking duration** `t_t = 20s`: collects workload statistics
- **Refresh duration** `t_r = 60s`: time between optimization rounds

The `--warmup` parameter (default: 30 queries) ensures the optimizer completes
at least one tracking+optimization cycle before measurement begins. For accurate
results at low alpha values (where queries are spaced further apart), consider
increasing warmup to ensure the optimizer has seen enough queries.

## Hardware Considerations

- **Memory**: SF30 generates ~30GB of data. The database is stored on disk,
  but DuckDB's buffer manager benefits from available RAM. Recommend >= 32GB.
- **Threads**: The `--threads` option controls DuckDB's worker thread count.
  Leave at default (system thread count) for production evaluation.
  Use `--threads 4` for quick tests on large machines.
- **Disk**: First run creates a persistent database (~35GB for SF3+SF30).
  Use `--skip-dbgen` on subsequent runs to avoid regeneration.
- **Duration**: Full evaluation (~30 runs at 200 queries each) takes
  approximately 30-60 minutes depending on hardware, after initial data
  generation.
