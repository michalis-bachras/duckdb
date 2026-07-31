# Native Scheduler Workload

`scheduler_workload` is the controlled native driver for comparing DuckDB's `default`, `stride`, `sla`, and `sla_energy` scheduling policies. It generates the mixed analytical workload described in Section 5.1 of *Self-Tuning Query Scheduling for Analytical Workloads* without Python threads, the Python GIL, pending-state polling, or a cursor-pool limit.

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

## SLA-Energy Training And Evaluation

SLA-energy evaluation has two separate stages. Exploration is enabled only during profile training. A measured run imports the resulting exact hardware-pair profiles and disables exploration, so probes do not perturb the reported policy.

Start with the policy-neutral snapshot produced by `train-profiles`, then run a saved training schedule with direct hardware control and deterministic epoch-scoped exploration:

```bash
sudo scheduler_workload run \
  --database 10=/data/tpch_sf10.duckdb \
  --database 100=/data/tpch_sf100.duckdb \
  --queries-dir /repo/workloads/tpch/queries/duckdb \
  --queries all \
  --threads 16 \
  --admission-cap 128 \
  --schedule training_schedule.csv \
  --scheduler sla_energy \
  --profile-input profiles.bin \
	  --profile-output energy_profiles.bin \
	  --energy-power-model base_power_core_uncore_sweep.csv \
	  --energy-exploration on \
  --energy-exploration-seed 1 \
  --energy-hardware-control on \
  --energy-attribution on \
  --energy-attribution-period-ms 100 \
  --output-dir results/sla_energy_training
```

Training may be repeated by using `energy_profiles.bin` as both the input and output snapshot until the required pipeline/pair coverage is mature. Each exact pair requires at least eight stable throughput observations and eight stable power observations. Every warmup epoch selects at most one incomplete optional-only core pair and one incomplete optional-only socket pair. If no optional-only socket appears for four active epochs, one active socket is sampled as a training-only fallback. Core and uncore targets remain selected until the next epoch or until the domain loses its assignment. Unstable mixed-frequency or migrated segments are rejected rather than contaminating the profile.

SLA-energy uses a lexicographic objective and has no energy-weight parameter. Every epoch first computes the same positive-gain mandatory and optional token sequence as pure SLA; the energy planner may choose hardware and placement for those fixed tokens, but may not drop, add, or redirect them. Mandatory workers may use a lower-EDP hardware pair only when it preserves the maximum-frequency mandatory cost and fragility. Optional tokens are ordered only by the fragility reduction obtained at their fastest applicable reference pair. After a token is selected, its hardware pair is the minimum normalized-EDP pair that preserves the reference mandatory cost and fragility. Lifecycle events are the exception: they have no chunk-throughput unit, so their optional worker/no-worker transition comes directly from the SLA service model and runs at the maximum-reference hardware pair without EDP optimization. O1 uses an unused SMT slot on an active core without changing its installed pair for throughput-modeled work. O2 uses an empty core on an active socket while preserving the socket's uncore frequency, and O3 densely activates an inactive socket. If no EDP-qualified O1-O3 placement exists, the fixed token is placed at the maximum-reference pair. Between epochs, dynamic query selection uses the same pure-SLA mandatory and optional gains. Reusing an active core and socket is preferred for optional work, but an inactive domain is not an eligibility barrier; the maximum-reference pair is the safe fallback.

Evaluate on a different saved schedule and seed with exploration disabled:

```bash
sudo scheduler_workload run \
  --database 10=/data/tpch_sf10.duckdb \
  --database 100=/data/tpch_sf100.duckdb \
  --queries-dir /repo/workloads/tpch/queries/duckdb \
  --queries all \
  --threads 16 \
  --admission-cap 128 \
  --schedule evaluation_schedule.csv \
  --scheduler sla_energy \
	  --profile-input energy_profiles.bin \
	  --energy-power-model base_power_core_uncore_sweep.csv \
	  --energy-exploration off \
  --energy-hardware-control on \
  --energy-attribution on \
  --energy-attribution-period-ms 100 \
  --output-dir results/sla_energy_evaluation
```

Periodic RAPL attribution is enabled explicitly with `--energy-attribution on`, independently of the selected scheduler. Before exporting a snapshot, the driver synchronously stops attribution and flushes the final partial window. Attribution updates the in-memory scheduler profile store even when CSV export is disabled. Use `--energy-attribution-export on` to materialize aggregate files below `OUTPUT_DIR/energy_attribution`. Use `--energy-attribution-debug-export on` together with aggregate export only for attribution diagnosis; it additionally exports per-socket windows, attributed segments, process activity, and individual runtime-overhead samples.

Default and pure-SLA runs aggregate pipeline energy by logical `(query_id, pipeline_id)` identity. SLA-energy runs create hardware-specific aggregate rows only when a segment carries a stable explicit core/uncore target; those rows use the controlled target rather than sampled per-CPU frequency. Pipeline signatures are exported once in `period_pipeline_metadata.csv` instead of being repeated in every aggregate row. Query aggregates are always logical.

For a fair energy baseline, run the identical schedule and profile snapshot with `--scheduler sla`, the same `--energy-power-model`, and `--energy-attribution on`. The driver pins the common worker pool whenever attribution is enabled, so DEFAULT, STRIDE, SLA, and SLA-energy measurements use stable worker-to-hardware topology. Only SLA-energy enables direct frequency control; the other policies leave frequency selection to the configured OS governor.

The SLA-family schedulers park capacity after positive mandatory and optional gains are exhausted. The SLA-only scheduler can opt into zero-gain residual workers up to pipeline demand caps with `--sla-residual-policy work_conserving` for controlled ablations; SLA-energy always parks residual capacity. Epoch traces expose `residual_workers` separately from positive-gain `optional_workers`.

The scheduler epoch defaults to 200 ms. Use `--sla-epoch-ms N` for controlled responsiveness and overhead experiments; the value must be positive and applies only to SLA-family policies.

The driver exports `query_sla_energy_hardware.csv` before shutdown and `query_sla_energy_hardware_restoration.csv` after explicitly deactivating the energy policy. The latter reports exact MSR restoration attempts, successful readback verifications, and failures. A real hardware-controlled run is invalid if any restoration write or readback fails. Both upshifts and downshifts are applied synchronously and verified. Exploration diagnostics report core-probe epochs, optional-only socket-probe epochs, forced socket-probe epochs, and the current optional-only-socket miss count.

`--energy-hardware-control off` is a dry-run mode for unit and scheduler validation. It publishes and verifies logical targets without opening MSRs. Combining SLA-energy dry-run targets with `--energy-attribution on` is rejected because the logical target would not identify the physical frequency used by an attributed segment.

SLA-energy runs additionally export `query_request_pipeline_hardware_profiles.csv` and `query_sla_energy_hardware.csv`. The hardware summary separates worker-target calls, no-op updates, assignments, releases, retargets, core/uncore transitions, lock waiting, and synchronous hardware-application time. With targeted scheduler tracing enabled, runs also export `query_sla_energy_worker_epochs.csv` and the bounded debug-only `query_sla_energy_hardware_transitions.csv`. The transition trace links worker reservations to requested and applied frequency pairs and records the write and latency cost of each change. Reconciled hardware targets are published once per epoch, while a genuine between-epoch reassignment can update only the affected worker. Completing a task releases its scheduler lease without releasing its persistent hardware reservation. A pipeline completion places that reservation in a transition hold so its immediate successor can reuse the hardware pair; final query teardown, an epoch replacement, parking, or policy exit releases it. The epoch `hardware_apply_ns` column reports synchronous plan-publication time.

## Common Environment

Every measured policy uses:

- the same prepared query text, typed request metadata, databases, deadlines, and arrival schedule;
- worker-only execution, zero external threads, one active pipeline per query, materialized results, and admission inside DuckDB;
- `query_admission_max_active=128` by default;
- query profiling enabled so bounded profiles continue adapting online;
- activation and admission debug tracing disabled;
- pinned worker topology whenever energy attribution is enabled; DEFAULT, STRIDE, and SLA leave frequency control to the OS governor, while SLA-energy applies explicit MSR targets.

Only `scheduler_policy` changes between runs.

The OS-frequency statement applies to the `default`, `stride`, and pure `sla` comparison. `sla_energy` deliberately uses direct MSR control and must be reported as a separate hardware-aware experiment.

Use `--pin-threads auto|on|off` to control policy-neutral worker pinning. Energy attribution and SLA-energy promote
`auto` to `on` and reject an explicit `off`. To measure total process-attribution overhead, replay the same schedule
under DEFAULT twice with `--pin-threads on`: once with `--energy-attribution off`, and once with
`--energy-attribution on`. Keep debug export disabled for both measured runs. Enabling attribution selects the
authoritative process-hierarchical model and requires its collector to start successfully. This two-arm comparison
holds scheduler policy and worker topology fixed while measuring the complete process-attribution mechanism.

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
