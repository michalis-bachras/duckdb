# Point and Histogram for SLA and SLA-energy

`query_sla_suffix_model` and `--sla-suffix-model` accept `model_default`, `point`, and
`histogram`. The default resolves to Point for `sla_energy` and Histogram for `sla`.
Explicit selections are independent of option order; RESET restores policy-dependent
selection. SQL `current_setting` and run metadata report the resolved selection.
Changing the predictor while scheduler-managed queries are active is rejected.

Point is the corrected Stage predictor: `0.8 * running_mean + 0.2 * latest`, with an
all-history arithmetic mean for each exact `(template, scale, pipeline, structure)`
key. It is issued at activation and represented as one exact point mass in the
shared SLA evaluator. Worker counts, frequency qualification, shared-domain
reconciliation, and dynamic additions therefore use identical forecast semantics.
Point does not change continuation p90, lifecycle-tail p90, safe hardware throughput,
active-power EDP, shared frequency control, or residual parking.

Prepare-finish and finish decisions use their owning pipeline's suffix, including
when an intervening child pipeline ran. They remain serial. Point reuses the owner's
immutable issuance; Histogram refreshes its distribution. Reuse does not issue or
learn again. Observations use the final profiler lifecycle timestamp, including
inserted internal events, and are learned only at successful query completion.
Failures discard outstanding forecasts. Out-of-order completions retain exact-key
pairing. Measured Point and Histogram runs update only their selected predictor;
bootstrap updates both from the same ordered observations.

Execution signatures exclude cardinality estimates and retain native units and
execution modes. Structural suffix identity excludes those execution details.
Hardware and throughput profiles remain indexed by execution identity and, where
applicable, the exact core/uncore pair.

## Fresh training

This backport writes snapshot version 11. All older snapshot versions are rejected
with regeneration instructions. Historical artifacts must be retained unchanged.
Snapshots contain both predictors, structural identity, hardware profiles, and the
bootstrap target; transient issuance and feedback counters are not serialized.

```
scheduler_workload train-profiles --database 10=/path/tpch_sf10.duckdb \
  --queries-dir /path/queries --queries all --threads 16 --pin-threads on \
  --repetitions 15 --seed 17 --sla-suffix-bootstrap on \
  --sla-suffix-bootstrap-observations 15 --profile-output /new/bootstrap.bin \
  --output-dir /new/bootstrap-diagnostics
```

Strict bootstrap requires a fresh store, target 1–100 (default 15), and matching
repetitions. Each balanced round is deterministically shuffled. Incomplete exact-key
training cannot be exported. An untrained Point key fails explicitly rather than
switching predictors. On-policy warmup uses `train-profiles --scheduler sla` or
`--scheduler sla_energy`, an imported bootstrap, an explicit predictor, and a fresh
output snapshot. Hardware-control-off is only for correctness tests; it is not a
physical energy comparison.

Use `--hardware-profile-input` to import calibrated hardware pairs without importing
the exploration run's suffix observations or execution-time estimates. Each pilot
policy starts from the same suffix bootstrap plus the same hardware calibration,
then performs its own on-policy warmup.

The runner exports `query_request_suffix_models.csv`,
`query_request_suffix_model_runtime.csv`, and `run_metadata.json`. Epoch traces add
`suffix_model` and `suffix_generation`; existing exact-sample counts describe the
issued Point forecast. Successful measured SLA runs require matched issued/resolved
counts, no outstanding forecasts, and no missing-key or duplicate/unmatched feedback.

Driver validity uses scheduled arrival through submission handoff. Registration
wall time and thread CPU time are exported separately from driver lag.

## Validation scope

The separate source-branch finishing correction is commit `068a7e6b9b` and passed
34 isolated tests / 869 assertions. This selective backport does not import MultiQT,
STRIDE admission changes, or work-conserving residual allocation fixes. The existing
zero-gain work-conserving residual test can stall on this older branch; SLA-energy
always parks residual capacity. Keep that limitation explicit when reporting tests.
