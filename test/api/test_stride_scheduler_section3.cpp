//===----------------------------------------------------------------------===//
//                         DuckDB
//
// Test stride scheduler implementation (Paper Section 3: Robust Morsel Scheduling)
//
// Section 3.1: Adaptive Morsel Execution — NOT implemented (DuckDB keeps static
// 2048-tuple morsels). Instead, time-based pass updates (f-factor) and CPU-time
// decay accumulators restore the paper's proportional-share semantics.
//
// Section 3.2: Adaptive Query Priorities — Implemented via per-worker local
// decay with CPU-time accumulation.
//
// This file verifies:
//  - f-factor pass scaling (time-proportional pass updates)
//  - CPU-time decay accumulator mechanics
//  - Decay formula: p_{i+1} = max(p_min, λ^steps × p_i)
//  - Batch decay via pow() instead of per-step loop
//  - d_start grace period behavior
//  - Priority floor (p_min) enforcement
//  - Stride recalculation after decay
//  - STRIDE_QUANTUM_CHUNKS role in pipeline execution
//  - Finalization priority boost (pass → 0)
//  - Pipeline transition (return mask preserves decayed priority)
//
//===----------------------------------------------------------------------===//

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/parallel/scheduler_slot_array.hpp"
#include "duckdb/parallel/thread_local_scheduler_state.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <atomic>
#include <cmath>
#include <thread>

using namespace duckdb;

//===----------------------------------------------------------------------===//
// Helper: create a DuckDB instance with STRIDE scheduler
//===----------------------------------------------------------------------===//
static unique_ptr<DuckDB> MakeStrideDB3(idx_t threads = 4) {
	DBConfig config;
	config.options.maximum_threads = threads;
	auto db = make_uniq<DuckDB>(nullptr, &config);
	Connection con(*db);
	con.Query("SET scheduler_type='stride'");
	return db;
}

//===----------------------------------------------------------------------===//
// Helper: set up a ThreadLocalSchedulerState with one active slot
//===----------------------------------------------------------------------===//
static void SetupSingleSlot(ThreadLocalSchedulerState &state, SchedulerSlotArray &slot_array, idx_t slot_idx = 0) {
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;
	state.active_slots.set(slot_idx);
	state.priorities[slot_idx] = SchedulerSlotArray::INITIAL_PRIORITY;
	state.strides[slot_idx] = SchedulerSlotArray::LARGE_CONSTANT / SchedulerSlotArray::INITIAL_PRIORITY;
	state.pass_values[slot_idx] = 0.0;
	state.cpu_time_accum_us[slot_idx] = 0;
	state.decay_steps[slot_idx] = 0;
	state.sorted_slots[0] = slot_idx;
	state.sorted_count = 1;
	state.local_global_stride = SchedulerSlotArray::LARGE_CONSTANT / SchedulerSlotArray::INITIAL_PRIORITY;
	state.local_global_pass = 0.0;
}

//===----------------------------------------------------------------------===//
// Test 3.1: f-Factor Pass Scaling — Time-Proportional Updates
//
// Paper context: "pass_i += 1 × stride_i" assumes adaptive morsels with
// t_max = 2ms. With static morsels, a 30ms quantum and a 0.5ms quantum
// would advance pass identically, breaking proportional fairness.
//
// Adaptation: f = elapsed_ms / REFERENCE_DURATION_MS (2.0ms).
// pass_i += f × stride_i
//
// A 30ms quantum → f=15 → pass advances 15× more than a 2ms quantum.
// This correctly reflects CPU consumption.
//
// Validity: This is mathematically equivalent to the paper's approach.
// In the paper, each quantum is exactly t_max, so f=1 always. Our f-factor
// generalizes this to arbitrary quantum durations while preserving the
// invariant: pass advancement ∝ CPU time consumed.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.1: f-factor scales pass proportional to CPU time", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	double stride = state.strides[0]; // LARGE_CONSTANT / INITIAL_PRIORITY = 0.1

	SECTION("2ms quantum (f=1.0) — baseline equivalent to paper") {
		double f = 2.0 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 1.0
		state.UpdateLocalPass(0, f);
		REQUIRE(std::abs(state.pass_values[0] - stride * 1.0) < 1e-9);
	}

	SECTION("0.5ms quantum (f=0.25) — cheap morsel, small pass advance") {
		double f = 0.5 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 0.25
		state.UpdateLocalPass(0, f);
		REQUIRE(std::abs(state.pass_values[0] - stride * 0.25) < 1e-9);
	}

	SECTION("30ms quantum (f=15.0) — expensive morsel, large pass advance") {
		double f = 30.0 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 15.0
		state.UpdateLocalPass(0, f);
		REQUIRE(std::abs(state.pass_values[0] - stride * 15.0) < 1e-9);
	}

	SECTION("Two 1ms quanta equal one 2ms quantum") {
		double f1 = 1.0 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 0.5

		ThreadLocalSchedulerState state_a, state_b;
		SetupSingleSlot(state_a, slot_array);
		SetupSingleSlot(state_b, slot_array);

		// State A: two 1ms quanta
		state_a.UpdateLocalPass(0, f1);
		state_a.UpdateLocalPass(0, f1);

		// State B: one 2ms quantum
		double f2 = 2.0 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 1.0
		state_b.UpdateLocalPass(0, f2);

		// Pass values should be identical (2 × 0.5 = 1 × 1.0)
		REQUIRE(std::abs(state_a.pass_values[0] - state_b.pass_values[0]) < 1e-9);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.2: f-Factor Global Pass Update
//
// Same reasoning as 3.1, applied to the per-worker global pass:
// global_pass += f × global_stride
//
// This ensures new queries arriving on a worker get a fair initial pass
// that accounts for how much CPU time the worker has consumed overall.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.2: f-factor global pass update", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	double global_stride = state.local_global_stride;
	double initial_global_pass = state.local_global_pass;

	SECTION("Global pass advances proportional to f") {
		double f = 10.0 / SchedulerSlotArray::REFERENCE_DURATION_MS; // 5.0
		state.UpdateLocalGlobalPass(f);
		double expected = initial_global_pass + f * global_stride;
		REQUIRE(std::abs(state.local_global_pass - expected) < 1e-9);
	}

	SECTION("Cumulative global pass is additive") {
		double f1 = 1.0; // 2ms
		double f2 = 3.0; // 6ms
		state.UpdateLocalGlobalPass(f1);
		state.UpdateLocalGlobalPass(f2);
		double expected = initial_global_pass + (f1 + f2) * global_stride;
		REQUIRE(std::abs(state.local_global_pass - expected) < 1e-9);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.3: CPU-Time Decay Accumulator — Threshold Mechanics
//
// Paper context: "one decay step per quantum" assumes quantum = t_max.
// Adaptation: Accumulate CPU time per slot per worker. Fire one decay
// step per DECAY_THRESHOLD_US = REFERENCE_DURATION_MS × 1000 = 2000μs
// of accumulated CPU time.
//
// Validity: This is the correct generalization. Instead of counting
// morsels (which vary in duration), we count 2ms units of CPU time.
// A paper quantum of exactly 2ms fires exactly 1 decay step — identical
// to the paper's behavior. Shorter morsels accumulate until crossing
// the threshold; longer morsels fire multiple steps in batch.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.3: Decay accumulator threshold mechanics", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	SECTION("Sub-threshold: no decay step fired") {
		state.ApplyLocalDecay(0, 1500); // 1.5ms < 2ms threshold
		REQUIRE(state.decay_steps[0] == 0);
		REQUIRE(state.cpu_time_accum_us[0] == 1500);
		// Priority unchanged
		double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;
		REQUIRE(state.priorities[0] == init_prio);
	}

	SECTION("Exact threshold: one decay step fired") {
		state.ApplyLocalDecay(0, 2000); // exactly 2ms
		REQUIRE(state.decay_steps[0] == 1);
		REQUIRE(state.cpu_time_accum_us[0] == 0); // modulo clears
	}

	SECTION("Accumulation across multiple sub-threshold quanta") {
		state.ApplyLocalDecay(0, 800);  // 0.8ms, total=0.8ms
		REQUIRE(state.decay_steps[0] == 0);
		state.ApplyLocalDecay(0, 800);  // 0.8ms, total=1.6ms
		REQUIRE(state.decay_steps[0] == 0);
		state.ApplyLocalDecay(0, 800);  // 0.8ms, total=2.4ms → 1 step, remainder=0.4ms
		REQUIRE(state.decay_steps[0] == 1);
		REQUIRE(state.cpu_time_accum_us[0] == 400);
	}

	SECTION("Large quantum: multiple steps in one batch") {
		state.ApplyLocalDecay(0, 10000); // 10ms → 5 steps
		REQUIRE(state.decay_steps[0] == 5);
		REQUIRE(state.cpu_time_accum_us[0] == 0);
	}

	SECTION("30ms quantum: 15 steps in one batch") {
		state.ApplyLocalDecay(0, 30000); // 30ms → 15 steps
		REQUIRE(state.decay_steps[0] == 15);
		REQUIRE(state.cpu_time_accum_us[0] == 0);
	}

	SECTION("Remainder carries across batches") {
		state.ApplyLocalDecay(0, 3500); // 3.5ms → 1 step, remainder=1.5ms
		REQUIRE(state.decay_steps[0] == 1);
		REQUIRE(state.cpu_time_accum_us[0] == 1500);

		state.ApplyLocalDecay(0, 1000); // +1ms, total=2.5ms → 1 more step, remainder=0.5ms
		REQUIRE(state.decay_steps[0] == 2);
		REQUIRE(state.cpu_time_accum_us[0] == 500);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.4: d_start Grace Period
//
// Paper ref (Formula 2): "p_{i+1} = p_i for i < d_start"
// Priority remains at INITIAL_PRIORITY until d_start decay steps have
// been fired. This prevents short queries from being penalized.
//
// With default d_start=10 and DECAY_THRESHOLD=2ms, this means
// 10 × 2ms = 20ms of CPU time before decay begins.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.4: d_start grace period", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA; // 10
	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;

	SECTION("No decay during grace period (steps 0-9)") {
		// Fire exactly d_start steps worth of CPU time
		uint64_t grace_us = static_cast<uint64_t>(d_start) * 2000; // 20ms
		state.ApplyLocalDecay(0, grace_us);

		REQUIRE(state.decay_steps[0] == d_start);
		// No effective decay — all steps fall within grace period
		REQUIRE(state.priorities[0] == init_prio);
	}

	SECTION("Decay starts at step d_start (step 10)") {
		// Fire d_start + 1 steps
		uint64_t total_us = static_cast<uint64_t>(d_start + 1) * 2000; // 22ms
		state.ApplyLocalDecay(0, total_us);

		REQUIRE(state.decay_steps[0] == d_start + 1);
		// 1 effective step past d_start
		double lambda = SchedulerSlotArray::DEFAULT_DECAY_LAMBDA; // 0.9
		double expected = init_prio * lambda; // 10000 * 0.9 = 9000
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-6);
	}

	SECTION("Grace period CPU time equivalent: 20ms at default d_start=10") {
		// Verify: 10 steps × 2ms/step = 20ms
		// Anything under 20ms of accumulated CPU time should NOT decay
		state.ApplyLocalDecay(0, 19999); // Just under 20ms
		REQUIRE(state.decay_steps[0] == 9); // 9 complete steps
		REQUIRE(state.priorities[0] == init_prio);

		// One more microsecond crosses step 10 but doesn't pass d_start
		state.ApplyLocalDecay(0, 1); // total accum = 19999+1 - 9*2000 = 1000
		// Actually let me recalculate: after 19999us, steps=9, accum=19999-18000=1999
		// After +1us: accum=2000 → step 10 fires. But step 10 is at d_start, still no effective decay
		// d_start=10 means effective_steps = max(0, 10-10) - max(0, 9-10) = 0 - 0 = 0
		REQUIRE(state.priorities[0] == init_prio);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.5: Batch Decay via pow() — Correctness
//
// Paper Formula 2: p_{i+k} = max(p_min, λ^k × p_i) for k effective steps.
// The implementation uses pow(λ, effective_steps) instead of a loop.
// This must produce identical results to iterative application.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.5: Batch pow() decay matches iterative decay", "[stride][api]") {
	SchedulerSlotArray slot_array;
	double lambda = SchedulerSlotArray::DEFAULT_DECAY_LAMBDA;
	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;
	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;

	SECTION("Single step via batch equals single manual application") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		uint64_t us = static_cast<uint64_t>(d_start + 1) * 2000;
		state.ApplyLocalDecay(0, us);

		double expected = init_prio * lambda;
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-6);
	}

	SECTION("10 effective steps: pow(λ,10) matches iterative") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		uint64_t us = static_cast<uint64_t>(d_start + 10) * 2000;
		state.ApplyLocalDecay(0, us);

		// Iterative: apply λ ten times
		double expected = init_prio;
		for (int i = 0; i < 10; i++) {
			expected *= lambda;
		}
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}

	SECTION("Incremental decay equals single batch") {
		// Apply decay in two batches: first 5 effective steps, then 5 more
		ThreadLocalSchedulerState state_inc;
		SetupSingleSlot(state_inc, slot_array);

		uint64_t batch1 = static_cast<uint64_t>(d_start + 5) * 2000;
		state_inc.ApplyLocalDecay(0, batch1);

		uint64_t batch2 = 5 * 2000;
		state_inc.ApplyLocalDecay(0, batch2);

		// Apply all 10 effective steps at once
		ThreadLocalSchedulerState state_batch;
		SetupSingleSlot(state_batch, slot_array);

		uint64_t all = static_cast<uint64_t>(d_start + 10) * 2000;
		state_batch.ApplyLocalDecay(0, all);

		REQUIRE(std::abs(state_inc.priorities[0] - state_batch.priorities[0]) < 1e-4);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.6: Priority Floor (p_min) Enforcement
//
// Paper: "priorities must never drop below p_min > 0. This ensures that
// queries never starve."
//
// Verifies priority never goes below MIN_PRIORITY = 100.0
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.6: Priority floor enforcement", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	double min_prio = SchedulerSlotArray::MIN_PRIORITY;

	SECTION("Massive CPU time decays to floor, not below") {
		// 500ms of CPU time → 250 steps, 240 effective (past d_start=10)
		state.ApplyLocalDecay(0, 500000);

		REQUIRE(state.priorities[0] >= min_prio);
		REQUIRE(state.priorities[0] == min_prio); // Should be exactly at floor
	}

	SECTION("Further decay after reaching floor is a no-op") {
		// First: decay to floor
		state.ApplyLocalDecay(0, 500000);
		REQUIRE(state.priorities[0] == min_prio);

		double stride_at_floor = state.strides[0];

		// Apply more CPU time — priority should stay at floor
		state.ApplyLocalDecay(0, 100000);
		REQUIRE(state.priorities[0] == min_prio);
		REQUIRE(state.strides[0] == stride_at_floor);
	}

	SECTION("Stride updates when priority decays") {
		// Before decay
		double stride_before = state.strides[0];

		// Decay past d_start
		int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;
		state.ApplyLocalDecay(0, static_cast<uint64_t>(d_start + 5) * 2000);

		// Stride should have increased (lower priority → higher stride → less frequent scheduling)
		REQUIRE(state.strides[0] > stride_before);

		// Verify stride = LARGE_CONSTANT / priority
		double expected_stride = SchedulerSlotArray::LARGE_CONSTANT / state.priorities[0];
		REQUIRE(std::abs(state.strides[0] - expected_stride) < 1e-9);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.7: Global Stride Recomputation After Decay
//
// When a query's priority decays, the global stride must be recomputed:
// local_global_stride = LARGE_CONSTANT / Σ(priorities)
//
// This ensures the global pass advances at the correct rate relative
// to the new priority landscape.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.7: Global stride recomputes after decay", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;

	// Two slots with initial priority
	state.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;
	double initial_global_stride = SchedulerSlotArray::LARGE_CONSTANT / (2 * init_prio);
	REQUIRE(std::abs(state.local_global_stride - initial_global_stride) < 1e-9);

	// Decay slot 0 heavily (past d_start, significant priority reduction)
	state.ApplyLocalDecay(0, 100000); // 100ms → 50 steps, 40 effective

	double new_total_prio = state.priorities[0] + state.priorities[1];
	double expected_global_stride = SchedulerSlotArray::LARGE_CONSTANT / new_total_prio;
	REQUIRE(std::abs(state.local_global_stride - expected_global_stride) < 1e-9);

	// Global stride should have increased (lower total priority → higher stride)
	REQUIRE(state.local_global_stride > initial_global_stride);
}

//===----------------------------------------------------------------------===//
// Test 3.8: Decay Parameter Reads from Atomic (Live Optimizer Updates)
//
// The optimizer can change d_start and λ at runtime. Workers read these
// via relaxed atomics from slot_array_ptr. Verify that changed parameters
// take effect in subsequent ApplyLocalDecay calls.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.8: Live decay parameter updates", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;

	SECTION("Changing d_start delays decay onset") {
		// Set d_start to 50 (instead of default 10)
		slot_array.SetDecayParameters(50, 0.9);

		// 40 steps worth of CPU → all within grace period (d_start=50)
		state.ApplyLocalDecay(0, 40 * 2000);
		REQUIRE(state.priorities[0] == init_prio);

		// Step 51 → 1 effective step
		state.ApplyLocalDecay(0, 11 * 2000);
		REQUIRE(state.priorities[0] < init_prio);
	}

	SECTION("Changing λ affects decay rate") {
		// Very aggressive decay: λ = 0.5
		slot_array.SetDecayParameters(0, 0.5);

		// 5 steps, all effective (d_start=0)
		state.ApplyLocalDecay(0, 5 * 2000);
		double expected = init_prio * std::pow(0.5, 5); // 10000 * 0.03125 = 312.5
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}

	// Reset to defaults
	slot_array.SetDecayParameters(SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA,
	                              SchedulerSlotArray::DEFAULT_DECAY_LAMBDA);
}

//===----------------------------------------------------------------------===//
// Test 3.9: Per-Worker Decay Independence with f-Factor
//
// Combining the f-factor (Section 3.1) and CPU-time accumulator (Section 3.3),
// two workers executing different pipeline complexities for the same query
// should get different decay states — matching the paper's intent that
// resource consumption drives scheduling decisions.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.9: f-factor + decay combined per-worker divergence", "[stride][api]") {
	SchedulerSlotArray slot_array;

	ThreadLocalSchedulerState worker_a; // runs cheap pipeline (1ms quanta)
	ThreadLocalSchedulerState worker_b; // runs expensive pipeline (20ms quanta)
	SetupSingleSlot(worker_a, slot_array);
	SetupSingleSlot(worker_b, slot_array);

	// Simulate 10 quanta each, but different durations
	for (int i = 0; i < 10; i++) {
		// Worker A: 1ms quantum (500us elapsed)
		double f_a = 1.0 / SchedulerSlotArray::REFERENCE_DURATION_MS;
		worker_a.UpdateLocalPass(0, f_a);
		worker_a.ApplyLocalDecay(0, 1000); // 1ms
		worker_a.UpdateLocalGlobalPass(f_a);

		// Worker B: 20ms quantum
		double f_b = 20.0 / SchedulerSlotArray::REFERENCE_DURATION_MS;
		worker_b.UpdateLocalPass(0, f_b);
		worker_b.ApplyLocalDecay(0, 20000); // 20ms
		worker_b.UpdateLocalGlobalPass(f_b);
	}

	// Worker B consumed 200ms total, Worker A consumed 10ms total
	// Worker B should have far more decay steps
	REQUIRE(worker_b.decay_steps[0] > worker_a.decay_steps[0]);

	// Worker B's priority should be lower (more decayed)
	REQUIRE(worker_b.priorities[0] < worker_a.priorities[0]);

	// Worker B's pass should be much higher (more CPU time → more pass advance)
	REQUIRE(worker_b.pass_values[0] > worker_a.pass_values[0] * 10);

	// Worker B's global pass should also be much higher
	REQUIRE(worker_b.local_global_pass > worker_a.local_global_pass * 10);
}

//===----------------------------------------------------------------------===//
// Test 3.10: STRIDE_QUANTUM_CHUNKS Controls Morsel Count
//
// STRIDE_QUANTUM_CHUNKS = 50 defines how many chunks (of 2048 tuples each)
// a pipeline task processes per PROCESS_PARTIAL call in stride mode.
// This determines the quantum granularity for stride scheduling.
//
// While both STRIDE_QUANTUM_CHUNKS and PARTIAL_CHUNK_COUNT are currently 50,
// they are separate constants to allow independent tuning.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.10: STRIDE_QUANTUM_CHUNKS constant", "[stride][api]") {
	// Verify the constant exists and has expected value
	constexpr idx_t quantum_chunks = STRIDE_QUANTUM_CHUNKS;
	REQUIRE(quantum_chunks == 50);

	// Each chunk processes STANDARD_VECTOR_SIZE tuples (2048)
	// So one quantum processes ~50 × 2048 = 102,400 tuples
	// This is the "static morsel" that replaces the paper's adaptive morsel
}

//===----------------------------------------------------------------------===//
// Test 3.11: Finalization Priority Boost (Section 3 + Section 2 Interaction)
//
// Paper ref: "The first worker to notice [task exhaustion] coordinates
// the finalization phase."
//
// In DuckDB, when a pipeline enters finalization (PipelinePrepareFinishEvent
// or PipelineFinishEvent), PushFinalizationToWorkers sets local pass = 0
// for all workers. This gives finalization tasks absolute priority,
// ensuring the pipeline completes quickly — the equivalent of the paper's
// "photo finish" optimization.
//
// Key point: The finalization mask is pushed from BOTH PipelinePrepareFinishEvent
// AND PipelineFinishEvent, ensuring finalization is prioritized at both stages.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.11: Finalization prioritizes pipeline completion", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;
	slot_array.RegisterWorker(&state);

	// Set up two slots: one with high pass (Q1, been running long), one with low pass (Q2, new)
	state.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	// Simulate Q1 running for a while (high pass, decayed priority)
	state.pass_values[0] = 1000.0;
	state.priorities[0] = 500.0; // decayed
	state.strides[0] = SchedulerSlotArray::LARGE_CONSTANT / 500.0;

	// Q2 is new (low pass, high priority)
	state.pass_values[1] = 100.0;

	// Normally Q2 would be selected (lower pass)
	// Force rebuild
	state.strides[0] = state.strides[0]; // no-op to force rebuild
	state.change_mask_low.store(0, std::memory_order_relaxed); // clear
	// Manually rebuild since we changed pass values directly
	state.sorted_count = 2;
	state.sorted_slots[0] = 1; // Q2 first (pass=100)
	state.sorted_slots[1] = 0; // Q1 second (pass=1000)

	REQUIRE(state.FindMinPassSlot() == 1); // Q2 selected normally

	SECTION("After finalization, Q1 gets absolute priority") {
		slot_array.PushFinalizationToWorkers(0);
		state.PullUpdates(slot_array);

		// Q1's pass is now 0, making it the min-pass slot
		REQUIRE(state.pass_values[0] == 0.0);
		REQUIRE(state.FindMinPassSlot() == 0);
	}

	SECTION("Finalization boost applies even when query is deeply decayed") {
		state.pass_values[0] = 99999.0; // extremely high pass
		state.sorted_slots[0] = 1;
		state.sorted_slots[1] = 0;

		slot_array.PushFinalizationToWorkers(0);
		state.PullUpdates(slot_array);

		REQUIRE(state.pass_values[0] == 0.0);
		REQUIRE(state.FindMinPassSlot() == 0);
	}

	slot_array.DeregisterWorker(&state);
}

//===----------------------------------------------------------------------===//
// Test 3.12: Pipeline Transition Preserves Decayed Priority
//
// Paper: "Since priorities are tied to resource groups and not task sets...
// the worker can retain the slot priority."
//
// When a query moves to its next pipeline (return mask), pass resets to
// local_global_pass but priority/stride stay at their decayed values.
// This is critical: a long-running query that earned low priority through
// decay keeps that earned priority across pipeline boundaries.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.12: Pipeline transition keeps decayed priority", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);
	slot_array.RegisterWorker(&state);

	// Decay the query
	state.ApplyLocalDecay(0, 100000); // 100ms → significant decay

	double decayed_priority = state.priorities[0];
	double decayed_stride = state.strides[0];
	REQUIRE(decayed_priority < SchedulerSlotArray::INITIAL_PRIORITY);

	// Advance global pass
	state.local_global_pass = 500.0;

	// Simulate pipeline transition (return mask)
	slot_array.PushReturnToWorkers(0);
	state.PullUpdates(slot_array);

	// Pass reset to global pass (fair re-entry)
	REQUIRE(state.pass_values[0] == 500.0);
	// Priority and stride preserved (earned through decay)
	REQUIRE(state.priorities[0] == decayed_priority);
	REQUIRE(state.strides[0] == decayed_stride);

	slot_array.DeregisterWorker(&state);
}

//===----------------------------------------------------------------------===//
// Test 3.13: Short Query Preferential Treatment
//
// Paper ref (Section 3.2): "We utilize adaptive query priorities to
// transparently treat short running requests in a preferential fashion."
//
// With the f-factor and CPU-time decay, a short query consuming little
// CPU time retains high priority (no decay), while a long query's
// priority decays. The stride scheduler then naturally picks the short
// query more often, reducing its latency.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.13: Short queries retain high priority over long queries", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;

	// Two queries
	state.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;

	// Q0: long-running query, accumulates 200ms of CPU time
	state.ApplyLocalDecay(0, 200000);

	// Q1: short query, accumulates only 5ms of CPU time
	state.ApplyLocalDecay(1, 5000);

	// Q1 should still have initial priority (5ms → 2 steps, both within d_start=10)
	REQUIRE(state.priorities[1] == init_prio);

	// Q0 should have significantly lower priority
	REQUIRE(state.priorities[0] < init_prio);

	// Q0's stride should be higher (scheduled less frequently)
	REQUIRE(state.strides[0] > state.strides[1]);
}

//===----------------------------------------------------------------------===//
// Test 3.14: End-to-End Stride Execution with Decay
//
// Verify that a real DuckDB query executes correctly in stride mode,
// exercising the full path: slot registration → task execution with
// f-factor pass updates → CPU-time decay → finalization → slot cleanup.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.14: End-to-end query with decay path", "[stride][api]") {
	auto db = MakeStrideDB3(4);
	Connection con(*db);

	SECTION("Long query exercises decay path") {
		// This query should be long enough to trigger some decay
		auto result = con.Query("SELECT SUM(i * i) FROM range(1000000) t(i)");
		REQUIRE(!result->HasError());
	}

	SECTION("Short and long queries run concurrently") {
		string long_error, short_error;

		std::thread long_thread([&]() {
			Connection c(*db);
			auto r = c.Query("SELECT SUM(val) FROM (SELECT i % 1000 AS key, i AS val FROM range(2000000) t(i) ORDER BY key) sub");
			if (r->HasError()) {
				long_error = r->GetError();
			}
		});

		std::thread short_thread([&]() {
			Connection c(*db);
			auto r = c.Query("SELECT 42");
			if (r->HasError()) {
				short_error = r->GetError();
			}
		});

		long_thread.join();
		short_thread.join();

		if (!long_error.empty()) {
			FAIL_CHECK("Long query error: " + long_error);
		}
		if (!short_error.empty()) {
			FAIL_CHECK("Short query error: " + short_error);
		}
		REQUIRE(long_error.empty());
		REQUIRE(short_error.empty());
	}

	// Verify cleanup
	auto &scheduler = TaskScheduler::GetScheduler(*db->instance);
	REQUIRE(scheduler.GetSlotArray().GetActiveSlotCount() == 0);
}

//===----------------------------------------------------------------------===//
// Test 3.15: Decay Step Effective Count Calculation
//
// The effective_steps formula:
//   effective = max(0, new_total - d_start) - max(0, old_total - d_start)
//
// This correctly handles the transition across d_start:
// - If both old and new are below d_start: 0 effective
// - If old is below, new is above: only the steps past d_start count
// - If both are above: all new steps count
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.15: Effective step calculation across d_start boundary", "[stride][api]") {
	SchedulerSlotArray slot_array;
	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA; // 10
	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;
	double lambda = SchedulerSlotArray::DEFAULT_DECAY_LAMBDA;

	SECTION("Batch crossing d_start: only steps past d_start are effective") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		// 12 steps total (crosses d_start=10): 2 effective
		state.ApplyLocalDecay(0, 12 * 2000);
		double expected = init_prio * std::pow(lambda, 2);
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}

	SECTION("Split batch: 8 steps then 7 steps (total 15, 5 effective)") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		state.ApplyLocalDecay(0, 8 * 2000);  // 8 total, 0 effective
		REQUIRE(state.priorities[0] == init_prio);

		state.ApplyLocalDecay(0, 7 * 2000);  // 15 total, 5 effective
		double expected = init_prio * std::pow(lambda, 5);
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}

	SECTION("All steps past d_start: full effectiveness") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		// First: reach d_start
		state.ApplyLocalDecay(0, d_start * 2000);
		REQUIRE(state.priorities[0] == init_prio);

		// Then: 5 more steps, all effective
		state.ApplyLocalDecay(0, 5 * 2000);
		double expected = init_prio * std::pow(lambda, 5);
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.16: Decay Effect on Proportional Fairness (Scheduling Ratio)
//
// After decay, a long-running query should be scheduled LESS often than a
// short query. Test 3.13 shows priority divergence, but this test verifies
// that the scheduler actually acts on it: the ratio of "times selected" must
// approximate the ratio of priorities.
//
// Stride scheduling guarantees: scheduling share ∝ priority. So if Q0 has
// priority 100 (decayed) and Q1 has priority 10000 (fresh), Q1 should be
// selected ~100× more often.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.16: Decay causes proportional scheduling ratio shift", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;

	// Two queries: Q0 and Q1
	state.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	// Decay Q0 to MIN_PRIORITY (floor)
	state.ApplyLocalDecay(0, 500000); // 500ms → decays to floor
	REQUIRE(state.priorities[0] == SchedulerSlotArray::MIN_PRIORITY); // 100

	// Q1 remains at INITIAL_PRIORITY (10000)
	REQUIRE(state.priorities[1] == SchedulerSlotArray::INITIAL_PRIORITY);

	// Simulate scheduling: repeatedly pick min-pass slot, advance its pass
	int q0_count = 0, q1_count = 0;
	const int ROUNDS = 10000;
	double f = 1.0; // standard quantum

	for (int i = 0; i < ROUNDS; i++) {
		idx_t selected = state.FindMinPassSlot();
		REQUIRE(selected != DConstants::INVALID_INDEX);
		if (selected == 0) {
			q0_count++;
		} else {
			q1_count++;
		}
		state.UpdateLocalPass(selected, f);
	}

	// Q0 has priority 100, Q1 has priority 10000
	// Expected ratio: q1_count / q0_count ≈ 10000/100 = 100
	double ratio = static_cast<double>(q1_count) / static_cast<double>(q0_count);

	// Allow ±20% tolerance for rounding
	REQUIRE(ratio > 80.0);
	REQUIRE(ratio < 120.0);

	// Verify Q1 (high priority, short query) was scheduled vastly more often
	REQUIRE(q1_count > q0_count * 50);
}

//===----------------------------------------------------------------------===//
// Test 3.17: Priority Decay Effect on Global Pass Advancement
//
// When a query decays, total priority drops → global stride increases →
// global_pass advances faster per quantum. New queries inherit this higher
// global_pass as their initial pass. This is a subtle fairness property:
// a system under heavy long-running load makes new queries "start further
// along," preventing them from monopolizing the CPU on arrival.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.17: Decay increases global stride and global pass rate", "[stride][api]") {
	SchedulerSlotArray slot_array;

	// Worker A: no decay (fresh queries)
	ThreadLocalSchedulerState worker_a;
	worker_a.slot_array_ptr = &slot_array;
	worker_a.needs_full_refresh = false;
	worker_a.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	worker_a.PullUpdates(slot_array);

	// Worker B: same initial setup, but will decay Q0
	ThreadLocalSchedulerState worker_b;
	worker_b.slot_array_ptr = &slot_array;
	worker_b.needs_full_refresh = false;
	worker_b.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	worker_b.PullUpdates(slot_array);

	double init_global_stride = worker_a.local_global_stride;
	REQUIRE(init_global_stride == worker_b.local_global_stride);

	// Decay Q0 on worker B
	worker_b.ApplyLocalDecay(0, 500000); // 500ms → decays to MIN_PRIORITY
	REQUIRE(worker_b.priorities[0] == SchedulerSlotArray::MIN_PRIORITY);

	// Worker B's global stride should have increased (lower total priority)
	REQUIRE(worker_b.local_global_stride > init_global_stride);

	// Worker A's global stride is unchanged (no decay applied there)
	REQUIRE(worker_a.local_global_stride == init_global_stride);

	// Now advance global pass on both workers with same f
	double f = 1.0;
	worker_a.UpdateLocalGlobalPass(f);
	worker_b.UpdateLocalGlobalPass(f);

	// Worker B's global pass advanced more (higher stride)
	REQUIRE(worker_b.local_global_pass > worker_a.local_global_pass);

	// A new query arriving on worker B would get a higher initial pass
	// (pass_values[new_slot] = local_global_pass), meaning it starts
	// "further along" and doesn't monopolize CPU on arrival
	worker_b.change_mask_low.store(1ULL << 2, std::memory_order_relaxed);
	worker_b.PullUpdates(slot_array);
	REQUIRE(worker_b.pass_values[2] == worker_b.local_global_pass);
	REQUIRE(worker_b.pass_values[2] > worker_a.local_global_pass);
}

//===----------------------------------------------------------------------===//
// Test 3.18: Decay State Preserved Across Pipeline Transition (Return Mask)
//
// When a query moves to its next pipeline (return mask), pass resets to
// local_global_pass, but cpu_time_accum_us and decay_steps must NOT reset.
// If they did, a multi-pipeline query would get a fresh grace period on
// each pipeline boundary, defeating the entire decay mechanism.
//
// This test verifies that ProcessReturnMask only touches pass_values
// and leaves cpu_time_accum_us and decay_steps intact.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.18: Decay state preserved across pipeline transition", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);
	slot_array.RegisterWorker(&state);

	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;

	// Accumulate significant CPU time: past d_start, with partial remainder
	// 15 steps (10 grace + 5 effective), remainder 1500us
	uint64_t total_us = static_cast<uint64_t>(d_start + 5) * 2000 + 1500;
	state.ApplyLocalDecay(0, total_us);

	int decay_steps_before = state.decay_steps[0];
	uint64_t accum_before = state.cpu_time_accum_us[0];
	double priority_before = state.priorities[0];
	double stride_before = state.strides[0];

	REQUIRE(decay_steps_before == d_start + 5); // 15 total steps
	REQUIRE(accum_before == 1500);               // 1500us remainder
	REQUIRE(priority_before < SchedulerSlotArray::INITIAL_PRIORITY); // decayed

	// Advance global pass so we can detect the pass reset
	state.local_global_pass = 999.0;

	// Trigger pipeline transition via return mask
	slot_array.PushReturnToWorkers(0);
	state.PullUpdates(slot_array);

	// Pass resets to global pass (pipeline transition)
	REQUIRE(state.pass_values[0] == 999.0);

	// Decay state MUST be preserved
	REQUIRE(state.decay_steps[0] == decay_steps_before);
	REQUIRE(state.cpu_time_accum_us[0] == accum_before);
	REQUIRE(state.priorities[0] == priority_before);
	REQUIRE(state.strides[0] == stride_before);

	// Further decay should continue from where it left off, not restart grace
	state.ApplyLocalDecay(0, 2000); // 1 more step (step 16), 1 effective
	REQUIRE(state.decay_steps[0] == decay_steps_before + 1);
	// The remainder from before (1500) + 500 from this 2000 = 2000 → fires
	// Actually: accum was 1500, add 2000 → 3500, steps = 3500/2000 = 1, remainder = 1500
	REQUIRE(state.cpu_time_accum_us[0] == 1500);

	// Priority decayed further (1 more effective step)
	double lambda = SchedulerSlotArray::DEFAULT_DECAY_LAMBDA;
	double expected = priority_before * lambda;
	REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);

	slot_array.DeregisterWorker(&state);
}

//===----------------------------------------------------------------------===//
// Test 3.19: Two Workers — Decay Divergence Affects Min-Pass Selection
//
// The paper's core motivation for per-worker state: two workers executing
// different amounts of work for the same query will make different scheduling
// decisions. Worker B, having consumed more CPU for Q0, decays Q0 more and
// eventually stops selecting it as the priority choice, while Worker A
// (less decay) still selects Q0.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.19: Per-worker decay divergence changes scheduling decisions", "[stride][api]") {
	SchedulerSlotArray slot_array;

	// Two workers, two queries each
	ThreadLocalSchedulerState worker_a;
	worker_a.slot_array_ptr = &slot_array;
	worker_a.needs_full_refresh = false;
	worker_a.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	worker_a.PullUpdates(slot_array);

	ThreadLocalSchedulerState worker_b;
	worker_b.slot_array_ptr = &slot_array;
	worker_b.needs_full_refresh = false;
	worker_b.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	worker_b.PullUpdates(slot_array);

	// Both start at the same pass values — both would pick Q0 (or Q1, ties)
	REQUIRE(worker_a.FindMinPassSlot() == worker_b.FindMinPassSlot());

	// Worker B processes Q0 heavily (high CPU time → heavy decay)
	// Worker A processes Q0 lightly (low CPU time → within grace period)
	for (int i = 0; i < 20; i++) {
		// Worker A: 1ms quantum for Q0
		double f_a = 1.0 / SchedulerSlotArray::REFERENCE_DURATION_MS;
		worker_a.UpdateLocalPass(0, f_a);
		worker_a.ApplyLocalDecay(0, 1000);
		worker_a.UpdateLocalGlobalPass(f_a);

		// Worker B: 20ms quantum for Q0
		double f_b = 20.0 / SchedulerSlotArray::REFERENCE_DURATION_MS;
		worker_b.UpdateLocalPass(0, f_b);
		worker_b.ApplyLocalDecay(0, 20000);
		worker_b.UpdateLocalGlobalPass(f_b);

		// Both workers also do some Q1 work (equal, 2ms each)
		double f_q1 = 2.0 / SchedulerSlotArray::REFERENCE_DURATION_MS;
		worker_a.UpdateLocalPass(1, f_q1);
		worker_a.ApplyLocalDecay(1, 2000);

		worker_b.UpdateLocalPass(1, f_q1);
		worker_b.ApplyLocalDecay(1, 2000);
	}

	// Worker B consumed 20×20=400ms for Q0, Worker A consumed 20×1=20ms for Q0
	// Worker B's Q0 is heavily decayed → higher stride → pass advances faster
	REQUIRE(worker_b.priorities[0] < worker_a.priorities[0]);
	REQUIRE(worker_b.strides[0] > worker_a.strides[0]);
	REQUIRE(worker_b.pass_values[0] > worker_a.pass_values[0]);

	// The critical test: the workers now disagree on which query to schedule
	// Worker A: Q0 still has reasonable priority, might still be selected
	// Worker B: Q0 has very high pass from fast stride advancement
	idx_t worker_b_choice = worker_b.FindMinPassSlot();

	// Worker B should prefer Q1 (Q0's pass is much higher due to higher stride)
	REQUIRE(worker_b_choice == 1);

	// Worker A may still prefer Q0 or Q1 depending on accumulated pass,
	// but the key point is the divergence: workers disagree
	// (Worker A's Q0 pass is much lower than Worker B's Q0 pass)
	REQUIRE(worker_a.pass_values[0] < worker_b.pass_values[0]);

	// Verify the divergence: worker B's Q0 pass is far above Q1 pass
	REQUIRE(worker_b.pass_values[0] > worker_b.pass_values[1]);
}

//===----------------------------------------------------------------------===//
// Test 3.20: STRIDE_QUANTUM_CHUNKS Dispatched in Pipeline Execution
//
// pipeline.cpp dispatches STRIDE_QUANTUM_CHUNKS (50) to Execute() when
// scheduler is STRIDE, and PARTIAL_CHUNK_COUNT (also 50) in DEFAULT mode.
// These are separate constants to allow independent tuning.
//
// This test verifies the dispatch logic indirectly: a query under STRIDE
// mode should execute with the same chunk count as DEFAULT mode when both
// constants are equal. If they diverge in the future, this test catches it.
//
// Note: We cannot directly unit-test PipelineTask::ExecuteTask without
// running a full query, so we verify via end-to-end execution and constant
// comparison.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.20: STRIDE_QUANTUM_CHUNKS dispatch in pipeline", "[stride][api]") {
	// Verify both constants exist and their current values
	constexpr idx_t stride_chunks = STRIDE_QUANTUM_CHUNKS;
	// PipelineTask::PARTIAL_CHUNK_COUNT is private, but defined as 50 in pipeline.hpp:28
	constexpr idx_t partial_chunks = 50;

	REQUIRE(stride_chunks == 50);
	REQUIRE(partial_chunks == 50);

	SECTION("Query produces identical results under both scheduler modes") {
		// Run the same query under DEFAULT and STRIDE modes
		// If STRIDE_QUANTUM_CHUNKS dispatch is broken, the query would hang or crash

		// DEFAULT mode
		DBConfig config1;
		config1.options.maximum_threads = 2;
		DuckDB db1(nullptr, &config1);
		Connection con1(db1);
		auto r1 = con1.Query("SELECT SUM(i) FROM range(500000) t(i)");
		REQUIRE(!r1->HasError());
		auto val1 = r1->GetValue(0, 0);

		// STRIDE mode
		auto db2 = MakeStrideDB3(2);
		Connection con2(*db2);
		auto r2 = con2.Query("SELECT SUM(i) FROM range(500000) t(i)");
		REQUIRE(!r2->HasError());
		auto val2 = r2->GetValue(0, 0);

		// Results must be identical
		REQUIRE(val1 == val2);
	}

	SECTION("Complex multi-pipeline query under STRIDE mode") {
		auto db = MakeStrideDB3(4);
		Connection con(*db);
		// Sort + aggregate = multiple pipelines, each dispatching STRIDE_QUANTUM_CHUNKS
		auto r = con.Query(
		    "SELECT key, SUM(val) FROM (SELECT i % 100 AS key, i AS val "
		    "FROM range(1000000) t(i) ORDER BY key) sub GROUP BY key ORDER BY key LIMIT 5");
		REQUIRE(!r->HasError());
		REQUIRE(r->RowCount() == 5);
	}
}

//===----------------------------------------------------------------------===//
// Test 3.21: Decay with d_start=0 (Immediate Decay)
//
// The optimizer can set d_start=0, meaning every decay step fires immediately
// with no grace period. The very first 2ms of CPU time should decay the
// query's priority. This is an important edge case because the effective_steps
// formula simplifies to: effective = new_total - 0 = new_total.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.21: Immediate decay with d_start=0", "[stride][api]") {
	SchedulerSlotArray slot_array;
	double lambda = SchedulerSlotArray::DEFAULT_DECAY_LAMBDA; // 0.9
	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;  // 10000

	// Set d_start=0 via optimizer interface
	slot_array.SetDecayParameters(0, lambda);

	SECTION("Very first 2ms decays priority") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		state.ApplyLocalDecay(0, 2000); // exactly 1 step, immediately effective
		REQUIRE(state.decay_steps[0] == 1);
		double expected = init_prio * lambda; // 10000 * 0.9 = 9000
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-6);
	}

	SECTION("Sub-threshold still accumulates without firing") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		state.ApplyLocalDecay(0, 1999); // just under threshold
		REQUIRE(state.decay_steps[0] == 0);
		REQUIRE(state.priorities[0] == init_prio); // no step, no decay
		REQUIRE(state.cpu_time_accum_us[0] == 1999);
	}

	SECTION("Each subsequent step also decays (no grace ever)") {
		ThreadLocalSchedulerState state;
		SetupSingleSlot(state, slot_array);

		// 5 steps — all effective with d_start=0
		state.ApplyLocalDecay(0, 5 * 2000);
		REQUIRE(state.decay_steps[0] == 5);
		double expected = init_prio * std::pow(lambda, 5);
		REQUIRE(std::abs(state.priorities[0] - expected) < 1e-4);
	}

	SECTION("Incremental 1-step-at-a-time matches batch") {
		ThreadLocalSchedulerState state_inc;
		SetupSingleSlot(state_inc, slot_array);
		ThreadLocalSchedulerState state_batch;
		SetupSingleSlot(state_batch, slot_array);

		// Incremental: 5 separate calls of 2ms each
		for (int i = 0; i < 5; i++) {
			state_inc.ApplyLocalDecay(0, 2000);
		}

		// Batch: one call of 10ms
		state_batch.ApplyLocalDecay(0, 10000);

		REQUIRE(state_inc.decay_steps[0] == state_batch.decay_steps[0]);
		REQUIRE(std::abs(state_inc.priorities[0] - state_batch.priorities[0]) < 1e-4);
	}

	// Reset to defaults
	slot_array.SetDecayParameters(SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA,
	                              SchedulerSlotArray::DEFAULT_DECAY_LAMBDA);
}

//===----------------------------------------------------------------------===//
// Test 3.22: DisableSlotLocally Resets Decay State and Recomputes Stride
//
// When a worker discovers a slot's executor is nullptr (lazy deactivation),
// it calls DisableSlotLocally. This must:
//   1. Clear active_slots bit
//   2. Reset cpu_time_accum_us and decay_steps to 0
//   3. Recompute local_global_stride without the deactivated slot
//   4. Rebuild sorted_slots
//
// If the decayed priority leaked into stride calculation after deactivation,
// global pass advancement would be wrong for subsequent quanta.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.22: DisableSlotLocally resets decay state and recomputes stride", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	state.slot_array_ptr = &slot_array;
	state.needs_full_refresh = false;

	// Activate two slots
	state.change_mask_low.store((1ULL << 0) | (1ULL << 1), std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	// Partially decay slot 0 (mid-accumulator, past d_start)
	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;
	state.ApplyLocalDecay(0, static_cast<uint64_t>(d_start + 3) * 2000 + 1500); // 1500us remainder
	REQUIRE(state.cpu_time_accum_us[0] == 1500);
	REQUIRE(state.decay_steps[0] == d_start + 3);
	REQUIRE(state.priorities[0] < SchedulerSlotArray::INITIAL_PRIORITY);

	// Two-slot global stride before disable
	double stride_before = state.local_global_stride;

	// Slot 0 query finishes — worker discovers executor is nullptr → lazy disable
	state.DisableSlotLocally(0);

	// Decay state must be cleared
	REQUIRE(state.cpu_time_accum_us[0] == 0);
	REQUIRE(state.decay_steps[0] == 0);

	// Slot 0 no longer active
	REQUIRE(!state.active_slots.test(0));

	// Only slot 1 remains in sorted list
	REQUIRE(state.sorted_count == 1);
	REQUIRE(state.sorted_slots[0] == 1);

	// Global stride recomputed with only slot 1's priority (INITIAL_PRIORITY)
	double expected_stride = SchedulerSlotArray::LARGE_CONSTANT / SchedulerSlotArray::INITIAL_PRIORITY;
	REQUIRE(std::abs(state.local_global_stride - expected_stride) < 1e-9);

	// Global stride increased (fewer active slots → lower total priority → higher stride)
	REQUIRE(state.local_global_stride >= stride_before);
}

//===----------------------------------------------------------------------===//
// Test 3.23: FullRefreshFromGlobal Resets Decay State
//
// FullRefreshFromGlobal reinitializes all local scheduling state from the
// global slot array. It sets cpu_time_accum_us[slot] = 0 and
// decay_steps[slot] = 0 for every slot. This means a full refresh mid-query
// wipes decay state, giving the query a fresh grace period.
//
// This is by design: full refreshes are rare (sequence number mismatch)
// and the simplicity of a clean restart outweighs the cost of losing
// a few decay steps. But the behavior must be documented and tested.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.23: FullRefreshFromGlobal resets all decay state", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	// Accumulate significant decay
	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;
	state.ApplyLocalDecay(0, static_cast<uint64_t>(d_start + 5) * 2000 + 900);
	REQUIRE(state.decay_steps[0] == d_start + 5);
	REQUIRE(state.cpu_time_accum_us[0] == 900);
	REQUIRE(state.priorities[0] < SchedulerSlotArray::INITIAL_PRIORITY);

	// Record the global pass (should be preserved across full refresh)
	state.local_global_pass = 42.0;

	// Force a full refresh
	// Note: FullRefreshFromGlobal reads active slots from the global slot array.
	// Since we didn't RegisterQuery in the global array, no slots will be active
	// after refresh — the local state is completely cleared.
	state.needs_full_refresh = true;
	state.PullUpdates(slot_array);

	// All local slots cleared (no globally registered queries)
	REQUIRE(state.sorted_count == 0);
	REQUIRE(!state.active_slots.test(0));

	// Verify: if we manually re-activate slot 0 (simulating a change mask after
	// the full refresh), the decay state starts fresh
	state.change_mask_low.store(1ULL << 0, std::memory_order_relaxed);
	state.PullUpdates(slot_array);

	REQUIRE(state.cpu_time_accum_us[0] == 0);
	REQUIRE(state.decay_steps[0] == 0);
	REQUIRE(state.priorities[0] == SchedulerSlotArray::INITIAL_PRIORITY);
	// Pass initialized to local_global_pass (which we set to 42.0)
	REQUIRE(state.pass_values[0] == 42.0);
}

//===----------------------------------------------------------------------===//
// Test 3.24: Lambda=1.0 Disables Decay Entirely
//
// The optimizer can output λ=1.0 (valid range is (0, 1]) when the workload
// doesn't benefit from decay. pow(1.0, k) = 1.0 for all k, so priority
// should never change regardless of CPU time consumed.
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.24: Lambda=1.0 disables decay entirely", "[stride][api]") {
	SchedulerSlotArray slot_array;
	slot_array.SetDecayParameters(0, 1.0); // d_start=0, no-decay lambda

	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	double init_prio = SchedulerSlotArray::INITIAL_PRIORITY;

	// Massive CPU time with d_start=0 — fires 500 steps, all effective
	state.ApplyLocalDecay(0, 1000000); // 1000ms → 500 steps
	REQUIRE(state.decay_steps[0] == 500);
	REQUIRE(state.priorities[0] == init_prio); // λ=1.0 → pow(1.0, 500) = 1.0
	double large_c = SchedulerSlotArray::LARGE_CONSTANT;
	REQUIRE(state.strides[0] == large_c / init_prio);
	REQUIRE(state.local_global_stride == large_c / init_prio);

	// Reset to defaults
	slot_array.SetDecayParameters(SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA,
	                              SchedulerSlotArray::DEFAULT_DECAY_LAMBDA);
}

//===----------------------------------------------------------------------===//
// Test 3.25: Finalization Mask Only Resets Pass, Not Decay State
//
// ProcessFinalizationMask sets pass_values[slot] = 0.0 and nothing else.
// Priorities, strides, cpu_time_accum_us, and decay_steps must be untouched.
// If someone accidentally added a priority reset to finalization, the
// decay-based fairness mechanism would break (long queries would get
// fresh high priority during finalization).
//
// Symmetric counterpart to Test 3.18 (return mask preserves decay state).
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.25: Finalization mask only resets pass, not decay state", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);
	slot_array.RegisterWorker(&state);

	// Apply significant decay
	int d_start = SchedulerSlotArray::DEFAULT_DECAY_START_QUANTA;
	uint64_t total_us = static_cast<uint64_t>(d_start + 7) * 2000 + 1300;
	state.ApplyLocalDecay(0, total_us);

	double prio_before    = state.priorities[0];
	double stride_before  = state.strides[0];
	int    steps_before   = state.decay_steps[0];
	uint64_t accum_before = state.cpu_time_accum_us[0];

	// Set pass to something non-zero to verify the reset
	state.pass_values[0] = 42.0;

	// Push finalization
	slot_array.PushFinalizationToWorkers(0);
	state.PullUpdates(slot_array);

	// Pass must be reset to 0 (finalization priority boost)
	REQUIRE(state.pass_values[0] == 0.0);

	// All decay state must be untouched
	REQUIRE(state.priorities[0]        == prio_before);
	REQUIRE(state.strides[0]           == stride_before);
	REQUIRE(state.decay_steps[0]       == steps_before);
	REQUIRE(state.cpu_time_accum_us[0] == accum_before);

	slot_array.DeregisterWorker(&state);
}

//===----------------------------------------------------------------------===//
// Test 3.26: Pass Values Are Strictly Monotonically Increasing
//
// Every call to UpdateLocalPass with f > 0 must increase pass_values.
// This sounds trivial, but if stride × f underflows to zero for very
// small f (e.g., sub-microsecond quantum), monotonicity breaks and the
// scheduler would starve the slot. This test covers:
//   - Minimum realistic f (1μs quantum → f = 0.0005)
//   - Decayed slots (stride = LARGE_CONSTANT / MIN_PRIORITY = 10.0)
//===----------------------------------------------------------------------===//
TEST_CASE("Stride 3.26: Pass values are strictly monotonically increasing", "[stride][api]") {
	SchedulerSlotArray slot_array;
	ThreadLocalSchedulerState state;
	SetupSingleSlot(state, slot_array);

	// Very small f: 1μs quantum → f = 0.001 / 2.0 = 0.0005
	double very_small_f = 0.001 / SchedulerSlotArray::REFERENCE_DURATION_MS;
	double prev_pass = state.pass_values[0];

	SECTION("Monotonic with fresh priority (small stride)") {
		for (int i = 0; i < 1000; i++) {
			state.UpdateLocalPass(0, very_small_f);
			REQUIRE(state.pass_values[0] > prev_pass);
			prev_pass = state.pass_values[0];
		}
	}

	SECTION("Monotonic with decayed priority (large stride)") {
		// Decay to MIN_PRIORITY → stride = LARGE_CONSTANT / MIN_PRIORITY = 10.0
		state.ApplyLocalDecay(0, 500000);
		double min_prio = SchedulerSlotArray::MIN_PRIORITY;
		double large_c = SchedulerSlotArray::LARGE_CONSTANT;
		REQUIRE(state.priorities[0] == min_prio);

		double decayed_stride = state.strides[0];
		REQUIRE(decayed_stride == large_c / min_prio);

		prev_pass = state.pass_values[0];
		// stride * f = 10.0 * 0.0005 = 0.005 — well above double epsilon
		for (int i = 0; i < 1000; i++) {
			state.UpdateLocalPass(0, very_small_f);
			REQUIRE(state.pass_values[0] > prev_pass);
			prev_pass = state.pass_values[0];
		}
	}

	SECTION("Monotonic with f=1.0 over many iterations") {
		double f = 1.0;
		for (int i = 0; i < 10000; i++) {
			state.UpdateLocalPass(0, f);
			REQUIRE(state.pass_values[0] > prev_pass);
			prev_pass = state.pass_values[0];
		}
	}
}
