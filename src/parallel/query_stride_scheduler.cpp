//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_stride_scheduler.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_stride_scheduler.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/query_stride_optimizer.hpp"

#include <algorithm>
#include <cmath>
#include <thread>
#include <unordered_map>

namespace duckdb {

constexpr double QueryStrideScheduler::LARGE_CONSTANT;
constexpr double QueryStrideScheduler::REFERENCE_DURATION_MS;
constexpr double QueryStrideScheduler::INITIAL_PRIORITY;
constexpr double QueryStrideScheduler::MINIMUM_PRIORITY;
constexpr int QueryStrideScheduler::DEFAULT_DECAY_START;
constexpr double QueryStrideScheduler::DEFAULT_DECAY_LAMBDA;
constexpr double QueryStrideScheduler::TRACKING_DURATION_MS;
constexpr double QueryStrideScheduler::REFRESH_DURATION_MS;

struct QueryStrideSlot {
	atomic<ProducerToken *> producer {nullptr};
	atomic<uint64_t> generation {0};
	atomic<idx_t> pins {0};
	atomic<uint64_t> task_set_generation {0};
	atomic<bool> task_set_active {false};
	atomic<bool> lifecycle_serial {false};
	atomic<double> initial_priority {QueryStrideScheduler::INITIAL_PRIORITY};
	atomic<double> minimum_priority {QueryStrideScheduler::MINIMUM_PRIORITY};
	atomic<bool> static_priority {false};
	atomic<uint64_t> quanta {0};
	atomic<uint64_t> worker_time_us {0};
	double arrival_time_ms = 0;

	uint64_t db_query_id = 0;
	uint64_t request_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
};

namespace {

struct SlotPin {
	explicit SlotPin(QueryStrideSlot *slot_p) : slot(slot_p) {
	}

	SlotPin(const SlotPin &) = delete;
	SlotPin &operator=(const SlotPin &) = delete;

	QueryStrideSlot *slot = nullptr;

	~SlotPin() {
		Release();
	}

	void Release() {
		if (slot) {
			slot->pins.fetch_sub(1, std::memory_order_release);
			slot = nullptr;
		}
	}
};

static void SetMaskBit(atomic<uint64_t> &low, atomic<uint64_t> &high, idx_t slot) {
	if (slot < 64) {
		low.fetch_or(1ULL << slot, std::memory_order_release);
	} else {
		high.fetch_or(1ULL << (slot - 64), std::memory_order_release);
	}
}

template <class FUNC>
static void VisitMask(uint64_t mask, idx_t offset, FUNC &&callback) {
	while (mask) {
		auto bit = static_cast<idx_t>(__builtin_ctzll(mask));
		callback(offset + bit);
		mask &= mask - 1;
	}
}

} // namespace

struct QueryStrideSchedulerState {
	std::array<QueryStrideSlot, QUERY_STRIDE_MAX_SLOTS> slots;
	mutable mutex registration_lock;
	std::unordered_map<uint64_t, idx_t> query_slots;
	atomic<idx_t> active_count {0};

	mutex worker_lock;
	vector<QueryStrideWorkerState *> workers;

	atomic<int> optimizer_phase {static_cast<int>(QueryStrideOptimizerPhase::IDLE)};
	atomic<double> phase_start_ms {0};
	atomic<bool> optimizer_initialized {false};
	bool tracking_active = false;
	double tracking_start_ms = 0;
	vector<QueryStrideTraceEntry> tracked_workload;
	std::array<uint64_t, QUERY_STRIDE_MAX_SLOTS> quanta_at_window_start {};
	std::array<uint64_t, QUERY_STRIDE_MAX_SLOTS> elapsed_at_window_start {};
	std::array<bool, QUERY_STRIDE_MAX_SLOTS> present_at_window_start {};
	atomic<double> last_objective {0};
	atomic<uint64_t> optimization_count {0};
	atomic<uint64_t> registered_queries {0};
	atomic<uint64_t> unregistered_queries {0};
	atomic<uint64_t> activated_task_sets {0};
	atomic<uint64_t> finished_task_sets {0};
	atomic<uint64_t> change_task_sets {0};
	atomic<uint64_t> return_task_sets {0};
	atomic<uint64_t> finalization_task_sets {0};
	atomic<uint64_t> worker_lazy_deactivations {0};
};

static double StrideNowMs() {
	auto now = std::chrono::steady_clock::now();
	return std::chrono::duration<double, std::milli>(now.time_since_epoch()).count();
}

static void StartTrackingLocked(QueryStrideSchedulerState &state, double now_ms) {
	state.tracked_workload.clear();
	state.tracking_start_ms = now_ms;
	for (idx_t slot_index = 0; slot_index < QUERY_STRIDE_MAX_SLOTS; slot_index++) {
		auto &slot = state.slots[slot_index];
		auto present = slot.producer.load(std::memory_order_acquire) != nullptr;
		state.present_at_window_start[slot_index] = present;
		state.quanta_at_window_start[slot_index] =
		    present ? slot.quanta.load(std::memory_order_acquire) : 0;
		state.elapsed_at_window_start[slot_index] =
		    present ? slot.worker_time_us.load(std::memory_order_acquire) : 0;
	}
	state.tracking_active = true;
}

static void RecordTrackedSlotLocked(QueryStrideSchedulerState &state, idx_t slot_index, double now_ms) {
	auto &slot = state.slots[slot_index];
	auto quanta = slot.quanta.load(std::memory_order_acquire);
	auto elapsed = slot.worker_time_us.load(std::memory_order_acquire);
	auto window_quanta = quanta - state.quanta_at_window_start[slot_index];
	if (window_quanta == 0 || elapsed < state.elapsed_at_window_start[slot_index]) {
		return;
	}
	auto window_elapsed = elapsed - state.elapsed_at_window_start[slot_index];
	QueryStrideTraceEntry trace;
	trace.arrival_time_ms =
	    state.present_at_window_start[slot_index] ? 0 : std::max(0.0, slot.arrival_time_ms - state.tracking_start_ms);
	trace.window_quanta = window_quanta;
	trace.average_quantum_ms =
	    (static_cast<double>(window_elapsed) / 1000.0) / static_cast<double>(window_quanta);
	auto entry_time = state.present_at_window_start[slot_index] ? state.tracking_start_ms : slot.arrival_time_ms;
	trace.in_window_wall_time_ms = std::max(0.0, now_ms - entry_time);
	trace.initial_priority = slot.initial_priority.load(std::memory_order_relaxed);
	trace.minimum_priority = slot.minimum_priority.load(std::memory_order_relaxed);
	trace.static_priority = slot.static_priority.load(std::memory_order_relaxed);
	state.tracked_workload.push_back(trace);
}

static vector<QueryStrideTraceEntry> StopTrackingLocked(QueryStrideSchedulerState &state, double now_ms) {
	for (idx_t slot_index = 0; slot_index < QUERY_STRIDE_MAX_SLOTS; slot_index++) {
		if (state.slots[slot_index].producer.load(std::memory_order_acquire)) {
			RecordTrackedSlotLocked(state, slot_index, now_ms);
		}
	}
	state.tracking_active = false;
	return state.tracked_workload;
}

QueryStrideWorkerState::QueryStrideWorkerState() {
	sorted_slots.fill(DConstants::INVALID_INDEX);
}

void QueryStrideWorkerState::RecomputeGlobalStride() {
	double total_priority = 0;
	for (idx_t slot = 0; slot < QUERY_STRIDE_MAX_SLOTS; slot++) {
		if (active_slots.test(slot)) {
			total_priority += priorities[slot];
		}
	}
	local_global_stride = total_priority > 0 ? QueryStrideScheduler::LARGE_CONSTANT / total_priority : 0;
}

void QueryStrideWorkerState::RebuildSortedSlots() {
	sorted_count = 0;
	for (idx_t slot = 0; slot < QUERY_STRIDE_MAX_SLOTS; slot++) {
		if (active_slots.test(slot)) {
			sorted_slots[sorted_count++] = slot;
		}
	}
	std::sort(sorted_slots.begin(), sorted_slots.begin() + sorted_count, [&](idx_t left, idx_t right) {
		if (pass_values[left] == pass_values[right]) {
			return left < right;
		}
		return pass_values[left] < pass_values[right];
	});
}

void QueryStrideWorkerState::ActivateNewSlot(idx_t slot, uint64_t generation, double initial_priority,
                                             double minimum_priority, bool static_priority) {
	active_slots.set(slot);
	generations[slot] = generation;
	priorities[slot] = initial_priority;
	minimum_priorities[slot] = minimum_priority;
	static_priorities[slot] = static_priority;
	strides[slot] = QueryStrideScheduler::LARGE_CONSTANT / initial_priority;
	pass_values[slot] = local_global_pass;
	cpu_time_accum_us[slot] = 0;
	decay_steps[slot] = 0;
}

void QueryStrideWorkerState::DeactivateTaskSet(idx_t slot) {
	if (slot >= QUERY_STRIDE_MAX_SLOTS || !active_slots.test(slot)) {
		return;
	}
	active_slots.reset(slot);
	RecomputeGlobalStride();
	RebuildSortedSlots();
}

void QueryStrideWorkerState::ResetSlot(idx_t slot) {
	if (slot >= QUERY_STRIDE_MAX_SLOTS) {
		return;
	}
	active_slots.reset(slot);
	generations[slot] = 0;
	cpu_time_accum_us[slot] = 0;
	decay_steps[slot] = 0;
	RecomputeGlobalStride();
	RebuildSortedSlots();
}

void QueryStrideWorkerState::FullRefresh(QueryStrideScheduler &scheduler) {
	// Publication uses the same lock, so an activation cannot land between the slot scan and mask reset.
	lock_guard<mutex> guard(scheduler.state->worker_lock);
	active_slots.reset();
	for (idx_t slot = 0; slot < QUERY_STRIDE_MAX_SLOTS; slot++) {
		uint64_t generation;
		double initial_priority;
		double minimum_priority;
		bool static_priority;
		bool lifecycle_serial;
		if (!scheduler.LoadSlotConfiguration(slot, generation, initial_priority, minimum_priority, static_priority,
		                                     lifecycle_serial)) {
			continue;
		}
		ActivateNewSlot(slot, generation, initial_priority, minimum_priority, static_priority);
		if (lifecycle_serial) {
			pass_values[slot] = 0;
		}
	}
	change_mask_low.exchange(0, std::memory_order_acquire);
	change_mask_high.exchange(0, std::memory_order_acquire);
	finalization_mask_low.exchange(0, std::memory_order_acquire);
	finalization_mask_high.exchange(0, std::memory_order_acquire);
	return_mask_low.exchange(0, std::memory_order_acquire);
	return_mask_high.exchange(0, std::memory_order_acquire);
	RecomputeGlobalStride();
	RebuildSortedSlots();
	needs_full_refresh = false;
}

void QueryStrideWorkerState::PullUpdates(QueryStrideScheduler &scheduler) {
	if (needs_full_refresh) {
		FullRefresh(scheduler);
		return;
	}
	auto change_low = change_mask_low.exchange(0, std::memory_order_acquire);
	auto change_high = change_mask_high.exchange(0, std::memory_order_acquire);
	auto return_low = return_mask_low.exchange(0, std::memory_order_acquire);
	auto return_high = return_mask_high.exchange(0, std::memory_order_acquire);
	auto finalization_low = finalization_mask_low.exchange(0, std::memory_order_acquire);
	auto finalization_high = finalization_mask_high.exchange(0, std::memory_order_acquire);
	if (!(change_low | change_high | return_low | return_high | finalization_low | finalization_high)) {
		return;
	}
	auto apply_change = [&](idx_t slot) {
		uint64_t generation;
		double initial_priority;
		double minimum_priority;
		bool static_priority;
		bool lifecycle_serial;
		if (!scheduler.LoadSlotConfiguration(slot, generation, initial_priority, minimum_priority, static_priority,
		                                     lifecycle_serial)) {
			ResetSlot(slot);
			return;
		}
		ActivateNewSlot(slot, generation, initial_priority, minimum_priority, static_priority);
		if (lifecycle_serial) {
			pass_values[slot] = 0;
		}
	};
	VisitMask(change_low, 0, apply_change);
	VisitMask(change_high, 64, apply_change);
	auto apply_return = [&](idx_t slot) {
		uint64_t generation;
		double initial_priority;
		double minimum_priority;
		bool static_priority;
		bool lifecycle_serial;
		if (!scheduler.LoadSlotConfiguration(slot, generation, initial_priority, minimum_priority, static_priority,
		                                     lifecycle_serial)) {
			ResetSlot(slot);
			return;
		}
		if (generations[slot] != generation) {
			ActivateNewSlot(slot, generation, initial_priority, minimum_priority, static_priority);
			if (lifecycle_serial) {
				pass_values[slot] = 0;
			}
			return;
		}
		active_slots.set(slot);
		pass_values[slot] = lifecycle_serial ? 0 : local_global_pass;
	};
	VisitMask(return_low, 0, apply_return);
	VisitMask(return_high, 64, apply_return);
	auto apply_finalization = [&](idx_t slot) {
		uint64_t generation;
		double initial_priority;
		double minimum_priority;
		bool static_priority;
		bool lifecycle_serial;
		if (active_slots.test(slot) &&
		    scheduler.LoadSlotConfiguration(slot, generation, initial_priority, minimum_priority, static_priority,
		                                    lifecycle_serial) &&
		    generations[slot] == generation && lifecycle_serial) {
			pass_values[slot] = 0;
		}
	};
	VisitMask(finalization_low, 0, apply_finalization);
	VisitMask(finalization_high, 64, apply_finalization);
	RecomputeGlobalStride();
	RebuildSortedSlots();
}

void QueryStrideWorkerState::UpdateAfterQuantum(QueryStrideScheduler &scheduler, idx_t slot, uint64_t generation,
                                                uint64_t elapsed_us) {
	if (slot >= QUERY_STRIDE_MAX_SLOTS || !active_slots.test(slot) || generations[slot] != generation) {
		return;
	}
	auto fraction = (static_cast<double>(elapsed_us) / 1000.0) / QueryStrideScheduler::REFERENCE_DURATION_MS;
	pass_values[slot] += strides[slot] * fraction;
	local_global_pass += local_global_stride * fraction;

	if (!static_priorities[slot]) {
		cpu_time_accum_us[slot] += elapsed_us;
		constexpr uint64_t DECAY_PERIOD_US = 2000;
		auto steps = static_cast<int>(cpu_time_accum_us[slot] / DECAY_PERIOD_US);
		cpu_time_accum_us[slot] %= DECAY_PERIOD_US;
		if (steps > 0) {
			auto old_steps = decay_steps[slot];
			decay_steps[slot] += steps;
			auto decay_start = scheduler.decay_start.load(std::memory_order_relaxed);
			auto effective_steps = std::max(0, decay_steps[slot] - decay_start) - std::max(0, old_steps - decay_start);
			if (effective_steps > 0) {
				auto lambda = scheduler.decay_lambda.load(std::memory_order_relaxed);
				priorities[slot] =
				    std::max(minimum_priorities[slot], priorities[slot] * std::pow(lambda, effective_steps));
				strides[slot] = QueryStrideScheduler::LARGE_CONSTANT / priorities[slot];
				RecomputeGlobalStride();
			}
		}
	}
	RebuildSortedSlots();
}

QueryStrideScheduler::QueryStrideScheduler(DatabaseInstance &db_p)
    : db(db_p), state(make_uniq<QueryStrideSchedulerState>()) {
}

QueryStrideScheduler::~QueryStrideScheduler() {
}

bool QueryStrideScheduler::Enabled() const {
	return db.GetQuerySchedulerPolicy() == QuerySchedulerPolicy::STRIDE;
}

void QueryStrideScheduler::RegisterQuery(const QueryRequestMetadata &metadata, ProducerToken &producer,
                                         double user_priority) {
	if (!Enabled()) {
		return;
	}
	lock_guard<mutex> guard(state->registration_lock);
	if (state->query_slots.find(metadata.db_query_id) != state->query_slots.end()) {
		throw InternalException("STRIDE query %llu was registered twice", metadata.db_query_id);
	}
	for (idx_t slot_index = 0; slot_index < QUERY_STRIDE_MAX_SLOTS; slot_index++) {
		auto &slot = state->slots[slot_index];
		if (slot.producer.load(std::memory_order_acquire)) {
			continue;
		}
		auto static_priority = metadata.stride_static_priority > 0;
		auto initial_priority =
		    (static_priority ? metadata.stride_static_priority : INITIAL_PRIORITY) * user_priority;
		auto minimum_priority = (static_priority ? metadata.stride_static_priority : MINIMUM_PRIORITY) * user_priority;
		if (!std::isfinite(initial_priority) || !std::isfinite(minimum_priority) || initial_priority <= 0 ||
		    minimum_priority <= 0) {
			throw InvalidInputException("Effective STRIDE priority must be finite and greater than zero");
		}
		auto generation = slot.generation.fetch_add(1, std::memory_order_acq_rel) + 1;
		(void)generation;
		slot.db_query_id = metadata.db_query_id;
		slot.request_id = metadata.request_id;
		slot.template_id = metadata.template_id;
		slot.scale_factor = metadata.scale_factor;
		slot.initial_priority.store(initial_priority, std::memory_order_relaxed);
		slot.minimum_priority.store(minimum_priority, std::memory_order_relaxed);
		slot.static_priority.store(static_priority, std::memory_order_relaxed);
		slot.lifecycle_serial.store(false, std::memory_order_relaxed);
		slot.task_set_generation.store(0, std::memory_order_relaxed);
		slot.task_set_active.store(false, std::memory_order_relaxed);
		slot.quanta.store(0, std::memory_order_relaxed);
		slot.worker_time_us.store(0, std::memory_order_relaxed);
		slot.arrival_time_ms = StrideNowMs();
		slot.producer.store(&producer, std::memory_order_release);
		state->query_slots.emplace(metadata.db_query_id, slot_index);
		state->active_count.fetch_add(1, std::memory_order_release);
		state->registered_queries.fetch_add(1, std::memory_order_relaxed);
		if (!state->optimizer_initialized.exchange(true, std::memory_order_acq_rel)) {
			StartTrackingLocked(*state, slot.arrival_time_ms);
			state->phase_start_ms.store(slot.arrival_time_ms, std::memory_order_relaxed);
			state->optimizer_phase.store(static_cast<int>(QueryStrideOptimizerPhase::TRACKING),
			                             std::memory_order_release);
		} else if (state->tracking_active) {
			state->present_at_window_start[slot_index] = false;
			state->quanta_at_window_start[slot_index] = 0;
			state->elapsed_at_window_start[slot_index] = 0;
		}
		return;
	}
	throw InvalidInputException(
	    "STRIDE supports at most 128 active queries; configure query_admission_max_active=128");
}

void QueryStrideScheduler::UnregisterQuery(uint64_t db_query_id) {
	lock_guard<mutex> guard(state->registration_lock);
	auto entry = state->query_slots.find(db_query_id);
	if (entry == state->query_slots.end()) {
		return;
	}
	auto slot_index = entry->second;
	auto &slot = state->slots[slot_index];
	if (state->tracking_active) {
		RecordTrackedSlotLocked(*state, slot_index, StrideNowMs());
	}
	slot.task_set_active.store(false, std::memory_order_release);
	slot.producer.store(nullptr, std::memory_order_release);
	slot.generation.fetch_add(1, std::memory_order_acq_rel);
	state->query_slots.erase(entry);
	state->active_count.fetch_sub(1, std::memory_order_release);
	state->unregistered_queries.fetch_add(1, std::memory_order_relaxed);
	while (slot.pins.load(std::memory_order_acquire) != 0) {
		std::this_thread::yield();
	}
}

void QueryStrideScheduler::OnEventScheduled(uint64_t db_query_id, Event &event) {
	idx_t slot_index;
	uint64_t previous_generation;
	bool lifecycle_serial;
	{
		lock_guard<mutex> guard(state->registration_lock);
		auto entry = state->query_slots.find(db_query_id);
		if (entry == state->query_slots.end()) {
			return;
		}
		slot_index = entry->second;
		auto &slot = state->slots[slot_index];
		previous_generation = slot.task_set_generation.fetch_add(1, std::memory_order_acq_rel);
		if (slot.task_set_active.exchange(true, std::memory_order_acq_rel)) {
			throw InternalException("STRIDE query %llu activated overlapping task sets", db_query_id);
		}
		lifecycle_serial = event.GetQueryActivationKind() != QueryActivationEventKind::PIPELINE;
		slot.lifecycle_serial.store(lifecycle_serial, std::memory_order_release);
		state->activated_task_sets.fetch_add(1, std::memory_order_relaxed);
	}
	if (previous_generation == 0) {
		state->change_task_sets.fetch_add(1, std::memory_order_relaxed);
		PushChange(slot_index);
	} else {
		state->return_task_sets.fetch_add(1, std::memory_order_relaxed);
		PushReturn(slot_index);
	}
	if (lifecycle_serial) {
		state->finalization_task_sets.fetch_add(1, std::memory_order_relaxed);
		PushFinalization(slot_index);
	}
	db.GetScheduler().Signal(db.GetScheduler().NumberOfThreads());
}

void QueryStrideScheduler::OnEventFinished(uint64_t db_query_id, Event &event) {
	(void)event;
	lock_guard<mutex> guard(state->registration_lock);
	auto entry = state->query_slots.find(db_query_id);
	if (entry == state->query_slots.end()) {
		return;
	}
	auto &slot = state->slots[entry->second];
	if (slot.task_set_active.exchange(false, std::memory_order_acq_rel)) {
		slot.lifecycle_serial.store(false, std::memory_order_release);
		state->finished_task_sets.fetch_add(1, std::memory_order_relaxed);
	}
}

void QueryStrideScheduler::RegisterWorker(QueryStrideWorkerState &worker) {
	lock_guard<mutex> guard(state->worker_lock);
	state->workers.push_back(&worker);
	worker.needs_full_refresh = true;
}

void QueryStrideScheduler::DeregisterWorker(QueryStrideWorkerState &worker) {
	lock_guard<mutex> guard(state->worker_lock);
	state->workers.erase(std::remove(state->workers.begin(), state->workers.end(), &worker), state->workers.end());
}

void QueryStrideScheduler::PushChange(idx_t slot) {
	lock_guard<mutex> guard(state->worker_lock);
	for (auto worker : state->workers) {
		SetMaskBit(worker->change_mask_low, worker->change_mask_high, slot);
	}
}

void QueryStrideScheduler::PushFinalization(idx_t slot) {
	lock_guard<mutex> guard(state->worker_lock);
	for (auto worker : state->workers) {
		SetMaskBit(worker->finalization_mask_low, worker->finalization_mask_high, slot);
	}
}

void QueryStrideScheduler::PushReturn(idx_t slot) {
	lock_guard<mutex> guard(state->worker_lock);
	for (auto worker : state->workers) {
		SetMaskBit(worker->return_mask_low, worker->return_mask_high, slot);
	}
}

bool QueryStrideScheduler::LoadSlotConfiguration(idx_t slot_index, uint64_t &generation, double &initial_priority,
                                                double &minimum_priority, bool &static_priority,
                                                bool &lifecycle_serial) const {
	if (slot_index >= QUERY_STRIDE_MAX_SLOTS) {
		return false;
	}
	auto &slot = state->slots[slot_index];
	auto producer = slot.producer.load(std::memory_order_acquire);
	if (!producer) {
		return false;
	}
	generation = slot.generation.load(std::memory_order_acquire);
	initial_priority = slot.initial_priority.load(std::memory_order_relaxed);
	minimum_priority = slot.minimum_priority.load(std::memory_order_relaxed);
	static_priority = slot.static_priority.load(std::memory_order_relaxed);
	lifecycle_serial = slot.lifecycle_serial.load(std::memory_order_relaxed);
	return slot.task_set_active.load(std::memory_order_acquire) &&
	       producer == slot.producer.load(std::memory_order_acquire) &&
	       generation == slot.generation.load(std::memory_order_acquire);
}

idx_t QueryStrideScheduler::EligibleWorkerCount(idx_t worker_count) const {
	return ComputeEligibleWorkerCount(ActiveQueryCount(), worker_count);
}

idx_t QueryStrideScheduler::ComputeEligibleWorkerCount(idx_t active, idx_t worker_count) {
	if (worker_count == 0) {
		return 0;
	}
	if (active <= QUERY_STRIDE_MAX_SLOTS / 2) {
		return worker_count;
	}
	auto remaining = QUERY_STRIDE_MAX_SLOTS - std::min(active, QUERY_STRIDE_MAX_SLOTS);
	return 1 + ((worker_count - 1) * remaining) / (QUERY_STRIDE_MAX_SLOTS / 2);
}

bool QueryStrideScheduler::IsWorkerEligible(idx_t slot_index, idx_t worker_id, idx_t worker_count) const {
	if (worker_count == 0 || worker_id == DConstants::INVALID_INDEX) {
		return false;
	}
	auto &slot = state->slots[slot_index];
	if (slot.lifecycle_serial.load(std::memory_order_acquire)) {
		return worker_id % worker_count == slot_index % worker_count;
	}
	auto eligible = EligibleWorkerCount(worker_count);
	auto rotated_worker = (worker_id + worker_count - (slot_index % worker_count)) % worker_count;
	return rotated_worker < eligible;
}

QueryStrideDequeueResult QueryStrideScheduler::TryDequeueTask(idx_t worker_id, QueryStrideWorkerState &worker,
                                                             shared_ptr<Task> &task,
                                                             QueryStrideTaskSelection &selection) {
	selection = QueryStrideTaskSelection();
	if (!Enabled() || ActiveQueryCount() == 0) {
		return QueryStrideDequeueResult::NOT_ACTIVE;
	}
	worker.PullUpdates(*this);
	auto worker_count = static_cast<idx_t>(db.GetScheduler().NumberOfThreads());
	for (idx_t rank = 0; rank < worker.sorted_count; rank++) {
		auto slot_index = worker.sorted_slots[rank];
		if (!IsWorkerEligible(slot_index, worker_id, worker_count)) {
			continue;
		}
		auto &slot = state->slots[slot_index];
		auto expected_generation = worker.generations[slot_index];
		auto producer = slot.producer.load(std::memory_order_acquire);
		if (!producer || slot.generation.load(std::memory_order_acquire) != expected_generation) {
			worker.ResetSlot(slot_index);
			continue;
		}
		if (!slot.task_set_active.load(std::memory_order_acquire)) {
			worker.DeactivateTaskSet(slot_index);
			state->worker_lazy_deactivations.fetch_add(1, std::memory_order_relaxed);
			continue;
		}
		slot.pins.fetch_add(1, std::memory_order_acquire);
		SlotPin pin {&slot};
		if (slot.producer.load(std::memory_order_acquire) != producer ||
		    slot.generation.load(std::memory_order_acquire) != expected_generation ||
		    !slot.task_set_active.load(std::memory_order_acquire)) {
			pin.Release();
			if (!slot.producer.load(std::memory_order_acquire) ||
			    slot.generation.load(std::memory_order_acquire) != expected_generation) {
				worker.ResetSlot(slot_index);
			} else {
				worker.DeactivateTaskSet(slot_index);
				state->worker_lazy_deactivations.fetch_add(1, std::memory_order_relaxed);
			}
			continue;
		}
		if (db.GetScheduler().GetTaskFromProducer(*producer, task)) {
			selection.slot = slot_index;
			selection.generation = expected_generation;
			return QueryStrideDequeueResult::TASK_FOUND;
		}
	}
	return QueryStrideDequeueResult::NO_TASK;
}

void QueryStrideScheduler::OnQuantumCompleted(QueryStrideWorkerState &worker,
                                              const QueryStrideTaskSelection &selection, uint64_t elapsed_us) {
	if (selection.slot >= QUERY_STRIDE_MAX_SLOTS) {
		return;
	}
	auto &slot = state->slots[selection.slot];
	if (slot.generation.load(std::memory_order_acquire) != selection.generation ||
	    !slot.producer.load(std::memory_order_acquire)) {
		worker.ResetSlot(selection.slot);
		return;
	}
	slot.quanta.fetch_add(1, std::memory_order_relaxed);
	slot.worker_time_us.fetch_add(elapsed_us, std::memory_order_relaxed);
	worker.UpdateAfterQuantum(*this, selection.slot, selection.generation, elapsed_us);
	MaybeAdvanceOptimizer();
}

void QueryStrideScheduler::MaybeAdvanceOptimizer() {
	if (!state->optimizer_initialized.load(std::memory_order_acquire)) {
		return;
	}
	auto phase = static_cast<QueryStrideOptimizerPhase>(state->optimizer_phase.load(std::memory_order_acquire));
	auto now_ms = StrideNowMs();
	auto elapsed_ms = now_ms - state->phase_start_ms.load(std::memory_order_relaxed);
	if (phase == QueryStrideOptimizerPhase::IDLE && elapsed_ms >= REFRESH_DURATION_MS) {
		int expected = static_cast<int>(QueryStrideOptimizerPhase::IDLE);
		if (!state->optimizer_phase.compare_exchange_strong(expected,
		                                                    static_cast<int>(QueryStrideOptimizerPhase::OPTIMIZING),
		                                                    std::memory_order_acq_rel)) {
			return;
		}
		{
			lock_guard<mutex> guard(state->registration_lock);
			StartTrackingLocked(*state, now_ms);
		}
		state->phase_start_ms.store(now_ms, std::memory_order_relaxed);
		state->optimizer_phase.store(static_cast<int>(QueryStrideOptimizerPhase::TRACKING),
		                             std::memory_order_release);
		return;
	}
	if (phase != QueryStrideOptimizerPhase::TRACKING || elapsed_ms < TRACKING_DURATION_MS) {
		return;
	}
	int expected = static_cast<int>(QueryStrideOptimizerPhase::TRACKING);
	if (!state->optimizer_phase.compare_exchange_strong(expected,
	                                                    static_cast<int>(QueryStrideOptimizerPhase::OPTIMIZING),
	                                                    std::memory_order_acq_rel)) {
		return;
	}
	vector<QueryStrideTraceEntry> workload;
	{
		lock_guard<mutex> guard(state->registration_lock);
		workload = StopTrackingLocked(*state, now_ms);
	}
	if (!workload.empty()) {
		auto result = QueryStrideOptimizer::Optimize(workload, static_cast<idx_t>(db.GetScheduler().NumberOfThreads()),
		                                            decay_lambda.load(std::memory_order_relaxed));
		decay_start.store(result.decay_start, std::memory_order_relaxed);
		decay_lambda.store(result.decay_lambda, std::memory_order_relaxed);
		state->last_objective.store(result.objective, std::memory_order_relaxed);
		state->optimization_count.fetch_add(1, std::memory_order_relaxed);
	}
	state->phase_start_ms.store(now_ms, std::memory_order_relaxed);
	state->optimizer_phase.store(static_cast<int>(QueryStrideOptimizerPhase::IDLE), std::memory_order_release);
}

idx_t QueryStrideScheduler::ActiveQueryCount() const {
	return state->active_count.load(std::memory_order_acquire);
}

vector<QueryStrideSchedulerSnapshot> QueryStrideScheduler::GetSnapshot() const {
	vector<QueryStrideSchedulerSnapshot> result;
	lock_guard<mutex> guard(state->registration_lock);
	auto worker_count = static_cast<idx_t>(db.GetScheduler().NumberOfThreads());
	for (idx_t slot_index = 0; slot_index < QUERY_STRIDE_MAX_SLOTS; slot_index++) {
		auto &slot = state->slots[slot_index];
		if (!slot.producer.load(std::memory_order_acquire)) {
			continue;
		}
		QueryStrideSchedulerSnapshot row;
		row.slot = slot_index;
		row.generation = slot.generation.load(std::memory_order_acquire);
		row.db_query_id = slot.db_query_id;
		row.request_id = slot.request_id;
		row.template_id = slot.template_id;
		row.scale_factor = slot.scale_factor;
		row.initial_priority = slot.initial_priority.load(std::memory_order_relaxed);
		row.minimum_priority = slot.minimum_priority.load(std::memory_order_relaxed);
		row.static_priority = slot.static_priority.load(std::memory_order_relaxed);
		row.task_set_active = slot.task_set_active.load(std::memory_order_relaxed);
		row.lifecycle_serial = slot.lifecycle_serial.load(std::memory_order_relaxed);
		row.eligible_workers = !row.task_set_active ? 0 : row.lifecycle_serial ? 1 : EligibleWorkerCount(worker_count);
		row.task_set_generation = slot.task_set_generation.load(std::memory_order_relaxed);
		row.quanta = slot.quanta.load(std::memory_order_relaxed);
		row.worker_time_us = slot.worker_time_us.load(std::memory_order_relaxed);
		result.push_back(row);
	}
	return result;
}

QueryStrideTuningSnapshot QueryStrideScheduler::GetTuningSnapshot() const {
	QueryStrideTuningSnapshot result;
	auto phase = static_cast<QueryStrideOptimizerPhase>(state->optimizer_phase.load(std::memory_order_acquire));
	result.phase = phase == QueryStrideOptimizerPhase::TRACKING
	                   ? "tracking"
	                   : phase == QueryStrideOptimizerPhase::OPTIMIZING ? "optimizing" : "idle";
	result.decay_start = decay_start.load(std::memory_order_relaxed);
	result.decay_lambda = decay_lambda.load(std::memory_order_relaxed);
	result.last_objective = state->last_objective.load(std::memory_order_relaxed);
	result.optimization_count = state->optimization_count.load(std::memory_order_relaxed);
	result.registered_queries = state->registered_queries.load(std::memory_order_relaxed);
	result.unregistered_queries = state->unregistered_queries.load(std::memory_order_relaxed);
	result.activated_task_sets = state->activated_task_sets.load(std::memory_order_relaxed);
	result.finished_task_sets = state->finished_task_sets.load(std::memory_order_relaxed);
	result.change_task_sets = state->change_task_sets.load(std::memory_order_relaxed);
	result.return_task_sets = state->return_task_sets.load(std::memory_order_relaxed);
	result.finalization_task_sets = state->finalization_task_sets.load(std::memory_order_relaxed);
	result.worker_lazy_deactivations = state->worker_lazy_deactivations.load(std::memory_order_relaxed);
	lock_guard<mutex> guard(state->registration_lock);
	result.tracked_query_count = state->tracked_workload.size();
	return result;
}

} // namespace duckdb
