#include "duckdb/storage/temporary_memory_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cmath>

namespace duckdb {

TemporaryMemoryState::TemporaryMemoryState(TemporaryMemoryManager &temporary_memory_manager_p,
                                           idx_t minimum_reservation_p)
    : temporary_memory_manager(temporary_memory_manager_p), remaining_size(0),
      minimum_reservation(minimum_reservation_p), reservation(0), materialization_penalty(1) {
}

TemporaryMemoryState::~TemporaryMemoryState() {
	temporary_memory_manager.Unregister(*this);
}

void TemporaryMemoryState::SetRemainingSize(idx_t new_remaining_size) {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
}

void TemporaryMemoryState::SetRemainingSizeAndUpdateReservation(ClientContext &context, idx_t new_remaining_size) {
	D_ASSERT(new_remaining_size != 0); // Use SetZero instead
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, new_remaining_size);
	temporary_memory_manager.UpdateState(context, *this);
}

void TemporaryMemoryState::SetZero() {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.SetRemainingSize(*this, 0);
	temporary_memory_manager.SetReservation(*this, 0);
}

idx_t TemporaryMemoryState::GetRemainingSize() const {
	return remaining_size;
}

void TemporaryMemoryState::SetMinimumReservation(idx_t new_minimum_reservation) {
	minimum_reservation = new_minimum_reservation;
}

idx_t TemporaryMemoryState::GetMinimumReservation() const {
	return minimum_reservation;
}

void TemporaryMemoryState::UpdateReservation(ClientContext &context) {
	auto guard = temporary_memory_manager.Lock();
	temporary_memory_manager.UpdateState(context, *this);
}

idx_t TemporaryMemoryState::GetReservation() const {
	return reservation;
}

void TemporaryMemoryState::SetMaterializationPenalty(idx_t new_materialization_penalty) {
	auto guard = temporary_memory_manager.Lock();
	materialization_penalty = new_materialization_penalty;
}

idx_t TemporaryMemoryState::GetMaterializationPenalty() const {
	return materialization_penalty;
}

TemporaryMemoryManager::TemporaryMemoryManager() : reservation(0), remaining_size(0) {
}

unique_lock<mutex> TemporaryMemoryManager::Lock() {
	return unique_lock<mutex>(lock);
}

idx_t TemporaryMemoryManager::DefaultMinimumReservation() const {
	return MinValue(num_threads * MINIMUM_RESERVATION_PER_STATE_PER_THREAD,
	                memory_limit / MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR);
}

void TemporaryMemoryManager::Unregister(TemporaryMemoryState &temporary_memory_state) {
	auto guard = Lock();

	SetReservation(temporary_memory_state, 0);
	SetRemainingSize(temporary_memory_state, 0);
	active_states.erase(temporary_memory_state);

	Verify();
}

void TemporaryMemoryManager::UpdateConfiguration(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &task_scheduler = TaskScheduler::GetScheduler(context);

	memory_limit =
	    LossyNumericCast<idx_t>(MAXIMUM_MEMORY_LIMIT_RATIO * static_cast<double>(buffer_manager.GetMaxMemory()));
	has_temporary_directory = buffer_manager.HasTemporaryDirectory();
	num_threads = NumericCast<idx_t>(task_scheduler.NumberOfThreads());
	num_connections = ConnectionManager::Get(context).GetConnectionCount();
	query_max_memory = buffer_manager.GetQueryMaxMemory();
}

TemporaryMemoryManager &TemporaryMemoryManager::Get(ClientContext &context) {
	return BufferManager::GetBufferManager(context).GetTemporaryMemoryManager();
}

unique_ptr<TemporaryMemoryState> TemporaryMemoryManager::Register(ClientContext &context) {
	auto guard = Lock();
	UpdateConfiguration(context);

	auto result = unique_ptr<TemporaryMemoryState>(new TemporaryMemoryState(*this, DefaultMinimumReservation()));
	SetRemainingSize(*result, result->GetMinimumReservation());
	SetReservation(*result, result->GetMinimumReservation());
	active_states.insert(*result);

	Verify();
	return result;
}

void TemporaryMemoryManager::UpdateState(ClientContext &context, TemporaryMemoryState &temporary_memory_state) {
	UpdateConfiguration(context);

	// The lower bound for the reservation of this state is either the minimum reservation or the remaining size
	const auto lower_bound =
	    MinValue(temporary_memory_state.GetMinimumReservation(), temporary_memory_state.GetRemainingSize());

	if (temporary_memory_state.GetRemainingSize() == 0) {
		// Sometimes set to 0 to denote end of state (before actually deleting the state)
		SetReservation(temporary_memory_state, 0);
	} else if (context.config.force_external) {
		// We're forcing external processing. Give it the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else if (!has_temporary_directory) {
		// We cannot offload, so we cannot limit memory usage. Set reservation equal to the remaining size
		SetReservation(temporary_memory_state, temporary_memory_state.GetRemainingSize());
	} else if (reservation - temporary_memory_state.GetReservation() + lower_bound >= memory_limit) {
		// We overshot. Set reservation equal to the minimum
		SetReservation(temporary_memory_state, lower_bound);
	} else {
		// The upper bound for the reservation of this state is the minimum of:
		// 1. Remaining size of the state
		// 2. The max memory per query
		// 3. MAXIMUM_FREE_MEMORY_RATIO * free memory
		auto upper_bound = MinValue(temporary_memory_state.GetRemainingSize(), query_max_memory);
		const auto free_memory = memory_limit - (reservation - temporary_memory_state.GetReservation());
		upper_bound = MinValue(upper_bound,
		                       LossyNumericCast<idx_t>(MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(free_memory)));
		upper_bound = MinValue(upper_bound, free_memory);

		idx_t new_reservation;
		if (lower_bound >= upper_bound) {
			new_reservation = lower_bound;
		} else {
			new_reservation = remaining_size > memory_limit ? ComputeReservation(temporary_memory_state) : upper_bound;
		}

		SetReservation(temporary_memory_state, new_reservation);
	}

	Verify();
}

void TemporaryMemoryManager::SetRemainingSize(TemporaryMemoryState &temporary_memory_state, idx_t new_remaining_size) {
	D_ASSERT(this->remaining_size >= temporary_memory_state.GetRemainingSize());
	this->remaining_size -= temporary_memory_state.GetRemainingSize();
	temporary_memory_state.remaining_size = new_remaining_size;
	this->remaining_size += temporary_memory_state.GetRemainingSize();
}

void TemporaryMemoryManager::SetReservation(TemporaryMemoryState &temporary_memory_state, idx_t new_reservation) {
	D_ASSERT(this->reservation >= temporary_memory_state.GetReservation());
	this->reservation -= temporary_memory_state.GetReservation();
	temporary_memory_state.reservation = new_reservation;
	this->reservation += temporary_memory_state.GetReservation();
}

//! Compute initial reservation for use in ComputeReservation
static idx_t ComputeInitialReservation(const TemporaryMemoryState &temporary_memory_state) {
	// Maximum of minimum reservation and the current reservation
	auto result = MaxValue(temporary_memory_state.GetMinimumReservation(), temporary_memory_state.GetReservation());
	// Bounded by the remaining size
	result = MinValue(result, temporary_memory_state.GetRemainingSize());
	// At least 1
	return MaxValue<idx_t>(result, 1);
}

static double ClampThroughput(double throughput) {
	static constexpr double MINIMUM_THROUGHPUT = 1e-300;
	if (!std::isfinite(throughput) || throughput <= MINIMUM_THROUGHPUT) {
		return MINIMUM_THROUGHPUT;
	}
	if (throughput > 1.0) {
		return 1.0;
	}
	return throughput;
}

static void ComputeDerivatives(const vector<reference<const TemporaryMemoryState>> &states, const vector<idx_t> &res,
                               vector<double> &der, const idx_t n) {
	// Cost function takes "throughput" (reservation / size) of each operator as its principal input
	double mat_cost = 0;
	double log_throughput_sum = 0;
	for (idx_t i = 0; i < n; i++) {
		auto &state = states[i].get();
		const auto resd = static_cast<double>(res[i]);
		const auto sizd = static_cast<double>(MaxValue<idx_t>(state.GetRemainingSize(), 1));
		const auto pend = static_cast<double>(state.GetMaterializationPenalty());
		const auto throughput = ClampThroughput(resd / sizd);
		log_throughput_sum += std::log(throughput);
		mat_cost += pend * (1 - throughput); // Materialization cost: sum of (1 - throughput)
	}
	const double nd = static_cast<double>(n);                          // n as double for convenience
	const double geomean_throughput = std::exp(log_throughput_sum / nd); // Geomean of throughputs
	const double tp_mult = 1 - geomean_throughput;                       // Throughput multiplier

	// Cost function: materialization cost * (1 - throughput multiplier), but we don't actually need to compute it
	// here. We need to compute the derivative with respect to every reservation, stored in "der"
	// Just use https://www.derivative-calculator.net with this (n = 3) to see what's going on
	// (3 - (a_1/s_1)-(a_2/s_2)-(a_3/s_3))*(1-((a_1/s_1)*(a_2/s_2)*(a_3/s_3))^(1/3))
	const double intermediate = -(geomean_throughput * mat_cost) / nd;
	for (idx_t i = 0; i < n; i++) {
		auto &state = states[i].get();
		const auto resd = static_cast<double>(MaxValue<idx_t>(res[i], 1));
		const auto sizd = static_cast<double>(MaxValue<idx_t>(state.GetRemainingSize(), 1));
		const auto pend = static_cast<double>(state.GetMaterializationPenalty());
		der[i] = intermediate / resd - pend * tp_mult / sizd;
		if (!std::isfinite(der[i])) {
			der[i] = NumericLimits<double>::Maximum();
		}
	}
}

idx_t TemporaryMemoryManager::ComputeReservation(const TemporaryMemoryState &temporary_memory_state) const {
	static constexpr idx_t OPTIMIZATION_ITERATIONS_MULTIPLIER = 5;
	static constexpr idx_t APPROXIMATE_RESERVATION_STATE_LIMIT = 32;

	// Use vectors for ease
	optional_idx state_index;
	vector<reference<const TemporaryMemoryState>> states;
	vector<idx_t> res;
	idx_t sum_of_initial_res = 0;

	idx_t i = 0;
	for (auto &state : active_states) {
		const auto initial_reservation = ComputeInitialReservation(state);
		sum_of_initial_res += initial_reservation;
		if (RefersToSameObject(state.get(), temporary_memory_state)) {
			state_index = i;
		} else if (initial_reservation >= state.get().GetRemainingSize()) {
			continue;
		}
		states.emplace_back(state);
		res.push_back(initial_reservation);
		i++;
	}
	const idx_t n = i;
	if (!state_index.IsValid()) {
		throw InternalException("Did not find state_index in ComputeReservation");
	}
	const auto current_state_index = state_index.GetIndex();

	if (sum_of_initial_res >= memory_limit) {
		return res[current_state_index];
	}
	const auto free_memory = memory_limit - sum_of_initial_res;

	if (n > APPROXIMATE_RESERVATION_STATE_LIMIT) {
		double total_weight = 0;
		double current_weight = 0;
		for (i = 0; i < n; i++) {
			auto &state = states[i].get();
			const auto room = state.GetRemainingSize() > res[i] ? state.GetRemainingSize() - res[i] : 0;
			double weight = 0;
			if (room > 0) {
				weight = static_cast<double>(MaxValue<idx_t>(state.GetMaterializationPenalty(), 1)) *
				         static_cast<double>(room);
				if (!std::isfinite(weight) || weight <= 0) {
					weight = 1;
				}
			}
			if (i == current_state_index) {
				current_weight = weight;
			}
			total_weight += weight;
		}

		auto &state = states[current_state_index].get();
		const auto initial_state_reservation = res[current_state_index];
		const auto state_room =
		    state.GetRemainingSize() > initial_state_reservation ? state.GetRemainingSize() - initial_state_reservation : 0;
		idx_t state_reservation = initial_state_reservation;
		if (state_room > 0 && total_weight > 0 && std::isfinite(total_weight) && current_weight > 0) {
			const auto share = static_cast<double>(free_memory) * current_weight / total_weight;
			idx_t extra_reservation;
			if (!std::isfinite(share) || share >= static_cast<double>(state_room)) {
				extra_reservation = state_room;
			} else {
				extra_reservation = LossyNumericCast<idx_t>(std::ceil(share));
				extra_reservation = MinValue(extra_reservation, state_room);
			}
			state_reservation += extra_reservation;
		}

		const auto state_remaining = initial_state_reservation + free_memory;
		auto upper_bound = LossyNumericCast<idx_t>(MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(state_remaining));
		auto num_other_states = MinValue(MAXIMUM_REMAINING_STATE_RESERVATIONS, num_connections);
		num_other_states = MaxValue(num_other_states, MINIMUM_REMAINING_STATE_RESERVATIONS);
		upper_bound = MinValue(upper_bound, num_other_states * DefaultMinimumReservation());
		state_reservation = MinValue(state_reservation, upper_bound);
		return MaxValue(state_reservation, initial_state_reservation);
	}

	// Distribute memory in OPTIMIZATION_ITERATIONS
	idx_t remaining_memory = free_memory;
	vector<double> der(n, NumericLimits<double>::Maximum());
	const idx_t optimization_iterations = OPTIMIZATION_ITERATIONS_MULTIPLIER * n;
	const idx_t max_optimization_iterations = optimization_iterations + n;
	for (idx_t opt_idx = 0; opt_idx < max_optimization_iterations && remaining_memory != 0; opt_idx++) {
		D_ASSERT(remaining_memory != 0);
		ComputeDerivatives(states, res, der, n);

		// Find the index of the state with the lowest derivative
		idx_t min_idx = 0;
		double min_der = NumericLimits<double>::Maximum();
		bool found_candidate = false;
		for (i = 0; i < n; i++) {
			auto &state = states[i].get();
			if (res[i] >= state.GetRemainingSize()) {
				continue; // We can't increase the reservation of "maxed" states, so we skip these
			}
			if (!std::isfinite(der[i])) {
				continue;
			}
			if (!found_candidate || der[i] < min_der) {
				min_idx = i;
				min_der = der[i];
				found_candidate = true;
			}
		}
		if (!found_candidate) {
			break;
		}
		auto &min_state = states[min_idx].get();

		// This is how much memory we will distribute in this round
		const auto remaining_iterations = opt_idx < optimization_iterations ? optimization_iterations - opt_idx : 1;
		const auto iter_memory = ExactNumericCast<idx_t>(
		    std::ceil(static_cast<double>(remaining_memory) / static_cast<double>(remaining_iterations)));

		// Compute how much we can add
		const auto state_room = min_state.GetRemainingSize() - res[min_idx];
		const auto delta = MinValue(iter_memory, state_room);
		D_ASSERT(delta <= remaining_memory);
		if (delta == 0) {
			break;
		}

		// Update counts
		res[min_idx] += delta;
		remaining_memory -= delta;
	}

	// We computed how the memory should be assigned to the states,
	// but we did not yet take into account the upper bound of MAXIMUM_FREE_MEMORY_RATIO * free_memory.
	// We don't simply bound this state's reservation because order matters,
	// and this function is called in arbitrary order for all active states.
	// So, we order the states by derivative (as if they got the max reservation),
	// and assign the states with the lowest derivate first.
	// This way, order does not matter _as much_, and the "best" states will _likely_ get more memory
	vector<idx_t> idxs(n, 0);
	vector<idx_t> max_res(n, 0);
	for (i = 0; i < n; i++) {
		idxs[i] = i;
		max_res[i] = states[i].get().GetRemainingSize() - 1;
	}
	ComputeDerivatives(states, max_res, der, n);
	std::sort(idxs.begin(), idxs.end(), [&](const idx_t &lhs, const idx_t &rhs) { return der[lhs] < der[rhs]; });

	// Loop through sorted indices until we encounter the state index
	remaining_memory = free_memory;
	for (const auto idx : idxs) {
		auto &state = states[idx].get();
		D_ASSERT(res[idx] <= state.GetRemainingSize());
		const auto initial_state_reservation = ComputeInitialReservation(state);
		// Bound by the ratio
		const auto state_remaining = initial_state_reservation + remaining_memory;
		auto upper_bound = LossyNumericCast<idx_t>(MAXIMUM_FREE_MEMORY_RATIO * static_cast<double>(state_remaining));
		// Or bound by leaving a number of minimum reservations for other states
		auto num_other_states = MinValue(MAXIMUM_REMAINING_STATE_RESERVATIONS, num_connections);
		num_other_states = MaxValue(num_other_states, MINIMUM_REMAINING_STATE_RESERVATIONS);
		upper_bound = MinValue(upper_bound, num_other_states * DefaultMinimumReservation());
		auto state_reservation = MinValue(res[idx], upper_bound);
		// But make sure it's never less than the initial reservation
		state_reservation = MaxValue(state_reservation, initial_state_reservation);
		// If this is the current state, we can just return
		if (idx == current_state_index) {
			return state_reservation;
		}
		// Decrement the remaining memory
		const auto delta = state_reservation - initial_state_reservation;
		remaining_memory -= delta;
	}

	// We cannot have gotten through the loop above without finding the current state
	throw InternalException("Did not find state_index in ComputeOptimalReservation");
}

void TemporaryMemoryManager::Verify() const {
#ifdef DEBUG
	idx_t total_reservation = 0;
	idx_t total_remaining_size = 0;
	for (auto &active_state : active_states) {
		total_reservation += active_state.get().GetReservation();
		total_remaining_size += active_state.get().GetRemainingSize();
	}
	D_ASSERT(total_reservation == this->reservation);
	D_ASSERT(total_remaining_size == this->remaining_size);
#endif
}

} // namespace duckdb
