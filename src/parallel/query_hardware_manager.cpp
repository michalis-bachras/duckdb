//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_hardware_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/parallel/query_hardware_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cerrno>
#include <cstring>
#include <deque>
#include <fstream>
#include <map>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <time.h>
#include <tuple>

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#endif

namespace duckdb {

struct CoreDomainKey {
	int socket_id = -1;
	int core_id = -1;

	bool operator<(const CoreDomainKey &other) const {
		if (socket_id != other.socket_id) {
			return socket_id < other.socket_id;
		}
		return core_id < other.core_id;
	}
};

struct WorkerTopology {
	idx_t worker_id = 0;
	int logical_cpu = -1;
	CoreDomainKey core;
};

namespace {

static constexpr uint64_t IA32_PERF_CTL = 0x199;
static constexpr uint64_t MSR_UNCORE_RATIO_LIMIT = 0x620;
static constexpr uint32_t SUPPORTED_INTEL_FAMILY = 6;
static constexpr uint32_t SUPPORTED_INTEL_MODEL = 79;

static uint64_t HardwareTimestampNs() {
	return NumericCast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static uint64_t HardwareThreadCPUNs() {
#if defined(CLOCK_THREAD_CPUTIME_ID)
	struct timespec value;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) == 0) {
		return NumericCast<uint64_t>(value.tv_sec) * 1000000000ULL + NumericCast<uint64_t>(value.tv_nsec);
	}
#endif
	return 0;
}

static uint64_t HardwareDurationNs(uint64_t end_ns, uint64_t start_ns) {
	return end_ns >= start_ns ? end_ns - start_ns : 0;
}

static void UpdateAtomicMaximum(std::atomic<uint64_t> &target, uint64_t value) {
	auto current = target.load(std::memory_order_relaxed);
	while (current < value &&
	       !target.compare_exchange_weak(current, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
	}
}

static double LookupCalibratedPower(const QueryHardwarePowerModelSnapshot &model, int socket_id,
	                                uint32_t core_frequency_khz, uint32_t uncore_frequency_khz) {
	for (const auto &entry : model.entries) {
		if (entry.socket_id == socket_id && entry.core_frequency_khz == core_frequency_khz &&
		    entry.uncore_frequency_khz == uncore_frequency_khz) {
			return entry.base_power_w;
		}
	}
	return 0;
}

static int ReadIntegerFile(const string &path) {
	std::ifstream input(path.c_str());
	int value = -1;
	if (!(input >> value)) {
		throw IOException("Could not read CPU topology from %s", path);
	}
	return value;
}

struct CPUDescriptor {
	string vendor;
	uint32_t family = 0;
	uint32_t model = 0;
};

static CPUDescriptor ReadCPUDescriptor() {
	CPUDescriptor result;
	std::ifstream input("/proc/cpuinfo");
	string line;
	while (std::getline(input, line)) {
		auto separator = line.find(':');
		if (separator == string::npos) {
			continue;
		}
		auto key = line.substr(0, separator);
		auto value = line.substr(separator + 1);
		StringUtil::Trim(key);
		StringUtil::Trim(value);
		if (key == "vendor_id") {
			result.vendor = value;
		} else if (key == "cpu family") {
			result.family = NumericCast<uint32_t>(std::stoul(value));
		} else if (key == "model") {
			result.model = NumericCast<uint32_t>(std::stoul(value));
		}
		if (!result.vendor.empty() && result.family != 0 && result.model != 0) {
			break;
		}
	}
	return result;
}

#ifdef __linux__
static uint64_t ReadMSR(int fd, uint64_t address) {
	uint64_t value = 0;
	if (pread(fd, &value, sizeof(value), NumericCast<off_t>(address)) != sizeof(value)) {
		throw IOException("Could not read MSR 0x%llx: %s", static_cast<unsigned long long>(address),
		                  std::strerror(errno));
	}
	return value;
}

static void WriteMSR(int fd, uint64_t address, uint64_t value) {
	if (pwrite(fd, &value, sizeof(value), NumericCast<off_t>(address)) != sizeof(value)) {
		throw IOException("Could not write MSR 0x%llx: %s", static_cast<unsigned long long>(address),
		                  std::strerror(errno));
	}
}
#endif

static uint32_t CoreRatioToKHz(uint64_t value) {
	return NumericCast<uint32_t>((value >> 8) & 0xffU) * 100000U;
}

static uint64_t SetCoreRatio(uint64_t value, uint32_t target_khz) {
	auto ratio = target_khz / 100000U;
	return (value & ~(0xffULL << 8)) | (static_cast<uint64_t>(ratio) << 8);
}

static uint32_t UncoreRatioToKHz(uint64_t value) {
	return NumericCast<uint32_t>(value & 0x7fU) * 100000U;
}

static uint64_t SetUncoreRatio(uint64_t value, uint32_t target_khz) {
	auto ratio = static_cast<uint64_t>(target_khz / 100000U);
	value &= ~((0x7fULL << 8) | 0x7fULL);
	return value | (ratio << 8) | ratio;
}

} // namespace

struct QueryHardwareManagerState {
	static constexpr idx_t MAX_TRANSITION_TRACE_ROWS = 250000;

	explicit QueryHardwareManagerState(DatabaseInstance &db_p) : db(db_p) {
		core_levels_khz = {1200000, 1500000, 1800000, 2200000, 2600000, 3000000, 3500000};
		uncore_levels_khz = {1200000, 1800000, 2200000, 2800000};
	}

	~QueryHardwareManagerState() {
		DeactivateLocked();
	}

	QueryRequestHardwareConfiguration AppliedConfigurationForWorkerLocked(idx_t worker_id) const {
		auto worker = workers.find(worker_id);
		if (worker == workers.end()) {
			return QueryRequestHardwareConfiguration();
		}
		QueryRequestHardwareConfiguration result;
		auto core = current_core_khz.find(worker->second.core);
		auto uncore = current_uncore_khz.find(worker->second.core.socket_id);
		result.core_frequency_khz = core == current_core_khz.end() ? Reference().core_frequency_khz : core->second;
		result.uncore_frequency_khz =
		    uncore == current_uncore_khz.end() ? Reference().uncore_frequency_khz : uncore->second;
		return result;
	}

	void AppendTransitionTrace(QueryHardwareTransitionSnapshot trace) {
		lock_guard<mutex> guard(transition_trace_lock);
		if (transition_trace.size() >= MAX_TRANSITION_TRACE_ROWS) {
			transition_trace.pop_front();
			transition_trace_dropped_count++;
		}
		transition_trace.push_back(std::move(trace));
	}

	WorkerTopology BuildWorkerTopology(idx_t worker_id, int logical_cpu) {
		WorkerTopology result;
		result.worker_id = worker_id;
		result.logical_cpu = logical_cpu;
		if (logical_cpu < 0) {
			return result;
		}
		auto prefix = StringUtil::Format("/sys/devices/system/cpu/cpu%d/topology/", logical_cpu);
		result.core.socket_id = ReadIntegerFile(prefix + "physical_package_id");
		result.core.core_id = ReadIntegerFile(prefix + "core_id");
		return result;
	}

	void RebuildTopologyDomainsLocked() {
		core_cpus.clear();
		socket_cpus.clear();
		for (const auto &entry : workers) {
			if (entry.second.logical_cpu < 0 || entry.second.core.socket_id < 0 || entry.second.core.core_id < 0) {
				continue;
			}
			core_cpus[entry.second.core].insert(entry.second.logical_cpu);
			socket_cpus[entry.second.core.socket_id].insert(entry.second.logical_cpu);
		}
	}

	void RegisterWorker(idx_t worker_id, int logical_cpu) {
		lock_guard<mutex> guard(lock);
		auto topology = BuildWorkerTopology(worker_id, logical_cpu);
		workers[worker_id] = topology;
		RebuildTopologyDomainsLocked();
		if (active && hardware_control_enabled && logical_cpu >= 0) {
			OpenCPULocked(logical_cpu);
			InitializeDomainStateLocked(topology);
		}
	}

	void UnregisterWorker(idx_t worker_id) {
		lock_guard<mutex> publish_guard(target_plan_lock);
		lock_guard<mutex> guard(lock);
		workers.erase(worker_id);
		worker_contexts.erase(worker_id);
		RebuildTopologyDomainsLocked();
	}

	void ValidatePlatformLocked() {
#ifndef __linux__
		throw NotImplementedException("SLA-energy direct MSR control is supported only on Linux");
#else
		auto descriptor = ReadCPUDescriptor();
		if (descriptor.vendor != "GenuineIntel" || descriptor.family != SUPPORTED_INTEL_FAMILY ||
		    descriptor.model != SUPPORTED_INTEL_MODEL) {
			throw InvalidInputException(
			    "SLA-energy MSR backend supports Intel family %u model %u; detected vendor=%s family=%u model=%u",
			    SUPPORTED_INTEL_FAMILY, SUPPORTED_INTEL_MODEL, descriptor.vendor, descriptor.family, descriptor.model);
		}
		if (workers.empty()) {
			throw InvalidInputException("SLA-energy hardware control requires registered pinned scheduler workers");
		}
		for (const auto &entry : workers) {
			if (entry.second.logical_cpu < 0) {
				throw InvalidInputException("SLA-energy worker %llu has no pinned logical CPU",
				                            static_cast<unsigned long long>(entry.first));
			}
		}
#endif
	}

	void OpenCPULocked(int logical_cpu) {
#ifdef __linux__
		if (msr_fds.find(logical_cpu) != msr_fds.end()) {
			return;
		}
		auto path = StringUtil::Format("/dev/cpu/%d/msr", logical_cpu);
		auto fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
		if (fd < 0) {
			throw IOException("Could not open %s for SLA-energy hardware control: %s", path, std::strerror(errno));
		}
		try {
			original_core_ctl[logical_cpu] = ReadMSR(fd, IA32_PERF_CTL);
		} catch (...) {
			close(fd);
			throw;
		}
		msr_fds[logical_cpu] = fd;
#else
		(void)logical_cpu;
#endif
	}

	void InitializeDomainStateLocked(const WorkerTopology &topology) {
		if (current_core_khz.find(topology.core) == current_core_khz.end()) {
			auto fd = msr_fds.find(topology.logical_cpu);
			current_core_khz[topology.core] =
			    fd == msr_fds.end() ? Reference().core_frequency_khz : CoreRatioToKHz(ReadMSR(fd->second, IA32_PERF_CTL));
		}
		if (current_uncore_khz.find(topology.core.socket_id) == current_uncore_khz.end()) {
			auto fd = msr_fds.find(topology.logical_cpu);
			if (fd == msr_fds.end()) {
				current_uncore_khz[topology.core.socket_id] = Reference().uncore_frequency_khz;
			} else {
				auto original = ReadMSR(fd->second, MSR_UNCORE_RATIO_LIMIT);
				original_uncore_limit[topology.core.socket_id] = original;
				uncore_representative_cpu[topology.core.socket_id] = topology.logical_cpu;
				current_uncore_khz[topology.core.socket_id] = UncoreRatioToKHz(original);
			}
		}
	}

	void ActivateLocked() {
		if (active) {
			return;
		}
		hardware_control_enabled = db.config.options.query_sla_energy_hardware_control_enabled;
		restoration_attempt_count = 0;
		restoration_verified_count = 0;
		restoration_failure_count = 0;
		restoration_status = hardware_control_enabled ? "pending" : "not_required";
		try {
			LoadPowerModelLocked();
			if (hardware_control_enabled) {
				if (!power_model.valid) {
					throw InvalidInputException(
					    "scheduler_policy=sla_energy requires query_sla_energy_power_model_path when hardware control is enabled");
				}
				ValidatePlatformLocked();
				for (const auto &entry : workers) {
					OpenCPULocked(entry.second.logical_cpu);
				}
			}
			for (const auto &entry : workers) {
				if (entry.second.logical_cpu >= 0) {
					InitializeDomainStateLocked(entry.second);
				}
			}
			auto reference = Reference();
			for (const auto &entry : core_cpus) {
				if (current_core_khz[entry.first] != reference.core_frequency_khz) {
					ApplyCoreLocked(entry.first, reference.core_frequency_khz);
				}
			}
			for (const auto &entry : socket_cpus) {
				if (current_uncore_khz[entry.first] != reference.uncore_frequency_khz) {
					ApplyUncoreLocked(entry.first, reference.uncore_frequency_khz);
				}
			}
			active = true;
			status = hardware_control_enabled ? "active_msr" : "active_dry_run";
		} catch (...) {
			// Mark the partially initialized manager active only for cleanup so DeactivateLocked restores any MSRs
			// already captured or modified before propagating the activation error.
			active = true;
			DeactivateLocked();
			throw;
		}
	}

	void LoadPowerModelLocked() {
		power_model = QueryHardwarePowerModelSnapshot();
		auto path = db.config.options.query_sla_energy_power_model_path;
		if (path.empty()) {
			power_model.minimum_core_frequency_khz = core_levels_khz.front();
			power_model.minimum_uncore_frequency_khz = uncore_levels_khz.front();
			return;
		}
		std::ifstream input(path.c_str());
		if (!input.good()) {
			throw IOException("Could not open SLA-energy power model at %s", path);
		}
		using PowerKey = std::tuple<int, uint32_t, uint32_t>;
		std::map<PowerKey, std::pair<double, idx_t>> aggregates;
		std::set<uint32_t> calibrated_core_levels;
		std::set<uint32_t> calibrated_uncore_levels;
		std::set<int> calibrated_sockets;
		bool hierarchy_schema = false;
		string line;
		while (std::getline(input, line)) {
			if (line.empty() || line[0] == '#') {
				continue;
			}
			std::stringstream parser(line);
			vector<string> fields;
			string field;
			while (std::getline(parser, field, ',')) {
				if (!field.empty() && field.back() == '\r') field.pop_back();
				fields.push_back(field);
			}
			if (fields.size() >= 8 && fields[0] == "socket_id") {
				hierarchy_schema = fields[5] == "idle_power_w" && fields[6] == "socket_awake_power_w" && fields[7] == "core_awake_power_w";
			}
			if (fields.size() < 5 || fields[0] == "socket_id") {
				continue;
			}
			try {
				auto socket_id = std::stoi(fields[0]);
				auto core_khz = NumericCast<uint32_t>(std::llround(std::stod(fields[1]) / 1000.0));
				auto uncore_khz = NumericCast<uint32_t>(std::llround(std::stod(fields[2]) / 1000.0));
				auto base_power_w = std::stod(fields[4]);
				if (socket_id < 0 || core_khz == 0 || uncore_khz == 0 || !std::isfinite(base_power_w) ||
				    base_power_w < 0) {
					continue;
				}
				auto &aggregate = aggregates[PowerKey(socket_id, core_khz, uncore_khz)];
				aggregate.first += base_power_w;
				aggregate.second++;
				calibrated_core_levels.insert(core_khz);
				calibrated_uncore_levels.insert(uncore_khz);
				calibrated_sockets.insert(socket_id);
			} catch (...) {
				continue;
			}
		}
		for (const auto &entry : aggregates) {
			QueryHardwarePowerCalibrationEntry calibration;
			calibration.socket_id = std::get<0>(entry.first);
			calibration.core_frequency_khz = std::get<1>(entry.first);
			calibration.uncore_frequency_khz = std::get<2>(entry.first);
			calibration.base_power_w = entry.second.first / static_cast<double>(entry.second.second);
			// Hierarchical rows use the same first five fields as the legacy table.
			std::ifstream hierarchy(path.c_str());
			string row;
			while (hierarchy_schema && std::getline(hierarchy, row)) {
				std::stringstream stream(row); vector<string> f; string v;
				while (std::getline(stream, v, ',')) f.push_back(v);
				if (f.size() < 8 || f[0] == "socket_id") continue;
				try {
					if (std::stoi(f[0]) == calibration.socket_id && std::llround(std::stod(f[1])/1000.0) == calibration.core_frequency_khz &&
					    std::llround(std::stod(f[2])/1000.0) == calibration.uncore_frequency_khz) {
						calibration.idle_w = std::stod(f[5]); calibration.awake_w = std::stod(f[6]); calibration.physical_core_w = std::stod(f[7]);
						calibration.hierarchy_valid = std::isfinite(calibration.idle_w) && std::isfinite(calibration.awake_w) &&
						    std::isfinite(calibration.physical_core_w) && calibration.idle_w >= 0 && calibration.awake_w >= 0 && calibration.physical_core_w >= 0;
					}
				} catch (...) {}
			}
			power_model.entries.push_back(calibration);
		}
		power_model.valid = !power_model.entries.empty();
		if (!power_model.valid) {
			throw InvalidInputException("SLA-energy power model %s contains no usable calibration rows", path);
		}
		core_levels_khz.assign(calibrated_core_levels.begin(), calibrated_core_levels.end());
		uncore_levels_khz.assign(calibrated_uncore_levels.begin(), calibrated_uncore_levels.end());
		if (db.config.options.query_sla_total_package) {
			core_levels_khz = {1200000, 2400000, 3500000};
			uncore_levels_khz = {2800000};
		}
		power_model.minimum_core_frequency_khz = core_levels_khz.front();
		power_model.minimum_uncore_frequency_khz = uncore_levels_khz.front();
		auto reference_core_khz = core_levels_khz.back();
		auto reference_uncore_khz = uncore_levels_khz.back();
		for (auto socket_id : calibrated_sockets) {
			if (LookupCalibratedPower(power_model, socket_id, reference_core_khz, reference_uncore_khz) <= 0) {
				throw InvalidInputException(
				    "SLA-energy power model %s has no reference calibration for socket %d at %u/%u kHz",
				    path, socket_id, reference_core_khz, reference_uncore_khz);
			}
		}
	}

	void RestoreHardwareLocked() {
#ifdef __linux__
		for (const auto &entry : original_core_ctl) {
			restoration_attempt_count++;
			auto fd = msr_fds.find(entry.first);
			if (fd == msr_fds.end()) {
				restoration_failure_count++;
				continue;
			}
			try {
					WriteMSR(fd->second, IA32_PERF_CTL, entry.second);
					msr_write_count++;
					core_msr_write_count++;
				if (ReadMSR(fd->second, IA32_PERF_CTL) == entry.second) {
					restoration_verified_count++;
				} else {
					restoration_failure_count++;
				}
			} catch (...) {
				restoration_failure_count++;
			}
		}
		for (const auto &entry : original_uncore_limit) {
			restoration_attempt_count++;
			auto cpu = uncore_representative_cpu.find(entry.first);
			if (cpu == uncore_representative_cpu.end()) {
				restoration_failure_count++;
				continue;
			}
			auto fd = msr_fds.find(cpu->second);
			if (fd == msr_fds.end()) {
				restoration_failure_count++;
				continue;
			}
			try {
					WriteMSR(fd->second, MSR_UNCORE_RATIO_LIMIT, entry.second);
					msr_write_count++;
					uncore_msr_write_count++;
				if (ReadMSR(fd->second, MSR_UNCORE_RATIO_LIMIT) == entry.second) {
					restoration_verified_count++;
				} else {
					restoration_failure_count++;
				}
			} catch (...) {
				restoration_failure_count++;
			}
		}
#endif
		if (restoration_attempt_count == 0) {
			restoration_status = "not_required";
		} else if (restoration_failure_count == 0 &&
		           restoration_verified_count == restoration_attempt_count) {
			restoration_status = "verified";
		} else {
			restoration_status = "failed";
		}
	}

	void DeactivateLocked() {
		auto was_active = active;
		auto controlled = hardware_control_enabled;
		if (was_active && controlled) {
			RestoreHardwareLocked();
		} else if (was_active) {
			restoration_status = "not_required";
		}
#ifdef __linux__
		for (const auto &entry : msr_fds) {
			close(entry.second);
		}
#endif
		msr_fds.clear();
		original_core_ctl.clear();
		original_uncore_limit.clear();
		uncore_representative_cpu.clear();
		current_core_khz.clear();
		current_uncore_khz.clear();
		worker_contexts.clear();
		power_model = QueryHardwarePowerModelSnapshot();
		active = false;
		hardware_control_enabled = false;
		if (was_active) {
			auto restoration_ok = restoration_status == "verified" || restoration_status == "not_required";
			status = controlled ? (restoration_ok ? "inactive_restored" : "inactive_restoration_failed")
			                    : "inactive_dry_run";
		}
		std::atomic_store_explicit(&published_plan, std::shared_ptr<const QueryHardwareTargetPlan>(),
		                           std::memory_order_release);
	}

	void ApplyCoreLocked(const CoreDomainKey &domain, uint32_t target_khz) {
		auto wall_start_ns = HardwareTimestampNs();
		auto cpu_start_ns = HardwareThreadCPUNs();
		if (hardware_control_enabled) {
#ifdef __linux__
			auto cpus = core_cpus.find(domain);
			if (cpus == core_cpus.end() || cpus->second.empty()) {
				throw InternalException("Missing logical CPUs for SLA-energy core domain");
			}
			for (auto cpu : cpus->second) {
				OpenCPULocked(cpu);
				auto fd = msr_fds[cpu];
				auto current = ReadMSR(fd, IA32_PERF_CTL);
				auto requested = SetCoreRatio(current, target_khz);
				WriteMSR(fd, IA32_PERF_CTL, requested);
				msr_write_count++;
				core_msr_write_count++;
				auto verified = ReadMSR(fd, IA32_PERF_CTL);
				if (CoreRatioToKHz(verified) != target_khz) {
					verification_failure_count++;
					throw IOException("SLA-energy core MSR verification failed for CPU %d", cpu);
				}
			}
#endif
		}
		current_core_khz[domain] = target_khz;
		core_transition_count.fetch_add(1, std::memory_order_relaxed);
		auto wall_ns = HardwareDurationNs(HardwareTimestampNs(), wall_start_ns);
		auto cpu_end_ns = HardwareThreadCPUNs();
		auto cpu_ns = cpu_start_ns == 0 || cpu_end_ns == 0 ? 0 : HardwareDurationNs(cpu_end_ns, cpu_start_ns);
		hardware_apply_wall_ns.fetch_add(wall_ns, std::memory_order_relaxed);
		hardware_apply_thread_cpu_ns.fetch_add(cpu_ns, std::memory_order_relaxed);
		UpdateAtomicMaximum(hardware_apply_max_wall_ns, wall_ns);
	}

	void ApplyUncoreLocked(int socket_id, uint32_t target_khz) {
		auto wall_start_ns = HardwareTimestampNs();
		auto cpu_start_ns = HardwareThreadCPUNs();
		if (hardware_control_enabled) {
#ifdef __linux__
			auto representative = uncore_representative_cpu.find(socket_id);
			if (representative == uncore_representative_cpu.end()) {
				throw InternalException("Missing representative CPU for SLA-energy socket domain");
			}
			OpenCPULocked(representative->second);
			auto fd = msr_fds[representative->second];
			auto current = ReadMSR(fd, MSR_UNCORE_RATIO_LIMIT);
			auto requested = SetUncoreRatio(current, target_khz);
			WriteMSR(fd, MSR_UNCORE_RATIO_LIMIT, requested);
			msr_write_count++;
			uncore_msr_write_count++;
			auto verified = ReadMSR(fd, MSR_UNCORE_RATIO_LIMIT);
			if (UncoreRatioToKHz(verified) != target_khz || NumericCast<uint32_t>((verified >> 8) & 0x7fU) * 100000U != target_khz) {
				verification_failure_count++;
				throw IOException("SLA-energy uncore MSR verification failed for socket %d", socket_id);
			}
#endif
		}
		current_uncore_khz[socket_id] = target_khz;
		uncore_transition_count.fetch_add(1, std::memory_order_relaxed);
		auto wall_ns = HardwareDurationNs(HardwareTimestampNs(), wall_start_ns);
		auto cpu_end_ns = HardwareThreadCPUNs();
		auto cpu_ns = cpu_start_ns == 0 || cpu_end_ns == 0 ? 0 : HardwareDurationNs(cpu_end_ns, cpu_start_ns);
		hardware_apply_wall_ns.fetch_add(wall_ns, std::memory_order_relaxed);
		hardware_apply_thread_cpu_ns.fetch_add(cpu_ns, std::memory_order_relaxed);
		UpdateAtomicMaximum(hardware_apply_max_wall_ns, wall_ns);
	}

	void PublishTargetsSerial(std::shared_ptr<const QueryHardwareTargetPlan> plan) {
		if (!plan) {
			return;
		}
		std::atomic_store_explicit(&published_plan, plan, std::memory_order_release);
		auto lock_wait_start_ns = HardwareTimestampNs();
		unique_lock<mutex> guard(lock);
		manager_lock_wait_ns.fetch_add(HardwareDurationNs(HardwareTimestampNs(), lock_wait_start_ns),
		                               std::memory_order_relaxed);
		if (!active) {
			return;
		}
		std::map<CoreDomainKey, uint32_t> desired_core;
		std::map<int, uint32_t> desired_uncore;
		std::unordered_map<idx_t, QueryHardwareWorkerTarget> worker_targets;
		for (const auto &target : plan->workers) {
			worker_targets[target.worker_id] = target;
		}
		for (const auto &entry : workers) {
			auto target_entry = worker_targets.find(entry.first);
			auto target = target_entry == worker_targets.end() || !target_entry->second.assigned
			                  ? Minimum()
			                  : target_entry->second.hardware;
			if (!target.IsValid()) {
				target = Reference();
			}
			auto &core_target = desired_core[entry.second.core];
			core_target = MaxValue<uint32_t>(core_target, target.core_frequency_khz);
			auto &uncore_target = desired_uncore[entry.second.core.socket_id];
			uncore_target = MaxValue<uint32_t>(uncore_target, target.uncore_frequency_khz);
		}
		for (const auto &entry : desired_core) {
			auto current = current_core_khz[entry.first];
			if (entry.second != current) {
				ApplyCoreLocked(entry.first, entry.second);
			}
		}
		for (const auto &entry : desired_uncore) {
			auto current = current_uncore_khz[entry.first];
			if (entry.second != current) {
				ApplyUncoreLocked(entry.first, entry.second);
			}
		}
		applied_generation = plan->generation;
	}

	void PublishTargets(std::shared_ptr<const QueryHardwareTargetPlan> plan) {
		publish_call_count.fetch_add(1, std::memory_order_relaxed);
		auto lock_wait_start_ns = HardwareTimestampNs();
		unique_lock<mutex> publish_guard(target_plan_lock);
		target_plan_lock_wait_ns.fetch_add(HardwareDurationNs(HardwareTimestampNs(), lock_wait_start_ns),
		                                  std::memory_order_relaxed);
		PublishTargetsSerial(std::move(plan));
	}

	void UpdateWorkerTarget(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware, bool assigned,
	                        uint64_t generation, const QueryHardwareTransitionContext &context) {
		auto update_start_ns = HardwareTimestampNs();
		auto update_cpu_start_ns = HardwareThreadCPUNs();
		worker_target_update_count.fetch_add(1, std::memory_order_relaxed);
		auto lock_wait_start_ns = HardwareTimestampNs();
		unique_lock<mutex> publish_guard(target_plan_lock);
		auto target_lock_wait = HardwareDurationNs(HardwareTimestampNs(), lock_wait_start_ns);
		target_plan_lock_wait_ns.fetch_add(target_lock_wait, std::memory_order_relaxed);
		auto current = std::atomic_load_explicit(&published_plan, std::memory_order_acquire);
		QueryHardwareWorkerTarget previous_target;
		bool previous_target_found = false;
		if (current) {
			for (const auto &target : current->workers) {
				if (target.worker_id == worker_id) {
					previous_target = target;
					previous_target_found = true;
					break;
				}
			}
		}
		QueryHardwareTransitionContext previous_context;
		auto context_entry = worker_contexts.find(worker_id);
		if (context_entry != worker_contexts.end()) {
			previous_context = context_entry->second;
		}
		auto trace_context = assigned ? context : previous_context;
		auto trace_enabled = context.trace_enabled || previous_context.trace_enabled;
		auto owner_same = !assigned ||
		                  (previous_context.db_query_id != 0 && previous_context.db_query_id == context.db_query_id &&
		                   previous_context.pipeline_generation == context.pipeline_generation &&
		                   previous_context.mandatory == context.mandatory &&
		                   previous_context.optional == context.optional);
		auto target_same = previous_target_found && previous_target.assigned == assigned && owner_same &&
		                   (!assigned || previous_target.hardware == hardware);
		auto lease_handoff = target_same && assigned && previous_context.lease_generation != 0 &&
		                     previous_context.lease_generation != context.lease_generation;

		QueryHardwareTransitionSnapshot trace;
		if (trace_enabled) {
			trace.timestamp_ns = update_start_ns;
			trace.plan_generation = generation;
			trace.db_query_id = trace_context.db_query_id;
			trace.request_id = trace_context.request_id;
			trace.pipeline_generation = trace_context.pipeline_generation;
			trace.lease_generation = trace_context.lease_generation;
			trace.pipeline_id = trace_context.pipeline_id;
			trace.worker_id = worker_id;
			trace.mandatory = trace_context.mandatory;
			trace.optional = trace_context.optional;
			trace.previous_assigned = previous_target_found && previous_target.assigned;
			trace.assigned = assigned;
			trace.previous_hardware =
			    previous_target_found ? previous_target.hardware : QueryRequestHardwareConfiguration();
			trace.requested_hardware = assigned ? hardware : Minimum();
			auto manager_wait_start_ns = HardwareTimestampNs();
			unique_lock<mutex> manager_guard(lock);
			manager_lock_wait_ns.fetch_add(HardwareDurationNs(HardwareTimestampNs(), manager_wait_start_ns),
			                               std::memory_order_relaxed);
			auto topology = workers.find(worker_id);
			if (topology != workers.end()) {
				trace.logical_cpu = topology->second.logical_cpu;
				trace.socket_id = topology->second.core.socket_id;
				trace.physical_core_id = topology->second.core.core_id;
			}
			trace.applied_before = AppliedConfigurationForWorkerLocked(worker_id);
			trace.msr_write_delta = msr_write_count;
		}

		if (target_same) {
			worker_target_noop_count.fetch_add(1, std::memory_order_relaxed);
			if (lease_handoff) {
				worker_lease_handoff_count.fetch_add(1, std::memory_order_relaxed);
			}
			if (assigned) {
				worker_contexts[worker_id] = context;
			} else {
				worker_contexts.erase(worker_id);
			}
			publish_guard.unlock();
			auto update_wall_ns = HardwareDurationNs(HardwareTimestampNs(), update_start_ns);
			auto update_cpu_end_ns = HardwareThreadCPUNs();
			auto update_cpu_ns = update_cpu_start_ns == 0 || update_cpu_end_ns == 0
			                         ? 0
			                         : HardwareDurationNs(update_cpu_end_ns, update_cpu_start_ns);
			target_update_wall_ns.fetch_add(update_wall_ns, std::memory_order_relaxed);
			target_update_thread_cpu_ns.fetch_add(update_cpu_ns, std::memory_order_relaxed);
			UpdateAtomicMaximum(target_update_max_wall_ns, update_wall_ns);
			if (trace_enabled) {
				trace.action = lease_handoff ? "lease_handoff" : "noop";
				trace.applied_after = trace.applied_before;
				trace.msr_write_delta = 0;
				trace.target_plan_lock_wait_ns = target_lock_wait;
				trace.update_wall_ns = update_wall_ns;
				trace.update_thread_cpu_ns = update_cpu_ns;
				AppendTransitionTrace(std::move(trace));
			}
			return;
		}

		auto previous_assigned = previous_target_found && previous_target.assigned;
		if (!previous_assigned && assigned) {
			worker_assignment_count.fetch_add(1, std::memory_order_relaxed);
			trace.action = "assign";
		} else if (previous_assigned && !assigned) {
			worker_release_count.fetch_add(1, std::memory_order_relaxed);
			trace.action = "release";
		} else if (previous_assigned && assigned) {
			worker_retarget_count.fetch_add(1, std::memory_order_relaxed);
			trace.action = "retarget";
		} else {
			trace.action = "initialize_idle";
		}
		auto next = std::make_shared<QueryHardwareTargetPlan>();
		if (current) {
			*next = *current;
		}
		next->generation = MaxValue<uint64_t>(next->generation, generation);
		bool found = false;
		for (auto &target : next->workers) {
			if (target.worker_id != worker_id) {
				continue;
			}
			target.hardware = hardware;
			target.assigned = assigned;
			found = true;
			break;
		}
		if (!found) {
			QueryHardwareWorkerTarget target;
			target.worker_id = worker_id;
			target.hardware = hardware;
			target.assigned = assigned;
			next->workers.push_back(target);
		}
		if (assigned) {
			worker_contexts[worker_id] = context;
		} else {
			worker_contexts.erase(worker_id);
		}
		PublishTargetsSerial(std::move(next));
		if (trace_enabled) {
			auto manager_wait_start_ns = HardwareTimestampNs();
			unique_lock<mutex> manager_guard(lock);
			manager_lock_wait_ns.fetch_add(HardwareDurationNs(HardwareTimestampNs(), manager_wait_start_ns),
			                               std::memory_order_relaxed);
			trace.applied_after = AppliedConfigurationForWorkerLocked(worker_id);
			trace.msr_write_delta = msr_write_count - trace.msr_write_delta;
			trace.core_changed =
			    trace.applied_before.core_frequency_khz != trace.applied_after.core_frequency_khz;
			trace.uncore_changed =
			    trace.applied_before.uncore_frequency_khz != trace.applied_after.uncore_frequency_khz;
		}
		publish_guard.unlock();
		auto update_wall_ns = HardwareDurationNs(HardwareTimestampNs(), update_start_ns);
		auto update_cpu_end_ns = HardwareThreadCPUNs();
		auto update_cpu_ns = update_cpu_start_ns == 0 || update_cpu_end_ns == 0
		                         ? 0
		                         : HardwareDurationNs(update_cpu_end_ns, update_cpu_start_ns);
		target_update_wall_ns.fetch_add(update_wall_ns, std::memory_order_relaxed);
		target_update_thread_cpu_ns.fetch_add(update_cpu_ns, std::memory_order_relaxed);
		UpdateAtomicMaximum(target_update_max_wall_ns, update_wall_ns);
		if (trace_enabled) {
			trace.target_plan_lock_wait_ns = target_lock_wait;
			trace.update_wall_ns = update_wall_ns;
			trace.update_thread_cpu_ns = update_cpu_ns;
			AppendTransitionTrace(std::move(trace));
		}
	}

	bool IsWorkerAtConfiguration(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware) const {
		if (!hardware.IsValid()) {
			return false;
		}
		lock_guard<mutex> guard(lock);
		if (!active) {
			return false;
		}
		auto worker = workers.find(worker_id);
		if (worker == workers.end()) {
			return false;
		}
		auto core = current_core_khz.find(worker->second.core);
		auto uncore = current_uncore_khz.find(worker->second.core.socket_id);
		return core != current_core_khz.end() && uncore != current_uncore_khz.end() &&
		       core->second == hardware.core_frequency_khz && uncore->second == hardware.uncore_frequency_khz;
	}

	QueryRequestHardwareConfiguration GetAppliedConfigurationForCPU(int logical_cpu) const {
		lock_guard<mutex> guard(lock);
		for (const auto &entry : workers) {
			if (entry.second.logical_cpu != logical_cpu) {
				continue;
			}
			return AppliedConfigurationForWorkerLocked(entry.first);
		}
		return QueryRequestHardwareConfiguration();
	}

	QueryHardwareWorkerDomainContext GetDomainContextExcludingWorker(idx_t worker_id) const {
		lock_guard<mutex> publish_guard(target_plan_lock);
		lock_guard<mutex> guard(lock);
		QueryHardwareWorkerDomainContext result;
		auto worker = workers.find(worker_id);
		auto plan = std::atomic_load_explicit(&published_plan, std::memory_order_acquire);
		if (worker == workers.end() || !plan) {
			return result;
		}
		result.plan_generation = plan->generation;
		for (const auto &target : plan->workers) {
			if (!target.assigned || target.worker_id == worker_id) {
				continue;
			}
			auto other = workers.find(target.worker_id);
			if (other == workers.end()) {
				continue;
			}
			if (other->second.core.socket_id != worker->second.core.socket_id) {
				continue;
			}
			result.socket_active_without_worker = true;
			result.requested_hardware.uncore_frequency_khz =
			    MaxValue<uint32_t>(result.requested_hardware.uncore_frequency_khz,
			                       target.hardware.uncore_frequency_khz);
			if (other->second.core.core_id == worker->second.core.core_id) {
				result.core_active_without_worker = true;
				result.requested_hardware.core_frequency_khz =
				    MaxValue<uint32_t>(result.requested_hardware.core_frequency_khz,
				                       target.hardware.core_frequency_khz);
			}
		}
		return result;
	}

	QueryRequestHardwareConfiguration Reference() const {
		QueryRequestHardwareConfiguration result;
		result.core_frequency_khz = core_levels_khz.back();
		result.uncore_frequency_khz = uncore_levels_khz.back();
		return result;
	}

	QueryRequestHardwareConfiguration Minimum() const {
		QueryRequestHardwareConfiguration result;
		result.core_frequency_khz = core_levels_khz.front();
		result.uncore_frequency_khz = uncore_levels_khz.front();
		return result;
	}

	DatabaseInstance &db;
	mutable mutex target_plan_lock;
	mutable mutex lock;
	bool active = false;
	bool hardware_control_enabled = false;
	string status = "inactive";
	vector<uint32_t> core_levels_khz;
	vector<uint32_t> uncore_levels_khz;
	QueryHardwarePowerModelSnapshot power_model;
	std::unordered_map<idx_t, WorkerTopology> workers;
	std::map<CoreDomainKey, std::set<int>> core_cpus;
	std::map<int, std::set<int>> socket_cpus;
	std::map<int, int> msr_fds;
	std::map<int, uint64_t> original_core_ctl;
	std::map<int, uint64_t> original_uncore_limit;
	std::map<int, int> uncore_representative_cpu;
	std::map<CoreDomainKey, uint32_t> current_core_khz;
	std::map<int, uint32_t> current_uncore_khz;
	std::unordered_map<idx_t, QueryHardwareTransitionContext> worker_contexts;
	std::shared_ptr<const QueryHardwareTargetPlan> published_plan;
	uint64_t applied_generation = 0;
	uint64_t msr_write_count = 0;
	uint64_t core_msr_write_count = 0;
	uint64_t uncore_msr_write_count = 0;
	std::atomic<uint64_t> publish_call_count {0};
	std::atomic<uint64_t> worker_target_update_count {0};
	std::atomic<uint64_t> worker_target_noop_count {0};
	std::atomic<uint64_t> worker_assignment_count {0};
	std::atomic<uint64_t> worker_release_count {0};
	std::atomic<uint64_t> worker_retarget_count {0};
	std::atomic<uint64_t> worker_lease_handoff_count {0};
	std::atomic<uint64_t> core_transition_count {0};
	std::atomic<uint64_t> uncore_transition_count {0};
	std::atomic<uint64_t> target_plan_lock_wait_ns {0};
	std::atomic<uint64_t> manager_lock_wait_ns {0};
	std::atomic<uint64_t> target_update_wall_ns {0};
	std::atomic<uint64_t> target_update_thread_cpu_ns {0};
	std::atomic<uint64_t> target_update_max_wall_ns {0};
	std::atomic<uint64_t> hardware_apply_wall_ns {0};
	std::atomic<uint64_t> hardware_apply_thread_cpu_ns {0};
	std::atomic<uint64_t> hardware_apply_max_wall_ns {0};
	mutable mutex transition_trace_lock;
	std::deque<QueryHardwareTransitionSnapshot> transition_trace;
	uint64_t transition_trace_dropped_count = 0;
	uint64_t verification_failure_count = 0;
	uint64_t restoration_attempt_count = 0;
	uint64_t restoration_verified_count = 0;
	uint64_t restoration_failure_count = 0;
	string restoration_status = "not_required";
};

QueryHardwareManager::QueryHardwareManager(DatabaseInstance &db_p)
    : db(db_p), state(make_uniq<QueryHardwareManagerState>(db_p)) {
}

QueryHardwareManager::~QueryHardwareManager() {
}

void QueryHardwareManager::RegisterWorker(idx_t worker_id, int logical_cpu) {
	state->RegisterWorker(worker_id, logical_cpu);
}

void QueryHardwareManager::UnregisterWorker(idx_t worker_id) {
	state->UnregisterWorker(worker_id);
}

void QueryHardwareManager::Activate() {
	lock_guard<mutex> guard(state->lock);
	state->ActivateLocked();
}

void QueryHardwareManager::Deactivate() {
	lock_guard<mutex> publish_guard(state->target_plan_lock);
	lock_guard<mutex> guard(state->lock);
	state->DeactivateLocked();
}

void QueryHardwareManager::PublishTargets(std::shared_ptr<const QueryHardwareTargetPlan> plan) {
	state->PublishTargets(std::move(plan));
}

void QueryHardwareManager::UpdateWorkerTarget(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware,
	                                          bool assigned, uint64_t generation,
	                                          const QueryHardwareTransitionContext &context) {
	state->UpdateWorkerTarget(worker_id, hardware, assigned, generation, context);
}

bool QueryHardwareManager::IsWorkerAtConfiguration(idx_t worker_id,
                                                    const QueryRequestHardwareConfiguration &hardware) const {
	return state->IsWorkerAtConfiguration(worker_id, hardware);
}

QueryRequestHardwareConfiguration QueryHardwareManager::GetAppliedConfigurationForCPU(int logical_cpu) const {
	return state->GetAppliedConfigurationForCPU(logical_cpu);
}

QueryHardwareWorkerDomainContext QueryHardwareManager::GetDomainContextExcludingWorker(idx_t worker_id) const {
	return state->GetDomainContextExcludingWorker(worker_id);
}

QueryRequestHardwareConfiguration QueryHardwareManager::ReferenceConfiguration() const {
	return state->Reference();
}

QueryRequestHardwareConfiguration QueryHardwareManager::MinimumConfiguration() const {
	return state->Minimum();
}

const vector<uint32_t> &QueryHardwareManager::CoreFrequencyLevelsKHz() const {
	return state->core_levels_khz;
}

const vector<uint32_t> &QueryHardwareManager::UncoreFrequencyLevelsKHz() const {
	return state->uncore_levels_khz;
}

double QueryHardwarePowerModelSnapshot::SocketIncrementalPowerW(int socket_id,
	                                                            uint32_t uncore_frequency_khz) const {
	if (!valid) {
		return -1;
	}
	auto baseline = LookupCalibratedPower(*this, socket_id, minimum_core_frequency_khz,
	                                     minimum_uncore_frequency_khz);
	auto target = LookupCalibratedPower(*this, socket_id, minimum_core_frequency_khz, uncore_frequency_khz);
	if (baseline <= 0 || target <= 0) {
		return -1;
	}
	return MaxValue<double>(0, target - baseline);
}

double QueryHardwarePowerModelSnapshot::CoreIncrementalPowerW(int socket_id, uint32_t core_frequency_khz,
	                                                          uint32_t uncore_frequency_khz,
	                                                          idx_t physical_cores_on_socket) const {
	if (!valid || physical_cores_on_socket == 0) {
		return -1;
	}
	auto baseline = LookupCalibratedPower(*this, socket_id, minimum_core_frequency_khz, uncore_frequency_khz);
	auto target = LookupCalibratedPower(*this, socket_id, core_frequency_khz, uncore_frequency_khz);
	if (baseline <= 0 || target <= 0) {
		return -1;
	}
	return MaxValue<double>(0, target - baseline) / static_cast<double>(physical_cores_on_socket);
}

vector<QueryHardwareWorkerTopology> QueryHardwareManager::GetWorkerTopology() const {
	lock_guard<mutex> guard(state->lock);
	vector<QueryHardwareWorkerTopology> result;
	result.reserve(state->workers.size());
	for (const auto &entry : state->workers) {
		QueryHardwareWorkerTopology topology;
		topology.worker_id = entry.first;
		topology.logical_cpu = entry.second.logical_cpu;
		topology.socket_id = entry.second.core.socket_id;
		topology.physical_core_id = entry.second.core.core_id;
		result.push_back(topology);
	}
	std::sort(result.begin(), result.end(), [](const QueryHardwareWorkerTopology &left,
	                                         const QueryHardwareWorkerTopology &right) {
		return left.worker_id < right.worker_id;
	});
	return result;
}

QueryHardwarePowerModelSnapshot QueryHardwareManager::GetPowerModelSnapshot() const {
	lock_guard<mutex> guard(state->lock);
	return state->power_model;
}

QueryHardwareManagerSnapshot QueryHardwareManager::GetSnapshot() const {
	lock_guard<mutex> guard(state->lock);
	QueryHardwareManagerSnapshot result;
	result.active = state->active;
	result.hardware_control_enabled = state->hardware_control_enabled;
	auto plan = std::atomic_load_explicit(&state->published_plan, std::memory_order_acquire);
	result.published_generation = plan ? plan->generation : 0;
	result.applied_generation = state->applied_generation;
	result.registered_workers = state->workers.size();
	result.physical_core_domains = state->core_cpus.size();
	result.socket_domains = state->socket_cpus.size();
	result.msr_write_count = state->msr_write_count;
	result.core_msr_write_count = state->core_msr_write_count;
	result.uncore_msr_write_count = state->uncore_msr_write_count;
	result.publish_call_count = state->publish_call_count.load(std::memory_order_relaxed);
	result.worker_target_update_count = state->worker_target_update_count.load(std::memory_order_relaxed);
	result.worker_target_noop_count = state->worker_target_noop_count.load(std::memory_order_relaxed);
	result.worker_assignment_count = state->worker_assignment_count.load(std::memory_order_relaxed);
	result.worker_release_count = state->worker_release_count.load(std::memory_order_relaxed);
	result.worker_retarget_count = state->worker_retarget_count.load(std::memory_order_relaxed);
	result.worker_lease_handoff_count = state->worker_lease_handoff_count.load(std::memory_order_relaxed);
	result.core_transition_count = state->core_transition_count.load(std::memory_order_relaxed);
	result.uncore_transition_count = state->uncore_transition_count.load(std::memory_order_relaxed);
	result.target_plan_lock_wait_ns = state->target_plan_lock_wait_ns.load(std::memory_order_relaxed);
	result.manager_lock_wait_ns = state->manager_lock_wait_ns.load(std::memory_order_relaxed);
	result.target_update_wall_ns = state->target_update_wall_ns.load(std::memory_order_relaxed);
	result.target_update_thread_cpu_ns = state->target_update_thread_cpu_ns.load(std::memory_order_relaxed);
	result.target_update_max_wall_ns = state->target_update_max_wall_ns.load(std::memory_order_relaxed);
	result.hardware_apply_wall_ns = state->hardware_apply_wall_ns.load(std::memory_order_relaxed);
	result.hardware_apply_thread_cpu_ns = state->hardware_apply_thread_cpu_ns.load(std::memory_order_relaxed);
	result.hardware_apply_max_wall_ns = state->hardware_apply_max_wall_ns.load(std::memory_order_relaxed);
	{
		lock_guard<mutex> trace_guard(state->transition_trace_lock);
		result.transition_trace_dropped_count = state->transition_trace_dropped_count;
	}
	result.verification_failure_count = state->verification_failure_count;
	result.restoration_attempt_count = state->restoration_attempt_count;
	result.restoration_verified_count = state->restoration_verified_count;
	result.restoration_failure_count = state->restoration_failure_count;
	result.power_model_loaded = state->power_model.valid;
	result.status = state->status;
	result.restoration_status = state->restoration_status;
	return result;
}

vector<QueryHardwareTransitionSnapshot> QueryHardwareManager::GetTransitionTrace() const {
	lock_guard<mutex> guard(state->transition_trace_lock);
	return vector<QueryHardwareTransitionSnapshot>(state->transition_trace.begin(), state->transition_trace.end());
}

uint64_t QueryHardwareManager::TransitionTraceDroppedCount() const {
	lock_guard<mutex> guard(state->transition_trace_lock);
	return state->transition_trace_dropped_count;
}

} // namespace duckdb
