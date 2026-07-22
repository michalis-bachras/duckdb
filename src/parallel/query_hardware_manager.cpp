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
#include <cmath>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <map>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
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
	explicit QueryHardwareManagerState(DatabaseInstance &db_p) : db(db_p) {
		core_levels_khz = {1200000, 1500000, 1800000, 2200000, 2600000, 3000000};
		uncore_levels_khz = {1200000, 1800000, 2200000, 2800000};
	}

	~QueryHardwareManagerState() {
		DeactivateLocked();
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
		lock_guard<mutex> guard(lock);
		workers.erase(worker_id);
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
		power_model.minimum_core_frequency_khz = core_levels_khz.front();
		power_model.minimum_uncore_frequency_khz = uncore_levels_khz.front();
		auto path = db.config.options.query_sla_energy_power_model_path;
		if (path.empty()) {
			return;
		}
		std::ifstream input(path.c_str());
		if (!input.good()) {
			throw IOException("Could not open SLA-energy power model at %s", path);
		}
		using PowerKey = std::tuple<int, uint32_t, uint32_t>;
		std::map<PowerKey, std::pair<double, idx_t>> aggregates;
		string line;
		while (std::getline(input, line)) {
			if (line.empty() || line[0] == '#') {
				continue;
			}
			std::stringstream parser(line);
			vector<string> fields;
			string field;
			while (std::getline(parser, field, ',')) {
				fields.push_back(field);
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
			power_model.entries.push_back(calibration);
		}
		power_model.valid = !power_model.entries.empty();
		if (!power_model.valid) {
			throw InvalidInputException("SLA-energy power model %s contains no usable calibration rows", path);
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
				auto verified = ReadMSR(fd, IA32_PERF_CTL);
				if (CoreRatioToKHz(verified) != target_khz) {
					verification_failure_count++;
					throw IOException("SLA-energy core MSR verification failed for CPU %d", cpu);
				}
			}
#endif
		}
		current_core_khz[domain] = target_khz;
	}

	void ApplyUncoreLocked(int socket_id, uint32_t target_khz) {
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
			auto verified = ReadMSR(fd, MSR_UNCORE_RATIO_LIMIT);
			if (UncoreRatioToKHz(verified) != target_khz || NumericCast<uint32_t>((verified >> 8) & 0x7fU) * 100000U != target_khz) {
				verification_failure_count++;
				throw IOException("SLA-energy uncore MSR verification failed for socket %d", socket_id);
			}
#endif
		}
		current_uncore_khz[socket_id] = target_khz;
	}

	void PublishTargetsSerial(std::shared_ptr<const QueryHardwareTargetPlan> plan) {
		if (!plan) {
			return;
		}
		std::atomic_store_explicit(&published_plan, plan, std::memory_order_release);
		lock_guard<mutex> guard(lock);
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
		lock_guard<mutex> publish_guard(target_plan_lock);
		PublishTargetsSerial(std::move(plan));
	}

	void UpdateWorkerTarget(idx_t worker_id, const QueryRequestHardwareConfiguration &hardware, bool assigned,
	                        uint64_t generation) {
		lock_guard<mutex> publish_guard(target_plan_lock);
		auto current = std::atomic_load_explicit(&published_plan, std::memory_order_acquire);
		if (current) {
			for (const auto &target : current->workers) {
				if (target.worker_id == worker_id && target.assigned == assigned &&
				    (!assigned || target.hardware == hardware)) {
					return;
				}
			}
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
		PublishTargetsSerial(std::move(next));
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
			QueryRequestHardwareConfiguration result;
			auto core = current_core_khz.find(entry.second.core);
			auto uncore = current_uncore_khz.find(entry.second.core.socket_id);
			result.core_frequency_khz = core == current_core_khz.end() ? Reference().core_frequency_khz : core->second;
			result.uncore_frequency_khz =
			    uncore == current_uncore_khz.end() ? Reference().uncore_frequency_khz : uncore->second;
			return result;
		}
		return QueryRequestHardwareConfiguration();
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
	std::shared_ptr<const QueryHardwareTargetPlan> published_plan;
	uint64_t applied_generation = 0;
	uint64_t msr_write_count = 0;
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
	                                          bool assigned, uint64_t generation) {
	state->UpdateWorkerTarget(worker_id, hardware, assigned, generation);
}

bool QueryHardwareManager::IsWorkerAtConfiguration(idx_t worker_id,
                                                    const QueryRequestHardwareConfiguration &hardware) const {
	return state->IsWorkerAtConfiguration(worker_id, hardware);
}

QueryRequestHardwareConfiguration QueryHardwareManager::GetAppliedConfigurationForCPU(int logical_cpu) const {
	return state->GetAppliedConfigurationForCPU(logical_cpu);
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
	result.verification_failure_count = state->verification_failure_count;
	result.restoration_attempt_count = state->restoration_attempt_count;
	result.restoration_verified_count = state->restoration_verified_count;
	result.restoration_failure_count = state->restoration_failure_count;
	result.power_model_loaded = state->power_model.valid;
	result.status = state->status;
	result.restoration_status = state->restoration_status;
	return result;
}

} // namespace duckdb
