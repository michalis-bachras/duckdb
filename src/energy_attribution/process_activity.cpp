#include "duckdb/energy_attribution/process_activity.hpp"

#ifdef DUCKDB_BUILD_EBPF_PROCESS_ATTRIBUTION

#include "process_activity.skel.h"
#include "process_activity_shared.h"

#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <bpf/bpf.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <linux/futex.h>
#include <linux/perf_event.h>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>

namespace duckdb {

namespace {

static uint64_t MonotonicNs() {
	struct timespec value;
	clock_gettime(CLOCK_MONOTONIC, &value);
	return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL + static_cast<uint64_t>(value.tv_nsec);
}

static int64_t LinuxTID() {
	return static_cast<int64_t>(syscall(SYS_gettid));
}

static uint32_t AtomicLoad(const uint32_t *address) {
	return __atomic_load_n(address, __ATOMIC_ACQUIRE);
}

static void AtomicStore(uint32_t *address, uint32_t value) {
	__atomic_store_n(address, value, __ATOMIC_RELEASE);
}

static uint32_t AtomicIncrement(uint32_t *address) {
	return __atomic_add_fetch(address, 1, __ATOMIC_ACQ_REL);
}

static int FutexWait(uint32_t *address, uint32_t expected, const struct timespec *timeout) {
	return static_cast<int>(syscall(SYS_futex, address, FUTEX_WAIT_PRIVATE, expected, timeout, nullptr, 0));
}

static int FutexWake(uint32_t *address, int count) {
	return static_cast<int>(syscall(SYS_futex, address, FUTEX_WAKE_PRIVATE, count, nullptr, nullptr, 0));
}

static int SleepUntil(uint64_t target_ns) {
	struct timespec target;
	target.tv_sec = static_cast<time_t>(target_ns / 1000000000ULL);
	target.tv_nsec = static_cast<long>(target_ns % 1000000000ULL);
	int result;
	do {
		result = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, nullptr);
	} while (result == EINTR);
	return result;
}

static int PerfEventOpen(struct perf_event_attr *attr, int cpu) {
	return static_cast<int>(syscall(SYS_perf_event_open, attr, -1, cpu, -1, PERF_FLAG_FD_CLOEXEC));
}

static bool ReadInt(const string &path, int &result) {
	std::ifstream input(path.c_str());
	return input.good() && static_cast<bool>(input >> result);
}

static double ScaleCounter(uint64_t value, uint64_t enabled, uint64_t running) {
	if (running == 0 || enabled == 0) {
		return 0;
	}
	return static_cast<double>(value) * static_cast<double>(enabled) / static_cast<double>(running);
}

static bool RaiseMemlockSoftLimit(string &error) {
	struct rlimit limit;
	if (getrlimit(RLIMIT_MEMLOCK, &limit) != 0) {
		error = StringUtil::Format("could not read RLIMIT_MEMLOCK: %s", std::strerror(errno));
		return false;
	}
	if (limit.rlim_cur >= limit.rlim_max) {
		return true;
	}
	auto requested = limit;
	requested.rlim_cur = requested.rlim_max;
	if (setrlimit(RLIMIT_MEMLOCK, &requested) != 0) {
		error = StringUtil::Format("could not raise RLIMIT_MEMLOCK: %s", std::strerror(errno));
		return false;
	}
	return true;
}

class LibbpfProcessActivityCollector final : public ProcessActivityCollector {
	struct HelperCheckpointSlot {
		std::atomic<uint64_t> generation {0};
		std::atomic<uint64_t> boundary_ns {0};
	};

public:
	~LibbpfProcessActivityCollector() override {
		Stop();
	}

	bool Start(bool required) override {
		if (available) {
			return true;
		}
		required_mode = required;
		duckdb_tgid = static_cast<uint32_t>(getpid());
		string memlock_error;
		if (!RaiseMemlockSoftLimit(memlock_error)) {
			return Fail(memlock_error);
		}
		skeleton = process_activity_bpf__open();
		if (!skeleton) {
			return Fail("could not open embedded process-attribution BPF object");
		}
		if (process_activity_bpf__load(skeleton) != 0) {
			return Fail(StringUtil::Format("could not load process-attribution BPF object: %s", std::strerror(errno)));
		}
		if (!OpenCpuCounters()) {
			return false;
		}
		possible_cpu_count = libbpf_num_possible_cpus();
		if (possible_cpu_count <= 0) {
			return Fail("could not determine possible CPU count");
		}
		per_cpu_values.resize(static_cast<idx_t>(possible_cpu_count));
		batch_keys.resize(DRAIN_BATCH_SIZE);
		batch_values.resize(static_cast<idx_t>(DRAIN_BATCH_SIZE) * static_cast<idx_t>(possible_cpu_count));
		completed_keys.reserve(128);
		diagnostic_values.resize(static_cast<idx_t>(possible_cpu_count));
		cleared_diagnostics.resize(static_cast<idx_t>(possible_cpu_count));
		if (process_activity_bpf__attach(skeleton) != 0) {
			return Fail(StringUtil::Format("could not attach process-attribution BPF programs: %s",
			                              std::strerror(errno)));
		}
		if (!StartHelpers()) {
			return false;
		}
		ClearDiagnostics();
		duckdb_process_activity_control control = {};
		control.generation = generation;
		control.transition_ns = 0;
		control.duckdb_tgid = duckdb_tgid;
		control.enabled = 1;
		if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.control), &zero, &control, BPF_ANY) != 0) {
			return Fail(StringUtil::Format("could not enable process-attribution BPF program: %s",
			                              std::strerror(errno)));
		}
		available = true;
		auto initial = RotateWindow(MonotonicNs() + 1000000, 5000000);
		if (!initial.valid && required_mode) {
			return Fail("could not establish initial process-attribution generation: " + initial.status);
		}
		return true;
	}

	ProcessActivityWindow RotateWindow(uint64_t requested_boundary_ns, uint64_t maximum_skew_ns) override {
		ProcessActivityWindow result;
		result.generation = generation;
		result.requested_boundary_ns = requested_boundary_ns;
		result.transition_ns = requested_boundary_ns;
		result.socket_topology = socket_topology;
		if (!available && generation == 1) {
			result.status = last_error.empty() ? "collector_not_available" : last_error;
			return result;
		}
		auto checkpoint_start = MonotonicNs();
		auto completed_generation = generation++;
		duckdb_process_activity_control control = {};
		control.generation = generation;
		control.transition_ns = requested_boundary_ns;
		control.duckdb_tgid = duckdb_tgid;
		control.enabled = 1;
		if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.control), &zero, &control, BPF_ANY) != 0) {
			result.status = StringUtil::Format("generation update failed: %s", std::strerror(errno));
			return result;
		}
		if (requested_boundary_ns > MonotonicNs()) {
			auto sleep_result = SleepUntil(requested_boundary_ns);
			if (sleep_result != 0) {
				result.status = StringUtil::Format("boundary sleep failed: %s", std::strerror(sleep_result));
				return result;
			}
		}
		AtomicStore(&helper_ack_count, 0);
		auto requested_helper_generation = AtomicIncrement(&helper_generation);
		FutexWake(&helper_generation, INT_MAX);
		auto hard_timeout_ns = std::max<uint64_t>(20000000, maximum_skew_ns * 4);
		auto deadline_ns = MonotonicNs() + hard_timeout_ns;
		while (AtomicLoad(&helper_ack_count) < helper_threads.size() &&
		       !stop_helpers.load(std::memory_order_acquire)) {
			auto now_ns = MonotonicNs();
			if (now_ns >= deadline_ns) {
				break;
			}
			auto remaining_ns = deadline_ns - now_ns;
			struct timespec timeout;
			timeout.tv_sec = static_cast<time_t>(remaining_ns / 1000000000ULL);
			timeout.tv_nsec = static_cast<long>(remaining_ns % 1000000000ULL);
			auto acknowledged = AtomicLoad(&helper_ack_count);
			FutexWait(&helper_ack_count, acknowledged, &timeout);
		}
		uint64_t acknowledged = 0;
		result.boundary_min_ns = std::numeric_limits<uint64_t>::max();
		result.boundary_max_ns = 0;
		for (idx_t i = 0; i < helper_threads.size(); i++) {
			if (helper_checkpoints[i].generation.load(std::memory_order_acquire) !=
			    requested_helper_generation) {
				continue;
			}
			auto boundary_ns = helper_checkpoints[i].boundary_ns.load(std::memory_order_relaxed);
			result.boundary_min_ns = std::min(result.boundary_min_ns, boundary_ns);
			result.boundary_max_ns = std::max(result.boundary_max_ns, boundary_ns);
			if (boundary_ns > requested_boundary_ns + maximum_skew_ns) {
				result.late_helper_count++;
			}
			acknowledged++;
		}
		if (result.boundary_min_ns == std::numeric_limits<uint64_t>::max()) {
			result.boundary_min_ns = 0;
		}
		if (result.boundary_max_ns > requested_boundary_ns) {
			result.maximum_checkpoint_delay_ns = result.boundary_max_ns - requested_boundary_ns;
		}
		if (acknowledged != helper_threads.size()) {
			result.status = StringUtil::Format("checkpoint_timeout:%llu/%llu",
			                                  static_cast<unsigned long long>(acknowledged),
			                                  static_cast<unsigned long long>(helper_threads.size()));
		}
		DrainGeneration(completed_generation, result);
		ReadDiagnostics(result);
		result.checkpoint_duration_ns = MonotonicNs() - checkpoint_start;
		if (result.status.empty() && (result.lost_records > 0 || result.lookup_failures > 0)) {
			result.status =
			    StringUtil::Format("process_activity_diagnostics_failed:activity_map_insert_failure=%llu,"
			                       "missing_counter=%llu,invalid_counter_delta=%llu,interrupt_underflow=%llu",
			                       static_cast<unsigned long long>(result.activity_map_insert_failure),
			                       static_cast<unsigned long long>(result.missing_counter),
			                       static_cast<unsigned long long>(result.invalid_counter_delta),
			                       static_cast<unsigned long long>(result.interrupt_underflow));
		}
		result.boundary_corrected = result.boundary_split_count > 0;
		result.valid = result.status.empty() && result.boundary_min_ns > 0;
		if (result.status.empty()) {
			result.status = result.valid && result.late_helper_count > 0 ? "boundary_corrected" : "ok";
		}
		return result;
	}

	void Stop() override {
		if (!skeleton && helper_threads.empty()) {
			return;
		}
		if (skeleton) {
			duckdb_process_activity_control control = {};
			bpf_map_update_elem(bpf_map__fd(skeleton->maps.control), &zero, &control, BPF_ANY);
		}
		stop_helpers.store(true, std::memory_order_release);
		AtomicIncrement(&helper_generation);
		FutexWake(&helper_generation, INT_MAX);
		FutexWake(&helper_ack_count, INT_MAX);
		for (auto &helper : helper_threads) {
			if (helper.joinable()) {
				helper.join();
			}
		}
		helper_threads.clear();
		helper_tids.clear();
		helper_checkpoints.reset();
		for (auto fd : perf_fds) {
			if (fd >= 0) {
				ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
				close(fd);
			}
		}
		perf_fds.clear();
		cpu_socket_ids.clear();
		socket_topology.clear();
		per_cpu_values.clear();
		batch_keys.clear();
		batch_values.clear();
		completed_keys.clear();
		diagnostic_values.clear();
		cleared_diagnostics.clear();
		possible_cpu_count = 0;
		batch_lookup_supported = true;
		if (skeleton) {
			process_activity_bpf__destroy(skeleton);
			skeleton = nullptr;
		}
		available = false;
	}

	bool Available() const override {
		return available;
	}

	uint32_t DuckDBProcessID() const override {
		return duckdb_tgid;
	}

	const string &LastError() const override {
		return last_error;
	}

private:
	bool Fail(const string &error) {
		last_error = error;
		Stop();
		return false;
	}

	int OpenCounter(uint32_t config, int cpu) {
		perf_event_attr attr = {};
		attr.type = PERF_TYPE_HARDWARE;
		attr.size = sizeof(attr);
		attr.config = config;
		attr.disabled = 0;
		attr.exclude_hv = 1;
		attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
		return PerfEventOpen(&attr, cpu);
	}

	bool OpenCpuCounters() {
		auto configured_cpus = static_cast<int>(sysconf(_SC_NPROCESSORS_CONF));
		auto online_cpus = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
		if (configured_cpus <= 0 || online_cpus != configured_cpus ||
		    configured_cpus > DUCKDB_PROCESS_ACTIVITY_MAX_CPUS) {
			return Fail(StringUtil::Format("all configured CPUs must be online and <= %d (configured=%d online=%d)",
			                              DUCKDB_PROCESS_ACTIVITY_MAX_CPUS, configured_cpus, online_cpus));
		}
		cpu_count = configured_cpus;
		for (int cpu = 0; cpu < cpu_count; cpu++) {
			int socket_id = -1;
			if (!ReadInt(StringUtil::Format("/sys/devices/system/cpu/cpu%d/topology/physical_package_id", cpu),
			             socket_id)) {
				return Fail(StringUtil::Format("could not read topology for CPU %d", cpu));
			}
			uint32_t cpu_key = static_cast<uint32_t>(cpu);
			duckdb_process_cpu_metadata metadata = {};
			metadata.socket_id = static_cast<uint32_t>(socket_id);
			if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.cpu_metadata), &cpu_key, &metadata, BPF_ANY) != 0) {
				return Fail(StringUtil::Format("could not publish socket for CPU %d: %s", cpu,
				                              std::strerror(errno)));
			}
			cpu_socket_ids.push_back(metadata.socket_id);
			auto topology_entry =
			    std::find_if(socket_topology.begin(), socket_topology.end(), [&](const ProcessActivitySocketTopology &entry) {
				    return entry.socket_id == socket_id;
			    });
			if (topology_entry == socket_topology.end()) {
				ProcessActivitySocketTopology topology;
				topology.socket_id = socket_id;
				topology.monitored_cpu_count = 1;
				socket_topology.push_back(topology);
			} else {
				topology_entry->monitored_cpu_count++;
			}
			auto cycles_fd = OpenCounter(PERF_COUNT_HW_CPU_CYCLES, cpu);
			auto ref_fd = OpenCounter(PERF_COUNT_HW_REF_CPU_CYCLES, cpu);
			if (cycles_fd < 0 || ref_fd < 0) {
				if (cycles_fd >= 0) {
					close(cycles_fd);
				}
				if (ref_fd >= 0) {
					close(ref_fd);
				}
				return Fail(StringUtil::Format("could not open CPU-wide PMU counters for CPU %d: %s", cpu,
				                              std::strerror(errno)));
			}
			if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.cycles), &cpu_key, &cycles_fd, BPF_ANY) != 0 ||
			    bpf_map_update_elem(bpf_map__fd(skeleton->maps.ref_cycles), &cpu_key, &ref_fd, BPF_ANY) != 0) {
				close(cycles_fd);
				close(ref_fd);
				return Fail(StringUtil::Format("could not install PMU counters for CPU %d: %s", cpu,
				                              std::strerror(errno)));
			}
			perf_fds.push_back(cycles_fd);
			perf_fds.push_back(ref_fd);
		}
		return true;
	}

	bool StartHelpers() {
		stop_helpers.store(false, std::memory_order_release);
		helper_started = 0;
		AtomicStore(&helper_generation, 0);
		AtomicStore(&helper_ack_count, 0);
		helper_start_failed = false;
		helper_start_error.clear();
		helper_tids.assign(static_cast<idx_t>(cpu_count), -1);
		helper_checkpoints =
		    std::unique_ptr<HelperCheckpointSlot[]>(new HelperCheckpointSlot[static_cast<idx_t>(cpu_count)]);
		for (int cpu = 0; cpu < cpu_count; cpu++) {
			helper_threads.emplace_back([this, cpu]() {
				cpu_set_t cpuset;
				CPU_ZERO(&cpuset);
				CPU_SET(cpu, &cpuset);
				auto affinity_result = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
				{
					lock_guard<mutex> guard(helper_lock);
					helper_tids[static_cast<idx_t>(cpu)] = LinuxTID();
					if (affinity_result != 0) {
						helper_start_failed = true;
						if (helper_start_error.empty()) {
							helper_start_error =
							    StringUtil::Format("could not pin process-attribution helper to CPU %d: %s", cpu,
							                       std::strerror(affinity_result));
						}
					}
					helper_started++;
				}
				helper_ack_cv.notify_all();
				if (affinity_result != 0) {
					return;
				}
				auto observed_generation = AtomicLoad(&helper_generation);
				while (!stop_helpers.load(std::memory_order_acquire)) {
					while (!stop_helpers.load(std::memory_order_acquire) &&
					       AtomicLoad(&helper_generation) == observed_generation) {
						FutexWait(&helper_generation, observed_generation, nullptr);
					}
					if (stop_helpers.load(std::memory_order_acquire)) {
						break;
					}
					observed_generation = AtomicLoad(&helper_generation);
					auto boundary_ns = MonotonicNs();
					auto &checkpoint = helper_checkpoints[static_cast<idx_t>(cpu)];
					checkpoint.boundary_ns.store(boundary_ns, std::memory_order_relaxed);
					checkpoint.generation.store(observed_generation, std::memory_order_release);
					if (AtomicIncrement(&helper_ack_count) == helper_threads.size()) {
						FutexWake(&helper_ack_count, 1);
					}
				}
			});
		}
		std::unique_lock<mutex> guard(helper_lock);
		if (!helper_ack_cv.wait_for(guard, std::chrono::seconds(2),
		                            [&]() { return helper_started == helper_threads.size(); })) {
			guard.unlock();
			return Fail("process-attribution checkpoint helpers did not start");
		}
		if (helper_start_failed) {
			auto error = helper_start_error;
			guard.unlock();
			return Fail(error);
		}
		for (int cpu = 0; cpu < cpu_count; cpu++) {
			uint32_t cpu_key = static_cast<uint32_t>(cpu);
			duckdb_process_cpu_metadata metadata = {};
			metadata.socket_id = cpu_socket_ids[static_cast<idx_t>(cpu)];
			metadata.helper_tid = static_cast<uint32_t>(helper_tids[static_cast<idx_t>(cpu)]);
			if (bpf_map_update_elem(bpf_map__fd(skeleton->maps.cpu_metadata), &cpu_key, &metadata, BPF_ANY) != 0) {
				return Fail(StringUtil::Format("could not publish process-attribution helper for CPU %d: %s", cpu,
				                              std::strerror(errno)));
			}
		}
		return true;
	}

	void AppendRecord(const duckdb_process_activity_key &key, const duckdb_process_activity_value *values,
	                  ProcessActivityWindow &result) {
		ProcessActivityRecord record;
		record.generation = key.generation;
		record.process_start_time_ns = key.process_start_time_ns;
		record.tgid = key.tgid;
		record.tid = key.tid;
		record.socket_id = static_cast<int>(key.socket_id);
		switch (key.kind) {
		case DUCKDB_PROCESS_ACTIVITY_HOST_IRQ:
			record.kind = ProcessActivityKind::HOST_IRQ;
			break;
		case DUCKDB_PROCESS_ACTIVITY_HOST_KERNEL:
			record.kind = ProcessActivityKind::HOST_KERNEL;
			break;
		case DUCKDB_PROCESS_ACTIVITY_MEASUREMENT_OVERHEAD:
			record.kind = ProcessActivityKind::MEASUREMENT_OVERHEAD;
			break;
		case DUCKDB_PROCESS_ACTIVITY_SYSTEM_IDLE:
			record.kind = ProcessActivityKind::SYSTEM_IDLE;
			break;
		default:
			record.kind = ProcessActivityKind::PROCESS;
			break;
		}
		for (int cpu = 0; cpu < possible_cpu_count; cpu++) {
			const auto &value = values[static_cast<idx_t>(cpu)];
			record.runtime_ns += value.runtime_ns;
			record.cycles += value.cycles;
			record.ref_cycles += value.ref_cycles;
			record.time_enabled_ns += value.time_enabled_ns;
			record.time_running_ns += value.time_running_ns;
			record.ref_time_enabled_ns += value.ref_time_enabled_ns;
			record.ref_time_running_ns += value.ref_time_running_ns;
			record.switch_count += value.switch_count;
			if (record.comm.empty() && value.comm[0] != '\0') {
				record.comm = string(value.comm, strnlen(value.comm, sizeof(value.comm)));
			}
		}
		record.pmu_running_ratio =
		    record.time_enabled_ns > 0
		        ? static_cast<double>(record.time_running_ns) / static_cast<double>(record.time_enabled_ns)
		        : 0;
		record.corrected_cycles = ScaleCounter(record.cycles, record.time_enabled_ns, record.time_running_ns);
		record.ref_pmu_running_ratio =
		    record.ref_time_enabled_ns > 0
		        ? static_cast<double>(record.ref_time_running_ns) / static_cast<double>(record.ref_time_enabled_ns)
		        : 0;
		auto corrected_ref = ScaleCounter(record.ref_cycles, record.ref_time_enabled_ns, record.ref_time_running_ns);
		record.frequency_ratio = corrected_ref > 0 ? record.corrected_cycles / corrected_ref : 0;
		record.pmu_valid = record.corrected_cycles > 0 && corrected_ref > 0 &&
		                   record.pmu_running_ratio >= 0.90 && record.ref_pmu_running_ratio >= 0.90;
		result.records.push_back(std::move(record));
	}

	bool DrainGenerationBatch(int fd, uint64_t completed_generation, ProcessActivityWindow &result) {
		bpf_map_batch_opts options = {};
		options.sz = sizeof(options);
		duckdb_process_activity_key cursor = {};
		void *input_cursor = nullptr;
		completed_keys.clear();
		while (true) {
			uint32_t count = DRAIN_BATCH_SIZE;
			errno = 0;
			auto status = bpf_map_lookup_batch(fd, input_cursor, &cursor, batch_keys.data(), batch_values.data(),
			                                   &count, &options);
			auto lookup_errno = errno;
			for (uint32_t i = 0; i < count; i++) {
				const auto &key = batch_keys[static_cast<idx_t>(i)];
				if (key.generation != completed_generation) {
					continue;
				}
				auto value_offset = static_cast<idx_t>(i) * static_cast<idx_t>(possible_cpu_count);
				AppendRecord(key, batch_values.data() + value_offset, result);
				completed_keys.push_back(key);
			}
			if (status == 0) {
				input_cursor = &cursor;
				continue;
			}
			if (lookup_errno == ENOENT) {
				break;
			}
			if (input_cursor == nullptr && count == 0 &&
			    (lookup_errno == EINVAL || lookup_errno == ENOTSUP || lookup_errno == EOPNOTSUPP)) {
				result.records.clear();
				completed_keys.clear();
				return false;
			}
			result.lookup_failures++;
			if (result.status.empty()) {
				result.status = StringUtil::Format("activity batch lookup failed: %s", std::strerror(lookup_errno));
			}
			return true;
		}
		if (completed_keys.empty()) {
			return true;
		}
		uint32_t delete_count = static_cast<uint32_t>(completed_keys.size());
		if (bpf_map_delete_batch(fd, completed_keys.data(), &delete_count, &options) == 0 &&
		    delete_count == completed_keys.size()) {
			return true;
		}
		for (const auto &key : completed_keys) {
			if (bpf_map_delete_elem(fd, &key) != 0 && errno != ENOENT) {
				result.lookup_failures++;
			}
		}
		return true;
	}

	void DrainGenerationScalar(int fd, uint64_t completed_generation, ProcessActivityWindow &result) {
		duckdb_process_activity_key key = {};
		duckdb_process_activity_key next_key = {};
		bool have_key = bpf_map_get_next_key(fd, nullptr, &next_key) == 0;
		while (have_key) {
			key = next_key;
			have_key = bpf_map_get_next_key(fd, &key, &next_key) == 0;
			if (key.generation != completed_generation) {
				continue;
			}
			if (bpf_map_lookup_elem(fd, &key, per_cpu_values.data()) != 0) {
				result.lookup_failures++;
				continue;
			}
			AppendRecord(key, per_cpu_values.data(), result);
			if (bpf_map_delete_elem(fd, &key) != 0 && errno != ENOENT) {
				result.lookup_failures++;
			}
		}
	}

	void DrainGeneration(uint64_t completed_generation, ProcessActivityWindow &result) {
		auto fd = bpf_map__fd(skeleton->maps.activity);
		if (possible_cpu_count <= 0 || per_cpu_values.empty()) {
			result.status = "could_not_determine_possible_cpu_count";
			return;
		}
		result.records.reserve(64);
		if (!batch_lookup_supported || !DrainGenerationBatch(fd, completed_generation, result)) {
			batch_lookup_supported = false;
			DrainGenerationScalar(fd, completed_generation, result);
		}
	}

	void ReadDiagnostics(ProcessActivityWindow &result) {
		if (possible_cpu_count <= 0 || diagnostic_values.empty()) {
			return;
		}
		if (bpf_map_lookup_elem(bpf_map__fd(skeleton->maps.diagnostics), &zero, diagnostic_values.data()) != 0) {
			return;
		}
		for (const auto &value : diagnostic_values) {
			result.activity_map_insert_failure += value.activity_map_insert_failure;
			result.missing_counter += value.missing_counter;
			result.invalid_counter_delta += value.invalid_counter_delta;
			result.interrupt_underflow += value.interrupt_underflow;
			result.boundary_split_count += value.boundary_split_count;
			result.boundary_split_runtime_ns += value.boundary_split_runtime_ns;
			result.boundary_split_cycles += value.boundary_split_cycles;
			result.boundary_split_ref_cycles += value.boundary_split_ref_cycles;
			if (value.boundary_split_count > 0) {
				result.boundary_split_cpu_count++;
			}
		}
		result.lost_records += result.activity_map_insert_failure;
		result.lookup_failures += result.missing_counter + result.invalid_counter_delta + result.interrupt_underflow;
		ClearDiagnostics();
	}

	void ClearDiagnostics() {
		if (possible_cpu_count <= 0 || cleared_diagnostics.empty()) {
			return;
		}
		bpf_map_update_elem(bpf_map__fd(skeleton->maps.diagnostics), &zero, cleared_diagnostics.data(), BPF_ANY);
	}

private:
	process_activity_bpf *skeleton = nullptr;
	vector<int> perf_fds;
	vector<std::thread> helper_threads;
	vector<int64_t> helper_tids;
	vector<uint32_t> cpu_socket_ids;
	vector<ProcessActivitySocketTopology> socket_topology;
	std::unique_ptr<HelperCheckpointSlot[]> helper_checkpoints;
	mutable mutex helper_lock;
	std::condition_variable helper_ack_cv;
	alignas(4) uint32_t helper_generation = 0;
	alignas(4) uint32_t helper_ack_count = 0;
	uint64_t helper_started = 0;
	bool helper_start_failed = false;
	string helper_start_error;
	uint64_t generation = 1;
	uint32_t duckdb_tgid = 0;
	int cpu_count = 0;
	int possible_cpu_count = 0;
	vector<duckdb_process_activity_value> per_cpu_values;
	static constexpr uint32_t DRAIN_BATCH_SIZE = 64;
	vector<duckdb_process_activity_key> batch_keys;
	vector<duckdb_process_activity_value> batch_values;
	vector<duckdb_process_activity_key> completed_keys;
	vector<duckdb_process_activity_diagnostics> diagnostic_values;
	vector<duckdb_process_activity_diagnostics> cleared_diagnostics;
	bool batch_lookup_supported = true;
	const uint32_t zero = 0;
	std::atomic<bool> stop_helpers {false};
	bool required_mode = false;
	bool available = false;
	string last_error;
};

} // namespace

unique_ptr<ProcessActivityCollector> ProcessActivityCollector::Create() {
	return make_uniq<LibbpfProcessActivityCollector>();
}

bool ProcessActivityCollector::IsCompiled() {
	return true;
}

const char *ProcessActivityKindToString(ProcessActivityKind kind) {
	switch (kind) {
	case ProcessActivityKind::PROCESS:
		return "PROCESS";
	case ProcessActivityKind::HOST_KERNEL:
		return "HOST_KERNEL";
	case ProcessActivityKind::HOST_IRQ:
		return "HOST_IRQ";
	case ProcessActivityKind::MEASUREMENT_OVERHEAD:
		return "MEASUREMENT_OVERHEAD";
	case ProcessActivityKind::SYSTEM_IDLE:
		return "SYSTEM_IDLE";
	default:
		return "UNKNOWN";
	}
}

} // namespace duckdb

#else

namespace duckdb {

unique_ptr<ProcessActivityCollector> ProcessActivityCollector::Create() {
	return nullptr;
}

bool ProcessActivityCollector::IsCompiled() {
	return false;
}

const char *ProcessActivityKindToString(ProcessActivityKind kind) {
	switch (kind) {
	case ProcessActivityKind::PROCESS:
		return "PROCESS";
	case ProcessActivityKind::HOST_KERNEL:
		return "HOST_KERNEL";
	case ProcessActivityKind::HOST_IRQ:
		return "HOST_IRQ";
	case ProcessActivityKind::MEASUREMENT_OVERHEAD:
		return "MEASUREMENT_OVERHEAD";
	case ProcessActivityKind::SYSTEM_IDLE:
		return "SYSTEM_IDLE";
	default:
		return "UNKNOWN";
	}
}

} // namespace duckdb

#endif
