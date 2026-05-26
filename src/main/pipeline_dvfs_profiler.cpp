#include "duckdb/main/pipeline_dvfs_profiler.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <limits>

#ifdef __linux__
#include <dirent.h>
#include <fcntl.h>
#include <linux/hw_breakpoint.h>
#include <linux/perf_event.h>
#include <linux/unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace duckdb {

PipelinePerfFD::PipelinePerfFD() : fd(-1) {
}

PipelinePerfFD::PipelinePerfFD(int fd_p) : fd(fd_p) {
}

PipelinePerfFD::~PipelinePerfFD() {
	Reset();
}

PipelinePerfFD::PipelinePerfFD(PipelinePerfFD &&other) noexcept : fd(other.Release()) {
}

PipelinePerfFD &PipelinePerfFD::operator=(PipelinePerfFD &&other) noexcept {
	if (this != &other) {
		Reset(other.Release());
	}
	return *this;
}

int PipelinePerfFD::Get() const {
	return fd;
}

void PipelinePerfFD::Reset(int fd_p) {
	if (fd >= 0) {
#ifdef __linux__
		close(fd);
#endif
	}
	fd = fd_p;
}

int PipelinePerfFD::Release() {
	auto result = fd;
	fd = -1;
	return result;
}

static uint64_t PipelineProfileTimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

PipelineDVFSProfiler::PipelineDVFSProfiler()
    : pipeline_dvfs_metrics_enabled(false), pipeline_task_trace_enabled(false), pipeline_perf_setup_attempted(false),
      pipeline_perf_supported(false), pipeline_perf_per_cpu_enabled(false), pipeline_perf_fd_count(0),
      pipeline_perf_cpu_count(0), pipeline_rapl_perf_supported(false), pipeline_rapl_perf_fd_count(0) {
}

PipelineDVFSProfiler::~PipelineDVFSProfiler() {
	Close();
}

bool PipelineDVFSProfiler::MetricsEnabled() const {
	return pipeline_dvfs_metrics_enabled;
}

bool PipelineDVFSProfiler::TaskTraceEnabled() const {
	return pipeline_task_trace_enabled;
}

uint64_t PipelineDVFSProfiler::TimestampNs() {
	return PipelineProfileTimestampNs();
}

void PipelineDVFSProfiler::Reset() {
	Close();
	pipeline_dvfs_metrics_enabled = false;
	pipeline_task_trace_enabled = false;
	pipeline_perf_setup_attempted = false;
	pipeline_perf_supported = false;
	pipeline_perf_per_cpu_enabled = false;
	pipeline_perf_scope.clear();
	pipeline_perf_cgroup_path.clear();
	pipeline_perf_error.clear();
	pipeline_perf_fd_count = 0;
	pipeline_perf_cpu_count = 0;
	pipeline_rapl_perf_supported = false;
	pipeline_rapl_perf_error.clear();
	pipeline_rapl_perf_fd_count = 0;
}

#ifdef __linux__

struct PipelineRaplPath {
	string energy_path;
	uint64_t max_uj = 0;
};

struct PipelineRaplPaths {
	vector<PipelineRaplPath> package_paths;
	vector<PipelineRaplPath> dram_paths;
	bool initialized = false;
};

static bool ReadUInt64File(const string &path, uint64_t &result) {
	std::ifstream input(path);
	if (!input.good()) {
		return false;
	}
	input >> result;
	return !input.fail();
}

static string JoinPath(const string &left, const string &right) {
	if (left.empty()) {
		return right;
	}
	if (left[left.size() - 1] == '/') {
		return left + right;
	}
	return left + "/" + right;
}

static bool DirectoryExists(const string &path) {
	auto dir = opendir(path.c_str());
	if (!dir) {
		return false;
	}
	closedir(dir);
	return true;
}

static string ReadTextFile(const string &path) {
	std::ifstream input(path);
	if (!input.good()) {
		return string();
	}
	string result;
	std::getline(input, result);
	return result;
}

static idx_t ColonCount(const string &value) {
	idx_t count = 0;
	for (auto c : value) {
		if (c == ':') {
			count++;
		}
	}
	return count;
}

static void RegisterRaplPath(const string &domain_path, PipelineRaplPaths &paths) {
	auto name = StringUtil::Lower(ReadTextFile(JoinPath(domain_path, "name")));
	auto energy_path = JoinPath(domain_path, "energy_uj");
	auto max_path = JoinPath(domain_path, "max_energy_range_uj");
	uint64_t ignored = 0;
	if (name.empty() || !ReadUInt64File(energy_path, ignored)) {
		return;
	}
	uint64_t max_uj = 0;
	ReadUInt64File(max_path, max_uj);

	PipelineRaplPath path;
	path.energy_path = energy_path;
	path.max_uj = max_uj;
	if (name.find("package") != string::npos) {
		paths.package_paths.push_back(std::move(path));
	} else if (name.find("dram") != string::npos) {
		paths.dram_paths.push_back(std::move(path));
	}
}

static PipelineRaplPaths &GetPipelineRaplPaths() {
	static PipelineRaplPaths paths;
	if (paths.initialized) {
		return paths;
	}
	paths.initialized = true;
	string root("/sys/class/powercap");
	auto dir = opendir(root.c_str());
	if (!dir) {
		return paths;
	}
	while (auto entry = readdir(dir)) {
		string filename(entry->d_name);
		if (filename.rfind("intel-rapl:", 0) != 0 || ColonCount(filename) != 1) {
			continue;
		}
		auto domain_path = JoinPath(root, filename);
		if (!DirectoryExists(domain_path)) {
			continue;
		}
		RegisterRaplPath(domain_path, paths);
		auto sub_dir = opendir(domain_path.c_str());
		if (!sub_dir) {
			continue;
		}
		while (auto sub_entry = readdir(sub_dir)) {
			string sub_filename(sub_entry->d_name);
			if (sub_filename.rfind("intel-rapl:", 0) == 0) {
				RegisterRaplPath(JoinPath(domain_path, sub_filename), paths);
			}
		}
		closedir(sub_dir);
	}
	closedir(dir);
	return paths;
}

static void WarmupRaplReads() {
	auto &paths = GetPipelineRaplPaths();
	uint64_t ignored = 0;
	for (auto &path : paths.package_paths) {
		ReadUInt64File(path.energy_path, ignored);
	}
	for (auto &path : paths.dram_paths) {
		ReadUInt64File(path.energy_path, ignored);
	}
}

static void ReadRaplStart(PipelineProfilingInfo &profile) {
	auto &paths = GetPipelineRaplPaths();
	profile.rapl_package_start_uj.clear();
	profile.rapl_package_max_uj.clear();
	profile.rapl_dram_start_uj.clear();
	profile.rapl_dram_max_uj.clear();

	for (auto &path : paths.package_paths) {
		uint64_t value = 0;
		if (ReadUInt64File(path.energy_path, value)) {
			profile.rapl_package_start_uj.push_back(value);
			profile.rapl_package_max_uj.push_back(path.max_uj);
		}
	}
	for (auto &path : paths.dram_paths) {
		uint64_t value = 0;
		if (ReadUInt64File(path.energy_path, value)) {
			profile.rapl_dram_start_uj.push_back(value);
			profile.rapl_dram_max_uj.push_back(path.max_uj);
		}
	}
	profile.rapl_supported = !profile.rapl_package_start_uj.empty() || !profile.rapl_dram_start_uj.empty();
}

static double ReadRaplDeltaJ(const vector<PipelineRaplPath> &paths, const vector<uint64_t> &start_values,
                             const vector<uint64_t> &max_values) {
	double total_j = 0;
	for (idx_t i = 0; i < start_values.size() && i < paths.size(); i++) {
		uint64_t end_value = 0;
		if (!ReadUInt64File(paths[i].energy_path, end_value)) {
			continue;
		}
		auto start_value = start_values[i];
		uint64_t delta = 0;
		if (end_value >= start_value) {
			delta = end_value - start_value;
		} else if (i < max_values.size() && max_values[i] > 0) {
			delta = (max_values[i] - start_value) + end_value;
		}
		total_j += static_cast<double>(delta) / 1000000.0;
	}
	return total_j;
}

enum class PipelineRaplPerfCounter : idx_t { PACKAGE = 0, DRAM = 1 };

struct PipelineRaplPerfSpec {
	PipelineRaplPerfCounter counter;
	string event_name;
	uint64_t config = 0;
	double scale = 0;
};

static bool ReadDoubleFile(const string &path, double &result) {
	auto text = ReadTextFile(path);
	if (text.empty()) {
		return false;
	}
	char *end = nullptr;
	errno = 0;
	auto parsed = std::strtod(text.c_str(), &end);
	if (end == text.c_str() || errno) {
		return false;
	}
	result = parsed;
	return true;
}

static bool ReadPowerEventConfig(const string &event_name, uint64_t &config) {
	auto event_path = JoinPath("/sys/bus/event_source/devices/power/events", event_name);
	auto text = ReadTextFile(event_path);
	auto event_pos = text.find("event=");
	if (event_pos == string::npos) {
		return false;
	}
	auto config_text = text.substr(event_pos + 6);
	auto comma_pos = config_text.find(',');
	if (comma_pos != string::npos) {
		config_text = config_text.substr(0, comma_pos);
	}
	char *end = nullptr;
	errno = 0;
	auto parsed = std::strtoull(config_text.c_str(), &end, 0);
	if (end == config_text.c_str() || errno) {
		return false;
	}
	config = parsed;
	return true;
}

static bool ReadPowerEventScale(const string &event_name, double &scale) {
	return ReadDoubleFile(JoinPath("/sys/bus/event_source/devices/power/events", event_name + ".scale"), scale);
}

static vector<int> ParseCPUList(const string &value) {
	vector<int> cpus;
	idx_t pos = 0;
	while (pos < value.size()) {
		while (pos < value.size() && (value[pos] == ',' || value[pos] == ' ' || value[pos] == '\n' ||
		                              value[pos] == '\t')) {
			pos++;
		}
		if (pos >= value.size()) {
			break;
		}
		auto end_pos = pos;
		while (end_pos < value.size() && value[end_pos] != ',' && value[end_pos] != '\n') {
			end_pos++;
		}
		auto token = value.substr(pos, end_pos - pos);
		auto dash_pos = token.find('-');
		try {
			if (dash_pos == string::npos) {
				cpus.push_back(std::stoi(token));
			} else {
				auto first_cpu = std::stoi(token.substr(0, dash_pos));
				auto last_cpu = std::stoi(token.substr(dash_pos + 1));
				for (auto cpu = first_cpu; cpu <= last_cpu; cpu++) {
					cpus.push_back(cpu);
				}
			}
		} catch (...) {
		}
		pos = end_pos + 1;
	}
	return cpus;
}

static vector<PipelineRaplPerfSpec> GetRaplPerfSpecs(string &error) {
	vector<PipelineRaplPerfSpec> specs;
	const pair<PipelineRaplPerfCounter, string> candidates[] = {
	    {PipelineRaplPerfCounter::PACKAGE, "energy-pkg"},
	    {PipelineRaplPerfCounter::DRAM, "energy-ram"},
	};
	for (auto &candidate : candidates) {
		uint64_t config = 0;
		double scale = 0;
		if (!ReadPowerEventConfig(candidate.second, config) || !ReadPowerEventScale(candidate.second, scale)) {
			continue;
		}
		PipelineRaplPerfSpec spec;
		spec.counter = candidate.first;
		spec.event_name = candidate.second;
		spec.config = config;
		spec.scale = scale;
		specs.push_back(std::move(spec));
	}
	if (specs.empty()) {
		error = "RAPL power PMU energy-pkg/energy-ram events are not available";
	}
	return specs;
}

enum class PipelinePerfCounter : idx_t { CYCLES = 0, INSTRUCTIONS = 1, CACHE_REFERENCES = 2, CACHE_MISSES = 3 };

struct PipelinePerfSpec {
	PipelinePerfCounter counter;
	uint64_t config;
};

static const PipelinePerfSpec PIPELINE_PERF_SPECS[] = {
    {PipelinePerfCounter::CYCLES, PERF_COUNT_HW_CPU_CYCLES},
    {PipelinePerfCounter::INSTRUCTIONS, PERF_COUNT_HW_INSTRUCTIONS},
    {PipelinePerfCounter::CACHE_REFERENCES, PERF_COUNT_HW_CACHE_REFERENCES},
    {PipelinePerfCounter::CACHE_MISSES, PERF_COUNT_HW_CACHE_MISSES},
};

static int PerfEventOpen(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
	return static_cast<int>(syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags));
}

static int OpenRaplPerfCounter(int cpu, uint64_t type, uint64_t config, int group_fd, bool disabled) {
	struct perf_event_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.type = static_cast<uint32_t>(type);
	attr.size = sizeof(attr);
	attr.config = config;
	attr.disabled = disabled ? 1 : 0;
	attr.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
	return PerfEventOpen(&attr, -1, cpu, group_fd, 0);
}

static int OpenCgroupPerfCounter(int cgroup_fd, int cpu, uint64_t config, int group_fd, bool disabled) {
	struct perf_event_attr attr;
	memset(&attr, 0, sizeof(attr));
	attr.type = PERF_TYPE_HARDWARE;
	attr.size = sizeof(attr);
	attr.config = config;
	attr.disabled = disabled ? 1 : 0;
	attr.exclude_hv = 1;
	attr.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
	return PerfEventOpen(&attr, cgroup_fd, cpu, group_fd, PERF_FLAG_PID_CGROUP);
}

static void OpenPipelineRaplPerfGroups(vector<PipelineRaplPerfGroupInfo> &groups, idx_t &fd_count, string &error) {
	uint64_t power_type = 0;
	if (!ReadUInt64File("/sys/bus/event_source/devices/power/type", power_type)) {
		error = "RAPL power PMU type is not available";
		return;
	}
	auto specs = GetRaplPerfSpecs(error);
	if (specs.empty()) {
		return;
	}
	auto cpus = ParseCPUList(ReadTextFile("/sys/bus/event_source/devices/power/cpumask"));
	if (cpus.empty()) {
		cpus.push_back(0);
	}

	int first_errno = 0;
	for (auto cpu : cpus) {
		PipelineRaplPerfGroupInfo group;
		for (auto &spec : specs) {
			auto fd = OpenRaplPerfCounter(cpu, power_type, spec.config, group.leader_fd, group.leader_fd < 0);
			if (fd < 0) {
				if (!first_errno) {
					first_errno = errno;
				}
				continue;
			}
			if (group.leader_fd < 0) {
				group.leader_fd = fd;
			}
			group.fds.emplace_back(fd);
			group.counter_ids.push_back(static_cast<idx_t>(spec.counter));
			group.scales.push_back(spec.scale);
		}
		if (group.leader_fd >= 0 && !group.fds.empty()) {
			fd_count += group.fds.size();
			groups.push_back(std::move(group));
		}
	}
	if (groups.empty() && first_errno) {
		error = string(strerror(first_errno));
	}
}

static vector<int> GetAffinityCPUs() {
	vector<int> cpus;
	cpu_set_t cpu_set;
	CPU_ZERO(&cpu_set);
	if (sched_getaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
		return cpus;
	}
	for (int cpu = 0; cpu < CPU_SETSIZE; cpu++) {
		if (CPU_ISSET(cpu, &cpu_set)) {
			cpus.push_back(cpu);
		}
	}
	return cpus;
}

static vector<int> GetPipelinePerfCPUs() {
	return GetAffinityCPUs();
}

static string ResolveCurrentCgroupPath() {
	std::ifstream input("/proc/self/cgroup");
	string line;
	while (std::getline(input, line)) {
		auto first_colon = line.find(':');
		if (first_colon == string::npos) {
			continue;
		}
		auto second_colon = line.find(':', first_colon + 1);
		if (second_colon == string::npos) {
			continue;
		}
		auto controllers = line.substr(first_colon + 1, second_colon - first_colon - 1);
		auto cgroup_path = line.substr(second_colon + 1);
		if (controllers.empty()) {
			return JoinPath("/sys/fs/cgroup", cgroup_path);
		}
	}
	return string();
}

static void OpenPipelinePerfGroups(vector<PipelinePerfGroupInfo> &groups, idx_t &fd_count, idx_t &cpu_count,
                                   string &scope, string &cgroup_path, string &error) {
	scope = "cgroup_per_cpu";
	cgroup_path = ResolveCurrentCgroupPath();
	if (cgroup_path.empty()) {
		error = "could not resolve current cgroup path";
		return;
	}
	auto cgroup_fd = open(cgroup_path.c_str(), O_RDONLY | O_DIRECTORY);
	if (cgroup_fd < 0) {
		error = string("could not open cgroup path ") + cgroup_path + ": " + strerror(errno);
		return;
	}

	auto cpus = GetPipelinePerfCPUs();
	cpu_count = cpus.size();
	if (cpus.empty()) {
		close(cgroup_fd);
		error = "could not resolve CPU affinity set for cgroup-per-CPU perf counters";
		return;
	}

	int first_errno = 0;
	for (auto cpu : cpus) {
		PipelinePerfGroupInfo group;
		group.cpu = cpu;
		for (auto &spec : PIPELINE_PERF_SPECS) {
			auto fd = OpenCgroupPerfCounter(cgroup_fd, cpu, spec.config, group.leader_fd, group.leader_fd < 0);
			if (fd < 0) {
				if (!first_errno) {
					first_errno = errno;
				}
				group.fds.clear();
				group.counter_ids.clear();
				group.leader_fd = -1;
				break;
			}
			if (group.leader_fd < 0) {
				group.leader_fd = fd;
			}
			group.fds.emplace_back(fd);
			group.counter_ids.push_back(static_cast<idx_t>(spec.counter));
		}
		if (group.leader_fd >= 0 && group.fds.size() == sizeof(PIPELINE_PERF_SPECS) / sizeof(PIPELINE_PERF_SPECS[0])) {
			fd_count += group.fds.size();
			groups.push_back(std::move(group));
		}
	}
	close(cgroup_fd);
	if (groups.empty() && first_errno) {
		error = string(strerror(first_errno));
	}
}

struct PipelinePerfGroupReadHeader {
	uint64_t nr = 0;
	uint64_t time_enabled = 0;
	uint64_t time_running = 0;
};

static void AddPerfCounterValue(PipelineProfilingInfo &profile, PipelinePerfCounter counter, double value) {
	switch (counter) {
	case PipelinePerfCounter::CYCLES:
		profile.cycles += value;
		break;
	case PipelinePerfCounter::INSTRUCTIONS:
		profile.instructions += value;
		break;
	case PipelinePerfCounter::CACHE_REFERENCES:
		profile.cache_references += value;
		break;
	case PipelinePerfCounter::CACHE_MISSES:
		profile.cache_misses += value;
		break;
	}
}

static void AddPerfCounterValue(PipelinePerfCPUCounters &counters, PipelinePerfCounter counter, double value) {
	switch (counter) {
	case PipelinePerfCounter::CYCLES:
		counters.cycles += value;
		break;
	case PipelinePerfCounter::INSTRUCTIONS:
		counters.instructions += value;
		break;
	case PipelinePerfCounter::CACHE_REFERENCES:
		counters.cache_references += value;
		break;
	case PipelinePerfCounter::CACHE_MISSES:
		counters.cache_misses += value;
		break;
	}
}

static void ResetAndEnablePerfGroups(const vector<PipelinePerfGroupInfo> &groups) {
	for (auto &group : groups) {
		if (group.leader_fd < 0) {
			continue;
		}
		ioctl(group.leader_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
		ioctl(group.leader_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
	}
}

static void DisablePerfGroups(const vector<PipelinePerfGroupInfo> &groups) {
	for (auto &group : groups) {
		if (group.leader_fd < 0) {
			continue;
		}
		ioctl(group.leader_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
	}
}

static void ResetAndEnableRaplPerfGroups(const vector<PipelineRaplPerfGroupInfo> &groups) {
	for (auto &group : groups) {
		if (group.leader_fd < 0) {
			continue;
		}
		ioctl(group.leader_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
		ioctl(group.leader_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
	}
}

static void DisableRaplPerfGroups(const vector<PipelineRaplPerfGroupInfo> &groups) {
	for (auto &group : groups) {
		if (group.leader_fd < 0) {
			continue;
		}
		ioctl(group.leader_fd, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
	}
}

template <class GROUP>
static PipelinePerfGroupSnapshot ReadCounterGroupSnapshot(const GROUP &group) {
	PipelinePerfGroupSnapshot snapshot;
	if (group.leader_fd < 0) {
		return snapshot;
	}
	vector<uint64_t> read_buffer(3 + group.counter_ids.size());
	auto bytes_read = read(group.leader_fd, read_buffer.data(), read_buffer.size() * sizeof(uint64_t));
	if (bytes_read < static_cast<ssize_t>(3 * sizeof(uint64_t))) {
		return snapshot;
	}
	PipelinePerfGroupReadHeader header;
	header.nr = read_buffer[0];
	header.time_enabled = read_buffer[1];
	header.time_running = read_buffer[2];
	auto values_available = NumericCast<idx_t>(read_buffer.size() - 3);
	auto values_read = MinValue<idx_t>(MinValue<idx_t>(header.nr, group.counter_ids.size()), values_available);
	snapshot.valid = true;
	snapshot.time_enabled = header.time_enabled;
	snapshot.time_running = header.time_running;
	for (idx_t i = 0; i < values_read; i++) {
		snapshot.values.push_back(read_buffer[3 + i]);
	}
	return snapshot;
}

static PipelinePerfGroupSnapshot ReadPerfGroupSnapshot(const PipelinePerfGroupInfo &group) {
	return ReadCounterGroupSnapshot(group);
}

static PipelinePerfGroupSnapshot ReadRaplPerfGroupSnapshot(const PipelineRaplPerfGroupInfo &group) {
	return ReadCounterGroupSnapshot(group);
}

static void ReadPerfGroupSnapshots(const vector<PipelinePerfGroupInfo> &groups,
                                   vector<PipelinePerfGroupSnapshot> &snapshots) {
	snapshots.clear();
	snapshots.reserve(groups.size());
	for (auto &group : groups) {
		snapshots.push_back(ReadPerfGroupSnapshot(group));
	}
}

static void ReadRaplPerfGroupSnapshots(const vector<PipelineRaplPerfGroupInfo> &groups,
                                       vector<PipelinePerfGroupSnapshot> &snapshots) {
	snapshots.clear();
	snapshots.reserve(groups.size());
	for (auto &group : groups) {
		snapshots.push_back(ReadRaplPerfGroupSnapshot(group));
	}
}

static void WarmupPerfGroupSnapshots(const vector<PipelinePerfGroupInfo> &groups) {
	vector<PipelinePerfGroupSnapshot> snapshots;
	ReadPerfGroupSnapshots(groups, snapshots);
}

static void WarmupRaplPerfGroupSnapshots(const vector<PipelineRaplPerfGroupInfo> &groups) {
	vector<PipelinePerfGroupSnapshot> snapshots;
	ReadRaplPerfGroupSnapshots(groups, snapshots);
}

static void AddPerfGroupSnapshotDelta(const PipelinePerfGroupInfo &group, const PipelinePerfGroupSnapshot &start,
                                      const PipelinePerfGroupSnapshot &end, PipelineProfilingInfo &profile,
                                      double &min_running_pct, bool record_per_cpu) {
	if (!start.valid || !end.valid || end.time_enabled < start.time_enabled || end.time_running < start.time_running) {
		return;
	}
	auto enabled_delta = end.time_enabled - start.time_enabled;
	auto running_delta = end.time_running - start.time_running;
	PipelinePerfCPUCounters cpu_counters;
	cpu_counters.cpu = group.cpu;
	if (enabled_delta > 0) {
		auto running_pct = 100.0 * static_cast<double>(running_delta) / static_cast<double>(enabled_delta);
		min_running_pct = std::min(min_running_pct, running_pct);
		cpu_counters.min_running_pct = running_pct;
	}
	auto values_read = MinValue<idx_t>(MinValue<idx_t>(start.values.size(), end.values.size()), group.counter_ids.size());
	bool has_value = false;
	for (idx_t i = 0; i < values_read; i++) {
		if (end.values[i] < start.values[i]) {
			continue;
		}
		double scaled_value = static_cast<double>(end.values[i] - start.values[i]);
		if (running_delta > 0 && enabled_delta > running_delta) {
			scaled_value *= static_cast<double>(enabled_delta) / static_cast<double>(running_delta);
		}
		auto counter = static_cast<PipelinePerfCounter>(group.counter_ids[i]);
		AddPerfCounterValue(profile, counter, scaled_value);
		if (record_per_cpu) {
			AddPerfCounterValue(cpu_counters, counter, scaled_value);
			has_value = true;
		}
	}
	if (record_per_cpu && has_value) {
		profile.per_cpu_perf.push_back(cpu_counters);
	}
}

static void AddRaplPerfGroupSnapshotDelta(const PipelineRaplPerfGroupInfo &group,
                                          const PipelinePerfGroupSnapshot &start,
                                          const PipelinePerfGroupSnapshot &end, PipelineProfilingInfo &profile,
                                          double &min_running_pct) {
	if (!start.valid || !end.valid || end.time_enabled < start.time_enabled || end.time_running < start.time_running) {
		return;
	}
	auto enabled_delta = end.time_enabled - start.time_enabled;
	auto running_delta = end.time_running - start.time_running;
	if (enabled_delta > 0) {
		auto running_pct = 100.0 * static_cast<double>(running_delta) / static_cast<double>(enabled_delta);
		min_running_pct = std::min(min_running_pct, running_pct);
	}
	auto values_read =
	    MinValue<idx_t>(MinValue<idx_t>(start.values.size(), end.values.size()), group.counter_ids.size());
	values_read = MinValue<idx_t>(values_read, group.scales.size());
	for (idx_t i = 0; i < values_read; i++) {
		if (end.values[i] < start.values[i]) {
			continue;
		}
		double scaled_value = static_cast<double>(end.values[i] - start.values[i]);
		if (running_delta > 0 && enabled_delta > running_delta) {
			scaled_value *= static_cast<double>(enabled_delta) / static_cast<double>(running_delta);
		}
		auto joules = scaled_value * group.scales[i];
		switch (static_cast<PipelineRaplPerfCounter>(group.counter_ids[i])) {
		case PipelineRaplPerfCounter::PACKAGE:
			profile.cpu_package_j += joules;
			break;
		case PipelineRaplPerfCounter::DRAM:
			profile.dram_j += joules;
			break;
		}
	}
}

static void ReadPerfSnapshotDeltas(const vector<PipelinePerfGroupInfo> &groups,
                                   const vector<PipelinePerfGroupSnapshot> &start_snapshots,
                                   PipelineProfilingInfo &profile, bool record_per_cpu) {
	double min_running_pct = std::numeric_limits<double>::infinity();
	if (record_per_cpu) {
		profile.per_cpu_perf.clear();
		profile.per_cpu_perf.reserve(groups.size());
	}
	for (idx_t group_idx = 0; group_idx < groups.size() && group_idx < start_snapshots.size(); group_idx++) {
		auto end_snapshot = ReadPerfGroupSnapshot(groups[group_idx]);
		AddPerfGroupSnapshotDelta(groups[group_idx], start_snapshots[group_idx], end_snapshot, profile, min_running_pct,
		                          record_per_cpu);
	}
	if (std::isfinite(min_running_pct)) {
		profile.perf_min_running_pct = min_running_pct;
	}
}

static void ReadRaplPerfSnapshotDeltas(const vector<PipelineRaplPerfGroupInfo> &groups,
                                       const vector<PipelinePerfGroupSnapshot> &start_snapshots,
                                       PipelineProfilingInfo &profile) {
	double min_running_pct = std::numeric_limits<double>::infinity();
	for (idx_t group_idx = 0; group_idx < groups.size() && group_idx < start_snapshots.size(); group_idx++) {
		auto end_snapshot = ReadRaplPerfGroupSnapshot(groups[group_idx]);
		AddRaplPerfGroupSnapshotDelta(groups[group_idx], start_snapshots[group_idx], end_snapshot, profile,
		                              min_running_pct);
	}
	if (std::isfinite(min_running_pct)) {
		profile.rapl_perf_min_running_pct = min_running_pct;
	}
}

#else

#endif

void PipelineDVFSProfiler::Initialize(const PipelineProfilingSettings &settings) {
	pipeline_dvfs_metrics_enabled = settings.dvfs_metrics;
	pipeline_task_trace_enabled = settings.task_trace;
	pipeline_perf_per_cpu_enabled = settings.per_cpu;
	if (!pipeline_dvfs_metrics_enabled || pipeline_perf_setup_attempted) {
		return;
	}
	pipeline_perf_setup_attempted = true;
#ifdef __linux__
	OpenPipelineRaplPerfGroups(pipeline_rapl_perf_groups, pipeline_rapl_perf_fd_count, pipeline_rapl_perf_error);
	pipeline_rapl_perf_supported = !pipeline_rapl_perf_groups.empty();
	if (pipeline_rapl_perf_supported) {
		ResetAndEnableRaplPerfGroups(pipeline_rapl_perf_groups);
		WarmupRaplPerfGroupSnapshots(pipeline_rapl_perf_groups);
	} else {
		WarmupRaplReads();
	}
	OpenPipelinePerfGroups(pipeline_perf_groups, pipeline_perf_fd_count, pipeline_perf_cpu_count, pipeline_perf_scope,
	                       pipeline_perf_cgroup_path, pipeline_perf_error);
	pipeline_perf_supported = !pipeline_perf_groups.empty();
	if (pipeline_perf_supported) {
		ResetAndEnablePerfGroups(pipeline_perf_groups);
		WarmupPerfGroupSnapshots(pipeline_perf_groups);
	}
#else
	pipeline_perf_error = "perf pipeline counters are only supported on Linux";
	pipeline_perf_supported = false;
	pipeline_rapl_perf_error = "RAPL power PMU counters are only supported on Linux";
	pipeline_rapl_perf_supported = false;
#endif
}

void PipelineDVFSProfiler::Start(PipelineProfilingInfo &profile) {
	profile.dvfs_metrics_enabled = pipeline_dvfs_metrics_enabled;
	if (!profile.dvfs_metrics_enabled) {
		return;
	}
#ifdef __linux__
	auto rapl_begin_ns = PipelineProfileTimestampNs();
	profile.rapl_source.clear();
	profile.rapl_error.clear();
	profile.rapl_perf_fd_count = pipeline_rapl_perf_fd_count;
	profile.rapl_perf_group_count = pipeline_rapl_perf_groups.size();
	profile.rapl_perf_min_running_pct = 0;
	profile.cpu_package_j = 0;
	profile.dram_j = 0;
	if (pipeline_rapl_perf_supported) {
		profile.rapl_supported = true;
		profile.rapl_source = "power_pmu";
		ReadRaplPerfGroupSnapshots(pipeline_rapl_perf_groups, profile.rapl_perf_start_snapshots);
	} else {
		profile.rapl_source = "sysfs";
		profile.rapl_error = pipeline_rapl_perf_error;
		ReadRaplStart(profile);
		if (!profile.rapl_supported) {
			profile.rapl_source = "none";
		}
	}
	profile.rapl_start_overhead_ns = PipelineProfileTimestampNs() - rapl_begin_ns;
	profile.perf_supported = pipeline_perf_supported;
	profile.perf_scope = pipeline_perf_scope;
	profile.perf_cgroup_path = pipeline_perf_cgroup_path;
	profile.perf_fd_count = pipeline_perf_fd_count;
	profile.perf_group_count = pipeline_perf_groups.size();
	profile.perf_cpu_count = pipeline_perf_cpu_count;
	profile.perf_error = pipeline_perf_error;
	profile.perf_min_running_pct = 0;
	profile.cycles = 0;
	profile.instructions = 0;
	profile.cache_references = 0;
	profile.cache_misses = 0;
	profile.per_cpu_perf.clear();
	if (pipeline_perf_supported) {
		auto perf_begin_ns = PipelineProfileTimestampNs();
		ReadPerfGroupSnapshots(pipeline_perf_groups, profile.perf_start_snapshots);
		profile.perf_start_overhead_ns = PipelineProfileTimestampNs() - perf_begin_ns;
	}
#else
	profile.perf_supported = false;
	profile.perf_error = pipeline_perf_error;
#endif
}

void PipelineDVFSProfiler::Stop(PipelineProfilingInfo &profile) {
	if (!profile.dvfs_metrics_enabled) {
		return;
	}
#ifdef __linux__
	if (pipeline_perf_supported) {
		auto perf_begin_ns = PipelineProfileTimestampNs();
		ReadPerfSnapshotDeltas(pipeline_perf_groups, profile.perf_start_snapshots, profile,
		                       pipeline_perf_per_cpu_enabled);
		profile.perf_end_overhead_ns = PipelineProfileTimestampNs() - perf_begin_ns;
	}
	auto rapl_begin_ns = PipelineProfileTimestampNs();
	if (pipeline_rapl_perf_supported) {
		ReadRaplPerfSnapshotDeltas(pipeline_rapl_perf_groups, profile.rapl_perf_start_snapshots, profile);
	} else {
		auto &paths = GetPipelineRaplPaths();
		profile.cpu_package_j =
		    ReadRaplDeltaJ(paths.package_paths, profile.rapl_package_start_uj, profile.rapl_package_max_uj);
		profile.dram_j = ReadRaplDeltaJ(paths.dram_paths, profile.rapl_dram_start_uj, profile.rapl_dram_max_uj);
	}
	profile.rapl_end_overhead_ns = PipelineProfileTimestampNs() - rapl_begin_ns;
#endif
}

void PipelineDVFSProfiler::Close() {
#ifdef __linux__
	DisablePerfGroups(pipeline_perf_groups);
	DisableRaplPerfGroups(pipeline_rapl_perf_groups);
#endif
	pipeline_perf_groups.clear();
	pipeline_rapl_perf_groups.clear();
}


} // namespace duckdb
