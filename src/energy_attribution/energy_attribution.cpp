//===----------------------------------------------------------------------===//
//                         DuckDB
//
// energy_attribution.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/energy_attribution/energy_attribution.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>

#ifdef __linux__
#include <fcntl.h>
#include <linux/perf_event.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace duckdb {

namespace {

static const char *DEFAULT_OUTPUT_DIR = "energy_attribution_output";
static const uint64_t NSEC_PER_SEC = 1000000000ULL;
static const idx_t DEFAULT_THREAD_SEGMENT_BUFFER_CAPACITY = 1024;
static const uint64_t DEFAULT_THREAD_SEGMENT_ID_BLOCK_SIZE = 1024;

struct PerfCounters {
	uint64_t cycles = 0;
	uint64_t instructions = 0;
	uint64_t ref_cycles = 0;
	uint64_t cache_refs = 0;
	uint64_t cache_misses = 0;
	uint64_t llc_misses = 0;
	uint64_t offcore_responses = 0;
	bool valid = false;
	bool scaled = false;
	string status;
};

struct RaplDomain {
	int socket_id = -1;
	string path;
	string energy_path;
	uint64_t max_energy_range_uj = 0;
	bool valid = false;
	int energy_fd = -1;
};

struct RaplSnapshot {
	int socket_id = -1;
	uint64_t energy_uj = 0;
	uint64_t max_energy_range_uj = 0;
	bool valid = false;
};

struct BasePowerEntry {
	int socket_id = -1;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_state = 0;
	double base_power_w = 0;
};

struct AttributedSegment {
	EnergySegmentRecord record;
	uint64_t window_id = 0;
	uint64_t slice_start_ns = 0;
	uint64_t slice_end_ns = 0;
	double frequency_ratio = 0;
	double corrected_cycles = 0;
	double activity_weight = 0;
	double weight_denominator = 0;
	double attributed_active_energy_j = 0;
	double base_time_weight = 0;
	double base_time_denominator_s = 0;
	double attributed_base_energy_j = 0;
	double throughput = 0;
	string attribution_status;
};

struct SocketEnergyWindow {
	uint64_t window_id = 0;
	int socket_id = -1;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	double duration_s = 0;
	uint64_t rapl_start_uj = 0;
	uint64_t rapl_end_uj = 0;
	double package_energy_j = 0;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	double base_energy_j = 0;
	double active_package_energy_j = 0;
	double summed_attributed_active_energy_j = 0;
	double active_conservation_error_j = 0;
	double summed_attributed_base_energy_j = 0;
	double base_conservation_error_j = 0;
	double total_base_energy_j = 0;
	double busy_wall_time_s = 0;
	double idle_gap_wall_time_s = 0;
	double busy_base_energy_j = 0;
	double idle_gap_base_energy_j = 0;
	idx_t segment_count = 0;
	bool rapl_valid = false;
	bool base_power_valid = false;
	bool used_time_fallback = false;
	string status;
};

struct PipelineProfileAggregate {
	string pipeline_signature;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_occupancy = 1;
	uint64_t samples = 0;
	double duration_s = 0;
	double work_units = 0;
	double corrected_cycles = 0;
	double active_energy_j = 0;
	double base_energy_j = 0;
};

struct PipelineEfficiencyAggregate {
	uint64_t query_id = 0;
	uint64_t pipeline_id = 0;
	string pipeline_signature;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_occupancy = 1;
	uint64_t sample_count = 0;
	uint64_t valid_input_sample_count = 0;
	uint64_t energy_only_sample_count = 0;
	double total_input_rows = 0;
	double total_input_chunks = 0;
	double total_duration_s = 0;
	double input_duration_s = 0;
	double attributed_active_energy_j = 0;
	double input_attributed_active_energy_j = 0;
	double base_time_share_energy_j = 0;
	double input_base_time_share_energy_j = 0;
	double corrected_cycles = 0;
	double input_corrected_cycles = 0;
	double energy_only_duration_s = 0;
	double energy_only_active_energy_j = 0;
	double energy_only_base_time_share_energy_j = 0;
};

struct PeriodicPipelineProfileKey {
	uint64_t query_id = 0;
	uint64_t pipeline_id = 0;
	int64_t core_freq_hz = 0;
	int64_t uncore_freq_hz = 0;

	bool operator==(const PeriodicPipelineProfileKey &other) const {
		return query_id == other.query_id && pipeline_id == other.pipeline_id && core_freq_hz == other.core_freq_hz &&
		       uncore_freq_hz == other.uncore_freq_hz;
	}
};

struct PeriodicPipelineProfileKeyHash {
	size_t operator()(const PeriodicPipelineProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.query_id);
		result ^= std::hash<uint64_t> {}(key.pipeline_id) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<int64_t> {}(key.core_freq_hz) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<int64_t> {}(key.uncore_freq_hz) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

static int64_t PeriodicFrequencyKey(double frequency_hz) {
	return static_cast<int64_t>(std::llround(frequency_hz));
}

static PeriodicPipelineProfileKey BuildPeriodicPipelineProfileKey(const EnergySegmentRecord &record) {
	PeriodicPipelineProfileKey key;
	key.query_id = record.query_id;
	key.pipeline_id = record.pipeline_id;
	key.core_freq_hz = PeriodicFrequencyKey(record.core_freq_hz);
	key.uncore_freq_hz = PeriodicFrequencyKey(record.uncore_freq_hz);
	return key;
}

struct QueryEnergyState;
static mutex g_query_lock;
static unordered_map<ClientContext *, shared_ptr<QueryEnergyState>> g_queries;
static mutex g_database_settings_lock;
static unordered_map<const DatabaseInstance *, EnergyAttributionSettings> g_database_settings;
static mutex g_base_power_cache_lock;
static unordered_map<string, vector<BasePowerEntry>> g_base_power_cache;

static void StoreDatabaseEnergySettings(DatabaseInstance &db, const EnergyAttributionSettings &settings) {
	lock_guard<mutex> guard(g_database_settings_lock);
	if (settings.enabled && settings.periodic_enabled) {
		g_database_settings[&db] = settings;
	} else {
		g_database_settings.erase(&db);
	}
}

static bool TryGetDatabaseEnergySettings(const DatabaseInstance &db, EnergyAttributionSettings &settings) {
	lock_guard<mutex> guard(g_database_settings_lock);
	auto entry = g_database_settings.find(&db);
	if (entry == g_database_settings.end()) {
		return false;
	}
	settings = entry->second;
	return settings.enabled && settings.periodic_enabled;
}

static void ClearDatabaseEnergySettings(DatabaseInstance &db) {
	lock_guard<mutex> guard(g_database_settings_lock);
	g_database_settings.erase(&db);
}

static uint64_t TimestampNs() {
	using namespace std::chrono;
	return static_cast<uint64_t>(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}

static uint64_t ThreadCpuNs() {
#ifdef __linux__
	struct timespec ts;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) {
		return 0;
	}
	return static_cast<uint64_t>(ts.tv_sec) * NSEC_PER_SEC + static_cast<uint64_t>(ts.tv_nsec);
#else
	return 0;
#endif
}

static uint64_t DurationNs(uint64_t end_ns, uint64_t start_ns) {
	return end_ns >= start_ns ? end_ns - start_ns : 0;
}

static bool ReadFileToString(const string &path, string &result) {
	std::ifstream input(path.c_str());
	if (!input.good()) {
		return false;
	}
	std::stringstream buffer;
	buffer << input.rdbuf();
	result = buffer.str();
	while (!result.empty() &&
	       (result.back() == '\n' || result.back() == '\r' || result.back() == ' ' || result.back() == '\t')) {
		result.pop_back();
	}
	return true;
}

static bool ReadUint64File(const string &path, uint64_t &result) {
	string value;
	if (!ReadFileToString(path, value) || value.empty()) {
		return false;
	}
	try {
		result = StringUtil::ToUnsigned(value);
		return true;
	} catch (...) {
		return false;
	}
}

static bool ReadIntFile(const string &path, int &result) {
	string value;
	if (!ReadFileToString(path, value) || value.empty()) {
		return false;
	}
	try {
		result = NumericCast<int>(StringUtil::ToSigned(value));
		return true;
	} catch (...) {
		return false;
	}
}

static bool PathExists(const string &path) {
	struct stat statbuf;
	return stat(path.c_str(), &statbuf) == 0;
}

static bool EnsureDirectory(const string &path) {
	if (path.empty()) {
		return false;
	}
	if (PathExists(path)) {
		return true;
	}
	if (mkdir(path.c_str(), 0755) == 0) {
		return true;
	}
	return errno == EEXIST;
}

static bool EnsureDirectoryRecursive(const string &path) {
	if (path.empty()) {
		return false;
	}
	string current;
	idx_t start = 0;
	if (path[0] == '/') {
		current = "/";
		start = 1;
	}
	while (start <= path.size()) {
		auto next = path.find('/', start);
		auto part = path.substr(start, next == string::npos ? string::npos : next - start);
		if (!part.empty()) {
			if (!current.empty() && current.back() != '/') {
				current += "/";
			}
			current += part;
			if (!EnsureDirectory(current)) {
				return false;
			}
		}
		if (next == string::npos) {
			break;
		}
		start = next + 1;
	}
	return true;
}

static string CsvEscape(const string &input) {
	bool needs_quote = false;
	for (idx_t i = 0; i < input.size(); i++) {
		if (input[i] == ',' || input[i] == '"' || input[i] == '\n' || input[i] == '\r') {
			needs_quote = true;
			break;
		}
	}
	if (!needs_quote) {
		return input;
	}
	string result = "\"";
	for (idx_t i = 0; i < input.size(); i++) {
		if (input[i] == '"') {
			result += "\"\"";
		} else if (input[i] == '\n' || input[i] == '\r') {
			result += ' ';
		} else {
			result += input[i];
		}
	}
	result += "\"";
	return result;
}

static string TrimCopy(string input) {
	StringUtil::Trim(input);
	return input;
}

static vector<string> SplitCSVLine(const string &line) {
	vector<string> result;
	string current;
	bool in_quotes = false;
	for (idx_t i = 0; i < line.size(); i++) {
		auto c = line[i];
		if (c == '"') {
			if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
				current += '"';
				i++;
			} else {
				in_quotes = !in_quotes;
			}
		} else if (c == ',' && !in_quotes) {
			result.push_back(TrimCopy(current));
			current.clear();
		} else {
			current += c;
		}
	}
	result.push_back(TrimCopy(current));
	return result;
}

static double ParseDoubleOrZero(const string &value) {
	if (value.empty()) {
		return 0;
	}
	try {
		return std::stod(value);
	} catch (...) {
		return 0;
	}
}

static int ParseIntOrDefault(const string &value, int default_value) {
	if (value.empty()) {
		return default_value;
	}
	try {
		return NumericCast<int>(StringUtil::ToSigned(value));
	} catch (...) {
		return default_value;
	}
}

static bool LoadBasePowerCSV(const string &path, vector<BasePowerEntry> &entries) {
	if (path.empty()) {
		return false;
	}
	std::ifstream input(path.c_str());
	if (!input.good()) {
		return false;
	}
	string line;
	bool first = true;
	while (std::getline(input, line)) {
		line = TrimCopy(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		auto fields = SplitCSVLine(line);
		if (first && !fields.empty() && StringUtil::Lower(fields[0]) == "socket_id") {
			first = false;
			continue;
		}
		first = false;
		if (fields.size() < 4) {
			continue;
		}
		BasePowerEntry entry;
		entry.socket_id = ParseIntOrDefault(fields[0], -1);
		if (fields.size() >= 5) {
			entry.core_freq_hz = ParseDoubleOrZero(fields[1]);
			entry.uncore_freq_hz = ParseDoubleOrZero(fields[2]);
			entry.smt_state = ParseIntOrDefault(fields[3], 0);
			entry.base_power_w = ParseDoubleOrZero(fields[4]);
		} else {
			entry.core_freq_hz = 0;
			entry.uncore_freq_hz = ParseDoubleOrZero(fields[1]);
			entry.smt_state = 0;
			entry.base_power_w = ParseDoubleOrZero(fields[2]);
		}
		if (entry.socket_id >= 0 && entry.base_power_w >= 0) {
			entries.push_back(entry);
		}
	}
	return !entries.empty();
}

static bool LoadBasePowerCSVCached(const string &path, vector<BasePowerEntry> &entries) {
	entries.clear();
	if (path.empty()) {
		return false;
	}
	{
		lock_guard<mutex> guard(g_base_power_cache_lock);
		auto entry = g_base_power_cache.find(path);
		if (entry != g_base_power_cache.end()) {
			entries = entry->second;
			return !entries.empty();
		}
	}
	vector<BasePowerEntry> loaded;
	if (!LoadBasePowerCSV(path, loaded)) {
		return false;
	}
	{
		lock_guard<mutex> guard(g_base_power_cache_lock);
		g_base_power_cache[path] = loaded;
	}
	entries = std::move(loaded);
	return !entries.empty();
}

static double FrequencyDistance(double lhs, double rhs) {
	if (lhs <= 0 || rhs <= 0) {
		return 0;
	}
	return std::abs(lhs - rhs) / std::max(lhs, rhs);
}

static bool LookupBasePower(const vector<BasePowerEntry> &entries, int socket_id, double core_freq_hz,
                            double uncore_freq_hz, int smt_state, double &base_power_w) {
	double best_score = std::numeric_limits<double>::max();
	bool found = false;
	for (idx_t i = 0; i < entries.size(); i++) {
		const auto &entry = entries[i];
		if (entry.socket_id != socket_id) {
			continue;
		}
		double score = FrequencyDistance(entry.core_freq_hz, core_freq_hz) +
		               FrequencyDistance(entry.uncore_freq_hz, uncore_freq_hz);
		if (entry.smt_state > 0 && smt_state > 0 && entry.smt_state != smt_state) {
			score += 1.0;
		}
		if (score < best_score) {
			best_score = score;
			base_power_w = entry.base_power_w;
			found = true;
		}
	}
	return found;
}

static int CurrentCPU() {
#ifdef __linux__
	return sched_getcpu();
#else
	return -1;
#endif
}

static int64_t CurrentTID() {
#ifdef __linux__
	return static_cast<int64_t>(syscall(SYS_gettid));
#else
	return -1;
#endif
}

static int ReadSocketForCPU(int cpu) {
	if (cpu < 0) {
		return -1;
	}
	int socket_id = -1;
	ReadIntFile(StringUtil::Format("/sys/devices/system/cpu/cpu%d/topology/physical_package_id", cpu), socket_id);
	return socket_id;
}

static int ReadCoreForCPU(int cpu) {
	if (cpu < 0) {
		return -1;
	}
	int core_id = -1;
	ReadIntFile(StringUtil::Format("/sys/devices/system/cpu/cpu%d/topology/core_id", cpu), core_id);
	return core_id;
}

static double ReadCoreFrequencyHz(int cpu) {
	if (cpu < 0) {
		return 0;
	}
	uint64_t khz = 0;
	if (!ReadUint64File(StringUtil::Format("/sys/devices/system/cpu/cpu%d/cpufreq/scaling_cur_freq", cpu), khz)) {
		ReadUint64File(StringUtil::Format("/sys/devices/system/cpu/cpufreq/policy%d/scaling_cur_freq", cpu), khz);
	}
	return static_cast<double>(khz) * 1000.0;
}

static double ReadUncoreFrequencyHz(int socket_id) {
	if (socket_id < 0) {
		return 0;
	}
	auto package = StringUtil::Format("/sys/devices/system/cpu/intel_uncore_frequency/package_%02d_die_00", socket_id);
	uint64_t khz = 0;
	if (ReadUint64File(package + "/current_freq_khz", khz)) {
		return static_cast<double>(khz) * 1000.0;
	}
	uint64_t min_khz = 0;
	uint64_t max_khz = 0;
	if (ReadUint64File(package + "/min_freq_khz", min_khz) && ReadUint64File(package + "/max_freq_khz", max_khz)) {
		if (min_khz == max_khz) {
			return static_cast<double>(min_khz) * 1000.0;
		}
		return static_cast<double>(max_khz) * 1000.0;
	}
	if (ReadUint64File(package + "/initial_max_freq_khz", khz)) {
		return static_cast<double>(khz) * 1000.0;
	}
	return 0;
}

struct CpuTopologyEntry {
	int socket_id = -1;
	int physical_core_id = -1;
	double core_freq_hz = 0;
	bool valid = false;
};

static vector<CpuTopologyEntry> g_cpu_topology;
static std::once_flag g_cpu_topology_once;

static idx_t GetConfiguredCPUCount() {
#ifdef __linux__
	auto count = sysconf(_SC_NPROCESSORS_CONF);
	if (count > 0) {
		return NumericCast<idx_t>(count);
	}
#endif
	return 0;
}

static void LoadCpuTopologyCache() {
	auto cpu_count = GetConfiguredCPUCount();
	g_cpu_topology.resize(cpu_count);
	for (idx_t cpu = 0; cpu < cpu_count; cpu++) {
		auto &entry = g_cpu_topology[cpu];
		entry.socket_id = ReadSocketForCPU(NumericCast<int>(cpu));
		entry.physical_core_id = ReadCoreForCPU(NumericCast<int>(cpu));
		entry.core_freq_hz = ReadCoreFrequencyHz(NumericCast<int>(cpu));
		entry.valid = entry.socket_id >= 0 && entry.physical_core_id >= 0;
	}
}

static void EnsureCpuTopologyCache() {
	std::call_once(g_cpu_topology_once, LoadCpuTopologyCache);
}

static CpuTopologyEntry LookupCpuTopology(int cpu, bool use_cache) {
	if (cpu < 0) {
		return CpuTopologyEntry();
	}
	if (use_cache) {
		EnsureCpuTopologyCache();
		if (cpu < NumericCast<int>(g_cpu_topology.size()) && g_cpu_topology[cpu].valid) {
			return g_cpu_topology[cpu];
		}
	}
	CpuTopologyEntry entry;
	entry.socket_id = ReadSocketForCPU(cpu);
	entry.physical_core_id = ReadCoreForCPU(cpu);
	entry.core_freq_hz = ReadCoreFrequencyHz(cpu);
	entry.valid = entry.socket_id >= 0 && entry.physical_core_id >= 0;
	return entry;
}

static bool GetSingleAffinityCPU(int &cpu_id) {
#ifdef __linux__
	cpu_set_t mask;
	CPU_ZERO(&mask);
	if (sched_getaffinity(0, sizeof(mask), &mask) != 0) {
		return false;
	}
	int found = -1;
	int count = 0;
	for (int cpu = 0; cpu < CPU_SETSIZE; cpu++) {
		if (!CPU_ISSET(cpu, &mask)) {
			continue;
		}
		found = cpu;
		count++;
		if (count > 1) {
			return false;
		}
	}
	if (count == 1) {
		cpu_id = found;
		return true;
	}
#endif
	return false;
}

static std::map<int, double> BuildQueryUncoreFrequencyCache(bool use_cache) {
	std::map<int, double> result;
	if (!use_cache) {
		return result;
	}
	EnsureCpuTopologyCache();
	vector<int> sockets;
	for (idx_t i = 0; i < g_cpu_topology.size(); i++) {
		const auto &entry = g_cpu_topology[i];
		if (entry.socket_id < 0) {
			continue;
		}
		if (std::find(sockets.begin(), sockets.end(), entry.socket_id) == sockets.end()) {
			sockets.push_back(entry.socket_id);
		}
	}
	for (idx_t i = 0; i < sockets.size(); i++) {
		result[sockets[i]] = ReadUncoreFrequencyHz(sockets[i]);
	}
	return result;
}

class RaplReader {
public:
	RaplReader() {
		Discover();
	}

	~RaplReader() {
		CloseDomains();
	}

	RaplReader(const RaplReader &) = delete;
	RaplReader &operator=(const RaplReader &) = delete;

	vector<RaplSnapshot> Read() const {
		vector<RaplSnapshot> result;
		Read(result);
		return result;
	}

	void Read(vector<RaplSnapshot> &result) const {
		result.clear();
		result.reserve(domains.size());
		for (idx_t i = 0; i < domains.size(); i++) {
			const auto &domain = domains[i];
			RaplSnapshot snapshot;
			snapshot.socket_id = domain.socket_id;
			snapshot.max_energy_range_uj = domain.max_energy_range_uj;
			if (domain.valid) {
				uint64_t value = 0;
				if (ReadDomainEnergy(domain, value)) {
					snapshot.energy_uj = value;
					snapshot.valid = true;
				}
			}
			result.push_back(snapshot);
		}
	}

	bool HasDomains() const {
		return !domains.empty();
	}

private:
	static bool ReadDomainEnergy(const RaplDomain &domain, uint64_t &value) {
#ifdef __linux__
		if (domain.energy_fd >= 0 && ReadUint64Fd(domain.energy_fd, value)) {
			return true;
		}
#endif
		return ReadUint64File(domain.energy_path, value);
	}

#ifdef __linux__
	static bool ReadUint64Fd(int fd, uint64_t &result) {
		char buffer[64];
		auto bytes = pread(fd, buffer, sizeof(buffer) - 1, 0);
		if (bytes <= 0) {
			return false;
		}
		buffer[bytes] = '\0';
		char *end_ptr = nullptr;
		errno = 0;
		auto parsed = std::strtoull(buffer, &end_ptr, 10);
		if (end_ptr == buffer || errno == ERANGE) {
			return false;
		}
		result = static_cast<uint64_t>(parsed);
		return true;
	}
#endif

	void Discover() {
#ifdef __linux__
		const string root = "/sys/class/powercap";
		DIR *dir = opendir(root.c_str());
		if (!dir) {
			return;
		}
		struct dirent *entry;
		while ((entry = readdir(dir)) != nullptr) {
			string name = entry->d_name;
			if (name == "." || name == ".." || name.find("intel-rapl:") != 0) {
				continue;
			}
			if (name.find(':', strlen("intel-rapl:")) != string::npos) {
				continue;
			}
			string path = root + "/" + name;
			string domain_name;
			ReadFileToString(path + "/name", domain_name);
			auto lower_name = StringUtil::Lower(domain_name);
			if (lower_name.find("package") == string::npos) {
				continue;
			}
			RaplDomain domain;
			domain.path = path;
			domain.energy_path = path + "/energy_uj";
			domain.socket_id = NumericCast<int>(domains.size());
			auto dash = lower_name.find('-');
			if (dash != string::npos && dash + 1 < lower_name.size()) {
				domain.socket_id = ParseIntOrDefault(lower_name.substr(dash + 1), domain.socket_id);
			}
			ReadUint64File(path + "/max_energy_range_uj", domain.max_energy_range_uj);
			domain.valid = PathExists(domain.energy_path);
			if (domain.valid) {
				domain.energy_fd = open(domain.energy_path.c_str(), O_RDONLY | O_CLOEXEC);
				if (domain.energy_fd < 0) {
					domain.valid = false;
				}
			}
			domains.push_back(domain);
		}
		closedir(dir);
#endif
	}

	void CloseDomains() {
#ifdef __linux__
		for (idx_t i = 0; i < domains.size(); i++) {
			if (domains[i].energy_fd >= 0) {
				close(domains[i].energy_fd);
				domains[i].energy_fd = -1;
			}
		}
#endif
	}

private:
	vector<RaplDomain> domains;
};

#ifdef __linux__
struct PerfEventSpec {
	string name;
	uint32_t type;
	uint64_t config;
	bool required;
};

static long PerfEventOpen(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
	return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

static uint64_t LLCacheReadMissConfig() {
	return PERF_COUNT_HW_CACHE_LL | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16);
}
#endif

class PerfEventGroup {
public:
	PerfEventGroup() {
	}

	~PerfEventGroup() {
		Close();
	}

	PerfEventGroup(const PerfEventGroup &) = delete;
	PerfEventGroup &operator=(const PerfEventGroup &) = delete;

	bool Open(int64_t tid, const string &counter_profile) {
#ifdef __linux__
		if (IsOpen()) {
			if (opened_tid == tid && opened_profile == counter_profile) {
				return true;
			}
			Close();
		}
		vector<PerfEventSpec> specs;
		specs.push_back({"cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, true});
		specs.push_back({"ref_cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_REF_CPU_CYCLES, false});
		// Diagnostic events intentionally disabled for the counter-weighted model to reduce multiplexing risk.
		// specs.push_back({"instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS, false});
		// specs.push_back({"cache_refs", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES, false});
		// specs.push_back({"cache_misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES, false});
		// specs.push_back({"llc_misses", PERF_TYPE_HW_CACHE, LLCacheReadMissConfig(), false});

		int group_fd = -1;
		for (idx_t i = 0; i < specs.size(); i++) {
			struct perf_event_attr attr;
			memset(&attr, 0, sizeof(attr));
			attr.type = specs[i].type;
			attr.size = sizeof(attr);
			attr.config = specs[i].config;
			attr.disabled = group_fd == -1 ? 1 : 0;
			attr.exclude_kernel = 1;
			attr.exclude_hv = 1;
			attr.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
			int fd = NumericCast<int>(PerfEventOpen(&attr, static_cast<pid_t>(tid), -1, group_fd, 0));
			if (fd < 0) {
				if (specs[i].required && group_fd == -1) {
					status = StringUtil::Format("perf_event_open(%s) failed: %s", specs[i].name, std::strerror(errno));
					Close();
					return false;
				}
				continue;
			}
			if (group_fd == -1) {
				group_fd = fd;
				leader_fd = fd;
			}
			event_names.push_back(specs[i].name);
			fds.push_back(fd);
		}
		if (leader_fd < 0) {
			status = "no perf events opened";
			return false;
		}
		ioctl(leader_fd, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
		if (ioctl(leader_fd, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) != 0) {
			status = StringUtil::Format("PERF_EVENT_IOC_ENABLE failed: %s", std::strerror(errno));
			Close();
			return false;
		}
		status = counter_profile;
		opened_tid = tid;
		opened_profile = counter_profile;
		return true;
#else
		status = "perf_event_open is Linux-only";
		return false;
#endif
	}

	bool IsOpen() const {
		return leader_fd >= 0;
	}

	EnergyPerfSnapshot ReadSnapshot() const {
		EnergyPerfSnapshot snapshot;
		snapshot.status = status;
#ifdef __linux__
		if (leader_fd < 0) {
			return snapshot;
		}
		uint64_t buffer[32];
		memset(buffer, 0, sizeof(buffer));
		auto bytes = read(leader_fd, buffer, sizeof(buffer));
		if (bytes < static_cast<ssize_t>(sizeof(uint64_t) * 3)) {
			snapshot.status =
			    StringUtil::Format("perf read failed: %s", bytes < 0 ? std::strerror(errno) : "short read");
			return snapshot;
		}
		auto nr = buffer[0];
		snapshot.time_enabled = buffer[1];
		snapshot.time_running = buffer[2];
		if (nr > event_names.size()) {
			nr = event_names.size();
		}
		for (idx_t i = 0; i < nr; i++) {
			if (event_names[i] == "cycles") {
				snapshot.cycles = buffer[3 + i];
			} else if (event_names[i] == "instructions") {
				snapshot.instructions = buffer[3 + i];
			} else if (event_names[i] == "ref_cycles") {
				snapshot.ref_cycles = buffer[3 + i];
			} else if (event_names[i] == "cache_refs") {
				snapshot.cache_refs = buffer[3 + i];
			} else if (event_names[i] == "cache_misses") {
				snapshot.cache_misses = buffer[3 + i];
			} else if (event_names[i] == "llc_misses") {
				snapshot.llc_misses = buffer[3 + i];
			}
		}
		snapshot.scaled = snapshot.time_running > 0 && snapshot.time_enabled > snapshot.time_running;
		snapshot.valid = true;
#endif
		return snapshot;
	}

	void Close() {
#ifdef __linux__
		for (idx_t i = 0; i < fds.size(); i++) {
			if (fds[i] >= 0) {
				close(fds[i]);
			}
		}
#endif
		fds.clear();
		event_names.clear();
		leader_fd = -1;
		opened_tid = -1;
		opened_profile.clear();
	}

private:
	vector<int> fds;
	vector<string> event_names;
	int leader_fd = -1;
	int64_t opened_tid = -1;
	string opened_profile;
	string status = "not opened";
};

static uint64_t SnapshotDeltaValue(uint64_t start, uint64_t end) {
	return end >= start ? end - start : 0;
}

static uint64_t ScaleCounterDelta(uint64_t value, double scale) {
	return static_cast<uint64_t>(static_cast<double>(value) * scale);
}

static PerfCounters DeltaPerfCounters(const EnergyPerfSnapshot &start, const EnergyPerfSnapshot &end) {
	PerfCounters counters;
	counters.status = end.status.empty() ? start.status : end.status;
	if (!start.valid || !end.valid) {
		counters.status = counters.status.empty() ? "perf snapshot unavailable" : counters.status;
		return counters;
	}
	auto time_enabled = SnapshotDeltaValue(start.time_enabled, end.time_enabled);
	auto time_running = SnapshotDeltaValue(start.time_running, end.time_running);
	double scale = 1.0;
	if (time_running > 0 && time_enabled > time_running) {
		scale = static_cast<double>(time_enabled) / static_cast<double>(time_running);
		counters.scaled = true;
	} else if (time_running == 0 && time_enabled > 0) {
		counters.status = "perf counters never ran during segment";
		return counters;
	}
	counters.cycles = ScaleCounterDelta(SnapshotDeltaValue(start.cycles, end.cycles), scale);
	counters.instructions = ScaleCounterDelta(SnapshotDeltaValue(start.instructions, end.instructions), scale);
	counters.ref_cycles = ScaleCounterDelta(SnapshotDeltaValue(start.ref_cycles, end.ref_cycles), scale);
	counters.cache_refs = ScaleCounterDelta(SnapshotDeltaValue(start.cache_refs, end.cache_refs), scale);
	counters.cache_misses = ScaleCounterDelta(SnapshotDeltaValue(start.cache_misses, end.cache_misses), scale);
	counters.llc_misses = ScaleCounterDelta(SnapshotDeltaValue(start.llc_misses, end.llc_misses), scale);
	counters.offcore_responses =
	    ScaleCounterDelta(SnapshotDeltaValue(start.offcore_responses, end.offcore_responses), scale);
	counters.valid = counters.cycles > 0 || counters.ref_cycles > 0 || counters.instructions > 0;
	return counters;
}

static void AddPerfCounters(PerfCounters &target, const PerfCounters &delta) {
	target.cycles += delta.cycles;
	target.instructions += delta.instructions;
	target.ref_cycles += delta.ref_cycles;
	target.cache_refs += delta.cache_refs;
	target.cache_misses += delta.cache_misses;
	target.llc_misses += delta.llc_misses;
	target.offcore_responses += delta.offcore_responses;
	target.valid = target.valid || delta.valid;
	target.scaled = target.scaled || delta.scaled;
	if (!delta.status.empty()) {
		if (target.status.empty()) {
			target.status = delta.status;
		} else if (target.status.find(delta.status) == string::npos) {
			target.status += "|" + delta.status;
		}
	}
}

static void ApplyPerfCounters(EnergySegmentRecord &record, const PerfCounters &counters) {
	record.cycles = counters.cycles;
	record.instructions = counters.instructions;
	record.ref_cycles = counters.ref_cycles;
	record.cache_refs = counters.cache_refs;
	record.cache_misses = counters.cache_misses;
	record.llc_misses = counters.llc_misses;
	record.offcore_responses = counters.offcore_responses;
	record.counters_valid = counters.valid;
	record.counters_scaled = counters.scaled;
	record.counter_status = counters.status;
}

struct EnergyThreadSegmentBuffer {
	int64_t linux_tid = -1;
	mutex lock;
	std::atomic<uint64_t> open_segment_start_ns {0};
	vector<EnergySegmentRecord> segments;

	void MarkSegmentOpen(uint64_t start_ns) {
		open_segment_start_ns.store(start_ns, std::memory_order_release);
	}

	void MarkSegmentClosed() {
		open_segment_start_ns.store(0, std::memory_order_release);
	}

	uint64_t OpenSegmentStartNs() const {
		return open_segment_start_ns.load(std::memory_order_acquire);
	}

	void Append(EnergySegmentRecord record) {
		lock_guard<mutex> guard(lock);
		segments.push_back(std::move(record));
	}

	void Drain(vector<EnergySegmentRecord> &target) {
		lock_guard<mutex> guard(lock);
		target.insert(target.end(), std::make_move_iterator(segments.begin()), std::make_move_iterator(segments.end()));
		segments.clear();
	}
};

struct QueryEnergyState : public EnergyAttributionQueryHandle {
	ClientContext *context = nullptr;
	EnergyAttributionSettings settings;
	uint64_t query_id = 0;
	string query;
	string output_dir;
	uint64_t start_ns = 0;
	vector<RaplSnapshot> rapl_start;
	RaplReader rapl;
	vector<BasePowerEntry> base_power_entries;
	std::map<int, double> uncore_freq_by_socket;
	mutex lock;
	std::atomic<uint64_t> next_segment_id {1};
	std::atomic<bool> periodic_active {false};
	vector<EnergySegmentRecord> segments;
	vector<shared_ptr<EnergyThreadSegmentBuffer>> segment_buffers;
	unordered_map<uint64_t, string> pipeline_signatures;
	bool base_power_loaded = false;
	bool rapl_available = false;
};

struct EnergyThreadState {
	int64_t tid = -1;
	bool hardware_initialized = false;
	bool pinned = false;
	int pinned_cpu = -1;
	int pinned_socket_id = -1;
	int pinned_physical_core_id = -1;
	double pinned_core_freq_hz = 0;
	QueryEnergyState *buffer_query = nullptr;
	uint64_t buffer_query_id = 0;
	uint64_t buffer_query_start_ns = 0;
	EnergyThreadSegmentBuffer *buffer = nullptr;
	QueryEnergyState *segment_id_query = nullptr;
	uint64_t segment_id_query_id = 0;
	uint64_t segment_id_query_start_ns = 0;
	uint64_t next_segment_id = 0;
	uint64_t segment_id_limit = 0;
	PerfEventGroup perf_group;
	string perf_profile;
	string perf_status = "not opened";
	bool perf_attempted = false;
	bool perf_open = false;

	int64_t GetTID() {
		if (tid < 0) {
			tid = CurrentTID();
		}
		return tid;
	}

	uint64_t NextSegmentId(QueryEnergyState &query) {
		if (segment_id_query != &query || segment_id_query_id != query.query_id ||
		    segment_id_query_start_ns != query.start_ns || next_segment_id >= segment_id_limit) {
			auto block_start =
			    query.next_segment_id.fetch_add(DEFAULT_THREAD_SEGMENT_ID_BLOCK_SIZE, std::memory_order_relaxed);
			segment_id_query = &query;
			segment_id_query_id = query.query_id;
			segment_id_query_start_ns = query.start_ns;
			next_segment_id = block_start;
			segment_id_limit = block_start + DEFAULT_THREAD_SEGMENT_ID_BLOCK_SIZE;
		}
		return next_segment_id++;
	}

	EnergyPerfSnapshot ReadPerfSnapshot(int64_t current_tid, const EnergyAttributionSettings &settings) {
		EnergyPerfSnapshot snapshot;
		if (!settings.perf_counters_enabled) {
			snapshot.status = "perf disabled";
			return snapshot;
		}
		if (perf_open && perf_profile == settings.counter_profile) {
			return perf_group.ReadSnapshot();
		}
		if (perf_attempted && !perf_open && perf_profile == settings.counter_profile) {
			snapshot.status = perf_status;
			return snapshot;
		}
		if (perf_open) {
			perf_group.Close();
			perf_open = false;
		}
		tid = current_tid;
		perf_profile = settings.counter_profile;
		perf_attempted = true;
		perf_open = perf_group.Open(current_tid, settings.counter_profile);
		perf_status = perf_open ? settings.counter_profile : perf_group.ReadSnapshot().status;
		if (!perf_open) {
			snapshot.status = perf_status;
			return snapshot;
		}
		return perf_group.ReadSnapshot();
	}

	void InitializeHardware(const EnergyAttributionSettings &settings) {
		if (hardware_initialized || !settings.metadata_cache_enabled) {
			return;
		}
		hardware_initialized = true;
		int affinity_cpu = -1;
		if (!GetSingleAffinityCPU(affinity_cpu)) {
			return;
		}
		auto topology = LookupCpuTopology(affinity_cpu, true);
		pinned = topology.valid;
		if (!pinned) {
			return;
		}
		pinned_cpu = affinity_cpu;
		pinned_socket_id = topology.socket_id;
		pinned_physical_core_id = topology.physical_core_id;
		pinned_core_freq_hz = topology.core_freq_hz;
	}
};

static thread_local EnergyThreadState g_thread_energy_state;

static EnergyThreadSegmentBuffer *GetThreadSegmentBuffer(QueryEnergyState &query, int64_t tid) {
	auto &thread_state = g_thread_energy_state;
	if (thread_state.buffer && thread_state.buffer_query == &query && thread_state.buffer_query_id == query.query_id &&
	    thread_state.buffer_query_start_ns == query.start_ns) {
		return thread_state.buffer;
	}
	auto buffer = make_shared_ptr<EnergyThreadSegmentBuffer>();
	buffer->linux_tid = tid;
	buffer->segments.reserve(DEFAULT_THREAD_SEGMENT_BUFFER_CAPACITY);
	{
		lock_guard<mutex> guard(query.lock);
		query.segment_buffers.push_back(buffer);
	}
	thread_state.buffer_query = &query;
	thread_state.buffer_query_id = query.query_id;
	thread_state.buffer_query_start_ns = query.start_ns;
	thread_state.buffer = buffer.get();
	return thread_state.buffer;
}

static string PipelineLabel(uint64_t pipeline_id) {
	return StringUtil::Format("pipeline_%llu", pipeline_id);
}

static string LookupPipelineSignature(QueryEnergyState &query, uint64_t pipeline_id) {
	if (!query.settings.pipeline_signatures_enabled) {
		return PipelineLabel(pipeline_id);
	}
	lock_guard<mutex> guard(query.lock);
	auto entry = query.pipeline_signatures.find(pipeline_id);
	return entry == query.pipeline_signatures.end() ? PipelineLabel(pipeline_id) : entry->second;
}

static void RegisterPipelineSignature(QueryEnergyState &query, uint64_t pipeline_id, string signature) {
	if (!query.settings.pipeline_signatures_enabled) {
		return;
	}
	lock_guard<mutex> guard(query.lock);
	if (query.pipeline_signatures.find(pipeline_id) == query.pipeline_signatures.end()) {
		query.pipeline_signatures[pipeline_id] = std::move(signature);
	}
}

static double GetQueryUncoreFrequency(QueryEnergyState &query, int socket_id) {
	if (socket_id < 0) {
		return 0;
	}
	if (query.settings.metadata_cache_enabled) {
		auto entry = query.uncore_freq_by_socket.find(socket_id);
		return entry == query.uncore_freq_by_socket.end() ? 0 : entry->second;
	}
	return ReadUncoreFrequencyHz(socket_id);
}

static void FillPipelineSignatures(const unordered_map<uint64_t, string> &signatures,
                                   vector<EnergySegmentRecord> &segments, bool full_signatures_enabled) {
	for (idx_t i = 0; i < segments.size(); i++) {
		if (full_signatures_enabled) {
			auto entry = signatures.find(segments[i].pipeline_id);
			if (entry != signatures.end()) {
				segments[i].pipeline_signature = entry->second;
				continue;
			}
		}
		segments[i].pipeline_signature = PipelineLabel(segments[i].pipeline_id);
	}
}

static double SegmentDuration(const EnergySegmentRecord &segment) {
	if (segment.duration_s > 0) {
		return segment.duration_s;
	}
	if (segment.end_ns > segment.start_ns) {
		return static_cast<double>(segment.end_ns - segment.start_ns) / static_cast<double>(NSEC_PER_SEC);
	}
	return 0;
}

static double SegmentFrequencyRatio(const EnergySegmentRecord &record) {
	if (record.ref_cycles > 0) {
		return static_cast<double>(record.cycles) / static_cast<double>(record.ref_cycles);
	}
	return 0;
}

static double SegmentCorrectedCycles(const EnergySegmentRecord &record) {
	if (record.ref_cycles > 0) {
		return static_cast<double>(record.cycles) * SegmentFrequencyRatio(record);
	}
	return static_cast<double>(record.cycles);
}

struct SMTOccupancyGroup {
	vector<uint64_t> starts;
	vector<uint64_t> ends;
};

static void ComputeSMTOccupancy(vector<AttributedSegment> &segments) {
	std::map<std::pair<int, int>, SMTOccupancyGroup> groups;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &record = segments[i].record;
		if (record.socket_id < 0 || record.physical_core_id < 0) {
			segments[i].record.smt_occupancy = 1;
			continue;
		}
		auto &group = groups[std::make_pair(record.socket_id, record.physical_core_id)];
		group.starts.push_back(record.start_ns);
		group.ends.push_back(record.end_ns);
	}
	for (auto entry = groups.begin(); entry != groups.end(); ++entry) {
		auto &group = entry->second;
		std::sort(group.starts.begin(), group.starts.end());
		std::sort(group.ends.begin(), group.ends.end());
	}
	for (idx_t i = 0; i < segments.size(); i++) {
		auto &record = segments[i].record;
		if (record.socket_id < 0 || record.physical_core_id < 0) {
			record.smt_occupancy = 1;
			continue;
		}
		auto entry = groups.find(std::make_pair(record.socket_id, record.physical_core_id));
		if (entry == groups.end()) {
			record.smt_occupancy = 1;
			continue;
		}
		const auto &group = entry->second;
		auto starts_before_end = std::lower_bound(group.starts.begin(), group.starts.end(), record.end_ns);
		auto ends_before_or_at_start = std::upper_bound(group.ends.begin(), group.ends.end(), record.start_ns);
		auto occupancy = static_cast<int64_t>(starts_before_end - group.starts.begin()) -
		                 static_cast<int64_t>(ends_before_or_at_start - group.ends.begin());
		record.smt_occupancy = MaxValue<int>(1, static_cast<int>(occupancy));
	}
}

static double AverageCoreFreqForSocket(const vector<AttributedSegment> &segments, int socket_id) {
	double weighted = 0;
	double duration = 0;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &segment = segments[i].record;
		if (segment.socket_id != socket_id || segment.core_freq_hz <= 0) {
			continue;
		}
		auto segment_duration = SegmentDuration(segment);
		weighted += segment.core_freq_hz * segment_duration;
		duration += segment_duration;
	}
	return duration > 0 ? weighted / duration : 0;
}

static double AverageUncoreFreqForSocket(const vector<AttributedSegment> &segments, int socket_id) {
	double weighted = 0;
	double duration = 0;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &segment = segments[i].record;
		if (segment.socket_id != socket_id || segment.uncore_freq_hz <= 0) {
			continue;
		}
		auto segment_duration = SegmentDuration(segment);
		weighted += segment.uncore_freq_hz * segment_duration;
		duration += segment_duration;
	}
	if (duration > 0) {
		return weighted / duration;
	}
	return ReadUncoreFrequencyHz(socket_id);
}

static uint64_t RaplDeltaUJ(const RaplSnapshot &start, const RaplSnapshot &end) {
	if (!start.valid || !end.valid) {
		return 0;
	}
	if (end.energy_uj >= start.energy_uj) {
		return end.energy_uj - start.energy_uj;
	}
	if (start.max_energy_range_uj > 0) {
		return (start.max_energy_range_uj - start.energy_uj) + end.energy_uj;
	}
	return 0;
}

static string SegmentStatusBase(const EnergySegmentRecord &segment) {
	string status;
	if (!segment.counters_valid) {
		status += "counters_invalid";
	}
	if (segment.counters_scaled) {
		if (!status.empty()) {
			status += "|";
		}
		status += "counters_scaled";
	}
	if (segment.migrated) {
		if (!status.empty()) {
			status += "|";
		}
		status += "migrated";
	}
	if (!segment.hardware_state_stable) {
		if (!status.empty()) {
			status += "|";
		}
		status += "hardware_state_unstable";
	}
	if (status.empty()) {
		status = "ok";
	}
	return status;
}

static double MergedIntervalDurationS(vector<std::pair<uint64_t, uint64_t>> &intervals) {
	if (intervals.empty()) {
		return 0;
	}
	std::sort(intervals.begin(), intervals.end());
	uint64_t merged_start = intervals[0].first;
	uint64_t merged_end = intervals[0].second;
	uint64_t duration_ns = 0;
	for (idx_t i = 1; i < intervals.size(); i++) {
		if (intervals[i].first <= merged_end) {
			merged_end = MaxValue<uint64_t>(merged_end, intervals[i].second);
			continue;
		}
		if (merged_end > merged_start) {
			duration_ns += merged_end - merged_start;
		}
		merged_start = intervals[i].first;
		merged_end = intervals[i].second;
	}
	if (merged_end > merged_start) {
		duration_ns += merged_end - merged_start;
	}
	return static_cast<double>(duration_ns) / static_cast<double>(NSEC_PER_SEC);
}

static vector<SocketEnergyWindow> BuildSocketWindows(QueryEnergyState &state, const vector<AttributedSegment> &segments,
                                                     const vector<RaplSnapshot> &rapl_end, uint64_t end_ns) {
	vector<SocketEnergyWindow> windows;
	std::map<int, RaplSnapshot> start_by_socket;
	for (idx_t i = 0; i < state.rapl_start.size(); i++) {
		start_by_socket[state.rapl_start[i].socket_id] = state.rapl_start[i];
	}
	for (idx_t i = 0; i < rapl_end.size(); i++) {
		const auto &end = rapl_end[i];
		auto start_entry = start_by_socket.find(end.socket_id);
		if (start_entry == start_by_socket.end()) {
			continue;
		}
		SocketEnergyWindow window;
		window.window_id = state.query_id;
		window.socket_id = end.socket_id;
		window.start_ns = state.start_ns;
		window.end_ns = end_ns;
		window.duration_s = end_ns > state.start_ns
		                        ? static_cast<double>(end_ns - state.start_ns) / static_cast<double>(NSEC_PER_SEC)
		                        : 0;
		window.rapl_start_uj = start_entry->second.energy_uj;
		window.rapl_end_uj = end.energy_uj;
		window.rapl_valid = start_entry->second.valid && end.valid;
		auto delta_uj = RaplDeltaUJ(start_entry->second, end);
		window.package_energy_j = static_cast<double>(delta_uj) / 1000000.0;
		window.core_freq_hz = AverageCoreFreqForSocket(segments, window.socket_id);
		window.uncore_freq_hz = AverageUncoreFreqForSocket(segments, window.socket_id);
		double base_power_w = 0;
		window.base_power_valid = LookupBasePower(state.base_power_entries, window.socket_id, window.core_freq_hz,
		                                          window.uncore_freq_hz, 0, base_power_w);
		window.base_energy_j = window.base_power_valid ? base_power_w * window.duration_s : 0;
		window.total_base_energy_j = window.base_energy_j;
		window.active_package_energy_j = std::max(0.0, window.package_energy_j - window.base_energy_j);
		if (!window.rapl_valid) {
			window.status = "rapl_unavailable";
		} else if (!window.base_power_valid) {
			window.status = "missing_base_power_calibration";
		} else {
			window.status = "ok";
		}
		windows.push_back(window);
	}
	if (!windows.empty()) {
		return windows;
	}
	std::map<int, bool> sockets;
	for (idx_t i = 0; i < segments.size(); i++) {
		if (segments[i].record.socket_id >= 0) {
			sockets[segments[i].record.socket_id] = true;
		}
	}
	for (auto entry = sockets.begin(); entry != sockets.end(); ++entry) {
		SocketEnergyWindow window;
		window.window_id = state.query_id;
		window.socket_id = entry->first;
		window.start_ns = state.start_ns;
		window.end_ns = end_ns;
		window.duration_s = end_ns > state.start_ns
		                        ? static_cast<double>(end_ns - state.start_ns) / static_cast<double>(NSEC_PER_SEC)
		                        : 0;
		window.core_freq_hz = AverageCoreFreqForSocket(segments, window.socket_id);
		window.uncore_freq_hz = AverageUncoreFreqForSocket(segments, window.socket_id);
		window.status = "rapl_unavailable";
		window.total_base_energy_j = window.base_energy_j;
		windows.push_back(window);
	}
	return windows;
}

static void AttributeSegments(vector<AttributedSegment> &segments, vector<SocketEnergyWindow> &windows) {
	ComputeSMTOccupancy(segments);
	for (idx_t i = 0; i < segments.size(); i++) {
		auto &segment = segments[i];
		segment.attribution_status = SegmentStatusBase(segment.record);
		if (segment.record.ref_cycles > 0) {
			segment.frequency_ratio = SegmentFrequencyRatio(segment.record);
			segment.corrected_cycles = SegmentCorrectedCycles(segment.record);
		} else {
			segment.frequency_ratio = 0;
			segment.corrected_cycles = SegmentCorrectedCycles(segment.record);
			if (segment.record.cycles > 0) {
				segment.attribution_status += "|missing_ref_cycles";
			}
		}
		auto duration = SegmentDuration(segment.record);
		if (duration > 0 && segment.record.work_units > 0) {
			segment.throughput = static_cast<double>(segment.record.work_units) / duration;
		}
	}

	vector<std::pair<idx_t, double>> eligible_segments;
	eligible_segments.reserve(segments.size());
	vector<std::pair<uint64_t, uint64_t>> busy_intervals;
	busy_intervals.reserve(segments.size());
	for (idx_t w = 0; w < windows.size(); w++) {
		auto &window = windows[w];
		double weight_sum = 0;
		double time_sum = 0;
		eligible_segments.clear();
		busy_intervals.clear();
		window.segment_count = 0;
		for (idx_t i = 0; i < segments.size(); i++) {
			auto &segment = segments[i];
			if (segment.record.socket_id != window.socket_id || segment.record.migrated) {
				continue;
			}
			auto duration = SegmentDuration(segment.record);
			eligible_segments.emplace_back(i, duration);
			window.segment_count++;
			time_sum += duration;
			if (segment.record.counters_valid && segment.corrected_cycles > 0) {
				weight_sum += segment.corrected_cycles;
			}
			if (segment.record.end_ns > segment.record.start_ns) {
				busy_intervals.emplace_back(segment.record.start_ns, segment.record.end_ns);
			}
		}
		window.used_time_fallback = weight_sum <= 0;
		window.total_base_energy_j = window.base_energy_j;
		window.busy_wall_time_s = MergedIntervalDurationS(busy_intervals);
		window.busy_wall_time_s = MinValue<double>(window.busy_wall_time_s, window.duration_s);
		window.idle_gap_wall_time_s = MaxValue<double>(0, window.duration_s - window.busy_wall_time_s);
		window.busy_base_energy_j =
		    window.duration_s > 0 ? window.base_energy_j * window.busy_wall_time_s / window.duration_s : 0;
		window.idle_gap_base_energy_j = MaxValue<double>(0, window.base_energy_j - window.busy_base_energy_j);
		for (idx_t i = 0; i < eligible_segments.size(); i++) {
			auto &segment = segments[eligible_segments[i].first];
			auto duration = eligible_segments[i].second;
			double weight = 0;
			if (weight_sum > 0 && segment.record.counters_valid && segment.corrected_cycles > 0) {
				segment.activity_weight = segment.corrected_cycles;
				segment.weight_denominator = weight_sum;
				weight = segment.activity_weight / segment.weight_denominator;
			} else if (weight_sum <= 0 && time_sum > 0) {
				segment.activity_weight = duration;
				segment.weight_denominator = time_sum;
				weight = segment.activity_weight / segment.weight_denominator;
				segment.attribution_status += "|time_weight_fallback";
			} else {
				segment.attribution_status += "|excluded_no_counter_weight";
			}
			segment.attributed_active_energy_j = window.active_package_energy_j * weight;
			window.summed_attributed_active_energy_j += segment.attributed_active_energy_j;
			if (time_sum <= 0) {
				segment.attribution_status += "|excluded_no_base_time";
				continue;
			}
			segment.base_time_weight = duration;
			segment.base_time_denominator_s = time_sum;
			segment.attributed_base_energy_j =
			    window.busy_base_energy_j * segment.base_time_weight / segment.base_time_denominator_s;
			window.summed_attributed_base_energy_j += segment.attributed_base_energy_j;
		}
		window.active_conservation_error_j = window.active_package_energy_j - window.summed_attributed_active_energy_j;
		window.base_conservation_error_j = window.busy_base_energy_j - window.summed_attributed_base_energy_j;
	}
}

static string ProfileKey(const EnergySegmentRecord &record) {
	return record.pipeline_signature + "|" +
	       StringUtil::Format("%.0f|%.0f|%d", record.core_freq_hz, record.uncore_freq_hz, record.smt_occupancy);
}

static string EfficiencyProfileKey(const EnergySegmentRecord &record) {
	return std::to_string(record.query_id) + "|" + std::to_string(record.pipeline_id) + "|" +
	       record.pipeline_signature + "|" +
	       StringUtil::Format("%.0f|%.0f|%d", record.core_freq_hz, record.uncore_freq_hz, record.smt_occupancy);
}

static vector<PipelineProfileAggregate> BuildProfiles(const vector<AttributedSegment> &segments) {
	std::map<string, PipelineProfileAggregate> profiles;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &segment = segments[i];
		if (!segment.record.counters_valid || !segment.record.hardware_state_stable || segment.record.work_units == 0 ||
		    SegmentDuration(segment.record) <= 0) {
			continue;
		}
		auto key = ProfileKey(segment.record);
		auto &profile = profiles[key];
		if (profile.samples == 0) {
			profile.pipeline_signature = segment.record.pipeline_signature;
			profile.core_freq_hz = segment.record.core_freq_hz;
			profile.uncore_freq_hz = segment.record.uncore_freq_hz;
			profile.smt_occupancy = segment.record.smt_occupancy;
		}
		profile.samples++;
		profile.duration_s += SegmentDuration(segment.record);
		profile.work_units += static_cast<double>(segment.record.work_units);
		profile.corrected_cycles += segment.corrected_cycles;
		profile.active_energy_j += segment.attributed_active_energy_j;
		profile.base_energy_j += segment.attributed_base_energy_j;
	}
	vector<PipelineProfileAggregate> result;
	for (auto entry = profiles.begin(); entry != profiles.end(); ++entry) {
		result.push_back(entry->second);
	}
	return result;
}

static void UpdateEfficiencyAggregateTimeRange(PipelineEfficiencyAggregate &profile, uint64_t start_ns,
                                               uint64_t end_ns) {
	if (start_ns > 0) {
		profile.start_ns = profile.start_ns == 0 ? start_ns : MinValue<uint64_t>(profile.start_ns, start_ns);
	}
	if (end_ns > 0) {
		profile.end_ns = MaxValue<uint64_t>(profile.end_ns, end_ns);
	}
}

static vector<PipelineEfficiencyAggregate> BuildEfficiencyProfiles(const vector<AttributedSegment> &segments) {
	std::map<string, PipelineEfficiencyAggregate> profiles;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &segment = segments[i];
		auto duration = SegmentDuration(segment.record);
		if (!segment.record.counters_valid || !segment.record.hardware_state_stable || duration <= 0) {
			continue;
		}

		auto key = EfficiencyProfileKey(segment.record);
		auto &profile = profiles[key];
		if (profile.sample_count == 0) {
			profile.query_id = segment.record.query_id;
			profile.pipeline_id = segment.record.pipeline_id;
			profile.pipeline_signature = segment.record.pipeline_signature;
			profile.core_freq_hz = segment.record.core_freq_hz;
			profile.uncore_freq_hz = segment.record.uncore_freq_hz;
			profile.smt_occupancy = segment.record.smt_occupancy;
		}
		UpdateEfficiencyAggregateTimeRange(profile, segment.record.start_ns, segment.record.end_ns);

		profile.sample_count++;
		profile.total_duration_s += duration;
		profile.attributed_active_energy_j += segment.attributed_active_energy_j;
		profile.base_time_share_energy_j += segment.attributed_base_energy_j;
		profile.corrected_cycles += segment.corrected_cycles;

		if (segment.record.work_units > 0) {
			profile.valid_input_sample_count++;
			profile.total_input_rows += static_cast<double>(segment.record.work_units);
			profile.total_input_chunks += static_cast<double>(segment.record.chunks);
			profile.input_duration_s += duration;
			profile.input_attributed_active_energy_j += segment.attributed_active_energy_j;
			profile.input_base_time_share_energy_j += segment.attributed_base_energy_j;
			profile.input_corrected_cycles += segment.corrected_cycles;
		} else {
			profile.energy_only_sample_count++;
			profile.energy_only_duration_s += duration;
			profile.energy_only_active_energy_j += segment.attributed_active_energy_j;
			profile.energy_only_base_time_share_energy_j += segment.attributed_base_energy_j;
		}
	}
	vector<PipelineEfficiencyAggregate> result;
	for (auto entry = profiles.begin(); entry != profiles.end(); ++entry) {
		result.push_back(entry->second);
	}
	return result;
}

static void WriteSegmentsCSV(const string &path, const vector<AttributedSegment> &segments) {
	std::ofstream out(path.c_str());
	out << "segment_id,plan_version,worker_id,linux_tid,socket_id,physical_core_id,logical_cpu_id,end_socket_id,"
	       "end_physical_core_id,end_logical_cpu_id,query_id,pipeline_id,pipeline_signature,role,start_ns,end_ns,"
	       "duration_s,work_units,tuples,chunks,core_freq_hz,uncore_freq_hz,smt_occupancy,cycles,instructions,"
	       "ref_cycles,cache_refs,cache_misses,llc_misses,offcore_responses,counters_valid,counters_scaled,"
	       "hardware_state_stable,migrated,counter_status\n";
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &r = segments[i].record;
		out << r.segment_id << "," << r.plan_version << "," << r.worker_id << "," << r.linux_tid << "," << r.socket_id
		    << "," << r.physical_core_id << "," << r.logical_cpu_id << "," << r.end_socket_id << ","
		    << r.end_physical_core_id << "," << r.end_logical_cpu_id << "," << r.query_id << "," << r.pipeline_id << ","
		    << CsvEscape(r.pipeline_signature) << "," << EnergySegmentRoleToString(r.role) << "," << r.start_ns << ","
		    << r.end_ns << "," << r.duration_s << "," << r.work_units << "," << r.tuples << "," << r.chunks << ","
		    << r.core_freq_hz << "," << r.uncore_freq_hz << "," << r.smt_occupancy << "," << r.cycles << ","
		    << r.instructions << "," << r.ref_cycles << "," << r.cache_refs << "," << r.cache_misses << ","
		    << r.llc_misses << "," << r.offcore_responses << "," << r.counters_valid << "," << r.counters_scaled << ","
		    << r.hardware_state_stable << "," << r.migrated << "," << CsvEscape(r.counter_status) << "\n";
	}
}

static void WriteClosedSegmentsCSV(const string &path, vector<EnergySegmentRecord> &segments) {
	std::sort(segments.begin(), segments.end(), [](const EnergySegmentRecord &lhs, const EnergySegmentRecord &rhs) {
		if (lhs.start_ns != rhs.start_ns) {
			return lhs.start_ns < rhs.start_ns;
		}
		return lhs.segment_id < rhs.segment_id;
	});
	std::ofstream out(path.c_str());
	out << "segment_id,plan_version,worker_id,linux_tid,socket_id,physical_core_id,logical_cpu_id,end_socket_id,"
	       "end_physical_core_id,end_logical_cpu_id,query_id,pipeline_id,pipeline_signature,role,start_ns,end_ns,"
	       "duration_s,work_units,tuples,chunks,core_freq_hz,uncore_freq_hz,smt_occupancy,cycles,instructions,"
	       "ref_cycles,cache_refs,cache_misses,llc_misses,offcore_responses,counters_valid,counters_scaled,"
	       "hardware_state_stable,migrated,counter_status,frequency_ratio,corrected_cycles,throughput_rows_per_s\n";
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &r = segments[i];
		auto duration = SegmentDuration(r);
		auto throughput = duration > 0 ? static_cast<double>(r.work_units) / duration : 0;
		out << r.segment_id << "," << r.plan_version << "," << r.worker_id << "," << r.linux_tid << "," << r.socket_id
		    << "," << r.physical_core_id << "," << r.logical_cpu_id << "," << r.end_socket_id << ","
		    << r.end_physical_core_id << "," << r.end_logical_cpu_id << "," << r.query_id << "," << r.pipeline_id << ","
		    << CsvEscape(r.pipeline_signature) << "," << EnergySegmentRoleToString(r.role) << "," << r.start_ns << ","
		    << r.end_ns << "," << duration << "," << r.work_units << "," << r.tuples << "," << r.chunks << ","
		    << r.core_freq_hz << "," << r.uncore_freq_hz << "," << r.smt_occupancy << "," << r.cycles << ","
		    << r.instructions << "," << r.ref_cycles << "," << r.cache_refs << "," << r.cache_misses << ","
		    << r.llc_misses << "," << r.offcore_responses << "," << r.counters_valid << "," << r.counters_scaled << ","
		    << r.hardware_state_stable << "," << r.migrated << "," << CsvEscape(r.counter_status) << ","
		    << SegmentFrequencyRatio(r) << "," << SegmentCorrectedCycles(r) << "," << throughput << "\n";
	}
}

static void WriteSocketWindowsCSV(const string &path, const vector<SocketEnergyWindow> &windows) {
	std::ofstream out(path.c_str());
	out << "window_id,socket_id,start_ns,end_ns,duration_s,rapl_start_uj,rapl_end_uj,package_energy_j,core_freq_hz,"
	       "uncore_freq_hz,base_energy_j,active_package_energy_j,summed_attributed_active_energy_j,"
	       "active_conservation_error_j,summed_attributed_base_energy_j,base_conservation_error_j,"
	       "summed_charged_energy_j,package_conservation_error_j,rapl_valid,base_power_valid,used_time_fallback,status,"
	       "segment_count,total_base_energy_j,busy_wall_time_s,idle_gap_wall_time_s,busy_base_energy_j,"
	       "idle_gap_base_energy_j\n";
	for (idx_t i = 0; i < windows.size(); i++) {
		const auto &w = windows[i];
		double summed_charged_energy = w.summed_attributed_active_energy_j + w.summed_attributed_base_energy_j;
		double package_conservation_error = w.package_energy_j - summed_charged_energy;
		out << w.window_id << "," << w.socket_id << "," << w.start_ns << "," << w.end_ns << "," << w.duration_s << ","
		    << w.rapl_start_uj << "," << w.rapl_end_uj << "," << w.package_energy_j << "," << w.core_freq_hz << ","
		    << w.uncore_freq_hz << "," << w.base_energy_j << "," << w.active_package_energy_j << ","
		    << w.summed_attributed_active_energy_j << "," << w.active_conservation_error_j << ","
		    << w.summed_attributed_base_energy_j << "," << w.base_conservation_error_j << "," << summed_charged_energy
		    << "," << package_conservation_error << "," << w.rapl_valid << "," << w.base_power_valid << ","
		    << w.used_time_fallback << "," << CsvEscape(w.status) << "," << w.segment_count << ","
		    << w.total_base_energy_j << "," << w.busy_wall_time_s << "," << w.idle_gap_wall_time_s << ","
		    << w.busy_base_energy_j << "," << w.idle_gap_base_energy_j << "\n";
	}
}

static void WriteAttributedSegmentsCSV(const string &path, const vector<AttributedSegment> &segments) {
	std::ofstream out(path.c_str());
	out << "segment_id,query_id,pipeline_id,pipeline_signature,socket_id,physical_core_id,logical_cpu_id,duration_s,"
	       "work_units,frequency_ratio,corrected_cycles,activity_weight,weight_denominator,"
	       "attributed_active_energy_j,base_time_weight,base_time_denominator_s,attributed_base_energy_j,"
	       "charged_total_energy_j,throughput,corrected_cycles_per_work,active_package_power_w,charged_power_w,"
	       "attribution_status\n";
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &s = segments[i];
		double cycles_per_work =
		    s.record.work_units > 0 ? s.corrected_cycles / static_cast<double>(s.record.work_units) : 0;
		double duration = SegmentDuration(s.record);
		double charged_total_energy = s.attributed_active_energy_j + s.attributed_base_energy_j;
		double active_power = duration > 0 ? s.attributed_active_energy_j / duration : 0;
		double charged_power = duration > 0 ? charged_total_energy / duration : 0;
		out << s.record.segment_id << "," << s.record.query_id << "," << s.record.pipeline_id << ","
		    << CsvEscape(s.record.pipeline_signature) << "," << s.record.socket_id << "," << s.record.physical_core_id
		    << "," << s.record.logical_cpu_id << "," << duration << "," << s.record.work_units << ","
		    << s.frequency_ratio << "," << s.corrected_cycles << "," << s.activity_weight << "," << s.weight_denominator
		    << "," << s.attributed_active_energy_j << "," << s.base_time_weight << "," << s.base_time_denominator_s
		    << "," << s.attributed_base_energy_j << "," << charged_total_energy << "," << s.throughput << ","
		    << cycles_per_work << "," << active_power << "," << charged_power << "," << CsvEscape(s.attribution_status)
		    << "\n";
	}
}

static void WriteProfilesCSV(const string &path, const vector<PipelineProfileAggregate> &profiles) {
	std::ofstream out(path.c_str());
	out << "pipeline_signature,core_freq_hz,uncore_freq_hz,smt_occupancy,sample_count,throughput_estimate,"
	       "corrected_cycles_per_work_estimate,active_package_power_estimate_w,charged_package_power_estimate_w,"
	       "total_work_units,total_duration_s,total_active_energy_j,total_base_energy_j,total_charged_energy_j\n";
	for (idx_t i = 0; i < profiles.size(); i++) {
		const auto &p = profiles[i];
		double throughput = p.duration_s > 0 ? p.work_units / p.duration_s : 0;
		double cycles_per_work = p.work_units > 0 ? p.corrected_cycles / p.work_units : 0;
		double active_power = p.duration_s > 0 ? p.active_energy_j / p.duration_s : 0;
		double charged_energy = p.active_energy_j + p.base_energy_j;
		double charged_power = p.duration_s > 0 ? charged_energy / p.duration_s : 0;
		out << CsvEscape(p.pipeline_signature) << "," << p.core_freq_hz << "," << p.uncore_freq_hz << ","
		    << p.smt_occupancy << "," << p.samples << "," << throughput << "," << cycles_per_work << "," << active_power
		    << "," << charged_power << "," << p.work_units << "," << p.duration_s << "," << p.active_energy_j << ","
		    << p.base_energy_j << "," << charged_energy << "\n";
	}
}

static void WriteEfficiencyProfilesCSV(const string &path, const vector<PipelineEfficiencyAggregate> &profiles,
                                       bool include_query_lifecycle = false) {
	std::ofstream out(path.c_str());
	out << "query_id,";
	if (include_query_lifecycle) {
		out << "start_ns,end_ns,elapsed_s,";
	}
	out << "pipeline_id,pipeline_signature,core_freq_hz,uncore_freq_hz,smt_occupancy,sample_count,"
	       "valid_input_sample_count,energy_only_sample_count,total_input_rows,total_input_chunks,total_duration_s,"
	       "input_duration_s,total_duration_throughput_rows_per_s,input_segment_throughput_rows_per_s,"
	       "attributed_active_energy_j,input_attributed_active_energy_j,base_time_share_energy_j,"
	       "input_base_time_share_energy_j,charged_total_energy_j,input_charged_total_energy_j,active_power_w,"
	       "charged_power_w,active_throughput_per_watt_rows_per_j,charged_throughput_per_watt_rows_per_j,"
	       "active_energy_per_row_j,charged_energy_per_row_j,corrected_cycles,corrected_cycles_per_row,"
	       "input_corrected_cycles_per_row,energy_only_duration_s,energy_only_active_energy_j,"
	       "energy_only_base_time_share_energy_j,energy_only_charged_energy_j\n";
	for (idx_t i = 0; i < profiles.size(); i++) {
		const auto &p = profiles[i];
		double total_throughput = p.total_duration_s > 0 ? p.total_input_rows / p.total_duration_s : 0;
		double input_throughput = p.input_duration_s > 0 ? p.total_input_rows / p.input_duration_s : 0;
		double charged_total_energy = p.attributed_active_energy_j + p.base_time_share_energy_j;
		double input_charged_total_energy = p.input_attributed_active_energy_j + p.input_base_time_share_energy_j;
		double active_power = p.total_duration_s > 0 ? p.attributed_active_energy_j / p.total_duration_s : 0;
		double charged_power = p.total_duration_s > 0 ? charged_total_energy / p.total_duration_s : 0;
		double active_throughput_per_watt = active_power > 0 ? total_throughput / active_power : 0;
		double charged_throughput_per_watt = charged_power > 0 ? total_throughput / charged_power : 0;
		double active_energy_per_row = p.total_input_rows > 0 ? p.attributed_active_energy_j / p.total_input_rows : 0;
		double charged_energy_per_row = p.total_input_rows > 0 ? charged_total_energy / p.total_input_rows : 0;
		double corrected_cycles_per_row = p.total_input_rows > 0 ? p.corrected_cycles / p.total_input_rows : 0;
		double input_corrected_cycles_per_row =
		    p.total_input_rows > 0 ? p.input_corrected_cycles / p.total_input_rows : 0;
		double energy_only_charged_energy = p.energy_only_active_energy_j + p.energy_only_base_time_share_energy_j;
		double elapsed_s =
		    p.end_ns > p.start_ns ? static_cast<double>(p.end_ns - p.start_ns) / static_cast<double>(NSEC_PER_SEC) : 0;
		out << p.query_id << ",";
		if (include_query_lifecycle) {
			out << p.start_ns << "," << p.end_ns << "," << elapsed_s << ",";
		}
		out << p.pipeline_id << "," << CsvEscape(p.pipeline_signature) << "," << p.core_freq_hz << ","
		    << p.uncore_freq_hz << "," << p.smt_occupancy << "," << p.sample_count << "," << p.valid_input_sample_count
		    << "," << p.energy_only_sample_count << "," << p.total_input_rows << "," << p.total_input_chunks << ","
		    << p.total_duration_s << "," << p.input_duration_s << "," << total_throughput << "," << input_throughput
		    << "," << p.attributed_active_energy_j << "," << p.input_attributed_active_energy_j << ","
		    << p.base_time_share_energy_j << "," << p.input_base_time_share_energy_j << "," << charged_total_energy
		    << "," << input_charged_total_energy << "," << active_power << "," << charged_power << ","
		    << active_throughput_per_watt << "," << charged_throughput_per_watt << "," << active_energy_per_row << ","
		    << charged_energy_per_row << "," << p.corrected_cycles << "," << corrected_cycles_per_row << ","
		    << input_corrected_cycles_per_row << "," << p.energy_only_duration_s << "," << p.energy_only_active_energy_j
		    << "," << p.energy_only_base_time_share_energy_j << "," << energy_only_charged_energy << "\n";
	}
}

static void WriteValidationSummary(const string &path, const vector<SocketEnergyWindow> &windows,
                                   const vector<AttributedSegment> &segments) {
	std::ofstream out(path.c_str());
	bool any_migration = false;
	idx_t input_segment_count = 0;
	idx_t energy_only_segment_count = 0;
	double input_segment_active_energy_j = 0;
	double energy_only_segment_active_energy_j = 0;
	double input_segment_base_energy_j = 0;
	double energy_only_segment_base_energy_j = 0;
	for (idx_t i = 0; i < segments.size(); i++) {
		any_migration = any_migration || segments[i].record.migrated;
		if (segments[i].record.work_units > 0) {
			input_segment_count++;
			input_segment_active_energy_j += segments[i].attributed_active_energy_j;
			input_segment_base_energy_j += segments[i].attributed_base_energy_j;
		} else {
			energy_only_segment_count++;
			energy_only_segment_active_energy_j += segments[i].attributed_active_energy_j;
			energy_only_segment_base_energy_j += segments[i].attributed_base_energy_j;
		}
	}
	out << "{\n";
	out << "  \"socket_windows\": [\n";
	for (idx_t i = 0; i < windows.size(); i++) {
		const auto &w = windows[i];
		double summed_charged_energy = w.summed_attributed_active_energy_j + w.summed_attributed_base_energy_j;
		double package_conservation_error = w.package_energy_j - summed_charged_energy;
		out << "    {\"socket_id\": " << w.socket_id << ", \"active_package_energy_j\": " << w.active_package_energy_j
		    << ", \"attributed_active_energy_j\": " << w.summed_attributed_active_energy_j
		    << ", \"active_conservation_error_j\": " << w.active_conservation_error_j
		    << ", \"base_energy_j\": " << w.base_energy_j
		    << ", \"attributed_base_energy_j\": " << w.summed_attributed_base_energy_j
		    << ", \"base_conservation_error_j\": " << w.base_conservation_error_j
		    << ", \"charged_energy_j\": " << summed_charged_energy
		    << ", \"package_conservation_error_j\": " << package_conservation_error
		    << ", \"time_fallback\": " << (w.used_time_fallback ? "true" : "false") << "}";
		if (i + 1 < windows.size()) {
			out << ",";
		}
		out << "\n";
	}
	out << "  ],\n";
	out << "  \"segment_count\": " << segments.size() << ",\n";
	out << "  \"input_segment_count\": " << input_segment_count << ",\n";
	out << "  \"energy_only_segment_count\": " << energy_only_segment_count << ",\n";
	out << "  \"input_segment_active_energy_j\": " << input_segment_active_energy_j << ",\n";
	out << "  \"energy_only_segment_active_energy_j\": " << energy_only_segment_active_energy_j << ",\n";
	out << "  \"input_segment_base_energy_j\": " << input_segment_base_energy_j << ",\n";
	out << "  \"energy_only_segment_base_energy_j\": " << energy_only_segment_base_energy_j << ",\n";
	out << "  \"migration_observed\": " << (any_migration ? "true" : "false") << ",\n";
	out << "  \"base_energy_attributed_to_pipelines\": true\n";
	out << "}\n";
}

static void FinalizeAndWrite(QueryEnergyState &state) {
	auto end_ns = TimestampNs();
	if (!state.settings.export_enabled && !state.settings.profile_update_enabled) {
		return;
	}
	vector<RaplSnapshot> rapl_end;
	if (state.settings.rapl_enabled) {
		rapl_end = state.rapl.Read();
	}
	vector<EnergySegmentRecord> raw_segments;
	unordered_map<uint64_t, string> pipeline_signatures;
	vector<shared_ptr<EnergyThreadSegmentBuffer>> segment_buffers;
	{
		lock_guard<mutex> guard(state.lock);
		raw_segments = state.segments;
		segment_buffers = state.segment_buffers;
		pipeline_signatures = state.pipeline_signatures;
	}
	for (idx_t i = 0; i < segment_buffers.size(); i++) {
		const auto &buffer = segment_buffers[i];
		if (!buffer) {
			continue;
		}
		buffer->Drain(raw_segments);
	}
	FillPipelineSignatures(pipeline_signatures, raw_segments, state.settings.pipeline_signatures_enabled);
	std::sort(
	    raw_segments.begin(), raw_segments.end(),
	    [](const EnergySegmentRecord &lhs, const EnergySegmentRecord &rhs) { return lhs.segment_id < rhs.segment_id; });
	vector<AttributedSegment> segments;
	for (idx_t i = 0; i < raw_segments.size(); i++) {
		AttributedSegment attributed;
		attributed.record = raw_segments[i];
		segments.push_back(attributed);
	}
	vector<SocketEnergyWindow> windows;
	if (state.settings.rapl_enabled) {
		windows = BuildSocketWindows(state, segments, rapl_end, end_ns);
		AttributeSegments(segments, windows);
	}
	auto profiles = BuildProfiles(segments);
	auto efficiency_profiles = BuildEfficiencyProfiles(segments);

	if (!state.settings.export_enabled) {
		return;
	}
	auto output_segments = segments;
	std::sort(output_segments.begin(), output_segments.end(),
	          [](const AttributedSegment &lhs, const AttributedSegment &rhs) {
		          if (lhs.record.start_ns != rhs.record.start_ns) {
			          return lhs.record.start_ns < rhs.record.start_ns;
		          }
		          return lhs.record.segment_id < rhs.record.segment_id;
	          });
	EnsureDirectoryRecursive(state.output_dir);
	WriteSegmentsCSV(state.output_dir + "/energy_segments.csv", output_segments);
	WriteSocketWindowsCSV(state.output_dir + "/socket_energy_windows.csv", windows);
	WriteAttributedSegmentsCSV(state.output_dir + "/attributed_segments.csv", output_segments);
	WriteProfilesCSV(state.output_dir + "/pipeline_energy_profiles.csv", profiles);
	WriteEfficiencyProfilesCSV(state.output_dir + "/pipeline_efficiency_profiles.csv", efficiency_profiles);
	WriteValidationSummary(state.output_dir + "/validation_summary.json", windows, segments);
}

static uint64_t ScaleByFraction(uint64_t value, double fraction) {
	if (value == 0 || fraction <= 0) {
		return 0;
	}
	return static_cast<uint64_t>(std::llround(static_cast<double>(value) * fraction));
}

static EnergySegmentRecord SliceSegmentRecord(const EnergySegmentRecord &record, uint64_t slice_start_ns,
                                              uint64_t slice_end_ns) {
	auto result = record;
	result.start_ns = slice_start_ns;
	result.end_ns = slice_end_ns;
	result.duration_s = slice_end_ns > slice_start_ns
	                        ? static_cast<double>(slice_end_ns - slice_start_ns) / static_cast<double>(NSEC_PER_SEC)
	                        : 0;
	auto original_duration_ns = record.end_ns > record.start_ns ? record.end_ns - record.start_ns : 0;
	double fraction = original_duration_ns > 0 ? static_cast<double>(slice_end_ns - slice_start_ns) /
	                                                 static_cast<double>(original_duration_ns)
	                                           : 0;
	result.work_units = ScaleByFraction(record.work_units, fraction);
	result.tuples = ScaleByFraction(record.tuples, fraction);
	result.chunks = ScaleByFraction(record.chunks, fraction);
	result.cycles = ScaleByFraction(record.cycles, fraction);
	result.instructions = ScaleByFraction(record.instructions, fraction);
	result.ref_cycles = ScaleByFraction(record.ref_cycles, fraction);
	result.cache_refs = ScaleByFraction(record.cache_refs, fraction);
	result.cache_misses = ScaleByFraction(record.cache_misses, fraction);
	result.llc_misses = ScaleByFraction(record.llc_misses, fraction);
	result.offcore_responses = ScaleByFraction(record.offcore_responses, fraction);
	return result;
}

static void AddSegmentToEfficiencyAggregate(PipelineEfficiencyAggregate &profile, const AttributedSegment &segment,
                                            bool initialize_pipeline_identity) {
	auto duration = SegmentDuration(segment.record);
	if (!segment.record.counters_valid || !segment.record.hardware_state_stable || duration <= 0) {
		return;
	}
	if (profile.sample_count == 0) {
		profile.query_id = segment.record.query_id;
		if (initialize_pipeline_identity) {
			profile.pipeline_id = segment.record.pipeline_id;
			profile.pipeline_signature = segment.record.pipeline_signature;
		} else {
			profile.pipeline_signature = StringUtil::Format("query_%llu", segment.record.query_id);
		}
		profile.core_freq_hz = segment.record.core_freq_hz;
		profile.uncore_freq_hz = segment.record.uncore_freq_hz;
		profile.smt_occupancy = segment.record.smt_occupancy;
	}
	UpdateEfficiencyAggregateTimeRange(profile, segment.record.start_ns, segment.record.end_ns);
	profile.sample_count++;
	profile.total_duration_s += duration;
	profile.attributed_active_energy_j += segment.attributed_active_energy_j;
	profile.base_time_share_energy_j += segment.attributed_base_energy_j;
	profile.corrected_cycles += segment.corrected_cycles;
	if (segment.record.work_units > 0) {
		profile.valid_input_sample_count++;
		profile.total_input_rows += static_cast<double>(segment.record.work_units);
		profile.total_input_chunks += static_cast<double>(segment.record.chunks);
		profile.input_duration_s += duration;
		profile.input_attributed_active_energy_j += segment.attributed_active_energy_j;
		profile.input_base_time_share_energy_j += segment.attributed_base_energy_j;
		profile.input_corrected_cycles += segment.corrected_cycles;
	} else {
		profile.energy_only_sample_count++;
		profile.energy_only_duration_s += duration;
		profile.energy_only_active_energy_j += segment.attributed_active_energy_j;
		profile.energy_only_base_time_share_energy_j += segment.attributed_base_energy_j;
	}
}

static void AddThroughputObservationToEfficiencyAggregate(PipelineEfficiencyAggregate &profile,
                                                          const EnergySegmentRecord &record,
                                                          bool initialize_pipeline_identity) {
	auto duration = SegmentDuration(record);
	if (!record.counters_valid || !record.hardware_state_stable || duration <= 0 || record.work_units == 0) {
		return;
	}
	if (profile.sample_count == 0 && profile.valid_input_sample_count == 0) {
		profile.query_id = record.query_id;
		if (initialize_pipeline_identity) {
			profile.pipeline_id = record.pipeline_id;
			profile.pipeline_signature = record.pipeline_signature;
		} else {
			profile.pipeline_signature = StringUtil::Format("query_%llu", record.query_id);
		}
		profile.core_freq_hz = record.core_freq_hz;
		profile.uncore_freq_hz = record.uncore_freq_hz;
		profile.smt_occupancy = record.smt_occupancy;
	}
	UpdateEfficiencyAggregateTimeRange(profile, record.start_ns, record.end_ns);
	profile.valid_input_sample_count++;
	profile.total_input_rows += static_cast<double>(record.work_units);
	profile.total_input_chunks += static_cast<double>(record.chunks);
	profile.input_duration_s += duration;
	profile.input_corrected_cycles += SegmentCorrectedCycles(record);
}

static void MergeEfficiencyAggregate(PipelineEfficiencyAggregate &target, const PipelineEfficiencyAggregate &source) {
	if (source.sample_count == 0 && source.valid_input_sample_count == 0 && source.energy_only_sample_count == 0) {
		return;
	}
	if (target.sample_count == 0 && target.valid_input_sample_count == 0 && target.energy_only_sample_count == 0) {
		target.query_id = source.query_id;
		target.pipeline_id = source.pipeline_id;
		target.pipeline_signature = source.pipeline_signature;
		target.core_freq_hz = source.core_freq_hz;
		target.uncore_freq_hz = source.uncore_freq_hz;
		target.smt_occupancy = source.smt_occupancy;
	}
	UpdateEfficiencyAggregateTimeRange(target, source.start_ns, source.end_ns);
	target.sample_count += source.sample_count;
	target.valid_input_sample_count += source.valid_input_sample_count;
	target.energy_only_sample_count += source.energy_only_sample_count;
	target.total_input_rows += source.total_input_rows;
	target.total_input_chunks += source.total_input_chunks;
	target.total_duration_s += source.total_duration_s;
	target.input_duration_s += source.input_duration_s;
	target.attributed_active_energy_j += source.attributed_active_energy_j;
	target.input_attributed_active_energy_j += source.input_attributed_active_energy_j;
	target.base_time_share_energy_j += source.base_time_share_energy_j;
	target.input_base_time_share_energy_j += source.input_base_time_share_energy_j;
	target.corrected_cycles += source.corrected_cycles;
	target.input_corrected_cycles += source.input_corrected_cycles;
	target.energy_only_duration_s += source.energy_only_duration_s;
	target.energy_only_active_energy_j += source.energy_only_active_energy_j;
	target.energy_only_base_time_share_energy_j += source.energy_only_base_time_share_energy_j;
}

struct PeriodicPendingWindow {
	SocketEnergyWindow window;
	vector<AttributedSegment> segments;
};

struct SchedulerWorkerRegistration {
	DatabaseInstance *db = nullptr;
	uint64_t worker_id = 0;
	int64_t linux_tid = -1;
	int logical_cpu = -1;
	uint64_t registered_ns = 0;
	bool active = false;
};

struct PeriodicRuntimeOverheadRecord {
	uint64_t sample_id = 0;
	string event;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	uint64_t wall_ns = 0;
	uint64_t cpu_ns = 0;
	uint64_t lock_wait_ns = 0;
	uint64_t lock_hold_ns = 0;
	uint64_t rapl_read_ns = 0;
	uint64_t create_windows_ns = 0;
	uint64_t drain_segments_ns = 0;
	uint64_t snapshot_active_workers_ns = 0;
	uint64_t queue_drain_ns = 0;
	uint64_t closed_segment_profile_ns = 0;
	uint64_t closed_segment_local_aggregate_ns = 0;
	uint64_t closed_segment_store_ns = 0;
	uint64_t closed_segment_merge_ns = 0;
	uint64_t worker_lock_wait_ns = 0;
	uint64_t snapshot_perf_read_ns = 0;
	uint64_t snapshot_record_build_ns = 0;
	uint64_t snapshot_lock_hold_ns = 0;
	uint64_t pending_append_ns = 0;
	uint64_t queue_lock_wait_ns = 0;
	uint64_t queue_swap_ns = 0;
	uint64_t slice_segments_ns = 0;
	uint64_t finalize_windows_ns = 0;
	uint64_t attribute_segments_ns = 0;
	uint64_t profile_update_ns = 0;
	uint64_t csv_append_ns = 0;
	idx_t windows_created = 0;
	idx_t windows_finalized = 0;
	idx_t segments_drained = 0;
	idx_t worker_snapshot_count = 0;
	idx_t worker_snapshot_skipped_inactive = 0;
	idx_t queued_segment_count = 0;
	idx_t closed_segment_count = 0;
	idx_t segments_sliced = 0;
	idx_t segments_attributed = 0;
	idx_t active_queries = 0;
	idx_t registered_workers = 0;
	idx_t tracked_queries = 0;
	idx_t pending_windows = 0;
	bool observed_query_activity = false;
};

struct PeriodicRuntimeOverheadStats {
	idx_t count = 0;
	uint64_t total_wall_ns = 0;
	uint64_t max_wall_ns = 0;
	uint64_t total_cpu_ns = 0;
	uint64_t max_cpu_ns = 0;
	uint64_t total_lock_wait_ns = 0;
	uint64_t max_lock_wait_ns = 0;
	uint64_t total_lock_hold_ns = 0;
	uint64_t max_lock_hold_ns = 0;
	uint64_t total_attribute_segments_ns = 0;
	uint64_t max_attribute_segments_ns = 0;
	uint64_t total_profile_update_ns = 0;
	uint64_t total_csv_append_ns = 0;
	idx_t windows_finalized = 0;
	idx_t segments_attributed = 0;
};

struct PeriodicWorkerSegmentState {
	int64_t linux_tid = -1;
	mutex lock;
	PerfEventGroup perf_group;
	string perf_profile;
	bool perf_open = false;
	string perf_status = "not opened";
	bool active = false;
	bool in_active_worker_list = false;
	std::atomic<bool> active_atomic {false};
	shared_ptr<QueryEnergyState> query;
	EnergySegmentRecord record;
	EnergyPerfSnapshot last_perf;
	uint64_t last_boundary_ns = 0;
	PerfCounters accumulated_counters;
};

struct PeriodicWorkerStateCache {
	uint64_t generation = 0;
	int64_t linux_tid = -1;
	shared_ptr<PeriodicWorkerSegmentState> state;
};

static thread_local PeriodicWorkerStateCache g_periodic_worker_state_cache;

class PeriodicEnergyRuntime {
public:
	static PeriodicEnergyRuntime &Get() {
		static PeriodicEnergyRuntime runtime;
		return runtime;
	}

	void Configure(DatabaseInstance &db, const EnergyAttributionSettings &settings_p) {
		std::thread thread_to_join;
		bool should_start = settings_p.enabled && settings_p.periodic_enabled;
		{
			std::unique_lock<mutex> guard(lock);
			if (running && active_db == &db) {
				if (SettingsMatch(settings, settings_p)) {
					return;
				}
				runtime_active.store(false, std::memory_order_release);
				stop_requested = true;
				cv.notify_all();
				if (worker.joinable()) {
					thread_to_join = std::move(worker);
				}
			} else if (running && active_db != &db) {
				return;
			} else if (!should_start) {
				return;
			}
		}
		if (thread_to_join.joinable()) {
			thread_to_join.join();
		}
		if (!should_start) {
			return;
		}
		{
			std::unique_lock<mutex> guard(lock);
			if (!running) {
				StartLocked(db, settings_p);
			}
		}
	}

	void Shutdown(DatabaseInstance &db) {
		std::thread thread_to_join;
		{
			std::unique_lock<mutex> guard(lock);
			if (running && active_db == &db) {
				runtime_active.store(false, std::memory_order_release);
				stop_requested = true;
				cv.notify_all();
				if (worker.joinable()) {
					thread_to_join = std::move(worker);
				}
			}
			for (auto entry = worker_registry.begin(); entry != worker_registry.end();) {
				if (entry->second.db == &db) {
					entry = worker_registry.erase(entry);
				} else {
					++entry;
				}
			}
		}
		if (thread_to_join.joinable()) {
			thread_to_join.join();
		}
	}

	void RegisterWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid, int logical_cpu) {
		std::unique_lock<mutex> guard(lock);
		SchedulerWorkerRegistration registration;
		registration.db = &db;
		registration.worker_id = worker_id;
		registration.linux_tid = linux_tid;
		registration.logical_cpu = logical_cpu;
		registration.registered_ns = TimestampNs();
		registration.active = true;
		worker_registry[std::make_pair(&db, worker_id)] = registration;
		if (running && active_db == &db) {
			auto worker_state = GetWorkerSegmentStateLocked(linux_tid);
			g_periodic_worker_state_cache.generation = runtime_generation.load(std::memory_order_acquire);
			g_periodic_worker_state_cache.linux_tid = linux_tid;
			g_periodic_worker_state_cache.state = worker_state;
		}
	}

	void UnregisterWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid) {
		std::unique_lock<mutex> guard(lock);
		auto entry = worker_registry.find(std::make_pair(&db, worker_id));
		if (entry == worker_registry.end()) {
			return;
		}
		if (entry->second.linux_tid == linux_tid) {
			entry->second.active = false;
		}
	}

	void RegisterQuery(const shared_ptr<QueryEnergyState> &query) {
		std::unique_lock<mutex> guard(lock);
		if (!running) {
			return;
		}
		query->periodic_active.store(true, std::memory_order_release);
		queries[query.get()] = query;
		query_active[query.get()] = true;
		observed_query_activity.store(true, std::memory_order_release);
		active_query_count.fetch_add(1, std::memory_order_relaxed);
		cv.notify_all();
	}

	void MarkQueryEnded(const shared_ptr<QueryEnergyState> &query) {
		auto wall_start_ns = TimestampNs();
		auto cpu_start_ns = ThreadCpuNs();
		query->periodic_active.store(false, std::memory_order_release);
		PeriodicRuntimeOverheadRecord overhead;
		InitializeRuntimeOverhead(overhead, "query_end_mark", wall_start_ns);
		{
			auto lock_wait_start_ns = TimestampNs();
			std::unique_lock<mutex> guard(lock);
			auto lock_acquired_ns = TimestampNs();
			overhead.lock_wait_ns += DurationNs(lock_acquired_ns, lock_wait_start_ns);
			if (!running) {
				return;
			}
			auto active_entry = query_active.find(query.get());
			if (active_entry != query_active.end() && active_entry->second) {
				active_entry->second = false;
				DecrementActiveQueryCount();
			}
			auto &query_profile = query_profiles[query->query_id];
			query_profile.query_id = query->query_id;
			if (query_profile.pipeline_signature.empty()) {
				query_profile.pipeline_signature = StringUtil::Format("query_%llu", query->query_id);
			}
			UpdateEfficiencyAggregateTimeRange(query_profile, query->start_ns, wall_start_ns);
			queries.erase(query.get());
			query_active.erase(query.get());
			overhead.lock_hold_ns += DurationNs(TimestampNs(), lock_acquired_ns);
		}
		FinishRuntimeOverhead(overhead, cpu_start_ns);
		cv.notify_all();
	}

	bool BeginSegment(const shared_ptr<QueryEnergyState> &query, EnergySegmentRecord record) {
		if (!query || !runtime_active.load(std::memory_order_acquire) ||
		    !query->periodic_active.load(std::memory_order_acquire)) {
			return false;
		}
		auto worker_state = GetCachedWorkerSegmentState(record.linux_tid);
		if (!worker_state) {
			return false;
		}
		vector<EnergySegmentRecord> slices;
		bool became_active = false;
		{
			lock_guard<mutex> worker_guard(worker_state->lock);
			if (worker_state->active) {
				SnapshotWorkerSegmentLocked(*worker_state, record.start_ns, nullptr, slices);
			} else {
				became_active = true;
			}
			worker_state->query = query;
			worker_state->record = std::move(record);
			worker_state->last_boundary_ns = worker_state->record.start_ns;
			worker_state->accumulated_counters = PerfCounters();
			worker_state->last_perf = ReadWorkerPerfSnapshotLocked(*worker_state, query->settings);
			worker_state->active = true;
			worker_state->active_atomic.store(true, std::memory_order_release);
		}
		if (became_active) {
			AddActiveWorkerState(worker_state);
		}
		EnqueueSegmentBatch(std::move(slices), EnergySegmentRecord(), false);
		observed_query_activity.store(true, std::memory_order_release);
		cv.notify_all();
		return true;
	}

	void EndSegment(const EnergySegmentRecord &end_record) {
		auto worker_state = GetCachedWorkerSegmentState(end_record.linux_tid);
		if (!worker_state) {
			return;
		}
		vector<EnergySegmentRecord> slices;
		EnergySegmentRecord closed_record;
		bool have_closed_record = false;
		{
			lock_guard<mutex> worker_guard(worker_state->lock);
			if (!worker_state->active || worker_state->record.segment_id != end_record.segment_id) {
				return;
			}
			SnapshotWorkerSegmentLocked(*worker_state, end_record.end_ns, &end_record, slices);
			closed_record = worker_state->record;
			closed_record.end_ns = end_record.end_ns;
			closed_record.duration_s = end_record.duration_s;
			closed_record.end_logical_cpu_id = end_record.end_logical_cpu_id;
			closed_record.end_socket_id = end_record.end_socket_id;
			closed_record.end_physical_core_id = end_record.end_physical_core_id;
			closed_record.work_units = end_record.work_units;
			closed_record.tuples = end_record.tuples;
			closed_record.chunks = end_record.chunks;
			closed_record.migrated = end_record.migrated;
			closed_record.hardware_state_stable = end_record.hardware_state_stable;
			ApplyPerfCounters(closed_record, worker_state->accumulated_counters);
			have_closed_record = true;
			worker_state->active = false;
			worker_state->active_atomic.store(false, std::memory_order_release);
			worker_state->query.reset();
			worker_state->last_boundary_ns = 0;
			worker_state->last_perf = EnergyPerfSnapshot();
		}
		EnqueueSegmentBatch(std::move(slices), std::move(closed_record), have_closed_record);
	}

private:
	static bool SettingsMatch(const EnergyAttributionSettings &lhs, const EnergyAttributionSettings &rhs) {
		return lhs.enabled == rhs.enabled && lhs.rapl_enabled == rhs.rapl_enabled &&
		       lhs.perf_counters_enabled == rhs.perf_counters_enabled && lhs.export_enabled == rhs.export_enabled &&
		       lhs.debug_export_enabled == rhs.debug_export_enabled &&
		       lhs.closed_segments_export_enabled == rhs.closed_segments_export_enabled &&
		       lhs.overhead_detail_enabled == rhs.overhead_detail_enabled &&
		       lhs.pipeline_signatures_enabled == rhs.pipeline_signatures_enabled &&
		       lhs.metadata_cache_enabled == rhs.metadata_cache_enabled &&
		       lhs.migration_check_enabled == rhs.migration_check_enabled &&
		       lhs.profile_update_enabled == rhs.profile_update_enabled &&
		       lhs.periodic_enabled == rhs.periodic_enabled && lhs.period_ms == rhs.period_ms &&
		       lhs.output_dir == rhs.output_dir && lhs.base_power_path == rhs.base_power_path &&
		       lhs.counter_profile == rhs.counter_profile && lhs.fail_policy == rhs.fail_policy;
	}

	void StartLocked(DatabaseInstance &db, const EnergyAttributionSettings &settings_p) {
		active_db = &db;
		settings = settings_p;
		period_ms = MaxValue<uint64_t>(1, settings.period_ms);
		run_start_ns = TimestampNs();
		next_window_id = 1;
		active_query_count.store(0, std::memory_order_relaxed);
		stop_requested = false;
		running = true;
		finalized_window_count = 0;
		attributed_segment_count = 0;
		next_runtime_overhead_id.store(1, std::memory_order_relaxed);
		total_active_conservation_error_j = 0;
		total_base_conservation_error_j = 0;
		total_package_gap_j = 0;
		output_opened = false;
		observed_query_activity.store(false, std::memory_order_relaxed);
		queries.clear();
		query_active.clear();
		pending_windows.clear();
		pending_window_count.store(0, std::memory_order_relaxed);
		pending_segment_records.clear();
		closed_segment_records.clear();
		{
			lock_guard<mutex> queue_guard(queue_lock);
			queued_segment_records.clear();
			queued_closed_segment_records.clear();
		}
		scratch_worker_states.clear();
		scratch_sampled_segments.clear();
		scratch_closed_segments.clear();
		scratch_queue_segments.clear();
		scratch_queue_closed_segments.clear();
		scratch_rapl_snapshots.clear();
		scratch_pipeline_profile_updates.clear();
		scratch_query_profile_updates.clear();
		for (auto &entry : scratch_pending_windows_by_socket) {
			entry.second.clear();
		}
		scratch_remaining_segment_records.clear();
		{
			lock_guard<mutex> active_guard(active_worker_lock);
			for (idx_t i = 0; i < active_worker_states.size(); i++) {
				if (active_worker_states[i]) {
					active_worker_states[i]->in_active_worker_list = false;
				}
			}
			active_worker_states.clear();
		}
		worker_segment_states.clear();
		pipeline_profiles.clear();
		query_profiles.clear();
		runtime_overhead.clear();
		base_power_entries.clear();
		base_power_loaded = false;
		if (settings.rapl_enabled) {
			base_power_loaded = LoadBasePowerCSVCached(settings.base_power_path, base_power_entries);
		}
		uncore_freq_by_socket = BuildQueryUncoreFrequencyCache(true);
		auto base_dir = settings.output_dir.empty() ? string(DEFAULT_OUTPUT_DIR) : settings.output_dir;
		output_dir = base_dir + "/" + StringUtil::Format("periodic_run_%llu", run_start_ns);
		runtime_generation.fetch_add(1, std::memory_order_acq_rel);
		runtime_active.store(true, std::memory_order_release);
		worker = std::thread([this]() { Run(); });
	}

	void Run() {
		last_sample_ns = TimestampNs();
		if (settings.rapl_enabled) {
			rapl.Read(last_rapl);
		}
		while (true) {
			bool should_stop = false;
			{
				std::unique_lock<mutex> guard(lock);
				cv.wait_for(guard, std::chrono::milliseconds(period_ms), [&]() { return stop_requested; });
				should_stop = stop_requested;
			}
			SampleOnce();
			if (should_stop) {
				ShutdownFinalize();
				WriteFinalOutputs();
				std::unique_lock<mutex> guard(lock);
				CloseOutputFilesLocked();
				queries.clear();
				query_active.clear();
				pending_windows.clear();
				pending_window_count.store(0, std::memory_order_relaxed);
				pending_segment_records.clear();
				closed_segment_records.clear();
				{
					lock_guard<mutex> queue_guard(queue_lock);
					queued_segment_records.clear();
					queued_closed_segment_records.clear();
				}
				scratch_worker_states.clear();
				scratch_sampled_segments.clear();
				scratch_closed_segments.clear();
				scratch_queue_segments.clear();
				scratch_queue_closed_segments.clear();
				scratch_rapl_snapshots.clear();
				scratch_pipeline_profile_updates.clear();
				scratch_query_profile_updates.clear();
				for (auto &entry : scratch_pending_windows_by_socket) {
					entry.second.clear();
				}
				scratch_remaining_segment_records.clear();
				{
					lock_guard<mutex> active_guard(active_worker_lock);
					for (idx_t i = 0; i < active_worker_states.size(); i++) {
						if (active_worker_states[i]) {
							active_worker_states[i]->in_active_worker_list = false;
						}
					}
					active_worker_states.clear();
				}
				worker_segment_states.clear();
				active_db = nullptr;
				running = false;
				runtime_active.store(false, std::memory_order_release);
				stop_requested = false;
				cv.notify_all();
				break;
			}
		}
	}

	void InitializeRuntimeOverhead(PeriodicRuntimeOverheadRecord &record, const string &event, uint64_t start_ns) {
		record.sample_id = next_runtime_overhead_id.fetch_add(1, std::memory_order_relaxed);
		record.event = event;
		record.start_ns = start_ns;
	}

	void FinishRuntimeOverhead(PeriodicRuntimeOverheadRecord &record, uint64_t cpu_start_ns) {
		auto cpu_end_ns = ThreadCpuNs();
		if (cpu_start_ns != 0 && cpu_end_ns != 0) {
			record.cpu_ns = DurationNs(cpu_end_ns, cpu_start_ns);
		}
		auto lock_wait_start_ns = TimestampNs();
		std::unique_lock<mutex> guard(lock);
		auto lock_acquired_ns = TimestampNs();
		record.lock_wait_ns += DurationNs(lock_acquired_ns, lock_wait_start_ns);
		record.active_queries = active_query_count.load(std::memory_order_relaxed);
		record.registered_workers = worker_registry.size();
		record.tracked_queries = queries.size();
		record.pending_windows = pending_window_count.load(std::memory_order_relaxed);
		record.observed_query_activity = observed_query_activity.load(std::memory_order_acquire);
		record.end_ns = TimestampNs();
		record.wall_ns = DurationNs(record.end_ns, record.start_ns);
		record.lock_hold_ns += DurationNs(record.end_ns, lock_acquired_ns);
		runtime_overhead.push_back(record);
	}

	void DecrementActiveQueryCount() {
		auto count = active_query_count.load(std::memory_order_relaxed);
		while (count > 0 && !active_query_count.compare_exchange_weak(count, count - 1, std::memory_order_relaxed,
		                                                              std::memory_order_relaxed)) {
		}
	}

	shared_ptr<PeriodicWorkerSegmentState> GetWorkerSegmentStateLocked(int64_t linux_tid) {
		auto entry = worker_segment_states.find(linux_tid);
		if (entry != worker_segment_states.end()) {
			return entry->second;
		}
		auto state = make_shared_ptr<PeriodicWorkerSegmentState>();
		state->linux_tid = linux_tid;
		worker_segment_states[linux_tid] = state;
		return state;
	}

	shared_ptr<PeriodicWorkerSegmentState> GetCachedWorkerSegmentState(int64_t linux_tid) {
		auto generation = runtime_generation.load(std::memory_order_acquire);
		if (generation == 0 || !runtime_active.load(std::memory_order_acquire)) {
			return nullptr;
		}
		if (g_periodic_worker_state_cache.generation == generation &&
		    g_periodic_worker_state_cache.linux_tid == linux_tid && g_periodic_worker_state_cache.state) {
			return g_periodic_worker_state_cache.state;
		}
		std::unique_lock<mutex> guard(lock);
		if (!running || runtime_generation.load(std::memory_order_acquire) != generation) {
			return nullptr;
		}
		auto state = GetWorkerSegmentStateLocked(linux_tid);
		g_periodic_worker_state_cache.generation = generation;
		g_periodic_worker_state_cache.linux_tid = linux_tid;
		g_periodic_worker_state_cache.state = state;
		return state;
	}

	void AddActiveWorkerState(const shared_ptr<PeriodicWorkerSegmentState> &state) {
		if (!state) {
			return;
		}
		lock_guard<mutex> guard(active_worker_lock);
		if (state->in_active_worker_list) {
			return;
		}
		state->in_active_worker_list = true;
		active_worker_states.push_back(state);
	}

	void CopyActiveWorkerStates(vector<shared_ptr<PeriodicWorkerSegmentState>> &states) {
		lock_guard<mutex> guard(active_worker_lock);
		states.reserve(active_worker_states.size());
		idx_t write = 0;
		for (idx_t i = 0; i < active_worker_states.size(); i++) {
			auto &state = active_worker_states[i];
			if (state && state->active_atomic.load(std::memory_order_acquire)) {
				states.push_back(state);
				active_worker_states[write++] = state;
				continue;
			}
			if (state) {
				state->in_active_worker_list = false;
			}
		}
		active_worker_states.resize(write);
	}

	void EnqueueSegmentBatch(vector<EnergySegmentRecord> records, EnergySegmentRecord closed_record,
	                         bool have_closed_record) {
		if (records.empty() && !have_closed_record) {
			return;
		}
		lock_guard<mutex> guard(queue_lock);
		if (!records.empty()) {
			queued_segment_records.insert(queued_segment_records.end(), std::make_move_iterator(records.begin()),
			                              std::make_move_iterator(records.end()));
		}
		if (have_closed_record) {
			queued_closed_segment_records.push_back(std::move(closed_record));
		}
	}

	PeriodicRuntimeOverheadRecord *DetailOverhead(PeriodicRuntimeOverheadRecord *overhead) const {
		return settings.overhead_detail_enabled ? overhead : nullptr;
	}

	static uint64_t DetailTimestamp(PeriodicRuntimeOverheadRecord *overhead) {
		return overhead ? TimestampNs() : 0;
	}

	static void DetailAddSince(PeriodicRuntimeOverheadRecord *overhead, uint64_t PeriodicRuntimeOverheadRecord::*field,
	                           uint64_t start_ns) {
		if (!overhead || start_ns == 0) {
			return;
		}
		overhead->*field += DurationNs(TimestampNs(), start_ns);
	}

	void DrainQueuedRecords(vector<EnergySegmentRecord> &segments, vector<EnergySegmentRecord> &closed_segments,
	                        vector<EnergySegmentRecord> &queue_segments,
	                        vector<EnergySegmentRecord> &queue_closed_segments,
	                        PeriodicRuntimeOverheadRecord *overhead = nullptr,
	                        PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		queue_segments.clear();
		queue_closed_segments.clear();
		{
			auto lock_wait_start_ns = DetailTimestamp(detail_overhead);
			lock_guard<mutex> guard(queue_lock);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::queue_lock_wait_ns, lock_wait_start_ns);
			auto swap_start_ns = DetailTimestamp(detail_overhead);
			queue_segments.swap(queued_segment_records);
			queue_closed_segments.swap(queued_closed_segment_records);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::queue_swap_ns, swap_start_ns);
		}
		if (overhead) {
			overhead->queued_segment_count += queue_segments.size();
			overhead->closed_segment_count += queue_closed_segments.size();
		}
		segments.insert(segments.end(), std::make_move_iterator(queue_segments.begin()),
		                std::make_move_iterator(queue_segments.end()));
		closed_segments.insert(closed_segments.end(), std::make_move_iterator(queue_closed_segments.begin()),
		                       std::make_move_iterator(queue_closed_segments.end()));
	}

	EnergyPerfSnapshot ReadWorkerPerfSnapshotLocked(PeriodicWorkerSegmentState &worker_state,
	                                                const EnergyAttributionSettings &segment_settings) {
		EnergyPerfSnapshot snapshot;
		if (!segment_settings.perf_counters_enabled) {
			snapshot.status = "perf disabled";
			return snapshot;
		}
		if (worker_state.perf_open && worker_state.perf_profile == segment_settings.counter_profile) {
			return worker_state.perf_group.ReadSnapshot();
		}
		if (worker_state.perf_open) {
			worker_state.perf_group.Close();
			worker_state.perf_open = false;
		}
		worker_state.perf_profile = segment_settings.counter_profile;
		worker_state.perf_open = worker_state.perf_group.Open(worker_state.linux_tid, segment_settings.counter_profile);
		worker_state.perf_status =
		    worker_state.perf_open ? segment_settings.counter_profile : worker_state.perf_group.ReadSnapshot().status;
		if (!worker_state.perf_open) {
			snapshot.status = worker_state.perf_status;
			return snapshot;
		}
		return worker_state.perf_group.ReadSnapshot();
	}

	static EnergySegmentRecord BuildPeriodicSliceRecord(const EnergySegmentRecord &base, uint64_t slice_start_ns,
	                                                    uint64_t slice_end_ns, const PerfCounters &counters,
	                                                    const EnergySegmentRecord *end_record) {
		EnergySegmentRecord slice;
		slice.segment_id = base.segment_id;
		slice.plan_version = base.plan_version;
		slice.worker_id = base.worker_id;
		slice.linux_tid = base.linux_tid;
		slice.socket_id = base.socket_id;
		slice.physical_core_id = base.physical_core_id;
		slice.logical_cpu_id = base.logical_cpu_id;
		slice.query_id = base.query_id;
		slice.pipeline_id = base.pipeline_id;
		slice.pipeline_signature = base.pipeline_signature;
		slice.role = base.role;
		slice.start_ns = slice_start_ns;
		slice.end_ns = slice_end_ns;
		slice.duration_s = static_cast<double>(slice_end_ns - slice_start_ns) / static_cast<double>(NSEC_PER_SEC);
		slice.work_units = 0;
		slice.tuples = 0;
		slice.chunks = 0;
		slice.core_freq_hz = base.core_freq_hz;
		slice.uncore_freq_hz = base.uncore_freq_hz;
		slice.smt_occupancy = base.smt_occupancy;
		if (end_record) {
			slice.end_logical_cpu_id = end_record->end_logical_cpu_id;
			slice.end_socket_id = end_record->end_socket_id;
			slice.end_physical_core_id = end_record->end_physical_core_id;
			slice.migrated = end_record->migrated;
			slice.hardware_state_stable = end_record->hardware_state_stable;
		} else {
			slice.end_logical_cpu_id = slice.logical_cpu_id;
			slice.end_socket_id = slice.socket_id;
			slice.end_physical_core_id = slice.physical_core_id;
			slice.migrated = false;
			slice.hardware_state_stable = slice.socket_id >= 0 && slice.physical_core_id >= 0;
		}
		ApplyPerfCounters(slice, counters);
		return slice;
	}

	void SnapshotWorkerSegmentLocked(PeriodicWorkerSegmentState &worker_state, uint64_t end_ns,
	                                 const EnergySegmentRecord *end_record, vector<EnergySegmentRecord> &target,
	                                 PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		if (!worker_state.active || end_ns <= worker_state.last_boundary_ns) {
			return;
		}
		auto query = worker_state.query;
		PerfCounters counters;
		if (query && query->settings.perf_counters_enabled && worker_state.last_perf.valid) {
			auto perf_start_ns = DetailTimestamp(detail_overhead);
			auto end_perf = ReadWorkerPerfSnapshotLocked(worker_state, query->settings);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::snapshot_perf_read_ns, perf_start_ns);
			counters = DeltaPerfCounters(worker_state.last_perf, end_perf);
			worker_state.last_perf = end_perf;
		} else if (query && query->settings.perf_counters_enabled) {
			counters.status =
			    worker_state.last_perf.status.empty() ? worker_state.perf_status : worker_state.last_perf.status;
		} else {
			counters.status = "perf disabled";
		}

		auto record_build_start_ns = DetailTimestamp(detail_overhead);
		auto slice =
		    BuildPeriodicSliceRecord(worker_state.record, worker_state.last_boundary_ns, end_ns, counters, end_record);
		DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::snapshot_record_build_ns,
		               record_build_start_ns);
		target.push_back(std::move(slice));
		AddPerfCounters(worker_state.accumulated_counters, counters);
		worker_state.last_boundary_ns = end_ns;
	}

	void SnapshotActiveWorkerSegments(uint64_t now_ns,
	                                  const vector<shared_ptr<PeriodicWorkerSegmentState>> &worker_states,
	                                  vector<EnergySegmentRecord> &target,
	                                  PeriodicRuntimeOverheadRecord *overhead = nullptr,
	                                  PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		auto before = target.size();
		for (idx_t i = 0; i < worker_states.size(); i++) {
			if (!worker_states[i]) {
				continue;
			}
			auto &worker_state = *worker_states[i];
			if (!worker_state.active_atomic.load(std::memory_order_acquire)) {
				if (detail_overhead) {
					detail_overhead->worker_snapshot_skipped_inactive++;
				}
				continue;
			}
			auto lock_wait_start_ns = DetailTimestamp(detail_overhead);
			lock_guard<mutex> worker_guard(worker_state.lock);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::worker_lock_wait_ns, lock_wait_start_ns);
			auto lock_hold_start_ns = DetailTimestamp(detail_overhead);
			if (detail_overhead) {
				detail_overhead->worker_snapshot_count++;
			}
			SnapshotWorkerSegmentLocked(worker_state, now_ns, nullptr, target, detail_overhead);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::snapshot_lock_hold_ns, lock_hold_start_ns);
		}
		if (overhead) {
			overhead->segments_drained += target.size() - before;
		}
	}

	void AddClosedSegmentObservations(vector<EnergySegmentRecord> &records,
	                                  PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		if (records.empty()) {
			return;
		}
		auto phase_start_ns = DetailTimestamp(detail_overhead);
		scratch_pipeline_profile_updates.clear();
		scratch_query_profile_updates.clear();
		scratch_pipeline_profile_updates.reserve(records.size());
		scratch_query_profile_updates.reserve(records.size());
		for (idx_t i = 0; i < records.size(); i++) {
			const auto &record = records[i];
			auto pipeline_key = BuildPeriodicPipelineProfileKey(record);
			AddThroughputObservationToEfficiencyAggregate(scratch_pipeline_profile_updates[pipeline_key], record, true);
			AddThroughputObservationToEfficiencyAggregate(scratch_query_profile_updates[record.query_id], record,
			                                              false);
		}
		DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::closed_segment_local_aggregate_ns,
		               phase_start_ns);
		phase_start_ns = DetailTimestamp(detail_overhead);
		if (settings.closed_segments_export_enabled) {
			closed_segment_records.reserve(closed_segment_records.size() + records.size());
			closed_segment_records.insert(closed_segment_records.end(), std::make_move_iterator(records.begin()),
			                              std::make_move_iterator(records.end()));
		}
		DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::closed_segment_store_ns, phase_start_ns);
		phase_start_ns = DetailTimestamp(detail_overhead);
		for (auto entry = scratch_pipeline_profile_updates.begin(); entry != scratch_pipeline_profile_updates.end();
		     ++entry) {
			MergeEfficiencyAggregate(pipeline_profiles[entry->first], entry->second);
		}
		for (auto entry = scratch_query_profile_updates.begin(); entry != scratch_query_profile_updates.end();
		     ++entry) {
			MergeEfficiencyAggregate(query_profiles[entry->first], entry->second);
		}
		DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::closed_segment_merge_ns, phase_start_ns);
	}

	void AddPendingSegmentRecordsToWindowsLocked(PeriodicRuntimeOverheadRecord *overhead = nullptr,
	                                             PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		if (pending_segment_records.empty()) {
			return;
		}
		AddSegmentRecordsToWindowsLocked(pending_segment_records, true, overhead, detail_overhead);
	}

	void AddSegmentRecordsToWindowsLocked(vector<EnergySegmentRecord> &records, bool rebuild_pending,
	                                      PeriodicRuntimeOverheadRecord *overhead = nullptr,
	                                      PeriodicRuntimeOverheadRecord *detail_overhead = nullptr) {
		if (records.empty()) {
			return;
		}
		if (rebuild_pending) {
			scratch_remaining_segment_records.clear();
			scratch_remaining_segment_records.reserve(records.size());
		}
		for (idx_t i = 0; i < records.size(); i++) {
			auto &record = records[i];
			auto added = AddSegmentToPendingWindowsLocked(record);
			if (overhead) {
				overhead->segments_sliced += added;
			}
			if (added == 0 && record.end_ns > last_sample_ns) {
				if (rebuild_pending) {
					scratch_remaining_segment_records.push_back(std::move(record));
				} else {
					auto append_start_ns = DetailTimestamp(detail_overhead);
					pending_segment_records.push_back(std::move(record));
					DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::pending_append_ns, append_start_ns);
				}
			}
		}
		if (rebuild_pending) {
			pending_segment_records.clear();
			pending_segment_records.swap(scratch_remaining_segment_records);
		}
	}

	void SampleOnce() {
		auto wall_start_ns = TimestampNs();
		auto cpu_start_ns = ThreadCpuNs();
		PeriodicRuntimeOverheadRecord overhead;
		auto detail_overhead = DetailOverhead(&overhead);
		InitializeRuntimeOverhead(overhead, "periodic_sample", wall_start_ns);
		auto now_ns = TimestampNs();
		if (settings.rapl_enabled) {
			auto phase_start_ns = TimestampNs();
			rapl.Read(scratch_rapl_snapshots);
			overhead.rapl_read_ns += DurationNs(TimestampNs(), phase_start_ns);
			auto windows_before = pending_windows.size();
			phase_start_ns = TimestampNs();
			CreateWindowsLocked(scratch_rapl_snapshots, now_ns);
			overhead.create_windows_ns += DurationNs(TimestampNs(), phase_start_ns);
			overhead.windows_created += pending_windows.size() - windows_before;
			last_rapl = scratch_rapl_snapshots;
		}
		last_sample_ns = now_ns;
		scratch_worker_states.clear();
		CopyActiveWorkerStates(scratch_worker_states);
		scratch_sampled_segments.clear();
		scratch_closed_segments.clear();
		auto phase_start_ns = TimestampNs();
		auto subphase_start_ns = TimestampNs();
		SnapshotActiveWorkerSegments(now_ns, scratch_worker_states, scratch_sampled_segments, &overhead,
		                             detail_overhead);
		overhead.snapshot_active_workers_ns += DurationNs(TimestampNs(), subphase_start_ns);
		subphase_start_ns = TimestampNs();
		DrainQueuedRecords(scratch_sampled_segments, scratch_closed_segments, scratch_queue_segments,
		                   scratch_queue_closed_segments, &overhead, detail_overhead);
		overhead.queue_drain_ns += DurationNs(TimestampNs(), subphase_start_ns);
		overhead.segments_drained += scratch_closed_segments.size();
		subphase_start_ns = TimestampNs();
		AddClosedSegmentObservations(scratch_closed_segments, detail_overhead);
		overhead.closed_segment_profile_ns += DurationNs(TimestampNs(), subphase_start_ns);
		overhead.drain_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		phase_start_ns = TimestampNs();
		BuildPendingWindowSocketIndexLocked();
		AddSegmentRecordsToWindowsLocked(scratch_sampled_segments, false, &overhead, detail_overhead);
		AddPendingSegmentRecordsToWindowsLocked(&overhead, detail_overhead);
		overhead.slice_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		phase_start_ns = TimestampNs();
		FinalizeReadyWindows(false, &overhead);
		overhead.finalize_windows_ns += DurationNs(TimestampNs(), phase_start_ns);
		FinishRuntimeOverhead(overhead, cpu_start_ns);
	}

	void ShutdownFinalize() {
		auto wall_start_ns = TimestampNs();
		auto cpu_start_ns = ThreadCpuNs();
		PeriodicRuntimeOverheadRecord overhead;
		auto detail_overhead = DetailOverhead(&overhead);
		InitializeRuntimeOverhead(overhead, "shutdown_finalize", wall_start_ns);
		auto now_ns = TimestampNs();
		scratch_worker_states.clear();
		CopyActiveWorkerStates(scratch_worker_states);
		scratch_sampled_segments.clear();
		scratch_closed_segments.clear();
		auto phase_start_ns = TimestampNs();
		auto subphase_start_ns = TimestampNs();
		SnapshotActiveWorkerSegments(now_ns, scratch_worker_states, scratch_sampled_segments, &overhead,
		                             detail_overhead);
		overhead.snapshot_active_workers_ns += DurationNs(TimestampNs(), subphase_start_ns);
		subphase_start_ns = TimestampNs();
		DrainQueuedRecords(scratch_sampled_segments, scratch_closed_segments, scratch_queue_segments,
		                   scratch_queue_closed_segments, &overhead, detail_overhead);
		overhead.queue_drain_ns += DurationNs(TimestampNs(), subphase_start_ns);
		overhead.segments_drained += scratch_closed_segments.size();
		subphase_start_ns = TimestampNs();
		AddClosedSegmentObservations(scratch_closed_segments, detail_overhead);
		overhead.closed_segment_profile_ns += DurationNs(TimestampNs(), subphase_start_ns);
		overhead.drain_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		phase_start_ns = TimestampNs();
		BuildPendingWindowSocketIndexLocked();
		AddSegmentRecordsToWindowsLocked(scratch_sampled_segments, false, &overhead, detail_overhead);
		AddPendingSegmentRecordsToWindowsLocked(&overhead, detail_overhead);
		overhead.slice_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		phase_start_ns = TimestampNs();
		FinalizeReadyWindows(true, &overhead);
		overhead.finalize_windows_ns += DurationNs(TimestampNs(), phase_start_ns);
		FinishRuntimeOverhead(overhead, cpu_start_ns);
	}

	void CreateWindowsLocked(const vector<RaplSnapshot> &current_rapl, uint64_t now_ns) {
		if (last_rapl.empty() || now_ns <= last_sample_ns) {
			return;
		}
		std::map<int, RaplSnapshot> start_by_socket;
		for (idx_t i = 0; i < last_rapl.size(); i++) {
			start_by_socket[last_rapl[i].socket_id] = last_rapl[i];
		}
		for (idx_t i = 0; i < current_rapl.size(); i++) {
			const auto &end = current_rapl[i];
			auto start_entry = start_by_socket.find(end.socket_id);
			if (start_entry == start_by_socket.end()) {
				continue;
			}
			PeriodicPendingWindow pending;
			auto &window = pending.window;
			window.window_id = next_window_id++;
			window.socket_id = end.socket_id;
			window.start_ns = last_sample_ns;
			window.end_ns = now_ns;
			window.duration_s = static_cast<double>(now_ns - last_sample_ns) / static_cast<double>(NSEC_PER_SEC);
			window.rapl_start_uj = start_entry->second.energy_uj;
			window.rapl_end_uj = end.energy_uj;
			window.rapl_valid = start_entry->second.valid && end.valid;
			window.package_energy_j = static_cast<double>(RaplDeltaUJ(start_entry->second, end)) / 1000000.0;
			auto uncore_entry = uncore_freq_by_socket.find(window.socket_id);
			window.uncore_freq_hz = uncore_entry == uncore_freq_by_socket.end() ? 0 : uncore_entry->second;
			window.status = window.rapl_valid ? "ok" : "rapl_unavailable";
			pending_windows.push_back(std::move(pending));
		}
		pending_window_count.store(pending_windows.size(), std::memory_order_relaxed);
	}

	void DrainSegmentsLocked(vector<EnergySegmentRecord> &drained) {
		for (auto entry = queries.begin(); entry != queries.end(); ++entry) {
			DrainQuerySegmentsLocked(entry->second, drained);
		}
	}

	void DrainQuerySegmentsLocked(const shared_ptr<QueryEnergyState> &query, vector<EnergySegmentRecord> &drained) {
		if (!query) {
			return;
		}
		vector<EnergySegmentRecord> query_segments;
		vector<shared_ptr<EnergyThreadSegmentBuffer>> buffers;
		unordered_map<uint64_t, string> pipeline_signatures;
		{
			lock_guard<mutex> query_guard(query->lock);
			query_segments.insert(query_segments.end(), std::make_move_iterator(query->segments.begin()),
			                      std::make_move_iterator(query->segments.end()));
			query->segments.clear();
			buffers = query->segment_buffers;
			pipeline_signatures = query->pipeline_signatures;
		}
		for (idx_t i = 0; i < buffers.size(); i++) {
			if (buffers[i]) {
				buffers[i]->Drain(query_segments);
			}
		}
		FillPipelineSignatures(pipeline_signatures, query_segments, query->settings.pipeline_signatures_enabled);
		drained.insert(drained.end(), std::make_move_iterator(query_segments.begin()),
		               std::make_move_iterator(query_segments.end()));
	}

	void BuildPendingWindowSocketIndexLocked() {
		for (auto &entry : scratch_pending_windows_by_socket) {
			entry.second.clear();
		}
		for (auto &pending : pending_windows) {
			scratch_pending_windows_by_socket[pending.window.socket_id].push_back(&pending);
		}
	}

	idx_t AddSegmentToPendingWindowsLocked(const EnergySegmentRecord &record) {
		if (record.end_ns <= record.start_ns || record.socket_id < 0) {
			return 0;
		}
		auto socket_entry = scratch_pending_windows_by_socket.find(record.socket_id);
		if (socket_entry == scratch_pending_windows_by_socket.end()) {
			return 0;
		}
		idx_t added = 0;
		auto &socket_windows = socket_entry->second;
		for (idx_t i = 0; i < socket_windows.size(); i++) {
			auto pending_ptr = socket_windows[i];
			if (!pending_ptr) {
				continue;
			}
			auto &pending = *pending_ptr;
			auto &window = pending.window;
			if (window.end_ns <= record.start_ns) {
				continue;
			}
			if (window.start_ns >= record.end_ns) {
				break;
			}
			auto slice_start = MaxValue<uint64_t>(record.start_ns, window.start_ns);
			auto slice_end = MinValue<uint64_t>(record.end_ns, window.end_ns);
			if (slice_end <= slice_start) {
				continue;
			}
			AttributedSegment attributed;
			if (slice_start == record.start_ns && slice_end == record.end_ns) {
				attributed.record = record;
			} else {
				attributed.record = SliceSegmentRecord(record, slice_start, slice_end);
			}
			attributed.window_id = window.window_id;
			attributed.slice_start_ns = slice_start;
			attributed.slice_end_ns = slice_end;
			pending.segments.push_back(std::move(attributed));
			added++;
		}
		return added;
	}

	uint64_t MinOpenSegmentStartNsLocked() {
		uint64_t result = 0;
		for (auto entry = queries.begin(); entry != queries.end(); ++entry) {
			auto query = entry->second;
			if (!query) {
				continue;
			}
			vector<shared_ptr<EnergyThreadSegmentBuffer>> buffers;
			{
				lock_guard<mutex> query_guard(query->lock);
				buffers = query->segment_buffers;
			}
			for (idx_t i = 0; i < buffers.size(); i++) {
				if (!buffers[i]) {
					continue;
				}
				auto open_start = buffers[i]->OpenSegmentStartNs();
				if (open_start == 0) {
					continue;
				}
				if (result == 0 || open_start < result) {
					result = open_start;
				}
			}
		}
		return result;
	}

	void FinalizeReadyWindows(bool force, PeriodicRuntimeOverheadRecord *overhead = nullptr) {
		(void)force;
		// Periodic sampling creates closed slices for active segments, so old query-buffer open segments do not gate
		// window finalization.
		while (!pending_windows.empty()) {
			auto &pending = pending_windows.front();
			FinalizeWindowLocked(pending, overhead);
			pending_windows.pop_front();
		}
		pending_window_count.store(pending_windows.size(), std::memory_order_relaxed);
	}

	void FinalizeWindowLocked(PeriodicPendingWindow &pending, PeriodicRuntimeOverheadRecord *overhead = nullptr) {
		auto &window = pending.window;
		if (!pending.segments.empty()) {
			window.core_freq_hz = AverageCoreFreqForSocket(pending.segments, window.socket_id);
			auto segment_uncore = AverageUncoreFreqForSocket(pending.segments, window.socket_id);
			if (segment_uncore > 0) {
				window.uncore_freq_hz = segment_uncore;
			}
		}
		if (window.uncore_freq_hz <= 0) {
			window.uncore_freq_hz = ReadUncoreFrequencyHz(window.socket_id);
		}
		double base_power_w = 0;
		window.base_power_valid = LookupBasePower(base_power_entries, window.socket_id, window.core_freq_hz,
		                                          window.uncore_freq_hz, 0, base_power_w);
		window.base_energy_j = window.base_power_valid ? base_power_w * window.duration_s : 0;
		window.total_base_energy_j = window.base_energy_j;
		window.active_package_energy_j = std::max(0.0, window.package_energy_j - window.base_energy_j);
		if (!window.rapl_valid) {
			window.status = "rapl_unavailable";
		} else if (!window.base_power_valid) {
			window.status = "missing_base_power_calibration";
		} else {
			window.status = "ok";
		}
		vector<SocketEnergyWindow> one_window;
		one_window.push_back(window);
		auto phase_start_ns = TimestampNs();
		AttributeSegments(pending.segments, one_window);
		if (overhead) {
			overhead->attribute_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		}
		window = one_window[0];
		finalized_window_count++;
		attributed_segment_count += pending.segments.size();
		if (overhead) {
			overhead->windows_finalized++;
			overhead->segments_attributed += pending.segments.size();
		}
		total_active_conservation_error_j += window.active_conservation_error_j;
		total_base_conservation_error_j += window.base_conservation_error_j;
		total_package_gap_j += window.package_energy_j -
		                       (window.summed_attributed_active_energy_j + window.summed_attributed_base_energy_j);
		phase_start_ns = TimestampNs();
		UpdateProfilesLocked(pending.segments);
		if (overhead) {
			overhead->profile_update_ns += DurationNs(TimestampNs(), phase_start_ns);
		}
		if (settings.export_enabled && settings.debug_export_enabled &&
		    observed_query_activity.load(std::memory_order_acquire)) {
			phase_start_ns = TimestampNs();
			OpenOutputFilesIfNeededLocked();
			AppendWindowLocked(window);
			AppendSegmentsLocked(pending.segments);
			if (overhead) {
				overhead->csv_append_ns += DurationNs(TimestampNs(), phase_start_ns);
			}
		}
	}

	void UpdateProfilesLocked(const vector<AttributedSegment> &segments) {
		for (idx_t i = 0; i < segments.size(); i++) {
			const auto &segment = segments[i];
			auto pipeline_key = BuildPeriodicPipelineProfileKey(segment.record);
			AddSegmentToEfficiencyAggregate(pipeline_profiles[pipeline_key], segment, true);
			AddSegmentToEfficiencyAggregate(query_profiles[segment.record.query_id], segment, false);
		}
	}

	void OpenOutputFilesLocked() {
		window_out.open((output_dir + "/period_socket_energy_windows.csv").c_str());
		segment_out.open((output_dir + "/period_attributed_segments.csv").c_str());
		window_out << "window_id,socket_id,start_ns,end_ns,duration_s,rapl_start_uj,rapl_end_uj,package_energy_j,"
		              "core_freq_hz,uncore_freq_hz,base_energy_j,active_package_energy_j,"
		              "summed_attributed_active_energy_j,active_conservation_error_j,summed_attributed_base_energy_j,"
		              "base_conservation_error_j,summed_charged_energy_j,package_conservation_error_j,rapl_valid,"
		              "base_power_valid,used_time_fallback,status,segment_count,total_base_energy_j,busy_wall_time_s,"
		              "idle_gap_wall_time_s,busy_base_energy_j,idle_gap_base_energy_j\n";
		segment_out << "window_id,segment_id,query_id,pipeline_id,pipeline_signature,socket_id,physical_core_id,"
		               "logical_cpu_id,slice_start_ns,slice_end_ns,duration_s,work_units,frequency_ratio,"
		               "corrected_cycles,activity_weight,weight_denominator,attributed_active_energy_j,"
		               "base_time_weight,base_time_denominator_s,attributed_base_energy_j,charged_total_energy_j,"
		               "throughput,corrected_cycles_per_work,active_power_w,charged_power_w,attribution_status\n";
		output_opened = true;
	}

	void OpenOutputFilesIfNeededLocked() {
		if (output_opened) {
			return;
		}
		EnsureDirectoryRecursive(output_dir);
		OpenOutputFilesLocked();
	}

	void AppendWindowLocked(const SocketEnergyWindow &w) {
		if (!window_out.good()) {
			return;
		}
		double summed_charged_energy = w.summed_attributed_active_energy_j + w.summed_attributed_base_energy_j;
		double package_conservation_error = w.package_energy_j - summed_charged_energy;
		window_out << w.window_id << "," << w.socket_id << "," << w.start_ns << "," << w.end_ns << "," << w.duration_s
		           << "," << w.rapl_start_uj << "," << w.rapl_end_uj << "," << w.package_energy_j << ","
		           << w.core_freq_hz << "," << w.uncore_freq_hz << "," << w.base_energy_j << ","
		           << w.active_package_energy_j << "," << w.summed_attributed_active_energy_j << ","
		           << w.active_conservation_error_j << "," << w.summed_attributed_base_energy_j << ","
		           << w.base_conservation_error_j << "," << summed_charged_energy << "," << package_conservation_error
		           << "," << w.rapl_valid << "," << w.base_power_valid << "," << w.used_time_fallback << ","
		           << CsvEscape(w.status) << "," << w.segment_count << "," << w.total_base_energy_j << ","
		           << w.busy_wall_time_s << "," << w.idle_gap_wall_time_s << "," << w.busy_base_energy_j << ","
		           << w.idle_gap_base_energy_j << "\n";
	}

	void AppendSegmentsLocked(const vector<AttributedSegment> &segments) {
		if (!segment_out.good()) {
			return;
		}
		for (idx_t i = 0; i < segments.size(); i++) {
			const auto &s = segments[i];
			auto duration = SegmentDuration(s.record);
			double charged_total_energy = s.attributed_active_energy_j + s.attributed_base_energy_j;
			double cycles_per_work =
			    s.record.work_units > 0 ? s.corrected_cycles / static_cast<double>(s.record.work_units) : 0;
			double active_power = duration > 0 ? s.attributed_active_energy_j / duration : 0;
			double charged_power = duration > 0 ? charged_total_energy / duration : 0;
			segment_out << s.window_id << "," << s.record.segment_id << "," << s.record.query_id << ","
			            << s.record.pipeline_id << "," << CsvEscape(s.record.pipeline_signature) << ","
			            << s.record.socket_id << "," << s.record.physical_core_id << "," << s.record.logical_cpu_id
			            << "," << s.slice_start_ns << "," << s.slice_end_ns << "," << duration << ","
			            << s.record.work_units << "," << s.frequency_ratio << "," << s.corrected_cycles << ","
			            << s.activity_weight << "," << s.weight_denominator << "," << s.attributed_active_energy_j
			            << "," << s.base_time_weight << "," << s.base_time_denominator_s << ","
			            << s.attributed_base_energy_j << "," << charged_total_energy << "," << s.throughput << ","
			            << cycles_per_work << "," << active_power << "," << charged_power << ","
			            << CsvEscape(s.attribution_status) << "\n";
		}
	}

	void WriteFinalOutputs() {
		std::unique_lock<mutex> guard(lock);
		if (!settings.export_enabled) {
			return;
		}
		auto saw_query_activity = observed_query_activity.load(std::memory_order_acquire);
		auto has_attribution_output = attributed_segment_count > 0 || !pipeline_profiles.empty() ||
		                              !query_profiles.empty() || !closed_segment_records.empty();
		if (!saw_query_activity && !has_attribution_output) {
			return;
		}
		EnsureDirectoryRecursive(output_dir);
		if (output_opened) {
			window_out.flush();
			segment_out.flush();
		}
		vector<PipelineEfficiencyAggregate> pipeline_result;
		for (auto entry = pipeline_profiles.begin(); entry != pipeline_profiles.end(); ++entry) {
			if (entry->second.sample_count > 0 || entry->second.valid_input_sample_count > 0) {
				pipeline_result.push_back(entry->second);
			}
		}
		std::sort(pipeline_result.begin(), pipeline_result.end(),
		          [](const PipelineEfficiencyAggregate &lhs, const PipelineEfficiencyAggregate &rhs) {
			          if (lhs.query_id != rhs.query_id) {
				          return lhs.query_id < rhs.query_id;
			          }
			          if (lhs.pipeline_id != rhs.pipeline_id) {
				          return lhs.pipeline_id < rhs.pipeline_id;
			          }
			          if (lhs.core_freq_hz != rhs.core_freq_hz) {
				          return lhs.core_freq_hz < rhs.core_freq_hz;
			          }
			          if (lhs.uncore_freq_hz != rhs.uncore_freq_hz) {
				          return lhs.uncore_freq_hz < rhs.uncore_freq_hz;
			          }
			          return lhs.pipeline_signature < rhs.pipeline_signature;
		          });
		vector<PipelineEfficiencyAggregate> query_result;
		for (auto entry = query_profiles.begin(); entry != query_profiles.end(); ++entry) {
			if (entry->second.sample_count > 0 || entry->second.valid_input_sample_count > 0 ||
			    entry->second.start_ns > 0 || entry->second.end_ns > 0) {
				query_result.push_back(entry->second);
			}
		}
		std::sort(query_result.begin(), query_result.end(),
		          [](const PipelineEfficiencyAggregate &lhs, const PipelineEfficiencyAggregate &rhs) {
			          return lhs.query_id < rhs.query_id;
		          });
		WriteEfficiencyProfilesCSV(output_dir + "/period_pipeline_profiles.csv", pipeline_result);
		WriteEfficiencyProfilesCSV(output_dir + "/period_query_profiles.csv", query_result, true);
		if (settings.closed_segments_export_enabled) {
			WriteClosedSegmentsCSV(output_dir + "/period_closed_segments.csv", closed_segment_records);
		}
		WriteRuntimeOverheadCSVLocked();
		WritePeriodicValidationSummaryLocked();
	}

	static void AddRuntimeOverheadStats(PeriodicRuntimeOverheadStats &stats,
	                                    const PeriodicRuntimeOverheadRecord &record) {
		stats.count++;
		stats.total_wall_ns += record.wall_ns;
		stats.max_wall_ns = MaxValue<uint64_t>(stats.max_wall_ns, record.wall_ns);
		stats.total_cpu_ns += record.cpu_ns;
		stats.max_cpu_ns = MaxValue<uint64_t>(stats.max_cpu_ns, record.cpu_ns);
		stats.total_lock_wait_ns += record.lock_wait_ns;
		stats.max_lock_wait_ns = MaxValue<uint64_t>(stats.max_lock_wait_ns, record.lock_wait_ns);
		stats.total_lock_hold_ns += record.lock_hold_ns;
		stats.max_lock_hold_ns = MaxValue<uint64_t>(stats.max_lock_hold_ns, record.lock_hold_ns);
		stats.total_attribute_segments_ns += record.attribute_segments_ns;
		stats.max_attribute_segments_ns =
		    MaxValue<uint64_t>(stats.max_attribute_segments_ns, record.attribute_segments_ns);
		stats.total_profile_update_ns += record.profile_update_ns;
		stats.total_csv_append_ns += record.csv_append_ns;
		stats.windows_finalized += record.windows_finalized;
		stats.segments_attributed += record.segments_attributed;
	}

	void WriteRuntimeOverheadCSVLocked() {
		std::ofstream out((output_dir + "/period_runtime_overhead.csv").c_str());
		out << "sample_id,event,start_ns,end_ns,wall_ns,cpu_ns,lock_wait_ns,lock_hold_ns,rapl_read_ns,"
		       "create_windows_ns,drain_segments_ns,slice_segments_ns,finalize_windows_ns,attribute_segments_ns,"
		       "profile_update_ns,csv_append_ns,windows_created,windows_finalized,segments_drained,"
		       "segments_sliced,segments_attributed,active_queries,registered_workers,tracked_queries,"
		       "pending_windows,observed_query_activity,snapshot_active_workers_ns,queue_drain_ns,"
		       "closed_segment_profile_ns,closed_segment_local_aggregate_ns,closed_segment_store_ns,"
		       "closed_segment_merge_ns,worker_lock_wait_ns,snapshot_perf_read_ns,snapshot_record_build_ns,"
		       "snapshot_lock_hold_ns,pending_append_ns,queue_lock_wait_ns,queue_swap_ns,worker_snapshot_count,"
		       "worker_snapshot_skipped_inactive,queued_segment_count,closed_segment_count\n";
		for (idx_t i = 0; i < runtime_overhead.size(); i++) {
			const auto &r = runtime_overhead[i];
			out << r.sample_id << "," << CsvEscape(r.event) << "," << r.start_ns << "," << r.end_ns << "," << r.wall_ns
			    << "," << r.cpu_ns << "," << r.lock_wait_ns << "," << r.lock_hold_ns << "," << r.rapl_read_ns << ","
			    << r.create_windows_ns << "," << r.drain_segments_ns << "," << r.slice_segments_ns << ","
			    << r.finalize_windows_ns << "," << r.attribute_segments_ns << "," << r.profile_update_ns << ","
			    << r.csv_append_ns << "," << r.windows_created << "," << r.windows_finalized << ","
			    << r.segments_drained << "," << r.segments_sliced << "," << r.segments_attributed << ","
			    << r.active_queries << "," << r.registered_workers << "," << r.tracked_queries << ","
			    << r.pending_windows << "," << r.observed_query_activity << "," << r.snapshot_active_workers_ns << ","
			    << r.queue_drain_ns << "," << r.closed_segment_profile_ns << "," << r.closed_segment_local_aggregate_ns
			    << "," << r.closed_segment_store_ns << "," << r.closed_segment_merge_ns << "," << r.worker_lock_wait_ns
			    << "," << r.snapshot_perf_read_ns << "," << r.snapshot_record_build_ns << "," << r.snapshot_lock_hold_ns
			    << "," << r.pending_append_ns << "," << r.queue_lock_wait_ns << "," << r.queue_swap_ns << ","
			    << r.worker_snapshot_count << "," << r.worker_snapshot_skipped_inactive << "," << r.queued_segment_count
			    << "," << r.closed_segment_count << "\n";
		}
	}

	void WritePeriodicValidationSummaryLocked() {
		PeriodicRuntimeOverheadStats all_overhead;
		PeriodicRuntimeOverheadStats periodic_overhead;
		for (idx_t i = 0; i < runtime_overhead.size(); i++) {
			AddRuntimeOverheadStats(all_overhead, runtime_overhead[i]);
			if (runtime_overhead[i].event == "periodic_sample") {
				AddRuntimeOverheadStats(periodic_overhead, runtime_overhead[i]);
			}
		}
		auto run_duration_ns = DurationNs(TimestampNs(), run_start_ns);
		auto avg_wall_ns = all_overhead.count == 0 ? 0 : all_overhead.total_wall_ns / all_overhead.count;
		auto avg_cpu_ns = all_overhead.count == 0 ? 0 : all_overhead.total_cpu_ns / all_overhead.count;
		auto periodic_avg_wall_ns =
		    periodic_overhead.count == 0 ? 0 : periodic_overhead.total_wall_ns / periodic_overhead.count;
		auto periodic_avg_cpu_ns =
		    periodic_overhead.count == 0 ? 0 : periodic_overhead.total_cpu_ns / periodic_overhead.count;
		double cpu_duty_cycle = run_duration_ns == 0 ? 0
		                                             : static_cast<double>(all_overhead.total_cpu_ns) /
		                                                   static_cast<double>(run_duration_ns);
		double periodic_cpu_duty_cycle = run_duration_ns == 0 ? 0
		                                                      : static_cast<double>(periodic_overhead.total_cpu_ns) /
		                                                            static_cast<double>(run_duration_ns);
		std::ofstream out((output_dir + "/period_validation_summary.json").c_str());
		out << "{\n";
		out << "  \"window_count\": " << finalized_window_count << ",\n";
		out << "  \"attributed_segment_count\": " << attributed_segment_count << ",\n";
		out << "  \"active_conservation_error_j\": " << total_active_conservation_error_j << ",\n";
		out << "  \"base_conservation_error_j\": " << total_base_conservation_error_j << ",\n";
		out << "  \"package_conservation_gap_j\": " << total_package_gap_j << ",\n";
		out << "  \"base_power_loaded\": " << (base_power_loaded ? "true" : "false") << ",\n";
		out << "  \"period_ms\": " << period_ms << ",\n";
		out << "  \"debug_export_enabled\": " << (settings.debug_export_enabled ? "true" : "false") << ",\n";
		out << "  \"closed_segments_export_enabled\": " << (settings.closed_segments_export_enabled ? "true" : "false")
		    << ",\n";
		out << "  \"closed_segment_record_count\": " << closed_segment_records.size() << ",\n";
		out << "  \"runtime_overhead_event_count\": " << all_overhead.count << ",\n";
		out << "  \"runtime_overhead_periodic_sample_count\": " << periodic_overhead.count << ",\n";
		out << "  \"runtime_overhead_total_wall_ns\": " << all_overhead.total_wall_ns << ",\n";
		out << "  \"runtime_overhead_total_cpu_ns\": " << all_overhead.total_cpu_ns << ",\n";
		out << "  \"runtime_overhead_avg_wall_ns\": " << avg_wall_ns << ",\n";
		out << "  \"runtime_overhead_max_wall_ns\": " << all_overhead.max_wall_ns << ",\n";
		out << "  \"runtime_overhead_avg_cpu_ns\": " << avg_cpu_ns << ",\n";
		out << "  \"runtime_overhead_max_cpu_ns\": " << all_overhead.max_cpu_ns << ",\n";
		out << "  \"runtime_overhead_cpu_duty_cycle\": " << cpu_duty_cycle << ",\n";
		out << "  \"runtime_overhead_periodic_total_wall_ns\": " << periodic_overhead.total_wall_ns << ",\n";
		out << "  \"runtime_overhead_periodic_total_cpu_ns\": " << periodic_overhead.total_cpu_ns << ",\n";
		out << "  \"runtime_overhead_periodic_avg_wall_ns\": " << periodic_avg_wall_ns << ",\n";
		out << "  \"runtime_overhead_periodic_max_wall_ns\": " << periodic_overhead.max_wall_ns << ",\n";
		out << "  \"runtime_overhead_periodic_avg_cpu_ns\": " << periodic_avg_cpu_ns << ",\n";
		out << "  \"runtime_overhead_periodic_max_cpu_ns\": " << periodic_overhead.max_cpu_ns << ",\n";
		out << "  \"runtime_overhead_periodic_cpu_duty_cycle\": " << periodic_cpu_duty_cycle << ",\n";
		out << "  \"runtime_overhead_periodic_windows_finalized\": " << periodic_overhead.windows_finalized << ",\n";
		out << "  \"runtime_overhead_periodic_segments_attributed\": " << periodic_overhead.segments_attributed << "\n";
		out << "}\n";
	}

	void CloseOutputFilesLocked() {
		if (window_out.is_open()) {
			window_out.close();
		}
		if (segment_out.is_open()) {
			segment_out.close();
		}
	}

private:
	mutex lock;
	std::condition_variable cv;
	std::thread worker;
	DatabaseInstance *active_db = nullptr;
	bool running = false;
	bool stop_requested = false;
	bool output_opened = false;
	std::atomic<bool> runtime_active {false};
	std::atomic<bool> observed_query_activity {false};
	std::atomic<idx_t> active_query_count {0};
	std::atomic<uint64_t> runtime_generation {0};
	EnergyAttributionSettings settings;
	uint64_t period_ms = 100;
	uint64_t run_start_ns = 0;
	std::atomic<uint64_t> next_runtime_overhead_id {1};
	string output_dir;
	RaplReader rapl;
	vector<RaplSnapshot> last_rapl;
	uint64_t last_sample_ns = 0;
	uint64_t next_window_id = 1;
	vector<BasePowerEntry> base_power_entries;
	bool base_power_loaded = false;
	std::map<int, double> uncore_freq_by_socket;
	std::map<QueryEnergyState *, shared_ptr<QueryEnergyState>> queries;
	std::map<QueryEnergyState *, bool> query_active;
	std::map<std::pair<DatabaseInstance *, uint64_t>, SchedulerWorkerRegistration> worker_registry;
	std::unordered_map<int64_t, shared_ptr<PeriodicWorkerSegmentState>> worker_segment_states;
	mutex active_worker_lock;
	vector<shared_ptr<PeriodicWorkerSegmentState>> active_worker_states;
	mutex queue_lock;
	vector<EnergySegmentRecord> queued_segment_records;
	vector<EnergySegmentRecord> queued_closed_segment_records;
	vector<shared_ptr<PeriodicWorkerSegmentState>> scratch_worker_states;
	vector<EnergySegmentRecord> scratch_sampled_segments;
	vector<EnergySegmentRecord> scratch_closed_segments;
	vector<EnergySegmentRecord> scratch_queue_segments;
	vector<EnergySegmentRecord> scratch_queue_closed_segments;
	vector<RaplSnapshot> scratch_rapl_snapshots;
	std::unordered_map<PeriodicPipelineProfileKey, PipelineEfficiencyAggregate, PeriodicPipelineProfileKeyHash>
	    scratch_pipeline_profile_updates;
	std::unordered_map<uint64_t, PipelineEfficiencyAggregate> scratch_query_profile_updates;
	std::deque<PeriodicPendingWindow> pending_windows;
	std::unordered_map<int, vector<PeriodicPendingWindow *>> scratch_pending_windows_by_socket;
	std::atomic<idx_t> pending_window_count {0};
	vector<EnergySegmentRecord> pending_segment_records;
	vector<EnergySegmentRecord> scratch_remaining_segment_records;
	vector<EnergySegmentRecord> closed_segment_records;
	std::unordered_map<PeriodicPipelineProfileKey, PipelineEfficiencyAggregate, PeriodicPipelineProfileKeyHash>
	    pipeline_profiles;
	std::unordered_map<uint64_t, PipelineEfficiencyAggregate> query_profiles;
	vector<PeriodicRuntimeOverheadRecord> runtime_overhead;
	std::ofstream window_out;
	std::ofstream segment_out;
	idx_t finalized_window_count = 0;
	idx_t attributed_segment_count = 0;
	double total_active_conservation_error_j = 0;
	double total_base_conservation_error_j = 0;
	double total_package_gap_j = 0;
};

} // namespace

const char *EnergySegmentRoleToString(EnergySegmentRole role) {
	switch (role) {
	case EnergySegmentRole::ASSIGNED_MANDATORY:
		return "ASSIGNED_MANDATORY";
	case EnergySegmentRole::ASSIGNED_OPTIONAL:
		return "ASSIGNED_OPTIONAL";
	case EnergySegmentRole::SAME_QUERY_SUCCESSOR:
		return "SAME_QUERY_SUCCESSOR";
	case EnergySegmentRole::FALLBACK_SIBLING:
		return "FALLBACK_SIBLING";
	case EnergySegmentRole::FALLBACK_SOCKET:
		return "FALLBACK_SOCKET";
	case EnergySegmentRole::FALLBACK_GLOBAL:
		return "FALLBACK_GLOBAL";
	case EnergySegmentRole::EXPLORATION_PROBE:
		return "EXPLORATION_PROBE";
	case EnergySegmentRole::UNKNOWN:
	default:
		return "UNKNOWN";
	}
}

static bool IsEnergyAttributionControlQuery(const string &query) {
	auto normalized = query;
	StringUtil::Trim(normalized);
	normalized = StringUtil::Lower(normalized);
	return normalized == "set" || StringUtil::StartsWith(normalized, "set ") ||
	       StringUtil::StartsWith(normalized, "set\t") || StringUtil::StartsWith(normalized, "set\n") ||
	       StringUtil::StartsWith(normalized, "pragma ") || normalized == "use" ||
	       StringUtil::StartsWith(normalized, "use ") || StringUtil::StartsWith(normalized, "use\t") ||
	       StringUtil::StartsWith(normalized, "use\n");
}

bool EnergyAttributionManager::Enabled(const ClientContext &context) {
	if (ClientConfig::GetConfig(context).energy_attribution.enabled) {
		return true;
	}
	EnergyAttributionSettings settings;
	return TryGetDatabaseEnergySettings(DatabaseInstance::GetDatabase(context), settings);
}

void EnergyAttributionManager::ConfigureDatabaseRuntime(ClientContext &context) {
	auto &settings = ClientConfig::GetConfig(context).energy_attribution;
	auto &db = DatabaseInstance::GetDatabase(context);
	StoreDatabaseEnergySettings(db, settings);
	PeriodicEnergyRuntime::Get().Configure(db, settings);
}

void EnergyAttributionManager::ShutdownDatabaseRuntime(DatabaseInstance &db) {
	ClearDatabaseEnergySettings(db);
	PeriodicEnergyRuntime::Get().Shutdown(db);
}

void EnergyAttributionManager::RegisterSchedulerWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid,
                                                       int logical_cpu) {
	PeriodicEnergyRuntime::Get().RegisterWorker(db, worker_id, linux_tid, logical_cpu);
}

void EnergyAttributionManager::UnregisterSchedulerWorker(DatabaseInstance &db, uint64_t worker_id, int64_t linux_tid) {
	PeriodicEnergyRuntime::Get().UnregisterWorker(db, worker_id, linux_tid);
}

void EnergyAttributionManager::BeginQuery(ClientContext &context, uint64_t query_id, const string &query) {
	auto &config = ClientConfig::GetConfig(context);
	auto &db = DatabaseInstance::GetDatabase(context);
	EnergyAttributionSettings settings;
	if (config.energy_attribution.enabled) {
		settings = config.energy_attribution;
		EnergyAttributionManager::ConfigureDatabaseRuntime(context);
	} else if (!TryGetDatabaseEnergySettings(db, settings)) {
		return;
	}
	if (IsEnergyAttributionControlQuery(query)) {
		return;
	}
	auto state = make_shared_ptr<QueryEnergyState>();
	state->context = &context;
	state->settings = settings;
	state->query_id = query_id;
	state->query = query;
	state->start_ns = TimestampNs();
	if (state->settings.metadata_cache_enabled) {
		state->uncore_freq_by_socket = BuildQueryUncoreFrequencyCache(true);
	}
	if (state->settings.rapl_enabled && !state->settings.periodic_enabled) {
		state->rapl_available = state->rapl.HasDomains();
		state->rapl_start = state->rapl.Read();
		state->base_power_loaded = LoadBasePowerCSVCached(state->settings.base_power_path, state->base_power_entries);
	}
	auto base_dir = state->settings.output_dir.empty() ? string(DEFAULT_OUTPUT_DIR) : state->settings.output_dir;
	state->output_dir = base_dir + "/" + StringUtil::Format("query_%llu_%llu", query_id, state->start_ns);
	{
		lock_guard<mutex> guard(g_query_lock);
		g_queries[&context] = state;
	}
	if (state->settings.periodic_enabled) {
		PeriodicEnergyRuntime::Get().RegisterQuery(state);
	}
}

void EnergyAttributionManager::EndQuery(ClientContext &context) {
	shared_ptr<QueryEnergyState> state;
	{
		lock_guard<mutex> guard(g_query_lock);
		auto entry = g_queries.find(&context);
		if (entry == g_queries.end()) {
			return;
		}
		state = entry->second;
		g_queries.erase(entry);
	}
	if (state && state->settings.periodic_enabled) {
		PeriodicEnergyRuntime::Get().MarkQueryEnded(state);
	} else if (state) {
		FinalizeAndWrite(*state);
	}
}

void EnergyAttributionManager::AttachPipeline(ClientContext &context, Pipeline &pipeline,
                                              uint64_t profiler_pipeline_id) {
	if (!EnergyAttributionManager::Enabled(context)) {
		return;
	}
	shared_ptr<QueryEnergyState> query;
	{
		lock_guard<mutex> guard(g_query_lock);
		auto entry = g_queries.find(&context);
		if (entry == g_queries.end()) {
			return;
		}
		query = entry->second;
	}
	if (!query) {
		return;
	}
	auto pipeline_id =
	    profiler_pipeline_id ? profiler_pipeline_id : static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&pipeline));
	if (query->settings.pipeline_signatures_enabled) {
		RegisterPipelineSignature(*query, pipeline_id, pipeline.ToString());
	}
	pipeline.SetEnergyAttributionPipeline(query, pipeline_id);
}

EnergySegmentScope::EnergySegmentScope(Pipeline &pipeline, int start_cpu_hint) {
	query_handle = pipeline.GetEnergyAttributionQueryHandle();
	if (!query_handle) {
		return;
	}
	auto query_ref = shared_ptr_cast<EnergyAttributionQueryHandle, QueryEnergyState>(query_handle);
	auto query = query_ref.get();
	query_state = query;
	auto &thread_state = g_thread_energy_state;
	thread_state.InitializeHardware(query->settings);
	auto tid = thread_state.GetTID();
	record = EnergySegmentRecord();
	record.segment_id = thread_state.NextSegmentId(*query);
	record.query_id = query->query_id;
	record.pipeline_id = pipeline.GetEnergyAttributionPipelineId();
	if (record.pipeline_id == 0) {
		record.pipeline_id = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&pipeline));
	}
	record.pipeline_signature = LookupPipelineSignature(*query, record.pipeline_id);
	record.role = EnergySegmentRole::UNKNOWN;
	record.worker_id = static_cast<uint64_t>(tid);
	record.linux_tid = tid;
	record.start_ns = TimestampNs();
	if (query->settings.metadata_cache_enabled && thread_state.pinned) {
		record.logical_cpu_id = thread_state.pinned_cpu;
		record.socket_id = thread_state.pinned_socket_id;
		record.physical_core_id = thread_state.pinned_physical_core_id;
		record.core_freq_hz = thread_state.pinned_core_freq_hz;
	} else {
		record.logical_cpu_id = start_cpu_hint >= 0 ? start_cpu_hint : CurrentCPU();
		auto topology = LookupCpuTopology(record.logical_cpu_id, query->settings.metadata_cache_enabled);
		record.socket_id = topology.socket_id;
		record.physical_core_id = topology.physical_core_id;
		record.core_freq_hz = topology.core_freq_hz;
	}
	record.uncore_freq_hz = GetQueryUncoreFrequency(*query, record.socket_id);
	if (query->settings.periodic_enabled) {
		active = PeriodicEnergyRuntime::Get().BeginSegment(query_ref, record);
		return;
	}
	segment_buffer = GetThreadSegmentBuffer(*query, tid);
	if (segment_buffer) {
		static_cast<EnergyThreadSegmentBuffer *>(segment_buffer)->MarkSegmentOpen(record.start_ns);
	}
	if (query->settings.perf_counters_enabled) {
		start_perf = thread_state.ReadPerfSnapshot(record.linux_tid, query->settings);
		perf_started = start_perf.valid;
	} else {
		start_perf.status = "perf disabled";
	}
	active = true;
}

EnergySegmentScope::~EnergySegmentScope() {
	if (!active || finished) {
		return;
	}
	finished = true;
	auto query = static_cast<QueryEnergyState *>(query_state);
	auto buffer = static_cast<EnergyThreadSegmentBuffer *>(segment_buffer);
	bool periodic = query && query->settings.periodic_enabled;
	PerfCounters counters;
	if (!periodic && query && query->settings.perf_counters_enabled) {
		if (perf_started) {
			auto end_perf = g_thread_energy_state.ReadPerfSnapshot(record.linux_tid, query->settings);
			counters = DeltaPerfCounters(start_perf, end_perf);
		} else {
			counters.status = start_perf.status.empty() ? "perf unavailable" : start_perf.status;
		}
	} else if (!periodic) {
		counters.status = "perf disabled";
	}
	record.end_ns = TimestampNs();
	bool end_metadata_set = false;
	if (query && !query->settings.migration_check_enabled && g_thread_energy_state.pinned) {
		record.end_logical_cpu_id = record.logical_cpu_id;
		record.end_socket_id = record.socket_id;
		record.end_physical_core_id = record.physical_core_id;
		end_metadata_set = true;
	} else {
		record.end_logical_cpu_id = end_cpu_hint >= 0 ? end_cpu_hint : CurrentCPU();
	}
	if (!end_metadata_set) {
		if (query && query->settings.migration_check_enabled && query->settings.metadata_cache_enabled &&
		    g_thread_energy_state.pinned && record.end_logical_cpu_id == g_thread_energy_state.pinned_cpu) {
			record.end_socket_id = g_thread_energy_state.pinned_socket_id;
			record.end_physical_core_id = g_thread_energy_state.pinned_physical_core_id;
		} else if (query) {
			auto topology = LookupCpuTopology(record.end_logical_cpu_id, query->settings.metadata_cache_enabled);
			record.end_socket_id = topology.socket_id;
			record.end_physical_core_id = topology.physical_core_id;
		} else {
			auto topology = LookupCpuTopology(record.end_logical_cpu_id, false);
			record.end_socket_id = topology.socket_id;
			record.end_physical_core_id = topology.physical_core_id;
		}
	}
	record.duration_s = record.end_ns > record.start_ns
	                        ? static_cast<double>(record.end_ns - record.start_ns) / static_cast<double>(NSEC_PER_SEC)
	                        : 0;
	record.migrated = record.logical_cpu_id != record.end_logical_cpu_id || record.socket_id != record.end_socket_id ||
	                  record.physical_core_id != record.end_physical_core_id;
	record.hardware_state_stable = !record.migrated && record.socket_id >= 0 && record.physical_core_id >= 0;
	if (periodic) {
		PeriodicEnergyRuntime::Get().EndSegment(record);
	} else {
		ApplyPerfCounters(record, counters);
		if (buffer) {
			buffer->Append(std::move(record));
			buffer->MarkSegmentClosed();
		} else if (query) {
			lock_guard<mutex> guard(query->lock);
			query->segments.push_back(std::move(record));
		}
	}
	active = false;
}

void EnergySegmentScope::SetEndCPUHint(int end_cpu_hint_p) {
	end_cpu_hint = end_cpu_hint_p;
}

void EnergySegmentScope::SetWork(uint64_t tuples, uint64_t chunks) {
	if (!active) {
		return;
	}
	record.tuples = tuples;
	record.chunks = chunks;
	record.work_units = tuples;
}

} // namespace duckdb
