//===----------------------------------------------------------------------===//
//                         DuckDB
//
// energy_attribution.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/energy_attribution/energy_attribution.hpp"
#include "duckdb/energy_attribution/process_activity.hpp"
#include "energy_attribution_base_power.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"

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

using energy_attribution::BasePowerEntry;
using energy_attribution::LoadBasePowerCSVCached;
using energy_attribution::LookupBasePower;

static const char *DEFAULT_OUTPUT_DIR = "energy_attribution_output";
static const uint64_t NSEC_PER_SEC = 1000000000ULL;
static const uint64_t DEFAULT_THREAD_SEGMENT_ID_BLOCK_SIZE = 1024;

struct PerfCounters {
	uint64_t task_clock_ns = 0;
	uint64_t cycles = 0;
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
	double task_clock_coverage = 0;
	double throughput = 0;
	bool process_attribution_valid = false;
	string process_counter_source;
	string attribution_status;
};

struct AttributedProcessActivity {
	ProcessActivityRecord record;
	uint64_t window_id = 0;
	double activity_weight = 0;
	double weight_denominator = 0;
	double attributed_active_energy_j = 0;
	double base_time_weight = 0;
	double base_time_denominator_ns = 0;
	double attributed_base_energy_j = 0;
	bool duckdb_process = false;
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
	double modeled_base_energy_j = 0;
	double base_model_excess_energy_j = 0;
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
	double process_attributed_active_energy_j = 0;
	double process_attributed_base_energy_j = 0;
	double process_conservation_error_j = 0;
	double process_base_conservation_error_j = 0;
	double duckdb_process_active_energy_j = 0;
	double duckdb_process_base_energy_j = 0;
	double external_process_active_energy_j = 0;
	double external_process_base_energy_j = 0;
	double host_kernel_active_energy_j = 0;
	double host_kernel_base_energy_j = 0;
	double host_irq_active_energy_j = 0;
	double host_irq_base_energy_j = 0;
	double system_idle_active_energy_j = 0;
	double system_idle_base_energy_j = 0;
	double measurement_overhead_active_energy_j = 0;
	double measurement_overhead_base_energy_j = 0;
	double duckdb_instrumented_active_energy_j = 0;
	double duckdb_instrumented_base_energy_j = 0;
	double duckdb_uninstrumented_active_energy_j = 0;
	double duckdb_uninstrumented_base_energy_j = 0;
	double duckdb_hierarchy_error_j = 0;
	double duckdb_base_hierarchy_error_j = 0;
	uint32_t process_monitored_cpu_count = 0;
	uint64_t process_observed_runtime_ns = 0;
	double process_expected_runtime_ns = 0;
	double process_runtime_coverage = 0;
	uint64_t process_generation = 0;
	uint64_t process_transition_ns = 0;
	uint64_t process_boundary_min_ns = 0;
	uint64_t process_boundary_max_ns = 0;
	uint64_t process_maximum_checkpoint_delay_ns = 0;
	uint64_t process_checkpoint_duration_ns = 0;
	uint64_t process_helper_count = 0;
	int64_t process_helper_futex_wake_count = -1;
	uint64_t process_acknowledged_helper_count = 0;
	uint64_t process_missing_helper_count = 0;
	int32_t process_slowest_helper_cpu = -1;
	string process_missing_helper_cpus;
	uint64_t process_activity_map_insert_failure = 0;
	uint64_t process_missing_counter = 0;
	uint64_t process_invalid_counter_delta = 0;
	uint64_t process_interrupt_underflow = 0;
	uint64_t process_boundary_split_count = 0;
	uint64_t process_boundary_split_cpu_count = 0;
	uint64_t process_boundary_split_runtime_ns = 0;
	uint64_t process_boundary_split_cycles = 0;
	uint64_t process_boundary_split_ref_cycles = 0;
	uint64_t process_late_helper_count = 0;
	uint64_t process_lost_records = 0;
	uint64_t process_lookup_failures = 0;
	idx_t segment_count = 0;
	bool rapl_valid = false;
	bool base_power_valid = false;
	bool used_time_fallback = false;
	bool process_boundary_corrected = false;
	bool process_attribution_valid = false;
	string process_status;
	string status;
};

struct ProcessEnergyAggregate {
	uint64_t process_start_time_ns = 0;
	uint32_t tgid = 0;
	int socket_id = -1;
	ProcessActivityKind kind = ProcessActivityKind::PROCESS;
	string comm;
	uint64_t sample_count = 0;
	uint64_t runtime_ns = 0;
	uint64_t cycles = 0;
	uint64_t ref_cycles = 0;
	double corrected_cycles = 0;
	double active_energy_j = 0;
	double base_energy_j = 0;
};

struct ProcessEnergyProfileKey {
	uint64_t process_start_time_ns = 0;
	uint32_t tgid = 0;
	int socket_id = -1;
	ProcessActivityKind kind = ProcessActivityKind::PROCESS;
	string comm;

	bool operator==(const ProcessEnergyProfileKey &other) const {
		return process_start_time_ns == other.process_start_time_ns && tgid == other.tgid &&
		       socket_id == other.socket_id && kind == other.kind && comm == other.comm;
	}
};

struct ProcessEnergyProfileKeyHash {
	size_t operator()(const ProcessEnergyProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.process_start_time_ns);
		result ^= std::hash<uint32_t> {}(key.tgid) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<int> {}(key.socket_id) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<int> {}(static_cast<int>(key.kind)) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<string> {}(key.comm) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

using ProcessEnergyProfileMap =
    std::unordered_map<ProcessEnergyProfileKey, ProcessEnergyAggregate, ProcessEnergyProfileKeyHash>;

struct PreparedSocketProcessActivity {
	int socket_id = -1;
	vector<idx_t> record_indices;
	double corrected_weight_sum = 0;
	double runtime_weight_sum = 0;
	uint32_t monitored_cpu_count = 0;
	uint32_t helper_count = 0;
	uint32_t acknowledged_helper_count = 0;
	uint32_t missing_helper_count = 0;
	uint64_t boundary_min_ns = 0;
	uint64_t boundary_max_ns = 0;
	uint64_t maximum_checkpoint_delay_ns = 0;
	uint64_t late_helper_count = 0;
	int32_t slowest_helper_cpu = -1;
	string missing_helper_cpus;
	bool has_non_idle_activity = false;
	bool all_pmu_valid = true;
	bool checkpoint_valid = false;
};

struct PreparedProcessActivity {
	const ProcessActivityWindow *source = nullptr;
	vector<PreparedSocketProcessActivity> sockets;
	std::unordered_map<int, idx_t> socket_to_index;
};

struct DuckDBThreadShare {
	double active_energy_j = 0;
	double base_energy_j = 0;
	uint64_t runtime_ns = 0;
	double corrected_cycles = 0;
	double corrected_ref_cycles = 0;
	double activity_weight = 0;
	double segment_cycle_sum = 0;
	uint64_t segment_task_clock_sum = 0;
	double covered_active_energy_j = 0;
	double covered_base_energy_j = 0;
	double task_clock_coverage = 0;
	double segment_denominator = 0;
	bool use_segment_time = false;
};

enum class PeriodicHardwareProfileMode : uint8_t {
	LOGICAL = 0,
	CONTROLLED = 1,
};

static const char *PeriodicHardwareProfileModeToString(PeriodicHardwareProfileMode mode) {
	switch (mode) {
	case PeriodicHardwareProfileMode::CONTROLLED:
		return "CONTROLLED";
	case PeriodicHardwareProfileMode::LOGICAL:
	default:
		return "LOGICAL";
	}
}

struct PipelineEfficiencyAggregate {
	uint64_t query_id = 0;
	uint64_t pipeline_id = 0;
	PeriodicHardwareProfileMode hardware_mode = PeriodicHardwareProfileMode::LOGICAL;
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
	double allocated_initialize_active_energy_j = 0;
	double allocated_initialize_base_energy_j = 0;
	double allocated_prepare_finish_active_energy_j = 0;
	double allocated_prepare_finish_base_energy_j = 0;
	double allocated_finish_active_energy_j = 0;
	double allocated_finish_base_energy_j = 0;
};

struct SystemEnergyAggregate {
	EnergySystemCategory category = EnergySystemCategory::NONE;
	uint64_t sample_count = 0;
	double duration_s = 0;
	double attributed_active_energy_j = 0;
	double base_time_share_energy_j = 0;
	double corrected_cycles = 0;
};

struct LifecyclePhaseProfileAggregate {
	uint64_t query_id = 0;
	uint64_t lifecycle_group_id = 0;
	uint64_t owner_pipeline_id = 0;
	uint64_t lifecycle_member_count = 0;
	EnergySegmentPhase phase = EnergySegmentPhase::EXECUTE;
	PeriodicHardwareProfileMode hardware_mode = PeriodicHardwareProfileMode::LOGICAL;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	double core_freq_hz = 0;
	double uncore_freq_hz = 0;
	int smt_occupancy = 1;
	uint64_t sample_count = 0;
	double total_duration_s = 0;
	double attributed_active_energy_j = 0;
	double base_time_share_energy_j = 0;
	double corrected_cycles = 0;
	vector<uint64_t> member_pipeline_ids;
};

struct PeriodicPipelineProfileKey {
	uint64_t query_id = 0;
	uint64_t pipeline_id = 0;
	PeriodicHardwareProfileMode hardware_mode = PeriodicHardwareProfileMode::LOGICAL;
	uint32_t core_frequency_khz = 0;
	uint32_t uncore_frequency_khz = 0;

	bool operator==(const PeriodicPipelineProfileKey &other) const {
		return query_id == other.query_id && pipeline_id == other.pipeline_id &&
		       hardware_mode == other.hardware_mode && core_frequency_khz == other.core_frequency_khz &&
		       uncore_frequency_khz == other.uncore_frequency_khz;
	}
};

struct PeriodicPipelineProfileKeyHash {
	size_t operator()(const PeriodicPipelineProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.query_id);
		result ^= std::hash<uint64_t> {}(key.pipeline_id) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<int> {}(static_cast<int>(key.hardware_mode)) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<uint32_t> {}(key.core_frequency_khz) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<uint32_t> {}(key.uncore_frequency_khz) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		return result;
	}
};

struct PeriodicLifecycleProfileKey {
	uint64_t query_id = 0;
	uint64_t lifecycle_group_id = 0;
	EnergySegmentPhase phase = EnergySegmentPhase::EXECUTE;
	PeriodicHardwareProfileMode hardware_mode = PeriodicHardwareProfileMode::LOGICAL;
	uint32_t core_frequency_khz = 0;
	uint32_t uncore_frequency_khz = 0;

	bool operator==(const PeriodicLifecycleProfileKey &other) const {
		return query_id == other.query_id && lifecycle_group_id == other.lifecycle_group_id && phase == other.phase &&
		       hardware_mode == other.hardware_mode && core_frequency_khz == other.core_frequency_khz &&
		       uncore_frequency_khz == other.uncore_frequency_khz;
	}
};

struct PeriodicLifecycleProfileKeyHash {
	size_t operator()(const PeriodicLifecycleProfileKey &key) const {
		size_t result = std::hash<uint64_t> {}(key.query_id);
		result ^= std::hash<uint64_t> {}(key.lifecycle_group_id) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<int> {}(static_cast<int>(key.phase)) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<int> {}(static_cast<int>(key.hardware_mode)) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<uint32_t> {}(key.core_frequency_khz) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		result ^= std::hash<uint32_t> {}(key.uncore_frequency_khz) + 0x9e3779b97f4a7c15ULL + (result << 6) +
		          (result >> 2);
		return result;
	}
};

static bool HasControlledHardwareProfile(const EnergySegmentRecord &record) {
	return record.hardware_state_stable && record.target_core_frequency_khz != 0 &&
	       record.target_uncore_frequency_khz != 0;
}

static PeriodicPipelineProfileKey BuildPeriodicPipelineProfileKey(const EnergySegmentRecord &record) {
	PeriodicPipelineProfileKey key;
	key.query_id = record.query_id;
	key.pipeline_id = record.pipeline_id;
	if (HasControlledHardwareProfile(record)) {
		key.hardware_mode = PeriodicHardwareProfileMode::CONTROLLED;
		key.core_frequency_khz = record.target_core_frequency_khz;
		key.uncore_frequency_khz = record.target_uncore_frequency_khz;
	}
	return key;
}

static PeriodicPipelineProfileKey BuildPeriodicPipelineProfileKeyForPipeline(const EnergySegmentRecord &record,
                                                                             uint64_t pipeline_id) {
	auto key = BuildPeriodicPipelineProfileKey(record);
	key.pipeline_id = pipeline_id;
	return key;
}

static PeriodicLifecycleProfileKey BuildPeriodicLifecycleProfileKey(const EnergySegmentRecord &record) {
	PeriodicLifecycleProfileKey key;
	key.query_id = record.query_id;
	key.lifecycle_group_id = record.lifecycle_group_id;
	key.phase = record.phase;
	if (HasControlledHardwareProfile(record)) {
		key.hardware_mode = PeriodicHardwareProfileMode::CONTROLLED;
		key.core_frequency_khz = record.target_core_frequency_khz;
		key.uncore_frequency_khz = record.target_uncore_frequency_khz;
	}
	return key;
}

struct QueryEnergyState;
static mutex g_query_lock;
static unordered_map<ClientContext *, shared_ptr<QueryEnergyState>> g_queries;
static std::atomic<uint64_t> g_system_segment_id {1};
static mutex g_database_settings_lock;
static unordered_map<const DatabaseInstance *, EnergyAttributionSettings> g_database_settings;
static void StoreDatabaseEnergySettings(DatabaseInstance &db, const EnergyAttributionSettings &settings) {
	lock_guard<mutex> guard(g_database_settings_lock);
	if (settings.enabled) {
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
	return settings.enabled;
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
static long PerfEventOpen(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
	return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
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

	bool Open(int64_t tid) {
#ifdef __linux__
		if (IsOpen()) {
			if (opened_tid == tid) {
				return true;
			}
			Close();
		}
		struct perf_event_attr task_clock_attr;
		memset(&task_clock_attr, 0, sizeof(task_clock_attr));
		task_clock_attr.type = PERF_TYPE_SOFTWARE;
		task_clock_attr.size = sizeof(task_clock_attr);
		task_clock_attr.config = PERF_COUNT_SW_TASK_CLOCK;
		task_clock_attr.disabled = 0;
		task_clock_attr.exclude_kernel = 0;
		task_clock_attr.exclude_hv = 1;
		task_clock_attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
		task_clock_fd =
		    NumericCast<int>(PerfEventOpen(&task_clock_attr, static_cast<pid_t>(tid), -1, -1, 0));
		if (task_clock_fd < 0) {
			status = StringUtil::Format("perf_event_open(task_clock) failed: %s", std::strerror(errno));
			Close();
			return false;
		}

		status = "process";
		cycles_fd = OpenIndependentHardwareCounter(tid, PERF_COUNT_HW_CPU_CYCLES);
		if (cycles_fd < 0) {
			status += StringUtil::Format("|cycles_unavailable:%s", std::strerror(errno));
		}
		status += "|ref_cycles_process_proxy";
		opened_tid = tid;
		return true;
#else
		status = "perf_event_open is Linux-only";
		return false;
#endif
	}

	bool IsOpen() const {
		return task_clock_fd >= 0;
	}

	EnergyPerfSnapshot ReadSnapshot() const {
		EnergyPerfSnapshot snapshot;
		snapshot.status = status;
#ifdef __linux__
		if (task_clock_fd < 0) {
			return snapshot;
		}
		uint64_t task_clock_buffer[3] = {};
		auto task_clock_bytes = read(task_clock_fd, task_clock_buffer, sizeof(task_clock_buffer));
		if (task_clock_bytes != static_cast<ssize_t>(sizeof(task_clock_buffer))) {
			snapshot.status =
			    StringUtil::Format("task-clock read failed: %s",
			                       task_clock_bytes < 0 ? std::strerror(errno) : "short read");
			return snapshot;
		}
		snapshot.task_clock_ns = task_clock_buffer[0];
		snapshot.task_clock_time_enabled = task_clock_buffer[1];
		snapshot.task_clock_time_running = task_clock_buffer[2];
		snapshot.task_clock_valid = true;
		snapshot.valid = true;
		snapshot.scaled = snapshot.task_clock_time_running > 0 &&
		                  snapshot.task_clock_time_enabled > snapshot.task_clock_time_running;

		if (cycles_fd >= 0) {
			uint64_t cycles_buffer[3] = {};
			auto cycles_bytes = read(cycles_fd, cycles_buffer, sizeof(cycles_buffer));
			if (cycles_bytes == static_cast<ssize_t>(sizeof(cycles_buffer))) {
				snapshot.cycles = cycles_buffer[0];
				snapshot.cycles_time_enabled = cycles_buffer[1];
				snapshot.cycles_time_running = cycles_buffer[2];
				snapshot.cycles_valid = true;
				snapshot.scaled = snapshot.scaled || (snapshot.cycles_time_running > 0 &&
				                                      snapshot.cycles_time_enabled > snapshot.cycles_time_running);
			} else {
				snapshot.status +=
				    StringUtil::Format("|cycles_read_failed:%s",
				                       cycles_bytes < 0 ? std::strerror(errno) : "short read");
			}
		}
#endif
		return snapshot;
	}

	void Close() {
#ifdef __linux__
		if (task_clock_fd >= 0) {
			close(task_clock_fd);
		}
		if (cycles_fd >= 0) {
			close(cycles_fd);
		}
#endif
		task_clock_fd = -1;
		cycles_fd = -1;
		opened_tid = -1;
	}

private:
#ifdef __linux__
	static int OpenIndependentHardwareCounter(int64_t tid, uint64_t config) {
		struct perf_event_attr attr;
		memset(&attr, 0, sizeof(attr));
		attr.type = PERF_TYPE_HARDWARE;
		attr.size = sizeof(attr);
		attr.config = config;
		attr.disabled = 0;
		attr.exclude_kernel = 0;
		attr.exclude_hv = 1;
		attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
		return NumericCast<int>(PerfEventOpen(&attr, static_cast<pid_t>(tid), -1, -1, 0));
	}
#endif

	int task_clock_fd = -1;
	int cycles_fd = -1;
	int64_t opened_tid = -1;
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
	auto task_time_enabled =
	    SnapshotDeltaValue(start.task_clock_time_enabled, end.task_clock_time_enabled);
	auto task_time_running =
	    SnapshotDeltaValue(start.task_clock_time_running, end.task_clock_time_running);
	double task_scale = 1.0;
	if (start.task_clock_valid && end.task_clock_valid && task_time_running > 0 &&
	    task_time_enabled > task_time_running) {
		task_scale = static_cast<double>(task_time_enabled) / static_cast<double>(task_time_running);
		counters.scaled = true;
	} else if (start.task_clock_valid && end.task_clock_valid && task_time_running == 0 && task_time_enabled > 0) {
		counters.status += "|task clock never ran during segment";
	} else if (!start.task_clock_valid || !end.task_clock_valid) {
		counters.status += "|task clock unavailable";
	}
	counters.task_clock_ns =
	    ScaleCounterDelta(SnapshotDeltaValue(start.task_clock_ns, end.task_clock_ns), task_scale);

	auto cycles_time_enabled =
	    SnapshotDeltaValue(start.cycles_time_enabled, end.cycles_time_enabled);
	auto cycles_time_running =
	    SnapshotDeltaValue(start.cycles_time_running, end.cycles_time_running);
	double cycles_scale = 1.0;
	if (start.cycles_valid && end.cycles_valid && cycles_time_running > 0 &&
	    cycles_time_enabled > cycles_time_running) {
		cycles_scale = static_cast<double>(cycles_time_enabled) / static_cast<double>(cycles_time_running);
		counters.scaled = true;
	} else if (start.cycles_valid && end.cycles_valid && cycles_time_running == 0 && cycles_time_enabled > 0) {
		counters.status += "|cycles never ran during segment";
	} else if (!start.cycles_valid || !end.cycles_valid) {
		counters.status += "|cycles unavailable";
	}
	if (start.cycles_valid && end.cycles_valid && cycles_time_running > 0) {
		counters.cycles =
		    ScaleCounterDelta(SnapshotDeltaValue(start.cycles, end.cycles), cycles_scale);
	}

	counters.valid = counters.task_clock_ns > 0 || counters.cycles > 0;
	return counters;
}

static void AddPerfCounters(PerfCounters &target, const PerfCounters &delta) {
	target.task_clock_ns += delta.task_clock_ns;
	target.cycles += delta.cycles;
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
	record.task_clock_ns = counters.task_clock_ns;
	record.cycles = counters.cycles;
	record.counters_valid = counters.valid;
	record.counters_scaled = counters.scaled;
	record.counter_status = counters.status;
}

struct QueryEnergyState : public EnergyAttributionQueryHandle {
	ClientContext *context = nullptr;
	EnergyAttributionSettings settings;
	uint64_t query_id = 0;
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	uint64_t start_ns = 0;
	std::map<int, double> uncore_freq_by_socket;
	std::atomic<uint64_t> next_segment_id {1};
	std::atomic<bool> periodic_active {false};
};

struct EnergyThreadState {
	int64_t tid = -1;
	bool hardware_initialized = false;
	bool pinned = false;
	int pinned_cpu = -1;
	int pinned_socket_id = -1;
	int pinned_physical_core_id = -1;
	double pinned_core_freq_hz = 0;
	QueryEnergyState *segment_id_query = nullptr;
	uint64_t segment_id_query_id = 0;
	uint64_t segment_id_query_start_ns = 0;
	uint64_t next_segment_id = 0;
	uint64_t segment_id_limit = 0;

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

	void InitializeHardware() {
		if (hardware_initialized) {
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

static string PipelineLabel(uint64_t pipeline_id) {
	return StringUtil::Format("pipeline_%llu", pipeline_id);
}

static double GetQueryUncoreFrequency(QueryEnergyState &query, int socket_id) {
	if (socket_id < 0) {
		return 0;
	}
	auto entry = query.uncore_freq_by_socket.find(socket_id);
	return entry == query.uncore_freq_by_socket.end() ? 0 : entry->second;
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

static double SegmentCorrectedCycles(const EnergySegmentRecord &record) {
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

struct SocketFrequencyAverages {
	double core_hz = 0;
	double uncore_hz = 0;
};

static SocketFrequencyAverages AverageFrequenciesForSocket(const vector<AttributedSegment> &segments, int socket_id) {
	double core_weighted = 0;
	double core_duration = 0;
	double uncore_weighted = 0;
	double uncore_duration = 0;
	for (idx_t i = 0; i < segments.size(); i++) {
		const auto &segment = segments[i].record;
		if (segment.socket_id != socket_id) {
			continue;
		}
		auto segment_duration = SegmentDuration(segment);
		if (segment.core_freq_hz > 0) {
			core_weighted += segment.core_freq_hz * segment_duration;
			core_duration += segment_duration;
		}
		if (segment.uncore_freq_hz > 0) {
			uncore_weighted += segment.uncore_freq_hz * segment_duration;
			uncore_duration += segment_duration;
		}
	}
	SocketFrequencyAverages result;
	result.core_hz = core_duration > 0 ? core_weighted / core_duration : 0;
	result.uncore_hz =
	    uncore_duration > 0 ? uncore_weighted / uncore_duration : ReadUncoreFrequencyHz(socket_id);
	return result;
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

static vector<RaplSnapshot> InterpolateRaplSnapshots(const vector<RaplSnapshot> &before,
                                                     const vector<RaplSnapshot> &after, uint64_t before_ns,
                                                     uint64_t after_ns, uint64_t boundary_ns) {
	vector<RaplSnapshot> result;
	std::map<int, RaplSnapshot> before_by_socket;
	for (const auto &snapshot : before) {
		before_by_socket[snapshot.socket_id] = snapshot;
	}
	auto fraction = after_ns > before_ns
	                    ? static_cast<double>(MinValue<uint64_t>(MaxValue<uint64_t>(boundary_ns, before_ns), after_ns) -
	                                          before_ns) /
	                          static_cast<double>(after_ns - before_ns)
	                    : 0;
	for (const auto &end : after) {
		auto start_entry = before_by_socket.find(end.socket_id);
		if (start_entry == before_by_socket.end()) {
			result.push_back(end);
			continue;
		}
		const auto &start = start_entry->second;
		RaplSnapshot interpolated = start;
		interpolated.valid = start.valid && end.valid;
		if (interpolated.valid) {
			auto interpolated_delta = static_cast<uint64_t>(static_cast<double>(RaplDeltaUJ(start, end)) * fraction);
			interpolated.energy_uj = start.energy_uj + interpolated_delta;
			if (interpolated.max_energy_range_uj > 0 &&
			    interpolated.energy_uj >= interpolated.max_energy_range_uj) {
				interpolated.energy_uj %= interpolated.max_energy_range_uj;
			}
		}
		result.push_back(interpolated);
	}
	return result;
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

static void PrepareSegmentsForAttribution(vector<AttributedSegment> &segments) {
	ComputeSMTOccupancy(segments);
	for (idx_t i = 0; i < segments.size(); i++) {
		auto &segment = segments[i];
		segment.attribution_status = SegmentStatusBase(segment.record);
		segment.frequency_ratio = 0;
		segment.corrected_cycles = SegmentCorrectedCycles(segment.record);
		auto duration = SegmentDuration(segment.record);
		if (duration > 0 && segment.record.work_units > 0) {
			segment.throughput = static_cast<double>(segment.record.work_units) / duration;
		}
	}
}

static void PrepareProcessActivity(const ProcessActivityWindow &activity, PreparedProcessActivity &prepared) {
	prepared.source = &activity;
	for (auto &socket : prepared.sockets) {
		socket.record_indices.clear();
		socket.corrected_weight_sum = 0;
		socket.runtime_weight_sum = 0;
		socket.monitored_cpu_count = 0;
		socket.helper_count = 0;
		socket.acknowledged_helper_count = 0;
		socket.missing_helper_count = 0;
		socket.boundary_min_ns = 0;
		socket.boundary_max_ns = 0;
		socket.maximum_checkpoint_delay_ns = 0;
		socket.late_helper_count = 0;
		socket.slowest_helper_cpu = -1;
		socket.missing_helper_cpus.clear();
		socket.has_non_idle_activity = false;
		socket.all_pmu_valid = true;
		socket.checkpoint_valid = false;
	}
	auto find_or_create_socket = [&](int socket_id) -> PreparedSocketProcessActivity & {
		auto socket_entry = prepared.socket_to_index.find(socket_id);
		if (socket_entry != prepared.socket_to_index.end()) {
			return prepared.sockets[socket_entry->second];
		}
		auto socket_index = prepared.sockets.size();
		PreparedSocketProcessActivity socket;
		socket.socket_id = socket_id;
		prepared.sockets.push_back(std::move(socket));
		prepared.socket_to_index.emplace(socket_id, socket_index);
		return prepared.sockets[socket_index];
	};
	for (const auto &topology : activity.socket_topology) {
		if (topology.socket_id < 0) {
			continue;
		}
		find_or_create_socket(topology.socket_id).monitored_cpu_count = topology.monitored_cpu_count;
	}
	for (const auto &checkpoint : activity.socket_checkpoints) {
		if (checkpoint.socket_id < 0) {
			continue;
		}
		auto &socket = find_or_create_socket(checkpoint.socket_id);
		socket.helper_count = checkpoint.helper_count;
		socket.acknowledged_helper_count = checkpoint.acknowledged_helper_count;
		socket.missing_helper_count = checkpoint.missing_helper_count;
		socket.boundary_min_ns = checkpoint.boundary_min_ns;
		socket.boundary_max_ns = checkpoint.boundary_max_ns;
		socket.maximum_checkpoint_delay_ns = checkpoint.maximum_checkpoint_delay_ns;
		socket.late_helper_count = checkpoint.late_helper_count;
		socket.slowest_helper_cpu = checkpoint.slowest_helper_cpu;
		socket.missing_helper_cpus = checkpoint.missing_helper_cpus;
		socket.checkpoint_valid = checkpoint.valid;
	}
	for (idx_t i = 0; i < activity.records.size(); i++) {
		const auto &record = activity.records[i];
		if (record.socket_id < 0 || record.runtime_ns == 0) {
			continue;
		}
		auto &socket = find_or_create_socket(record.socket_id);
		auto activity_weight = record.corrected_cycles * record.frequency_ratio;
		socket.record_indices.push_back(i);
		socket.runtime_weight_sum += static_cast<double>(record.runtime_ns);
		if (record.kind != ProcessActivityKind::SYSTEM_IDLE) {
			socket.has_non_idle_activity = true;
			socket.corrected_weight_sum += record.pmu_valid && activity_weight > 0 ? activity_weight : 0;
			socket.all_pmu_valid = socket.all_pmu_valid && record.pmu_valid && activity_weight > 0;
		}
	}
}

static const PreparedSocketProcessActivity *FindPreparedSocketActivity(const PreparedProcessActivity &prepared,
                                                                       int socket_id) {
	auto entry = prepared.socket_to_index.find(socket_id);
	if (entry == prepared.socket_to_index.end()) {
		return nullptr;
	}
	return &prepared.sockets[entry->second];
}

static void UpdateProcessProfile(ProcessEnergyProfileMap &profiles, const ProcessActivityRecord &record,
                                 double active_energy_j, double base_energy_j) {
	ProcessEnergyProfileKey key;
	key.process_start_time_ns = record.process_start_time_ns;
	key.tgid = record.tgid;
	key.socket_id = record.socket_id;
	key.kind = record.kind;
	key.comm = record.comm;
	auto &profile = profiles[key];
	profile.process_start_time_ns = record.process_start_time_ns;
	profile.tgid = record.tgid;
	profile.socket_id = record.socket_id;
	profile.kind = record.kind;
	profile.comm = record.comm;
	profile.sample_count++;
	profile.runtime_ns += record.runtime_ns;
	profile.cycles += record.cycles;
	profile.ref_cycles += record.ref_cycles;
	profile.corrected_cycles += record.corrected_cycles;
	profile.active_energy_j += active_energy_j;
	profile.base_energy_j += base_energy_j;
}

static void AttributeSegmentsByProcess(
    vector<AttributedSegment> &segments, const ProcessActivityWindow &activity,
    const PreparedSocketProcessActivity *prepared_socket, uint32_t duckdb_tgid, SocketEnergyWindow &window,
    vector<AttributedProcessActivity> &process_rows, bool materialize_process_rows,
    vector<DuckDBThreadShare> &thread_shares, std::unordered_map<int64_t, idx_t> &tid_to_thread,
    vector<int32_t> &segment_thread_slots, ProcessEnergyProfileMap &process_profiles) {

	process_rows.clear();
	thread_shares.clear();
	tid_to_thread.clear();
	segment_thread_slots.assign(segments.size(), -1);
	window.process_generation = activity.generation;
	window.process_transition_ns = activity.transition_ns;
	window.process_boundary_min_ns = activity.boundary_min_ns;
	window.process_boundary_max_ns = activity.boundary_max_ns;
	window.process_maximum_checkpoint_delay_ns = activity.maximum_checkpoint_delay_ns;
	window.process_checkpoint_duration_ns = activity.checkpoint_duration_ns;
	window.process_helper_count = activity.helper_count;
	window.process_helper_futex_wake_count = activity.helper_futex_wake_count;
	window.process_acknowledged_helper_count = activity.acknowledged_helper_count;
	window.process_missing_helper_count = activity.missing_helper_count;
	window.process_slowest_helper_cpu = activity.slowest_helper_cpu;
	window.process_missing_helper_cpus = activity.missing_helper_cpus;
	window.process_activity_map_insert_failure = activity.activity_map_insert_failure;
	window.process_missing_counter = activity.missing_counter;
	window.process_invalid_counter_delta = activity.invalid_counter_delta;
	window.process_interrupt_underflow = activity.interrupt_underflow;
	window.process_boundary_split_count = activity.boundary_split_count;
	window.process_boundary_split_cpu_count = activity.boundary_split_cpu_count;
	window.process_boundary_split_runtime_ns = activity.boundary_split_runtime_ns;
	window.process_boundary_split_cycles = activity.boundary_split_cycles;
	window.process_boundary_split_ref_cycles = activity.boundary_split_ref_cycles;
	window.process_late_helper_count = activity.late_helper_count;
	window.process_lost_records = activity.lost_records;
	window.process_lookup_failures = activity.lookup_failures;
	window.process_boundary_corrected = activity.boundary_corrected;
	window.process_status = activity.status;
	auto socket_checkpoint_valid = prepared_socket && prepared_socket->checkpoint_valid;
	if (prepared_socket) {
		window.process_boundary_min_ns = prepared_socket->boundary_min_ns;
		window.process_boundary_max_ns = prepared_socket->boundary_max_ns;
		window.process_maximum_checkpoint_delay_ns = prepared_socket->maximum_checkpoint_delay_ns;
		window.process_helper_count = prepared_socket->helper_count;
		window.process_acknowledged_helper_count = prepared_socket->acknowledged_helper_count;
		window.process_missing_helper_count = prepared_socket->missing_helper_count;
		window.process_slowest_helper_cpu = prepared_socket->slowest_helper_cpu;
		window.process_missing_helper_cpus = prepared_socket->missing_helper_cpus;
		window.process_late_helper_count = prepared_socket->late_helper_count;
		window.process_boundary_corrected = prepared_socket->late_helper_count > 0;
		if (activity.status == "ok" || activity.status == "boundary_corrected") {
			window.process_status = window.process_boundary_corrected ? "boundary_corrected" : "ok";
		}
		if (!socket_checkpoint_valid) {
			auto checkpoint_status = StringUtil::Format(
			    "checkpoint_timeout:%llu/%llu", static_cast<unsigned long long>(prepared_socket->acknowledged_helper_count),
			    static_cast<unsigned long long>(prepared_socket->helper_count));
			if (window.process_status != "ok" && window.process_status != "boundary_corrected" &&
			    !window.process_status.empty()) {
				checkpoint_status += "|" + window.process_status;
			} else if (window.process_boundary_corrected) {
				checkpoint_status += "|boundary_corrected";
			}
			window.process_status = std::move(checkpoint_status);
		}
	}

	if (!prepared_socket || prepared_socket->record_indices.empty() || prepared_socket->runtime_weight_sum <= 0) {
		window.process_attribution_valid = false;
		if (window.process_status.empty() || window.process_status == "ok") {
			window.process_status = "no_process_activity";
		}
		return;
	}

	auto idle_only = !prepared_socket->has_non_idle_activity;
	auto use_time_fallback =
	    !idle_only && (!prepared_socket->all_pmu_valid || prepared_socket->corrected_weight_sum <= 0);
	auto weight_denominator = use_time_fallback || idle_only ? prepared_socket->runtime_weight_sum
	                                                        : prepared_socket->corrected_weight_sum;
	auto base_time_denominator = prepared_socket->runtime_weight_sum;
	window.process_monitored_cpu_count = prepared_socket->monitored_cpu_count;
	window.process_observed_runtime_ns =
	    prepared_socket->runtime_weight_sum > 0
	        ? static_cast<uint64_t>(std::llround(prepared_socket->runtime_weight_sum))
	        : 0;
	window.process_expected_runtime_ns =
	    window.duration_s * static_cast<double>(prepared_socket->monitored_cpu_count) *
	    static_cast<double>(NSEC_PER_SEC);
	window.process_runtime_coverage =
	    window.process_expected_runtime_ns > 0
	        ? prepared_socket->runtime_weight_sum / window.process_expected_runtime_ns
	        : 0;
	auto runtime_coverage_valid = prepared_socket->monitored_cpu_count > 0 &&
	                              window.process_runtime_coverage >= 0.98 &&
	                              window.process_runtime_coverage <= 1.02;
	if (materialize_process_rows) {
		process_rows.reserve(prepared_socket->record_indices.size());
	}
	for (const auto record_index : prepared_socket->record_indices) {
		const auto &record = activity.records[record_index];
		auto activity_weight =
		    use_time_fallback || idle_only
		        ? static_cast<double>(record.runtime_ns)
		        : (record.kind != ProcessActivityKind::SYSTEM_IDLE && record.pmu_valid
		               ? record.corrected_cycles * record.frequency_ratio
		               : 0);
		auto attributed_active_energy_j =
		    weight_denominator > 0 ? window.active_package_energy_j * activity_weight / weight_denominator : 0;
		auto attributed_base_energy_j =
		    base_time_denominator > 0
		        ? window.base_energy_j * static_cast<double>(record.runtime_ns) / base_time_denominator
		        : 0;
		auto duckdb_process = record.tgid == duckdb_tgid;
		window.process_attributed_active_energy_j += attributed_active_energy_j;
		window.process_attributed_base_energy_j += attributed_base_energy_j;
		if (duckdb_process) {
			window.duckdb_process_active_energy_j += attributed_active_energy_j;
			window.duckdb_process_base_energy_j += attributed_base_energy_j;
		}
		if (record.kind == ProcessActivityKind::MEASUREMENT_OVERHEAD) {
			window.measurement_overhead_active_energy_j += attributed_active_energy_j;
			window.measurement_overhead_base_energy_j += attributed_base_energy_j;
		} else if (record.kind == ProcessActivityKind::HOST_IRQ) {
			window.host_irq_active_energy_j += attributed_active_energy_j;
			window.host_irq_base_energy_j += attributed_base_energy_j;
		} else if (record.kind == ProcessActivityKind::HOST_KERNEL) {
			window.host_kernel_active_energy_j += attributed_active_energy_j;
			window.host_kernel_base_energy_j += attributed_base_energy_j;
		} else if (record.kind == ProcessActivityKind::SYSTEM_IDLE) {
			window.system_idle_active_energy_j += attributed_active_energy_j;
			window.system_idle_base_energy_j += attributed_base_energy_j;
		} else if (duckdb_process) {
			auto tid = static_cast<int64_t>(record.tid);
			auto thread_entry = tid_to_thread.find(tid);
			idx_t thread_index;
			if (thread_entry == tid_to_thread.end()) {
				thread_index = thread_shares.size();
				thread_shares.emplace_back();
				tid_to_thread.emplace(tid, thread_index);
			} else {
				thread_index = thread_entry->second;
			}
			auto &thread = thread_shares[thread_index];
			thread.active_energy_j += attributed_active_energy_j;
			thread.base_energy_j += attributed_base_energy_j;
			thread.runtime_ns += record.runtime_ns;
			if (record.pmu_valid && record.frequency_ratio > 0) {
				thread.corrected_cycles += record.corrected_cycles;
				thread.corrected_ref_cycles += record.corrected_cycles / record.frequency_ratio;
				thread.activity_weight += record.corrected_cycles * record.frequency_ratio;
			}
		} else {
			window.external_process_active_energy_j += attributed_active_energy_j;
			window.external_process_base_energy_j += attributed_base_energy_j;
		}
		UpdateProcessProfile(process_profiles, record, attributed_active_energy_j, attributed_base_energy_j);
		if (materialize_process_rows) {
			AttributedProcessActivity row;
			row.record = record;
			row.window_id = window.window_id;
			row.activity_weight = activity_weight;
			row.weight_denominator = weight_denominator;
			row.attributed_active_energy_j = attributed_active_energy_j;
			row.base_time_weight = static_cast<double>(record.runtime_ns);
			row.base_time_denominator_ns = base_time_denominator;
			row.attributed_base_energy_j = attributed_base_energy_j;
			row.duckdb_process = duckdb_process;
			process_rows.push_back(std::move(row));
		}
	}

	for (idx_t i = 0; i < segments.size(); i++) {
		auto &segment = segments[i];
		if (segment.record.socket_id != window.socket_id) {
			continue;
		}
		segment.attributed_active_energy_j = 0;
		segment.attributed_base_energy_j = 0;
		segment.base_time_weight = 0;
		segment.base_time_denominator_s = 0;
		segment.process_attribution_valid = false;
		segment.process_counter_source.clear();
		auto thread_entry = tid_to_thread.find(segment.record.linux_tid);
		if (thread_entry == tid_to_thread.end()) {
			continue;
		}
		auto thread_index = thread_entry->second;
		auto &thread = thread_shares[thread_index];
		auto reference_rate =
		    thread.runtime_ns > 0 ? thread.corrected_ref_cycles / static_cast<double>(thread.runtime_ns) : 0;
		auto thread_frequency_ratio =
		    thread.corrected_ref_cycles > 0 ? thread.corrected_cycles / thread.corrected_ref_cycles : 0;
		auto activity_rate =
		    thread.runtime_ns > 0 ? thread.activity_weight / static_cast<double>(thread.runtime_ns) : 0;
		if (segment.record.cycles > 0 && segment.record.task_clock_ns > 0 && reference_rate > 0) {
			auto estimated_ref_cycles = reference_rate * static_cast<double>(segment.record.task_clock_ns);
			segment.frequency_ratio =
			    estimated_ref_cycles > 0 ? static_cast<double>(segment.record.cycles) / estimated_ref_cycles : 0;
			segment.corrected_cycles = static_cast<double>(segment.record.cycles) * segment.frequency_ratio;
			segment.process_counter_source = "process_ref_cycles_proxy";
			segment.attribution_status += "|process_ref_cycles_proxy";
		} else if (segment.record.task_clock_ns > 0 && activity_rate > 0) {
			segment.frequency_ratio = thread_frequency_ratio;
			segment.corrected_cycles = activity_rate * static_cast<double>(segment.record.task_clock_ns);
			segment.process_counter_source = "process_activity_rate_fallback";
			segment.attribution_status += "|process_activity_rate_fallback";
		}
		segment_thread_slots[i] = NumericCast<int32_t>(thread_index);
		thread.segment_cycle_sum += segment.corrected_cycles;
		thread.segment_task_clock_sum += segment.record.task_clock_ns;
	}

	for (idx_t thread_index = 0; thread_index < thread_shares.size(); thread_index++) {
		auto &thread = thread_shares[thread_index];
		if (thread.runtime_ns == 0 || thread.segment_task_clock_sum == 0) {
			window.duckdb_uninstrumented_active_energy_j += thread.active_energy_j;
			window.duckdb_uninstrumented_base_energy_j += thread.base_energy_j;
			continue;
		}
		auto covered_task_clock = MinValue<uint64_t>(thread.runtime_ns, thread.segment_task_clock_sum);
		auto coverage = static_cast<double>(covered_task_clock) / static_cast<double>(thread.runtime_ns);
		auto covered_active_energy = thread.active_energy_j * coverage;
		auto covered_base_energy = thread.base_energy_j * coverage;
		window.duckdb_uninstrumented_active_energy_j += thread.active_energy_j - covered_active_energy;
		window.duckdb_uninstrumented_base_energy_j += thread.base_energy_j - covered_base_energy;
		auto use_segment_time = use_time_fallback || thread.segment_cycle_sum <= 0;
		auto segment_denominator =
		    use_segment_time ? static_cast<double>(thread.segment_task_clock_sum) : thread.segment_cycle_sum;
		if (segment_denominator <= 0) {
			window.duckdb_uninstrumented_active_energy_j += covered_active_energy;
			window.duckdb_uninstrumented_base_energy_j += covered_base_energy;
			continue;
		}
		thread.covered_active_energy_j = covered_active_energy;
		thread.covered_base_energy_j = covered_base_energy;
		thread.task_clock_coverage = coverage;
		thread.segment_denominator = segment_denominator;
		thread.use_segment_time = use_segment_time;
	}
	for (idx_t i = 0; i < segments.size(); i++) {
		auto thread_slot = segment_thread_slots[i];
		if (thread_slot < 0) {
			continue;
		}
		auto thread_index = static_cast<idx_t>(thread_slot);
		const auto &thread = thread_shares[thread_index];
		if (thread.segment_denominator <= 0) {
			continue;
		}
		auto &segment = segments[i];
		auto use_segment_time = thread.use_segment_time;
		auto segment_weight =
		    use_segment_time ? static_cast<double>(segment.record.task_clock_ns) : segment.corrected_cycles;
		segment.activity_weight = segment_weight;
		segment.weight_denominator = thread.segment_denominator;
		segment.task_clock_coverage = thread.task_clock_coverage;
		segment.attributed_active_energy_j =
		    thread.covered_active_energy_j * segment_weight / thread.segment_denominator;
		segment.base_time_weight =
		    static_cast<double>(segment.record.task_clock_ns) / static_cast<double>(NSEC_PER_SEC);
		segment.base_time_denominator_s =
		    static_cast<double>(thread.segment_task_clock_sum) / static_cast<double>(NSEC_PER_SEC);
		segment.attributed_base_energy_j =
		    thread.segment_task_clock_sum > 0
		        ? thread.covered_base_energy_j * static_cast<double>(segment.record.task_clock_ns) /
		              static_cast<double>(thread.segment_task_clock_sum)
		        : 0;
		segment.process_attribution_valid = activity.valid && socket_checkpoint_valid && !use_time_fallback;
		segment.attribution_status += use_segment_time ? "|process_task_clock_fallback" : "|process_corrected_cycles";
		window.duckdb_instrumented_active_energy_j += segment.attributed_active_energy_j;
		window.duckdb_instrumented_base_energy_j += segment.attributed_base_energy_j;
	}
	window.process_conservation_error_j =
	    window.active_package_energy_j - window.process_attributed_active_energy_j;
	window.process_base_conservation_error_j =
	    window.base_energy_j - window.process_attributed_base_energy_j;
	window.duckdb_hierarchy_error_j =
	    window.duckdb_process_active_energy_j -
	    (window.duckdb_instrumented_active_energy_j + window.duckdb_uninstrumented_active_energy_j +
	     window.measurement_overhead_active_energy_j);
	window.duckdb_base_hierarchy_error_j =
	    window.duckdb_process_base_energy_j -
	    (window.duckdb_instrumented_base_energy_j + window.duckdb_uninstrumented_base_energy_j +
	     window.measurement_overhead_base_energy_j);
	auto process_tolerance = std::max(1e-12, window.active_package_energy_j * 1e-9);
	auto process_base_tolerance = std::max(1e-12, window.base_energy_j * 1e-9);
	auto duckdb_tolerance = std::max(1e-12, window.duckdb_process_active_energy_j * 1e-9);
	auto duckdb_base_tolerance = std::max(1e-12, window.duckdb_process_base_energy_j * 1e-9);
	window.process_attribution_valid = window.rapl_valid && window.base_power_valid && activity.valid &&
	                                   socket_checkpoint_valid &&
	                                   !use_time_fallback && runtime_coverage_valid &&
	                                   std::abs(window.process_conservation_error_j) <= process_tolerance &&
	                                   std::abs(window.process_base_conservation_error_j) <= process_base_tolerance &&
	                                   std::abs(window.duckdb_hierarchy_error_j) <= duckdb_tolerance &&
	                                   std::abs(window.duckdb_base_hierarchy_error_j) <= duckdb_base_tolerance;
	if (use_time_fallback) {
		window.process_status += "|socket_task_clock_fallback";
	}
	if (idle_only) {
		window.process_status += "|socket_idle_active_residual";
	}
	if (!runtime_coverage_valid) {
		window.process_status += "|socket_runtime_coverage_invalid";
	}
	if (std::abs(window.process_base_conservation_error_j) > process_base_tolerance) {
		window.process_status += "|process_base_conservation_failed";
	}
	if (std::abs(window.duckdb_hierarchy_error_j) > duckdb_tolerance) {
		window.process_status += "|duckdb_hierarchy_conservation_failed";
	}
	if (std::abs(window.duckdb_base_hierarchy_error_j) > duckdb_base_tolerance) {
		window.process_status += "|duckdb_base_hierarchy_conservation_failed";
	}
	if (!window.rapl_valid || !window.base_power_valid) {
		window.process_status += "|energy_input_invalid";
	}
	if (!window.process_attribution_valid) {
		for (auto &segment : segments) {
			if (segment.record.socket_id == window.socket_id) {
				segment.process_attribution_valid = false;
			}
		}
	}
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

static void UpdateLifecycleProfileTimeRange(LifecyclePhaseProfileAggregate &profile, uint64_t start_ns,
                                            uint64_t end_ns) {
	if (start_ns > 0) {
		profile.start_ns = profile.start_ns == 0 ? start_ns : MinValue<uint64_t>(profile.start_ns, start_ns);
	}
	if (end_ns > 0) {
		profile.end_ns = MaxValue<uint64_t>(profile.end_ns, end_ns);
	}
}

static string JoinUInt64List(const vector<uint64_t> &values) {
	std::ostringstream out;
	for (idx_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			out << ";";
		}
		out << values[i];
	}
	return out.str();
}

static void WriteLifecyclePhaseProfilesCSV(const string &path, const vector<LifecyclePhaseProfileAggregate> &profiles) {
	std::ofstream out(path.c_str());
	out << "query_id,lifecycle_group_id,phase_kind,owner_pipeline_id,lifecycle_member_count,member_pipeline_ids,"
	       "hardware_mode,core_freq_hz,uncore_freq_hz,smt_occupancy,sample_count,start_ns,end_ns,elapsed_s,total_duration_s,"
	       "attributed_active_energy_j,base_time_share_energy_j,charged_total_energy_j,active_power_w,charged_power_w,"
	       "corrected_cycles\n";
	for (idx_t i = 0; i < profiles.size(); i++) {
		const auto &p = profiles[i];
		double elapsed_s =
		    p.end_ns > p.start_ns ? static_cast<double>(p.end_ns - p.start_ns) / static_cast<double>(NSEC_PER_SEC) : 0;
		double charged_total_energy = p.attributed_active_energy_j + p.base_time_share_energy_j;
		double active_power = p.total_duration_s > 0 ? p.attributed_active_energy_j / p.total_duration_s : 0;
		double charged_power = p.total_duration_s > 0 ? charged_total_energy / p.total_duration_s : 0;
		out << p.query_id << "," << p.lifecycle_group_id << "," << EnergySegmentPhaseToString(p.phase) << ","
		    << p.owner_pipeline_id << "," << p.lifecycle_member_count << ","
		    << CsvEscape(JoinUInt64List(p.member_pipeline_ids)) << ","
		    << PeriodicHardwareProfileModeToString(p.hardware_mode) << "," << p.core_freq_hz << ","
		    << p.uncore_freq_hz << "," << p.smt_occupancy << "," << p.sample_count << "," << p.start_ns << ","
		    << p.end_ns << "," << elapsed_s << "," << p.total_duration_s << "," << p.attributed_active_energy_j << ","
		    << p.base_time_share_energy_j << "," << charged_total_energy << "," << active_power << ","
		    << charged_power << "," << p.corrected_cycles << "\n";
	}
}

static void WriteLifecycleGroupMembersCSV(const string &path, const vector<LifecyclePhaseProfileAggregate> &profiles) {
	std::ofstream out(path.c_str());
	out << "query_id,lifecycle_group_id,phase_kind,hardware_mode,core_freq_hz,uncore_freq_hz,owner_pipeline_id,"
	       "lifecycle_member_count,member_pipeline_id\n";
	for (idx_t i = 0; i < profiles.size(); i++) {
		const auto &p = profiles[i];
		for (auto member_pipeline_id : p.member_pipeline_ids) {
			out << p.query_id << "," << p.lifecycle_group_id << "," << EnergySegmentPhaseToString(p.phase) << ","
			    << PeriodicHardwareProfileModeToString(p.hardware_mode) << "," << p.core_freq_hz << ","
			    << p.uncore_freq_hz << "," << p.owner_pipeline_id << "," << p.lifecycle_member_count << ","
			    << member_pipeline_id << "\n";
		}
	}
}

static void WriteSystemEnergyProfilesCSV(
    const string &path, const std::map<EnergySystemCategory, SystemEnergyAggregate> &profiles) {
	std::ofstream out(path.c_str());
	out << "system_category,sample_count,duration_s,attributed_active_energy_j,base_time_share_energy_j,"
	       "charged_total_energy_j,corrected_cycles\n";
	for (const auto &entry : profiles) {
		const auto &profile = entry.second;
		out << EnergySystemCategoryToString(profile.category) << "," << profile.sample_count << ","
		    << profile.duration_s << "," << profile.attributed_active_energy_j << ","
		    << profile.base_time_share_energy_j << ","
		    << profile.attributed_active_energy_j + profile.base_time_share_energy_j << ","
		    << profile.corrected_cycles << "\n";
	}
}

static void WriteEfficiencyProfilesCSV(const string &path, const vector<PipelineEfficiencyAggregate> &profiles,
                                       bool include_query_lifecycle = false) {
	std::ofstream out(path.c_str());
	out << "query_id,";
	if (include_query_lifecycle) {
		out << "start_ns,end_ns,elapsed_s,";
	}
	out << "pipeline_id,hardware_mode,core_freq_hz,uncore_freq_hz,smt_occupancy,sample_count,"
	       "valid_input_sample_count,energy_only_sample_count,total_input_rows,total_input_chunks,total_duration_s,"
	       "input_duration_s,total_duration_throughput_rows_per_s,input_segment_throughput_rows_per_s,"
	       "attributed_active_energy_j,input_attributed_active_energy_j,base_time_share_energy_j,"
	       "input_base_time_share_energy_j,charged_total_energy_j,input_charged_total_energy_j,active_power_w,"
	       "charged_power_w,active_throughput_per_watt_rows_per_j,charged_throughput_per_watt_rows_per_j,"
	       "active_energy_per_row_j,charged_energy_per_row_j,corrected_cycles,corrected_cycles_per_row,"
	       "input_corrected_cycles_per_row,energy_only_duration_s,energy_only_active_energy_j,"
	       "energy_only_base_time_share_energy_j,energy_only_charged_energy_j,allocated_initialize_energy_j,"
	       "allocated_prepare_finish_energy_j,allocated_finish_energy_j,allocated_lifecycle_energy_j,"
	       "total_with_lifecycle_energy_j,allocated_initialize_active_energy_j,"
	       "allocated_initialize_base_energy_j,allocated_prepare_finish_active_energy_j,"
	       "allocated_prepare_finish_base_energy_j,allocated_finish_active_energy_j,"
	       "allocated_finish_base_energy_j,allocated_lifecycle_active_energy_j,"
	       "allocated_lifecycle_base_energy_j,total_with_lifecycle_active_energy_j,"
	       "total_with_lifecycle_base_energy_j\n";
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
		double allocated_initialize_energy =
		    p.allocated_initialize_active_energy_j + p.allocated_initialize_base_energy_j;
		double allocated_prepare_finish_energy =
		    p.allocated_prepare_finish_active_energy_j + p.allocated_prepare_finish_base_energy_j;
		double allocated_finish_energy = p.allocated_finish_active_energy_j + p.allocated_finish_base_energy_j;
		double allocated_lifecycle_active_energy =
		    p.allocated_initialize_active_energy_j + p.allocated_prepare_finish_active_energy_j +
		    p.allocated_finish_active_energy_j;
		double allocated_lifecycle_base_energy =
		    p.allocated_initialize_base_energy_j + p.allocated_prepare_finish_base_energy_j +
		    p.allocated_finish_base_energy_j;
		double allocated_lifecycle_energy = allocated_lifecycle_active_energy + allocated_lifecycle_base_energy;
		double elapsed_s =
		    p.end_ns > p.start_ns ? static_cast<double>(p.end_ns - p.start_ns) / static_cast<double>(NSEC_PER_SEC) : 0;
		out << p.query_id << ",";
		if (include_query_lifecycle) {
			out << p.start_ns << "," << p.end_ns << "," << elapsed_s << ",";
		}
		out << p.pipeline_id << "," << PeriodicHardwareProfileModeToString(p.hardware_mode) << "," << p.core_freq_hz
		    << "," << p.uncore_freq_hz << "," << p.smt_occupancy << "," << p.sample_count << ","
		    << p.valid_input_sample_count << "," << p.energy_only_sample_count << "," << p.total_input_rows << ","
		    << p.total_input_chunks << "," << p.total_duration_s << "," << p.input_duration_s << ","
		    << total_throughput << "," << input_throughput << "," << p.attributed_active_energy_j << ","
		    << p.input_attributed_active_energy_j << "," << p.base_time_share_energy_j << ","
		    << p.input_base_time_share_energy_j << "," << charged_total_energy << "," << input_charged_total_energy
		    << "," << active_power << "," << charged_power << "," << active_throughput_per_watt << ","
		    << charged_throughput_per_watt << "," << active_energy_per_row << "," << charged_energy_per_row << ","
		    << p.corrected_cycles << "," << corrected_cycles_per_row << "," << input_corrected_cycles_per_row << ","
		    << p.energy_only_duration_s << "," << p.energy_only_active_energy_j << ","
		    << p.energy_only_base_time_share_energy_j << "," << energy_only_charged_energy << ","
		    << allocated_initialize_energy << "," << allocated_prepare_finish_energy << ","
		    << allocated_finish_energy << "," << allocated_lifecycle_energy << ","
		    << charged_total_energy + allocated_lifecycle_energy << "," << p.allocated_initialize_active_energy_j
		    << "," << p.allocated_initialize_base_energy_j << "," << p.allocated_prepare_finish_active_energy_j << ","
		    << p.allocated_prepare_finish_base_energy_j << "," << p.allocated_finish_active_energy_j << ","
		    << p.allocated_finish_base_energy_j << "," << allocated_lifecycle_active_energy << ","
		    << allocated_lifecycle_base_energy << ","
		    << p.attributed_active_energy_j + allocated_lifecycle_active_energy << ","
		    << p.base_time_share_energy_j + allocated_lifecycle_base_energy << "\n";
	}
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
	result.task_clock_ns = ScaleByFraction(record.task_clock_ns, fraction);
	result.cycles = ScaleByFraction(record.cycles, fraction);
	return result;
}

static void AddSegmentToEfficiencyAggregate(PipelineEfficiencyAggregate &profile, const AttributedSegment &segment,
                                            bool initialize_pipeline_identity,
                                            const PeriodicPipelineProfileKey *hardware_key) {
	auto duration = SegmentDuration(segment.record);
	if (!segment.record.counters_valid || !segment.record.hardware_state_stable || duration <= 0) {
		return;
	}
	if (profile.sample_count == 0) {
		profile.query_id = segment.record.query_id;
		if (initialize_pipeline_identity) {
			profile.pipeline_id = segment.record.pipeline_id;
		}
		if (hardware_key) {
			profile.hardware_mode = hardware_key->hardware_mode;
			profile.core_freq_hz = static_cast<double>(hardware_key->core_frequency_khz) * 1000.0;
			profile.uncore_freq_hz = static_cast<double>(hardware_key->uncore_frequency_khz) * 1000.0;
		}
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
                                                          bool initialize_pipeline_identity,
                                                          const PeriodicPipelineProfileKey *hardware_key) {
	auto duration = SegmentDuration(record);
	if (!record.counters_valid || !record.hardware_state_stable || duration <= 0 || record.work_units == 0) {
		return;
	}
	if (profile.sample_count == 0 && profile.valid_input_sample_count == 0) {
		profile.query_id = record.query_id;
		if (initialize_pipeline_identity) {
			profile.pipeline_id = record.pipeline_id;
		}
		if (hardware_key) {
			profile.hardware_mode = hardware_key->hardware_mode;
			profile.core_freq_hz = static_cast<double>(hardware_key->core_frequency_khz) * 1000.0;
			profile.uncore_freq_hz = static_cast<double>(hardware_key->uncore_frequency_khz) * 1000.0;
		}
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
		target.hardware_mode = source.hardware_mode;
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
	target.allocated_initialize_active_energy_j += source.allocated_initialize_active_energy_j;
	target.allocated_initialize_base_energy_j += source.allocated_initialize_base_energy_j;
	target.allocated_prepare_finish_active_energy_j += source.allocated_prepare_finish_active_energy_j;
	target.allocated_prepare_finish_base_energy_j += source.allocated_prepare_finish_base_energy_j;
	target.allocated_finish_active_energy_j += source.allocated_finish_active_energy_j;
	target.allocated_finish_base_energy_j += source.allocated_finish_base_energy_j;
}

static bool AddLifecycleSegmentToProfile(LifecyclePhaseProfileAggregate &profile, const AttributedSegment &segment,
                                         const PeriodicLifecycleProfileKey &hardware_key) {
	auto duration = SegmentDuration(segment.record);
	if (!segment.record.counters_valid || !segment.record.hardware_state_stable || duration <= 0) {
		return false;
	}
	if (profile.sample_count == 0) {
		profile.query_id = segment.record.query_id;
		profile.lifecycle_group_id = segment.record.lifecycle_group_id;
		profile.owner_pipeline_id = segment.record.owner_pipeline_id;
		profile.lifecycle_member_count = segment.record.lifecycle_member_count;
		profile.phase = segment.record.phase;
		profile.hardware_mode = hardware_key.hardware_mode;
		profile.core_freq_hz = static_cast<double>(hardware_key.core_frequency_khz) * 1000.0;
		profile.uncore_freq_hz = static_cast<double>(hardware_key.uncore_frequency_khz) * 1000.0;
		profile.smt_occupancy = segment.record.smt_occupancy;
		profile.member_pipeline_ids = segment.record.lifecycle_member_pipeline_ids;
	}
	UpdateLifecycleProfileTimeRange(profile, segment.record.start_ns, segment.record.end_ns);
	profile.sample_count++;
	profile.total_duration_s += duration;
	profile.attributed_active_energy_j += segment.attributed_active_energy_j;
	profile.base_time_share_energy_j += segment.attributed_base_energy_j;
	profile.corrected_cycles += segment.corrected_cycles;
	return true;
}

static void AddAllocatedLifecycleEnergy(PipelineEfficiencyAggregate &profile, const AttributedSegment &segment,
                                        uint64_t pipeline_id, double allocated_active_energy_j,
                                        double allocated_base_energy_j,
                                        const PeriodicPipelineProfileKey &hardware_key) {
	if (profile.sample_count == 0 && profile.valid_input_sample_count == 0 && profile.energy_only_sample_count == 0) {
		profile.query_id = segment.record.query_id;
		profile.pipeline_id = pipeline_id;
		profile.hardware_mode = hardware_key.hardware_mode;
		profile.core_freq_hz = static_cast<double>(hardware_key.core_frequency_khz) * 1000.0;
		profile.uncore_freq_hz = static_cast<double>(hardware_key.uncore_frequency_khz) * 1000.0;
		profile.smt_occupancy = segment.record.smt_occupancy;
	}
	UpdateEfficiencyAggregateTimeRange(profile, segment.record.start_ns, segment.record.end_ns);
	switch (segment.record.phase) {
	case EnergySegmentPhase::INITIALIZE:
		profile.allocated_initialize_active_energy_j += allocated_active_energy_j;
		profile.allocated_initialize_base_energy_j += allocated_base_energy_j;
		break;
	case EnergySegmentPhase::PREPARE_FINISH:
		profile.allocated_prepare_finish_active_energy_j += allocated_active_energy_j;
		profile.allocated_prepare_finish_base_energy_j += allocated_base_energy_j;
		break;
	case EnergySegmentPhase::FINISH:
		profile.allocated_finish_active_energy_j += allocated_active_energy_j;
		profile.allocated_finish_base_energy_j += allocated_base_energy_j;
		break;
	case EnergySegmentPhase::INTERNAL:
	case EnergySegmentPhase::EXECUTE:
	default:
		break;
	}
}

struct PeriodicPendingWindow {
	SocketEnergyWindow window;
	vector<AttributedSegment> segments;
	shared_ptr<ProcessActivityWindow> process_activity;
};

struct SchedulerWorkerRegistration {
	DatabaseInstance *db = nullptr;
	uint64_t worker_id = 0;
	int64_t linux_tid = -1;
	int logical_cpu = -1;
	uint64_t registered_ns = 0;
	bool active = false;
};

struct PendingHardwareEnergyObservation {
	uint64_t template_id = 0;
	uint64_t scale_factor = 0;
	PipelineProfileIdentity pipeline_identity;
	QueryRequestHardwareConfiguration hardware;
	uint64_t duration_ns = 0;
	double attributed_active_energy_j = 0;
	double attributed_base_energy_j = 0;
	bool hardware_state_stable = false;
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

enum class PeriodicSampleReason : uint8_t {
	ACTIVE_PERIODIC,
	IDLE_PERIODIC,
	ACTIVITY_TRANSITION,
	SHUTDOWN
};

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

struct PeriodicWorkerSegmentState {
	int64_t linux_tid = -1;
	mutex lock;
	PerfEventGroup perf_group;
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
		bool should_start = settings_p.enabled;
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
		auto previous_active_queries = active_query_count.fetch_add(1, std::memory_order_relaxed);
		if (adaptive_sampling_enabled && previous_active_queries == 0) {
			activity_transition_generation++;
			cv.notify_all();
		}
	}

	void RegisterPipelineMetadata(uint64_t query_id, uint64_t pipeline_id, string pipeline_signature) {
		std::unique_lock<mutex> guard(lock);
		if (!running) {
			return;
		}
		pipeline_signatures.emplace(std::make_pair(query_id, pipeline_id), std::move(pipeline_signature));
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
				if (DecrementActiveQueryCount() && adaptive_sampling_enabled) {
					activity_transition_generation++;
				}
			}
			auto &query_profile = query_profiles[query->query_id];
			query_profile.query_id = query->query_id;
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
			worker_state->last_perf = ReadWorkerPerfSnapshotLocked(*worker_state);
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

	bool BeginSystemSegment(DatabaseInstance &db, EnergySegmentRecord record) {
		if (!runtime_active.load(std::memory_order_acquire)) {
			return false;
		}
		{
			lock_guard<mutex> guard(lock);
			if (!running || active_db != &db) {
				return false;
			}
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
			worker_state->query.reset();
			worker_state->record = std::move(record);
			worker_state->last_boundary_ns = worker_state->record.start_ns;
			worker_state->accumulated_counters = PerfCounters();
			worker_state->last_perf = ReadWorkerPerfSnapshotLocked(*worker_state);
			worker_state->active = true;
			worker_state->active_atomic.store(true, std::memory_order_release);
		}
		if (became_active) {
			AddActiveWorkerState(worker_state);
		}
		EnqueueSegmentBatch(std::move(slices), EnergySegmentRecord(), false);
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
	string PipelineSignatureLocked(uint64_t query_id, uint64_t pipeline_id,
	                              EnergySystemCategory category = EnergySystemCategory::NONE) const {
		if (category != EnergySystemCategory::NONE) {
			return EnergySystemCategoryToString(category);
		}
		auto entry = pipeline_signatures.find(std::make_pair(query_id, pipeline_id));
		return entry == pipeline_signatures.end() ? PipelineLabel(pipeline_id) : entry->second;
	}

	static bool SettingsMatch(const EnergyAttributionSettings &lhs, const EnergyAttributionSettings &rhs) {
		return lhs.enabled == rhs.enabled && lhs.export_enabled == rhs.export_enabled &&
		       lhs.debug_export_enabled == rhs.debug_export_enabled && lhs.period_ms == rhs.period_ms &&
		       lhs.idle_period_ms == rhs.idle_period_ms &&
		       lhs.output_dir == rhs.output_dir && lhs.base_power_path == rhs.base_power_path;
	}

	void StartLocked(DatabaseInstance &db, const EnergyAttributionSettings &settings_p) {
		active_db = &db;
		settings = settings_p;
		period_ms = MaxValue<uint64_t>(1, settings.period_ms);
		effective_idle_period_ms =
		    settings.idle_period_ms == 0 ? period_ms : MaxValue(period_ms, settings.idle_period_ms);
		adaptive_sampling_enabled = effective_idle_period_ms > period_ms;
		run_start_ns = TimestampNs();
		next_window_id = 1;
		active_query_count.store(0, std::memory_order_relaxed);
		activity_transition_generation = 0;
		active_periodic_sample_count = 0;
		idle_periodic_sample_count = 0;
		activity_transition_sample_count = 0;
		shutdown_sample_count = 0;
		max_active_periodic_interval_ns = 0;
		max_idle_periodic_interval_ns = 0;
		stop_requested = false;
		running = true;
		finalized_window_count = 0;
		attributed_segment_count = 0;
		process_window_count = 0;
		process_valid_window_count = 0;
		process_checkpoint_timeout_generation_count = 0;
		next_runtime_overhead_id.store(1, std::memory_order_relaxed);
		total_active_conservation_error_j = 0;
		total_base_conservation_error_j = 0;
		total_package_gap_j = 0;
		total_duckdb_hierarchy_error_j = 0;
		total_duckdb_base_hierarchy_error_j = 0;
		output_opened = false;
		observed_query_activity.store(false, std::memory_order_relaxed);
		queries.clear();
		query_active.clear();
		pipeline_signatures.clear();
		pending_windows.clear();
		pending_window_count.store(0, std::memory_order_relaxed);
		pending_segment_records.clear();
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
		scratch_process_rows.clear();
		scratch_hardware_energy_observations.clear();
		scratch_prepared_process_activity.source = nullptr;
		scratch_process_thread_shares.clear();
		scratch_process_tid_to_thread.clear();
		scratch_process_tid_to_thread.reserve(64);
		scratch_segment_thread_slots.clear();
		scratch_prepared_process_activity.socket_to_index.reserve(8);
		scratch_rapl_snapshots.clear();
		scratch_pipeline_profile_updates.clear();
		scratch_query_profile_updates.clear();
		for (auto &entry : scratch_pending_windows_by_socket) {
			entry.second.clear();
		}
		scratch_segment_counts_by_socket.clear();
		scratch_segment_counts_by_socket.reserve(8);
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
		lifecycle_phase_profiles.clear();
		query_profiles.clear();
		system_energy_profiles.clear();
		process_energy_profiles.clear();
		process_energy_profiles.reserve(512);
		runtime_overhead.clear();
		runtime_overhead_stats = PeriodicRuntimeOverheadStats();
		periodic_runtime_overhead_stats = PeriodicRuntimeOverheadStats();
		base_power_entries.clear();
		base_power_loaded = LoadBasePowerCSVCached(settings.base_power_path, base_power_entries);
		uncore_freq_by_socket = BuildQueryUncoreFrequencyCache(true);
		auto base_dir = settings.output_dir.empty() ? string(DEFAULT_OUTPUT_DIR) : settings.output_dir;
		output_dir = base_dir + "/" + StringUtil::Format("periodic_run_%llu", run_start_ns);
		process_collector = ProcessActivityCollector::Create();
		if (!process_collector || !process_collector->Start(true)) {
			process_collector_status =
			    process_collector ? process_collector->LastError() : "process attribution was not compiled";
			process_collector.reset();
			running = false;
			stop_requested = false;
			throw InvalidInputException("Process energy attribution is unavailable: %s", process_collector_status);
		}
		process_collector_status = "ok";
		runtime_generation.fetch_add(1, std::memory_order_acq_rel);
		runtime_active.store(true, std::memory_order_release);
		worker = std::thread([this]() { Run(); });
	}

	void Run() {
		using SampleClock = std::chrono::steady_clock;
		last_sample_ns = TimestampNs();
		rapl.Read(last_rapl);
		uint64_t observed_transition_generation;
		bool queries_active;
		{
			std::unique_lock<mutex> guard(lock);
			observed_transition_generation = activity_transition_generation;
			queries_active = active_query_count.load(std::memory_order_relaxed) > 0;
		}
		auto current_period_ms = queries_active ? period_ms : effective_idle_period_ms;
		auto next_deadline = SampleClock::now() + std::chrono::milliseconds(current_period_ms);
		while (true) {
			bool should_stop = false;
			bool activity_transition = false;
			{
				std::unique_lock<mutex> guard(lock);
				cv.wait_until(guard, next_deadline, [&]() {
					return stop_requested ||
					       (adaptive_sampling_enabled &&
					        activity_transition_generation != observed_transition_generation);
				});
				should_stop = stop_requested;
				activity_transition = adaptive_sampling_enabled &&
				                      activity_transition_generation != observed_transition_generation;
				observed_transition_generation = activity_transition_generation;
				queries_active = active_query_count.load(std::memory_order_relaxed) > 0;
			}
			auto sample_started = SampleClock::now();
			auto previous_sample_ns = last_sample_ns;
			auto sample_reason = should_stop
			                         ? PeriodicSampleReason::SHUTDOWN
			                         : activity_transition
			                               ? PeriodicSampleReason::ACTIVITY_TRANSITION
			                               : queries_active ? PeriodicSampleReason::ACTIVE_PERIODIC
			                                                : PeriodicSampleReason::IDLE_PERIODIC;
			if (active_db) {
				EnergySystemSegmentScope energy_scope(*active_db, EnergySystemCategory::ENERGY_SAMPLER);
				SampleOnce();
			} else {
				SampleOnce();
			}
			RecordSampleCadence(sample_reason, DurationNs(last_sample_ns, previous_sample_ns));
			if (should_stop) {
				ShutdownFinalize();
				WriteFinalOutputs();
				std::unique_lock<mutex> guard(lock);
				CloseOutputFilesLocked();
				queries.clear();
				query_active.clear();
				pipeline_signatures.clear();
				pending_windows.clear();
				pending_window_count.store(0, std::memory_order_relaxed);
				pending_segment_records.clear();
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
					scratch_process_rows.clear();
					scratch_hardware_energy_observations.clear();
					scratch_prepared_process_activity.source = nullptr;
					scratch_process_thread_shares.clear();
					scratch_process_tid_to_thread.clear();
					scratch_segment_thread_slots.clear();
				scratch_rapl_snapshots.clear();
				scratch_pipeline_profile_updates.clear();
				scratch_query_profile_updates.clear();
				for (auto &entry : scratch_pending_windows_by_socket) {
					entry.second.clear();
				}
				scratch_segment_counts_by_socket.clear();
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
				if (process_collector) {
					process_collector->Stop();
					process_collector.reset();
				}
				active_db = nullptr;
				running = false;
				runtime_active.store(false, std::memory_order_release);
				stop_requested = false;
				cv.notify_all();
				break;
			}
			current_period_ms = queries_active ? period_ms : effective_idle_period_ms;
			auto now = SampleClock::now();
			if (activity_transition) {
				next_deadline = sample_started + std::chrono::milliseconds(current_period_ms);
				if (next_deadline <= now) {
					next_deadline = now;
				}
			} else {
				next_deadline += std::chrono::milliseconds(current_period_ms);
				if (next_deadline <= now) {
					next_deadline = now + std::chrono::milliseconds(current_period_ms);
				}
			}
		}
	}

	void RecordSampleCadence(PeriodicSampleReason reason, uint64_t interval_ns) {
		switch (reason) {
		case PeriodicSampleReason::ACTIVE_PERIODIC:
			active_periodic_sample_count++;
			max_active_periodic_interval_ns = MaxValue(max_active_periodic_interval_ns, interval_ns);
			break;
		case PeriodicSampleReason::IDLE_PERIODIC:
			idle_periodic_sample_count++;
			max_idle_periodic_interval_ns = MaxValue(max_idle_periodic_interval_ns, interval_ns);
			break;
		case PeriodicSampleReason::ACTIVITY_TRANSITION:
			activity_transition_sample_count++;
			break;
		case PeriodicSampleReason::SHUTDOWN:
			shutdown_sample_count++;
			break;
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
		AddRuntimeOverheadStats(runtime_overhead_stats, record);
		if (record.event == "periodic_sample") {
			AddRuntimeOverheadStats(periodic_runtime_overhead_stats, record);
		}
		if (settings.export_enabled && settings.debug_export_enabled) {
			runtime_overhead.push_back(record);
		}
	}

	bool DecrementActiveQueryCount() {
		auto count = active_query_count.load(std::memory_order_relaxed);
		while (count > 0 && !active_query_count.compare_exchange_weak(count, count - 1, std::memory_order_relaxed,
		                                                              std::memory_order_relaxed)) {
		}
		return count == 1;
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
		return settings.debug_export_enabled ? overhead : nullptr;
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

	EnergyPerfSnapshot ReadWorkerPerfSnapshotLocked(PeriodicWorkerSegmentState &worker_state) {
		EnergyPerfSnapshot snapshot;
		if (worker_state.perf_open) {
			return worker_state.perf_group.ReadSnapshot();
		}
		worker_state.perf_open = worker_state.perf_group.Open(worker_state.linux_tid);
		worker_state.perf_status = worker_state.perf_open ? "process" : worker_state.perf_group.ReadSnapshot().status;
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
		slice.template_id = base.template_id;
		slice.scale_factor = base.scale_factor;
		slice.pipeline_id = base.pipeline_id;
		slice.pipeline_signature_hash = base.pipeline_signature_hash;
		slice.phase = base.phase;
		slice.system_category = base.system_category;
		slice.lifecycle_group_id = base.lifecycle_group_id;
		slice.owner_pipeline_id = base.owner_pipeline_id;
		slice.lifecycle_member_count = base.lifecycle_member_count;
		slice.lifecycle_member_pipeline_ids = base.lifecycle_member_pipeline_ids;
		slice.start_ns = slice_start_ns;
		slice.end_ns = slice_end_ns;
		slice.duration_s = static_cast<double>(slice_end_ns - slice_start_ns) / static_cast<double>(NSEC_PER_SEC);
		slice.work_units = 0;
		slice.tuples = 0;
		slice.chunks = 0;
		slice.core_freq_hz = base.core_freq_hz;
		slice.uncore_freq_hz = base.uncore_freq_hz;
		slice.target_core_frequency_khz = base.target_core_frequency_khz;
		slice.target_uncore_frequency_khz = base.target_uncore_frequency_khz;
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
		PerfCounters counters;
		if (worker_state.last_perf.valid) {
			auto perf_start_ns = DetailTimestamp(detail_overhead);
			auto end_perf = ReadWorkerPerfSnapshotLocked(worker_state);
			DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::snapshot_perf_read_ns, perf_start_ns);
			counters = DeltaPerfCounters(worker_state.last_perf, end_perf);
			worker_state.last_perf = end_perf;
		} else {
			counters.status =
			    worker_state.last_perf.status.empty() ? worker_state.perf_status : worker_state.last_perf.status;
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
			if (record.system_category != EnergySystemCategory::NONE) {
				continue;
			}
			auto pipeline_key = BuildPeriodicPipelineProfileKey(record);
			AddThroughputObservationToEfficiencyAggregate(
			    scratch_pipeline_profile_updates[pipeline_key], record, true, &pipeline_key);
			AddThroughputObservationToEfficiencyAggregate(scratch_query_profile_updates[record.query_id], record,
			                                              false, nullptr);
			if (active_db && record.template_id != 0 && record.scale_factor != 0 && record.pipeline_id != 0 &&
			    record.pipeline_signature_hash != 0 && record.target_core_frequency_khz != 0 &&
			    record.target_uncore_frequency_khz != 0 && record.work_units > 0 && record.end_ns > record.start_ns) {
				PipelineProfileIdentity identity;
				identity.valid = true;
				identity.pipeline_id = record.pipeline_id;
				identity.pipeline_signature_hash = record.pipeline_signature_hash;
				QueryRequestHardwareConfiguration hardware;
				hardware.core_frequency_khz = record.target_core_frequency_khz;
				hardware.uncore_frequency_khz = record.target_uncore_frequency_khz;
				active_db->GetQueryRequestProfileStore().RecordPipelineHardwareThroughput(
				    record.template_id, record.scale_factor, identity, hardware, static_cast<double>(record.work_units),
				    record.end_ns - record.start_ns, record.hardware_state_stable);
			}
		}
		DetailAddSince(detail_overhead, &PeriodicRuntimeOverheadRecord::closed_segment_local_aggregate_ns,
		               phase_start_ns);
		phase_start_ns = DetailTimestamp(detail_overhead);
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
		ReservePendingWindowSegmentCapacityLocked(records);
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
		shared_ptr<ProcessActivityWindow> process_activity;
		{
			auto phase_start_ns = TimestampNs();
			if (process_collector && process_collector->Available()) {
				scratch_rapl_before.clear();
				scratch_rapl_after.clear();
				auto before_ns = TimestampNs();
				rapl.Read(scratch_rapl_before);
				auto maximum_skew_ns =
				    MinValue<uint64_t>(5000000, MaxValue<uint64_t>(100000, period_ms * 100000));
				auto completion_timeout_ns =
				    MaxValue<uint64_t>(20000000, MinValue<uint64_t>(50000000, period_ms * 500000));
				auto boundary_lead_ns =
				    MinValue<uint64_t>(1000000, MaxValue<uint64_t>(100000, period_ms * 10000));
				auto transition_ns = TimestampNs() + boundary_lead_ns;
				auto captured =
				    process_collector->RotateWindow(transition_ns, maximum_skew_ns, completion_timeout_ns);
				if (captured.missing_helper_count > 0) {
					process_checkpoint_timeout_generation_count++;
				}
				auto after_ns = TimestampNs();
				rapl.Read(scratch_rapl_after);
				auto boundary_ns = captured.transition_ns > before_ns && captured.transition_ns <= after_ns
				                       ? captured.transition_ns
				                       : transition_ns;
				scratch_rapl_snapshots =
				    InterpolateRaplSnapshots(scratch_rapl_before, scratch_rapl_after, before_ns, after_ns, boundary_ns);
				now_ns = boundary_ns;
				process_activity = make_shared_ptr<ProcessActivityWindow>(std::move(captured));
			} else {
				rapl.Read(scratch_rapl_snapshots);
			}
			overhead.rapl_read_ns += DurationNs(TimestampNs(), phase_start_ns);
			auto windows_before = pending_windows.size();
			phase_start_ns = TimestampNs();
			CreateWindowsLocked(scratch_rapl_snapshots, now_ns, process_activity);
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
		FlushHardwareEnergyObservations();
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
		FlushHardwareEnergyObservations();
		overhead.finalize_windows_ns += DurationNs(TimestampNs(), phase_start_ns);
		FinishRuntimeOverhead(overhead, cpu_start_ns);
	}

	void CreateWindowsLocked(const vector<RaplSnapshot> &current_rapl, uint64_t now_ns,
	                         const shared_ptr<ProcessActivityWindow> &process_activity) {
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
			pending.process_activity = process_activity;
			pending_windows.push_back(std::move(pending));
		}
		pending_window_count.store(pending_windows.size(), std::memory_order_relaxed);
	}

	void BuildPendingWindowSocketIndexLocked() {
		for (auto &entry : scratch_pending_windows_by_socket) {
			entry.second.clear();
		}
		for (auto &pending : pending_windows) {
			scratch_pending_windows_by_socket[pending.window.socket_id].push_back(&pending);
		}
	}

	void ReservePendingWindowSegmentCapacityLocked(const vector<EnergySegmentRecord> &records) {
		scratch_segment_counts_by_socket.clear();
		for (idx_t i = 0; i < records.size(); i++) {
			const auto &record = records[i];
			if (record.end_ns <= record.start_ns || record.socket_id < 0) {
				continue;
			}
			scratch_segment_counts_by_socket[record.socket_id]++;
		}
		for (const auto &entry : scratch_segment_counts_by_socket) {
			auto socket_entry = scratch_pending_windows_by_socket.find(entry.first);
			if (socket_entry == scratch_pending_windows_by_socket.end()) {
				continue;
			}
			for (auto pending_ptr : socket_entry->second) {
				if (pending_ptr) {
					pending_ptr->segments.reserve(pending_ptr->segments.size() + entry.second);
				}
			}
		}
	}

	idx_t AddSegmentToPendingWindowsLocked(EnergySegmentRecord &record) {
		if (record.end_ns <= record.start_ns || record.socket_id < 0) {
			return 0;
		}
		auto socket_entry = scratch_pending_windows_by_socket.find(record.socket_id);
		if (socket_entry == scratch_pending_windows_by_socket.end()) {
			return 0;
		}
		idx_t added = 0;
		auto &socket_windows = socket_entry->second;
		if (socket_windows.size() == 1 && socket_windows[0]) {
			auto &pending = *socket_windows[0];
			auto &window = pending.window;
			if (record.start_ns >= window.start_ns && record.end_ns <= window.end_ns) {
				AttributedSegment attributed;
				attributed.window_id = window.window_id;
				attributed.slice_start_ns = record.start_ns;
				attributed.slice_end_ns = record.end_ns;
				attributed.record = std::move(record);
				pending.segments.push_back(std::move(attributed));
				return 1;
			}
		}
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

	void FinalizeReadyWindows(bool force, PeriodicRuntimeOverheadRecord *overhead = nullptr) {
		(void)force;
		// Periodic sampling creates closed slices for active segments, so old query-buffer open segments do not gate
		// window finalization.
		scratch_prepared_process_activity.source = nullptr;
		while (!pending_windows.empty()) {
			auto &pending = pending_windows.front();
			const PreparedSocketProcessActivity *prepared_socket = nullptr;
			if (pending.process_activity) {
				if (scratch_prepared_process_activity.source != pending.process_activity.get()) {
					PrepareProcessActivity(*pending.process_activity, scratch_prepared_process_activity);
				}
				prepared_socket =
				    FindPreparedSocketActivity(scratch_prepared_process_activity, pending.window.socket_id);
			}
			FinalizeWindowLocked(pending, prepared_socket, overhead);
			pending_windows.pop_front();
		}
		pending_window_count.store(pending_windows.size(), std::memory_order_relaxed);
	}

	void FinalizeWindowLocked(PeriodicPendingWindow &pending, const PreparedSocketProcessActivity *prepared_socket,
	                          PeriodicRuntimeOverheadRecord *overhead = nullptr) {
		auto &window = pending.window;
		if (!pending.segments.empty()) {
			auto frequencies = AverageFrequenciesForSocket(pending.segments, window.socket_id);
			window.core_freq_hz = frequencies.core_hz;
			if (frequencies.uncore_hz > 0) {
				window.uncore_freq_hz = frequencies.uncore_hz;
			}
		}
		if (window.uncore_freq_hz <= 0) {
			window.uncore_freq_hz = ReadUncoreFrequencyHz(window.socket_id);
		}
		double base_power_w = 0;
		window.base_power_valid = LookupBasePower(base_power_entries, window.socket_id, window.core_freq_hz,
		                                          window.uncore_freq_hz, 0, base_power_w);
		window.modeled_base_energy_j = window.base_power_valid ? base_power_w * window.duration_s : 0;
		window.base_energy_j = window.rapl_valid
		                           ? MinValue<double>(window.modeled_base_energy_j, window.package_energy_j)
		                           : window.modeled_base_energy_j;
		window.base_model_excess_energy_j =
		    MaxValue<double>(0, window.modeled_base_energy_j - window.base_energy_j);
		window.total_base_energy_j = window.base_energy_j;
		window.active_package_energy_j = std::max(0.0, window.package_energy_j - window.base_energy_j);
		if (!window.rapl_valid) {
			window.status = "rapl_unavailable";
		} else if (!window.base_power_valid) {
			window.status = "missing_base_power_calibration";
		} else {
			window.status = "ok";
		}
		auto phase_start_ns = TimestampNs();
		PrepareSegmentsForAttribution(pending.segments);
		scratch_process_rows.clear();
		bool process_profile_valid = false;
		if (pending.process_activity && process_collector) {
			auto process_window = window;
			process_window.process_attributed_active_energy_j = 0;
			process_window.process_attributed_base_energy_j = 0;
			process_window.process_conservation_error_j = 0;
			process_window.process_base_conservation_error_j = 0;
			process_window.duckdb_process_active_energy_j = 0;
			process_window.duckdb_process_base_energy_j = 0;
			process_window.external_process_active_energy_j = 0;
			process_window.external_process_base_energy_j = 0;
			process_window.host_kernel_active_energy_j = 0;
			process_window.host_kernel_base_energy_j = 0;
			process_window.host_irq_active_energy_j = 0;
			process_window.host_irq_base_energy_j = 0;
			process_window.system_idle_active_energy_j = 0;
			process_window.system_idle_base_energy_j = 0;
			process_window.measurement_overhead_active_energy_j = 0;
			process_window.measurement_overhead_base_energy_j = 0;
			process_window.duckdb_instrumented_active_energy_j = 0;
			process_window.duckdb_instrumented_base_energy_j = 0;
			process_window.duckdb_uninstrumented_active_energy_j = 0;
			process_window.duckdb_uninstrumented_base_energy_j = 0;
			process_window.duckdb_hierarchy_error_j = 0;
			process_window.duckdb_base_hierarchy_error_j = 0;
			auto materialize_process_rows = settings.export_enabled && settings.debug_export_enabled &&
			                                observed_query_activity.load(std::memory_order_acquire);
			AttributeSegmentsByProcess(
			    pending.segments, *pending.process_activity, prepared_socket, process_collector->DuckDBProcessID(),
			    process_window, scratch_process_rows, materialize_process_rows, scratch_process_thread_shares,
			    scratch_process_tid_to_thread, scratch_segment_thread_slots, process_energy_profiles);
			window = std::move(process_window);
			process_window_count++;
			if (window.process_attribution_valid) {
				process_valid_window_count++;
				process_profile_valid = true;
				window.summed_attributed_active_energy_j = window.process_attributed_active_energy_j;
				window.active_conservation_error_j = window.process_conservation_error_j;
				window.summed_attributed_base_energy_j = window.process_attributed_base_energy_j;
				window.base_conservation_error_j = window.process_base_conservation_error_j;
			} else {
				window.status += "|process_window_invalid";
			}
			total_duckdb_hierarchy_error_j += window.duckdb_hierarchy_error_j;
			total_duckdb_base_hierarchy_error_j += window.duckdb_base_hierarchy_error_j;
		} else {
			window.process_attribution_valid = false;
			window.process_status = process_collector_status;
			window.status += "|process_backend_unavailable";
			for (auto &segment : pending.segments) {
				segment.process_attribution_valid = false;
			}
		}
		if (overhead) {
			overhead->attribute_segments_ns += DurationNs(TimestampNs(), phase_start_ns);
		}
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
		if (process_profile_valid) {
			UpdateProfilesLocked(pending.segments);
			auto &uninstrumented = system_energy_profiles[EnergySystemCategory::DUCKDB_UNINSTRUMENTED];
			uninstrumented.category = EnergySystemCategory::DUCKDB_UNINSTRUMENTED;
			uninstrumented.sample_count++;
			uninstrumented.duration_s += window.duration_s;
			uninstrumented.attributed_active_energy_j += window.duckdb_uninstrumented_active_energy_j;
			uninstrumented.base_time_share_energy_j += window.duckdb_uninstrumented_base_energy_j;
			auto &sampler = system_energy_profiles[EnergySystemCategory::ENERGY_SAMPLER];
			sampler.category = EnergySystemCategory::ENERGY_SAMPLER;
			sampler.sample_count++;
			sampler.duration_s +=
			    static_cast<double>(window.process_checkpoint_duration_ns) / static_cast<double>(NSEC_PER_SEC);
			sampler.attributed_active_energy_j += window.measurement_overhead_active_energy_j;
			sampler.base_time_share_energy_j += window.measurement_overhead_base_energy_j;
		}
		if (overhead) {
			overhead->profile_update_ns += DurationNs(TimestampNs(), phase_start_ns);
		}
		if (settings.export_enabled && settings.debug_export_enabled &&
		    observed_query_activity.load(std::memory_order_acquire)) {
			phase_start_ns = TimestampNs();
			OpenOutputFilesIfNeededLocked();
			AppendWindowLocked(window);
			AppendSegmentsLocked(pending.segments);
			AppendProcessRowsLocked(scratch_process_rows);
			if (overhead) {
				overhead->csv_append_ns += DurationNs(TimestampNs(), phase_start_ns);
			}
		}
	}

	void UpdateProfilesLocked(const vector<AttributedSegment> &segments) {
		for (idx_t i = 0; i < segments.size(); i++) {
			const auto &segment = segments[i];
			if (segment.record.system_category != EnergySystemCategory::NONE) {
				auto &profile = system_energy_profiles[segment.record.system_category];
				profile.category = segment.record.system_category;
				profile.sample_count++;
				profile.duration_s += SegmentDuration(segment.record);
				profile.attributed_active_energy_j += segment.attributed_active_energy_j;
				profile.base_time_share_energy_j += segment.attributed_base_energy_j;
				profile.corrected_cycles += segment.corrected_cycles;
				continue;
			}
			if (segment.record.template_id != 0 && segment.record.scale_factor != 0 &&
			    segment.record.pipeline_id != 0 && segment.record.pipeline_signature_hash != 0 &&
			    segment.record.target_core_frequency_khz != 0 &&
			    segment.record.target_uncore_frequency_khz != 0 && segment.record.end_ns > segment.record.start_ns &&
			    (segment.record.phase == EnergySegmentPhase::EXECUTE ||
			     segment.record.phase == EnergySegmentPhase::INTERNAL)) {
				PendingHardwareEnergyObservation observation;
				observation.template_id = segment.record.template_id;
				observation.scale_factor = segment.record.scale_factor;
				observation.pipeline_identity.valid = true;
				observation.pipeline_identity.pipeline_id = segment.record.pipeline_id;
				observation.pipeline_identity.pipeline_signature_hash = segment.record.pipeline_signature_hash;
				observation.hardware.core_frequency_khz = segment.record.target_core_frequency_khz;
				observation.hardware.uncore_frequency_khz = segment.record.target_uncore_frequency_khz;
				observation.duration_ns = segment.record.end_ns - segment.record.start_ns;
				observation.attributed_active_energy_j = segment.attributed_active_energy_j;
				observation.attributed_base_energy_j = segment.attributed_base_energy_j;
				observation.hardware_state_stable = segment.record.hardware_state_stable;
				scratch_hardware_energy_observations.push_back(std::move(observation));
			}
			AddSegmentToEfficiencyAggregate(query_profiles[segment.record.query_id], segment, false, nullptr);
			if (segment.record.phase == EnergySegmentPhase::EXECUTE ||
			    segment.record.phase == EnergySegmentPhase::INTERNAL || segment.record.lifecycle_group_id == 0 ||
			    segment.record.lifecycle_member_pipeline_ids.empty()) {
				auto pipeline_key = BuildPeriodicPipelineProfileKey(segment.record);
				AddSegmentToEfficiencyAggregate(pipeline_profiles[pipeline_key], segment, true, &pipeline_key);
				continue;
			}
			auto lifecycle_key = BuildPeriodicLifecycleProfileKey(segment.record);
			if (!AddLifecycleSegmentToProfile(lifecycle_phase_profiles[lifecycle_key], segment, lifecycle_key)) {
				continue;
			}
			auto member_count = static_cast<double>(segment.record.lifecycle_member_pipeline_ids.size());
			auto allocated_active_energy =
			    member_count > 0 ? segment.attributed_active_energy_j / member_count
			                     : segment.attributed_active_energy_j;
			auto allocated_base_energy =
			    member_count > 0 ? segment.attributed_base_energy_j / member_count
			                     : segment.attributed_base_energy_j;
			for (auto member_pipeline_id : segment.record.lifecycle_member_pipeline_ids) {
				auto member_key = BuildPeriodicPipelineProfileKeyForPipeline(segment.record, member_pipeline_id);
				AddAllocatedLifecycleEnergy(pipeline_profiles[member_key], segment, member_pipeline_id,
				                            allocated_active_energy, allocated_base_energy, member_key);
			}
		}
	}

	void FlushHardwareEnergyObservations() {
		if (!active_db || scratch_hardware_energy_observations.empty()) {
			return;
		}
		for (const auto &observation : scratch_hardware_energy_observations) {
			active_db->GetQueryRequestProfileStore().RecordPipelineHardwareEnergy(
			    observation.template_id, observation.scale_factor, observation.pipeline_identity, observation.hardware,
			    observation.duration_ns, observation.attributed_active_energy_j,
			    observation.attributed_base_energy_j, observation.hardware_state_stable);
		}
		scratch_hardware_energy_observations.clear();
	}

	void OpenOutputFilesLocked() {
		window_out.open((output_dir + "/period_socket_energy_windows.csv").c_str());
		segment_out.open((output_dir + "/period_attributed_segments.csv").c_str());
		process_out.open((output_dir + "/period_process_activity_windows.csv").c_str());
		window_out << "window_id,socket_id,start_ns,end_ns,duration_s,rapl_start_uj,rapl_end_uj,package_energy_j,"
		              "core_freq_hz,uncore_freq_hz,base_energy_j,active_package_energy_j,"
		              "summed_attributed_active_energy_j,active_conservation_error_j,summed_attributed_base_energy_j,"
		              "base_conservation_error_j,summed_charged_energy_j,package_conservation_error_j,rapl_valid,"
		              "base_power_valid,used_time_fallback,status,segment_count,total_base_energy_j,busy_wall_time_s,"
		              "idle_gap_wall_time_s,busy_base_energy_j,idle_gap_base_energy_j,"
		              "process_generation,process_transition_ns,process_boundary_min_ns,process_boundary_max_ns,"
		              "process_maximum_checkpoint_delay_ns,process_checkpoint_duration_ns,"
		              "process_helper_count,process_helper_futex_wake_count,process_acknowledged_helper_count,"
		              "process_missing_helper_count,process_missing_helper_cpus,process_slowest_helper_cpu,"
		              "process_activity_map_insert_failure,process_missing_counter,"
		              "process_invalid_counter_delta,process_interrupt_underflow,"
		              "process_boundary_split_count,process_boundary_split_cpu_count,"
		              "process_boundary_split_runtime_ns,process_boundary_split_cycles,"
		              "process_boundary_split_ref_cycles,process_late_helper_count,process_boundary_corrected,"
		              "process_lost_records,process_lookup_failures,"
		              "process_attribution_valid,process_status,process_attributed_active_energy_j,"
		              "process_conservation_error_j,duckdb_process_active_energy_j,"
		              "external_process_active_energy_j,host_kernel_active_energy_j,host_irq_active_energy_j,"
		              "measurement_overhead_active_energy_j,duckdb_instrumented_active_energy_j,"
		              "duckdb_uninstrumented_active_energy_j,duckdb_hierarchy_error_j,"
		              "modeled_base_energy_j,base_model_excess_energy_j,process_attributed_base_energy_j,"
		              "process_base_conservation_error_j,duckdb_process_base_energy_j,"
		              "external_process_base_energy_j,host_kernel_base_energy_j,host_irq_base_energy_j,"
		              "system_idle_active_energy_j,system_idle_base_energy_j,"
		              "measurement_overhead_base_energy_j,duckdb_instrumented_base_energy_j,"
		              "duckdb_uninstrumented_base_energy_j,duckdb_base_hierarchy_error_j,"
		              "process_monitored_cpu_count,process_observed_runtime_ns,process_expected_runtime_ns,"
		              "process_runtime_coverage\n";
		segment_out << "window_id,segment_id,query_id,pipeline_id,pipeline_signature,phase_kind,system_category,"
		               "lifecycle_group_id,"
		               "owner_pipeline_id,lifecycle_member_count,socket_id,physical_core_id,logical_cpu_id,linux_tid,"
		               "slice_start_ns,slice_end_ns,duration_s,work_units,frequency_ratio,"
		               "corrected_cycles,activity_weight,weight_denominator,attributed_active_energy_j,"
		               "base_time_weight,base_time_denominator_s,attributed_base_energy_j,charged_total_energy_j,"
		               "throughput,corrected_cycles_per_work,active_power_w,charged_power_w,task_clock_ns,"
		               "task_clock_coverage,"
		               "process_attribution_valid,counter_status,attribution_status,"
		               "process_counter_source\n";
		process_out << "window_id,generation,process_start_time_ns,tgid,tid,socket_id,kind,comm,runtime_ns,"
		               "cycles,ref_cycles,time_enabled_ns,time_running_ns,ref_time_enabled_ns,ref_time_running_ns,"
		               "pmu_running_ratio,ref_pmu_running_ratio,pmu_valid,"
		               "corrected_cycles,frequency_ratio,activity_weight,weight_denominator,"
		               "attributed_active_energy_j,duckdb_process,base_time_weight,"
		               "base_time_denominator_ns,attributed_base_energy_j,charged_total_energy_j\n";
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
		           << w.idle_gap_base_energy_j << "," << w.process_generation << "," << w.process_transition_ns << ","
		           << w.process_boundary_min_ns
		           << "," << w.process_boundary_max_ns << "," << w.process_maximum_checkpoint_delay_ns << ","
		           << w.process_checkpoint_duration_ns << "," << w.process_helper_count << ","
		           << w.process_helper_futex_wake_count << "," << w.process_acknowledged_helper_count << ","
		           << w.process_missing_helper_count << "," << CsvEscape(w.process_missing_helper_cpus) << ","
		           << w.process_slowest_helper_cpu << "," << w.process_activity_map_insert_failure << ","
		           << w.process_missing_counter << "," << w.process_invalid_counter_delta << ","
		           << w.process_interrupt_underflow << "," << w.process_boundary_split_count << ","
		           << w.process_boundary_split_cpu_count << "," << w.process_boundary_split_runtime_ns << ","
		           << w.process_boundary_split_cycles << "," << w.process_boundary_split_ref_cycles << ","
		           << w.process_late_helper_count << "," << w.process_boundary_corrected << ","
		           << w.process_lost_records << "," << w.process_lookup_failures << ","
		           << w.process_attribution_valid << ","
		           << CsvEscape(w.process_status) << "," << w.process_attributed_active_energy_j << ","
		           << w.process_conservation_error_j << "," << w.duckdb_process_active_energy_j << ","
		           << w.external_process_active_energy_j << "," << w.host_kernel_active_energy_j << ","
		           << w.host_irq_active_energy_j << "," << w.measurement_overhead_active_energy_j << ","
		           << w.duckdb_instrumented_active_energy_j << "," << w.duckdb_uninstrumented_active_energy_j << ","
		           << w.duckdb_hierarchy_error_j << "," << w.modeled_base_energy_j << ","
		           << w.base_model_excess_energy_j << "," << w.process_attributed_base_energy_j << ","
		           << w.process_base_conservation_error_j << "," << w.duckdb_process_base_energy_j << ","
		           << w.external_process_base_energy_j << "," << w.host_kernel_base_energy_j << ","
		           << w.host_irq_base_energy_j << "," << w.system_idle_active_energy_j << ","
		           << w.system_idle_base_energy_j << "," << w.measurement_overhead_base_energy_j << ","
		           << w.duckdb_instrumented_base_energy_j << "," << w.duckdb_uninstrumented_base_energy_j << ","
		           << w.duckdb_base_hierarchy_error_j << "," << w.process_monitored_cpu_count << ","
		           << w.process_observed_runtime_ns << "," << w.process_expected_runtime_ns << ","
		           << w.process_runtime_coverage << "\n";
	}

	void AppendSegmentsLocked(const vector<AttributedSegment> &segments) {
		if (!segment_out.good()) {
			return;
		}
		for (idx_t i = 0; i < segments.size(); i++) {
			const auto &s = segments[i];
			auto pipeline_signature =
			    PipelineSignatureLocked(s.record.query_id, s.record.pipeline_id, s.record.system_category);
			auto duration = SegmentDuration(s.record);
			double charged_total_energy = s.attributed_active_energy_j + s.attributed_base_energy_j;
			double cycles_per_work =
			    s.record.work_units > 0 ? s.corrected_cycles / static_cast<double>(s.record.work_units) : 0;
			double active_power = duration > 0 ? s.attributed_active_energy_j / duration : 0;
			double charged_power = duration > 0 ? charged_total_energy / duration : 0;
			segment_out << s.window_id << "," << s.record.segment_id << "," << s.record.query_id << ","
			            << s.record.pipeline_id << "," << CsvEscape(pipeline_signature) << ","
			            << EnergySegmentPhaseToString(s.record.phase) << ","
			            << EnergySystemCategoryToString(s.record.system_category) << ","
			            << s.record.lifecycle_group_id << ","
			            << s.record.owner_pipeline_id << "," << s.record.lifecycle_member_count << ","
			            << s.record.socket_id << "," << s.record.physical_core_id << "," << s.record.logical_cpu_id
			            << "," << s.record.linux_tid
			            << "," << s.slice_start_ns << "," << s.slice_end_ns << "," << duration << ","
			            << s.record.work_units << "," << s.frequency_ratio << "," << s.corrected_cycles << ","
			            << s.activity_weight << "," << s.weight_denominator << "," << s.attributed_active_energy_j
			            << "," << s.base_time_weight << "," << s.base_time_denominator_s << ","
			            << s.attributed_base_energy_j << "," << charged_total_energy << "," << s.throughput << ","
			            << cycles_per_work << "," << active_power << "," << charged_power << ","
			            << s.record.task_clock_ns << "," << s.task_clock_coverage << ","
			            << s.process_attribution_valid << ","
			            << CsvEscape(s.record.counter_status) << "," << CsvEscape(s.attribution_status) << ","
			            << CsvEscape(s.process_counter_source) << "\n";
		}
	}

	void AppendProcessRowsLocked(const vector<AttributedProcessActivity> &rows) {
		if (!process_out.good()) {
			return;
		}
		for (const auto &row : rows) {
			process_out << row.window_id << "," << row.record.generation << ","
			            << row.record.process_start_time_ns << "," << row.record.tgid << "," << row.record.tid << ","
			            << row.record.socket_id << "," << ProcessActivityKindToString(row.record.kind) << ","
			            << CsvEscape(row.record.comm) << "," << row.record.runtime_ns << "," << row.record.cycles
			            << "," << row.record.ref_cycles << "," << row.record.time_enabled_ns << ","
			            << row.record.time_running_ns << "," << row.record.ref_time_enabled_ns << ","
			            << row.record.ref_time_running_ns << "," << row.record.pmu_running_ratio << ","
			            << row.record.ref_pmu_running_ratio << "," << row.record.pmu_valid << ","
			            << row.record.corrected_cycles << ","
			            << row.record.frequency_ratio << "," << row.activity_weight << "," << row.weight_denominator
			            << "," << row.attributed_active_energy_j << "," << row.duckdb_process << ","
			            << row.base_time_weight << "," << row.base_time_denominator_ns << ","
			            << row.attributed_base_energy_j << ","
			            << row.attributed_active_energy_j + row.attributed_base_energy_j << "\n";
		}
	}

	void WriteProcessEnergyProfilesLocked() {
		std::ofstream out((output_dir + "/period_process_energy_profiles.csv").c_str());
		out << "process_start_time_ns,tgid,socket_id,kind,comm,sample_count,runtime_ns,cycles,ref_cycles,"
		       "corrected_cycles,active_energy_j,base_energy_j,charged_total_energy_j\n";
		vector<const ProcessEnergyAggregate *> sorted_profiles;
		sorted_profiles.reserve(process_energy_profiles.size());
		for (const auto &entry : process_energy_profiles) {
			sorted_profiles.push_back(&entry.second);
		}
		std::sort(sorted_profiles.begin(), sorted_profiles.end(),
		          [](const ProcessEnergyAggregate *lhs, const ProcessEnergyAggregate *rhs) {
			          if (lhs->process_start_time_ns != rhs->process_start_time_ns) {
				          return lhs->process_start_time_ns < rhs->process_start_time_ns;
			          }
			          if (lhs->tgid != rhs->tgid) {
				          return lhs->tgid < rhs->tgid;
			          }
			          if (lhs->socket_id != rhs->socket_id) {
				          return lhs->socket_id < rhs->socket_id;
			          }
			          if (lhs->kind != rhs->kind) {
				          return lhs->kind < rhs->kind;
			          }
			          return lhs->comm < rhs->comm;
		          });
		for (const auto profile_ptr : sorted_profiles) {
			const auto &profile = *profile_ptr;
			out << profile.process_start_time_ns << "," << profile.tgid << "," << profile.socket_id << ","
			    << ProcessActivityKindToString(profile.kind) << "," << CsvEscape(profile.comm) << ","
			    << profile.sample_count << "," << profile.runtime_ns << "," << profile.cycles << ","
			    << profile.ref_cycles << "," << profile.corrected_cycles << "," << profile.active_energy_j << ","
			    << profile.base_energy_j << "," << profile.active_energy_j + profile.base_energy_j << "\n";
		}
	}

	void WritePipelineMetadataLocked() {
		std::ofstream out((output_dir + "/period_pipeline_metadata.csv").c_str());
		out << "query_id,pipeline_id,pipeline_signature\n";
		for (const auto &entry : pipeline_signatures) {
			out << entry.first.first << "," << entry.first.second << "," << CsvEscape(entry.second) << "\n";
		}
	}

	void WriteFinalOutputs() {
		std::unique_lock<mutex> guard(lock);
		if (!settings.export_enabled) {
			return;
		}
		auto saw_query_activity = observed_query_activity.load(std::memory_order_acquire);
		auto has_attribution_output = attributed_segment_count > 0 || !pipeline_profiles.empty() ||
		                              !query_profiles.empty();
		if (!saw_query_activity && !has_attribution_output) {
			return;
		}
		EnsureDirectoryRecursive(output_dir);
		if (output_opened) {
			window_out.flush();
			segment_out.flush();
			process_out.flush();
		}
		vector<PipelineEfficiencyAggregate> pipeline_result;
		for (auto entry = pipeline_profiles.begin(); entry != pipeline_profiles.end(); ++entry) {
			auto allocated_lifecycle_energy =
			    entry->second.allocated_initialize_active_energy_j +
			    entry->second.allocated_initialize_base_energy_j +
			    entry->second.allocated_prepare_finish_active_energy_j +
			    entry->second.allocated_prepare_finish_base_energy_j +
			    entry->second.allocated_finish_active_energy_j +
			    entry->second.allocated_finish_base_energy_j;
			if (entry->second.sample_count > 0 || entry->second.valid_input_sample_count > 0 ||
			    allocated_lifecycle_energy > 0) {
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
			          if (lhs.hardware_mode != rhs.hardware_mode) {
				          return static_cast<uint8_t>(lhs.hardware_mode) <
				                 static_cast<uint8_t>(rhs.hardware_mode);
			          }
			          if (lhs.core_freq_hz != rhs.core_freq_hz) {
				          return lhs.core_freq_hz < rhs.core_freq_hz;
			          }
			          return lhs.uncore_freq_hz < rhs.uncore_freq_hz;
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
		vector<LifecyclePhaseProfileAggregate> lifecycle_result;
		for (auto entry = lifecycle_phase_profiles.begin(); entry != lifecycle_phase_profiles.end(); ++entry) {
			if (entry->second.sample_count > 0) {
				lifecycle_result.push_back(entry->second);
			}
		}
		std::sort(lifecycle_result.begin(), lifecycle_result.end(),
		          [](const LifecyclePhaseProfileAggregate &lhs, const LifecyclePhaseProfileAggregate &rhs) {
			          if (lhs.query_id != rhs.query_id) {
				          return lhs.query_id < rhs.query_id;
			          }
			          if (lhs.lifecycle_group_id != rhs.lifecycle_group_id) {
				          return lhs.lifecycle_group_id < rhs.lifecycle_group_id;
			          }
			          if (lhs.phase != rhs.phase) {
				          return static_cast<uint8_t>(lhs.phase) < static_cast<uint8_t>(rhs.phase);
			          }
			          if (lhs.hardware_mode != rhs.hardware_mode) {
				          return static_cast<uint8_t>(lhs.hardware_mode) <
				                 static_cast<uint8_t>(rhs.hardware_mode);
			          }
			          if (lhs.core_freq_hz != rhs.core_freq_hz) {
				          return lhs.core_freq_hz < rhs.core_freq_hz;
			          }
			          return lhs.uncore_freq_hz < rhs.uncore_freq_hz;
		          });
		WriteEfficiencyProfilesCSV(output_dir + "/period_pipeline_profiles.csv", pipeline_result);
		WriteEfficiencyProfilesCSV(output_dir + "/period_query_profiles.csv", query_result, true);
		WritePipelineMetadataLocked();
		WriteLifecyclePhaseProfilesCSV(output_dir + "/period_lifecycle_phase_profiles.csv", lifecycle_result);
		WriteLifecycleGroupMembersCSV(output_dir + "/period_lifecycle_group_members.csv", lifecycle_result);
		WriteSystemEnergyProfilesCSV(output_dir + "/period_system_energy_profiles.csv", system_energy_profiles);
		WriteProcessEnergyProfilesLocked();
		if (settings.debug_export_enabled) {
			WriteRuntimeOverheadCSVLocked();
		}
		WritePeriodicValidationSummaryLocked();
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
		const auto &all_overhead = runtime_overhead_stats;
		const auto &periodic_overhead = periodic_runtime_overhead_stats;
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
		double system_active_energy_j = 0;
		double system_base_energy_j = 0;
		for (const auto &entry : system_energy_profiles) {
			system_active_energy_j += entry.second.attributed_active_energy_j;
			system_base_energy_j += entry.second.base_time_share_energy_j;
		}
		std::ofstream out((output_dir + "/period_validation_summary.json").c_str());
		out << "{\n";
		out << "  \"attribution_schema_version\": 3,\n";
		out << "  \"window_count\": " << finalized_window_count << ",\n";
		out << "  \"attributed_segment_count\": " << attributed_segment_count << ",\n";
		out << "  \"active_conservation_error_j\": " << total_active_conservation_error_j << ",\n";
		out << "  \"base_conservation_error_j\": " << total_base_conservation_error_j << ",\n";
		out << "  \"package_conservation_gap_j\": " << total_package_gap_j << ",\n";
		out << "  \"system_attributed_active_energy_j\": " << system_active_energy_j << ",\n";
		out << "  \"system_attributed_base_energy_j\": " << system_base_energy_j << ",\n";
		out << "  \"base_power_loaded\": " << (base_power_loaded ? "true" : "false") << ",\n";
		out << "  \"period_ms\": " << period_ms << ",\n";
		out << "  \"idle_period_ms\": " << settings.idle_period_ms << ",\n";
		out << "  \"effective_idle_period_ms\": " << effective_idle_period_ms << ",\n";
		out << "  \"adaptive_sampling_enabled\": " << (adaptive_sampling_enabled ? "true" : "false") << ",\n";
		out << "  \"active_periodic_sample_count\": " << active_periodic_sample_count << ",\n";
		out << "  \"idle_periodic_sample_count\": " << idle_periodic_sample_count << ",\n";
		out << "  \"activity_transition_sample_count\": " << activity_transition_sample_count << ",\n";
		out << "  \"shutdown_sample_count\": " << shutdown_sample_count << ",\n";
		out << "  \"max_active_periodic_interval_ns\": " << max_active_periodic_interval_ns << ",\n";
		out << "  \"max_idle_periodic_interval_ns\": " << max_idle_periodic_interval_ns << ",\n";
		out << "  \"process_collector_compiled\": "
		    << (ProcessActivityCollector::IsCompiled() ? "true" : "false") << ",\n";
		out << "  \"process_collector_status\": \"" << process_collector_status << "\",\n";
		out << "  \"process_window_count\": " << process_window_count << ",\n";
		out << "  \"process_valid_window_count\": " << process_valid_window_count << ",\n";
		out << "  \"process_checkpoint_timeout_generation_count\": "
		    << process_checkpoint_timeout_generation_count << ",\n";
		out << "  \"process_valid_window_fraction\": "
		    << (process_window_count == 0
		            ? 0
		            : static_cast<double>(process_valid_window_count) / static_cast<double>(process_window_count))
		    << ",\n";
		out << "  \"duckdb_hierarchy_conservation_error_j\": " << total_duckdb_hierarchy_error_j << ",\n";
		out << "  \"duckdb_base_hierarchy_conservation_error_j\": "
		    << total_duckdb_base_hierarchy_error_j << ",\n";
		out << "  \"debug_export_enabled\": " << (settings.debug_export_enabled ? "true" : "false") << ",\n";
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
		if (process_out.is_open()) {
			process_out.close();
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
	uint64_t effective_idle_period_ms = 100;
	bool adaptive_sampling_enabled = false;
	uint64_t activity_transition_generation = 0;
	idx_t active_periodic_sample_count = 0;
	idx_t idle_periodic_sample_count = 0;
	idx_t activity_transition_sample_count = 0;
	idx_t shutdown_sample_count = 0;
	uint64_t max_active_periodic_interval_ns = 0;
	uint64_t max_idle_periodic_interval_ns = 0;
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
	std::map<std::pair<uint64_t, uint64_t>, string> pipeline_signatures;
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
	vector<AttributedProcessActivity> scratch_process_rows;
	vector<PendingHardwareEnergyObservation> scratch_hardware_energy_observations;
	PreparedProcessActivity scratch_prepared_process_activity;
	vector<DuckDBThreadShare> scratch_process_thread_shares;
	std::unordered_map<int64_t, idx_t> scratch_process_tid_to_thread;
	vector<int32_t> scratch_segment_thread_slots;
	vector<RaplSnapshot> scratch_rapl_snapshots;
	vector<RaplSnapshot> scratch_rapl_before;
	vector<RaplSnapshot> scratch_rapl_after;
	std::unordered_map<PeriodicPipelineProfileKey, PipelineEfficiencyAggregate, PeriodicPipelineProfileKeyHash>
	    scratch_pipeline_profile_updates;
	std::unordered_map<uint64_t, PipelineEfficiencyAggregate> scratch_query_profile_updates;
	std::deque<PeriodicPendingWindow> pending_windows;
	std::unordered_map<int, vector<PeriodicPendingWindow *>> scratch_pending_windows_by_socket;
	std::unordered_map<int, idx_t> scratch_segment_counts_by_socket;
	std::atomic<idx_t> pending_window_count {0};
	vector<EnergySegmentRecord> pending_segment_records;
	vector<EnergySegmentRecord> scratch_remaining_segment_records;
	std::unordered_map<PeriodicPipelineProfileKey, PipelineEfficiencyAggregate, PeriodicPipelineProfileKeyHash>
	    pipeline_profiles;
	std::unordered_map<PeriodicLifecycleProfileKey, LifecyclePhaseProfileAggregate, PeriodicLifecycleProfileKeyHash>
	    lifecycle_phase_profiles;
	std::unordered_map<uint64_t, PipelineEfficiencyAggregate> query_profiles;
	std::map<EnergySystemCategory, SystemEnergyAggregate> system_energy_profiles;
	ProcessEnergyProfileMap process_energy_profiles;
	unique_ptr<ProcessActivityCollector> process_collector;
	string process_collector_status = "not_started";
	vector<PeriodicRuntimeOverheadRecord> runtime_overhead;
	PeriodicRuntimeOverheadStats runtime_overhead_stats;
	PeriodicRuntimeOverheadStats periodic_runtime_overhead_stats;
	std::ofstream window_out;
	std::ofstream segment_out;
	std::ofstream process_out;
	idx_t finalized_window_count = 0;
	idx_t attributed_segment_count = 0;
	idx_t process_window_count = 0;
	idx_t process_valid_window_count = 0;
	idx_t process_checkpoint_timeout_generation_count = 0;
	double total_active_conservation_error_j = 0;
	double total_base_conservation_error_j = 0;
	double total_package_gap_j = 0;
	double total_duckdb_hierarchy_error_j = 0;
	double total_duckdb_base_hierarchy_error_j = 0;
};

} // namespace

const char *EnergySegmentPhaseToString(EnergySegmentPhase phase) {
	switch (phase) {
	case EnergySegmentPhase::EXECUTE:
		return "EXECUTE";
	case EnergySegmentPhase::INTERNAL:
		return "INTERNAL";
	case EnergySegmentPhase::INITIALIZE:
		return "INITIALIZE";
	case EnergySegmentPhase::PREPARE_FINISH:
		return "PREPARE_FINISH";
	case EnergySegmentPhase::FINISH:
		return "FINISH";
	default:
		return "UNKNOWN";
	}
}

const char *EnergySystemCategoryToString(EnergySystemCategory category) {
	switch (category) {
	case EnergySystemCategory::SLA_SCHEDULER:
		return "SLA_SCHEDULER";
	case EnergySystemCategory::STRIDE_SCHEDULER:
		return "STRIDE_SCHEDULER";
	case EnergySystemCategory::ENERGY_SAMPLER:
		return "ENERGY_SAMPLER";
	case EnergySystemCategory::DUCKDB_UNINSTRUMENTED:
		return "DUCKDB_UNINSTRUMENTED";
	case EnergySystemCategory::NONE:
	default:
		return "NONE";
	}
}

static bool IsEnergyAttributionControlQuery(const string &query) {
	auto normalized = query;
	StringUtil::Trim(normalized);
	normalized = StringUtil::Lower(normalized);
	if (normalized == "set" || StringUtil::StartsWith(normalized, "set ") ||
	    StringUtil::StartsWith(normalized, "set\t") || StringUtil::StartsWith(normalized, "set\n") ||
	    StringUtil::StartsWith(normalized, "pragma ") || normalized == "use" ||
	    StringUtil::StartsWith(normalized, "use ") || StringUtil::StartsWith(normalized, "use\t") ||
	    StringUtil::StartsWith(normalized, "use\n")) {
		return true;
	}
	return StringUtil::Contains(normalized, "duckdb_debug_query_request_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_pipeline_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_samples") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_pipeline_instances") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_internal_event_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_internal_event_instances") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_continuation_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_throughput_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_request_downstream_suffix_profiles") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_admission") ||
	       StringUtil::Contains(normalized, "duckdb_debug_query_activation_events");
}

bool EnergyAttributionManager::Enabled(const ClientContext &context) {
	if (ClientConfig::GetConfig(context).energy_attribution.enabled) {
		return true;
	}
	EnergyAttributionSettings settings;
	return TryGetDatabaseEnergySettings(DatabaseInstance::GetDatabase(context), settings);
}

bool EnergyAttributionManager::LifecyclePhasesEnabled(const ClientContext &context) {
	return EnergyAttributionManager::Enabled(context);
}

uint64_t EnergyAttributionManager::GetPipelineAttributionId(Pipeline &pipeline) {
	auto pipeline_id = pipeline.GetEnergyAttributionPipelineId();
	if (pipeline_id != 0) {
		return pipeline_id;
	}
	return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&pipeline));
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
	QueryRequestMetadata request_metadata;
	if (QueryRequestMetadataManager::TryGetActive(context, request_metadata)) {
		state->template_id = request_metadata.template_id;
		state->scale_factor = request_metadata.scale_factor;
	}
	state->start_ns = TimestampNs();
	state->uncore_freq_by_socket = BuildQueryUncoreFrequencyCache(true);
	{
		lock_guard<mutex> guard(g_query_lock);
		g_queries[&context] = state;
	}
	PeriodicEnergyRuntime::Get().RegisterQuery(state);
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
	if (state) {
		PeriodicEnergyRuntime::Get().MarkQueryEnded(state);
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
	PeriodicEnergyRuntime::Get().RegisterPipelineMetadata(query->query_id, pipeline_id, pipeline.ToString());
	pipeline.SetEnergyAttributionPipeline(query, pipeline_id);
}

static const vector<uint64_t> &EmptyLifecycleMemberPipelineIds() {
	static const vector<uint64_t> empty;
	return empty;
}

EnergySystemSegmentScope::EnergySystemSegmentScope(DatabaseInstance &db, EnergySystemCategory category) {
	EnergyAttributionSettings settings;
	if (category == EnergySystemCategory::NONE || !TryGetDatabaseEnergySettings(db, settings)) {
		return;
	}
	auto &thread_state = g_thread_energy_state;
	thread_state.InitializeHardware();
	auto tid = thread_state.GetTID();
	record.segment_id = g_system_segment_id.fetch_add(1, std::memory_order_relaxed);
	record.worker_id = static_cast<uint64_t>(tid);
	record.linux_tid = tid;
	record.system_category = category;
	record.start_ns = TimestampNs();
	record.logical_cpu_id = CurrentCPU();
	auto topology = LookupCpuTopology(record.logical_cpu_id, true);
	record.socket_id = topology.socket_id;
	record.physical_core_id = topology.physical_core_id;
	record.core_freq_hz = topology.core_freq_hz;
	active = PeriodicEnergyRuntime::Get().BeginSystemSegment(db, record);
}

EnergySystemSegmentScope::~EnergySystemSegmentScope() {
	if (!active) {
		return;
	}
	record.end_ns = TimestampNs();
	record.duration_s = record.end_ns > record.start_ns
	                        ? static_cast<double>(record.end_ns - record.start_ns) / static_cast<double>(NSEC_PER_SEC)
	                        : 0;
	record.end_logical_cpu_id = CurrentCPU();
	auto topology = LookupCpuTopology(record.end_logical_cpu_id, false);
	record.end_socket_id = topology.socket_id;
	record.end_physical_core_id = topology.physical_core_id;
	record.migrated = record.logical_cpu_id != record.end_logical_cpu_id || record.socket_id != record.end_socket_id ||
	                  record.physical_core_id != record.end_physical_core_id;
	record.hardware_state_stable = !record.migrated && record.socket_id >= 0 && record.physical_core_id >= 0;
	PeriodicEnergyRuntime::Get().EndSegment(record);
}

EnergySegmentScope::EnergySegmentScope(Pipeline &pipeline, int start_cpu_hint)
    : EnergySegmentScope(pipeline, EnergySegmentPhase::EXECUTE, 0, EmptyLifecycleMemberPipelineIds(), start_cpu_hint) {
}

EnergySegmentScope::EnergySegmentScope(Pipeline &pipeline, Event &event, EnergySegmentPhase phase, int start_cpu_hint)
    : EnergySegmentScope(pipeline, phase, event.HasEnergyLifecycleInfo() ? event.GetEnergyLifecycleGroupId() : 0,
                         event.HasEnergyLifecycleInfo() ? event.GetEnergyLifecycleMemberPipelineIds()
                                                        : EmptyLifecycleMemberPipelineIds(),
                         start_cpu_hint) {
}

EnergySegmentScope::EnergySegmentScope(Pipeline &pipeline, EnergySegmentPhase phase, uint64_t lifecycle_group_id,
                                       const vector<uint64_t> &lifecycle_member_pipeline_ids, int start_cpu_hint) {
	if (phase != EnergySegmentPhase::EXECUTE && lifecycle_group_id == 0) {
		return;
	}
	query_handle = pipeline.GetEnergyAttributionQueryHandle();
	if (!query_handle) {
		return;
	}
	auto query_ref = shared_ptr_cast<EnergyAttributionQueryHandle, QueryEnergyState>(query_handle);
	auto query = query_ref.get();
	query_state = query;
	auto &thread_state = g_thread_energy_state;
	thread_state.InitializeHardware();
	auto tid = thread_state.GetTID();
	record = EnergySegmentRecord();
	record.segment_id = thread_state.NextSegmentId(*query);
	record.query_id = query->query_id;
	record.template_id = query->template_id;
	record.scale_factor = query->scale_factor;
	record.pipeline_id = EnergyAttributionManager::GetPipelineAttributionId(pipeline);
	PipelineWorkSnapshot work_snapshot;
	if (pipeline.GetWorkSnapshot(work_snapshot) && work_snapshot.profile_identity.valid) {
		record.pipeline_signature_hash = work_snapshot.profile_identity.pipeline_signature_hash;
	}
	record.phase = phase;
	record.owner_pipeline_id = record.pipeline_id;
	record.lifecycle_group_id = lifecycle_group_id;
	record.lifecycle_member_count = lifecycle_member_pipeline_ids.size();
	record.lifecycle_member_pipeline_ids = lifecycle_member_pipeline_ids;
	record.worker_id = static_cast<uint64_t>(tid);
	record.linux_tid = tid;
	record.start_ns = TimestampNs();
	if (thread_state.pinned) {
		record.logical_cpu_id = thread_state.pinned_cpu;
		record.socket_id = thread_state.pinned_socket_id;
		record.physical_core_id = thread_state.pinned_physical_core_id;
		record.core_freq_hz = thread_state.pinned_core_freq_hz;
	} else {
		record.logical_cpu_id = start_cpu_hint >= 0 ? start_cpu_hint : CurrentCPU();
		auto topology = LookupCpuTopology(record.logical_cpu_id, true);
		record.socket_id = topology.socket_id;
		record.physical_core_id = topology.physical_core_id;
		record.core_freq_hz = topology.core_freq_hz;
	}
	record.uncore_freq_hz = GetQueryUncoreFrequency(*query, record.socket_id);
	if (DatabaseInstance::GetDatabase(*query->context).GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY) {
		auto target = DatabaseInstance::GetDatabase(*query->context)
		                  .GetQuerySLAScheduler()
		                  .GetHardwareManager()
		                  .GetAppliedConfigurationForCPU(record.logical_cpu_id);
		record.target_core_frequency_khz = target.core_frequency_khz;
		record.target_uncore_frequency_khz = target.uncore_frequency_khz;
	}
	active = PeriodicEnergyRuntime::Get().BeginSegment(query_ref, record);
}

EnergySegmentScope::~EnergySegmentScope() {
	if (!active || finished) {
		return;
	}
	finished = true;
	auto query = static_cast<QueryEnergyState *>(query_state);
	record.end_ns = TimestampNs();
	record.end_logical_cpu_id = end_cpu_hint >= 0 ? end_cpu_hint : CurrentCPU();
	if (query && g_thread_energy_state.pinned && record.end_logical_cpu_id == g_thread_energy_state.pinned_cpu) {
		record.end_socket_id = g_thread_energy_state.pinned_socket_id;
		record.end_physical_core_id = g_thread_energy_state.pinned_physical_core_id;
	} else if (query) {
		auto topology = LookupCpuTopology(record.end_logical_cpu_id, true);
		record.end_socket_id = topology.socket_id;
		record.end_physical_core_id = topology.physical_core_id;
	} else {
		auto topology = LookupCpuTopology(record.end_logical_cpu_id, false);
		record.end_socket_id = topology.socket_id;
		record.end_physical_core_id = topology.physical_core_id;
	}
	record.duration_s = record.end_ns > record.start_ns
	                        ? static_cast<double>(record.end_ns - record.start_ns) / static_cast<double>(NSEC_PER_SEC)
	                        : 0;
	record.migrated = record.logical_cpu_id != record.end_logical_cpu_id || record.socket_id != record.end_socket_id ||
	                  record.physical_core_id != record.end_physical_core_id;
	record.hardware_state_stable = !record.migrated && record.socket_id >= 0 && record.physical_core_id >= 0;
	if (record.hardware_state_stable && query && record.target_core_frequency_khz > 0 &&
	    record.target_uncore_frequency_khz > 0 &&
	    DatabaseInstance::GetDatabase(*query->context).GetQuerySchedulerPolicy() == QuerySchedulerPolicy::SLA_ENERGY) {
		auto end_target = DatabaseInstance::GetDatabase(*query->context)
		                      .GetQuerySLAScheduler()
		                      .GetHardwareManager()
		                      .GetAppliedConfigurationForCPU(record.end_logical_cpu_id);
		record.hardware_state_stable = end_target.core_frequency_khz == record.target_core_frequency_khz &&
		                               end_target.uncore_frequency_khz == record.target_uncore_frequency_khz;
	}
	PeriodicEnergyRuntime::Get().EndSegment(record);
	active = false;
}

void EnergySegmentScope::SetEndCPUHint(int end_cpu_hint_p) {
	end_cpu_hint = end_cpu_hint_p;
}

void EnergySegmentScope::SetWork(uint64_t tuples, uint64_t chunks, uint64_t work_units) {
	if (!active) {
		return;
	}
	record.tuples = tuples;
	record.chunks = chunks;
	record.work_units = work_units;
}

} // namespace duckdb
