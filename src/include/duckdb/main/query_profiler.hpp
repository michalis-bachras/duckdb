//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/enums/profiler_format.hpp"
#include "duckdb/common/enums/explain_format.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/source_throughput.hpp"
#include "duckdb/main/pipeline_dvfs_profiler.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/main/profiling_utils.hpp"

namespace duckdb {

class ClientContext;
class ExpressionExecutor;
class ProfilingNode;
class PhysicalOperator;
class SQLStatement;
struct ActiveTimer;

enum class ProfilingCoverage : uint8_t { SELECT = 0, ALL = 1 };

struct OperatorInformation {
	explicit OperatorInformation() {
	}

	string name;

	double time = 0;
	idx_t elements_returned = 0;
	idx_t result_set_size = 0;
	idx_t system_peak_buffer_manager_memory = 0;
	idx_t system_peak_temp_directory_size = 0;
	idx_t rows_scanned = 0;

	InsertionOrderPreservingMap<string> extra_info;

	template <typename T>
	void AddMetric(MetricType type, T metric) {
		switch (type) {
		case MetricType::OPERATOR_TIMING:
			time += metric;
			break;
		case MetricType::OPERATOR_CARDINALITY:
			elements_returned += LossyNumericCast<idx_t>(metric);
			break;
		case MetricType::RESULT_SET_SIZE:
			result_set_size += LossyNumericCast<idx_t>(metric);
			break;
		case MetricType::SYSTEM_PEAK_BUFFER_MEMORY: {
			if (metric > system_peak_buffer_manager_memory) {
				system_peak_buffer_manager_memory += LossyNumericCast<idx_t>(metric);
			}
			break;
		}
		case MetricType::SYSTEM_PEAK_TEMP_DIR_SIZE: {
			if (metric > system_peak_temp_directory_size) {
				system_peak_temp_directory_size = LossyNumericCast<idx_t>(metric);
			}
			break;
		}
		case MetricType::OPERATOR_ROWS_SCANNED:
			rows_scanned = LossyNumericCast<idx_t>(metric);
			break;
		default:
			throw InternalException("OperatorProfiler: Unknown metric type");
		}
	}
};

//! One PipelineTask execution interval, used to align external per-core PMU samples with active pipelines.
struct PipelineTaskProfilingInfo {
	idx_t task_id = 0;
	idx_t pipeline_id = 0;
	string task_signature_key;
	uint64_t thread_id = 0;
	int start_cpu = -1;
	int end_cpu = -1;
	uint64_t start_ns = 0;
	uint64_t end_ns = 0;
	string source_tuple_kind = "unknown";
	string source_tuple_confidence = "unknown";
	idx_t source_tuples_touched = 0;
	idx_t source_chunks_touched = 0;
	idx_t source_native_units_touched = 0;
	string source_native_unit;
	bool adaptive_morsel_candidate = false;
	idx_t pipeline_input_tuples = 0;
	idx_t pipeline_input_chunks = 0;
	double estimated_tuples_per_task_s = 0;
};

//! Coarse pipeline-level timing information emitted into the existing JSON query profile.
struct PipelineProfilingInfo {
	idx_t pipeline_id = 0;

	string source_name;
	string source_type;
	vector<string> operator_names;
	vector<string> operator_types;
	vector<idx_t> operator_estimated_cardinalities;
	string sink_name;
	string sink_type;
	idx_t source_estimated_cardinality = 0;
	idx_t sink_estimated_cardinality = 0;
	idx_t operator_count = 0;
	string operator_type_sequence;
	double estimated_task_input_rows = 0;
	string estimated_task_input_rows_bucket;
	idx_t source_max_threads = 0;
	string source_input_kind = "unknown";
	string source_input_confidence = "unknown";
	idx_t planned_input_rows = 0;
	idx_t planned_input_chunks_equiv = 0;
	idx_t planned_input_native_units = 0;
	string planned_input_native_unit;
	double planned_task_input_chunks_equiv = 0;
	string planned_task_input_chunks_bucket = "unknown";

	idx_t schedule_count = 0;
	idx_t task_count = 0;
	uint64_t start_ns = 0;
	uint64_t tasks_done_ns = 0;
	uint64_t finish_done_ns = 0;

	bool throughput_enabled = false;
	string source_tuple_kind = "unknown";
	string source_tuple_confidence = "unknown";
	idx_t source_tuples_touched = 0;
	idx_t source_chunks_touched = 0;
	idx_t source_native_units_touched = 0;
	string source_native_unit;
	bool adaptive_morsel_candidate = false;
	idx_t pipeline_input_tuples = 0;
	idx_t pipeline_input_chunks = 0;
	idx_t pipeline_input_task_count = 0;
	uint64_t pipeline_input_task_duration_ns = 0;
	uint64_t throughput_task_duration_ns = 0;
	idx_t throughput_task_count = 0;
	string task_signature_key;
	double estimated_tuples_per_task_s = 0;
	double last_task_tuples_per_s = 0;
	double throughput_ewma_alpha = 0.8;
	idx_t throughput_sample_count = 0;
	idx_t throughput_sample_tuples = 0;
	uint64_t throughput_sample_ns = 0;

	bool dvfs_metrics_enabled = false;
	bool dvfs_measurement_stopped = false;
	bool rapl_supported = false;
	string rapl_source;
	string rapl_error;
	bool perf_supported = false;
	string perf_scope;
	string perf_cgroup_path;
	string perf_error;
	uint64_t measurement_start_overhead_ns = 0;
	uint64_t measurement_end_overhead_ns = 0;
	uint64_t rapl_start_overhead_ns = 0;
	uint64_t perf_start_overhead_ns = 0;
	uint64_t rapl_end_overhead_ns = 0;
	uint64_t perf_end_overhead_ns = 0;

	vector<uint64_t> rapl_package_start_uj;
	vector<uint64_t> rapl_package_max_uj;
	vector<uint64_t> rapl_dram_start_uj;
	vector<uint64_t> rapl_dram_max_uj;
	double cpu_package_j = 0;
	double dram_j = 0;

	idx_t rapl_perf_fd_count = 0;
	idx_t rapl_perf_group_count = 0;
	double rapl_perf_min_running_pct = 0;
	vector<PipelinePerfGroupSnapshot> rapl_perf_start_snapshots;

	idx_t perf_fd_count = 0;
	idx_t perf_group_count = 0;
	idx_t perf_cpu_count = 0;
	double perf_min_running_pct = 0;
	double cycles = 0;
	double instructions = 0;
	double cache_references = 0;
	double cache_misses = 0;
	vector<PipelinePerfGroupSnapshot> perf_start_snapshots;
	vector<PipelinePerfCPUCounters> per_cpu_perf;
};

//! The OperatorProfiler measures timings of individual operators
//! This class exists once for all operators and collects `OperatorInfo` for each operator
class OperatorProfiler {
	friend class QueryProfiler;

public:
	DUCKDB_API explicit OperatorProfiler(ClientContext &context);
	~OperatorProfiler() {
	}

public:
	DUCKDB_API void StartOperator(optional_ptr<const PhysicalOperator> phys_op);
	DUCKDB_API void EndOperator(optional_ptr<DataChunk> chunk);
	DUCKDB_API void FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate);

	//! Adds the timings in the OperatorProfiler (tree) to the QueryProfiler (tree).
	DUCKDB_API void Flush(const PhysicalOperator &phys_op);
	DUCKDB_API OperatorInformation &GetOperatorInfo(const PhysicalOperator &phys_op);
	DUCKDB_API bool OperatorInfoIsInitialized(const PhysicalOperator &phys_op);

public:
	ClientContext &context;

private:
	//! Whether or not the profiler is enabled
	bool enabled;
	//! Sub-settings for the operator profiler
	profiler_settings_t settings;

	//! The timer used to time the execution time of the individual Physical Operators
	Profiler op;
	//! The stack of Physical Operators that are currently active
	optional_ptr<const PhysicalOperator> active_operator;
	//! A mapping of physical operators to profiled operator information.
	reference_map_t<const PhysicalOperator, OperatorInformation> operator_infos;
};

//! QueryProfiler collects the profiling metrics of a query.
class QueryProfiler {
public:
	using TreeMap = reference_map_t<const PhysicalOperator, reference<ProfilingNode>>;

public:
	DUCKDB_API explicit QueryProfiler(ClientContext &context);

public:
	DUCKDB_API bool IsEnabled() const;
	DUCKDB_API bool IsDetailedEnabled() const;
	DUCKDB_API ProfilerPrintFormat GetPrintFormat(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API bool PrintOptimizerOutput() const;
	DUCKDB_API string GetSaveLocation() const;

	DUCKDB_API static QueryProfiler &Get(ClientContext &context);

	DUCKDB_API void Start(const string &query);
	DUCKDB_API void Reset();
	DUCKDB_API void StartQuery(const string &query, bool is_explain_analyze = false, bool start_at_optimizer = false);
	DUCKDB_API void StartRequestMetadataQuery(const string &query);
	DUCKDB_API void EndQuery();
	//! Finalize query metrics for output; safe to call multiple times.
	DUCKDB_API void FinalizeMetrics();

	//! Adds amount to a specific metric type.
	DUCKDB_API void AddToCounter(MetricType type, const idx_t amount);

	//! Start/End a timer for a specific metric type.
	DUCKDB_API ActiveTimer StartTimer(MetricType type);

	DUCKDB_API void StartExplainAnalyze();

	//! Adds the timings gathered by an OperatorProfiler to this query profiler
	DUCKDB_API void Flush(OperatorProfiler &profiler);
	//! Adds the top level query information to the global profiler.
	DUCKDB_API void SetBlockedTime(const double &blocked_thread_time);

	DUCKDB_API void StartPhase(MetricType phase_metric);
	DUCKDB_API void EndPhase();

	DUCKDB_API void Initialize(const PhysicalOperator &root);

	//! Register and update coarse pipeline-level timings for JSON profiling output.
	DUCKDB_API idx_t RegisterPipelineProfile(const PhysicalOperator &source,
	                                         const vector<reference<PhysicalOperator>> &operators,
	                                         optional_ptr<PhysicalOperator> sink);
	DUCKDB_API void RecordPipelineProfileStart(idx_t pipeline_id, idx_t task_count, idx_t source_max_threads,
	                                           const SourceInputVolume &source_input_volume);
	DUCKDB_API void RecordPipelineProfileTasksDone(idx_t pipeline_id);
	DUCKDB_API void RecordPipelineProfileFinishDone(idx_t pipeline_id);
	DUCKDB_API idx_t RecordPipelineTaskStart(idx_t pipeline_id, uint64_t thread_id, int start_cpu);
	DUCKDB_API void RecordPipelineTaskEnd(idx_t task_id, int end_cpu, const SourceThroughputCounters &source_throughput,
	                                      idx_t pipeline_input_tuples, idx_t pipeline_input_chunks,
	                                      const SourceThroughputEstimate &throughput_estimate);
	DUCKDB_API vector<PipelineProfilingInfo> GetPipelineProfilesSnapshot() const;

	DUCKDB_API string QueryTreeToString() const;
	DUCKDB_API void QueryTreeToStream(std::ostream &str) const;
	DUCKDB_API void Print();

	//! return the printed as a string. Unlike ToString, which is always formatted as a string,
	//! the return value is formatted based on the current print format (see GetPrintFormat()).
	DUCKDB_API string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const;
	DUCKDB_API string ToString(ProfilerPrintFormat format) const;

	// Sanitize a Value::MAP
	static Value JSONSanitize(const Value &input);
	static string JSONSanitize(const string &text);
	static string DrawPadded(const string &str, idx_t width);
	DUCKDB_API void ToLog() const;
	DUCKDB_API string ToJSON() const;
	DUCKDB_API void WriteToFile(const char *path, string &info) const;
	DUCKDB_API idx_t GetBytesRead() const;
	DUCKDB_API idx_t GetBytesWritten() const;

	idx_t OperatorSize() {
		return tree_map.size();
	}

	void Finalize(ProfilingNode &node);

	//! Return the root of the query tree.
	optional_ptr<ProfilingNode> GetRoot() {
		return root.get();
	}

	//! Provides access to the root of the query tree, but ensures there are no concurrent modifications.
	//! This can be useful when implementing continuous profiling or making customizations.
	DUCKDB_API void GetRootUnderLock(const std::function<void(optional_ptr<ProfilingNode>)> &callback) {
		lock_guard<std::mutex> guard(lock);
		callback(GetRoot());
	}

private:
	unique_ptr<ProfilingNode> CreateTree(const PhysicalOperator &root, const profiler_settings_t &settings,
	                                     const idx_t depth = 0);
	void Render(const ProfilingNode &node, std::ostream &str) const;
	string RenderDisabledMessage(ProfilerPrintFormat format) const;

private:
	ClientContext &context;

	//! Whether or not the query profiler is running
	bool running;
	//! The lock used for accessing the global query profiler or flushing information to it from a thread
	mutable std::mutex lock;

	//! Whether or not the query requires profiling
	bool query_requires_profiling;

	//! The root of the query tree
	unique_ptr<ProfilingNode> root;

	//! Top level query information.
	QueryMetrics query_metrics;

	//! A map of a Physical Operator pointer to a tree node
	TreeMap tree_map;
	//! Pipeline-level profiling records emitted alongside the operator tree.
	vector<PipelineProfilingInfo> pipeline_profiles;
	//! Pipeline task execution intervals emitted alongside the operator tree.
	vector<PipelineTaskProfilingInfo> pipeline_task_profiles;
	//! Fast lookup for pipeline records by stable id.
	unordered_map<idx_t, idx_t> pipeline_profile_index;
	//! Fast lookup for task records by stable id.
	unordered_map<idx_t, idx_t> pipeline_task_profile_index;
	//! Next stable per-query pipeline profile id.
	idx_t next_pipeline_profile_id;
	//! Next stable per-query pipeline task profile id.
	idx_t next_pipeline_task_profile_id;
	//! Optional direct DVFS measurement support for pipeline intervals.
	PipelineDVFSProfiler pipeline_dvfs_profiler;
	//! Whether or not we are running as part of a explain_analyze query
	bool is_explain_analyze;
	//! Whether root metrics have been finalized for output
	bool metrics_finalized;
	//! Whether the profiler was started only to collect request metadata pipeline profiles.
	bool request_metadata_pipeline_profiles;

public:
	const TreeMap &GetTreeMap() const {
		return tree_map;
	}

private:
	//! The timer used to time the individual phases of the planning process
	Profiler phase_profiler;
	//! A mapping of the phase names to the timings
	using PhaseTimingStorage = unordered_map<MetricType, double, MetricTypeHashFunction>;
	PhaseTimingStorage phase_timings;
	using PhaseTimingItem = PhaseTimingStorage::value_type;
	//! The stack of currently active phases
	vector<MetricType> phase_stack;

private:
	void MoveOptimizerPhasesToRoot();
	void FinalizeMetricsInternal();
	PipelineProfilingInfo *GetPipelineProfile(idx_t pipeline_id);

	//! Check whether or not an operator type requires query profiling. If none of the ops in a query require profiling
	//! no profiling information is output.
	bool OperatorRequiresProfiling(const PhysicalOperatorType op_type);
	ExplainFormat GetExplainFormat(ProfilerPrintFormat format) const;
};

} // namespace duckdb
