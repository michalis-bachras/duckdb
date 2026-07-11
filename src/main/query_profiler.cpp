#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/profiling_utils.hpp"
#include "duckdb/main/profiling_info.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "yyjson.hpp"
#include "yyjson_utils.hpp"

#include <algorithm>
#include <cmath>
#include <utility>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), next_pipeline_profile_id(1),
      next_pipeline_task_profile_id(1), is_explain_analyze(false), metrics_finalized(false),
      request_metadata_pipeline_profiles(false) {
}

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze || ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return !is_explain_analyze && ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat(ExplainFormat format) const {
	auto print_format = ClientConfig::GetConfig(context).profiler_print_format;
	switch (format) {
	case ExplainFormat::DEFAULT:
		if (print_format != ProfilerPrintFormat::NO_OUTPUT) {
			return print_format;
		}
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case ExplainFormat::TEXT:
		return ProfilerPrintFormat::QUERY_TREE;
	case ExplainFormat::JSON:
		return ProfilerPrintFormat::JSON;
	case ExplainFormat::HTML:
		return ProfilerPrintFormat::HTML;
	case ExplainFormat::GRAPHVIZ:
		return ProfilerPrintFormat::GRAPHVIZ;
	case ExplainFormat::MERMAID:
		return ProfilerPrintFormat::MERMAID;
	default:
		throw NotImplementedException("No mapping from ExplainFormat::%s to ProfilerPrintFormat",
		                              EnumUtil::ToString(format));
	}
}

ExplainFormat QueryProfiler::GetExplainFormat(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return ExplainFormat::TEXT;
	case ProfilerPrintFormat::JSON:
		return ExplainFormat::JSON;
	case ProfilerPrintFormat::HTML:
		return ExplainFormat::HTML;
	case ProfilerPrintFormat::GRAPHVIZ:
		return ExplainFormat::GRAPHVIZ;
	case ProfilerPrintFormat::MERMAID:
		return ExplainFormat::MERMAID;
	case ProfilerPrintFormat::NO_OUTPUT:
		throw InternalException("Should not attempt to get ExplainFormat for ProfilerPrintFormat::NO_OUTPUT");
	default:
		throw NotImplementedException("No mapping from ProfilePrintFormat::%s to ExplainFormat",
		                              EnumUtil::ToString(format));
	}
}

bool QueryProfiler::PrintOptimizerOutput() const {
	return GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled();
}

string QueryProfiler::GetSaveLocation() const {
	return is_explain_analyze ? string() : ClientConfig::GetConfig(context).profiler_save_location;
}

QueryProfiler &QueryProfiler::Get(ClientContext &context) {
	return *ClientData::Get(context).profiler;
}

void QueryProfiler::Start(const string &query) {
	Reset();
	running = true;
	query_metrics.query_name = query;
	query_metrics.latency_timer = make_uniq<ActiveTimer>(StartTimer(MetricType::LATENCY));
}

void QueryProfiler::Reset() {
	pipeline_dvfs_profiler.Reset();
	tree_map.clear();
	pipeline_profiles.clear();
	pipeline_task_profiles.clear();
	pipeline_profile_index.clear();
	pipeline_task_profile_index.clear();
	next_pipeline_profile_id = 1;
	next_pipeline_task_profile_id = 1;
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();
	running = false;
	query_metrics.Reset();
	metrics_finalized = false;
	request_metadata_pipeline_profiles = false;
}

PipelineProfilingInfo *QueryProfiler::GetPipelineProfile(idx_t pipeline_id) {
	auto entry = pipeline_profile_index.find(pipeline_id);
	if (entry == pipeline_profile_index.end()) {
		return nullptr;
	}
	return &pipeline_profiles[entry->second];
}

static void AppendPipelineOperatorType(string &sequence, const string &operator_type) {
	if (!sequence.empty()) {
		sequence += ">";
	}
	sequence += operator_type;
}

static string EstimatedTaskInputRowsBucket(double rows) {
	if (!std::isfinite(rows) || rows <= 0) {
		return "unknown";
	}
	if (rows < 1) {
		return "<1";
	}
	auto exponent = LossyNumericCast<int>(std::floor(std::log10(rows)));
	return StringUtil::Format("1e%d-1e%d", exponent, exponent + 1);
}

static string SourceThroughputSignatureKey(const PipelineProfilingInfo &profile) {
	return StringUtil::Format("%s::%s::%s", profile.operator_type_sequence, profile.source_input_kind,
	                          profile.planned_task_input_chunks_bucket);
}

static SourceInputVolume ApplySourceInputVolumeFallback(const PipelineProfilingInfo &profile,
                                                        SourceInputVolume volume) {
	if (volume.kind != "unknown" || profile.source_type != "UNGROUPED_AGGREGATE") {
		return volume;
	}
	volume.kind = SourceThroughputKindToString(SourceThroughputKind::SINGLE_AGGREGATE_ROW);
	volume.confidence = "exact";
	volume.rows = 1;
	volume.chunks_equiv = 1;
	volume.native_units = 1;
	volume.native_unit = "row";
	return volume;
}

static SourceThroughputCounters ApplySourceThroughputFallback(const PipelineProfilingInfo &profile,
                                                              SourceThroughputCounters counters) {
	if (counters.reported) {
		return counters;
	}
	if (profile.source_input_kind == "hash_join_no_source_scan") {
		counters.AddTuples(0, SourceThroughputKind::NO_SOURCE_SCAN, "exact", false, 0, 0, "none");
		return counters;
	}
	if (SourceThroughputKindFromString(profile.source_input_kind) == SourceThroughputKind::SINGLE_AGGREGATE_ROW) {
		counters.AddTuples(1, SourceThroughputKind::SINGLE_AGGREGATE_ROW, "exact", false, 1, 1, "row");
		return counters;
	}
	if (profile.source_input_kind != "unknown" && profile.source_input_confidence != "unknown" &&
	    profile.planned_input_rows == 0 && profile.planned_input_chunks_equiv == 0) {
		auto native_unit =
		    profile.planned_input_native_unit.empty() ? string("none") : profile.planned_input_native_unit;
		counters.AddTuples(0, profile.source_input_kind, profile.source_input_confidence, false, 0, 0, native_unit);
		return counters;
	}
	return counters;
}

static void MergeSourceThroughputCounters(PipelineProfilingInfo &profile, const SourceThroughputCounters &counters) {
	if (!counters.reported) {
		return;
	}
	if (profile.source_tuple_kind == "unknown") {
		profile.source_tuple_kind = counters.tuple_kind;
		profile.source_tuple_confidence = counters.tuple_confidence;
		profile.source_native_unit = counters.native_unit;
	} else if (profile.source_tuple_kind != counters.tuple_kind) {
		profile.source_tuple_kind = SourceThroughputKindToString(SourceThroughputKind::MIXED_SOURCE_TUPLES);
		profile.source_tuple_confidence = "estimate";
	} else if (profile.source_tuple_confidence != counters.tuple_confidence &&
	           profile.source_tuple_confidence != "estimate") {
		profile.source_tuple_confidence = "estimate";
	}
	profile.source_tuples_touched += counters.tuples_touched;
	profile.source_chunks_touched += counters.chunks_touched;
	profile.source_native_units_touched += counters.native_units_touched;
	profile.adaptive_morsel_candidate = profile.adaptive_morsel_candidate || counters.adaptive_morsel_candidate;
}

idx_t QueryProfiler::RegisterPipelineProfile(const PhysicalOperator &source,
                                             const vector<reference<PhysicalOperator>> &operators,
                                             optional_ptr<PhysicalOperator> sink) {
	lock_guard<std::mutex> guard(lock);
	auto profiler_enabled = IsEnabled();
	auto request_metadata_enabled =
	    request_metadata_pipeline_profiles || QueryRequestMetadataManager::HasActiveMetadata(context);
	if (!running || (!profiler_enabled && !request_metadata_enabled)) {
		return 0;
	}
	auto &pipeline_settings = ClientConfig::GetConfig(context).pipeline_profiling;
	if (!pipeline_settings.IsAnyEnabled() && !request_metadata_enabled) {
		return 0;
	}
	if (pipeline_settings.IsAnyEnabled()) {
		pipeline_dvfs_profiler.Initialize(pipeline_settings);
	}

	PipelineProfilingInfo profile;
	profile.pipeline_id = next_pipeline_profile_id++;
	profile.estimated_task_input_rows_bucket = "unknown";
	profile.source_name = source.GetName();
	profile.source_type = EnumUtil::ToString(source.type);
	profile.source_estimated_cardinality = source.estimated_cardinality;
	profile.operator_count = 1;
	AppendPipelineOperatorType(profile.operator_type_sequence, profile.source_type);
	for (auto &op_ref : operators) {
		auto &op = op_ref.get();
		profile.operator_names.push_back(op.GetName());
		profile.operator_types.push_back(EnumUtil::ToString(op.type));
		profile.operator_estimated_cardinalities.push_back(op.estimated_cardinality);
		profile.operator_count++;
		AppendPipelineOperatorType(profile.operator_type_sequence, profile.operator_types.back());
	}
	if (sink) {
		profile.sink_name = sink->GetName();
		profile.sink_type = EnumUtil::ToString(sink->type);
		profile.sink_estimated_cardinality = sink->estimated_cardinality;
		profile.operator_count++;
		AppendPipelineOperatorType(profile.operator_type_sequence, profile.sink_type);
	}
	pipeline_profiles.push_back(std::move(profile));
	pipeline_profile_index[pipeline_profiles.back().pipeline_id] = pipeline_profiles.size() - 1;
	return pipeline_profiles.back().pipeline_id;
}

void QueryProfiler::RecordPipelineProfileStart(idx_t pipeline_id, idx_t task_count, idx_t source_max_threads,
                                               const SourceInputVolume &source_input_volume) {
	if (!pipeline_id) {
		return;
	}
	lock_guard<std::mutex> guard(lock);
	auto profile = GetPipelineProfile(pipeline_id);
	if (!profile) {
		return;
	}
	if (!profile->start_ns) {
		profile->dvfs_metrics_enabled = pipeline_dvfs_profiler.MetricsEnabled();
		profile->throughput_enabled = ClientConfig::GetConfig(context).pipeline_profiling.throughput;
		profile->source_max_threads = source_max_threads;
		auto planned_volume = ApplySourceInputVolumeFallback(*profile, source_input_volume);
		profile->source_input_kind = planned_volume.kind;
		profile->source_input_confidence = planned_volume.confidence;
		profile->planned_input_rows = planned_volume.rows;
		// Historical counterpart of live remaining work. Scheduler ETA should derive native-unit throughput as
		// planned_input_chunks_equiv / task_runtime, rather than using the tuple/sec EWMA.
		profile->planned_input_chunks_equiv = planned_volume.chunks_equiv;
		profile->planned_input_native_units = planned_volume.native_units;
		profile->planned_input_native_unit = planned_volume.native_unit;
		if (task_count > 0) {
			profile->estimated_task_input_rows =
			    static_cast<double>(profile->source_estimated_cardinality) / static_cast<double>(task_count);
			profile->estimated_task_input_rows_bucket =
			    EstimatedTaskInputRowsBucket(profile->estimated_task_input_rows);
			if (profile->planned_input_chunks_equiv > 0) {
				profile->planned_task_input_chunks_equiv =
				    static_cast<double>(profile->planned_input_chunks_equiv) / static_cast<double>(task_count);
				profile->planned_task_input_chunks_bucket =
				    EstimatedTaskInputRowsBucket(profile->planned_task_input_chunks_equiv);
			} else if (profile->source_input_kind == "hash_join_no_source_scan") {
				profile->planned_task_input_chunks_bucket =
				    SourceThroughputKindToString(SourceThroughputKind::NO_SOURCE_SCAN);
			}
		}
		profile->task_signature_key = SourceThroughputSignatureKey(*profile);
		auto measurement_begin_ns = PipelineDVFSProfiler::TimestampNs();
		if (profile->dvfs_metrics_enabled) {
			pipeline_dvfs_profiler.Start(*profile);
		}
		profile->start_ns = PipelineDVFSProfiler::TimestampNs();
		profile->measurement_start_overhead_ns = profile->start_ns - measurement_begin_ns;
	}
	profile->schedule_count++;
	profile->task_count += task_count;
}

void QueryProfiler::RecordPipelineProfileTasksDone(idx_t pipeline_id) {
	if (!pipeline_id) {
		return;
	}
	lock_guard<std::mutex> guard(lock);
	auto profile = GetPipelineProfile(pipeline_id);
	if (!profile) {
		return;
	}
	profile->tasks_done_ns = PipelineDVFSProfiler::TimestampNs();
	if (profile->dvfs_metrics_enabled && !profile->dvfs_measurement_stopped) {
		auto measurement_begin_ns = profile->tasks_done_ns;
		pipeline_dvfs_profiler.Stop(*profile);
		profile->measurement_end_overhead_ns = PipelineDVFSProfiler::TimestampNs() - measurement_begin_ns;
		profile->dvfs_measurement_stopped = true;
	}
}

void QueryProfiler::RecordPipelineProfileFinishDone(idx_t pipeline_id) {
	if (!pipeline_id) {
		return;
	}
	lock_guard<std::mutex> guard(lock);
	auto profile = GetPipelineProfile(pipeline_id);
	if (!profile) {
		return;
	}
	profile->finish_done_ns = PipelineDVFSProfiler::TimestampNs();
}

idx_t QueryProfiler::RecordPipelineTaskStart(idx_t pipeline_id, uint64_t thread_id, int start_cpu) {
	auto &pipeline_settings = ClientConfig::GetConfig(context).pipeline_profiling;
	if (!pipeline_id || (!pipeline_dvfs_profiler.TaskTraceEnabled() && !pipeline_settings.throughput)) {
		return 0;
	}
	lock_guard<std::mutex> guard(lock);
	if (!running || !IsEnabled() || !GetPipelineProfile(pipeline_id)) {
		return 0;
	}
	PipelineTaskProfilingInfo task_profile;
	task_profile.task_id = next_pipeline_task_profile_id++;
	task_profile.pipeline_id = pipeline_id;
	task_profile.thread_id = thread_id;
	task_profile.start_cpu = start_cpu;
	task_profile.start_ns = PipelineDVFSProfiler::TimestampNs();
	pipeline_task_profiles.push_back(std::move(task_profile));
	pipeline_task_profile_index[pipeline_task_profiles.back().task_id] = pipeline_task_profiles.size() - 1;
	return pipeline_task_profiles.back().task_id;
}

void QueryProfiler::RecordPipelineTaskEnd(idx_t task_id, int end_cpu, const SourceThroughputCounters &source_throughput,
                                          idx_t pipeline_input_tuples, idx_t pipeline_input_chunks,
                                          const SourceThroughputEstimate &throughput_estimate) {
	if (!task_id) {
		return;
	}
	lock_guard<std::mutex> guard(lock);
	auto entry = pipeline_task_profile_index.find(task_id);
	if (entry == pipeline_task_profile_index.end()) {
		return;
	}
	auto &task_profile = pipeline_task_profiles[entry->second];
	task_profile.end_cpu = end_cpu;
	task_profile.end_ns = PipelineDVFSProfiler::TimestampNs();
	auto duration_ns = task_profile.end_ns >= task_profile.start_ns ? task_profile.end_ns - task_profile.start_ns : 0;
	auto profile = GetPipelineProfile(task_profile.pipeline_id);
	if (!profile) {
		return;
	}
	auto counters = ApplySourceThroughputFallback(*profile, source_throughput);
	task_profile.source_tuple_kind = counters.tuple_kind;
	task_profile.source_tuple_confidence = counters.tuple_confidence;
	task_profile.source_tuples_touched = counters.tuples_touched;
	task_profile.source_chunks_touched = counters.chunks_touched;
	task_profile.source_native_units_touched = counters.native_units_touched;
	task_profile.source_native_unit = counters.native_unit;
	task_profile.adaptive_morsel_candidate = counters.adaptive_morsel_candidate;
	task_profile.pipeline_input_tuples = pipeline_input_tuples;
	task_profile.pipeline_input_chunks = pipeline_input_chunks;

	if (pipeline_input_tuples > 0 || pipeline_input_chunks > 0) {
		profile->pipeline_input_tuples += pipeline_input_tuples;
		profile->pipeline_input_chunks += pipeline_input_chunks;
		profile->pipeline_input_task_count++;
		profile->pipeline_input_task_duration_ns += duration_ns;
	}

	if (profile->throughput_enabled && counters.reported) {
		profile->throughput_task_duration_ns += duration_ns;
		profile->throughput_task_count++;
		MergeSourceThroughputCounters(*profile, counters);
		task_profile.task_signature_key = profile->task_signature_key;
		task_profile.estimated_tuples_per_task_s = throughput_estimate.estimated_tuples_per_task_s;
		profile->estimated_tuples_per_task_s = throughput_estimate.estimated_tuples_per_task_s;
		profile->last_task_tuples_per_s = throughput_estimate.last_task_tuples_per_s;
		profile->throughput_ewma_alpha = throughput_estimate.alpha;
		profile->throughput_sample_count = throughput_estimate.sample_count;
		profile->throughput_sample_tuples = throughput_estimate.sample_tuples;
		profile->throughput_sample_ns = throughput_estimate.sample_ns;
	}
}

void QueryProfiler::StartQuery(const string &query, bool is_explain_analyze_p, bool start_at_optimizer) {
	lock_guard<std::mutex> guard(lock);
	if (is_explain_analyze_p) {
		StartExplainAnalyze();
	}
	if (!IsEnabled()) {
		return;
	}
	if (start_at_optimizer && !PrintOptimizerOutput()) {
		// This is the StartQuery call before the optimizer, but we don't have to print optimizer output
		return;
	}
	if (running) {
		// Called while already running: this should only happen when we print optimizer output
		// D_ASSERT(PrintOptimizerOutput());
		return;
	}
	Start(query);
}

void QueryProfiler::StartRequestMetadataQuery(const string &query) {
	lock_guard<std::mutex> guard(lock);
	if (running || IsEnabled() || !QueryRequestMetadataManager::HasActiveMetadata(context)) {
		return;
	}
	Start(query);
	request_metadata_pipeline_profiles = true;
}

bool QueryProfiler::OperatorRequiresProfiling(const PhysicalOperatorType op_type) {
	const auto &config = ClientConfig::GetConfig(context);
	if (config.profiling_coverage == ProfilingCoverage::ALL) {
		return true;
	}

	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::RECURSIVE_KEY_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
	case PhysicalOperatorType::EXTENSION:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::Finalize(ProfilingNode &node) {
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		Finalize(*child);

		auto &info = node.GetProfilingInfo();
		auto type = PhysicalOperatorType(info.GetMetricValue<uint8_t>(MetricType::OPERATOR_TYPE));
		if (type == PhysicalOperatorType::UNION &&
		    info.Enabled(info.expanded_settings, MetricType::OPERATOR_CARDINALITY)) {
			auto &child_info = child->GetProfilingInfo();
			auto value = child_info.metrics[MetricType::OPERATOR_CARDINALITY].GetValue<idx_t>();
			info.MetricSum(MetricType::OPERATOR_CARDINALITY, value);
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	is_explain_analyze = true;
}

void QueryProfiler::EndQuery() {
	unique_lock<std::mutex> guard(lock);
	auto profiler_enabled = IsEnabled();
	auto request_metadata_only = request_metadata_pipeline_profiles && !profiler_enabled;
	if (!running || (!profiler_enabled && !request_metadata_pipeline_profiles)) {
		return;
	}

	if (profiler_enabled) {
		FinalizeMetricsInternal();
	}
	running = false;
	pipeline_dvfs_profiler.Reset();
	bool emit_output = false;

	// Print or output the query profiling after query termination.
	// EXPLAIN ANALYZE output is not written by the profiler.
	if (IsEnabled() && !is_explain_analyze && ClientConfig::GetConfig(context).emit_profiler_output) {
		emit_output = true;
	}

	is_explain_analyze = false;
	request_metadata_pipeline_profiles = false;

	guard.unlock();

	if (request_metadata_only) {
		return;
	}

	// To log is inexpensive, whether to log or not depends on whether logging is active
	ToLog();

	if (emit_output) {
		string tree = ToString();
		auto save_location = GetSaveLocation();

		if (save_location.empty()) {
			Printer::Print(tree);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), tree);
		}
	}
}

vector<PipelineProfilingInfo> QueryProfiler::GetPipelineProfilesSnapshot() const {
	lock_guard<std::mutex> guard(lock);
	return pipeline_profiles;
}

void QueryProfiler::FinalizeMetrics() {
	lock_guard<std::mutex> guard(lock);
	FinalizeMetricsInternal();
}

void QueryProfiler::AddToCounter(const MetricType type, const idx_t amount) {
	if (IsEnabled()) {
		query_metrics.UpdateMetric(type, amount);
	}
}

idx_t QueryProfiler::GetBytesRead() const {
	return query_metrics.GetMetricValue(MetricType::TOTAL_BYTES_READ);
}

idx_t QueryProfiler::GetBytesWritten() const {
	return query_metrics.GetMetricValue(MetricType::TOTAL_BYTES_WRITTEN);
}

ActiveTimer QueryProfiler::StartTimer(const MetricType type) {
	return ActiveTimer(query_metrics, type, IsEnabled());
}

string QueryProfiler::ToString(ExplainFormat explain_format) const {
	return ToString(GetPrintFormat(explain_format));
}

string QueryProfiler::ToString(ProfilerPrintFormat format) const {
	if (!IsEnabled()) {
		return RenderDisabledMessage(format);
	}
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::HTML:
	case ProfilerPrintFormat::GRAPHVIZ:
	case ProfilerPrintFormat::MERMAID: {
		lock_guard<std::mutex> guard(lock);
		// checking the tree to ensure the query is really empty
		// the query string is empty when a logical plan is deserialized
		if (query_metrics.query_name.empty() && !root) {
			return "";
		}
		auto renderer = TreeRenderer::CreateRenderer(GetExplainFormat(format));
		stringstream str;
		renderer->Render(*root, str);
		return str.str();
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::StartPhase(MetricType phase_metric) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	// start a new phase
	phase_stack.push_back(phase_metric);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	D_ASSERT(!phase_stack.empty());

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (!phase_stack.empty()) {
		phase_profiler.Start();
	}
}

OperatorProfiler::OperatorProfiler(ClientContext &context) : context(context) {
	enabled = QueryProfiler::Get(context).IsEnabled();
	auto &context_metrics = ClientConfig::GetConfig(context).profiler_settings;

	// Expand.
	for (const auto metric : context_metrics) {
		settings.insert(metric);
		ProfilingInfo::Expand(settings, metric);
	}

	// Reduce.
	auto root_metrics = MetricsUtils::GetRootScopeMetrics();
	for (const auto metric : root_metrics) {
		settings.erase(metric);
	}
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}
	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}
	active_operator = phys_op;

	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricType::EXTRA_INFO)) {
			if (!OperatorInfoIsInitialized(*active_operator)) {
				// first time calling into this operator - fetch the info
				auto &info = GetOperatorInfo(*active_operator);
				auto params = active_operator->ParamsToString();
				info.extra_info = params;
			}
		}

		// Start the timing of the current operator.
		if (ProfilingInfo::Enabled(settings, MetricType::OPERATOR_TIMING)) {
			op.Start();
		}
	}
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while no operator is active");
	}

	if (!settings.empty()) {
		auto &info = GetOperatorInfo(*active_operator);
		if (ProfilingInfo::Enabled(settings, MetricType::OPERATOR_TIMING)) {
			op.End();
			info.AddMetric(MetricType::OPERATOR_TIMING, op.Elapsed());
		}
		if (ProfilingInfo::Enabled(settings, MetricType::OPERATOR_CARDINALITY) && chunk) {
			info.AddMetric(MetricType::OPERATOR_CARDINALITY, chunk->size());
		}
		if (ProfilingInfo::Enabled(settings, MetricType::RESULT_SET_SIZE) && chunk) {
			auto result_set_size = chunk->GetAllocationSize();
			info.AddMetric(MetricType::RESULT_SET_SIZE, result_set_size);
		}
		if (ProfilingInfo::Enabled(settings, MetricType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			auto used_memory = BufferManager::GetBufferManager(context).GetBufferPool().GetUsedMemory(false);
			info.AddMetric(MetricType::SYSTEM_PEAK_BUFFER_MEMORY, used_memory);
		}
		if (ProfilingInfo::Enabled(settings, MetricType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			auto used_swap = BufferManager::GetBufferManager(context).GetUsedSwap();
			info.AddMetric(MetricType::SYSTEM_PEAK_TEMP_DIR_SIZE, used_swap);
		}
	}
	active_operator = nullptr;
}

void OperatorProfiler::FinishSource(GlobalSourceState &gstate, LocalSourceState &lstate) {
	if (!enabled) {
		return;
	}
	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call FinishSource while no operator is active");
	}
	if (!settings.empty()) {
		if (ProfilingInfo::Enabled(settings, MetricType::EXTRA_INFO)) {
			// we're emitting extra info - get the extra source info
			auto &info = GetOperatorInfo(*active_operator);
			auto extra_info = active_operator->ExtraSourceParams(gstate, lstate);
			for (auto &new_info : extra_info) {
				auto entry = info.extra_info.find(new_info.first);
				if (entry != info.extra_info.end()) {
					// entry exists - override
					entry->second = std::move(new_info.second);
				} else {
					// entry does not exist yet - insert
					info.extra_info.insert(std::move(new_info));
				}
			}
		}
		if (ProfilingInfo::Enabled(settings, MetricType::OPERATOR_ROWS_SCANNED) &&
		    active_operator.get()->type == PhysicalOperatorType::TABLE_SCAN) {
			const auto &table_scan = active_operator->Cast<PhysicalTableScan>();
			const auto rows_scanned = table_scan.GetRowsScanned(gstate, lstate);
			auto &info = GetOperatorInfo(*active_operator);
			if (rows_scanned.IsValid()) {
				// Use exact value if available.
				info.AddMetric(MetricType::OPERATOR_ROWS_SCANNED, rows_scanned.GetIndex());
			} else {
				// Otherwise estimate as the cardinality of the table scan, if there is no exact value available.
				auto &bind_data = table_scan.bind_data;
				if (bind_data && table_scan.function.cardinality) {
					auto cardinality = table_scan.function.cardinality(context, &(*bind_data));
					if (cardinality && cardinality->has_estimated_cardinality) {
						info.AddMetric(MetricType::OPERATOR_ROWS_SCANNED, cardinality->estimated_cardinality);
					}
				}
			}
		}
	}
}

bool OperatorProfiler::OperatorInfoIsInitialized(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	return entry != operator_infos.end();
}

OperatorInformation &OperatorProfiler::GetOperatorInfo(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry != operator_infos.end()) {
		return entry->second;
	}

	// Add a new entry.
	operator_infos[phys_op] = OperatorInformation();
	return operator_infos[phys_op];
}

void OperatorProfiler::Flush(const PhysicalOperator &phys_op) {
	auto entry = operator_infos.find(phys_op);
	if (entry == operator_infos.end()) {
		return;
	}

	auto &info = operator_infos.find(phys_op)->second;
	info.name = phys_op.GetName();
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.operator_infos) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());

		auto &tree_node = entry->second.get();
		auto &info = tree_node.GetProfilingInfo();

		if (ProfilingInfo::Enabled(profiler.settings, MetricType::OPERATOR_TIMING)) {
			info.MetricSum<double>(MetricType::OPERATOR_TIMING, node.second.time);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::OPERATOR_CARDINALITY)) {
			info.MetricSum<idx_t>(MetricType::OPERATOR_CARDINALITY, node.second.elements_returned);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::OPERATOR_ROWS_SCANNED)) {
			info.MetricSum<idx_t>(MetricType::OPERATOR_ROWS_SCANNED, node.second.rows_scanned);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::RESULT_SET_SIZE)) {
			info.MetricSum<idx_t>(MetricType::RESULT_SET_SIZE, node.second.result_set_size);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::EXTRA_INFO)) {
			info.metrics[MetricType::EXTRA_INFO] = Value::MAP(node.second.extra_info);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::SYSTEM_PEAK_BUFFER_MEMORY)) {
			query_metrics.query_global_info.MetricMax(MetricType::SYSTEM_PEAK_BUFFER_MEMORY,
			                                          node.second.system_peak_buffer_manager_memory);
		}
		if (ProfilingInfo::Enabled(profiler.settings, MetricType::SYSTEM_PEAK_TEMP_DIR_SIZE)) {
			query_metrics.query_global_info.MetricMax(MetricType::SYSTEM_PEAK_TEMP_DIR_SIZE,
			                                          node.second.system_peak_temp_directory_size);
		}
	}
	profiler.operator_infos.clear();
}

void QueryProfiler::SetBlockedTime(const double &blocked_thread_time) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}

	auto &info = root->GetProfilingInfo();
	if (info.Enabled(info.expanded_settings, MetricType::BLOCKED_THREAD_TIME)) {
		query_metrics.query_global_info.metrics[MetricType::BLOCKED_THREAD_TIME] = blocked_thread_time;
	}
}

string QueryProfiler::DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		auto half_spaces = width / 2;
		auto extra_left_space = NumericCast<idx_t>(width % 2 != 0 ? 1 : 0);
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = NumericCast<char>(toupper(str[0]));
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = NumericCast<char>(toupper(str[i + 1]));
			}
		}
	}
	return str;
}

static string RenderTiming(double timing) {
	string timing_s;
	if (timing >= 1) {
		timing_s = StringUtil::Format("%.2f", timing);
	} else if (timing >= 0.1) {
		timing_s = StringUtil::Format("%.3f", timing);
	} else {
		timing_s = StringUtil::Format("%.4f", timing);
	}
	return timing_s + "s";
}

string QueryProfiler::QueryTreeToString() const {
	duckdb::stringstream str;
	QueryTreeToStream(str);
	return str.str();
}

void RenderPhaseTimings(std::ostream &ss, const pair<string, double> &head, map<string, double> &timings, idx_t width) {
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│" + QueryProfiler::DrawPadded(RenderTitleCase(head.first) + ": " + RenderTiming(head.second), width - 2) +
	          "│\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";

	for (const auto &entry : timings) {
		ss << "││" +
		          QueryProfiler::DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
		                                    width - 4) +
		          "││\n";
	}
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
}

void PrintPhaseTimingsToStream(std::ostream &ss, const ProfilingInfo &info, idx_t width) {
	map<string, double> optimizer_timings;
	map<string, double> planner_timings;
	map<string, double> physical_planner_timings;

	pair<string, double> optimizer_head;
	pair<string, double> planner_head;
	pair<string, double> physical_planner_head;

	for (const auto &entry : info.metrics) {
		if (MetricsUtils::IsOptimizerMetric(entry.first)) {
			optimizer_timings[EnumUtil::ToString(entry.first).substr(10)] = entry.second.GetValue<double>();
		} else if (MetricsUtils::IsPhaseTimingMetric(entry.first)) {
			switch (entry.first) {
			case MetricType::CUMULATIVE_OPTIMIZER_TIMING:
				continue;
			case MetricType::ALL_OPTIMIZERS:
				optimizer_head = {"Optimizer", entry.second.GetValue<double>()};
				break;
			case MetricType::PHYSICAL_PLANNER:
				physical_planner_head = {"Physical Planner", entry.second.GetValue<double>()};
				break;
			case MetricType::PLANNER:
				planner_head = {"Planner", entry.second.GetValue<double>()};
				break;
			default:
				break;
			}

			auto metric = EnumUtil::ToString(entry.first);
			if (StringUtil::StartsWith(metric, "PHYSICAL_PLANNER") && entry.first != MetricType::PHYSICAL_PLANNER) {
				physical_planner_timings[metric.substr(17)] = entry.second.GetValue<double>();
			} else if (StringUtil::StartsWith(metric, "PLANNER") && entry.first != MetricType::PLANNER) {
				planner_timings[metric.substr(8)] = entry.second.GetValue<double>();
			}
		}
	}

	RenderPhaseTimings(ss, optimizer_head, optimizer_timings, width);
	RenderPhaseTimings(ss, physical_planner_head, physical_planner_timings, width);
	RenderPhaseTimings(ss, planner_head, planner_timings, width);
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	lock_guard<std::mutex> guard(lock);

	bool show_query_name = false;
	if (root) {
		auto &info = root->GetProfilingInfo();
		auto &settings = info.expanded_settings;
		show_query_name = info.Enabled(settings, MetricType::QUERY_NAME);
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << (show_query_name ? StringUtil::Replace(query_metrics.query_name, "\n", " ") : "") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query_metrics.query_name.empty() && !root) {
		return;
	}

	for (auto &state : context.registered_state->States()) {
		state->WriteProfilingInformation(ss);
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 50;
	ss << "┌────────────────────────────────────────────────┐\n";
	ss << "│┌──────────────────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(query_metrics.GetMetricInSeconds(MetricType::LATENCY));
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└──────────────────────────────────────────────┘│\n";
	ss << "└────────────────────────────────────────────────┘\n";
	// render the main operator tree
	if (root) {
		// print phase timings
		if (PrintOptimizerOutput()) {
			PrintPhaseTimingsToStream(ss, root->GetProfilingInfo(), TOTAL_BOX_WIDTH);
		}
		Render(*root, ss);
	}
}

Value QueryProfiler::JSONSanitize(const Value &input) {
	D_ASSERT(input.type().id() == LogicalTypeId::MAP);

	InsertionOrderPreservingMap<string> result;
	auto children = MapValue::GetChildren(input);
	for (auto &child : children) {
		auto struct_children = StructValue::GetChildren(child);
		auto key = struct_children[0].GetValue<string>();
		auto value = struct_children[1].GetValue<string>();

		if (StringUtil::StartsWith(key, "__")) {
			key = StringUtil::Replace(key, "__", "");
			key = StringUtil::Replace(key, "_", " ");
			key = StringUtil::Title(key);
		}
		result[key] = value;
	}
	return Value::MAP(result);
}

string QueryProfiler::JSONSanitize(const std::string &text) {
	string result;
	result.reserve(text.size());
	for (char i : text) {
		switch (i) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += i;
			break;
		}
	}
	return result;
}

static yyjson_mut_val *ToJSONRecursive(yyjson_mut_doc *doc, ProfilingNode &node) {
	auto result_obj = yyjson_mut_obj(doc);
	auto &profiling_info = node.GetProfilingInfo();

	if (profiling_info.Enabled(profiling_info.settings, MetricType::EXTRA_INFO)) {
		profiling_info.metrics[MetricType::EXTRA_INFO] =
		    QueryProfiler::JSONSanitize(profiling_info.metrics.at(MetricType::EXTRA_INFO));
	}

	profiling_info.WriteMetricsToJSON(doc, result_obj);

	auto children_list = yyjson_mut_arr(doc);
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = ToJSONRecursive(doc, *node.GetChild(i));
		yyjson_mut_arr_add_val(children_list, child);
	}
	yyjson_mut_obj_add_val(doc, result_obj, "children", children_list);
	return result_obj;
}

static void PipelineProfilesToJSON(yyjson_mut_doc *doc, yyjson_mut_val *result_obj,
                                   const vector<PipelineProfilingInfo> &pipeline_profiles) {
	auto pipelines_list = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, result_obj, "pipeline_info", pipelines_list);

	for (auto &profile : pipeline_profiles) {
		auto pipeline_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "pipeline_id", profile.pipeline_id);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_name", profile.source_name.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_type", profile.source_type.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "sink_name", profile.sink_name.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "sink_type", profile.sink_type.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "operator_type_sequence", profile.operator_type_sequence.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "operator_count", profile.operator_count);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "source_estimated_cardinality",
		                        profile.source_estimated_cardinality);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "sink_estimated_cardinality", profile.sink_estimated_cardinality);

		auto operator_names = yyjson_mut_arr(doc);
		for (auto &operator_name : profile.operator_names) {
			yyjson_mut_arr_add_strcpy(doc, operator_names, operator_name.c_str());
		}
		yyjson_mut_obj_add_val(doc, pipeline_obj, "operator_names", operator_names);

		auto operator_types = yyjson_mut_arr(doc);
		for (auto &operator_type : profile.operator_types) {
			yyjson_mut_arr_add_strcpy(doc, operator_types, operator_type.c_str());
		}
		yyjson_mut_obj_add_val(doc, pipeline_obj, "operator_types", operator_types);

		auto operator_estimated_cardinalities = yyjson_mut_arr(doc);
		for (auto estimated_cardinality : profile.operator_estimated_cardinalities) {
			yyjson_mut_arr_add_val(operator_estimated_cardinalities, yyjson_mut_uint(doc, estimated_cardinality));
		}
		yyjson_mut_obj_add_val(doc, pipeline_obj, "operator_estimated_cardinalities", operator_estimated_cardinalities);

		yyjson_mut_obj_add_uint(doc, pipeline_obj, "schedule_count", profile.schedule_count);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "task_count", profile.task_count);
		yyjson_mut_obj_add_real(doc, pipeline_obj, "estimated_task_input_rows", profile.estimated_task_input_rows);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "estimated_task_input_rows_bucket",
		                       profile.estimated_task_input_rows_bucket.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "source_max_threads", profile.source_max_threads);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_input_kind", profile.source_input_kind.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_input_confidence", profile.source_input_confidence.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "planned_input_rows", profile.planned_input_rows);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "planned_input_chunks_equiv", profile.planned_input_chunks_equiv);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "planned_input_native_units", profile.planned_input_native_units);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "planned_input_native_unit",
		                       profile.planned_input_native_unit.c_str());
		yyjson_mut_obj_add_real(doc, pipeline_obj, "planned_task_input_chunks_equiv",
		                        profile.planned_task_input_chunks_equiv);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "planned_task_input_chunks_bucket",
		                       profile.planned_task_input_chunks_bucket.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_enabled", profile.throughput_enabled ? 1 : 0);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_tuple_kind", profile.source_tuple_kind.c_str());
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_tuple_confidence", profile.source_tuple_confidence.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "source_tuples_touched", profile.source_tuples_touched);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "source_chunks_touched", profile.source_chunks_touched);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "source_native_units_touched", profile.source_native_units_touched);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "source_native_unit", profile.source_native_unit.c_str());
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "adaptive_morsel_candidate",
		                        profile.adaptive_morsel_candidate ? 1 : 0);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "pipeline_input_tuples", profile.pipeline_input_tuples);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "pipeline_input_chunks", profile.pipeline_input_chunks);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "pipeline_input_task_count", profile.pipeline_input_task_count);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "pipeline_input_task_duration_ns",
		                        profile.pipeline_input_task_duration_ns);
		if (profile.pipeline_input_task_duration_ns > 0) {
			yyjson_mut_obj_add_real(
			    doc, pipeline_obj, "pipeline_input_tuples_per_s",
			    SourceTuplesPerTaskSecond(profile.pipeline_input_tuples, profile.pipeline_input_task_duration_ns));
		}
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_task_count", profile.throughput_task_count);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_task_duration_ns", profile.throughput_task_duration_ns);
		yyjson_mut_obj_add_str(doc, pipeline_obj, "task_signature_key", profile.task_signature_key.c_str());
		yyjson_mut_obj_add_real(doc, pipeline_obj, "estimated_tuples_per_task_s", profile.estimated_tuples_per_task_s);
		yyjson_mut_obj_add_real(doc, pipeline_obj, "last_task_tuples_per_s", profile.last_task_tuples_per_s);
		yyjson_mut_obj_add_real(doc, pipeline_obj, "throughput_ewma_alpha", profile.throughput_ewma_alpha);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_sample_count", profile.throughput_sample_count);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_sample_tuples", profile.throughput_sample_tuples);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "throughput_sample_ns", profile.throughput_sample_ns);
		if (profile.throughput_task_duration_ns > 0) {
			yyjson_mut_obj_add_real(
			    doc, pipeline_obj, "source_tuples_per_task_s",
			    SourceTuplesPerTaskSecond(profile.source_tuples_touched, profile.throughput_task_duration_ns));
		}
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "start_ns", profile.start_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "tasks_done_ns", profile.tasks_done_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "finish_done_ns", profile.finish_done_ns);

		auto task_end_ns = profile.tasks_done_ns;
		auto lifecycle_end_ns = profile.finish_done_ns ? profile.finish_done_ns : profile.tasks_done_ns;
		auto task_duration_ns = task_end_ns >= profile.start_ns ? task_end_ns - profile.start_ns : 0;
		auto lifecycle_duration_ns = lifecycle_end_ns >= profile.start_ns ? lifecycle_end_ns - profile.start_ns : 0;
		auto finish_tail_ns =
		    profile.finish_done_ns && profile.tasks_done_ns && profile.finish_done_ns >= profile.tasks_done_ns
		        ? profile.finish_done_ns - profile.tasks_done_ns
		        : 0;

		yyjson_mut_obj_add_uint(doc, pipeline_obj, "task_end_ns", task_end_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "task_duration_ns", task_duration_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "lifecycle_end_ns", lifecycle_end_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "lifecycle_duration_ns", lifecycle_duration_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "finish_tail_ns", finish_tail_ns);
		auto end_ns = task_end_ns;
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "end_ns", end_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "duration_ns", task_duration_ns);

		yyjson_mut_obj_add_uint(doc, pipeline_obj, "dvfs_metrics_enabled", profile.dvfs_metrics_enabled ? 1 : 0);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "measurement_start_overhead_ns",
		                        profile.measurement_start_overhead_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "measurement_end_overhead_ns", profile.measurement_end_overhead_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "rapl_start_overhead_ns", profile.rapl_start_overhead_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_start_overhead_ns", profile.perf_start_overhead_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "rapl_end_overhead_ns", profile.rapl_end_overhead_ns);
		yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_end_overhead_ns", profile.perf_end_overhead_ns);
		if (profile.dvfs_metrics_enabled) {
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "rapl_supported", profile.rapl_supported ? 1 : 0);
			yyjson_mut_obj_add_str(doc, pipeline_obj, "rapl_source", profile.rapl_source.c_str());
			if (!profile.rapl_error.empty()) {
				yyjson_mut_obj_add_str(doc, pipeline_obj, "rapl_error", profile.rapl_error.c_str());
			}
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "rapl_perf_fd_count", profile.rapl_perf_fd_count);
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "rapl_perf_group_count", profile.rapl_perf_group_count);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "rapl_perf_min_running_pct", profile.rapl_perf_min_running_pct);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "cpu_package_j", profile.cpu_package_j);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "dram_j", profile.dram_j);
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_supported", profile.perf_supported ? 1 : 0);
			yyjson_mut_obj_add_str(doc, pipeline_obj, "perf_scope", profile.perf_scope.c_str());
			yyjson_mut_obj_add_str(doc, pipeline_obj, "perf_cgroup_path", profile.perf_cgroup_path.c_str());
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_fd_count", profile.perf_fd_count);
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_group_count", profile.perf_group_count);
			yyjson_mut_obj_add_uint(doc, pipeline_obj, "perf_cpu_count", profile.perf_cpu_count);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "perf_min_running_pct", profile.perf_min_running_pct);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "cycles", profile.cycles);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "instructions", profile.instructions);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "cache_references", profile.cache_references);
			yyjson_mut_obj_add_real(doc, pipeline_obj, "cache_misses", profile.cache_misses);
			if (profile.cycles > 0) {
				yyjson_mut_obj_add_real(doc, pipeline_obj, "ipc", profile.instructions / profile.cycles);
			}
			if (profile.instructions > 0) {
				yyjson_mut_obj_add_real(doc, pipeline_obj, "cache_mpki",
				                        1000.0 * profile.cache_misses / profile.instructions);
			}
			if (!profile.per_cpu_perf.empty()) {
				auto per_cpu_list = yyjson_mut_arr(doc);
				for (auto &cpu_profile : profile.per_cpu_perf) {
					auto cpu_obj = yyjson_mut_obj(doc);
					yyjson_mut_obj_add_int(doc, cpu_obj, "cpu", cpu_profile.cpu);
					yyjson_mut_obj_add_real(doc, cpu_obj, "perf_min_running_pct", cpu_profile.min_running_pct);
					yyjson_mut_obj_add_real(doc, cpu_obj, "cycles", cpu_profile.cycles);
					yyjson_mut_obj_add_real(doc, cpu_obj, "instructions", cpu_profile.instructions);
					yyjson_mut_obj_add_real(doc, cpu_obj, "cache_references", cpu_profile.cache_references);
					yyjson_mut_obj_add_real(doc, cpu_obj, "cache_misses", cpu_profile.cache_misses);
					if (cpu_profile.cycles > 0) {
						yyjson_mut_obj_add_real(doc, cpu_obj, "ipc", cpu_profile.instructions / cpu_profile.cycles);
					}
					if (cpu_profile.instructions > 0) {
						yyjson_mut_obj_add_real(doc, cpu_obj, "cache_mpki",
						                        1000.0 * cpu_profile.cache_misses / cpu_profile.instructions);
					}
					yyjson_mut_arr_add_val(per_cpu_list, cpu_obj);
				}
				yyjson_mut_obj_add_val(doc, pipeline_obj, "per_cpu_perf", per_cpu_list);
			}
			if (!profile.perf_error.empty()) {
				yyjson_mut_obj_add_str(doc, pipeline_obj, "perf_error", profile.perf_error.c_str());
			}
		}

		yyjson_mut_arr_add_val(pipelines_list, pipeline_obj);
	}
}

static void PipelineTaskProfilesToJSON(yyjson_mut_doc *doc, yyjson_mut_val *result_obj,
                                       const vector<PipelineTaskProfilingInfo> &pipeline_task_profiles) {
	auto task_list = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, result_obj, "pipeline_task_info", task_list);

	for (auto &profile : pipeline_task_profiles) {
		auto task_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_uint(doc, task_obj, "task_id", profile.task_id);
		yyjson_mut_obj_add_uint(doc, task_obj, "pipeline_id", profile.pipeline_id);
		yyjson_mut_obj_add_uint(doc, task_obj, "thread_id", profile.thread_id);
		yyjson_mut_obj_add_int(doc, task_obj, "start_cpu", profile.start_cpu);
		yyjson_mut_obj_add_int(doc, task_obj, "end_cpu", profile.end_cpu);
		yyjson_mut_obj_add_uint(doc, task_obj, "start_ns", profile.start_ns);
		yyjson_mut_obj_add_uint(doc, task_obj, "end_ns", profile.end_ns);
		auto duration_ns = profile.end_ns >= profile.start_ns ? profile.end_ns - profile.start_ns : 0;
		yyjson_mut_obj_add_uint(doc, task_obj, "duration_ns", duration_ns);
		yyjson_mut_obj_add_str(doc, task_obj, "source_tuple_kind", profile.source_tuple_kind.c_str());
		yyjson_mut_obj_add_str(doc, task_obj, "source_tuple_confidence", profile.source_tuple_confidence.c_str());
		yyjson_mut_obj_add_uint(doc, task_obj, "source_tuples_touched", profile.source_tuples_touched);
		yyjson_mut_obj_add_uint(doc, task_obj, "source_chunks_touched", profile.source_chunks_touched);
		yyjson_mut_obj_add_uint(doc, task_obj, "source_native_units_touched", profile.source_native_units_touched);
		yyjson_mut_obj_add_str(doc, task_obj, "source_native_unit", profile.source_native_unit.c_str());
		yyjson_mut_obj_add_uint(doc, task_obj, "adaptive_morsel_candidate", profile.adaptive_morsel_candidate ? 1 : 0);
		yyjson_mut_obj_add_uint(doc, task_obj, "pipeline_input_tuples", profile.pipeline_input_tuples);
		yyjson_mut_obj_add_uint(doc, task_obj, "pipeline_input_chunks", profile.pipeline_input_chunks);
		yyjson_mut_obj_add_str(doc, task_obj, "task_signature_key", profile.task_signature_key.c_str());
		yyjson_mut_obj_add_real(doc, task_obj, "estimated_tuples_per_task_s", profile.estimated_tuples_per_task_s);
		if (duration_ns > 0) {
			yyjson_mut_obj_add_real(doc, task_obj, "pipeline_input_tuples_per_s",
			                        SourceTuplesPerTaskSecond(profile.pipeline_input_tuples, duration_ns));
		}
		if (duration_ns > 0) {
			yyjson_mut_obj_add_real(doc, task_obj, "source_tuples_per_task_s",
			                        SourceTuplesPerTaskSecond(profile.source_tuples_touched, duration_ns));
		}
		yyjson_mut_arr_add_val(task_list, task_obj);
	}
}

static string StringifyAndFree(ConvertedJSONHolder &json_holder, yyjson_mut_val *object) {
	json_holder.stringified_json = yyjson_mut_val_write_opts(
	    object, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr, nullptr, nullptr);
	if (!json_holder.stringified_json) {
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	auto result = string(json_holder.stringified_json);
	return result;
}

void QueryProfiler::ToLog() const {
	lock_guard<std::mutex> guard(lock);

	if (!root) {
		// No root, not much to do
		return;
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToLog(context);
}

string QueryProfiler::ToJSON() const {
	lock_guard<std::mutex> guard(lock);
	ConvertedJSONHolder json_holder;

	json_holder.doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_obj(json_holder.doc);
	yyjson_mut_doc_set_root(json_holder.doc, result_obj);

	if (query_metrics.query_name.empty() && !root) {
		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "empty");
		return StringifyAndFree(json_holder, result_obj);
	}
	if (!root) {
		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "error");
		return StringifyAndFree(json_holder, result_obj);
	}

	auto &settings = root->GetProfilingInfo();

	settings.WriteMetricsToJSON(json_holder.doc, result_obj);

	// recursively print the physical operator tree
	auto children_list = yyjson_mut_arr(json_holder.doc);
	yyjson_mut_obj_add_val(json_holder.doc, result_obj, "children", children_list);
	auto child = ToJSONRecursive(json_holder.doc, *root->GetChild(0));
	yyjson_mut_arr_add_val(children_list, child);
	auto &pipeline_settings = ClientConfig::GetConfig(context).pipeline_profiling;
	if (pipeline_settings.EmitPipelineInfo()) {
		PipelineProfilesToJSON(json_holder.doc, result_obj, pipeline_profiles);
	}
	if (pipeline_settings.task_trace || pipeline_settings.throughput) {
		PipelineTaskProfilesToJSON(json_holder.doc, result_obj, pipeline_task_profiles);
	}
	return StringifyAndFree(json_holder, result_obj);
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	auto &fs = FileSystem::GetFileSystem(context);
	auto flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
	auto file = fs.OpenFile(path, flags);
	file->Write((void *)info.c_str(), info.size());
	file->Close();
}

profiler_settings_t EraseQueryRootSettings(profiler_settings_t settings) {
	profiler_settings_t phase_timing_settings_to_erase;

	for (auto &setting : settings) {
		if (MetricsUtils::IsOptimizerMetric(setting) || MetricsUtils::IsPhaseTimingMetric(setting) ||
		    MetricsUtils::IsRootScopeMetric(setting)) {
			phase_timing_settings_to_erase.insert(setting);
		}
	}

	for (auto &setting : phase_timing_settings_to_erase) {
		settings.erase(setting);
	}

	return settings;
}

unique_ptr<ProfilingNode> QueryProfiler::CreateTree(const PhysicalOperator &root_p, const profiler_settings_t &settings,
                                                    const idx_t depth) {
	if (OperatorRequiresProfiling(root_p.type)) {
		query_requires_profiling = true;
	}

	unique_ptr<ProfilingNode> node = make_uniq<ProfilingNode>();
	auto &info = node->GetProfilingInfo();
	info = ProfilingInfo(settings, depth);
	auto child_settings = settings;
	if (depth == 0) {
		child_settings = EraseQueryRootSettings(child_settings);
	}
	node->depth = depth;

	if (depth != 0) {
		info.metrics[MetricType::OPERATOR_NAME] = root_p.GetName();
		info.MetricSum<uint8_t>(MetricType::OPERATOR_TYPE, static_cast<uint8_t>(root_p.type));
	}
	if (info.Enabled(info.settings, MetricType::EXTRA_INFO)) {
		info.metrics[MetricType::EXTRA_INFO] = Value::MAP(root_p.ParamsToString());
	}

	tree_map.insert(make_pair(reference<const PhysicalOperator>(root_p), reference<ProfilingNode>(*node)));
	auto children = root_p.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), child_settings, depth + 1);
		node->AddChild(std::move(child_node));
	}
	return node;
}

string QueryProfiler::RenderDisabledMessage(ProfilerPrintFormat format) const {
	switch (format) {
	case ProfilerPrintFormat::NO_OUTPUT:
		return "";
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return "Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!";
	case ProfilerPrintFormat::HTML:
		return R"(
				<!DOCTYPE html>
                <html lang="en"><head/><body>
                  Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!
                </body></html>
			)";
	case ProfilerPrintFormat::GRAPHVIZ:
		return R"(
				digraph G {
				    node [shape=box, style=rounded, fontname="Courier New", fontsize=10];
				    node_0_0 [label="Query profiling is disabled. Use 'PRAGMA enable_profiling;' to enable profiling!"];
				}
			)";
	case ProfilerPrintFormat::MERMAID:
		return R"(flowchart TD
    node_0_0["`**DISABLED**
Query profiling is disabled.
Use 'PRAGMA enable_profiling;' to enable profiling!`"]
)";
	case ProfilerPrintFormat::JSON: {
		ConvertedJSONHolder json_holder;
		json_holder.doc = yyjson_mut_doc_new(nullptr);
		auto result_obj = yyjson_mut_obj(json_holder.doc);
		yyjson_mut_doc_set_root(json_holder.doc, result_obj);

		yyjson_mut_obj_add_str(json_holder.doc, result_obj, "result", "disabled");
		return StringifyAndFree(json_holder, result_obj);
	}
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", EnumUtil::ToString(format));
	}
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	lock_guard<std::mutex> guard(lock);
	if (!IsEnabled() || !running) {
		return;
	}
	query_requires_profiling = false;
	ClientConfig &config = ClientConfig::GetConfig(context);
	root = CreateTree(root_op, config.profiler_settings, 0);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

void QueryProfiler::Render(const ProfilingNode &node, std::ostream &ss) const {
	TextTreeRenderer renderer;
	if (IsDetailedEnabled()) {
		renderer.EnableDetailed();
	} else {
		renderer.EnableStandard();
	}
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(QueryTreeToString());
}

void QueryProfiler::MoveOptimizerPhasesToRoot() {
	auto &root_info = root->GetProfilingInfo();
	auto &root_metrics = root_info.metrics;

	for (auto &entry : phase_timings) {
		auto &phase = entry.first;
		auto &timing = entry.second;
		if (root_info.Enabled(root_info.expanded_settings, phase)) {
			root_metrics[phase] = Value::CreateValue(timing);
		}
	}
}

void QueryProfiler::FinalizeMetricsInternal() {
	if (metrics_finalized || !IsEnabled() || !root) {
		return;
	}

	if (query_metrics.latency_timer) {
		query_metrics.latency_timer->EndTimer();
	}

	auto &info = root->GetProfilingInfo();
	if (info.Enabled(info.expanded_settings, MetricType::OPERATOR_CARDINALITY)) {
		Finalize(*root->GetChild(0));
	}

	auto &child_info = root->children[0]->GetProfilingInfo();
	const auto &settings = info.expanded_settings;
	for (const auto &global_info_entry : query_metrics.query_global_info.metrics) {
		info.metrics[global_info_entry.first] = global_info_entry.second;
	}

	MoveOptimizerPhasesToRoot();
	for (auto &metric : info.metrics) {
		if (info.Enabled(settings, metric.first)) {
			ProfilingUtils::CollectMetrics(metric.first, query_metrics, metric.second, *root, child_info);
		}
	}
	metrics_finalized = true;
}

} // namespace duckdb
