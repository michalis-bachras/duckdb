#include "scheduler_workload.hpp"
#include "phase_pair_controller.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/pipeline_dvfs_profiler.hpp"
#include "duckdb.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/database.hpp"
#include <cstdlib>

#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>

using namespace duckdb;
using namespace duckdb::scheduler_workload;

static void Require(bool condition, const string &message) {
	if (!condition) {
		throw InternalException("scheduler_workload test failed: %s", message);
	}
}

// Re-enter read/installation APIs from notifications. This would deadlock if a
// future change accidentally moved callbacks under the registration mutex.
class BoundaryProbe final : public QueryStrideTaskSetControl {
public:
	explicit BoundaryProbe(QueryStrideScheduler &scheduler_p) : scheduler(scheduler_p) {}
	void OnReady(const QueryStrideTaskSetInfo &) override {
		(void)scheduler.GetSnapshot();
		try {scheduler.SetTaskSetControl(nullptr);}
		catch (InvalidInputException &) {rejected_install++;}
		ready++;
	}
	void OnFinished(const QueryStrideTaskSetInfo &) override {(void)scheduler.GetSnapshot();finished++;}
	void OnUnregistered(uint64_t,uint64_t) noexcept override {(void)scheduler.GetSnapshot();unregistered++;}
	bool IsWorkerEligible(uint64_t,uint64_t,uint64_t,idx_t worker,bool serial) const noexcept override {
		return !serial || worker==0;
	}
	QueryStrideScheduler &scheduler;
	atomic<idx_t> ready {0},finished {0},unregistered {0},rejected_install {0};
};

int main() {
	try {
		{
			DuckDB database(nullptr);Connection connection(database);
			Require(!connection.Query("SET threads=4")->HasError(),"probe workers");
			Require(!connection.Query("SET external_threads=0")->HasError(),"probe worker-only mode");
			Require(!connection.Query("SET query_worker_only_execution_enable=true")->HasError(),"probe worker-only mode");
			Require(!connection.Query("SET query_activation_scheduler_enable=true")->HasError(),"probe event identities");
			Require(!connection.Query("SET query_request_profiling_enable=true")->HasError(),"probe typed profiling");
			Require(!connection.Query("SET query_admission_max_active=128")->HasError(),"probe STRIDE admission capacity");
			Require(!connection.Query("SET scheduler_policy='stride'")->HasError(),"probe STRIDE policy");
			auto &scheduler=database.instance->GetQueryStrideScheduler();
			auto probe=std::make_shared<BoundaryProbe>(scheduler);scheduler.SetTaskSetControl(probe);
			PendingQueryParameters parameters;parameters.has_request_metadata=true;
			parameters.query_parameters.output_type=QueryResultOutputType::FORCE_MATERIALIZED;
			parameters.request_metadata.valid=true;parameters.request_metadata.parse_status="typed";
			parameters.request_metadata.request_id=77;parameters.request_metadata.template_id=1;
			parameters.request_metadata.scale_factor=1;
			parameters.request_metadata.deadline_ns=PipelineDVFSProfiler::TimestampNs()+10000000000ULL;
			auto pending=connection.PendingQuery("SELECT sum(i) FROM range(10000) t(i)",std::move(parameters));
			Require(!pending->HasError(),"probe pending query: "+(pending->HasError() ? pending->GetError() : ""));
			auto result=pending->Execute();Require(!result->HasError(),"probe controlled query");
			Require(probe->ready>0 && probe->finished==probe->ready && probe->unregistered==1,
			        "task boundaries and unregistration notified: "+std::to_string(probe->ready.load())+"/"+
			        std::to_string(probe->finished.load())+"/"+std::to_string(probe->unregistered.load()));
			Require(probe->rejected_install==probe->ready,"live control replacement rejected");
			scheduler.SetTaskSetControl(nullptr);
			Require(!connection.Query("SELECT 42")->HasError(),"normal execution after detaching control");
		}
		{
			PhasePairController control({{10,2,"pipeline",{0,2},0},{20,6,"hash_join_finalize",{1,3},1}});
			QueryStrideTaskSetInfo info;info.request_id=10;info.query_generation=7;
			info.task_set_generation=1;info.pipeline_id=1;info.kind=QueryActivationEventKind::INITIALIZE;
			control.OnReady(info);
			Require(!control.IsWorkerEligible(10,7,1,0,true),"preparation starts paused");
			control.Prepare(0);
			Require(control.IsWorkerEligible(10,7,1,0,true),"selected preparation worker");
			Require(!control.IsWorkerEligible(10,7,1,2,true),"stable single lifecycle worker");
			Require(!control.IsWorkerEligible(10,6,1,0,true),"stale query generation cannot run");
			info.task_set_generation=2;info.pipeline_id=2;info.kind=QueryActivationEventKind::PIPELINE;
			control.OnReady(info);control.WaitReady(0);
			Require(!control.IsWorkerEligible(10,7,2,0,false),"target is held at boundary");
			control.Release({0});
			Require(control.IsWorkerEligible(10,7,2,2,false),"released target uses its own worker set");
			Require(!control.IsWorkerEligible(10,7,2,1,false),"other query workers cannot be borrowed");
			control.OnFinished(info);control.WaitFinished({0});
			Require(!control.IsWorkerEligible(10,7,3,0,true),"successor held until drain");
			control.Drain(0);Require(control.IsWorkerEligible(10,7,3,0,true),"drain releases successor");
			control.OnUnregistered(10,7);control.WaitUnregistered(0);
			control.Cancel();bool cancelled=false;
			try {control.WaitReady(1);} catch (InvalidInputException &) {cancelled=true;}
			Require(cancelled,"cancellation wakes phase waiters");
			bool overlap=false;
			try {PhasePairController bad({{1,2,"pipeline",{0},0},{2,2,"pipeline",{0},0}});}
			catch (InvalidInputException &) {overlap=true;}
			Require(overlap,"overlapping worker assignments rejected");
		}
        PipelineTaskPMUSnapshot pmu_a,pmu_b;
        pmu_a.valid=pmu_b.valid=true;pmu_a.group=pmu_b.group="A";
        pmu_a.time_enabled=100;pmu_a.time_running=100;pmu_a.values={10,20};
        pmu_b.time_enabled=200;pmu_b.time_running=200;pmu_b.values={40,80};
        auto delta=PipelineTaskPMUDelta(pmu_a,pmu_b);
        Require(delta.valid && delta.values[0]==30 && delta.values[1]==60,"task PMU delta");
        pmu_b.time_running=150;
        Require(!PipelineTaskPMUDelta(pmu_a,pmu_b).valid,"PMU scheduling coverage must be reported");
        pmu_b.time_running=200;pmu_b.values[0]=1;
        Require(!PipelineTaskPMUDelta(pmu_a,pmu_b).valid,"counter regression must be detected");
        Require(!PipelineTaskPMUDelta(PipelineTaskPMUSnapshot(),PipelineTaskPMUSnapshot()).valid,"disabled PMU");
        {
            auto previous=std::getenv("OLAP_PIPELINE_PMU_GROUP");
            string saved=previous?previous:"";
            setenv("OLAP_PIPELINE_PMU_GROUP","off",1);
            DuckDB database(nullptr);
            Connection connection(database);
            auto setting=connection.Query("PRAGMA custom_profiling_settings='{\"DEFAULT\":\"false\",\"PIPELINE_INFO\":\"true\",\"PIPELINE_THROUGHPUT\":\"true\"}'");
            Require(!setting->HasError(),"configure lifecycle profiling");
            Require(!connection.Query("SET enable_profiling='no_output'")->HasError(),"enable lifecycle profiling");
            Require(!connection.Query("SET query_activation_scheduler_enable=true")->HasError(),"enable event identities");
            PendingQueryParameters parameters;
            parameters.query_parameters.output_type=QueryResultOutputType::FORCE_MATERIALIZED;
            parameters.has_request_metadata=true;
            parameters.request_metadata.valid=true;
            parameters.request_metadata.parse_status="typed";
            parameters.request_metadata.request_id=1;
            parameters.request_metadata.template_id=16;
            parameters.request_metadata.scale_factor=300;
            parameters.request_metadata.deadline_ns=PipelineDVFSProfiler::TimestampNs()+3600000000000ULL;
            auto pending=connection.PendingQuery("SELECT i%8 AS g,count(DISTINCT i%1000) FROM range(100000) t(i) GROUP BY g",std::move(parameters));
            Require(!pending->HasError(),"prepare distinct lifecycle query");
            auto result=pending->Execute();
            Require(!result->HasError(),"distinct lifecycle query");
            auto segments=QueryProfiler::Get(*connection.context).GetPipelinePMUSegmentsSnapshot();
            bool distinct=false,normal=false,finish=false;
            idx_t normal_count=0,expected=0;
            for (const auto &segment:segments) {
                distinct|=segment.phase=="hash_aggregate_distinct_finalize";
                normal|=segment.phase=="pipeline";
                finish|=segment.phase=="finish";
                normal_count+=segment.phase=="pipeline";
                Require(segment.end_ns>=segment.start_ns && segment.outcome=="finished","completed lifecycle segment");
            }
            for (const auto &pipeline:QueryProfiler::Get(*connection.context).GetPipelineProfilesSnapshot()) expected+=pipeline.worker_task_count;
            Require(distinct && normal && finish,"ordinary and distinct-finalization task coverage");
            Require(normal_count==expected,"ordinary tasks must not be counted twice");
            if (saved.empty()) unsetenv("OLAP_PIPELINE_PMU_GROUP");
            else setenv("OLAP_PIPELINE_PMU_GROUP",saved.c_str(),1);
        }
		WorkloadConfig config;
		{
			const char *args[]={"scheduler_workload_pairs","pair-profiles","--database","1=:memory:",
			    "--queries-dir","/tmp/queries","--queries","6,13","--threads","8","--admission-cap","2",
			    "--warmup-repetitions","1","--profile-timing-export","on","--output-dir","/tmp/pairs",
			    "--scheduler-process-partial","on","--scheduler","default"};
			for (auto policy : {"default","stride"}) {
				args[21]=policy;
				args[11]=string(policy)=="stride" ? "128" : "2";
				auto parsed=ParseConfig(sizeof(args)/sizeof(args[0]),const_cast<char **>(args));
				Require(parsed.command==WorkloadCommand::PAIR_PROFILES && parsed.threads==8,"shared pair mode parses");
				Require(SchedulerName(parsed.scheduler_policy)==policy &&
				        parsed.admission_cap==(string(policy)=="stride" ? 128 : 2),"pair policy uses the required admission capacity");
				Require(parsed.scheduler_process_partial,"pair partial processing parses");
			}
			args[7]="6";
			bool rejected=false;
			try { (void)ParseConfig(sizeof(args)/sizeof(args[0]),const_cast<char **>(args)); }
			catch (InvalidInputException &) {rejected=true;}
			Require(rejected,"pair mode requires exactly two templates");
			args[7]="6,13";args[21]="sla";rejected=false;
			try { (void)ParseConfig(sizeof(args)/sizeof(args[0]),const_cast<char **>(args)); }
			catch (InvalidInputException &) {rejected=true;}
			Require(rejected,"SLA pair mode requires a separate deadline and profile protocol");
		}
		{
			const char *args[]={"scheduler_workload","run","--database","1=:memory:","--queries-dir","/tmp/queries",
			    "--schedule","/tmp/schedule.csv","--output-dir","/tmp/run","--scheduler","stride","--admission-cap","2"};
			bool rejected=false;
			try { (void)ParseConfig(sizeof(args)/sizeof(args[0]),const_cast<char **>(args)); }
			catch (InvalidInputException &) {rejected=true;}
			Require(rejected,"normal STRIDE runs retain the admission-cap requirement");
		}
		Require(config.admission_policy == QueryAdmissionPolicy::FIFO && config.admission_cap == 128,
		        "FIFO admission and 128 active queries remain the defaults");
		Require(!config.scheduler_process_partial,"partial processing remains disabled by default");
		for (auto scheduler : {"default", "sla", "stride"}) {
			for (auto admission : {"fifo", "edf"}) {
				const char *args[] = {"scheduler_workload", "run", "--database", "10=/tmp/db", "--queries-dir", "/tmp/q",
				                      "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/out", "--scheduler", scheduler,
				                      "--admission-policy", admission};
				auto parsed = ParseConfig(sizeof(args) / sizeof(args[0]), const_cast<char **>(args));
				Require(string(QueryAdmissionPolicyName(parsed.admission_policy)) == admission,
				        "admission policy is independent of the worker scheduler");
			}
		}
		const char *invalid_admission[] = {"scheduler_workload", "run", "--database", "10=/tmp/db", "--queries-dir", "/tmp/q",
		                                  "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/out",
		                                  "--admission-policy", "unknown"};
		bool rejected_admission = false;
		try {
			ParseConfig(sizeof(invalid_admission) / sizeof(invalid_admission[0]), const_cast<char **>(invalid_admission));
		} catch (InvalidInputException &) {
			rejected_admission = true;
		}
		Require(rejected_admission, "unknown admission policies are rejected");
		config.command = WorkloadCommand::GENERATE;
		config.seed = 42;
		config.duration_s = 1000;
		config.load_factor = 0.95;
		config.sla_tag = 30;
		config.query_templates = {1, 2, 3, 4};
		ScaleDatabase sf10;
		sf10.scale_factor = 10;
		sf10.weight = 3;
		ScaleDatabase sf100;
		sf100.scale_factor = 100;
		sf100.weight = 1;
		config.databases = {sf10, sf100};
		vector<CalibrationEntry> calibration;
		for (const auto &database : config.databases) {
			for (const auto query : config.query_templates) {
				CalibrationEntry entry;
				entry.template_id = query;
				entry.scale_factor = database.scale_factor;
				entry.sample_count = 3;
				entry.mean_runtime_s = database.scale_factor == 10 ? 0.1 : 0.9;
				calibration.push_back(entry);
			}
		}
		auto first = GenerateSchedule(config, calibration);
		auto second = GenerateSchedule(config, calibration);
		Require(ScheduleHash(first) == ScheduleHash(second), "same seed must produce the same schedule");
		Require(first.size() == second.size() && first.size() > 1000, "deterministic schedule size");
		idx_t sf10_count = 0;
		for (const auto &entry : first) {
			sf10_count += entry.scale_factor == 10 ? 1 : 0;
			Require(entry.deadline_offset_ns > entry.scheduled_offset_ns, "deadline follows arrival");
			Require(entry.sla_tag == config.sla_tag, "SLA tag is part of the immutable schedule");
		}
		auto sf10_fraction = static_cast<double>(sf10_count) / first.size();
		Require(std::fabs(sf10_fraction - 0.75) < 0.03, "3:1 scale-factor distribution");
		auto mean_duration = WeightedMeanDuration(config, calibration);
		Require(std::fabs(mean_duration - 0.3) < 1e-12, "weighted mean duration");
		auto realized_lambda = static_cast<double>(first.size()) / config.duration_s;
		auto configured_lambda = config.load_factor / mean_duration;
		Require(std::fabs(realized_lambda - configured_lambda) / configured_lambda < 0.05,
		        "exponential arrivals converge to configured lambda");

		const string path = "/tmp/duckdb_scheduler_workload_schedule_test.csv";
		WriteSchedule(path, first);
		auto round_trip = ReadSchedule(path);
		Require(ScheduleHash(first) == ScheduleHash(round_trip), "schedule CSV round trip");
		std::ifstream input(path);
		std::ostringstream crlf;
		string line;
		while (std::getline(input, line)) {
			crlf << line << "\r\n";
		}
		input.close();
		std::ofstream windows_output(path, std::ios::binary);
		windows_output << crlf.str();
		windows_output.close();
		Require(ScheduleHash(first) == ScheduleHash(ReadSchedule(path)), "CRLF schedule preserves immutable requests");
		std::remove(path.c_str());

		const char *arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla_energy",
		    "--pin-threads", "on", "--energy-power-model", "/tmp/power.csv", "--energy-hardware-control", "off",
		    "--energy-exploration", "on", "--energy-exploration-seed", "17", "--sla-residual-policy", "park",
		    "--no-progress-diagnostic-s", "15", "--sla-epoch-ms", "50", "--energy-attribution-period-ms", "50",
		    "--energy-attribution-idle-period-ms", "500", "--energy-attribution-export", "on",
		    "--energy-attribution-debug-export", "on"};
		auto parsed = ParseConfig(sizeof(arguments) / sizeof(arguments[0]), const_cast<char **>(arguments));
		Require(parsed.scheduler_policy == QuerySchedulerPolicy::SLA_ENERGY, "SLA-energy scheduler parsing");
		Require(parsed.pin_threads == "on", "worker pinning mode parsing");
		Require(!parsed.sla_energy_hardware_control, "dry-run hardware mode parsing");
		Require(parsed.sla_energy_exploration, "exploration mode parsing");
		Require(parsed.sla_energy_exploration_seed == 17, "exploration seed parsing");
		Require(!parsed.sla_residual_workers_enabled, "parked residual-worker policy parsing");
		Require(parsed.no_progress_diagnostic_s == 15, "no-progress diagnostic interval parsing");
		Require(parsed.sla_epoch_ms == 50, "SLA epoch duration parsing");
		Require(!parsed.energy_attribution_enabled, "attribution is explicit and defaults off");
		Require(parsed.energy_attribution_period_ms == 50, "attribution period parsing");
		Require(parsed.energy_attribution_idle_period_ms == 500, "idle attribution period parsing");
		Require(parsed.energy_attribution_export, "attribution export parsing");
		Require(parsed.energy_attribution_debug_export, "attribution debug export parsing");

		const char *sla_attribution_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla",
		    "--energy-power-model", "/tmp/power.csv", "--energy-attribution", "on"};
		auto sla_attribution = ParseConfig(sizeof(sla_attribution_arguments) / sizeof(sla_attribution_arguments[0]),
		                                   const_cast<char **>(sla_attribution_arguments));
		Require(sla_attribution.scheduler_policy == QuerySchedulerPolicy::SLA,
		        "SLA baseline scheduler parsing");
		Require(!sla_attribution.sla_residual_workers_enabled, "residual workers default to parked");
		Require(sla_attribution.energy_attribution_enabled, "policy-independent attribution parsing");
		Require(!sla_attribution.energy_attribution_export, "attribution export defaults off");
		Require(sla_attribution.energy_power_model_path == "/tmp/power.csv", "attribution power model parsing");

		const char *unsafe_dry_run_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla_energy",
		    "--energy-power-model", "/tmp/power.csv", "--energy-hardware-control", "off",
		    "--energy-attribution", "on"};
		bool rejected_unsafe_dry_run = false;
		try {
			ParseConfig(sizeof(unsafe_dry_run_arguments) / sizeof(unsafe_dry_run_arguments[0]),
			            const_cast<char **>(unsafe_dry_run_arguments));
		} catch (InvalidInputException &) {
			rejected_unsafe_dry_run = true;
		}
		Require(rejected_unsafe_dry_run, "attribution rejects logical-only SLA-energy hardware targets");

		const char *removed_lambda_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla_energy",
		    "--energy-power-model", "/tmp/power.csv", "--energy-lambda", "1"};
		bool rejected_removed_lambda = false;
		try {
			ParseConfig(sizeof(removed_lambda_arguments) / sizeof(removed_lambda_arguments[0]),
			            const_cast<char **>(removed_lambda_arguments));
		} catch (InvalidInputException &) {
			rejected_removed_lambda = true;
		}
		Require(rejected_removed_lambda, "removed energy-lambda option is rejected");

		const char *zero_epoch_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla",
		    "--sla-epoch-ms", "0"};
		bool rejected_zero_epoch = false;
		try {
			ParseConfig(sizeof(zero_epoch_arguments) / sizeof(zero_epoch_arguments[0]),
			            const_cast<char **>(zero_epoch_arguments));
		} catch (InvalidInputException &) {
			rejected_zero_epoch = true;
		}
		Require(rejected_zero_epoch, "zero SLA epoch duration is rejected");

		const char *obsolete_attribution_model_arguments[] = {
		    "scheduler_workload", "run", "--database", "10=/tmp/sf10.duckdb", "--queries-dir", "/tmp/queries",
		    "--schedule", "/tmp/schedule.csv", "--output-dir", "/tmp/output", "--scheduler", "sla",
		    "--energy-attribution-model", "process"};
		bool rejected_obsolete_attribution_model = false;
		try {
			ParseConfig(sizeof(obsolete_attribution_model_arguments) /
			                sizeof(obsolete_attribution_model_arguments[0]),
			            const_cast<char **>(obsolete_attribution_model_arguments));
		} catch (InvalidInputException &) {
			rejected_obsolete_attribution_model = true;
		}
		Require(rejected_obsolete_attribution_model, "obsolete attribution model option is rejected");

		const char *invalid_pin_threads_arguments[] = {
		    "scheduler_workload", "run",         "--database",   "10=/tmp/sf10.duckdb",
		    "--queries-dir",      "/tmp/queries", "--schedule",   "/tmp/schedule.csv",
		    "--output-dir",       "/tmp/output",  "--pin-threads", "sometimes"};
		bool rejected_invalid_pin_threads = false;
		try {
			ParseConfig(sizeof(invalid_pin_threads_arguments) / sizeof(invalid_pin_threads_arguments[0]),
			            const_cast<char **>(invalid_pin_threads_arguments));
		} catch (InvalidInputException &) {
			rejected_invalid_pin_threads = true;
		}
		Require(rejected_invalid_pin_threads, "unknown worker pinning mode is rejected");

		const char *unpinned_attribution_arguments[] = {
		    "scheduler_workload", "run",           "--database",          "10=/tmp/sf10.duckdb",
		    "--queries-dir",      "/tmp/queries",  "--schedule",          "/tmp/schedule.csv",
		    "--output-dir",       "/tmp/output",   "--pin-threads",       "off",
		    "--energy-power-model", "/tmp/power.csv", "--energy-attribution", "on"};
		bool rejected_unpinned_attribution = false;
		try {
			ParseConfig(sizeof(unpinned_attribution_arguments) / sizeof(unpinned_attribution_arguments[0]),
			            const_cast<char **>(unpinned_attribution_arguments));
		} catch (InvalidInputException &) {
			rejected_unpinned_attribution = true;
		}
		Require(rejected_unpinned_attribution, "energy attribution rejects explicitly unpinned workers");
        const char *point_args[] = {"scheduler_workload", "run", "--database", "10=/tmp/db", "--queries-dir", "/tmp/q",
            "--schedule", "/tmp/s.csv", "--output-dir", "/tmp/out", "--sla-suffix-model", "histogram",
            "--scheduler", "sla_energy", "--energy-power-model", "/tmp/power.csv"};
        auto energy_histogram = ParseConfig(sizeof(point_args)/sizeof(point_args[0]), const_cast<char **>(point_args));
        Require(ResolveQuerySLASuffixModel(energy_histogram.sla_suffix_model, energy_histogram.scheduler_policy) == QuerySLASuffixModel::HISTOGRAM,
                "explicit predictor survives later scheduler option");
        energy_histogram.sla_suffix_model = QuerySLASuffixModel::MODEL_DEFAULT;
        Require(ResolveQuerySLASuffixModel(energy_histogram.sla_suffix_model, energy_histogram.scheduler_policy) == QuerySLASuffixModel::POINT,
                "energy model_default resolves to Point");
        const char *bootstrap_args[] = {"scheduler_workload", "train-profiles", "--database", "10=/tmp/db", "--queries-dir", "/tmp/q",
            "--profile-output", "/tmp/fresh.bin", "--sla-suffix-bootstrap", "on", "--repetitions", "15"};
        auto bootstrap = ParseConfig(sizeof(bootstrap_args)/sizeof(bootstrap_args[0]), const_cast<char **>(bootstrap_args));
        Require(bootstrap.sla_suffix_bootstrap && bootstrap.sla_suffix_bootstrap_observations == 15, "balanced bootstrap configuration");
        WorkloadConfig lag_config;
        lag_config.max_driver_lag_ms = 10;
        RequestResult lag_result;
        lag_result.status = "ok";
        lag_result.request.deadline_offset_ns = 1000000000;
        lag_result.arrival_ns = 1001000000;
        lag_result.submit_begin_ns = 1002000000;
        lag_result.submit_end_ns = 1202000000;
        lag_result.database_registration_cpu_ns = 150000000;
        lag_result.result_ready_ns = 1300000000;
        auto lag_summary = SummarizeRun(lag_config, {lag_result.request}, {lag_result}, 1000000000, 0, 0);
        Require(lag_summary.valid, "database registration latency is not driver lag");
        Require(lag_summary.p99_driver_handoff_lag_ms == 2 && lag_summary.p99_submission_lag_ms == 202,
                "handoff and registration are measured separately");
        Require(lag_summary.p99_database_registration_cpu_ms == 150, "registration CPU time is preserved");
        lag_result.submit_begin_ns = 1020000000;
        Require(!SummarizeRun(lag_config, {lag_result.request}, {lag_result}, 1000000000, 0, 0).valid,
                "actual driver lag invalidates the run");
        lag_config.package_energy = true;
        lag_result.submit_begin_ns = 1002000000;
        lag_result.admitted_ns = 1010000000;
        lag_result.admission_dispatched_ns = 1011000000;
        lag_result.execution_started_ns = 1250000000;
        auto energy_lag_summary = SummarizeRun(lag_config, {lag_result.request}, {lag_result}, 1000000000, 0, 0);
        Require(energy_lag_summary.valid && energy_lag_summary.p99_admission_resume_lag_ms == 240,
                "database initialization and scheduler wait remain diagnostic, not notification delay");
        lag_result.admission_dispatched_ns = 1030000000;
        Require(!SummarizeRun(lag_config, {lag_result.request}, {lag_result}, 1000000000, 0, 0).valid,
                "delayed admission notification invalidates energy runs");
		WorkloadConfig admission_config;
		admission_config.admission_policy = QueryAdmissionPolicy::EDF;
		RequestResult on_time, late;
		on_time.status = late.status = "ok";
		on_time.request.request_id = 1;
		late.request.request_id = 2;
		on_time.request.deadline_offset_ns = 100000000;
		late.request.deadline_offset_ns = 200000000;
		on_time.request.sla_penalty_per_s = late.request.sla_penalty_per_s = 1;
		on_time.arrival_ns = late.arrival_ns = 1000000000;
		on_time.submit_begin_ns = late.submit_begin_ns = 1000000000;
		on_time.submit_end_ns = late.submit_end_ns = 1010000000;
		on_time.admitted_ns = on_time.execution_started_ns = 1050000000;
		late.admitted_ns = late.execution_started_ns = 1300000000;
		on_time.result_ready_ns = 1100000000;
		late.result_ready_ns = 1400000000;
		auto admission_summary = SummarizeRun(admission_config, {on_time.request, late.request}, {on_time, late},
		                                     1000000000, 1, 1);
		Require(admission_summary.valid && admission_summary.admission_policy == "edf" && admission_summary.admission_cap == 128,
		        "summary identifies the independent admission policy and cap");
		Require(admission_summary.late_at_admission_count == 1 && admission_summary.queued_count == 2,
		        "late admission is distinct from queueing and terminal deadline misses");
		Require(std::abs(admission_summary.mean_arrival_to_admission_s - .175) < 1e-12 &&
		            std::abs(admission_summary.p95_arrival_to_admission_s - .2875) < 1e-12,
		        "arrival-to-admission latency uses scheduled arrivals and interpolated percentiles");
		Require(std::abs(admission_summary.total_sla_cost - .2) < 1e-12 && admission_summary.sla_miss_count == 1,
		        "admission diagnostics preserve the end-to-end tardiness objective");
        Require(ResultFingerprintsMatch("v2:2:123:2:100:20:30:5000", "v2:2:123:2:100:20.000000000001:30:5000"),
                "floating aggregation order tolerates small numeric jitter");
        Require(!ResultFingerprintsMatch("v2:2:123:2:100:20:30:5000", "v2:2:124:2:100:20:30:5000"),
                "a changed non-floating row hash fails correctness");
        Require(!ResultFingerprintsMatch("v2:2:123:2:100:20:30:5000", "v2:2:123:2:100:21:30:5000"),
                "changed keyed numeric probes fail correctness");
		std::cout << "scheduler_workload tests passed\n";
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
