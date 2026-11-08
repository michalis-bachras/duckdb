#include "scheduler_workload.hpp"
#include "duckdb/parallel/query_sla_model.hpp"
#include "phase_pair_controller.hpp"
#include "live_sla_observer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/pipeline_dvfs_profiler.hpp"
#include "duckdb.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_admission_controller.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/query_sla_scheduler.hpp"
#include <cstdlib>
#include <chrono>
#include <algorithm>
#include <tuple>

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

static void TestInitializationProgress() {
	QuerySLAModelInput input;
	input.metadata.valid = true;
	input.metadata.db_query_id = 1;
	input.metadata.deadline_ns = 2500000000ULL;
	input.metadata.sla_penalty_per_s = 1;
	input.metadata.estimated_service_ns = 5000000000ULL;
	input.metadata.query_start_ns = input.now_ns = 1000000000ULL;
	input.epoch_ns = 100000000;
	input.event_kind = QueryActivationEventKind::INITIALIZE;
	input.work.parallelism_valid = true;
	input.work.preferred_parallelism = 1;
	input.suffix = QuerySLALifecycleSuffixPointMass(input.metadata.estimated_service_ns);
	auto before = BuildQuerySLAModel(input);
	Require(before.valid && !QuerySLAHasCompletionForecast(before),
	        "initialization priors support dispatch but cannot certify a hardware downshift");
	Require(before.suffix_p90_ns == 5000000000.0, "initialization retains pending query service");
	input.now_ns += 14000000000ULL;
	auto queued = BuildQuerySLAModel(input);
	Require(queued.suffix_p90_ns == before.suffix_p90_ns && queued.mandatory_cost[0] > before.mandatory_cost[0],
	        "waiting does not consume unexecuted query service");
	input.suffix = QuerySLALifecycleSuffixPointMass();
	auto unknown = BuildQuerySLAModel(input);
	Require(unknown.valid && !QuerySLAHasCompletionForecast(unknown),
	        "unknown initialization remains dispatchable without advertising imminent completion");

	input.metadata.sla_penalty_per_s = 0;
	input.metadata.deadline_ns = input.now_ns + 1000000000000ULL;
	auto initialize = BuildQuerySLAModel(input);
	Require(initialize.valid && initialize.mandatory_gain[0] == 0 && initialize.optional_gain[0] == 0,
	        "required progress does not invent a positive SLA marginal gain");
	QuerySLAModel data;
	data.valid = true;
	data.db_query_id = 99;
	data.event_kind = QueryActivationEventKind::PIPELINE;
	data.demand_cap = 4;
	data.mandatory_gain = {100, 90, 80, 70};
	data.optional_gain = {0, 0, 0, 0};
	auto allocation = AllocateQuerySLAWorkers({data, initialize}, 4, false, true);
	Require(allocation.mandatory[1] == 1 && allocation.mandatory[0] == 3 &&
	        allocation.tokens.size() == 4 && allocation.unassigned_workers == 0,
	        "initialization progresses while data pipelines share the remaining worker budget");
	Require(AllocateQuerySLAWorkers({initialize}, 0, false, true).mandatory[0] == 0,
	        "required progress cannot exceed a zero worker budget");
	for (auto kind : {QueryActivationEventKind::PREPARE_FINISH, QueryActivationEventKind::FINISH,
	                  QueryActivationEventKind::COMPLETE}) {
		initialize.event_kind = kind;
		Require(AllocateQuerySLAWorkers({data, initialize}, 1, false, true).mandatory[1] == 1,
		        "serial lifecycle transitions progress despite competing positive data gains");
	}

	auto oldest = BuildQuerySLAModel(input);
	oldest.db_query_id = 1;
	oldest.query_start_ns = 10;
	auto newer = oldest;
	newer.db_query_id = 2;
	newer.query_start_ns = 20;
	auto newest = newer;
	newest.db_query_id = 3;
	newest.query_start_ns = 30;
	auto first = AllocateQuerySLAWorkers({newest, oldest, newer}, 2, false, true);
	Require(first.mandatory == vector<idx_t> {0, 1, 1},
	        "lifecycle contention serves older queries independent of unordered capture order");
	auto second = AllocateQuerySLAWorkers({newest, newer}, 1, false, true);
	Require(second.mandatory == vector<idx_t> {0, 1} &&
	        AllocateQuerySLAWorkers({newest}, 1, false, true).mandatory[0] == 1,
	        "later lifecycle queries progress as earlier serial stages complete");
}

class ResidualDispatchEvent final : public Event {
public:
	explicit ResidualDispatchEvent(Executor &executor, QueryActivationEventKind kind) : Event(executor) {
		SetQueryActivationInfo(1, kind, 1);
		total_tasks = 1;
		work.valid = true;
		work.pipeline_id = 1;
		work.profile_identity.valid = true;
		work.profile_identity.pipeline_id = 1;
		work.profile_identity.pipeline_signature_hash = 101;
		work.profile_identity.pipeline_structure_hash = 201;
		work.parallelism_valid = true;
		work.preferred_parallelism = 1;
		work.scheduled_tasks = work.remaining_tasks = 1;
		work.lifecycle_tail_estimate.valid = true;
		work.lifecycle_tail_estimate.p90_ns = 1000000;
	}
	void Schedule() override {}
	bool GetPipelineWorkSnapshot(PipelineWorkSnapshot &result) const override {
		result = work;
		return true;
	}
private:
	PipelineWorkSnapshot work;
};

class ResidualDispatchTask final : public Task {
public:
	TaskExecutionResult Execute(TaskExecutionMode) override { return TaskExecutionResult::TASK_FINISHED; }
};

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
		TestInitializationProgress();
		{
			DuckDB database(nullptr); Connection connection(database);
			auto &admission = database.instance->GetQueryAdmissionController();
			admission.SetPolicy(QueryAdmissionPolicy::SJF);
			QueryRequestMetadata m; m.valid = true; m.template_id = m.scale_factor = 1;
			m.deadline_ns = 100; m.db_query_id = 1;
			auto active = admission.EnqueueOrAcquire(*connection.context, m, 1);
			m.db_query_id = 2; m.estimated_service_ns = 30;
			auto longer = admission.EnqueueOrAcquire(*connection.context, m, 1);
			m.db_query_id = 3; m.estimated_service_ns = 10;
			auto shorter = admission.EnqueueOrAcquire(*connection.context, m, 1);
			m.db_query_id = 4; m.estimated_service_ns = 0;
			auto unknown = admission.EnqueueOrAcquire(*connection.context, m, 1);
			m.db_query_id = 5; m.estimated_service_ns = 10;
			auto cancelled = admission.EnqueueOrAcquire(*connection.context, m, 1);
			cancelled.reset();
			const string activity_path="/tmp/duckdb_admission_activity_test.json";
			LiveSLAObserver activity_observer(*database.instance,activity_path,-1);
			string activity_json;
			for (idx_t attempt=0;attempt<100;attempt++) {
				std::ifstream input(activity_path);std::ostringstream contents;contents<<input.rdbuf();activity_json=contents.str();
				if (activity_json.find("\"registered_queries\":4")!=string::npos) break;
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			activity_observer.Stop();std::remove(activity_path.c_str());std::remove((activity_path+".tmp").c_str());
			Require(activity_observer.Error().empty() && activity_json.find("\"registered_queries\":4")!=string::npos &&
			        activity_json.find("admission_sjf")!=string::npos,
			        "admission activity includes the active request and waiting requests without SLA forecasts");
			Require(active->IsAdmitted() && !shorter->IsAdmitted(), "SJF respects admission cap");
			active.reset();
			Require(shorter->IsAdmitted() && !longer->IsAdmitted(), "SJF admits prior shorter service first");
			shorter.reset();
			Require(longer->IsAdmitted() && !unknown->IsAdmitted(), "unknown service follows known estimates");
			longer.reset();
			Require(unknown->IsAdmitted(), "SJF drains unknown jobs after known work");
		}
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
			for (auto admission : {"fifo", "edf", "sjf"}) {
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
		Require(round_trip[0].estimated_service_ns > 0, "prior calibration survives schedule round trip");
		auto changed_hint = round_trip;
		changed_hint[0].estimated_service_ns++;
		Require(ScheduleHash(changed_hint) != ScheduleHash(round_trip), "schedule hash protects size hints");
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

        // Keep residual dispatch coverage and separately test required initialization
        // with known and unknown remaining query work, even with zero SLA gains.
        for (idx_t scenario = 0; scenario < 3; scenario++) {
            const bool initialization = scenario != 0;
            DBConfig config;
            config.options.maximum_threads = 1;
            config.options.query_sla_residual_workers_enabled = true;
            config.options.query_sla_energy_hardware_control_enabled = false;
            DuckDB db(nullptr, &config);
            Connection con(db);
            Executor executor(*con.context);
            auto producer = db.instance->GetScheduler().CreateProducer();
            db.instance->SetQuerySchedulerPolicy(QuerySchedulerPolicy::SLA);
            auto &scheduler = db.instance->GetQuerySLAScheduler();
            QueryRequestMetadata metadata;
            metadata.valid = true;
            metadata.db_query_id = metadata.request_id = 1;
            metadata.template_id = metadata.scale_factor = 1;
            metadata.query_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            metadata.deadline_ns = metadata.query_start_ns + 60000000000ULL;
            metadata.estimated_service_ns = scenario == 1 ? 5000000000ULL : 0;
            // A zero-weight request has zero SLA marginal gains but must still run
            // under the work-conserving policy.
            metadata.sla_penalty_per_s = 0;
            if (!initialization) {
                auto &store = db.instance->GetQueryRequestProfileStore();
                store.SetSuffixBootstrapMode(true, 15);
                PipelineProfilingInfo profile;
                profile.pipeline_id = 1;
                profile.pipeline_signature_hash = 101;
                profile.pipeline_structure_hash = 201;
                profile.start_ns = 10;
                profile.tasks_done_ns = 20;
                profile.finish_done_ns = 30;
                profile.planned_input_chunks_equiv = 1;
                profile.planned_input_native_unit = "row";
                profile.task_count = profile.worker_task_count = 1;
                profile.worker_task_duration_ns = 10;
                for (idx_t sample = 0; sample < 15; sample++) {
                    auto training = metadata;
                    training.db_query_id = 1000 + sample;
                    training.query_start_ns = 10;
                    store.RecordQueryCompletion(training, 31, {profile}, {}, QuerySLASuffixUpdateMode::BOOTSTRAP_ALL);
                }
                Require(store.SuffixBootstrapComplete(), "residual test suffix bootstrap");
                store.SetSuffixBootstrapMode(false);
            }
            scheduler.RegisterQuery(metadata, *producer, true);
            // Registered requests between events must not look like an empty workload.
            auto live_path = string("/tmp/scheduler_workload_observer_") + std::to_string(metadata.query_start_ns);
            LiveSLAObserver observer(*db.instance, live_path, -1);
            string observed;
            for (idx_t attempt = 0; attempt < 50; attempt++) {
                std::ifstream input(live_path);
                std::stringstream contents;
                contents << input.rdbuf();
                observed = contents.str();
                if (observed.find("\"registered_queries\":1") != string::npos) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            observer.Stop();
            std::remove(live_path.c_str());
            std::remove((live_path + ".tmp").c_str());
            Require(observer.Error().empty() && observed.find("\"registered_queries\":1") != string::npos &&
                    observed.find("\"active\":[]") != string::npos,
                    "live observation distinguishes an unmodeled registered request from no requests");
            auto event = make_shared_ptr<ResidualDispatchEvent>(executor, initialization
                ? QueryActivationEventKind::INITIALIZE : QueryActivationEventKind::PIPELINE);
            scheduler.OnEventScheduled(metadata.db_query_id, event, 1);
            db.instance->GetScheduler().ScheduleTask(*producer, make_shared_ptr<ResidualDispatchTask>());
            scheduler.RequestInitialEpoch();
            shared_ptr<Task> task;
            QuerySLAWorkerSelection selection;
            Require(scheduler.TryDequeueTask(0, task, &selection) == QuerySLADequeueResult::TASK_FOUND,
                    "a published zero-gain residual slot dispatches its queued task");
            auto snapshot = scheduler.GetSnapshot();
            Require(snapshot.size() == 1 && snapshot[0].assigned_workers == 1 &&
                    (initialization ? snapshot[0].mandatory_workers : snapshot[0].residual_workers) == 1,
                    "dispatch consumes one published lifecycle/residual slot within the budget");
            auto trace = scheduler.GetEpochTrace();
            Require(!trace.empty() && trace.back().assigned_workers == 0 &&
                    (initialization ? trace.back().mandatory_workers : trace.back().residual_workers) == 1,
                    "the lifecycle/residual grant was published before task assignment");
            if (initialization) {
                Require(!snapshot[0].model_valid && !trace.back().model_valid,
                        "initialization priors cannot advertise a trained completion forecast");
                Require(trace.back().suffix_p90_ns == metadata.estimated_service_ns,
                        "activation and periodic epochs retain the pending query service prior");
                Require(trace.back().predicted_query_finish_p90_ns == 0,
                        "initialization does not certify imminent completion to the hardware guard");
                Require(trace.back().suffix_primary_level == (scenario == 1 ? "query_service_prior" : "unknown"),
                        "an initialization prior is distinguished from a trained suffix");
            }
            scheduler.ReleaseTaskAssignment(0, selection);
            scheduler.OnEventFinished(metadata.db_query_id, *event);
            scheduler.UnregisterQuery(metadata.db_query_id);
        }
        // Execution priority grants respect pipeline capacity and backfill a serial phase.
        QuerySLAModel serial, parallel;
        serial.valid = parallel.valid = true;
        serial.db_query_id = 1; parallel.db_query_id = 2;
        serial.deadline_ns = 2000000000ULL; parallel.deadline_ns = 3000000000ULL;
        serial.demand_cap = 1; parallel.demand_cap = 4;
        serial.event_kind = QueryActivationEventKind::FINISH;
        parallel.event_kind = QueryActivationEventKind::PIPELINE;
        parallel.remaining_work_units = 4;
        parallel.service_rates = {0, 1, 2, 3, 4};
        auto backfill = AllocateDeadlineBackfillWorkers({parallel, serial}, 4, false);
        Require(backfill.residual[0] == 3 && backfill.residual[1] == 1 && backfill.unassigned_workers == 0,
                "execution EDF backfills pipeline capacity without oversubscription");
        auto expired = parallel;
        expired.db_query_id = 3; expired.deadline_ns = 500000000ULL;
        auto edf = AllocateDeadlineBackfillWorkers({expired, parallel}, 4, false);
        auto triage = AllocateDeadlineBackfillWorkers({expired, parallel}, 4, true);
        Require(edf.residual[0] == 4 && triage.residual[1] == 4,
                "triage distinguishes individually infeasible work from still recoverable work");
        auto late_long = expired;
        late_long.remaining_work_units = 8;
        auto late_drain = AllocateDeadlineBackfillWorkers({late_long, expired}, 4, true);
        Require(late_drain.residual[1] == 4, "late work drains in remaining-service order");
        QuerySLAModel invalid;
        auto bounded = AllocateDeadlineBackfillWorkers({invalid, serial}, 8, true);
        Require(bounded.residual[0] == 0 && bounded.residual[1] == 1 && bounded.unassigned_workers == 7,
                "invalid models receive no fabricated capacity and no grant exceeds demand");


        auto almost_done = expired;
        almost_done.remaining_work_units = 1;
        almost_done.deadline_ns = 100000000ULL;
        auto srpt = AllocateDeadlineBackfillWorkers({parallel, almost_done}, 4, false, true);
        auto deadline_triage = AllocateDeadlineBackfillWorkers({parallel, almost_done}, 4, true);
        Require(srpt.residual[1] == 4 && deadline_triage.residual[0] == 4,
                "remaining-service priority can finish an almost-complete late job before a larger on-time job");
        auto serial_srpt = AllocateDeadlineBackfillWorkers({parallel, serial}, 4, false, true);
        Require(serial_srpt.residual[1] == 1 && serial_srpt.residual[0] == 3,
                "SRPT backfill preserves pipeline demand caps and uses spare capacity");

        auto incumbent = parallel;
        Require(PipelineTask::NextAdaptiveChunkCount(50,2000000,2000000) == 50,
                "adaptive quantum holds an on-target task budget");
        Require(PipelineTask::NextAdaptiveChunkCount(50,20000,2000000) == 100 &&
                PipelineTask::NextAdaptiveChunkCount(50,200000000,2000000) == 25,
                "adaptive quantum bounds one-step growth and shrinkage");
        Require(PipelineTask::NextAdaptiveChunkCount(1,200000000,2000000) == 1 &&
                PipelineTask::NextAdaptiveChunkCount(8192,20000,2000000) == 8192,
                "adaptive quantum preserves positive bounded chunk budgets");
        incumbent.db_query_id = 1; incumbent.deadline_ns = 10000000000ULL;
        incumbent.service_rates = {0,10,20,30,40}; incumbent.remaining_work_units = 10;
        incumbent.previous_workers = 4; incumbent.prior_service_ns = 1000000000ULL;
        auto challenger = incumbent;
        challenger.db_query_id = 2; challenger.previous_workers = 0; challenger.remaining_work_units = 9;
        auto held = AllocatePriorityBackfillWorkers({incumbent,challenger},4,QuerySLABackfillPriority::STABLE_SRPT);
        Require(held.residual[0] == 4, "small forecast changes do not displace the incumbent");
        challenger.remaining_work_units = 7;
        auto moved = AllocatePriorityBackfillWorkers({incumbent,challenger},4,QuerySLABackfillPriority::STABLE_SRPT);
        Require(moved.residual[1] == 4, "a material service improvement can displace the incumbent");
        challenger.deadline_ns = 300000000ULL;
        auto urgent = AllocatePriorityBackfillWorkers({incumbent,challenger},4,QuerySLABackfillPriority::STABLE_SRPT);
        Require(urgent.residual[1] == 4, "deadline urgency overrides incumbency");
        challenger.deadline_ns = 20000000000ULL;
        challenger.model_now_ns = incumbent.model_now_ns = 5000000000ULL;
        challenger.query_start_ns = 1; incumbent.query_start_ns = 4900000000ULL;
        challenger.remaining_work_units = 100;
        auto aged = AllocatePriorityBackfillWorkers({incumbent,challenger},4,QuerySLABackfillPriority::STABLE_SRPT);
        Require(aged.residual[1] == 4, "aging lets deferred large work regain priority");
        challenger.prior_service_ns = 100000000ULL;
        auto psjf = AllocatePriorityBackfillWorkers({incumbent,challenger},4,QuerySLABackfillPriority::PSJF);
        Require(psjf.residual[1] == 4, "PSJF uses prior size rather than an underestimated remaining size");
        auto early_long = parallel;
        early_long.db_query_id = 1; early_long.deadline_ns = 3000000000ULL;
        early_long.remaining_work_units = 12; early_long.model_now_ns = 0;
        auto early_short = early_long;
        early_short.db_query_id = 2; early_short.remaining_work_units = 4;
        auto second_short = early_short; second_short.db_query_id = 3;
        auto subset = AllocatePriorityBackfillWorkers({early_long,early_short,second_short},4,
                                                      QuerySLABackfillPriority::MOORE);
        Require(subset.residual[1] == 4 && subset.residual[0] == 0,
                "collective feasibility defers one long job to keep two shorter jobs on time");
        // Independent exhaustive oracle: all 24 orders of four jobs, for 32
        // different sizes/deadlines. Check the chosen first job, not DP internals.
        for (idx_t sample = 0; sample < 32; sample++) {
            vector<QuerySLAModel> jobs;
            for (idx_t j = 0; j < 4; j++) {
                auto job = parallel;
                job.db_query_id = j+1; job.model_now_ns = 0;
                job.remaining_work_units = (sample+j*7)%12+1;
                job.deadline_ns = ((sample*3+j*5)%20+1)*250000000ULL;
                jobs.push_back(job);
            }
            vector<idx_t> permutation {0,1,2,3};
            std::tuple<idx_t,uint64_t,uint64_t> best;
            idx_t first_job = 0; bool found = false;
            do {
                uint64_t completion = 0, tardiness = 0, flow = 0; idx_t misses = 0;
                for (auto j:permutation) {
                    completion += jobs[j].remaining_work_units*250000000ULL;
                    if (completion > jobs[j].deadline_ns) {
                        misses++; tardiness += completion-jobs[j].deadline_ns;
                    }
                    flow += completion;
                }
                auto objective = std::make_tuple(misses,tardiness,flow);
                if (!found || objective < best) {found = true;best = objective;first_job = permutation[0];}
            } while (std::next_permutation(permutation.begin(),permutation.end()));
            auto exact = AllocatePriorityBackfillWorkers(jobs,4,QuerySLABackfillPriority::BOUNDED_DP);
            Require(exact.residual[first_job] == 4,"bounded queue DP matches exhaustive scheduling oracle");
        }
        vector<QuerySLAModel> larger;
        for (idx_t j = 0; j < 9; j++) {auto job=early_short;job.db_query_id=j+1;larger.push_back(job);}
        auto fallback = AllocatePriorityBackfillWorkers(larger,4,QuerySLABackfillPriority::BOUNDED_DP);
        auto moore_fallback = AllocatePriorityBackfillWorkers(larger,4,QuerySLABackfillPriority::MOORE);
        Require(fallback.residual == moore_fallback.residual,"queue DP uses bounded fallback above eight jobs");
        auto primary = parallel;primary.db_query_id=1;primary.prior_service_ns=2000000000ULL;
        auto next = parallel;next.db_query_id=2;next.prior_service_ns=1000000000ULL;
        uint64_t committed=0;
        auto initial_primary=AllocateCommittedSJFWorkers({primary,next},4,committed);
        Require(committed==2 && initial_primary.residual[1]==4,"primary starts in prior-size order");
        auto newcomer=next;newcomer.db_query_id=3;newcomer.prior_service_ns=100000000ULL;
        auto retained=AllocateCommittedSJFWorkers({next,newcomer},4,committed);
        Require(committed==2 && retained.residual[0]==4 && retained.residual[1]==0,
                "a shorter arrival does not displace the committed query");
        auto serial_primary=next;serial_primary.demand_cap=1;
        auto lent=AllocateCommittedSJFWorkers({serial_primary,newcomer},4,committed);
        Require(lent.residual[0]==1 && lent.residual[1]==3,"only spare serial-phase capacity is lent");
        auto boundary=AllocateCommittedSJFWorkers({newcomer},4,committed);
        Require(committed==2 && boundary.residual[0]==4,"a pipeline gap preserves query commitment");
        auto returned=AllocateCommittedSJFWorkers({next,newcomer},4,committed);
        Require(returned.residual[0]==4 && returned.residual[1]==0,"primary regains its parallel capacity");
        committed=0;
        auto replacement=AllocateCommittedSJFWorkers({primary,newcomer},4,committed);
        Require(committed==3 && replacement.residual[1]==4,"completion permits selecting the next shortest query");
		std::cout << "scheduler_workload tests passed\n";
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
