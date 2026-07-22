#include "catch.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_model.hpp"
#include "duckdb/parallel/query_sla_energy_model.hpp"
#include "duckdb/parallel/query_sla_energy_planner.hpp"

#include <algorithm>
#include <cmath>

using namespace duckdb;

namespace {

static QuerySLAModelInput BaseModelInput() {
	QuerySLAModelInput input;
	input.metadata.valid = true;
	input.metadata.db_query_id = 42;
	input.metadata.deadline_ns = 2500000000ULL;
	input.metadata.sla_penalty_per_s = 2;
	input.event_kind = QueryActivationEventKind::PIPELINE;
	input.generation = 7;
	input.now_ns = 1000000000ULL;
	input.epoch_ns = 1000000000ULL;
	input.work.pipeline_id = 3;
	input.work.parallelism_valid = true;
	input.work.preferred_parallelism = 4;
	input.work.remaining_chunks_equiv = 20;
	input.work.selected_throughput_valid = true;
	input.work.selected_single_worker_chunks_per_s = 10;
	input.work.continuation_estimate.valid = true;
	input.work.continuation_estimate.kind = ContinuationEstimateKind::NS_PER_WORK_UNIT;
	input.work.continuation_estimate.p90 = 100000000;
	input.suffix = QuerySLALifecycleSuffixPointMass();
	return input;
}

static QuerySLAModel AllocationModel(uint64_t query_id, vector<double> mandatory, vector<double> optional) {
	QuerySLAModel model;
	model.valid = true;
	model.db_query_id = query_id;
	model.demand_cap = mandatory.size();
	model.mandatory_gain = std::move(mandatory);
	model.optional_gain = std::move(optional);
	return model;
}

static QueryRequestHardwareConfiguration TestHardware(uint32_t core_khz, uint32_t uncore_khz) {
	QueryRequestHardwareConfiguration result;
	result.core_frequency_khz = core_khz;
	result.uncore_frequency_khz = uncore_khz;
	return result;
}

static QueryRequestPipelineHardwareProfileEstimate TestHardwareProfile(uint32_t core_khz, uint32_t uncore_khz,
	                                                                    double throughput, double active_power_w,
	                                                                    bool mature = true) {
	QueryRequestPipelineHardwareProfileEstimate result;
	result.valid = true;
	result.mature = mature;
	result.hardware = TestHardware(core_khz, uncore_khz);
	result.throughput_sample_count = mature ? PIPELINE_HARDWARE_PROFILE_MIN_STABLE_SAMPLES : 1;
	result.power_sample_count = mature ? PIPELINE_HARDWARE_PROFILE_MIN_STABLE_SAMPLES : 1;
	result.safe_work_units_per_s = throughput;
	result.mean_work_units_per_s = throughput;
	result.ewma_work_units_per_s = throughput;
	result.mean_active_power_w = active_power_w;
	result.ewma_active_power_w = active_power_w;
	return result;
}

static QueryHardwareWorkerTopology TestWorker(idx_t worker_id, int cpu, int socket, int core) {
	QueryHardwareWorkerTopology result;
	result.worker_id = worker_id;
	result.logical_cpu = cpu;
	result.socket_id = socket;
	result.physical_core_id = core;
	return result;
}

static QueryHardwarePowerModelSnapshot TestPowerModel() {
	QueryHardwarePowerModelSnapshot result;
	result.valid = true;
	result.minimum_core_frequency_khz = 1200000;
	result.minimum_uncore_frequency_khz = 1200000;
	for (int socket = 0; socket < 2; socket++) {
		for (auto core_khz : {1200000U, 2200000U, 3000000U}) {
			for (auto uncore_khz : {1200000U, 1800000U, 2800000U}) {
				QueryHardwarePowerCalibrationEntry entry;
				entry.socket_id = socket;
				entry.core_frequency_khz = core_khz;
				entry.uncore_frequency_khz = uncore_khz;
				entry.base_power_w = 20.0 + static_cast<double>(core_khz - 1200000U) / 200000.0 +
				                     static_cast<double>(uncore_khz - 1200000U) / 400000.0;
				result.entries.push_back(entry);
			}
		}
	}
	return result;
}

static QuerySLAEnergyModelInput PlannerQuery(uint64_t query_id, uint64_t deadline_ns = 2500000000ULL,
	                                          idx_t demand_cap = 4) {
	auto sla_input = BaseModelInput();
	sla_input.metadata.db_query_id = query_id;
	sla_input.metadata.deadline_ns = deadline_ns;
	sla_input.work.preferred_parallelism = demand_cap;
	QuerySLAEnergyModelInput result;
	result.sla_input = sla_input;
	result.reference_model = BuildQuerySLAModel(sla_input);
	result.reference_hardware = TestHardware(3000000, 2800000);
	result.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	result.hardware_profiles.push_back(TestHardwareProfile(2200000, 1800000, 10, 5));
	result.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 6, 3));
	return result;
}

static QuerySLAEnergyPlannerInput PlannerInput(QuerySLAEnergyModelInput query, idx_t worker_budget,
	                                           vector<QueryHardwareWorkerTopology> topology) {
	QuerySLAEnergyPlannerInput result;
	result.queries.push_back(std::move(query));
	result.topology = std::move(topology);
	result.power_model = TestPowerModel();
	result.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	result.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	result.worker_budget = worker_budget;
	result.epoch_ns = 1000000000ULL;
	result.energy_lambda = 0;
	return result;
}

} // namespace

TEST_CASE("SLA model preserves linear single-worker throughput semantics", "[api][sla][model]") {
	auto input = BaseModelInput();
	auto model = BuildQuerySLAModel(input);
	REQUIRE(model.valid);
	REQUIRE(model.service_rates == vector<double> {0, 10, 20, 30, 40});
	REQUIRE(model.mandatory_cost.size() == 5);
	REQUIRE(model.mandatory_cost[0] == Approx(3.0));
	REQUIRE(model.mandatory_cost[1] == Approx(1.0));
	REQUIRE(model.mandatory_cost[2] == Approx(0.0));
	REQUIRE(model.mandatory_gain[0] == Approx(2.0));
	REQUIRE(model.mandatory_gain[1] == Approx(1.0));
}

TEST_CASE("SLA model accepts an explicit non-linear aggregate service curve", "[api][sla][model]") {
	auto input = BaseModelInput();
	input.service_rates = {0, 10, 15, 18, 20};
	auto model = BuildQuerySLAModel(input);
	REQUIRE(model.valid);
	REQUIRE(model.service_rates == input.service_rates);
	REQUIRE(model.mandatory_cost[1] > model.mandatory_cost[2]);
	REQUIRE(model.mandatory_cost[2] == Approx(0.0));
	REQUIRE(model.mandatory_cost[4] == Approx(0.0));

	input.service_rates.pop_back();
	auto incomplete = BuildProvisionalQuerySLAModel(input);
	REQUIRE_FALSE(incomplete.valid);
	REQUIRE(incomplete.error == "active pipeline event has an incomplete worker service-rate curve");
}

TEST_CASE("SLA allocator separates mandatory optional and residual capacity", "[api][sla][model]") {
	vector<QuerySLAModel> models;
	models.push_back(AllocationModel(10, {9, 2, -1}, {4, 1, 1}));
	models.push_back(AllocationModel(20, {8, 7, -2}, {6, 3, 0}));

	auto conserving = AllocateQuerySLAWorkers(models, 6, true);
	REQUIRE(conserving.mandatory == vector<idx_t> {2, 2});
	REQUIRE(conserving.optional == vector<idx_t> {1, 0});
	REQUIRE(conserving.residual == vector<idx_t> {0, 1});
	REQUIRE(conserving.unassigned_workers == 0);

	auto non_conserving = AllocateQuerySLAWorkers(models, 6, false);
	REQUIRE(non_conserving.mandatory == vector<idx_t> {2, 2});
	REQUIRE(non_conserving.optional == vector<idx_t> {1, 0});
	REQUIRE(non_conserving.residual == vector<idx_t> {0, 0});
	REQUIRE(non_conserving.unassigned_workers == 1);
}

TEST_CASE("SLA lifecycle events remain serial and need no throughput model", "[api][sla][model]") {
	auto input = BaseModelInput();
	input.event_kind = QueryActivationEventKind::FINISH;
	input.work.preferred_parallelism = 8;
	input.work.selected_throughput_valid = false;
	input.work.continuation_estimate.valid = false;
	auto model = BuildQuerySLAModel(input);
	REQUIRE(model.valid);
	REQUIRE(model.demand_cap == 1);
	REQUIRE(model.service_rates == vector<double> {0, 0});
	REQUIRE(model.mandatory_cost.size() == 2);
}

TEST_CASE("SLA energy allocator lowers hardware only with mature SLA-equivalent profiles",
	      "[api][sla][energy_model]") {
	auto sla_input = BaseModelInput();
	QuerySLAEnergyModelInput input;
	input.sla_input = sla_input;
	input.reference_model = BuildQuerySLAModel(sla_input);
	input.reference_hardware.core_frequency_khz = 3000000;
	input.reference_hardware.uncore_frequency_khz = 2800000;

	QueryRequestPipelineHardwareProfileEstimate reference;
	reference.mature = true;
	reference.hardware = input.reference_hardware;
	reference.safe_work_units_per_s = 10;
	reference.ewma_active_power_w = 10;
	input.hardware_profiles.push_back(reference);
	QueryRequestPipelineHardwareProfileEstimate efficient = reference;
	efficient.hardware.core_frequency_khz = 2200000;
	efficient.hardware.uncore_frequency_khz = 1800000;
	efficient.ewma_active_power_w = 5;
	input.hardware_profiles.push_back(efficient);

	auto allocation = AllocateQuerySLAEnergyWorkers({input}, 2, 1.0);
	REQUIRE(allocation.queries.size() == 1);
	REQUIRE(allocation.queries[0].mandatory_workers == 2);
	REQUIRE(allocation.queries[0].workers.size() == 2);
	REQUIRE(allocation.queries[0].workers[0].hardware == efficient.hardware);
	REQUIRE(allocation.queries[0].predicted_sla_cost == Approx(input.reference_model.mandatory_cost[2]));

	input.hardware_profiles[1].mature = false;
	auto cold = AllocateQuerySLAEnergyWorkers({input}, 2, 1.0);
	REQUIRE(cold.queries[0].mandatory_workers == 2);
	REQUIRE(cold.queries[0].workers[0].hardware == input.reference_hardware);
}

TEST_CASE("SLA energy planner preserves mandatory counts and uses one mature hardware pair",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto input = PlannerInput(PlannerQuery(100), 2, topology);
	auto reference = AllocateQuerySLAWorkers({input.queries[0].reference_model}, 2, false);
	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.queries.size() == 1);
	REQUIRE(plan.queries[0].mandatory_workers == reference.mandatory[0]);
	REQUIRE(plan.diagnostics.mandatory_workers == reference.mandatory[0]);
	REQUIRE(plan.workers.size() == reference.mandatory[0]);
	for (const auto &worker : plan.workers) {
		REQUIRE(worker.mandatory);
		REQUIRE(worker.preferred_hardware == TestHardware(2200000, 1800000));
		REQUIRE(worker.normal_hardware == TestHardware(2200000, 1800000));
	}
	REQUIRE(plan.workers[0].worker_id == 0);
	REQUIRE(plan.workers[1].worker_id == 1);
}

TEST_CASE("SLA energy planner rejects immature non-reference hardware", "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0)};
	auto query = PlannerQuery(101);
	query.hardware_profiles[1] = TestHardwareProfile(2200000, 1800000, 10, 5, false);
	auto plan = PlanQuerySLAEnergyEpoch(PlannerInput(std::move(query), 2, topology));
	REQUIRE(plan.valid);
	REQUIRE(plan.diagnostics.immature_pair_rejections > 0);
	for (const auto &worker : plan.workers) {
		REQUIRE(worker.preferred_hardware == TestHardware(3000000, 2800000));
	}
}

TEST_CASE("SLA energy planner reconciles domain-coupled pairs before publishing estimates",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 1, 0, 1)};
	auto query_a = PlannerQuery(201);
	query_a.hardware_profiles.clear();
	query_a.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_a.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 10, 2));
	auto query_b = PlannerQuery(202);
	query_b.hardware_profiles.clear();
	query_b.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_b.hardware_profiles.push_back(TestHardwareProfile(2200000, 2800000, 10, 4));

	QuerySLAEnergyPlannerInput input;
	input.queries = {query_a, query_b};
	input.topology = topology;
	input.power_model = TestPowerModel();
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 2;
	input.epoch_ns = 1000000000ULL;
	input.energy_lambda = 0;
	auto plan = PlanQuerySLAEnergyEpoch(input);

	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 2);
	REQUIRE(plan.diagnostics.domain_pair_promotions == 1);
	for (const auto &worker : plan.workers) {
		auto expected = worker.query_index == 0 ? TestHardware(3000000, 2800000)
		                                        : TestHardware(2200000, 2800000);
		REQUIRE(worker.normal_hardware == expected);
		REQUIRE(worker.execution_hardware == worker.normal_hardware);
	}
	REQUIRE(plan.queries[0].workers.size() == 1);
	REQUIRE(plan.queries[0].workers[0].hardware == TestHardware(3000000, 2800000));
	REQUIRE(plan.queries[1].workers.size() == 1);
	REQUIRE(plan.queries[1].workers[0].hardware == TestHardware(2200000, 2800000));
	REQUIRE(plan.queries[0].aggregate_service_rate == Approx(10));
	REQUIRE(plan.queries[1].aggregate_service_rate == Approx(10));
}

TEST_CASE("SLA energy planner applies O1 and O2 without exceeding demand", "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto plan = PlanQuerySLAEnergyEpoch(PlannerInput(PlannerQuery(102, 3500000000ULL), 4, topology));
	REQUIRE(plan.valid);
	REQUIRE(plan.queries[0].mandatory_workers == 1);
	REQUIRE(plan.diagnostics.o1_workers == 1);
	REQUIRE(plan.diagnostics.o2_workers == 2);
	REQUIRE(plan.workers.size() == 4);
	REQUIRE(plan.workers.size() <= plan.queries[0].workers.size());
	REQUIRE(plan.workers.size() <= 4);
}

TEST_CASE("SLA energy planner uses bounded O3 placement and optional-only exploration",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 4, 1, 0), TestWorker(3, 12, 1, 0)};
	auto input = PlannerInput(PlannerQuery(103, 3500000000ULL), 4, topology);
	input.exploration_enabled = true;
	input.exploration_seed = 17;
	input.epoch_generation = 3;
	auto plan = PlanQuerySLAEnergyEpoch(input);
	auto repeated = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(repeated.valid);
	REQUIRE(plan.diagnostics.o1_workers == 1);
	REQUIRE(plan.diagnostics.o3_workers == 2);
	REQUIRE(plan.diagnostics.uncore_probes <= 1);
	REQUIRE(plan.diagnostics.core_probes <= 1);
	REQUIRE(plan.diagnostics.uncore_probes == repeated.diagnostics.uncore_probes);
	REQUIRE(plan.diagnostics.core_probes == repeated.diagnostics.core_probes);
	REQUIRE(plan.workers.size() == repeated.workers.size());
	for (idx_t i = 0; i < plan.workers.size(); i++) {
		REQUIRE(plan.workers[i].worker_id == repeated.workers[i].worker_id);
		REQUIRE(plan.workers[i].execution_hardware == repeated.workers[i].execution_hardware);
		REQUIRE(plan.workers[i].exploration == repeated.workers[i].exploration);
	}
	for (const auto &worker : plan.workers) {
		if (worker.exploration) {
			REQUIRE(worker.optional);
			REQUIRE_FALSE(worker.mandatory);
			REQUIRE_FALSE(worker.liveness);
		}
	}
}

TEST_CASE("SLA energy exploration skips fully mature exact hardware pairs",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto query = PlannerQuery(104, 3500000000ULL);
	query.hardware_profiles.clear();
	for (auto core_khz : {1200000U, 2200000U, 3000000U}) {
		for (auto uncore_khz : {1200000U, 1800000U, 2800000U}) {
			query.hardware_profiles.push_back(TestHardwareProfile(core_khz, uncore_khz, 10, 5));
		}
	}
	auto input = PlannerInput(std::move(query), 4, topology);
	input.exploration_enabled = true;

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.diagnostics.core_probes == 0);
	REQUIRE(plan.diagnostics.uncore_probes == 0);
	for (const auto &worker : plan.workers) {
		REQUIRE_FALSE(worker.exploration);
	}
}

TEST_CASE("SLA energy exploration forces uncore only after four optional-only socket misses",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0)};
	auto query = PlannerQuery(105, 3500000000ULL, 2);
	query.sla_input.work.remaining_chunks_equiv = 1;
	auto input = PlannerInput(std::move(query), 2, topology);
	input.exploration_enabled = true;
	input.epochs_without_optional_only_socket = 2;

	auto before_threshold = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(before_threshold.valid);
	REQUIRE_FALSE(before_threshold.diagnostics.optional_only_socket_available);
	REQUIRE(before_threshold.diagnostics.uncore_probes == 0);
	REQUIRE(before_threshold.diagnostics.forced_uncore_probes == 0);

	input.epochs_without_optional_only_socket = 3;
	auto forced = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(forced.valid);
	REQUIRE_FALSE(forced.diagnostics.optional_only_socket_available);
	REQUIRE(forced.diagnostics.uncore_probes == 1);
	REQUIRE(forced.diagnostics.forced_uncore_probes == 1);
	REQUIRE(std::any_of(forced.workers.begin(), forced.workers.end(),
	                    [](const QuerySLAEnergyPlacedWorker &worker) { return worker.exploration; }));
}

TEST_CASE("SLA energy hardware manager applies upshifts and downshifts immediately",
          "[api][sla][energy_hardware]") {
	DBConfig config;
	config.options.query_sla_energy_hardware_control_enabled = false;
	DuckDB db(nullptr, &config);
	QueryHardwareManager manager(*db.instance);
	manager.RegisterWorker(0, 0);
	manager.Activate();

	auto reference = manager.ReferenceConfiguration();
	auto minimum = manager.MinimumConfiguration();
	REQUIRE(manager.IsWorkerAtConfiguration(0, reference));
	REQUIRE_FALSE(manager.IsWorkerAtConfiguration(0, minimum));

	auto lower = std::make_shared<QueryHardwareTargetPlan>();
	lower->generation = 1;
	QueryHardwareWorkerTarget lower_target;
	lower_target.worker_id = 0;
	lower_target.hardware = minimum;
	lower_target.assigned = true;
	lower->workers.push_back(lower_target);
	manager.PublishTargets(lower);
	REQUIRE(manager.IsWorkerAtConfiguration(0, minimum));
	auto writes_after_downshift = manager.GetSnapshot().msr_write_count;
	manager.PublishTargets(lower);
	REQUIRE(manager.GetSnapshot().msr_write_count == writes_after_downshift);

	auto higher = std::make_shared<QueryHardwareTargetPlan>(*lower);
	higher->generation = 2;
	higher->workers[0].hardware = reference;
	manager.PublishTargets(higher);
	REQUIRE(manager.IsWorkerAtConfiguration(0, reference));
	manager.Deactivate();
	auto inactive = manager.GetSnapshot();
	REQUIRE_FALSE(inactive.active);
	REQUIRE(inactive.status == "inactive_dry_run");
	REQUIRE(inactive.restoration_status == "not_required");
	REQUIRE(inactive.restoration_attempt_count == 0);
	REQUIRE(inactive.restoration_verified_count == 0);
	REQUIRE(inactive.restoration_failure_count == 0);
}
