#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/query_hardware_manager.hpp"
#include "duckdb/parallel/query_sla_model.hpp"
#include "duckdb/parallel/query_sla_energy_model.hpp"
#include "duckdb/parallel/query_sla_energy_planner.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>

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
	result.profile_version = 1;
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
	result.logical_allocation =
	    AllocateQuerySLAWorkers({result.queries[0].reference_model}, worker_budget, false);
	result.topology = std::move(topology);
	result.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	result.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	result.worker_budget = worker_budget;
	result.epoch_ns = 1000000000ULL;
	return result;
}

static void SetLogicalAllocation(QuerySLAEnergyPlannerInput &input) {
	vector<QuerySLAModel> models;
	models.reserve(input.queries.size());
	for (const auto &query : input.queries) {
		models.push_back(query.reference_model);
	}
	input.logical_allocation = AllocateQuerySLAWorkers(models, input.worker_budget, false);
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

TEST_CASE("SLA model rejects a missing downstream suffix", "[api][sla][model]") {
	auto input = BaseModelInput();
	input.suffix = DownstreamSuffixEstimate();
	auto model = BuildQuerySLAModel(input);
	REQUIRE_FALSE(model.valid);
	REQUIRE(model.error == "active data pipeline has no trained downstream-suffix histogram");
	REQUIRE(model.mandatory_gain.empty());
	REQUIRE(model.optional_gain.empty());
}

TEST_CASE("SLA allocator ignores malformed empty gain vectors", "[api][sla][model]") {
	QuerySLAModel malformed;
	malformed.valid = true;
	malformed.demand_cap = 1;
	auto allocation = AllocateQuerySLAWorkers({malformed}, 1, false);
	REQUIRE(allocation.tokens.empty());
	REQUIRE(allocation.mandatory == vector<idx_t> {0});
	REQUIRE(allocation.optional == vector<idx_t> {0});
	REQUIRE(allocation.unassigned_workers == 1);
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
	REQUIRE(non_conserving.tokens.size() == 5);
	REQUIRE(non_conserving.tokens[0].query_index == 0);
	REQUIRE(non_conserving.tokens[0].worker_ordinal == 0);
	REQUIRE(non_conserving.tokens[0].mandatory);
	REQUIRE(non_conserving.tokens[1].query_index == 1);
	REQUIRE(non_conserving.tokens[1].worker_ordinal == 0);
	REQUIRE(non_conserving.tokens[1].mandatory);
	REQUIRE(non_conserving.tokens[2].query_index == 1);
	REQUIRE(non_conserving.tokens[2].worker_ordinal == 1);
	REQUIRE(non_conserving.tokens[2].mandatory);
	REQUIRE(non_conserving.tokens[3].query_index == 0);
	REQUIRE(non_conserving.tokens[3].worker_ordinal == 1);
	REQUIRE(non_conserving.tokens[3].mandatory);
	REQUIRE(non_conserving.tokens[4].query_index == 0);
	REQUIRE(non_conserving.tokens[4].worker_ordinal == 2);
	REQUIRE(non_conserving.tokens[4].optional);
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

	input.now_ns = 3000000000ULL;
	auto late_model = BuildQuerySLAModel(input);
	REQUIRE(late_model.valid);
	REQUIRE(late_model.mandatory_cost[0] > late_model.mandatory_cost[1]);
	REQUIRE(late_model.mandatory_gain[0] > 0);
}

TEST_CASE("SLA throughput falls back conservatively to continuation history", "[api][sla][model]") {
	PipelineContinuationEstimate continuation;
	continuation.valid = true;
	continuation.level = ContinuationEstimateLevel::GLOBAL_RAW;
	continuation.kind = ContinuationEstimateKind::RAW_LATENCY_NS;
	continuation.sample_count = 17;
	continuation.mean = 50000000;
	continuation.p90 = 100000000;

	auto raw = DerivePipelineThroughputFromContinuation(continuation, 20);
	REQUIRE(raw.valid);
	REQUIRE(raw.level == PipelineThroughputEstimateLevel::CONTINUATION_FALLBACK);
	REQUIRE(raw.sample_count == 17);
	REQUIRE(raw.mean_work_units_per_s == Approx(400));
	REQUIRE(raw.ewma_work_units_per_s == Approx(200));

	auto raw_input = BaseModelInput();
	raw_input.metadata.deadline_ns = raw_input.now_ns;
	raw_input.epoch_ns = 50000000;
	raw_input.work.total_chunks_equiv = 20;
	raw_input.work.remaining_chunks_equiv = 20;
	raw_input.work.continuation_estimate = continuation;
	raw_input.work.selected_throughput_valid = raw.valid;
	raw_input.work.selected_single_worker_chunks_per_s = raw.ewma_work_units_per_s;
	raw_input.work.historical_throughput_estimate = raw;
	auto raw_model = BuildQuerySLAModel(raw_input);
	REQUIRE(raw_model.valid);
	REQUIRE(raw_model.mandatory_cost[0] > raw_model.mandatory_cost[1]);
	REQUIRE(raw_model.mandatory_gain[0] > 0);
	auto raw_allocation = AllocateQuerySLAWorkers({raw_model}, 1, false);
	REQUIRE(raw_allocation.mandatory == vector<idx_t> {1});
	REQUIRE(raw_allocation.unassigned_workers == 0);

	continuation.kind = ContinuationEstimateKind::NS_PER_WORK_UNIT;
	continuation.mean = 2000000;
	continuation.p90 = 4000000;
	auto normalized = DerivePipelineThroughputFromContinuation(continuation, 20);
	REQUIRE(normalized.valid);
	REQUIRE(normalized.mean_work_units_per_s == Approx(500));
	REQUIRE(normalized.ewma_work_units_per_s == Approx(250));

	auto input = BaseModelInput();
	input.work.selected_throughput_valid = normalized.valid;
	input.work.selected_single_worker_chunks_per_s = normalized.ewma_work_units_per_s;
	input.work.historical_throughput_estimate = normalized;
	auto model = BuildQuerySLAModel(input);
	REQUIRE(model.valid);
	REQUIRE(model.throughput == Approx(250));
	REQUIRE(model.service_rates == vector<double> {0, 250, 500, 750, 1000});

	REQUIRE_FALSE(DerivePipelineThroughputFromContinuation(continuation, 0).valid);
	continuation.valid = false;
	REQUIRE_FALSE(DerivePipelineThroughputFromContinuation(continuation, 20).valid);
}

TEST_CASE("SLA energy lifecycle optional workers preserve the pure SLA transition without throughput",
	      "[api][sla][energy_model]") {
	for (auto event_kind : {QueryActivationEventKind::INITIALIZE, QueryActivationEventKind::PREPARE_FINISH,
	                        QueryActivationEventKind::FINISH, QueryActivationEventKind::COMPLETE}) {
		auto sla_input = BaseModelInput();
		sla_input.event_kind = event_kind;
		sla_input.work.preferred_parallelism = 8;
		sla_input.work.selected_throughput_valid = false;
		sla_input.work.selected_single_worker_chunks_per_s = 0;
		sla_input.work.remaining_chunks_equiv = 0;
		auto reference_model = BuildQuerySLAModel(sla_input);
		REQUIRE(reference_model.valid);
		REQUIRE(reference_model.demand_cap == 1);
		REQUIRE(reference_model.service_rates == vector<double> {0, 0});
		REQUIRE(reference_model.mandatory_gain[0] == Approx(0));
		REQUIRE(reference_model.optional_gain[0] > 0);

		QuerySLAEnergyModelInput input;
		input.sla_input = sla_input;
		input.reference_model = reference_model;
		input.reference_hardware = TestHardware(3000000, 2800000);
		auto selected = SelectQuerySLAOptionalAddition(
		    input, 0, reference_model.optional_risk[0], 0, input.reference_hardware,
		    {TestHardware(1200000, 1200000), input.reference_hardware}, QuerySLAEnergyOptionalStage::O3);
		REQUIRE(selected.valid);
		REQUIRE(selected.optional);
		REQUIRE(selected.hardware == input.reference_hardware);
		REQUIRE(selected.safe_throughput == Approx(0));
		REQUIRE(selected.normalized_edp == Approx(0));
		REQUIRE(selected.optional_gain == Approx(reference_model.optional_gain[0]));
		REQUIRE(selected.optional_fragility_before == Approx(reference_model.optional_risk[0]));
		REQUIRE(selected.optional_fragility_target == Approx(reference_model.optional_risk[1]));
		REQUIRE(selected.optional_fragility_after == Approx(reference_model.optional_risk[1]));
	}
}

TEST_CASE("SLA energy zero-work pipelines use the worker-presence model",
	      "[api][sla][energy_model][energy_planner]") {
	auto sla_input = BaseModelInput();
	sla_input.work.preferred_parallelism = 1;
	sla_input.work.remaining_chunks_equiv = 0;
	sla_input.work.selected_throughput_valid = false;
	sla_input.work.selected_single_worker_chunks_per_s = 0;
	auto reference_model = BuildQuerySLAModel(sla_input);
	REQUIRE(reference_model.valid);
	REQUIRE(reference_model.demand_cap == 1);
	REQUIRE(reference_model.service_rates == vector<double> {0, 0});
	REQUIRE(reference_model.mandatory_gain[0] == Approx(0));
	REQUIRE(reference_model.optional_gain[0] > 0);

	QuerySLAEnergyModelInput query;
	query.sla_input = sla_input;
	query.reference_model = reference_model;
	query.reference_hardware = TestHardware(3000000, 2800000);
	auto selected = SelectQuerySLAOptionalAddition(
	    query, 0, reference_model.optional_risk[0], 0, query.reference_hardware,
	    {query.reference_hardware}, QuerySLAEnergyOptionalStage::O3);
	REQUIRE(selected.valid);
	REQUIRE(selected.optional);
	REQUIRE(selected.hardware == query.reference_hardware);
	REQUIRE(selected.safe_throughput == Approx(0));
	REQUIRE(selected.optional_gain == Approx(reference_model.optional_gain[0]));

	auto input = PlannerInput(std::move(query), 1, {TestWorker(0, 0, 0, 0)});
	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 1);
	REQUIRE(plan.queries[0].mandatory_workers == 0);
	REQUIRE(plan.queries[0].optional_workers == 1);
	REQUIRE(plan.workers[0].optional);
	REQUIRE(plan.workers[0].preferred_safe_throughput == Approx(0));
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

	auto allocation = AllocateQuerySLAMandatoryWorkers({input}, {2});
	REQUIRE(allocation.valid);
	REQUIRE(allocation.queries.size() == 1);
	REQUIRE(allocation.queries[0].mandatory_workers == 2);
	REQUIRE(allocation.queries[0].workers.size() == 2);
	REQUIRE(allocation.queries[0].workers[0].hardware == efficient.hardware);
	REQUIRE(allocation.queries[0].predicted_sla_cost == Approx(input.reference_model.mandatory_cost[2]));

	input.hardware_profiles[1].mature = false;
	auto cold = AllocateQuerySLAMandatoryWorkers({input}, {2});
	REQUIRE(cold.valid);
	REQUIRE(cold.queries[0].mandatory_workers == 2);
	REQUIRE(cold.queries[0].workers[0].hardware == input.reference_hardware);
}

TEST_CASE("SLA energy mandatory counts preserve the pure SLA reference model",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(90);
	input.hardware_profiles[0] = TestHardwareProfile(3000000, 2800000, 20, 20);
	auto reference = AllocateQuerySLAWorkers({input.reference_model}, 4, false);
	auto allocation = AllocateQuerySLAMandatoryWorkers({input}, reference.mandatory);

	REQUIRE(reference.mandatory == vector<idx_t> {2});
	REQUIRE(allocation.valid);
	REQUIRE(allocation.queries[0].mandatory_workers == 2);
	REQUIRE(allocation.queries[0].mandatory_reference_safe_throughput == Approx(10));
}

TEST_CASE("SLA energy mandatory hardware minimizes normalized EDP after SLA filters",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(91);
	input.hardware_profiles.clear();
	input.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	input.hardware_profiles.push_back(TestHardwareProfile(2200000, 1800000, 12, 8));
	input.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 15, 11));
	auto reference_model = BuildQuerySLAEnergyReferenceModel(input);

	auto selected = SelectQuerySLAMandatoryAddition(input, reference_model, 0, 0);
	REQUIRE(selected.valid);
	REQUIRE(selected.hardware == TestHardware(1200000, 1200000));
	REQUIRE(selected.normalized_edp == Approx(11.0 / (15.0 * 15.0)));
	REQUIRE(12.0 / 8.0 > 15.0 / 11.0);
}

TEST_CASE("SLA energy mandatory hardware rejects a fragility regression at equal SLA cost",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(92, 4000000000ULL);
	input.hardware_profiles.clear();
	input.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	input.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 8, 2));
	auto reference_model = BuildQuerySLAEnergyReferenceModel(input);
	auto reference = QuerySLAEvaluateServiceRate(input.sla_input, 10, 1);
	auto slower = QuerySLAEvaluateServiceRate(input.sla_input, 8, 1);

	REQUIRE(reference.mandatory_cost == Approx(0));
	REQUIRE(slower.mandatory_cost == Approx(0));
	REQUIRE(slower.optional_risk > reference.optional_risk);
	auto selected = SelectQuerySLAMandatoryAddition(input, reference_model, 0, 0);
	REQUIRE(selected.valid);
	REQUIRE(selected.hardware == input.reference_hardware);
}

TEST_CASE("SLA energy optional hardware preserves fastest-reference fragility before minimizing EDP",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(93, 2050000000ULL);
	input.hardware_profiles.clear();
	auto reference = TestHardwareProfile(3000000, 2800000, 10, 10);
	auto slower = TestHardwareProfile(1200000, 1200000, 9, 1);
	input.hardware_profiles = {reference, slower};
	input.reference_model = BuildQuerySLAEnergyReferenceModel(input);
	auto current = QuerySLAEvaluateServiceRate(input.sla_input, 10, 1);
	REQUIRE(current.valid);

	auto selected = SelectQuerySLAOptionalAddition(
	    input, 10, current.optional_risk, 1, input.reference_hardware,
	    {slower.hardware, reference.hardware}, QuerySLAEnergyOptionalStage::O3);
	REQUIRE(selected.valid);
	REQUIRE(selected.hardware == reference.hardware);
	REQUIRE(selected.optional_gain > 0);
	REQUIRE(selected.optional_fragility_after == Approx(selected.optional_fragility_target));
}

TEST_CASE("SLA energy optional hardware selects minimum EDP after the fragility filter",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(94, 2050000000ULL);
	input.hardware_profiles.clear();
	auto reference = TestHardwareProfile(3000000, 2800000, 10, 10);
	auto efficient = TestHardwareProfile(1200000, 1200000, 10, 2);
	input.hardware_profiles = {reference, efficient};
	input.reference_model = BuildQuerySLAEnergyReferenceModel(input);
	auto current = QuerySLAEvaluateServiceRate(input.sla_input, 10, 1);
	REQUIRE(current.valid);

	auto selected = SelectQuerySLAOptionalAddition(
	    input, 10, current.optional_risk, 1, input.reference_hardware,
	    {reference.hardware, efficient.hardware}, QuerySLAEnergyOptionalStage::O3);
	REQUIRE(selected.valid);
	REQUIRE(selected.hardware == efficient.hardware);
	REQUIRE(selected.normalized_edp == Approx(2.0 / 100.0));
	REQUIRE(selected.optional_fragility_after == Approx(selected.optional_fragility_target));
}

TEST_CASE("SLA energy optional eligibility uses the accepted current service",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(95, 3500000000ULL);
	input.hardware_profiles.clear();
	auto reference = TestHardwareProfile(3000000, 2800000, 10, 10);
	auto faster = TestHardwareProfile(1200000, 1200000, 20, 2);
	input.hardware_profiles = {reference, faster};
	auto pure_gain = input.reference_model.optional_gain[1];
	auto faster_current = QuerySLAEvaluateServiceRate(input.sla_input, 20, 1);
	REQUIRE(faster_current.valid);
	REQUIRE(pure_gain > 0);

	auto selected = SelectQuerySLAOptionalAddition(
	    input, 20, faster_current.optional_risk, 1, input.reference_hardware,
	    {reference.hardware, faster.hardware}, QuerySLAEnergyOptionalStage::O3);
	REQUIRE(selected.valid);
	auto current_reference = QuerySLAEvaluateServiceRate(input.sla_input, 30, 2);
	REQUIRE(current_reference.valid);
	REQUIRE(selected.optional_gain == Approx(faster_current.optional_risk - current_reference.optional_risk));
	REQUIRE(selected.optional_gain <= pure_gain);
	REQUIRE(selected.optional_fragility_before == Approx(faster_current.optional_risk));
	REQUIRE(selected.optional_fragility_target == Approx(current_reference.optional_risk));
}

TEST_CASE("SLA energy fixed optional fallback permits an already-satisfied fragility target",
	      "[api][sla][energy_model]") {
	auto input = PlannerQuery(96, 3500000000ULL);
	input.hardware_profiles.clear();
	auto reference = TestHardwareProfile(3000000, 2800000, 10, 10);
	input.hardware_profiles = {reference};
	auto target = QuerySLAEvaluateServiceRate(input.sla_input, 20, 2);
	REQUIRE(target.valid);

	auto ordinary = SelectQuerySLAOptionalAddition(
	    input, 10, target.optional_risk, 1, input.reference_hardware, {input.reference_hardware},
	    QuerySLAEnergyOptionalStage::O3);
	REQUIRE_FALSE(ordinary.valid);

	auto fallback = SelectQuerySLAOptionalAddition(
	    input, 10, target.optional_risk, 1, input.reference_hardware, {input.reference_hardware},
	    QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK);
	REQUIRE(fallback.valid);
	REQUIRE(fallback.optional);
	REQUIRE(fallback.hardware == input.reference_hardware);
	REQUIRE(fallback.worker_ordinal == 1);
	REQUIRE(fallback.optional_gain == Approx(0));
	REQUIRE(fallback.optional_fragility_before == Approx(fallback.optional_fragility_after));
}

TEST_CASE("SLA energy mandatory allocation preserves internal event service curves",
	      "[api][sla][energy_model]") {
	auto sla_input = BaseModelInput();
	sla_input.event_kind = QueryActivationEventKind::INTERNAL;
	sla_input.service_rates = {0, 5, 9, 12, 14};
	sla_input.work.selected_throughput_valid = false;
	sla_input.work.selected_single_worker_chunks_per_s = 0;
	QuerySLAEnergyModelInput input;
	input.sla_input = sla_input;
	input.reference_model = BuildQuerySLAModel(sla_input);
	input.reference_hardware = TestHardware(3000000, 2800000);

	auto reference = BuildQuerySLAEnergyReferenceModel(input);
	REQUIRE(reference.valid);
	REQUIRE(reference.service_rates == sla_input.service_rates);
	auto allocation = AllocateQuerySLAMandatoryWorkers({input}, {4});
	REQUIRE(allocation.valid);
	REQUIRE(allocation.queries[0].mandatory_workers == 4);
	REQUIRE(allocation.queries[0].aggregate_service_rate == Approx(14));
	REQUIRE(allocation.queries[0].mandatory_reference_safe_throughput == Approx(3.5));
	for (const auto &worker : allocation.queries[0].workers) {
		REQUIRE(worker.hardware == input.reference_hardware);
	}
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

TEST_CASE("SLA energy planner preserves the canonical mandatory aggregate through placement",
	      "[api][sla][energy_planner]") {
	auto query = PlannerQuery(199, 2500000000ULL, 15);
	query.sla_input.work.selected_single_worker_chunks_per_s = 3849.1070059754002;
	query.reference_model = BuildQuerySLAModel(query.sla_input);
	query.hardware_profiles.clear();

	QuerySLAEnergyPlannerInput input;
	input.queries = {query};
	input.logical_allocation.mandatory = {15};
	input.logical_allocation.optional = {0};
	input.logical_allocation.residual = {0};
	input.worker_budget = 15;
	input.epoch_ns = 1000000000ULL;
	for (idx_t worker_id = 0; worker_id < 15; worker_id++) {
		input.topology.push_back(TestWorker(worker_id, NumericCast<int>(worker_id), 0,
		                                   NumericCast<int>(worker_id)));
	}

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.queries.size() == 1);
	REQUIRE(plan.queries[0].mandatory_workers == 15);
	REQUIRE(plan.queries[0].aggregate_service_rate == query.reference_model.service_rates[15]);
	REQUIRE(plan.queries[0].mandatory_preferred_service_rate == query.reference_model.service_rates[15]);
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

TEST_CASE("SLA energy planner repairs a domain-coupled throughput regression",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 1, 0, 1)};
	auto query_a = PlannerQuery(201);
	query_a.hardware_profiles.clear();
	query_a.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_a.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 10, 2));
	query_a.hardware_profiles.push_back(TestHardwareProfile(1200000, 2800000, 5, 2));
	auto query_b = PlannerQuery(202);
	query_b.hardware_profiles.clear();
	query_b.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_b.hardware_profiles.push_back(TestHardwareProfile(2200000, 2800000, 10, 4));

	QuerySLAEnergyPlannerInput input;
	input.queries = {query_a, query_b};
	input.topology = topology;
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 2;
	input.epoch_ns = 1000000000ULL;
	SetLogicalAllocation(input);
	auto plan = PlanQuerySLAEnergyEpoch(input);

	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 2);
	REQUIRE(plan.diagnostics.worker_frequency_adjustments == 1);
	for (const auto &worker : plan.workers) {
		auto preferred = worker.query_index == 0 ? TestHardware(1200000, 1200000)
		                                         : TestHardware(2200000, 2800000);
		auto normal = worker.query_index == 0 ? TestHardware(3000000, 2800000)
		                                      : TestHardware(2200000, 2800000);
		REQUIRE(worker.preferred_hardware == preferred);
		REQUIRE(worker.normal_hardware == normal);
		REQUIRE(worker.execution_hardware == worker.normal_hardware);
		REQUIRE(worker.normal_safe_throughput == Approx(10));
	}
	REQUIRE(plan.queries[0].workers.size() == 1);
	REQUIRE(plan.queries[0].workers[0].hardware == TestHardware(3000000, 2800000));
	REQUIRE(plan.queries[1].workers.size() == 1);
	REQUIRE(plan.queries[1].workers[0].hardware == TestHardware(2200000, 2800000));
	REQUIRE(plan.queries[0].aggregate_service_rate == Approx(10));
	REQUIRE(plan.queries[1].aggregate_service_rate == Approx(10));
}

TEST_CASE("SLA energy planner selects a shared core pair by SLA then aggregate EDP",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0)};
	auto query_a = PlannerQuery(203, 2500000000ULL, 1);
	query_a.hardware_profiles.clear();
	query_a.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_a.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 10, 1));
	query_a.hardware_profiles.push_back(TestHardwareProfile(2200000, 1800000, 10, 4));
	auto query_b = PlannerQuery(204, 2500000000ULL, 1);
	query_b.hardware_profiles.clear();
	query_b.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
	query_b.hardware_profiles.push_back(TestHardwareProfile(2200000, 1800000, 10, 5));

	QuerySLAEnergyPlannerInput input;
	input.queries = {query_a, query_b};
	input.topology = topology;
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 2;
	input.epoch_ns = 1000000000ULL;
	SetLogicalAllocation(input);

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 2);
	REQUIRE(plan.diagnostics.worker_frequency_adjustments == 1);
	for (const auto &worker : plan.workers) {
		REQUIRE(worker.normal_hardware == TestHardware(2200000, 1800000));
		REQUIRE(worker.execution_hardware == worker.normal_hardware);
		REQUIRE(worker.normal_safe_throughput == Approx(10));
		REQUIRE(worker.normal_normalized_edp > 0);
	}
	REQUIRE(plan.queries[0].workers[0].hardware == TestHardware(2200000, 1800000));
	REQUIRE(plan.queries[1].workers[0].hardware == TestHardware(2200000, 1800000));
	REQUIRE(plan.queries[0].predicted_sla_cost ==
	        Approx(input.queries[0].reference_model.mandatory_cost[1]));
	REQUIRE(plan.queries[1].predicted_sla_cost ==
	        Approx(input.queries[1].reference_model.mandatory_cost[1]));
}

TEST_CASE("SLA energy planner densely places frequency-sorted mandatory tokens", "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {
	    TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0), TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1),
	    TestWorker(4, 16, 1, 0), TestWorker(5, 24, 1, 0), TestWorker(6, 17, 1, 1), TestWorker(7, 25, 1, 1)};
	QuerySLAEnergyPlannerInput input;
	for (idx_t i = 0; i < 6; i++) {
		auto query = PlannerQuery(220 + i, 2500000000ULL, 1);
		query.hardware_profiles.clear();
		query.hardware_profiles.push_back(TestHardwareProfile(3000000, 2800000, 10, 10));
		if (i >= 2) {
			query.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 10, 1));
			query.hardware_profiles.push_back(TestHardwareProfile(1200000, 2800000, 10, 2));
		}
		input.queries.push_back(std::move(query));
	}
	input.topology = topology;
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 6;
	input.epoch_ns = 1000000000ULL;
	SetLogicalAllocation(input);

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 6);
	REQUIRE(plan.diagnostics.worker_frequency_adjustments == 2);
	for (const auto &worker : plan.workers) {
		REQUIRE(worker.worker_id == worker.query_index);
		if (worker.query_index < 2) {
			REQUIRE(worker.preferred_hardware == TestHardware(3000000, 2800000));
			REQUIRE(worker.normal_hardware == worker.preferred_hardware);
		} else if (worker.query_index < 4) {
			REQUIRE(worker.preferred_hardware == TestHardware(1200000, 1200000));
			REQUIRE(worker.normal_hardware == TestHardware(1200000, 2800000));
		} else {
			REQUIRE(worker.preferred_hardware == TestHardware(1200000, 1200000));
			REQUIRE(worker.normal_hardware == worker.preferred_hardware);
		}
	}
}

TEST_CASE("SLA energy planner precomputes EDP-qualified dynamic mandatory additions",
	      "[api][sla][energy_planner]") {
	QuerySLAEnergyPlannerInput input;
	input.queries = {PlannerQuery(211), PlannerQuery(212)};
	input.topology = {TestWorker(0, 0, 0, 0)};
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 1;
	input.epoch_ns = 1000000000ULL;
	SetLogicalAllocation(input);

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.workers.size() == 1);
	auto dynamic_count = plan.queries[0].dynamic_mandatory_workers.size() +
	                     plan.queries[1].dynamic_mandatory_workers.size();
	REQUIRE(dynamic_count > 0);
	auto dynamic_optional_count = plan.queries[0].dynamic_optional_workers.size() +
	                              plan.queries[1].dynamic_optional_workers.size();
	REQUIRE(dynamic_optional_count > 0);
	for (idx_t query_index = 0; query_index < plan.queries.size(); query_index++) {
		const auto &query = plan.queries[query_index];
		auto reference_model = BuildQuerySLAEnergyReferenceModel(input.queries[query_index]);
		auto service_rate = query.aggregate_service_rate;
		auto worker_count = query.workers.size();
		for (const auto &worker : query.dynamic_mandatory_workers) {
			auto expected =
			    SelectQuerySLAMandatoryAddition(input.queries[query_index], reference_model, service_rate, worker_count);
			REQUIRE(worker.valid);
			REQUIRE(worker.mandatory);
			REQUIRE(worker.mandatory_gain > 0);
			REQUIRE(worker.hardware.IsValid());
			REQUIRE(worker.safe_throughput > 0);
			REQUIRE(worker.normalized_edp > 0);
			REQUIRE(worker.worker_ordinal == worker_count);
			REQUIRE(expected.valid);
			REQUIRE(worker.hardware == expected.hardware);
			REQUIRE(worker.safe_throughput == Approx(expected.safe_throughput));
			REQUIRE(worker.mandatory_gain == Approx(expected.mandatory_gain));
			REQUIRE(worker.profile_version == expected.profile_version);
			service_rate += worker.safe_throughput;
			worker_count++;
		}
		for (const auto &worker : query.dynamic_optional_workers) {
			REQUIRE(worker.valid);
			REQUIRE(worker.optional);
			REQUIRE(worker.optional_stage == QuerySLAEnergyOptionalStage::DYNAMIC);
			REQUIRE(worker.optional_gain > 0);
			REQUIRE(worker.optional_fragility_after <=
			        worker.optional_fragility_target +
			            1e-9 * MaxValue<double>(1.0, std::abs(worker.optional_fragility_target)));
			REQUIRE(worker.worker_ordinal == worker_count);
			worker_count++;
		}
	}
}

TEST_CASE("SLA energy planner applies O1 and O2 without exceeding demand", "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto query = PlannerQuery(102, 3500000000ULL);
	query.hardware_profiles.push_back(TestHardwareProfile(3000000, 1800000, 10, 8));
	auto plan = PlanQuerySLAEnergyEpoch(PlannerInput(std::move(query), 4, topology));
	REQUIRE(plan.valid);
	REQUIRE(plan.queries[0].mandatory_workers == 1);
	REQUIRE(plan.diagnostics.o1_workers == 1);
	REQUIRE(plan.diagnostics.o2_workers == 2);
	REQUIRE(plan.workers.size() == 4);
	REQUIRE(plan.workers.size() <= plan.queries[0].workers.size());
	REQUIRE(plan.workers.size() <= 4);
	for (const auto &worker : plan.workers) {
		if (!worker.optional) {
			continue;
		}
		REQUIRE(worker.optional_stage != QuerySLAEnergyOptionalStage::NONE);
		REQUIRE(worker.optional_gain > 0);
		REQUIRE(worker.optional_fragility_after <=
		        worker.optional_fragility_target +
		            1e-9 * MaxValue<double>(1.0, std::abs(worker.optional_fragility_target)));
	}
}

TEST_CASE("SLA energy planner preserves fixed optional tokens with reference fallback",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto input = PlannerInput(PlannerQuery(107, 3500000000ULL), 4, topology);
	input.core_frequency_levels_khz.clear();
	input.uncore_frequency_levels_khz.clear();
	auto logical = input.logical_allocation;

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.queries[0].mandatory_workers == logical.mandatory[0]);
	REQUIRE(plan.queries[0].optional_workers == logical.optional[0]);
	REQUIRE(plan.diagnostics.reference_fallback_workers > 0);
	REQUIRE(plan.diagnostics.o1_workers + plan.diagnostics.reference_fallback_workers == logical.optional[0]);
	REQUIRE(plan.workers.size() == logical.mandatory[0] + logical.optional[0]);
	for (const auto &worker : plan.workers) {
		if (worker.optional_stage == QuerySLAEnergyOptionalStage::REFERENCE_FALLBACK) {
			REQUIRE(worker.preferred_hardware == input.queries[0].reference_hardware);
		}
	}
}

TEST_CASE("SLA energy planner preserves fixed optional counts after fragility reaches zero",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto query = PlannerQuery(108, 3500000000ULL);
	query.hardware_profiles.push_back(TestHardwareProfile(1200000, 1200000, 100, 1));
	auto input = PlannerInput(std::move(query), 4, topology);
	auto logical = input.logical_allocation;
	REQUIRE(logical.optional[0] > 1);

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.queries[0].mandatory_workers == logical.mandatory[0]);
	REQUIRE(plan.queries[0].optional_workers == logical.optional[0]);
	REQUIRE(plan.diagnostics.reference_fallback_workers > 0);
	REQUIRE(plan.workers.size() == logical.mandatory[0] + logical.optional[0]);
}

TEST_CASE("SLA energy planner places a lifecycle optional worker at reference hardware",
	      "[api][sla][energy_planner]") {
	auto sla_input = BaseModelInput();
	sla_input.event_kind = QueryActivationEventKind::INITIALIZE;
	sla_input.work.preferred_parallelism = 8;
	sla_input.work.selected_throughput_valid = false;
	sla_input.work.selected_single_worker_chunks_per_s = 0;
	sla_input.work.remaining_chunks_equiv = 0;
	QuerySLAEnergyModelInput query;
	query.sla_input = sla_input;
	query.reference_model = BuildQuerySLAModel(sla_input);
	query.reference_hardware = TestHardware(3000000, 2800000);

	auto plan = PlanQuerySLAEnergyEpoch(PlannerInput(std::move(query), 1, {TestWorker(0, 0, 0, 0)}));
	REQUIRE(plan.valid);
	REQUIRE(plan.diagnostics.mandatory_workers == 0);
	REQUIRE(plan.diagnostics.o3_workers == 1);
	REQUIRE(plan.workers.size() == 1);
	REQUIRE(plan.queries[0].mandatory_workers == 0);
	REQUIRE(plan.queries[0].optional_workers == 1);
	REQUIRE(plan.workers[0].optional);
	REQUIRE(plan.workers[0].optional_stage == QuerySLAEnergyOptionalStage::O3);
	REQUIRE(plan.workers[0].preferred_hardware == TestHardware(3000000, 2800000));
	REQUIRE(plan.workers[0].preferred_safe_throughput == Approx(0));
	REQUIRE(plan.workers[0].preferred_normalized_edp == Approx(0));
}

TEST_CASE("SLA energy planner ranks optional tokens by fragility and permits one odd O2 token",
	      "[api][sla][energy_planner]") {
	vector<QueryHardwareWorkerTopology> topology = {TestWorker(0, 0, 0, 0), TestWorker(1, 8, 0, 0),
	                                                TestWorker(2, 1, 0, 1), TestWorker(3, 9, 0, 1)};
	auto high_fragility = PlannerQuery(105, 3500000000ULL, 2);
	auto low_fragility = PlannerQuery(106, 3500000000ULL, 2);
	high_fragility.sla_input.metadata.sla_penalty_per_s = 10;
	low_fragility.sla_input.metadata.sla_penalty_per_s = 1;
	high_fragility.reference_model = BuildQuerySLAModel(high_fragility.sla_input);
	low_fragility.reference_model = BuildQuerySLAModel(low_fragility.sla_input);
	high_fragility.hardware_profiles.push_back(TestHardwareProfile(3000000, 1800000, 10, 8));
	low_fragility.hardware_profiles.push_back(TestHardwareProfile(3000000, 1800000, 10, 8));

	QuerySLAEnergyPlannerInput input;
	input.queries = {high_fragility, low_fragility};
	input.topology = topology;
	input.core_frequency_levels_khz = {1200000, 2200000, 3000000};
	input.uncore_frequency_levels_khz = {1200000, 1800000, 2800000};
	input.worker_budget = 3;
	input.epoch_ns = 1000000000ULL;
	SetLogicalAllocation(input);

	auto plan = PlanQuerySLAEnergyEpoch(input);
	REQUIRE(plan.valid);
	REQUIRE(plan.diagnostics.mandatory_workers == 2);
	REQUIRE(plan.diagnostics.o1_workers == 0);
	REQUIRE(plan.diagnostics.o2_workers == 1);
	REQUIRE(plan.diagnostics.o3_workers == 0);
	REQUIRE(plan.workers.size() == 3);
	REQUIRE(plan.queries[0].optional_workers == 1);
	REQUIRE(plan.queries[1].optional_workers == 0);
	auto optional = std::find_if(plan.queries[0].workers.begin(), plan.queries[0].workers.end(),
	                             [](const QuerySLAEnergyWorkerAssignment &worker) { return worker.optional; });
	REQUIRE(optional != plan.queries[0].workers.end());
	REQUIRE(optional->optional_stage == QuerySLAEnergyOptionalStage::O2);
	REQUIRE(optional->optional_gain > 0);
	REQUIRE(optional->hardware == TestHardware(2200000, 1800000));
}

TEST_CASE("SLA energy planner densely packs O3 tokens and applies optional-only exploration",
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
	auto query = PlannerQuery(105, 1500000000ULL, 2);
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
	REQUIRE(reference.core_frequency_khz == 3500000);
	REQUIRE(reference.uncore_frequency_khz == 2800000);
	REQUIRE(manager.CoreFrequencyLevelsKHz().back() == 3500000);
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

TEST_CASE("SLA energy hardware manager derives its frequency grid from calibration",
          "[api][sla][energy_hardware]") {
	auto power_model_path = TestCreatePath("sla_energy_frequency_grid.csv");
	{
		std::ofstream output(power_model_path);
		output << "socket_id,core_freq_hz,uncore_freq_hz,smt_state,base_power_w\n";
		output << "0,1200000000,1200000000,0,10\n";
		output << "0,1200000000,2800000000,0,11\n";
		output << "0,3500000000,1200000000,0,12\n";
		output << "0,3500000000,2800000000,0,13\n";
	}

	DBConfig config;
	config.options.query_sla_energy_hardware_control_enabled = false;
	config.options.query_sla_energy_power_model_path = power_model_path;
	DuckDB db(nullptr, &config);
	QueryHardwareManager manager(*db.instance);
	manager.RegisterWorker(0, 0);
	manager.Activate();

	REQUIRE(manager.CoreFrequencyLevelsKHz() == vector<uint32_t> {1200000, 3500000});
	REQUIRE(manager.UncoreFrequencyLevelsKHz() == vector<uint32_t> {1200000, 2800000});
	REQUIRE(manager.MinimumConfiguration().core_frequency_khz == 1200000);
	REQUIRE(manager.MinimumConfiguration().uncore_frequency_khz == 1200000);
	REQUIRE(manager.ReferenceConfiguration().core_frequency_khz == 3500000);
	REQUIRE(manager.ReferenceConfiguration().uncore_frequency_khz == 2800000);

	manager.Deactivate();
	db.instance->GetFileSystem().RemoveFile(power_model_path);
}

TEST_CASE("SLA energy hardware diagnostics classify worker target transitions",
          "[api][sla][energy_hardware]") {
	DBConfig config;
	config.options.query_sla_energy_hardware_control_enabled = false;
	DuckDB db(nullptr, &config);
	QueryHardwareManager manager(*db.instance);
	manager.RegisterWorker(0, 0);
	manager.Activate();

	QueryHardwareTransitionContext first;
	first.db_query_id = 101;
	first.request_id = 7;
	first.pipeline_generation = 3;
	first.lease_generation = 11;
	first.pipeline_id = 4;
	first.mandatory = true;
	first.trace_enabled = true;

	auto minimum = manager.MinimumConfiguration();
	auto reference = manager.ReferenceConfiguration();
	manager.UpdateWorkerTarget(0, minimum, true, 1, first);

	auto handoff = first;
	handoff.lease_generation = 12;
	manager.UpdateWorkerTarget(0, minimum, true, 1, handoff);
	manager.UpdateWorkerTarget(0, reference, true, 2, handoff);
	manager.UpdateWorkerTarget(0, minimum, false, 2);

	auto snapshot = manager.GetSnapshot();
	REQUIRE(snapshot.worker_target_update_count == 4);
	REQUIRE(snapshot.worker_target_noop_count == 1);
	REQUIRE(snapshot.worker_assignment_count == 1);
	REQUIRE(snapshot.worker_release_count == 1);
	REQUIRE(snapshot.worker_retarget_count == 1);
	REQUIRE(snapshot.worker_lease_handoff_count == 1);
	REQUIRE(snapshot.core_transition_count == 3);
	REQUIRE(snapshot.uncore_transition_count == 3);
	REQUIRE(snapshot.msr_write_count == 0);
	REQUIRE(snapshot.core_msr_write_count == 0);
	REQUIRE(snapshot.uncore_msr_write_count == 0);
	REQUIRE(snapshot.transition_trace_dropped_count == 0);

	auto trace = manager.GetTransitionTrace();
	REQUIRE(trace.size() == 4);
	REQUIRE(trace[0].action == "assign");
	REQUIRE(trace[0].request_id == 7);
	REQUIRE(trace[0].pipeline_id == 4);
	REQUIRE(trace[0].core_changed);
	REQUIRE(trace[0].uncore_changed);
	REQUIRE(trace[0].msr_write_delta == 0);
	REQUIRE(trace[1].action == "lease_handoff");
	REQUIRE_FALSE(trace[1].core_changed);
	REQUIRE_FALSE(trace[1].uncore_changed);
	REQUIRE(trace[2].action == "retarget");
	REQUIRE(trace[2].core_changed);
	REQUIRE(trace[2].uncore_changed);
	REQUIRE(trace[3].action == "release");
	REQUIRE(trace[3].request_id == 7);
	REQUIRE(trace[3].core_changed);
	REQUIRE(trace[3].uncore_changed);

	manager.Deactivate();
}
