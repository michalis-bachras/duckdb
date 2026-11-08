//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/query_sla_model.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_request_metadata.hpp"
#include "duckdb/main/query_request_profile_store.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! Immutable inputs used to construct the SLA cost curves for one active query event.
struct QuerySLAModelInput {
	bool total_package = false;
	QueryRequestMetadata metadata;
	PipelineWorkSnapshot work;
	DownstreamSuffixEstimate suffix;
	QueryActivationEventKind event_kind = QueryActivationEventKind::UNREGISTERED;
	uint64_t generation = 0;
	uint64_t now_ns = 0;
	uint64_t epoch_ns = 0;
	//! Optional aggregate service rates indexed by worker count. Empty uses linear single-worker throughput.
	vector<double> service_rates;
};

struct QuerySLAModel {
	uint64_t query_start_ns = 0;
	uint64_t prior_service_ns = 0;
	idx_t previous_workers = 0;
	bool total_package = false;
	bool valid = false;
	string error;
	uint64_t db_query_id = 0;
	uint64_t deadline_ns = 0;
	uint64_t model_now_ns = 0;
	double lifecycle_tail_p90_ns = 0;
	double suffix_p90_ns = 0;
	uint64_t generation = 0;
	idx_t pipeline_id = 0;
	QueryActivationEventKind event_kind = QueryActivationEventKind::UNREGISTERED;
	idx_t demand_cap = 0;
	idx_t remaining_work_units = 0;
	double throughput = 0;
	bool throughput_is_live = false;
	vector<double> service_rates;
	vector<double> mandatory_cost;
	vector<double> optional_risk;
	vector<double> mandatory_gain;
	vector<double> optional_gain;
};

struct QuerySLAServiceEvaluation {
	bool valid = false;
	double current_pipeline_finish_ns = 0;
	double mandatory_cost = 0;
	double optional_risk = 0;
};

//! The three allocation classes remain separate so derived policies can park residual capacity.
struct QuerySLAAllocationToken {
	idx_t query_index = DConstants::INVALID_INDEX;
	idx_t worker_ordinal = DConstants::INVALID_INDEX;
	double gain = 0;
	bool mandatory = false;
	bool optional = false;
};

struct QuerySLAAllocation {
	vector<idx_t> mandatory;
	vector<idx_t> optional;
	vector<idx_t> residual;
	vector<QuerySLAAllocationToken> tokens;
	idx_t unassigned_workers = 0;
};

double QuerySLASuffixMeanNs(const DownstreamSuffixEstimate &suffix);
double QuerySLASuffixQuantileNs(const DownstreamSuffixEstimate &suffix, double quantile);
double QuerySLACurrentPipelineFinishNs(const PipelineWorkSnapshot &work, QueryActivationEventKind event_kind,
	                                   double aggregate_service_rate, bool has_worker, uint64_t now_ns,
	                                   uint64_t epoch_ns);
double QuerySLAExpectedTardiness(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns);
double QuerySLAExpectedFragility(const DownstreamSuffixEstimate &suffix, double downstream_budget_ns,
	                             double epoch_ns);
DownstreamSuffixEstimate QuerySLALifecycleSuffixPointMass(uint64_t remaining_ns = 0);
//! Short serial events need execution even when their SLA marginal gain is zero.
bool QuerySLARequiresLifecycleWorker(const QuerySLAModel &model);
//! Initialization priors support allocation, not a trained completion forecast for hardware downshifts.
bool QuerySLAHasCompletionForecast(const QuerySLAModel &model);
QuerySLAServiceEvaluation QuerySLAEvaluateServiceRate(const QuerySLAModelInput &input,
	                                                  double aggregate_service_rate, idx_t workers);

QuerySLAModel BuildProvisionalQuerySLAModel(const QuerySLAModelInput &input);
QuerySLAModel BuildQuerySLAModel(const QuerySLAModelInput &input);
enum class QuerySLABackfillPriority : uint8_t { EDF, TRIAGE, SRPT, PSJF, STABLE_SRPT, MOORE, BOUNDED_DP };
QuerySLAAllocation AllocatePriorityBackfillWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                                  QuerySLABackfillPriority priority, uint64_t epoch_ns = 100000000);
//! Retain the primary query until its caller clears the id; lend only spare pipeline capacity.
QuerySLAAllocation AllocateCommittedSJFWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                             uint64_t &committed_query_id);
//! Experimental execution EDF/backfill; optional infeasibility triage is not a hard schedulability test.
QuerySLAAllocation AllocateDeadlineBackfillWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
                                                 bool triage, bool shortest_remaining = false);
QuerySLAAllocation AllocateQuerySLAWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
	                                       bool work_conserving, bool ensure_lifecycle_progress = false);

} // namespace duckdb
