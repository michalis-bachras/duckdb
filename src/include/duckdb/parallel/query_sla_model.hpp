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
	bool valid = false;
	string error;
	uint64_t db_query_id = 0;
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
struct QuerySLAAllocation {
	vector<idx_t> mandatory;
	vector<idx_t> optional;
	vector<idx_t> residual;
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
DownstreamSuffixEstimate QuerySLALifecycleSuffixPointMass();
QuerySLAServiceEvaluation QuerySLAEvaluateServiceRate(const QuerySLAModelInput &input,
	                                                  double aggregate_service_rate, idx_t workers);

QuerySLAModel BuildProvisionalQuerySLAModel(const QuerySLAModelInput &input);
QuerySLAModel BuildQuerySLAModel(const QuerySLAModelInput &input);
QuerySLAAllocation AllocateQuerySLAWorkers(const vector<QuerySLAModel> &models, idx_t worker_budget,
	                                       bool work_conserving);

} // namespace duckdb
