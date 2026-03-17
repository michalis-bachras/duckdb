//===----------------------------------------------------------------------===//
// Scheduler Evaluation Client
//
// Compares STRIDE vs DEFAULT scheduling using the methodology from
// "Self-Tuning Query Scheduling for Analytical Workloads" (SIGMOD '21).
//
// Phase 1: Sequential calibration — measure isolation times for each TPC-H
//          query under each scheduler type.
// Phase 2: Mixed workload evaluation — exponential arrivals, SF3+SF30 mix,
//          measure relative slowdown at various load factors.
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "tpch_extension.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using Clock = std::chrono::steady_clock;
using Duration = std::chrono::duration<double>;

//===----------------------------------------------------------------------===//
// Data Structures
//===----------------------------------------------------------------------===//

struct IsolationTime {
	std::string scheduler_type;
	int query_num;
	int scale_factor;
	double duration_sec; // median of N runs
};

struct QueryEvent {
	int event_id;
	double arrival_time; // seconds from experiment start
	int query_num;       // 1-22
	int scale_factor;    // 3 or 30
};

struct QueryMeasurement {
	int event_id;
	int query_num;
	int scale_factor;
	std::string scheduler_type;
	double alpha;
	int seed;
	double arrival_time;
	double start_time;       // actual start (after waiting)
	double wall_time_sec;
	double isolation_time_sec;
	double relative_slowdown; // wall_time / isolation_time
};

struct ExperimentSummary {
	std::string scheduler_type;
	double alpha;
	int seed;
	double mean_relative_slowdown;
	double p50_slowdown;
	double p95_slowdown;
	double p99_slowdown;
	double throughput_qps;
	double total_wall_sec;
	int n_queries;
};

//===----------------------------------------------------------------------===//
// Configuration
//===----------------------------------------------------------------------===//

struct Config {
	std::string db_path = "eval_scheduler.duckdb";
	std::string output_dir = "./eval_results";
	bool run_phase1 = true;
	bool run_phase2 = true;
	bool skip_dbgen = false;
	std::vector<double> alphas = {0.80, 0.85, 0.90, 0.95, 1.00};
	int queries_per_run = 200;
	int warmup_queries = 30;
	int reps = 3;
	int base_seed = 42;
	int threads = 0; // 0 = DuckDB default
	int calibration_runs = 3;
	std::vector<int> scale_factors = {3, 30};
	std::vector<std::string> schedulers = {"DEFAULT", "STRIDE"};
};

//===----------------------------------------------------------------------===//
// Utilities
//===----------------------------------------------------------------------===//

static double percentile(std::vector<double> &sorted_vals, double p) {
	if (sorted_vals.empty()) {
		return 0.0;
	}
	double idx = p * static_cast<double>(sorted_vals.size() - 1);
	size_t lo = static_cast<size_t>(std::floor(idx));
	size_t hi = static_cast<size_t>(std::ceil(idx));
	if (lo == hi) {
		return sorted_vals[lo];
	}
	double frac = idx - static_cast<double>(lo);
	return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac;
}

static std::string timestamp_str() {
	auto now = std::chrono::system_clock::now();
	auto t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%H:%M:%S", std::localtime(&t));
	return std::string(buf);
}

static void log(const std::string &msg) {
	std::cout << "[" << timestamp_str() << "] " << msg << std::endl;
}

//===----------------------------------------------------------------------===//
// Data Generation
//===----------------------------------------------------------------------===//

static void generate_data(DuckDB &db, const Config &config) {
	Connection con(db);

	for (int sf : config.scale_factors) {
		std::string schema = "sf" + std::to_string(sf);
		log("Generating TPC-H SF" + std::to_string(sf) + " data in schema '" + schema + "'...");

		// Check if data already exists
		auto check = con.Query("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '" + schema + "'");
		if (check->HasError()) {
			throw std::runtime_error("Schema check failed: " + check->GetError());
		}
		auto count = check->GetValue(0, 0).GetValue<int64_t>();
		if (count > 0) {
			// Verify the schema has tables
			auto tbl_check = con.Query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + schema + "'");
			auto tbl_count = tbl_check->GetValue(0, 0).GetValue<int64_t>();
			if (tbl_count >= 8) {
				log("  Schema '" + schema + "' already has " + std::to_string(tbl_count) + " tables, skipping.");
				continue;
			}
		}

		// Create schema if it doesn't exist
		auto create_schema = con.Query("CREATE SCHEMA IF NOT EXISTS " + schema);
		if (create_schema->HasError()) {
			throw std::runtime_error("CREATE SCHEMA failed: " + create_schema->GetError());
		}

		auto result = con.Query("CALL dbgen(sf=" + std::to_string(sf) + ", schema='" + schema + "')");
		if (result->HasError()) {
			throw std::runtime_error("dbgen failed for SF" + std::to_string(sf) + ": " + result->GetError());
		}
		log("  SF" + std::to_string(sf) + " data generated.");
	}
}

//===----------------------------------------------------------------------===//
// Phase 1: Sequential Calibration
//===----------------------------------------------------------------------===//

static std::vector<IsolationTime> run_phase1(DuckDB &db, const Config &config) {
	std::vector<IsolationTime> results;
	log("=== Phase 1: Sequential Calibration ===");

	for (const auto &sched : config.schedulers) {
		for (int sf : config.scale_factors) {
			std::string schema = "sf" + std::to_string(sf);
			log("  Calibrating: scheduler=" + sched + ", SF=" + std::to_string(sf));

			Connection con(db);
			con.Query("SET scheduler_type='" + sched + "'");
			con.Query("SET schema='" + schema + "'");

			for (int q = 1; q <= 22; q++) {
				std::string query_sql = TpchExtension::GetQuery(q);

				// Warmup run
				auto warmup = con.Query(query_sql);
				if (warmup->HasError()) {
					log("    WARNING: Q" + std::to_string(q) + " warmup error: " + warmup->GetError());
					continue;
				}

				// Timed runs
				std::vector<double> times;
				for (int r = 0; r < config.calibration_runs; r++) {
					auto t0 = Clock::now();
					auto res = con.Query(query_sql);
					auto t1 = Clock::now();

					if (res->HasError()) {
						log("    WARNING: Q" + std::to_string(q) + " run " + std::to_string(r) + " error: " + res->GetError());
						continue;
					}
					times.push_back(Duration(t1 - t0).count());
				}

				if (times.empty()) {
					continue;
				}

				std::sort(times.begin(), times.end());
				double median = times[times.size() / 2];

				IsolationTime it;
				it.scheduler_type = sched;
				it.query_num = q;
				it.scale_factor = sf;
				it.duration_sec = median;
				results.push_back(it);

				printf("    Q%02d SF%d [%s]: %.4fs (median of %zu runs)\n",
				       q, sf, sched.c_str(), median, times.size());
			}
		}
	}

	return results;
}

//===----------------------------------------------------------------------===//
// Workload Generation
//===----------------------------------------------------------------------===//

static std::vector<QueryEvent> generate_workload(double alpha, double mean_query_dur_sec,
                                                  int n_queries, unsigned seed) {
	double lambda = alpha / mean_query_dur_sec;
	std::mt19937 rng(seed);
	std::exponential_distribution<double> arrival_dist(lambda);
	std::uniform_int_distribution<int> query_dist(1, 22);
	// 75% SF3, 25% SF30
	std::discrete_distribution<int> sf_dist({3, 1}); // index 0 = SF3, index 1 = SF30

	std::vector<QueryEvent> events;
	double t = 0.0;
	for (int i = 0; i < n_queries; i++) {
		t += arrival_dist(rng);
		int sf = (sf_dist(rng) == 0) ? 3 : 30;
		int q = query_dist(rng);
		events.push_back({i, t, q, sf});
	}
	return events;
}

//===----------------------------------------------------------------------===//
// Phase 2: Mixed Workload Evaluation
//===----------------------------------------------------------------------===//

static std::vector<QueryMeasurement> run_experiment(DuckDB &db, const std::string &scheduler_type,
                                                     double alpha, int seed,
                                                     const std::vector<QueryEvent> &events,
                                                     const std::map<std::tuple<std::string, int, int>, double> &iso_times,
                                                     int warmup_count) {
	std::vector<QueryMeasurement> results;
	std::mutex results_mutex;
	std::atomic<int> completed{0};
	int total = static_cast<int>(events.size());

	auto t_start = Clock::now();
	std::vector<std::thread> workers;
	workers.reserve(events.size());

	for (const auto &event : events) {
		// Wait until arrival time
		auto target = t_start + std::chrono::duration<double>(event.arrival_time);
		std::this_thread::sleep_until(target);

		workers.emplace_back([&db, &event, &scheduler_type, &iso_times, &results, &results_mutex,
		                      alpha, seed, warmup_count, &completed, total]() {
			try {
				Connection con(db);
				con.Query("SET scheduler_type='" + scheduler_type + "'");
				con.Query("SET schema='sf" + std::to_string(event.scale_factor) + "'");

				std::string query_sql = TpchExtension::GetQuery(event.query_num);

				auto actual_start = Clock::now();
				auto res = con.Query(query_sql);
				auto t1 = Clock::now();

				double wall = Duration(t1 - actual_start).count();
				double start_offset = Duration(actual_start - Clock::now()).count(); // will be negative, recalc below

				if (res->HasError()) {
					int done = ++completed;
					if (done % 20 == 0) {
						printf("    [%d/%d] Q%02d SF%d ERROR: %s\n",
						       done, total, event.query_num, event.scale_factor, res->GetError().c_str());
					}
					return;
				}

				auto key = std::make_tuple(scheduler_type, event.query_num, event.scale_factor);
				double iso = 1.0; // fallback
				auto it = iso_times.find(key);
				if (it != iso_times.end()) {
					iso = it->second;
				}
				double slowdown = wall / iso;

				// Only record measurements after warmup
				if (event.event_id >= warmup_count) {
					std::lock_guard<std::mutex> lock(results_mutex);
					results.push_back({event.event_id - warmup_count, event.query_num, event.scale_factor,
					                   scheduler_type, alpha, seed, event.arrival_time, 0.0,
					                   wall, iso, slowdown});
				}

				int done = ++completed;
				if (done % 20 == 0 || done == total) {
					printf("    [%d/%d] completed\n", done, total);
				}
			} catch (const std::exception &e) {
				++completed;
				fprintf(stderr, "    Thread exception for Q%d SF%d: %s\n",
				        event.query_num, event.scale_factor, e.what());
			}
		});
	}

	// Join all threads
	for (auto &w : workers) {
		if (w.joinable()) {
			w.join();
		}
	}

	return results;
}

static double compute_mean_query_duration(const std::vector<IsolationTime> &iso_times,
                                           const std::string &scheduler_type,
                                           const std::vector<int> &scale_factors) {
	// Weighted mean: 75% SF3 + 25% SF30
	// First compute mean isolation time per SF for this scheduler
	std::map<int, std::vector<double>> sf_times;
	for (const auto &it : iso_times) {
		if (it.scheduler_type == scheduler_type) {
			sf_times[it.scale_factor].push_back(it.duration_sec);
		}
	}

	double mean_sf3 = 0.0, mean_sf30 = 0.0;
	if (sf_times.count(3) && !sf_times[3].empty()) {
		mean_sf3 = std::accumulate(sf_times[3].begin(), sf_times[3].end(), 0.0) / sf_times[3].size();
	}
	if (sf_times.count(30) && !sf_times[30].empty()) {
		mean_sf30 = std::accumulate(sf_times[30].begin(), sf_times[30].end(), 0.0) / sf_times[30].size();
	}

	// Weighted average matching the 75/25 sampling probability
	return 0.75 * mean_sf3 + 0.25 * mean_sf30;
}

static ExperimentSummary compute_summary(const std::vector<QueryMeasurement> &measurements,
                                          const std::string &scheduler_type, double alpha, int seed,
                                          double total_wall_sec) {
	ExperimentSummary summary;
	summary.scheduler_type = scheduler_type;
	summary.alpha = alpha;
	summary.seed = seed;
	summary.n_queries = static_cast<int>(measurements.size());
	summary.total_wall_sec = total_wall_sec;

	if (measurements.empty()) {
		summary.mean_relative_slowdown = 0.0;
		summary.p50_slowdown = 0.0;
		summary.p95_slowdown = 0.0;
		summary.p99_slowdown = 0.0;
		summary.throughput_qps = 0.0;
		return summary;
	}

	std::vector<double> slowdowns;
	slowdowns.reserve(measurements.size());
	for (const auto &m : measurements) {
		slowdowns.push_back(m.relative_slowdown);
	}

	double sum = std::accumulate(slowdowns.begin(), slowdowns.end(), 0.0);
	summary.mean_relative_slowdown = sum / slowdowns.size();
	summary.throughput_qps = static_cast<double>(measurements.size()) / total_wall_sec;

	std::sort(slowdowns.begin(), slowdowns.end());
	summary.p50_slowdown = percentile(slowdowns, 0.50);
	summary.p95_slowdown = percentile(slowdowns, 0.95);
	summary.p99_slowdown = percentile(slowdowns, 0.99);

	return summary;
}

static void run_phase2(DuckDB &db, const Config &config,
                        const std::vector<IsolationTime> &iso_times,
                        std::vector<QueryMeasurement> &all_measurements,
                        std::vector<ExperimentSummary> &all_summaries) {
	log("=== Phase 2: Mixed Workload Evaluation ===");

	// Build isolation time lookup
	std::map<std::tuple<std::string, int, int>, double> iso_map;
	for (const auto &it : iso_times) {
		iso_map[std::make_tuple(it.scheduler_type, it.query_num, it.scale_factor)] = it.duration_sec;
	}

	for (const auto &sched : config.schedulers) {
		double mean_dur = compute_mean_query_duration(iso_times, sched, config.scale_factors);
		log("  Mean query duration for " + sched + ": " + std::to_string(mean_dur) + "s");

		for (double alpha : config.alphas) {
			for (int rep = 0; rep < config.reps; rep++) {
				int seed = config.base_seed + rep;
				int total_queries = config.warmup_queries + config.queries_per_run;

				log("  Running: scheduler=" + sched + " alpha=" + std::to_string(alpha) +
				    " seed=" + std::to_string(seed) +
				    " (" + std::to_string(total_queries) + " queries, " +
				    std::to_string(config.warmup_queries) + " warmup)");

				auto events = generate_workload(alpha, mean_dur, total_queries, static_cast<unsigned>(seed));

				double lambda = alpha / mean_dur;
				printf("    lambda=%.2f queries/sec, expected inter-arrival=%.4fs\n", lambda, 1.0 / lambda);

				auto t0 = Clock::now();
				auto measurements = run_experiment(db, sched, alpha, seed, events, iso_map, config.warmup_queries);
				auto t1 = Clock::now();
				double total_wall = Duration(t1 - t0).count();

				auto summary = compute_summary(measurements, sched, alpha, seed, total_wall);
				all_summaries.push_back(summary);

				printf("    Result: mean_slowdown=%.3f p50=%.3f p95=%.3f p99=%.3f throughput=%.2f qps wall=%.1fs\n",
				       summary.mean_relative_slowdown, summary.p50_slowdown,
				       summary.p95_slowdown, summary.p99_slowdown,
				       summary.throughput_qps, summary.total_wall_sec);

				all_measurements.insert(all_measurements.end(), measurements.begin(), measurements.end());
			}
		}
	}
}

//===----------------------------------------------------------------------===//
// CSV Output
//===----------------------------------------------------------------------===//

static void write_isolation_csv(const std::vector<IsolationTime> &data, const std::string &path) {
	FILE *f = fopen(path.c_str(), "w");
	if (!f) {
		throw std::runtime_error("Cannot open " + path);
	}
	fprintf(f, "scheduler_type,query_num,scale_factor,duration_sec\n");
	for (const auto &it : data) {
		fprintf(f, "%s,%d,%d,%.6f\n", it.scheduler_type.c_str(), it.query_num, it.scale_factor, it.duration_sec);
	}
	fclose(f);
}

static void write_query_log_csv(const std::vector<QueryMeasurement> &data, const std::string &path) {
	FILE *f = fopen(path.c_str(), "w");
	if (!f) {
		throw std::runtime_error("Cannot open " + path);
	}
	fprintf(f, "scheduler_type,alpha,seed,event_id,query_num,scale_factor,"
	           "arrival_time,wall_time_sec,isolation_time_sec,relative_slowdown\n");
	for (const auto &m : data) {
		fprintf(f, "%s,%.2f,%d,%d,%d,%d,%.6f,%.6f,%.6f,%.6f\n",
		        m.scheduler_type.c_str(), m.alpha, m.seed, m.event_id, m.query_num, m.scale_factor,
		        m.arrival_time, m.wall_time_sec, m.isolation_time_sec, m.relative_slowdown);
	}
	fclose(f);
}

static void write_summary_csv(const std::vector<ExperimentSummary> &data, const std::string &path) {
	FILE *f = fopen(path.c_str(), "w");
	if (!f) {
		throw std::runtime_error("Cannot open " + path);
	}
	fprintf(f, "scheduler_type,alpha,seed,mean_relative_slowdown,p50_slowdown,"
	           "p95_slowdown,p99_slowdown,throughput_qps,total_wall_sec,n_queries\n");
	for (const auto &s : data) {
		fprintf(f, "%s,%.2f,%d,%.6f,%.6f,%.6f,%.6f,%.4f,%.2f,%d\n",
		        s.scheduler_type.c_str(), s.alpha, s.seed,
		        s.mean_relative_slowdown, s.p50_slowdown, s.p95_slowdown, s.p99_slowdown,
		        s.throughput_qps, s.total_wall_sec, s.n_queries);
	}
	fclose(f);
}

//===----------------------------------------------------------------------===//
// Comparison Report
//===----------------------------------------------------------------------===//

static void print_comparison(const std::vector<ExperimentSummary> &summaries) {
	log("=== Comparison: STRIDE vs DEFAULT ===");
	printf("\n%-10s %6s %6s | %-9s %-9s %-9s %-9s | %-9s\n",
	       "Scheduler", "Alpha", "Seed", "MeanSlowD", "P50", "P95", "P99", "QPS");
	printf("%-10s %6s %6s | %-9s %-9s %-9s %-9s | %-9s\n",
	       "----------", "------", "------", "---------", "---------", "---------", "---------", "---------");

	for (const auto &s : summaries) {
		printf("%-10s %6.2f %6d | %9.3f %9.3f %9.3f %9.3f | %9.2f\n",
		       s.scheduler_type.c_str(), s.alpha, s.seed,
		       s.mean_relative_slowdown, s.p50_slowdown, s.p95_slowdown, s.p99_slowdown,
		       s.throughput_qps);
	}

	// Aggregate by (scheduler, alpha)
	printf("\n=== Aggregated (mean across seeds) ===\n");
	printf("%-10s %6s | %-9s %-9s %-9s %-9s | %-9s\n",
	       "Scheduler", "Alpha", "MeanSlowD", "P50", "P95", "P99", "QPS");
	printf("%-10s %6s | %-9s %-9s %-9s %-9s | %-9s\n",
	       "----------", "------", "---------", "---------", "---------", "---------", "---------");

	std::map<std::pair<std::string, double>, std::vector<const ExperimentSummary *>> groups;
	for (const auto &s : summaries) {
		groups[{s.scheduler_type, s.alpha}].push_back(&s);
	}

	for (const auto &kv : groups) {
		double mean_sd = 0, mean_p50 = 0, mean_p95 = 0, mean_p99 = 0, mean_qps = 0;
		for (const auto *s : kv.second) {
			mean_sd += s->mean_relative_slowdown;
			mean_p50 += s->p50_slowdown;
			mean_p95 += s->p95_slowdown;
			mean_p99 += s->p99_slowdown;
			mean_qps += s->throughput_qps;
		}
		double n = static_cast<double>(kv.second.size());
		printf("%-10s %6.2f | %9.3f %9.3f %9.3f %9.3f | %9.2f\n",
		       kv.first.first.c_str(), kv.first.second,
		       mean_sd / n, mean_p50 / n, mean_p95 / n, mean_p99 / n, mean_qps / n);
	}
}

//===----------------------------------------------------------------------===//
// CLI Argument Parsing
//===----------------------------------------------------------------------===//

static Config parse_args(int argc, char *argv[]) {
	Config config;

	for (int i = 1; i < argc; i++) {
		std::string arg = argv[i];

		if (arg == "--phase1") {
			config.run_phase1 = true;
			config.run_phase2 = false;
		} else if (arg == "--phase2") {
			config.run_phase1 = false;
			config.run_phase2 = true;
		} else if (arg == "--all") {
			config.run_phase1 = true;
			config.run_phase2 = true;
		} else if (arg == "--skip-dbgen") {
			config.skip_dbgen = true;
		} else if (arg == "--db" && i + 1 < argc) {
			config.db_path = argv[++i];
		} else if (arg == "--output" && i + 1 < argc) {
			config.output_dir = argv[++i];
		} else if (arg == "--alpha" && i + 1 < argc) {
			config.alphas.clear();
			std::string alpha_str = argv[++i];
			std::stringstream ss(alpha_str);
			std::string token;
			while (std::getline(ss, token, ',')) {
				config.alphas.push_back(std::stod(token));
			}
		} else if (arg == "--queries" && i + 1 < argc) {
			config.queries_per_run = std::stoi(argv[++i]);
		} else if (arg == "--warmup" && i + 1 < argc) {
			config.warmup_queries = std::stoi(argv[++i]);
		} else if (arg == "--reps" && i + 1 < argc) {
			config.reps = std::stoi(argv[++i]);
		} else if (arg == "--threads" && i + 1 < argc) {
			config.threads = std::stoi(argv[++i]);
		} else if (arg == "--calibration-runs" && i + 1 < argc) {
			config.calibration_runs = std::stoi(argv[++i]);
		} else if (arg == "--help" || arg == "-h") {
			printf("Usage: scheduler_eval [OPTIONS]\n\n");
			printf("Options:\n");
			printf("  --phase1              Run calibration only\n");
			printf("  --phase2              Run evaluation only\n");
			printf("  --all                 Run both phases (default)\n");
			printf("  --db PATH             Database path (default: eval_scheduler.duckdb)\n");
			printf("  --output DIR          Output directory (default: ./eval_results)\n");
			printf("  --skip-dbgen          Skip data generation (reuse existing database)\n");
			printf("  --alpha LIST          Comma-separated alpha values (default: 0.8,0.85,0.9,0.95,1.0)\n");
			printf("  --queries N           Measured queries per run (default: 200)\n");
			printf("  --warmup N            Warmup queries per run (default: 30)\n");
			printf("  --reps N              Repetitions per config (default: 3)\n");
			printf("  --threads N           DuckDB thread count (default: system)\n");
			printf("  --calibration-runs N  Runs per query for isolation time (default: 3)\n");
			printf("  -h, --help            Show this help\n");
			exit(0);
		} else {
			fprintf(stderr, "Unknown argument: %s\n", arg.c_str());
			exit(1);
		}
	}

	return config;
}

//===----------------------------------------------------------------------===//
// Load/Save Isolation Times (for --phase2 without --phase1)
//===----------------------------------------------------------------------===//

static std::vector<IsolationTime> load_isolation_csv(const std::string &path) {
	std::vector<IsolationTime> results;
	FILE *f = fopen(path.c_str(), "r");
	if (!f) {
		return results;
	}

	char line[512];
	// Skip header
	if (!fgets(line, sizeof(line), f)) {
		fclose(f);
		return results;
	}

	while (fgets(line, sizeof(line), f)) {
		IsolationTime it;
		char sched[64];
		if (sscanf(line, "%63[^,],%d,%d,%lf", sched, &it.query_num, &it.scale_factor, &it.duration_sec) == 4) {
			it.scheduler_type = sched;
			results.push_back(it);
		}
	}
	fclose(f);
	return results;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main(int argc, char *argv[]) {
	Config config = parse_args(argc, argv);

	log("Scheduler Evaluation Client");
	log("  Database: " + config.db_path);
	log("  Output: " + config.output_dir);

	// Create output directory
	std::string mkdir_cmd = "mkdir -p " + config.output_dir;
	system(mkdir_cmd.c_str());

	// Create database
	DBConfig db_config;
	if (config.threads > 0) {
		db_config.options.maximum_threads = static_cast<idx_t>(config.threads);
	}
	DuckDB db(config.db_path, &db_config);
	log("  DuckDB initialized with " + std::to_string(db.NumberOfThreads()) + " threads");

	// Generate data
	if (!config.skip_dbgen) {
		generate_data(db, config);
	}

	// Phase 1: Calibration
	std::vector<IsolationTime> iso_times;
	std::string iso_path = config.output_dir + "/isolation_times.csv";

	if (config.run_phase1) {
		iso_times = run_phase1(db, config);
		write_isolation_csv(iso_times, iso_path);
		log("  Isolation times saved to " + iso_path);
	} else if (config.run_phase2) {
		// Try to load from previous run
		iso_times = load_isolation_csv(iso_path);
		if (iso_times.empty()) {
			log("ERROR: No isolation times found at " + iso_path + ". Run --phase1 first.");
			return 1;
		}
		log("  Loaded " + std::to_string(iso_times.size()) + " isolation times from " + iso_path);
	}

	// Phase 2: Mixed Workload
	if (config.run_phase2) {
		std::vector<QueryMeasurement> all_measurements;
		std::vector<ExperimentSummary> all_summaries;

		run_phase2(db, config, iso_times, all_measurements, all_summaries);

		// Write results
		std::string query_log_path = config.output_dir + "/query_log.csv";
		std::string summary_path = config.output_dir + "/summary.csv";

		write_query_log_csv(all_measurements, query_log_path);
		write_summary_csv(all_summaries, summary_path);

		log("  Query log saved to " + query_log_path);
		log("  Summary saved to " + summary_path);

		// Print comparison
		print_comparison(all_summaries);
	}

	log("Done.");
	return 0;
}
