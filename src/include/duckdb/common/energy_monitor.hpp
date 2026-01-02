//
// Created by michalis on 10/7/25.
//

#pragma once


#include "duckdb/common/types.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"

#include <thread>

namespace duckdb {

constexpr uint64_t SAMPLE_INTERVAL_MS = 500;
constexpr uint64_t DEFAULT_RAPL_MAX = 262143328850ULL;

struct RAPLDomain {
	string name;
	string energy_path;
	uint64_t max_value;

	RAPLDomain(string n,string path,uint64_t max):
		name(std::move(n)), energy_path(std::move(path)), max_value(max) {}
};

struct DomainEnergyReading {
	string domain_name;
	uint64_t energy_uj;

	DomainEnergyReading(string domain_name,uint64_t energy_uj):
		domain_name(std::move(domain_name)), energy_uj(energy_uj) {}
};

class EnergyMonitor {
public:

	EnergyMonitor();
	~EnergyMonitor();

	// Check if RAPL is available
	static bool IsRAPLAvailable();

	// Get available RAPL domains
	static vector<RAPLDomain> DetectRAPLDomains();

	// Start monitoring for a query
	void StartMonitoring();

	// Stop monitoring and get final results
	unordered_map<string, double> StopMonitoring();

	// Get current accumulated energy per domain (joules)
	unordered_map<string, double> GetAccumulatedEnergyPerDomain() const;


private:

	// Background monitoring thread
	void MonitoringLoop();

	// Read RAPL energy from all available domains
	vector<DomainEnergyReading> ReadRAPLEnergy();

	bool ReadRAPLFileContent(const string& filepath, string& content);

	std::thread monitor_thread;
	atomic<bool> should_stop{false};
	atomic<bool> is_running{false};

	// Per-domain state
	unordered_map<string, uint64_t> last_reading_per_domain;
	unordered_map<string, double> accumulated_energy_per_domain;

	// RAPL domains
	vector<RAPLDomain> rapl_domains;

	mutable mutex rapl_lock;




};

}// namespace duckdb

