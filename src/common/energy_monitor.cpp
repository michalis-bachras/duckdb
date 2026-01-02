//
// Created by michalis on 10/7/25.
//


#include "duckdb/common/energy_monitor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_buffer.hpp"
#include <fstream>
#include <chrono>
#include <dirent.h>

namespace duckdb {

bool EnergyMonitor::IsRAPLAvailable() {
	const char* rapl_base = "/sys/class/powercap/intel-rapl";
	DIR* dir = opendir(rapl_base);
	if (!dir) {
		return false;
	}
	closedir(dir);
	return true;
}

vector<RAPLDomain> EnergyMonitor::DetectRAPLDomains() {
    vector<RAPLDomain> domains;
    const char* rapl_base = "/sys/class/powercap/intel-rapl";

    DIR* dir = opendir(rapl_base);
    if (!dir) {
        return domains;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        string dirname = entry->d_name;

        if (dirname.find("intel-rapl:") == 0) {
            string domain_path = string(rapl_base) + "/" + dirname;
            string energy_file = domain_path + "/energy_uj";

            std::ifstream test_file(energy_file);
            if (!test_file.is_open()) {
                continue;
            }
            test_file.close();

            // Read domain name
            string domain_name;
            string name_file = domain_path + "/name";
            std::ifstream name_stream(name_file);
            if (name_stream.is_open()) {
                std::getline(name_stream, domain_name);
                size_t end = domain_name.find_last_not_of(" \n\r\t");
                if (end != string::npos) {
                    domain_name.erase(end + 1);
                }
            } else {
                domain_name = dirname;
            }

            // Read max value
            uint64_t max_value = 0;
            string max_file = domain_path + "/max_energy_range_uj";
            std::ifstream max_stream(max_file);
            if (max_stream.is_open()) {
                string max_content;
                std::getline(max_stream, max_content);
                try {
                    max_value = std::stoull(max_content);
                } catch (...) {
                    max_value = 0;
                }
            }

            if (max_value == 0) {
                max_value = DEFAULT_RAPL_MAX;
            }

            domains.emplace_back(domain_name, energy_file, max_value);
        }
    }
    closedir(dir);

    return domains;
}

EnergyMonitor::EnergyMonitor() {
	// Detect available RAPL domains for this instance
	rapl_domains = DetectRAPLDomains();
}

EnergyMonitor::~EnergyMonitor() {
	if (is_running.load()) {
		StopMonitoring();
	}
}

bool EnergyMonitor::ReadRAPLFileContent(const string& filepath, string& content) {
	std::ifstream file(filepath);
	if (!file.is_open()) {
		return false;
	}

	// Most RAPL files contain just one line of data
	std::getline(file, content);

	size_t end = content.find_last_not_of(" \n\r\t");

	if (end != string::npos) {
		content.erase(end + 1);
	}
	return true;
}


vector<DomainEnergyReading> EnergyMonitor::ReadRAPLEnergy() {
	vector<DomainEnergyReading> readings;

	for (const auto& domain : rapl_domains) {
		std::ifstream rapl_file(domain.energy_path);
		if (rapl_file.is_open()) {
			uint64_t energy_uj;
			rapl_file >> energy_uj;
			readings.emplace_back(domain.name, energy_uj);
		}
	}

	return readings;
}



void EnergyMonitor::MonitoringLoop() {
	// Take initial readings
	auto initial_readings = ReadRAPLEnergy();
	{
		std::lock_guard<mutex> guard(rapl_lock);
		for (const auto& reading : initial_readings) {
			last_reading_per_domain[reading.domain_name] = reading.energy_uj;
			accumulated_energy_per_domain[reading.domain_name] = 0.0;
		}
	}

	// Main monitoring loop - runs until should_stop becomes true
	// This loop samples energy every SAMPLE_INTERVAL_MS milliseconds
	while (!should_stop.load(std::memory_order_acquire)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(SAMPLE_INTERVAL_MS));

		//Read current energy values from all domains
		auto current_readings = ReadRAPLEnergy();
		if (current_readings.empty()) {
			// If we got no readings, something went wrong
			// Continue to the next iteration rather than processing bad data
			continue;
		}

		for (const auto& reading : current_readings) {
			const string& domain_name = reading.domain_name;
			uint64_t current_energy_uj = reading.energy_uj;

			uint64_t last_energy_uj = last_reading_per_domain[domain_name];
			uint64_t domain_max = DEFAULT_RAPL_MAX;
			for (const auto& domain : rapl_domains) {
				if (domain.name == domain_name) {
					domain_max = domain.max_value;
					break;
				}
			}

			// Calculate energy difference (handle RAPL wraparound)
			uint64_t energy_diff_uj;
			if (current_energy_uj >= last_energy_uj) {
				energy_diff_uj = current_energy_uj - last_energy_uj;
			} else {
				energy_diff_uj = (domain_max - last_energy_uj) + current_energy_uj;
			}

			double energy_joules = static_cast<double>(energy_diff_uj)/ 1000000.0;

			// Add this energy delta to the accumulator for this domain
			accumulated_energy_per_domain[domain_name] += energy_joules;

			// Update the last reading for the next iteration
			last_reading_per_domain[domain_name] = current_energy_uj;

		}
	}
}

void EnergyMonitor::StartMonitoring() {
	if (rapl_domains.empty()) {
		return;  // RAPL not available
	}

	is_running.store(true);
	should_stop.store(false);
	monitor_thread = std::thread(&EnergyMonitor::MonitoringLoop, this);
}

unordered_map<string, double> EnergyMonitor::StopMonitoring() {
	should_stop.store(true, std::memory_order_release);

	if (monitor_thread.joinable()) {
		monitor_thread.join();
	}

	is_running.store(false);

	std::lock_guard<mutex> guard(rapl_lock);
	return accumulated_energy_per_domain;
}

unordered_map<string, double> EnergyMonitor::GetAccumulatedEnergyPerDomain() const {
	std::lock_guard<mutex> guard(rapl_lock);
	return accumulated_energy_per_domain;
}
} // namespace duckdb












