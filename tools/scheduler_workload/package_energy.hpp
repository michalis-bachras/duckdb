#pragma once

// Native workload-boundary measurements. No idle subtraction, no frequency writes.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <string>
#include <stdexcept>
#include <thread>
#include <vector>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace duckdb {
namespace scheduler_workload {

class PackageEnergyMeter {
	struct Zone {
		std::string path, name;
		uint64_t range = 0;
	};
	struct Sample {
		uint64_t before_ns, after_ns;
		std::vector<uint64_t> values;
	};
	std::vector<Zone> zones;
	std::vector<Sample> samples;
	std::thread sampler;
	std::mutex lock;
	std::condition_variable wake;
	bool stopping = false;
	bool enabled;
	std::string error;
	uint64_t begin_ns = 0, end_ns = 0;
	int observer_cpu = -1;

	static uint64_t Now() {
		return std::chrono::duration_cast<std::chrono::nanoseconds>(
		           std::chrono::steady_clock::now().time_since_epoch()).count();
	}
	static uint64_t Read(const std::string &path) {
		uint64_t value;
		std::ifstream input(path);
		if (!(input >> value)) throw std::runtime_error("Cannot read energy counter: " + path);
		return value;
	}
	void SampleCounters() {
		Sample s; s.before_ns = Now();
		for (const auto &z : zones) s.values.push_back(Read(z.path + "/energy_uj"));
		s.after_ns = Now(); samples.push_back(std::move(s));
	}

public:
	explicit PackageEnergyMeter(bool enable) : enabled(enable) {
		if (!enabled) return;
		for (int i = 0; i < 16; i++) {
			auto path = std::string("/sys/class/powercap/intel-rapl:") + std::to_string(i);
			std::ifstream input(path + "/name"); std::string name;
			if (std::getline(input, name) && name.find("package-") == 0) {
				Zone zone; zone.path=path; zone.name=name; zone.range=Read(path + "/max_energy_range_uj");
				zones.push_back(zone);
				(void)Read(path + "/energy_uj");
			}
		}
		if (zones.size() != 2) throw std::runtime_error("Both package RAPL domains are required");
	}
	~PackageEnergyMeter() { Stop(); }
	uint64_t Start() {
		if (!enabled) return Now();
		SampleCounters();
		begin_ns = samples.back().after_ns;
		sampler = std::thread([&]() {
#ifdef __linux__
			cpu_set_t available;
			CPU_ZERO(&available);
			if (pthread_getaffinity_np(pthread_self(), sizeof(available), &available) == 0) {
				for (int cpu = CPU_SETSIZE - 1; cpu >= 0; cpu--) {
					if (!CPU_ISSET(cpu, &available)) continue;
					cpu_set_t single; CPU_ZERO(&single); CPU_SET(cpu, &single);
					if (pthread_setaffinity_np(pthread_self(), sizeof(single), &single) == 0) observer_cpu = cpu;
					break;
				}
			}
#endif
			std::unique_lock<std::mutex> guard(lock);
			try {
				while (!wake.wait_for(guard, std::chrono::milliseconds(100), [&]() { return stopping; })) {
					SampleCounters();
				}
			} catch (const std::exception &e) { error = e.what(); }
		});
		return begin_ns;
	}
	void Stop() {
		if (!enabled || !sampler.joinable()) return;
		{ std::lock_guard<std::mutex> guard(lock); stopping = true; }
		wake.notify_all(); sampler.join();
		try { SampleCounters(); end_ns = samples.back().after_ns; }
		catch (const std::exception &e) { error = e.what(); }
	}
	void Write(const std::string &directory) {
		if (!enabled) return;
		Stop();
		std::ofstream raw(directory + "/package_energy_samples.csv");
		raw << "sample,before_ns,after_ns,package,energy_uj,max_energy_range_uj\n";
		std::vector<double> totals(zones.size(), 0);
		double max_gap = 0; uint64_t max_skew = 0; bool valid = error.empty() && samples.size() >= 2;
		for (size_t i = 0; i < samples.size(); i++) {
			const auto &s = samples[i]; max_skew = std::max(max_skew, s.after_ns-s.before_ns);
			if (i) max_gap = std::max(max_gap, (s.after_ns-samples[i-1].after_ns)/1e9);
			for (size_t j = 0; j < zones.size(); j++) {
				raw << i << ',' << s.before_ns << ',' << s.after_ns << ',' << zones[j].name << ',' << s.values[j] << ',' << zones[j].range << '\n';
				if (i) {
					auto previous = samples[i-1].values[j];
					auto delta = s.values[j] >= previous ? s.values[j]-previous : zones[j].range-previous+s.values[j];
					totals[j] += delta/1e6;
					// A 1000-W bound is deliberately conservative; missing whole wraps cannot pass silently.
					if ((s.after_ns-samples[i-1].after_ns)/1e9 >= zones[j].range/1e6/1000) valid = false;
				}
			}
		}
		auto boundary_skew = samples.empty() ? uint64_t(0) : std::max(samples.front().after_ns-samples.front().before_ns,
		    samples.back().after_ns-samples.back().before_ns);
		// Intermediate cumulative increments telescope; their read skew does not
		// change the finite-work energy. Gap/wrap checks still cover every interval.
		valid = valid && max_gap <= .5 && boundary_skew <= 2000000;
		std::ofstream output(directory + "/package_energy.json");
		output << std::setprecision(17) << "{\"scope\":\"both CPU packages, first arrival release to last terminal result\",\"valid\":" << (valid?"true":"false")
		       << ",\"start_ns\":" << begin_ns << ",\"end_ns\":" << end_ns << ",\"max_gap_s\":" << max_gap
		       << ",\"max_read_skew_ns\":" << max_skew << ",\"total_package_j\":" << totals[0]+totals[1]
		       << ",\"max_boundary_read_skew_ns\":" << boundary_skew
		       << ",\"boundary_energy_error_bound_j\":" << (samples.empty()?0:
		          (samples.front().after_ns-samples.front().before_ns+samples.back().after_ns-samples.back().before_ns)*2e-6)
		       << ",\"package_0_j\":" << totals[0] << ",\"package_1_j\":" << totals[1]
		       << ",\"observer_cpu\":" << observer_cpu << "}\n";
	}
};

} // namespace scheduler_workload
} // namespace duckdb
