// Standalone checks compile the exact production helper block, without hardware collection.
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <iostream>
#include ENERGY_SAMPLER_IMPLEMENTATION

static void Check(bool value) {
	if (!value) throw std::runtime_error("Sampler affinity regression failed");
}
template <class F> static void Reject(F action) {
	bool rejected = false;
	try { action(); } catch (const std::exception &) { rejected = true; }
	Check(rejected);
}
int main() {
	try {
		using namespace duckdb::energy_attribution;
		Check(ParseEnergySamplerCPU(nullptr) == -1);
		Check(ParseEnergySamplerCPU("8") == 8);
		for (auto text : {"", "-1", "+8", "8x", " 8", "999999999999999999999"})
			Reject([&] { ParseEnergySamplerCPU(text); });
		cpu_set_t original;
		Check(pthread_getaffinity_np(pthread_self(), sizeof(original), &original) == 0);
		int cpu = -1;
		for (int i = 0; i < CPU_SETSIZE; ++i) if (CPU_ISSET(i, &original)) { cpu = i; break; }
		Check(cpu >= 0);
		std::exception_ptr failure;
		std::thread child([&] {
			try {
				ApplyEnergySamplerCPU(-1);
				cpu_set_t inherited;
				Check(pthread_getaffinity_np(pthread_self(), sizeof(inherited), &inherited) == 0);
				Check(CPU_EQUAL(&original, &inherited));
				ApplyEnergySamplerCPU(cpu);
				Check(sched_getcpu() == cpu);
				char name[16];
				Check(pthread_getname_np(pthread_self(), name, sizeof(name)) == 0);
				Check(std::strcmp(name, "ea-sampler") == 0);
				Reject([&] { ApplyEnergySamplerCPU(-2); });
				Reject([&] { ApplyEnergySamplerCPU(CPU_SETSIZE); });
				// This host has fewer CPUs than CPU_SETSIZE; exercise the syscall failure path.
				if (cpu != CPU_SETSIZE - 1) Reject([&] { ApplyEnergySamplerCPU(CPU_SETSIZE - 1); });
			} catch (...) { failure = std::current_exception(); }
		});
		child.join();
		if (failure) std::rethrow_exception(failure);
		cpu_set_t after;
		Check(pthread_getaffinity_np(pthread_self(), sizeof(after), &after) == 0);
		Check(CPU_EQUAL(&original, &after));
		std::cout << "Sampler parsing, inherited/explicit affinity, identity and failure checks passed\n";
		return 0;
	} catch (const std::exception &e) { std::cerr << e.what() << '\n'; return 1; }
}
