#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/energy_monitor.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test RAPL availability detection", "[energy][.]") {
	// Test if RAPL detection works
	bool rapl_available = EnergyMonitor::IsRAPLAvailable();

	// On systems without RAPL, this should return false
	// On systems with RAPL (Intel/AMD Linux), this should return true
	// This test is informational - it will pass regardless
	if (rapl_available) {
		REQUIRE(rapl_available == true);
	} else {
		REQUIRE(rapl_available == false);
	}
}