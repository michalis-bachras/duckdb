//===----------------------------------------------------------------------===//
//                         DuckDB
//
// query_request_metadata.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/query_request_metadata.hpp"

#include "duckdb/main/database.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_request_profile_store.hpp"

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>

namespace duckdb {

namespace {

static std::mutex g_request_metadata_lock;
static std::unordered_map<ClientContext *, QueryRequestMetadata> g_active_request_metadata;

static uint64_t TimestampNs() {
	return static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	        .count());
}

static bool ParseUint64Strict(const string &value, uint64_t &result) {
	if (value.empty()) {
		return false;
	}
	char *end = nullptr;
	errno = 0;
	auto parsed = std::strtoull(value.c_str(), &end, 10);
	if (errno != 0 || end == value.c_str() || (end && *end != '\0')) {
		return false;
	}
	result = static_cast<uint64_t>(parsed);
	return true;
}

static bool ParseDoubleStrict(const string &value, double &result) {
	if (value.empty()) {
		return false;
	}
	char *end = nullptr;
	errno = 0;
	auto parsed = std::strtod(value.c_str(), &end);
	if (errno != 0 || end == value.c_str() || (end && *end != '\0') || !std::isfinite(parsed)) {
		return false;
	}
	result = parsed;
	return true;
}

static QueryRequestMetadata ParseQueryRequestMetadata(const string &query) {
	QueryRequestMetadata result;
	string trimmed = query;
	StringUtil::Trim(trimmed);
	if (!StringUtil::StartsWith(trimmed, "/*")) {
		return result;
	}
	auto comment_end = trimmed.find("*/", 2);
	if (comment_end == string::npos) {
		result.parse_status = "malformed_comment";
		return result;
	}
	auto payload = trimmed.substr(2, comment_end - 2);
	StringUtil::Trim(payload);
	std::stringstream stream(payload);
	string token;
	if (!(stream >> token) || token != "duckdb_sla_v1") {
		return result;
	}

	std::unordered_map<string, string> fields;
	while (stream >> token) {
		auto separator = token.find('=');
		if (separator == string::npos || separator == 0 || separator + 1 >= token.size()) {
			result.parse_status = "malformed_token";
			return result;
		}
		fields[token.substr(0, separator)] = token.substr(separator + 1);
	}

	auto require_uint = [&](const string &key, uint64_t &target) -> bool {
		auto entry = fields.find(key);
		return entry != fields.end() && ParseUint64Strict(entry->second, target);
	};
	auto require_double = [&](const string &key, double &target) -> bool {
		auto entry = fields.find(key);
		return entry != fields.end() && ParseDoubleStrict(entry->second, target);
	};

	if (!require_uint("request_id", result.request_id) || !require_uint("template_id", result.template_id) ||
	    !require_uint("scale_factor", result.scale_factor) || !require_double("sla_tag", result.sla_tag) ||
	    !require_double("penalty_per_s", result.sla_penalty_per_s) ||
	    !require_uint("deadline_ns", result.deadline_ns)) {
		result.parse_status = "missing_or_invalid_field";
		return result;
	}
	result.valid = true;
	result.parse_status = "ok";
	return result;
}

} // namespace

bool QueryRequestMetadataManager::ProfilingEnabled(ClientContext &context) {
	const auto &config = ClientConfig::GetConfig(context);
	return config.query_request_profiling_enabled || config.query_activation_scheduler_enabled ||
	       DBConfig::GetConfig(context).options.query_sla_scheduler_enabled;
}

bool QueryRequestMetadataManager::NeedsMetadata(ClientContext &context) {
	const auto &config = ClientConfig::GetConfig(context);
	return config.query_request_profiling_enabled || config.query_admission_max_active > 0 ||
	       config.query_activation_scheduler_enabled || config.query_activation_debug_enabled ||
	       DBConfig::GetConfig(context).options.query_sla_scheduler_enabled;
}

void QueryRequestMetadataManager::BeginQuery(ClientContext &context, uint64_t db_query_id, const string &query) {
	auto metadata = ParseQueryRequestMetadata(query);
	lock_guard<std::mutex> guard(g_request_metadata_lock);
	if (!metadata.valid) {
		g_active_request_metadata.erase(&context);
		return;
	}
	metadata.db_query_id = db_query_id;
	metadata.query_start_ns = TimestampNs();
	g_active_request_metadata[&context] = std::move(metadata);
}

void QueryRequestMetadataManager::RefreshQueryStart(ClientContext &context) {
	lock_guard<std::mutex> guard(g_request_metadata_lock);
	auto entry = g_active_request_metadata.find(&context);
	if (entry == g_active_request_metadata.end() || !entry->second.valid) {
		return;
	}
	entry->second.query_start_ns = TimestampNs();
}

void QueryRequestMetadataManager::EndQuery(ClientContext &context, bool success) {
	QueryRequestMetadata metadata;
	{
		lock_guard<std::mutex> guard(g_request_metadata_lock);
		auto entry = g_active_request_metadata.find(&context);
		if (entry == g_active_request_metadata.end()) {
			return;
		}
		metadata = entry->second;
		g_active_request_metadata.erase(entry);
	}
	if (!success || !ProfilingEnabled(context)) {
		return;
	}
	auto query_end_ns = TimestampNs();
	auto pipeline_profiles = QueryProfiler::Get(context).GetPipelineProfilesSnapshot();
	DatabaseInstance::GetDatabase(context).GetQueryRequestProfileStore().RecordQueryCompletion(
	    metadata, query_end_ns, pipeline_profiles);
}

bool QueryRequestMetadataManager::HasActiveMetadata(ClientContext &context) {
	lock_guard<std::mutex> guard(g_request_metadata_lock);
	auto entry = g_active_request_metadata.find(&context);
	return entry != g_active_request_metadata.end() && entry->second.valid;
}

bool QueryRequestMetadataManager::TryGetActive(ClientContext &context, QueryRequestMetadata &metadata) {
	lock_guard<std::mutex> guard(g_request_metadata_lock);
	auto entry = g_active_request_metadata.find(&context);
	if (entry == g_active_request_metadata.end() || !entry->second.valid) {
		return false;
	}
	metadata = entry->second;
	return true;
}

} // namespace duckdb
