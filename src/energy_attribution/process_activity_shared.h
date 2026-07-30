#pragma once

#ifndef __VMLINUX_H__
#include <linux/types.h>
#endif

#define DUCKDB_PROCESS_ACTIVITY_COMM_LENGTH 16
#define DUCKDB_PROCESS_ACTIVITY_MAX_CPUS 4096
#define DUCKDB_PROCESS_ACTIVITY_MAX_RECORDS 16384

enum duckdb_process_activity_kind {
	DUCKDB_PROCESS_ACTIVITY_PROCESS = 0,
	DUCKDB_PROCESS_ACTIVITY_HOST_KERNEL = 1,
	DUCKDB_PROCESS_ACTIVITY_HOST_IRQ = 2,
	DUCKDB_PROCESS_ACTIVITY_MEASUREMENT_OVERHEAD = 3,
	DUCKDB_PROCESS_ACTIVITY_SYSTEM_IDLE = 4
};

struct duckdb_process_activity_control {
	__u64 generation;
	__u64 transition_ns;
	__u32 duckdb_tgid;
	__u32 enabled;
};

struct duckdb_process_cpu_metadata {
	__u32 socket_id;
	__u32 helper_tid;
};

struct duckdb_process_activity_key {
	__u64 generation;
	__u64 process_start_time_ns;
	__u32 tgid;
	__u32 tid;
	__u32 socket_id;
	__u32 kind;
};

struct duckdb_process_counter_snapshot {
	__u64 counter;
	__u64 enabled;
	__u64 running;
	__u32 valid;
	__u32 reserved;
};

struct duckdb_process_running_slice {
	struct duckdb_process_activity_key key;
	__u64 start_ns;
	struct duckdb_process_counter_snapshot cycles;
	struct duckdb_process_counter_snapshot ref_cycles;
	__u32 valid;
	__u32 reserved;
};

struct duckdb_process_interrupt_state {
	__u64 start_ns;
	__u64 generation;
	struct duckdb_process_counter_snapshot cycles;
	struct duckdb_process_counter_snapshot ref_cycles;
	__u32 depth;
	__u32 reserved;
};

struct duckdb_process_cpu_state {
	struct duckdb_process_running_slice running;
	struct duckdb_process_interrupt_state interrupt;
};

struct duckdb_process_activity_value {
	__u64 runtime_ns;
	__u64 cycles;
	__u64 ref_cycles;
	__u64 time_enabled_ns;
	__u64 time_running_ns;
	__u64 ref_time_enabled_ns;
	__u64 ref_time_running_ns;
	__u64 switch_count;
	char comm[DUCKDB_PROCESS_ACTIVITY_COMM_LENGTH];
};

struct duckdb_process_activity_diagnostics {
	__u64 activity_map_insert_failure;
	__u64 missing_counter;
	__u64 invalid_counter_delta;
	__u64 interrupt_underflow;
	__u64 boundary_split_count;
	__u64 boundary_split_runtime_ns;
	__u64 boundary_split_cycles;
	__u64 boundary_split_ref_cycles;
};
