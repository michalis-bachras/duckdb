#include "vmlinux.h"

#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#include "process_activity_shared.h"

char LICENSE[] SEC("license") = "GPL";

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct duckdb_process_activity_control);
} control SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, DUCKDB_PROCESS_ACTIVITY_MAX_CPUS);
	__type(key, __u32);
	__type(value, struct duckdb_process_cpu_metadata);
} cpu_metadata SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(max_entries, DUCKDB_PROCESS_ACTIVITY_MAX_CPUS);
	__uint(key_size, sizeof(__u32));
	__uint(value_size, sizeof(__u32));
} cycles SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(max_entries, DUCKDB_PROCESS_ACTIVITY_MAX_CPUS);
	__uint(key_size, sizeof(__u32));
	__uint(value_size, sizeof(__u32));
} ref_cycles SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct duckdb_process_cpu_state);
} cpu_state SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_HASH);
	__uint(max_entries, DUCKDB_PROCESS_ACTIVITY_MAX_RECORDS);
	__type(key, struct duckdb_process_activity_key);
	__type(value, struct duckdb_process_activity_value);
} activity SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct duckdb_process_activity_diagnostics);
} diagnostics SEC(".maps");

static __always_inline struct duckdb_process_counter_snapshot read_counter(void *map) {
	struct duckdb_process_counter_snapshot result = {};
	struct bpf_perf_event_value value = {};
	if (bpf_perf_event_read_value(map, BPF_F_CURRENT_CPU, &value, sizeof(value)) == 0) {
		result.counter = value.counter;
		result.enabled = value.enabled;
		result.running = value.running;
		result.valid = 1;
	}
	return result;
}

static __always_inline void note_missing_counter(void) {
	__u32 zero = 0;
	struct duckdb_process_activity_diagnostics *diag = bpf_map_lookup_elem(&diagnostics, &zero);
	if (diag) {
		diag->missing_counter++;
	}
}

static __always_inline void note_invalid_counter_delta(void) {
	__u32 zero = 0;
	struct duckdb_process_activity_diagnostics *diag = bpf_map_lookup_elem(&diagnostics, &zero);
	if (diag) {
		diag->invalid_counter_delta++;
	}
}

static __always_inline __u64 interpolate_value(__u64 start, __u64 end, __u64 elapsed, __u64 duration) {
	__u64 delta;
	__u64 quotient;
	__u64 remainder;

	if (end <= start || !duration || elapsed >= duration) {
		return elapsed >= duration ? end : start;
	}
	delta = end - start;
	quotient = delta / duration;
	remainder = delta % duration;
	return start + quotient * elapsed + (remainder * elapsed) / duration;
}

static __always_inline struct duckdb_process_counter_snapshot
interpolate_counter(const struct duckdb_process_counter_snapshot *start,
                    const struct duckdb_process_counter_snapshot *end, __u64 elapsed, __u64 duration) {
	struct duckdb_process_counter_snapshot result = {};

	if (!start->valid || !end->valid || end->counter < start->counter || end->enabled < start->enabled ||
	    end->running < start->running) {
		return result;
	}
	result.counter = interpolate_value(start->counter, end->counter, elapsed, duration);
	result.enabled = interpolate_value(start->enabled, end->enabled, elapsed, duration);
	result.running = interpolate_value(start->running, end->running, elapsed, duration);
	result.valid = 1;
	return result;
}

static __always_inline __u64 effective_generation(const struct duckdb_process_activity_control *current,
                                                   __u64 now_ns) {
	if (current->generation > 1 && current->transition_ns && now_ns < current->transition_ns) {
		return current->generation - 1;
	}
	return current->generation;
}

static __always_inline void accumulate(const struct duckdb_process_activity_key *key, struct task_struct *task,
                                       const char *fallback_comm, __u64 runtime_ns,
                                       const struct duckdb_process_counter_snapshot *start_cycles,
                                       const struct duckdb_process_counter_snapshot *end_cycles,
                                       const struct duckdb_process_counter_snapshot *start_ref,
                                       const struct duckdb_process_counter_snapshot *end_ref) {
	struct duckdb_process_activity_value zero = {};
	struct duckdb_process_activity_value *value;

	if (!runtime_ns) {
		return;
	}
	value = bpf_map_lookup_elem(&activity, key);
	if (!value) {
		if (task) {
			struct task_struct *comm_task = task;
			if (key->kind == DUCKDB_PROCESS_ACTIVITY_PROCESS && key->tid == 0) {
				struct task_struct *leader = BPF_CORE_READ(task, group_leader);
				if (leader) {
					comm_task = leader;
				}
			}
			BPF_CORE_READ_STR_INTO(&zero.comm, comm_task, comm);
		} else if (fallback_comm) {
			bpf_probe_read_kernel(zero.comm, sizeof(zero.comm), fallback_comm);
		}
		bpf_map_update_elem(&activity, key, &zero, BPF_NOEXIST);
		value = bpf_map_lookup_elem(&activity, key);
		if (!value) {
			__u32 index = 0;
			struct duckdb_process_activity_diagnostics *diag = bpf_map_lookup_elem(&diagnostics, &index);
			if (diag) {
				diag->activity_map_insert_failure++;
			}
			return;
		}
	}
	value->runtime_ns += runtime_ns;
	value->switch_count++;
	if (!start_cycles->valid || !end_cycles->valid) {
		note_missing_counter();
	} else if (end_cycles->counter >= start_cycles->counter && end_cycles->enabled >= start_cycles->enabled &&
	           end_cycles->running >= start_cycles->running) {
		value->cycles += end_cycles->counter - start_cycles->counter;
		value->time_enabled_ns += end_cycles->enabled - start_cycles->enabled;
		value->time_running_ns += end_cycles->running - start_cycles->running;
	} else {
		note_invalid_counter_delta();
	}
	if (!start_ref->valid || !end_ref->valid) {
		note_missing_counter();
	} else if (end_ref->counter >= start_ref->counter && end_ref->enabled >= start_ref->enabled &&
	           end_ref->running >= start_ref->running) {
		value->ref_cycles += end_ref->counter - start_ref->counter;
		value->ref_time_enabled_ns += end_ref->enabled - start_ref->enabled;
		value->ref_time_running_ns += end_ref->running - start_ref->running;
	} else {
		note_invalid_counter_delta();
	}
}

static __always_inline void accumulate_interval(
    const struct duckdb_process_activity_key *original_key, struct task_struct *task, const char *fallback_comm,
    __u64 start_ns, __u64 end_ns, const struct duckdb_process_counter_snapshot *start_cycles,
    const struct duckdb_process_counter_snapshot *end_cycles,
    const struct duckdb_process_counter_snapshot *start_ref,
    const struct duckdb_process_counter_snapshot *end_ref,
    const struct duckdb_process_activity_control *current) {
	struct duckdb_process_activity_key new_key;
	struct duckdb_process_counter_snapshot boundary_cycles;
	struct duckdb_process_counter_snapshot boundary_ref;
	struct duckdb_process_activity_diagnostics *diag;
	__u64 transition_ns;
	__u64 duration;
	__u64 elapsed;
	__u32 zero = 0;

	if (!original_key || !current || end_ns <= start_ns) {
		return;
	}
	transition_ns = current->transition_ns;
	if (original_key->generation + 1 != current->generation || !transition_ns || end_ns <= transition_ns) {
		accumulate(original_key, task, fallback_comm, end_ns - start_ns, start_cycles, end_cycles, start_ref, end_ref);
		return;
	}
	new_key = *original_key;
	new_key.generation = current->generation;
	if (start_ns >= transition_ns) {
		accumulate(&new_key, task, fallback_comm, end_ns - start_ns, start_cycles, end_cycles, start_ref, end_ref);
		diag = bpf_map_lookup_elem(&diagnostics, &zero);
		if (diag) {
			diag->boundary_split_count++;
			diag->boundary_split_runtime_ns += end_ns - start_ns;
			if (start_cycles->valid && end_cycles->valid && end_cycles->counter >= start_cycles->counter) {
				diag->boundary_split_cycles += end_cycles->counter - start_cycles->counter;
			}
			if (start_ref->valid && end_ref->valid && end_ref->counter >= start_ref->counter) {
				diag->boundary_split_ref_cycles += end_ref->counter - start_ref->counter;
			}
		}
		return;
	}
	duration = end_ns - start_ns;
	elapsed = transition_ns - start_ns;
	boundary_cycles = interpolate_counter(start_cycles, end_cycles, elapsed, duration);
	boundary_ref = interpolate_counter(start_ref, end_ref, elapsed, duration);
	accumulate(original_key, task, fallback_comm, elapsed, start_cycles, &boundary_cycles, start_ref, &boundary_ref);
	accumulate(&new_key, task, fallback_comm, end_ns - transition_ns, &boundary_cycles, end_cycles, &boundary_ref,
	           end_ref);
	diag = bpf_map_lookup_elem(&diagnostics, &zero);
	if (diag) {
		diag->boundary_split_count++;
		diag->boundary_split_runtime_ns += end_ns - transition_ns;
		if (boundary_cycles.valid && end_cycles->valid && end_cycles->counter >= boundary_cycles.counter) {
			diag->boundary_split_cycles += end_cycles->counter - boundary_cycles.counter;
		}
		if (boundary_ref.valid && end_ref->valid && end_ref->counter >= boundary_ref.counter) {
			diag->boundary_split_ref_cycles += end_ref->counter - boundary_ref.counter;
		}
	}
}

static __always_inline struct duckdb_process_cpu_metadata *current_cpu_metadata(void) {
	__u32 cpu = bpf_get_smp_processor_id();
	return bpf_map_lookup_elem(&cpu_metadata, &cpu);
}

static __always_inline struct duckdb_process_activity_key build_key(struct task_struct *task, __u64 generation,
                                                                     __u32 kind,
                                                                     const struct duckdb_process_cpu_metadata *metadata,
                                                                     const struct duckdb_process_activity_control *current) {
	struct duckdb_process_activity_key key = {};
	struct task_struct *leader;
	__u32 tid;

	key.generation = generation;
	key.socket_id = metadata ? metadata->socket_id : 0;
	key.kind = kind;
	if (kind != DUCKDB_PROCESS_ACTIVITY_PROCESS || !task) {
		return key;
	}
	key.tgid = BPF_CORE_READ(task, tgid);
	tid = BPF_CORE_READ(task, pid);
	if (key.tgid == 0) {
		key.kind = DUCKDB_PROCESS_ACTIVITY_SYSTEM_IDLE;
		return key;
	}
	leader = BPF_CORE_READ(task, group_leader);
	if (leader) {
		key.process_start_time_ns = BPF_CORE_READ(leader, start_boottime);
	}
	if (metadata && metadata->helper_tid && tid == metadata->helper_tid) {
		key.kind = DUCKDB_PROCESS_ACTIVITY_MEASUREMENT_OVERHEAD;
		key.tgid = current ? current->duckdb_tgid : key.tgid;
		key.tid = 0;
		return key;
	}
	// DuckDB needs TID granularity for its segment hierarchy. Other activity is
	// attributed at process granularity and shares one key per socket.
	key.tid = current && key.tgid == current->duckdb_tgid ? tid : 0;
	return key;
}

static __always_inline void close_running(struct duckdb_process_running_slice *slice, struct task_struct *task,
                                          __u64 now_ns, const struct duckdb_process_activity_control *current,
                                          const struct duckdb_process_counter_snapshot *end_cycles,
                                          const struct duckdb_process_counter_snapshot *end_ref) {
	if (!slice || !slice->valid || now_ns <= slice->start_ns) {
		return;
	}
	accumulate_interval(&slice->key, task, 0, slice->start_ns, now_ns, &slice->cycles, end_cycles,
	                    &slice->ref_cycles, end_ref, current);
	slice->valid = 0;
}

static __always_inline void open_running(struct duckdb_process_running_slice *slice,
                                         const struct duckdb_process_activity_control *current,
                                         struct task_struct *task, __u64 now_ns,
                                         const struct duckdb_process_cpu_metadata *metadata,
                                         const struct duckdb_process_counter_snapshot *start_cycles,
                                         const struct duckdb_process_counter_snapshot *start_ref) {
	if (!slice || !current || !current->enabled) {
		return;
	}
	slice->valid = 0;
	slice->key =
	    build_key(task, effective_generation(current, now_ns), DUCKDB_PROCESS_ACTIVITY_PROCESS, metadata, current);
	slice->start_ns = now_ns;
	slice->cycles = *start_cycles;
	slice->ref_cycles = *start_ref;
	slice->reserved = 0;
	slice->valid = 1;
}

SEC("tp_btf/sched_switch")
int BPF_PROG(handle_sched_switch, bool preempt, struct task_struct *previous, struct task_struct *next) {
	__u32 zero = 0;
	struct duckdb_process_cpu_state *state = bpf_map_lookup_elem(&cpu_state, &zero);
	struct duckdb_process_activity_control *current = bpf_map_lookup_elem(&control, &zero);
	struct duckdb_process_counter_snapshot cycle_snapshot;
	struct duckdb_process_counter_snapshot ref_snapshot;
	struct duckdb_process_cpu_metadata *metadata;
	__u64 now_ns;

	if (!current || !current->enabled) {
		if (state) {
			state->running.valid = 0;
			state->interrupt.depth = 0;
		}
		return 0;
	}
	if (!state || state->interrupt.depth != 0) {
		return 0;
	}
	now_ns = bpf_ktime_get_ns();
	cycle_snapshot = read_counter(&cycles);
	ref_snapshot = read_counter(&ref_cycles);
	metadata = current_cpu_metadata();
	close_running(&state->running, previous, now_ns, current, &cycle_snapshot, &ref_snapshot);
	open_running(&state->running, current, next, now_ns, metadata, &cycle_snapshot, &ref_snapshot);
	return 0;
}

static __always_inline int interrupt_enter(void) {
	__u32 zero = 0;
	struct duckdb_process_cpu_state *state = bpf_map_lookup_elem(&cpu_state, &zero);
	struct duckdb_process_activity_control *current = bpf_map_lookup_elem(&control, &zero);
	struct duckdb_process_counter_snapshot cycle_snapshot;
	struct duckdb_process_counter_snapshot ref_snapshot;
	struct task_struct *task;
	__u64 now_ns;
	if (!state) {
		return 0;
	}
	if (!current || !current->enabled) {
		state->interrupt.depth = 0;
		return 0;
	}
	if (state->interrupt.depth++ != 0) {
		return 0;
	}
	now_ns = bpf_ktime_get_ns();
	cycle_snapshot = read_counter(&cycles);
	ref_snapshot = read_counter(&ref_cycles);
	task = (struct task_struct *)bpf_get_current_task_btf();
	close_running(&state->running, task, now_ns, current, &cycle_snapshot, &ref_snapshot);
	state->interrupt.start_ns = now_ns;
	state->interrupt.generation = effective_generation(current, now_ns);
	state->interrupt.cycles = cycle_snapshot;
	state->interrupt.ref_cycles = ref_snapshot;
	return 0;
}

static __always_inline int interrupt_exit(void) {
	__u32 zero = 0;
	struct duckdb_process_cpu_state *state = bpf_map_lookup_elem(&cpu_state, &zero);
	struct duckdb_process_activity_control *current = bpf_map_lookup_elem(&control, &zero);
	struct duckdb_process_counter_snapshot cycle_snapshot;
	struct duckdb_process_counter_snapshot ref_snapshot;
	struct duckdb_process_cpu_metadata *metadata;
	struct duckdb_process_activity_key key;
	struct task_struct *task;
	__u64 now_ns;
	char comm[DUCKDB_PROCESS_ACTIVITY_COMM_LENGTH] = {};
	comm[0] = 'H';
	comm[1] = 'O';
	comm[2] = 'S';
	comm[3] = 'T';
	comm[4] = '_';
	comm[5] = 'I';
	comm[6] = 'R';
	comm[7] = 'Q';

	if (!current || !current->enabled) {
		if (state) {
			state->interrupt.depth = 0;
		}
		return 0;
	}
	if (!state || state->interrupt.depth == 0) {
		struct duckdb_process_activity_diagnostics *diag = bpf_map_lookup_elem(&diagnostics, &zero);
		if (diag) {
			diag->interrupt_underflow++;
		}
		return 0;
	}
	if (--state->interrupt.depth != 0) {
		return 0;
	}
	now_ns = bpf_ktime_get_ns();
	cycle_snapshot = read_counter(&cycles);
	ref_snapshot = read_counter(&ref_cycles);
	metadata = current_cpu_metadata();
	key = build_key(0, state->interrupt.generation, DUCKDB_PROCESS_ACTIVITY_HOST_IRQ, metadata, current);
	if (now_ns > state->interrupt.start_ns) {
		accumulate_interval(&key, 0, comm, state->interrupt.start_ns, now_ns, &state->interrupt.cycles,
		                    &cycle_snapshot, &state->interrupt.ref_cycles, &ref_snapshot, current);
	}
	task = (struct task_struct *)bpf_get_current_task_btf();
	open_running(&state->running, current, task, now_ns, metadata, &cycle_snapshot, &ref_snapshot);
	return 0;
}

SEC("tracepoint/irq/irq_handler_entry")
int handle_irq_entry(void *ctx) {
	return interrupt_enter();
}

SEC("tracepoint/irq/irq_handler_exit")
int handle_irq_exit(void *ctx) {
	return interrupt_exit();
}

SEC("tracepoint/irq/softirq_entry")
int handle_softirq_entry(void *ctx) {
	return interrupt_enter();
}

SEC("tracepoint/irq/softirq_exit")
int handle_softirq_exit(void *ctx) {
	return interrupt_exit();
}
