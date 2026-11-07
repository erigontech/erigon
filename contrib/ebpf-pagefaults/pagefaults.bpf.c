#include <vmlinux.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "maps.bpf.h"

#define TASK_COMM_LEN 16
#define FILENAME_LEN 64

// Layout is the on-wire format the exporter decodes byte-by-byte; keep
// pid first so the u32 is 4-aligned and the char arrays add no padding.
struct key_t {
    u32 pid;
    char comm[TASK_COMM_LEN];
    char filename[FILENAME_LEN];
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 16384);
    __type(key, struct key_t);
    __type(value, u64);
} page_faults_total SEC(".maps");

SEC("kprobe/handle_mm_fault")
int BPF_KPROBE(kprobe__handle_mm_fault, struct vm_area_struct *vma)
{
    struct key_t key = {};
    struct file *file;

    key.pid = bpf_get_current_pid_tgid() >> 32;
    bpf_get_current_comm(&key.comm, sizeof(key.comm));

    file = BPF_CORE_READ(vma, vm_file);
    if (file) {
        const unsigned char *name = BPF_CORE_READ(file, f_path.dentry, d_name.name);
        bpf_probe_read_kernel_str(&key.filename, sizeof(key.filename), name);
    } else {
        __builtin_memcpy(&key.filename, "[anon]", 7);
    }

    increment_map(&page_faults_total, &key, 1);

    return 0;
}

char LICENSE[] SEC("license") = "GPL";
