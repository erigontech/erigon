package syscheck

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon/common/log/v3"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
)

// CheckKernelAllocationHints inspects Linux kernel /proc sysctls related to
// virtual memory overcommit and max vma mappings, plus basic RAM/SWAP, and
// logs precise remediation steps if something looks risky for heavy mmap/fork.
//
// It returns a slice of human-readable recommendations that were logged.
func CheckKernelAllocationHints(ctx context.Context, log log.Logger) {
	var hints []string

	if runtime.GOOS != "linux" {
		return // non-Linux: nothing to check TODO: figure out hints for other OS later
	}

	// Helpers
	readInt := func(path string) (int64, error) {
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		s := strings.TrimSpace(string(b))
		// /proc/sys values are usually small ints
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %q from %s: %w", s, path, err)
		}
		return v, nil
	}
	logHint := func(msg string, kv ...any) {
		hints = append(hints, msg)
		log.Warn(msg, kv...)
	}

	// Kernel version (useful context in logs)
	if kv, err := host.KernelVersionWithContext(ctx); err == nil && kv != "" {
		log.Info("kernel detected", "version", kv)
	}

	// 1) vm.overcommit_memory
	// 0 = heuristic (risky for fork-heavy or COW), 1 = always overcommit (recommended for Erigon-like mmap + Redis-style fork workloads), 2 = strict
	if v, err := readInt("/proc/sys/vm/overcommit_memory"); err == nil {
		switch v {
		case 0:
			logHint(
				"vm.overcommit_memory=0 can cause fork()/allocation failures under load; set to 1",
				"current", v,
				"fix", `echo "vm.overcommit_memory = 1" | sudo tee /etc/sysctl.d/99-erigon.conf && sudo sysctl -p /etc/sysctl.d/99-erigon.conf`,
			)
		case 2:
			logHint(
				"vm.overcommit_memory=2 (strict) may cause allocation failures for large mmap/fork workloads; consider 1",
				"current", v,
				"fix", `echo "vm.overcommit_memory = 1" | sudo tee /etc/sysctl.d/99-erigon.conf && sudo sysctl -p /etc/sysctl.d/99-erigon.conf`,
			)
		default:
			log.Info("vm.overcommit_memory looks OK", "current", v)
		}
	} else {
		log.Debug("unable to read vm.overcommit_memory", "err", err)
	}

	// 2) vm.max_map_count (upper bound on number of VMAs / mmaps)
	// We suggest 16777216 https://github.com/erigontech/erigon?tab=readme-ov-file#erigon-crashes-due-to-kernel-allocation-limits.
	const wantMaxMap = int64(16777216)
	if v, err := readInt("/proc/sys/vm/max_map_count"); err == nil {
		if v < wantMaxMap {
			logHint(
				"vm.max_map_count is low for large memory-mapped databases; raise it",
				"current", v,
				"recommended", wantMaxMap,
				"fix", fmt.Sprintf(`echo "vm.max_map_count = %d" | sudo tee -a /etc/sysctl.d/99-erigon.conf && sudo sysctl -p /etc/sysctl.d/99-erigon.conf`, wantMaxMap),
			)
		} else {
			log.Info("vm.max_map_count looks OK", "current", v)
		}
	} else {
		log.Debug("unable to read vm/max_map_count", "err", err)
	}

	// 3) RAM/SWAP sanity — fork-heavy workloads are sensitive when swap=0 and headroom is tiny.
	if vmStat, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		swapStat, _ := mem.SwapMemoryWithContext(ctx)

		// Heuristic: if free+cached < 15% of total and swap total == 0, warn.
		headroom := float64(vmStat.Available) / float64(vmStat.Total+1)
		if headroom < 0.15 && (swapStat == nil || swapStat.Total == 0) {
			logHint(
				"Low available memory headroom and no swap may cause fork()/allocation failures",
				"mem_total", vmStat.Total,
				"mem_available", vmStat.Available,
				"swap_total", func() uint64 {
					if swapStat != nil {
						return swapStat.Total
					}
					return 0
				}(),
				"suggestion", "Consider adding a small swapfile (e.g., 4–8 GiB) to improve fork reliability.",
				"fix", `sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab && sudo swapon -a`,
			)
		}
	} else {
		log.Debug("unable to read memory stats", "err", err)
	}

	// 4) Optional: detect containerized PID 1 (some kernels/namespaces can mask sysctl effects)
	if b, err := os.ReadFile("/proc/1/cgroup"); err == nil && strings.Contains(string(b), "docker") {
		log.Info("running under container cgroup; ensure sysctls are applied on the host or via --sysctl in the runtime")
	}

	if len(hints) == 0 {
		log.Info("kernel allocation settings look sane for mmap/fork workloads")
	}
	return
}
