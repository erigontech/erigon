package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/erigontech/erigon/cmd/pagemon/internal/cluster"
	"github.com/erigontech/erigon/cmd/pagemon/internal/metrics"
	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/pattern"
	"github.com/erigontech/erigon/cmd/pagemon/internal/report"
	"github.com/erigontech/erigon/cmd/pagemon/internal/sampler"
)

// fileInfo holds a baseline mincore snapshot for one file.
type fileInfo struct {
	residency []bool
	fileSize  int64
	sampled   bool
}

// validateCmdPid checks that exactly one of cmd/pid is set.
func validateCmdPid(cmd string, pid int) error {
	if cmd == "" && pid == 0 {
		return fmt.Errorf("one of --cmd or --pid is required")
	}
	if cmd != "" && pid != 0 {
		return fmt.Errorf("--cmd and --pid are mutually exclusive")
	}
	return nil
}

// takeBaseline snapshots current page residency for each path.
func takeBaseline(paths []string) ([]fileInfo, error) {
	infos := make([]fileInfo, len(paths))
	for i, path := range paths {
		res, size, samp, err := mincore.Residency(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		infos[i] = fileInfo{residency: res, fileSize: size, sampled: samp}
	}
	return infos, nil
}

// waitForPID blocks until the process exits or the user sends SIGINT/SIGTERM.
func waitForPID(pid int) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)

	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("pid %d not found: %w", pid, err)
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case s := <-sig:
			return fmt.Errorf("interrupted by %s", s)
		case <-ticker.C:
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				return nil
			}
		}
	}
}

// buildResult derives a FileResult from a residency bitmap and optional snapshots.
// logicalBytes is the byte count the workload logically needed; 0 means unknown.
func buildResult(path string, fileSize int64, residency []bool, sampled bool, snaps []sampler.Snapshot, logicalBytes int64) report.FileResult {
	ps := mincore.PageSize()
	m := metrics.Compute(residency, ps)
	clusters := cluster.Detect(residency, ps, cluster.DefaultGapThreshold)
	gaps := cluster.InterGaps(clusters, ps)
	pat := pattern.Classify(m, clusters, snaps)

	return report.FileResult{
		Path:         path,
		FileSize:     fileSize,
		Sampled:      sampled,
		LogicalBytes: logicalBytes,
		Residency:    residency,
		Metrics:      m,
		Clusters:     clusters,
		Gaps:         gaps,
		Pattern:      pat,
		Snapshots:    snaps,
	}
}

// deltaResidency returns a bitmap of pages present in after but not in before.
func deltaResidency(before, after []bool) []bool {
	n := len(after)
	if len(before) < n {
		n = len(before)
	}
	delta := make([]bool, len(after))
	for i := 0; i < n; i++ {
		delta[i] = after[i] && !before[i]
	}
	for i := n; i < len(after); i++ {
		delta[i] = after[i]
	}
	return delta
}
