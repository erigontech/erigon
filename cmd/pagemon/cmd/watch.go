package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/report"
	"github.com/erigontech/erigon/cmd/pagemon/internal/sampler"
)

var watchFlags struct {
	cmd      string
	pid      int
	interval time.Duration
}

var watchCmd = &cobra.Command{
	Use:   "watch (--cmd <shell-command> | --pid <pid>) [--interval 50ms] <file>...",
	Short: "Sample page-cache residency during a command or while a PID runs",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runWatch,
}

func init() {
	watchCmd.Flags().StringVar(&watchFlags.cmd, "cmd", "", "shell command to launch and watch")
	watchCmd.Flags().IntVar(&watchFlags.pid, "pid", 0, "PID of an already-running process to watch until it exits (or Ctrl-C)")
	watchCmd.Flags().DurationVar(&watchFlags.interval, "interval", 50*time.Millisecond, "mincore sampling interval")
}

func runWatch(cmd *cobra.Command, args []string) error {
	if watchFlags.cmd == "" && watchFlags.pid == 0 {
		return fmt.Errorf("one of --cmd or --pid is required")
	}
	if watchFlags.cmd != "" && watchFlags.pid != 0 {
		return fmt.Errorf("--cmd and --pid are mutually exclusive")
	}

	// Baseline snapshot.
	before := make([][]bool, len(args))
	sizes := make([]int64, len(args))
	sampled := make([]bool, len(args))
	for i, path := range args {
		res, size, samp, err := mincore.Residency(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		before[i] = res
		sizes[i] = size
		sampled[i] = samp
	}

	// Start samplers.
	samplers := make([]*sampler.Sampler, len(args))
	for i, path := range args {
		s := sampler.New(path, watchFlags.interval)
		s.Start()
		samplers[i] = s
	}

	// Status printer: every 5s write a summary line to stderr so the user
	// can see that sampling is active.
	stopStatus := make(chan struct{})
	fmt.Fprintln(os.Stderr, "sampling started — status every 5s, report on exit")
	go printStatus(stopStatus, args, before, samplers)

	var (
		cmdLabel string
		runErr   error
		dur      time.Duration
	)

	t0 := time.Now()
	if watchFlags.cmd != "" {
		cmdLabel = watchFlags.cmd
		c := exec.Command("sh", "-c", watchFlags.cmd) //nolint:gosec
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		runErr = c.Run()
	} else {
		cmdLabel = fmt.Sprintf("pid %d", watchFlags.pid)
		runErr = waitForPID(watchFlags.pid)
	}
	dur = time.Since(t0)
	close(stopStatus)

	// Stop samplers.
	allSnaps := make([][]sampler.Snapshot, len(args))
	for i, s := range samplers {
		allSnaps[i] = s.Stop()
	}

	if runErr != nil {
		return fmt.Errorf("watch ended: %w", runErr)
	}

	// Build delta results.
	var results []report.FileResult
	for i, path := range args {
		var finalRes []bool
		if n := len(allSnaps[i]); n > 0 {
			finalRes = allSnaps[i][n-1].Residency
		} else {
			finalRes, _, _, _ = mincore.Residency(path) //nolint:errcheck
		}
		delta := deltaResidency(before[i], finalRes)
		results = append(results, buildResult(path, sizes[i], delta, sampled[i], allSnaps[i]))
	}

	report.WriteWatch(os.Stdout, report.MeasureHeader{
		Command:  cmdLabel,
		Duration: dur,
	}, results)
	return nil
}

// waitForPID blocks until the process exits or the user sends SIGINT/SIGTERM.
// Returns nil when the process exits naturally, or an error explaining why we stopped.
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
				// Process no longer exists.
				return nil
			}
		}
	}
}

// printStatus writes a one-line status to stderr every 5s while sampling.
// Shows pages newly loaded since the baseline for each file.
func printStatus(stop <-chan struct{}, paths []string, baseline [][]bool, samplers []*sampler.Sampler) {
	ps := mincore.PageSize()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	t0 := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			elapsed := time.Since(t0).Round(time.Second)
			for i, s := range samplers {
				snap := s.Latest()
				if snap.Residency == nil {
					continue
				}
				newPages := int64(0)
				base := baseline[i]
				for j, in := range snap.Residency {
					if in && (j >= len(base) || !base[j]) {
						newPages++
					}
				}
				fmt.Fprintf(os.Stderr, "  [%s] %s: +%d pages new (+%s loaded)\n",
					elapsed, filepath.Base(paths[i]), newPages, report.HumanBytes(newPages*ps))
			}
		}
	}
}
