package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/report"
	"github.com/erigontech/erigon/cmd/pagemon/internal/sampler"
)

var watchFlags struct {
	cmd          string
	pid          int
	interval     time.Duration
	logicalBytes int64
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
	watchCmd.Flags().Int64Var(&watchFlags.logicalBytes, "logical-bytes", 0, "bytes the workload logically needs (enables read-amplification line in report)")
}

func runWatch(cmd *cobra.Command, args []string) error {
	if err := validateCmdPid(watchFlags.cmd, watchFlags.pid); err != nil {
		return err
	}

	bases, err := takeBaseline(args)
	if err != nil {
		return err
	}

	// Start samplers.
	samplers := make([]*sampler.Sampler, len(args))
	for i, path := range args {
		s := sampler.New(path, watchFlags.interval, bases[i].residency)
		s.Start()
		samplers[i] = s
	}

	// Status printer: every 5s write a summary line to stderr so the user
	// can see that sampling is active.
	stopStatus := make(chan struct{})
	fmt.Fprintln(os.Stderr, "sampling started — status every 5s, report on exit")
	go printStatus(stopStatus, args, samplers)

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

	// Build and print the report regardless of how the watch ended.
	// Ctrl-C on a --pid watch is normal usage, not a failure.
	var results []report.FileResult
	for i, path := range args {
		var finalRes []bool
		if n := len(allSnaps[i]); n > 0 {
			finalRes = allSnaps[i][n-1].Residency
		} else {
			finalRes, _, _, _ = mincore.Residency(path) //nolint:errcheck
		}
		delta := deltaResidency(bases[i].residency, finalRes)
		results = append(results, buildResult(path, bases[i].fileSize, delta, bases[i].sampled, allSnaps[i], watchFlags.logicalBytes))
	}

	report.WriteWatch(os.Stdout, report.MeasureHeader{
		Command:  cmdLabel,
		Duration: dur,
	}, results)

	if runErr != nil && !isInterrupt(runErr) {
		return fmt.Errorf("watch ended: %w", runErr)
	}
	return nil
}

// printStatus writes a one-line status to stderr every 5s while sampling.
func printStatus(stop <-chan struct{}, paths []string, samplers []*sampler.Sampler) {
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
				fmt.Fprintf(os.Stderr, "  [%s] %s: +%d pages new (+%s loaded)\n",
					elapsed, filepath.Base(paths[i]), snap.NewPages, report.HumanBytes(snap.NewPages*ps))
			}
		}
	}
}
