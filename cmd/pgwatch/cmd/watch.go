package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pgwatch/internal/mincore"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/report"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/sampler"
)

var watchFlags struct {
	cmd      string
	interval time.Duration
}

var watchCmd = &cobra.Command{
	Use:   "watch --cmd <shell-command> [--interval 50ms] <file>...",
	Short: "Sample page-cache residency during a command and report temporal phases",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runWatch,
}

func init() {
	watchCmd.Flags().StringVar(&watchFlags.cmd, "cmd", "", "shell command to run (required)")
	watchCmd.Flags().DurationVar(&watchFlags.interval, "interval", 50*time.Millisecond, "mincore sampling interval")
	_ = watchCmd.MarkFlagRequired("cmd")
}

func runWatch(cmd *cobra.Command, args []string) error {
	// Take a "before" baseline so we can compute delta residency.
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

	// Run command.
	t0 := time.Now()
	c := exec.Command("sh", "-c", watchFlags.cmd) //nolint:gosec
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	runErr := c.Run()
	dur := time.Since(t0)

	// Stop samplers and collect snapshots.
	allSnaps := make([][]sampler.Snapshot, len(args))
	for i, s := range samplers {
		allSnaps[i] = s.Stop()
	}

	if runErr != nil {
		return fmt.Errorf("command failed: %w", runErr)
	}

	// Build delta results with temporal data.
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
		Command:  watchFlags.cmd,
		Duration: dur,
	}, results)
	return nil
}
