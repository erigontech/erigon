package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pgwatch/internal/mincore"
	"github.com/erigontech/erigon/cmd/pgwatch/internal/report"
)

var measureFlags struct {
	cmd    string
	noDrop bool
}

var measureCmd = &cobra.Command{
	Use:   "measure --cmd <shell-command> [--no-drop] <file>...",
	Short: "Drop caches, run a command, then report newly loaded pages",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runMeasure,
}

func init() {
	measureCmd.Flags().StringVar(&measureFlags.cmd, "cmd", "", "shell command to run (required)")
	measureCmd.Flags().BoolVar(&measureFlags.noDrop, "no-drop", false, "skip drop_caches (observational/production mode)")
	_ = measureCmd.MarkFlagRequired("cmd")
}

func runMeasure(cmd *cobra.Command, args []string) error {
	if !measureFlags.noDrop {
		if err := dropCaches(); err != nil {
			return fmt.Errorf("drop_caches: %w (use --no-drop for production systems)", err)
		}
	}

	// Snapshot before.
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

	// Run command.
	t0 := time.Now()
	c := exec.Command("sh", "-c", measureFlags.cmd) //nolint:gosec
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	dur := time.Since(t0)

	// Snapshot after and compute delta.
	var results []report.FileResult
	for i, path := range args {
		after, _, _, err := mincore.Residency(path)
		if err != nil {
			return fmt.Errorf("%s (after): %w", path, err)
		}
		delta := deltaResidency(before[i], after)
		results = append(results, buildResult(path, sizes[i], delta, sampled[i], nil))
	}

	report.WriteMeasure(os.Stdout, report.MeasureHeader{
		Command:  measureFlags.cmd,
		Duration: dur,
	}, results)
	return nil
}

// dropCaches writes "3" to /proc/sys/vm/drop_caches to flush the page cache.
func dropCaches() error {
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString("3\n")
	return err
}
