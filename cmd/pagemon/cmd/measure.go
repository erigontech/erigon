package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/report"
)

var measureFlags struct {
	cmd    string
	pid    int
	noDrop bool
}

var measureCmd = &cobra.Command{
	Use:   "measure (--cmd <shell-command> | --pid <pid>) [--no-drop] <file>...",
	Short: "Report pages newly loaded into cache by a command or a running PID",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runMeasure,
}

func init() {
	measureCmd.Flags().StringVar(&measureFlags.cmd, "cmd", "", "shell command to run")
	measureCmd.Flags().IntVar(&measureFlags.pid, "pid", 0, "PID of an already-running process to watch until it exits (or Ctrl-C)")
	measureCmd.Flags().BoolVar(&measureFlags.noDrop, "no-drop", false, "skip drop_caches (always implied with --pid)")
}

func runMeasure(cmd *cobra.Command, args []string) error {
	if measureFlags.cmd == "" && measureFlags.pid == 0 {
		return fmt.Errorf("one of --cmd or --pid is required")
	}
	if measureFlags.cmd != "" && measureFlags.pid != 0 {
		return fmt.Errorf("--cmd and --pid are mutually exclusive")
	}

	// Drop caches only when launching our own command and --no-drop is not set.
	if measureFlags.pid == 0 && !measureFlags.noDrop {
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

	// Run command or wait for PID.
	var cmdLabel string
	t0 := time.Now()
	if measureFlags.cmd != "" {
		cmdLabel = measureFlags.cmd
		c := exec.Command("sh", "-c", measureFlags.cmd) //nolint:gosec
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Run(); err != nil {
			return fmt.Errorf("command failed: %w", err)
		}
	} else {
		cmdLabel = fmt.Sprintf("pid %d", measureFlags.pid)
		if err := waitForPID(measureFlags.pid); err != nil {
			return fmt.Errorf("watch ended: %w", err)
		}
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
		Command:  cmdLabel,
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
