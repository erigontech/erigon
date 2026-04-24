package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/pagemon/internal/mincore"
	"github.com/erigontech/erigon/cmd/pagemon/internal/report"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot <file>...",
	Short: "Show current page-cache residency for one or more files",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runSnapshot,
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	var results []report.FileResult
	for _, path := range args {
		res, size, sampled, err := mincore.Residency(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		results = append(results, buildResult(path, size, res, sampled, nil))
	}
	report.WriteSnapshot(os.Stdout, results)
	return nil
}
