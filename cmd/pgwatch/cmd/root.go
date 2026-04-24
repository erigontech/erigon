package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pgwatch",
	Short: "Page-cache working-set analyser for database files",
	Long: `pgwatch measures which pages of a file are resident in the Linux page cache
before and after a command runs, then classifies the access pattern and reports
cluster structure to help identify data-colocation opportunities.

Requires Linux (mincore syscall).`,
}

// Execute is the entry point called from main.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(measureCmd)
	rootCmd.AddCommand(watchCmd)
	rootCmd.AddCommand(diffCmd)
}
