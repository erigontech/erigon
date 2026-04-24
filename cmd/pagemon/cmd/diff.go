package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff <before-snapshot> <after-snapshot>",
	Short: "Compare two saved snapshots (Phase 2 — not yet implemented)",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("diff is a Phase 2 feature; use --out on snapshot/measure to save snapshots, then run diff")
	},
}
