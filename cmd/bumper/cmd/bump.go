package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"

	"github.com/erigontech/erigon/cmd/bumper/internal/tui"
)

var bumpCmd = &cobra.Command{
	Use:   "bump",
	Short: "Edit versions.yaml in TUI and regenerate code",
	RunE: func(cmd *cobra.Command, args []string) error {
		file := "./db/state/versions.yaml"
		out := "./db/state/version_schema_gen.go"

		if err := tui.Run(file); err != nil {
			return fmt.Errorf("tui: %w", err)
		}
		log.Info("started generating:")
		return statecfg.GenerateSchemaVersions(file, out)
	},
}
