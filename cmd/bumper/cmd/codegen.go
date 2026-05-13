package cmd

import (
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/db/state/statecfg"
)

var (
	codegenFile string
	codegenOut  string
)

var codegenCmd = &cobra.Command{
	Use:     "codegen",
	Short:   "Regenerate version_schema_gen.go from versions.yaml",
	Example: `go run ./cmd/bumper codegen`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return statecfg.GenerateSchemaVersions(codegenFile, codegenOut)
	},
}

func init() {
	codegenCmd.Flags().StringVar(&codegenFile, "file", "./db/state/statecfg/versions.yaml", "path to versions.yaml")
	codegenCmd.Flags().StringVar(&codegenOut, "out", "./db/state/statecfg/version_schema_gen.go", "path to output file")
	rootCmd.AddCommand(codegenCmd)
}
