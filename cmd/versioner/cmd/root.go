package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "schema-tool",
	Short: "Manage schema versions and file renaming",
	Long: `schema-tool is a CLI to:
 1) Rename files with version mismatches
 2) Bump schema versions in code
 3) Inspect schema fields via reflection
`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(renameCmd)
	rootCmd.AddCommand(bumpCmd)
	rootCmd.AddCommand(inspectCmd)
}
