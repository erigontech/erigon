package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "versioner",
	Short: "Manage schema versions and file renaming",
	Long: `versioner is a CLI to:
 1) Rename files containing new versions but named with old versions
 2) Bump schema versions in code during PR prep
`,
}

// Execute adds all child commands to the root and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Add subcommands
	rootCmd.AddCommand(renameCmd)
	rootCmd.AddCommand(bumpCmd)
}
