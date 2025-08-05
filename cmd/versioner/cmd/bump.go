package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

var domain string
var facet string
var newVersion string

var bumpCmd = &cobra.Command{
	Use:   "bump",
	Short: "Bump a schema version in code",
	RunE: func(cmd *cobra.Command, args []string) error {
		if domain == "" || facet == "" || newVersion == "" {
			return fmt.Errorf("--domain, --facet and --to flags are required")
		}
		fmt.Printf("Bumping %s.%s to %s\n", domain, facet, newVersion)
		// TODO: parse version_schema.go AST, update the constant
		return nil
	},
}

func init() {
	bumpCmd.Flags().StringVar(&domain, "domain", "", "Domain name (e.g. accounts)")
	bumpCmd.Flags().StringVar(&facet, "facet", "", "Facet/key to bump (e.g. data-kv)")
	bumpCmd.Flags().StringVar(&newVersion, "to", "", "New version (e.g. 2.0.0-standard)")
}
