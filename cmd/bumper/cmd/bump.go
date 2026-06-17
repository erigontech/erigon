package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/statecfg"

	"github.com/erigontech/erigon/cmd/bumper/internal/schema"
	"github.com/erigontech/erigon/cmd/bumper/internal/tui"
)

var (
	bumpFile    string
	bumpOut     string
	bumpDomains []string
	bumpExts    []string
	bumpMajor   bool
	bumpMinor   bool
	bumpDryRun  bool
)

var bumpCmd = &cobra.Command{
	Use:   "bump",
	Short: "Bump versions of files in the erigon schema and regenerate code",
	Long: `bump edits db/state/statecfg/versions.yaml and regenerates version_schema_gen.go.

With no selection flags it opens the interactive TUI. With --major/--minor it runs
non-interactively, bumping every entry matching --domain/--ext.`,
	Example: `  # interactive
  go run ./cmd/bumper bump

  # one commitment .kv minor bump
  go run ./cmd/bumper bump --domain commitment --ext kv --minor

  # major-bump every inverted-index .ef across all domains
  go run ./cmd/bumper bump --ext ef --major

  # preview without writing
  go run ./cmd/bumper bump --ext ef --major --dry-run`,
	RunE: func(cmd *cobra.Command, args []string) error {
		interactive := !bumpMajor && !bumpMinor && len(bumpDomains) == 0 && len(bumpExts) == 0
		if interactive {
			if err := tui.Run(bumpFile); err != nil {
				return fmt.Errorf("tui: %w", err)
			}
			log.Info("regenerating schema")
			return statecfg.GenerateSchemaVersions(bumpFile, bumpOut)
		}
		return runBump()
	},
}

func runBump() error {
	if bumpMajor == bumpMinor {
		return fmt.Errorf("specify exactly one of --major or --minor")
	}
	kind := schema.Minor
	if bumpMajor {
		kind = schema.Major
	}

	s, err := schema.Load(bumpFile)
	if err != nil {
		return err
	}

	changes, err := schema.Bump(s, schema.Selector{Cats: bumpDomains, Keys: bumpExts}, kind)
	if err != nil {
		return err
	}

	for _, c := range changes {
		fmt.Printf("%-14s %-6s %-4s %s -> %s\n", c.Cat, c.Part, c.Key, c.From, c.To)
	}

	if bumpDryRun {
		fmt.Printf("dry-run: %d entr%s would change, nothing written\n", len(changes), plural(len(changes)))
		return nil
	}

	if err := schema.Save(bumpFile, s); err != nil {
		return err
	}
	if err := statecfg.GenerateSchemaVersions(bumpFile, bumpOut); err != nil {
		return err
	}
	fmt.Printf("bumped %d entr%s; regenerated %s\n", len(changes), plural(len(changes)), bumpOut)
	return nil
}

func plural(n int) string {
	if n == 1 {
		return "y"
	}
	return "ies"
}

func init() {
	bumpCmd.Flags().StringVar(&bumpFile, "file", "./db/state/statecfg/versions.yaml", "path to versions.yaml")
	bumpCmd.Flags().StringVar(&bumpOut, "out", "./db/state/statecfg/version_schema_gen.go", "path to generated output")
	bumpCmd.Flags().StringSliceVarP(&bumpDomains, "domain", "d", nil, "domains to bump (default: all), e.g. commitment,receipt")
	bumpCmd.Flags().StringSliceVarP(&bumpExts, "ext", "e", nil, "file extensions/keys to bump (default: all), e.g. kv,ef")
	bumpCmd.Flags().BoolVar(&bumpMajor, "major", false, "major bump (old Erigon can't read new files); resets minor to 0")
	bumpCmd.Flags().BoolVar(&bumpMinor, "minor", false, "minor bump (content change still readable by the current reader)")
	bumpCmd.Flags().BoolVar(&bumpDryRun, "dry-run", false, "print the changes without writing files")
}
