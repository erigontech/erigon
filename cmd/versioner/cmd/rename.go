package cmd

import (
	"fmt"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

var datadir string
var excludeDomains []string
var excludeExts []string

var renameCmd = &cobra.Command{
	Use:   "rename",
	Short: "Rename versioned files to match schema versions",
	RunE: func(cmd *cobra.Command, args []string) error {
		if datadir == "" {
			return fmt.Errorf("--datadir flag is required")
		}
		p := tea.NewProgram(NewSelectorModel(excludeDomains, excludeExts), tea.WithAltScreen())
		finalModel, err := p.Run()
		if err != nil {
			return err
		}
		sel := finalModel.(*SelectorModel)
		if sel.canceled {
			fmt.Println("Action cancelled by user.")
			return nil
		}
		domains, exts := sel.GetSelection()
		fmt.Printf("Renaming in %s, selected domains: %v, extensions: %v\n", datadir, domains, exts)
		// TODO: walk files under datadir, parse internal version, and rename accordingly
		return nil
	},
}

func init() {
	renameCmd.Flags().StringVar(&datadir, "datadir", "", "Directory containing versioned files")
	renameCmd.Flags().StringSliceVar(&excludeDomains, "exclude-domains", []string{}, "Domains to skip")
	renameCmd.Flags().StringSliceVar(&excludeExts, "exclude-exts", []string{}, "Extensions to skip (e.g. .efi)")
}
