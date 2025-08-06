package cmd

import (
	"fmt"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/spf13/cobra"
)

var (
	datadir        string
	includeDomains []string
	includeExts    []string
	excludeDomains []string
	excludeExts    []string
)

var renameCmd = &cobra.Command{
	Use:   "rename",
	Short: "Rename versioned files to match schema versions",
	RunE: func(cmd *cobra.Command, args []string) error {
		if datadir == "" {
			return fmt.Errorf("--datadir flag is required")
		}
		p := tea.NewProgram(NewSelectorModel(includeDomains, includeExts, excludeDomains, excludeExts), tea.WithAltScreen())
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

		// collect rename operations
		var changedFiles []string
		// TODO: implement file walking and renaming, appending to changedFiles

		if len(changedFiles) > 0 {
			fmt.Println("Renamed files:")
			for _, f := range changedFiles {
				fmt.Printf(" - %s\n", f)
			}
		} else {
			fmt.Println("No files were renamed.")
		}
		return nil
	},
}

func init() {
	renameCmd.Flags().StringVar(&datadir, "datadir", "", "Directory containing versioned files")
	renameCmd.Flags().StringSliceVar(&includeDomains, "include-domains", []string{}, "Domains to include (default: all)")
	renameCmd.Flags().StringSliceVar(&excludeDomains, "exclude-domains", []string{}, "Domains to exclude")
	renameCmd.Flags().StringSliceVar(&includeExts, "include-exts", []string{}, "Extensions to include (default: all)")
	renameCmd.Flags().StringSliceVar(&excludeExts, "exclude-exts", []string{}, "Extensions to exclude")
}

func renameFiles(domains []string, exts []string) error {
	renameVerMap := make(map[string]string)
	for _, dString := range domains {
		d, err := kv.String2Domain(dString)
		if err == nil {
			state.Schema.GetDomainCfg(d).GetVersions().Domain.DataKV.Current
			state.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorBT.Current
			state.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorKVI.Current
			state.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorKVEI.Current
			state.Schema.GetDomainCfg(d).GetVersions().Hist.DataV.Current
			state.Schema.GetDomainCfg(d).GetVersions().Hist.AccessorVI.Current
		} else {
			ii, _ := kv.String2InvertedIdx(dString)
			state.Schema.GetIICfg(ii).GetVersions().II.DataEF.Current
			state.Schema.GetIICfg(ii).GetVersions().II.AccessorEFI.Current
		}
	}

	state.Schema.GetDomainCfg()
}
