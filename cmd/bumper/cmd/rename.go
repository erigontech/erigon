package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"

	datadir2 "github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

var (
	datadir        string
	includeDomains []string
	includeExts    []string
	excludeDomains []string
	excludeExts    []string
)

var renameCmd = &cobra.Command{
	Use:     "rename",
	Short:   "Rename versioned files to match schema versions",
	Example: `To start rename TUI in the datadir: go run ./cmd/bumper rename --datadir /path/to/your/datadir`,
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
		changedFiles, err := renameFiles(domains, exts, datadir2.New(datadir))
		if err != nil {
			return err
		}
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

type fileSmallMapping struct {
	name uint16
	ext  string
}

func renameFiles(domains []string, exts []string, dirs datadir2.Dirs) ([]string, error) {
	renameVerMap := make(map[fileSmallMapping]snaptype.Version)
	for _, dString := range domains {
		d, err := kv.String2Domain(dString)
		if err == nil {
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".kv",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Domain.DataKV.Current
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".bt",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorBT.Current
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".kvi",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorKVI.Current
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".kvei",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Domain.AccessorKVEI.Current
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".v",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Hist.DataV.Current
			renameVerMap[fileSmallMapping{
				name: uint16(d),
				ext:  ".vi",
			}] = statecfg.Schema.GetDomainCfg(d).GetVersions().Hist.AccessorVI.Current
		} else {
			ii, _ := kv.String2InvertedIdx(dString)
			renameVerMap[fileSmallMapping{
				name: uint16(ii),
				ext:  ".ef",
			}] = statecfg.Schema.GetIICfg(ii).GetVersions().II.DataEF.Current
			renameVerMap[fileSmallMapping{
				name: uint16(ii),
				ext:  ".efi",
			}] = statecfg.Schema.GetIICfg(ii).GetVersions().II.AccessorEFI.Current
		}
	}
	changedFiles := make([]string, 0)
	extForRenameMap := make(map[string]struct{})
	for _, e := range exts {
		extForRenameMap[e] = struct{}{}
	}
	domainsForRenameMap := make(map[uint16]struct{})
	for _, d := range domains {
		dEnum, err := kv.String2Enum(d)
		if err != nil {
			return nil, err
		}
		domainsForRenameMap[dEnum] = struct{}{}
	}
	if err := filepath.WalkDir(dirs.Snap, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if _, ok := extForRenameMap[filepath.Ext(path)]; !ok {
			return nil
		}

		// Call the internal rename function on each file
		dir, fName := filepath.Split(path)
		f, _, ok := snaptype.ParseFileName(dir, fName)
		if !ok {
			return nil
		}
		dEnum, err := kv.String2Enum(f.TypeString)
		if err != nil {
			return err
		}
		if _, okEnum := domainsForRenameMap[dEnum]; !okEnum {
			return nil
		}
		newVer := renameVerMap[fileSmallMapping{
			name: dEnum,
			ext:  f.Ext,
		}]
		if !f.Version.Eq(newVer) {
			newFileName := version.ReplaceVersion(path, f.Version, newVer)
			if err := os.Rename(path, newFileName); err != nil {
				return fmt.Errorf("failed to rename %s: %w", path, err)
			}
			changedFiles = append(changedFiles, newFileName)
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("error walking directory %s: %w", dirs.Snap, err)
	}

	return changedFiles, nil
}
