package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/erigontech/erigon/cmd/txnbench/internal/bench"
	"os"

	"github.com/spf13/cobra"
)

var compareCmd = &cobra.Command{
	Use:   "compare <old.json> <new.json>",
	Short: "Compare two bench result JSON files (benchstat-like output)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return errors.New("provide exactly two files: old.json new.json")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		oldFile, newFile := args[0], args[1]

		var oldRes, newRes bench.BenchOutput
		{
			f, err := os.Open(oldFile)
			if err != nil {
				return err
			}
			defer f.Close()
			if err := json.NewDecoder(f).Decode(&oldRes); err != nil {
				return err
			}
		}
		{
			f, err := os.Open(newFile)
			if err != nil {
				return err
			}
			defer f.Close()
			if err := json.NewDecoder(f).Decode(&newRes); err != nil {
				return err
			}
		}

		report := bench.CompareResults(oldRes, newRes)
		fmt.Println(report)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)
}
