package cmd

import (
	"context"
	"fmt"
	"github.com/erigontech/erigon/cmd/txnbench/internal/bench"
	"github.com/erigontech/erigon/cmd/txnbench/internal/rpcclient"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
)

var genCmd = &cobra.Command{
	Use:   "gen",
	Short: "Generate benchdata.toml with tx hashes at 0.25, 0.50, 0.75 of chain height",
	RunE: func(cmd *cobra.Command, args []string) error {
		rpcURL := os.Getenv(envRPC)
		if rpcURL == "" {
			rpcURL = defaultRPC
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		client := rpcclient.New(rpcURL, 30*time.Second)
		data, err := bench.GenerateBenchData(ctx, client)
		if err != nil {
			return err
		}

		fname := "benchdata.toml"
		f, err := os.Create(fname)
		if err != nil {
			return fmt.Errorf("create %s: %w", fname, err)
		}
		defer f.Close()

		enc := toml.NewEncoder(f)
		if err := enc.Encode(data); err != nil {
			return fmt.Errorf("encode toml: %w", err)
		}

		fmt.Printf("benchdata written to %s\n", fname)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(genCmd)
}
