package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/erigontech/erigon/cmd/txnbench/internal/bench"
	"github.com/erigontech/erigon/cmd/txnbench/internal/rpcclient"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run <name>",
	Short: "Run benchmark against node using benchdata.toml and write results to <name>.json",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errors.New("provide exactly one argument: name")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		rpcURL := os.Getenv(envRPC)
		if rpcURL == "" {
			rpcURL = defaultRPC
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		client := rpcclient.New(rpcURL, 30*time.Second)

		result, err := bench.RunBenchmark(ctx, client, name)
		if err != nil {
			return err
		}

		out := name + ".json"
		f, err := os.Create(out)
		if err != nil {
			return fmt.Errorf("create %s: %w", out, err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}

		fmt.Printf("bench results written to %s\n", out)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
