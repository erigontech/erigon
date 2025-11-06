package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	defaultRPC = "http://127.0.0.0:8545"
	envRPC     = "BENCH_RPC_URL"
)

var rootCmd = &cobra.Command{
	Use:   "bench",
	Short: "Chain-agnostic bench for eth_getTransactionByHash",
	Long:  "Generates benchdata, runs latency benchmarks for eth_getTransactionByHash, and compares results.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {

}
