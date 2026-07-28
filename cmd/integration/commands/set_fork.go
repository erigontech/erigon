// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var (
	setForkRpcURL string
	setForkTarget string
)

func init() {
	setForkCmd.Flags().StringVar(&setForkRpcURL, "rpcendpoint", "http://127.0.0.1:8545", "JSON-RPC endpoint of the running erigon node")
	setForkCmd.Flags().StringVar(&setForkTarget, "chain", "", "target chain name to transition into (must be a direct parent/child of the current chain)")
	must(setForkCmd.MarkFlagRequired("chain"))
	rootCmd.AddCommand(setForkCmd)
}

var setForkCmd = &cobra.Command{
	Use:   "set_fork",
	Short: "Transition a running erigon onto a different chain via debug_setFork",
	Long: `Sends debug_setFork(<chain>) to a running erigon over JSON-RPC. The
target chain must be a direct parent/child of the currently-loaded
chain (fork children of the current chain, or the parent when
transitioning back).

The RPC unwinds to the CutBlock and swaps chain.Config in-process
across every registered captor (sentry, storage, caplin, txpool,
downloader, manifest_exchange). If RestartRequired=true in the
response, restart erigon with --chain=<chain> to complete the
transition manually.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		client, err := rpc.Dial(setForkRpcURL, logger)
		if err != nil {
			logger.Error("dial erigon RPC", "url", setForkRpcURL, "err", err)
			os.Exit(1)
		}
		defer client.Close()

		var result rpchelper.SetForkResult
		if err := client.CallContext(ctx, &result, "debug_setFork", setForkTarget); err != nil {
			logger.Error("debug_setFork call failed", "target", setForkTarget, "err", err)
			os.Exit(1)
		}

		out, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(out))
		if result.RestartRequired {
			logger.Warn("[set_fork] restart_required=true — restart erigon with --chain=" + result.ToChain + " to complete the transition")
		}
	},
}
