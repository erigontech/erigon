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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var (
	setForkRpcURL         string
	setForkTarget         string
	setForkTimeoutSec     int
	setForkAuthorityUCAN  string
	setForkAuthorityFile  string
)

func init() {
	setForkCmd.Flags().StringVar(&setForkRpcURL, "rpcendpoint", "http://127.0.0.1:8545", "JSON-RPC endpoint of the running erigon node")
	setForkCmd.Flags().StringVar(&setForkTarget, "chain", "", "target chain name to transition into (must be a direct parent/child of the current chain)")
	setForkCmd.Flags().IntVar(&setForkTimeoutSec, "timeout-sec", 1800, "HTTP timeout for the debug_setFork call (SetHead unwind + captor swap can take >30s)")
	setForkCmd.Flags().StringVar(&setForkAuthorityUCAN, "authority-ucan", "", "base64-encoded fork-transition UCAN (mutually exclusive with --authority-ucan-file)")
	setForkCmd.Flags().StringVar(&setForkAuthorityFile, "authority-ucan-file", "", "path to a file containing the base64-encoded fork-transition UCAN")
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

		authorityUCAN, err := loadAuthorityUCAN(setForkAuthorityUCAN, setForkAuthorityFile)
		if err != nil {
			logger.Error("resolve authority UCAN", "err", err)
			os.Exit(1)
		}

		client, err := dialWithTimeout(setForkRpcURL, time.Duration(setForkTimeoutSec)*time.Second, logger)
		if err != nil {
			logger.Error("dial erigon RPC", "url", setForkRpcURL, "err", err)
			os.Exit(1)
		}
		defer client.Close()

		var result rpchelper.SetForkResult
		if err := client.CallContext(ctx, &result, "debug_setFork", setForkTarget, authorityUCAN); err != nil {
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

// dialWithTimeout builds an rpc.Client whose underlying http.Client
// uses `timeout` for the whole request round-trip. The default
// rpc.Dial hard-codes 30s (rpc/http.go:118) which is too short for
// debug_setFork — a mode-B unwind + captor swap can take minutes.
func dialWithTimeout(url string, timeout time.Duration, logger log.Logger) (*rpc.Client, error) {
	httpClient := &http.Client{Timeout: timeout}
	return rpc.DialHTTPWithClient(url, httpClient, logger)
}

// loadAuthorityUCAN resolves the caller-provided UCAN. Exactly one of
// --authority-ucan or --authority-ucan-file must be supplied; the file
// form is preferred for shell hygiene (avoids the token appearing in
// process listings and shell history).
func loadAuthorityUCAN(inline, file string) (string, error) {
	inline = strings.TrimSpace(inline)
	file = strings.TrimSpace(file)
	switch {
	case inline != "" && file != "":
		return "", fmt.Errorf("--authority-ucan and --authority-ucan-file are mutually exclusive")
	case inline != "":
		return inline, nil
	case file != "":
		raw, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", file, err)
		}
		return strings.TrimSpace(string(raw)), nil
	default:
		return "", fmt.Errorf("one of --authority-ucan or --authority-ucan-file is required")
	}
}
