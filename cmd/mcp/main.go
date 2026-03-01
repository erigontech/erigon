// Copyright 2024 The Erigon Authors
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

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/logging"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	mcpserver "github.com/erigontech/erigon/rpc/mcp"
)

// defaultRPCPorts lists well-known Erigon / geth JSON-RPC ports to probe
// during auto-discovery.
var defaultRPCPorts = []uint{8545, 8546, 8547}

func main() {
	var (
		rpcURL    string
		port      uint
		dataDir   string
		transport string
		sseAddr   string
		logDir    string
		privAPI   string
	)

	rootCmd := &cobra.Command{
		Use:   "mcp",
		Short: "Standalone MCP server for Erigon",
		Long: `MCP (Model Context Protocol) server for Erigon.

Three connection modes (in priority order):

  1. JSON-RPC proxy (--rpc.url or --port)
     Connects to a running Erigon node via HTTP JSON-RPC.
       mcp --rpc.url http://127.0.0.1:8545
       mcp --port 8545

  2. Direct datadir (--datadir, with optional --private.api.addr)
     Opens Erigon's MDBX database directly (read-only), like an
     external rpcdaemon. Requires no running Erigon if using gRPC
     private API, or opens DB if datadir is provided.
       mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

  3. Auto-discovery (no flags)
     Probes localhost ports 8545-8547 for a running JSON-RPC endpoint.

Transports:
  --transport stdio   (default) Read/write MCP protocol on stdin/stdout.
  --transport sse     Serve over HTTP with Server-Sent Events.

Examples:
  # Claude Desktop config (stdio, auto-discovery):
  mcp

  # Point at running Erigon:
  mcp --port 8545

  # Direct DB access (offline):
  mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

  # SSE transport:
  mcp --port 8545 --transport sse --sse.addr 127.0.0.1:8553`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := debug.SetupCobra(cmd, "mcp")

			// Determine log directory.
			if logDir == "" && dataDir != "" {
				logDir = filepath.Join(dataDir, "logs")
			}

			ctx := cmd.Context()

			// --- Mode 1: Direct datadir (rpcdaemon-style) ---
			if dataDir != "" && !cmd.Flags().Changed("rpc.url") && port == 0 {
				err := runDatadirMode(ctx, logger, dataDir, privAPI, logDir, transport, sseAddr)
				if err != nil {
					return fmt.Errorf("datadir mode failed: %w (use --rpc.url to connect via JSON-RPC instead)", err)
				}
				return nil
			}

			// --- Mode 2/3: JSON-RPC proxy ---
			url := rpcURL
			if port > 0 {
				url = fmt.Sprintf("http://127.0.0.1:%d", port)
			} else {
				// Auto-discover: probe default ports.
				if discovered := autoDiscover(logger); discovered != "" {
					url = discovered
				}
			}

			client, err := rpc.Dial(url, logger)
			if err != nil {
				return fmt.Errorf("failed to connect to Erigon at %s: %w", url, err)
			}
			defer client.Close()

			// Verify connectivity (non-fatal).
			var blockNum string
			if err := client.CallContext(ctx, &blockNum, "eth_blockNumber"); err != nil {
				logger.Warn("[MCP] Could not reach Erigon RPC — starting anyway", "url", url, "err", err)
			} else {
				logger.Info("[MCP] Connected to Erigon", "url", url, "block", blockNum)
			}

			srv := mcpserver.NewStandaloneMCPServer(client, logDir)
			return serve(ctx, srv, transport, sseAddr, logger)
		},
	}

	// Register debug, logging, and metric flags required by debug.SetupCobra().
	// This mirrors what rpcdaemon does in cli.RootCommand().
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	rootCmd.Flags().StringVar(&rpcURL, "rpc.url", "http://127.0.0.1:8545", "Erigon JSON-RPC endpoint URL")
	rootCmd.Flags().UintVar(&port, "port", 0, "Erigon JSON-RPC port (shorthand for --rpc.url=http://127.0.0.1:{port})")
	rootCmd.Flags().StringVar(&dataDir, "datadir", "", "Erigon data directory (enables direct DB access mode)")
	rootCmd.Flags().StringVar(&privAPI, "private.api.addr", "127.0.0.1:9090", "Erigon gRPC private API address (used with --datadir)")
	rootCmd.Flags().StringVar(&transport, "transport", "stdio", "MCP transport: 'stdio' or 'sse'")
	rootCmd.Flags().StringVar(&sseAddr, "sse.addr", "127.0.0.1:8553", "SSE server listen address (when transport=sse)")
	rootCmd.Flags().StringVar(&logDir, "log.dir", "", "Erigon log directory (overrides datadir-based detection)")

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		rootCancel()
	}()

	if err := rootCmd.ExecuteContext(rootCtx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// serve starts the MCP server in the chosen transport mode.
func serve(ctx context.Context, srv mcpserver.MCPTransport, transport, sseAddr string, logger log.Logger) error {
	switch transport {
	case "stdio":
		logger.Info("[MCP] Starting stdio transport")
		return srv.ServeContext(ctx)
	case "sse":
		logger.Info("[MCP] Starting SSE transport", "addr", sseAddr)
		return srv.ServeSSE(sseAddr)
	default:
		return fmt.Errorf("unknown transport: %s (use 'stdio' or 'sse')", transport)
	}
}

// autoDiscover probes localhost on well-known JSON-RPC ports.
func autoDiscover(logger log.Logger) string {
	logger.Info("[MCP] Auto-discovering Erigon JSON-RPC endpoint...")
	for _, p := range defaultRPCPorts {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			url := fmt.Sprintf("http://%s", addr)
			logger.Info("[MCP] Discovered Erigon endpoint", "url", url)
			return url
		}
	}
	logger.Warn("[MCP] No Erigon endpoint found on default ports, using http://127.0.0.1:8545")
	return ""
}

// runDatadirMode starts the MCP server using direct DB access,
// similar to running an external rpcdaemon.
func runDatadirMode(ctx context.Context, logger log.Logger, dataDir, privAPI, logDir, transport, sseAddr string) error {
	logger.Info("[MCP] Starting in datadir mode (direct DB access)", "datadir", dataDir)

	// Create a child context so RemoteServices can propagate shutdown signals
	// (e.g. from gRPC disconnect or DB errors) back to us.
	ctx, rootCancel := context.WithCancel(ctx)
	defer rootCancel()

	// Build Dirs from the datadir path. Use Open() (not New()) because MCP is
	// read-only and must not create directories in the datadir.
	// This is the root cause of the "salt files missing" error: without cfg.Dirs,
	// CheckSaltFilesExist receives zero-valued Dirs with empty Snap path, so it
	// looks for salt-blocks.txt in the current working directory instead of
	// <datadir>/snapshots/.
	dirs := datadir.Open(dataDir)

	cfg := &httpcfg.HttpCfg{
		Sync:       ethconfig.Defaults.Sync,
		Enabled:    true,
		StateCache: kvcache.DefaultCoherentConfig,
		API:        []string{"eth", "erigon", "ots"}, // APIs needed by MCP tools

		DataDir:           dataDir,
		Dirs:              dirs,
		WithDatadir:       true,
		PrivateApiAddr:    privAPI,
		TxPoolApiAddr:     privAPI, // inherit from private API, same as rpcdaemon
		DBReadConcurrency: min(max(10, runtime.GOMAXPROCS(-1)*64), 9_000),
	}

	db, backend, txPool, mining, stateCache, blockReader, engine, ff, bridgeReader, heimdallReader, err :=
		cli.RemoteServices(ctx, cfg, logger, rootCancel)
	if err != nil {
		return fmt.Errorf("failed to initialize datadir services: %w", err)
	}
	defer db.Close()
	if engine != nil {
		defer engine.Close()
	}
	if bridgeReader != nil {
		defer bridgeReader.Close()
	}
	if heimdallReader != nil {
		defer heimdallReader.Close()
	}

	// Create the typed JSON-RPC APIs — same path as rpcdaemon.
	apiList := jsonrpc.APIList(db, backend, txPool, mining, ff, stateCache, blockReader, cfg, engine, logger, bridgeReader, heimdallReader)

	// Extract the EthAPI, ErigonAPI, OtterscanAPI from the API list.
	var (
		ethAPI    jsonrpc.EthAPI
		erigonAPI jsonrpc.ErigonAPI
		otsAPI    jsonrpc.OtterscanAPI
	)
	for _, api := range apiList {
		switch s := api.Service.(type) {
		case jsonrpc.EthAPI:
			ethAPI = s
		case jsonrpc.ErigonAPI:
			erigonAPI = s
		case jsonrpc.OtterscanAPI:
			otsAPI = s
		}
	}

	if ethAPI == nil || erigonAPI == nil {
		return fmt.Errorf("failed to initialize required APIs from datadir")
	}

	srv := mcpserver.NewErigonMCPServer(ethAPI, erigonAPI, otsAPI, logDir)
	return serve(ctx, srv, transport, sseAddr, logger)
}
