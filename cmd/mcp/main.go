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
	"path/filepath"
	"slices"
	"time"

	"github.com/urfave/cli/v3"

	rpcdaemoncli "github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/cmd/utils/flags"
	"github.com/erigontech/erigon/common/disk"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/diagnostics/mem"
	nodecli "github.com/erigontech/erigon/node/cli"
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

var (
	rpcURLFlag = cli.StringFlag{
		Name:  "rpc.url",
		Usage: "Erigon JSON-RPC endpoint URL",
		Value: "http://127.0.0.1:8545",
	}
	portFlag = cli.UintFlag{
		Name:  "port",
		Usage: "Erigon JSON-RPC port (shorthand for --rpc.url=http://127.0.0.1:{port})",
	}
	dataDirFlag = flags.DirectoryFlag{
		Name:  "datadir",
		Usage: "Erigon data directory (enables direct DB access mode)",
	}
	privAPIFlag = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "Erigon gRPC private API address (used with --datadir)",
		Value: "127.0.0.1:9090",
	}
	transportFlag = cli.StringFlag{
		Name:  "transport",
		Usage: "MCP transport: 'stdio' or 'http' ('sse' is a deprecated alias of 'http')",
		Value: "stdio",
	}
	sseAddrFlag = cli.StringFlag{
		Name:  "sse.addr",
		Usage: "HTTP listen address (when transport=http)",
		Value: "127.0.0.1:8553",
	}
	logDirFlag = cli.StringFlag{
		Name:  "log.dir",
		Usage: "Erigon log directory (overrides datadir-based detection)",
	}
)

const longDescription = `MCP (Model Context Protocol) server for Erigon.

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
  --transport http    Serve over HTTP: streamable HTTP at /mcp, SSE at /sse.
  --transport sse     Deprecated alias of http.

Examples:
  # Claude Desktop config (stdio, auto-discovery):
  mcp

  # Point at running Erigon:
  mcp --port 8545

  # Direct DB access (offline):
  mcp --datadir /data/erigon --private.api.addr 127.0.0.1:9090

  # HTTP transport (streamable HTTP + SSE):
  mcp --port 8545 --transport http --sse.addr 127.0.0.1:8553`

func main() {
	app := nodecli.NewApp("Standalone MCP server for Erigon")
	app.Name = "mcp"
	app.UsageText = "mcp [flags]"
	app.Description = longDescription
	app.Action = runMCP
	app.Flags = slices.Concat(
		[]cli.Flag{&rpcURLFlag, &portFlag, &dataDirFlag, &privAPIFlag, &transportFlag, &sseAddrFlag, &logDirFlag},
		debug.Flags, utils.MetricFlags, logging.Flags,
	)

	// cancel is owned by ListenSignals; main exits the process, so there is
	// nothing to release on the way out.
	ctx, cancel := context.WithCancel(context.Background())
	go debug.ListenSignals(cancel, log.Root())

	if err := app.Run(ctx, os.Args); err != nil {
		os.Exit(1) // fallback: NewApp's ExitErrHandler normally prints and exits first
	}
}

func runMCP(ctx context.Context, cmd *cli.Command) error {
	// Deferred here, not in a cli After hook: NewApp's ExitErrHandler exits the
	// process on an Action error before urfave unwinds After, so profiles
	// started by --pprof.cpuprofile / --trace would never be flushed.
	logger, _, _, _, err := debug.SetupWithPrefix(ctx, cmd, "mcp", true /* rootLogger */)
	defer debug.Exit()
	if err != nil {
		return err
	}

	go mem.LogMemStats(ctx, logger)
	go disk.UpdateDiskStats(ctx, logger)

	dataDir := cmd.String(dataDirFlag.Name)
	transport := cmd.String(transportFlag.Name)
	sseAddr := cmd.String(sseAddrFlag.Name)
	port := cmd.Uint(portFlag.Name)

	logDir := cmd.String(logDirFlag.Name)
	if logDir == "" && dataDir != "" {
		logDir = filepath.Join(dataDir, "logs")
	}

	// --- Mode 1: Direct datadir (rpcdaemon-style) ---
	if dataDir != "" && !cmd.IsSet(rpcURLFlag.Name) && port == 0 {
		err := runDatadirMode(ctx, logger, dataDir, cmd.String(privAPIFlag.Name), logDir, transport, sseAddr)
		if err != nil {
			return fmt.Errorf("datadir mode failed: %w (use --rpc.url to connect via JSON-RPC instead)", err)
		}
		return nil
	}

	// --- Mode 2/3: JSON-RPC proxy ---
	url := cmd.String(rpcURLFlag.Name)
	switch {
	case port > 0:
		url = fmt.Sprintf("http://127.0.0.1:%d", port)
	case !cmd.IsSet(rpcURLFlag.Name):
		// Only probe when the user named no endpoint at all; discovery
		// must never override an explicit --rpc.url.
		if discovered := autoDiscover(ctx, logger); discovered != "" {
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

	srv := mcpserver.NewErigonMCPServer(client, logDir, false)
	return serve(ctx, srv, transport, sseAddr, logger)
}

// serve starts the MCP server in the chosen transport mode.
func serve(ctx context.Context, srv *mcpserver.ErigonMCPServer, transport, sseAddr string, logger log.Logger) error {
	switch transport {
	case "stdio":
		logger.Info("[MCP] Starting stdio transport")
		return srv.ServeContext(ctx)
	case "http", "sse":
		logger.Info("[MCP] Starting HTTP transport", "addr", sseAddr, "endpoints", "/mcp (streamable HTTP), /sse + /message (SSE)")
		return srv.ListenAndServe(ctx, sseAddr)
	default:
		return fmt.Errorf("unknown transport: %s (use 'stdio' or 'http')", transport)
	}
}

// autoDiscover probes localhost on well-known JSON-RPC ports.
func autoDiscover(ctx context.Context, logger log.Logger) string {
	logger.Info("[MCP] Auto-discovering Erigon JSON-RPC endpoint...")
	var dialer net.Dialer
	for _, p := range defaultRPCPorts {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		conn, err := dialer.DialContext(dialCtx, "tcp", addr)
		cancel()
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

	// Use Open() (not New()) because MCP is read-only and must not create
	// directories in the datadir. Without cfg.Dirs, CheckSaltFilesExist gets
	// zero-valued Dirs and looks for salt-blocks.txt in the working directory
	// instead of <datadir>/snapshots/.
	dirs := datadir.Open(dataDir)

	cfg := &httpcfg.HttpCfg{
		Sync:                ethconfig.Defaults.Sync,
		Enabled:             true,
		StateCache:          kvcache.DefaultCoherentConfig,
		RpcBatchConcurrency: 2,
		API:                 []string{"eth", "erigon", "ots", "txpool", "net", "admin", "debug", "trace"}, // APIs needed by MCP tools

		DataDir:           dataDir,
		Dirs:              dirs,
		WithDatadir:       true,
		PrivateApiAddr:    privAPI,
		TxPoolApiAddr:     privAPI, // inherit from private API, same as rpcdaemon
		DBReadConcurrency: httpcfg.DefaultDBReadConcurrency(),
	}

	db, backend, txPool, mining, stateCache, blockReader, engine, ff, err :=
		rpcdaemoncli.RemoteServices(ctx, cfg, logger, rootCancel)
	if err != nil {
		return fmt.Errorf("failed to initialize datadir services: %w", err)
	}
	defer db.Close()
	if engine != nil {
		defer engine.Close()
	}

	// Create the JSON-RPC APIs and serve them over an in-process connection —
	// same path as rpcdaemon.
	apiList := jsonrpc.APIList(db, backend, txPool, mining, ff, stateCache, blockReader, cfg, engine, logger, nil, nil)
	rpcSrv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.DebugSingleRequest, cfg.RpcStreamingDisable, logger, cfg.RPCSlowLogThreshold)
	defer rpcSrv.Stop()
	for _, api := range apiList {
		if err := rpcSrv.RegisterName(api.Namespace, api.Service); err != nil {
			return fmt.Errorf("failed to register %s API: %w", api.Namespace, err)
		}
	}
	// NonBlockingAcquire so BeginRo fails fast instead of blocking MCP
	// handlers when all DB read slots are held.
	client := rpc.DialInProcWithContext(kv.WithNonBlockingAcquire(ctx), rpcSrv, logger)
	defer client.Close()

	srv := mcpserver.NewErigonMCPServer(client, logDir, false)
	return serve(ctx, srv, transport, sseAddr, logger)
}
