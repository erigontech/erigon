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

// Package rpc provides the RpcProvider — the component extracted from
// backend.go responsible for embedded RPC services.
//
// Sequencing note: EthBackendRPC, MiningRPC, and StateDiffClient must be
// created in backend.go before the txpool is initialised (the txpool takes
// them as dependencies). Those three objects are therefore passed into this
// provider as deps rather than created here. The provider owns the rest of
// the RPC stack: EmbeddedServices, JSON-RPC API objects, MCP server, and
// the HTTP / engine server goroutines.
package rpc

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/polygon/heimdall"

	rpcdaemoncli "github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/mcp"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// bridgeReader is a local alias for the unexported jsonrpc.bridgeReader interface.
type bridgeReader interface {
	Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error)
	EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error)
}

// spanProducersReader is a local alias for the unexported jsonrpc.spanProducersReader interface.
type spanProducersReader interface {
	Producers(ctx context.Context, blockNum uint64) (*heimdall.ValidatorSet, error)
}

// Provider holds the embedded RPC component runtime state.
// Lifecycle: Configure → Initialize → BuildApiList → Start.
//
// EthBackendRPC, MiningRPC, and StateDiffClient are created by backend.go
// before the txpool initialises and are passed in as Deps. The provider
// owns everything that follows: EmbeddedServices, JSON-RPC API objects,
// MCP server, and the HTTP/engine goroutines.
//
// After Initialize, all public fields except Connections.apis are ready.
// Connections is populated by BuildApiList (standard namespaces) and may be
// extended by other components via Connections.Register before Start is called.
type Provider struct {
	// Public outputs — available after Initialize.
	EthBackendRPC       *privateapi.EthBackendServer
	MiningRPC           *privateapi.MiningServer
	StateDiffClient     *direct.StateDiffClientDirect
	EthRpcClient        rpchelper.ApiBackend
	TxPoolRpcClient     txpoolproto.TxpoolClient
	MiningRpcClient     txpoolproto.MiningClient
	RpcDaemonStateCache kvcache.Cache
	RpcFilters          *rpchelper.Filters
	EthApi              *jsonrpc.APIImpl
	McpServer           *mcp.ErigonMCPServer
	ExecModuleCache     *execmodule.Cache    // shared with execmodule.NewExecModule
	HttpCfg             httpcfg.HttpCfg      // modified copy (LocalCache set), shared downstream
	Connections         *jsonrpc.Connections // populated by BuildApiList; extend via Register before Start

	// Configuration — set by Configure.
	httpCfgInput httpcfg.HttpCfg
	mcpAddress   string
}

// ErrGroup is the same interface used in nodebuilder.
type ErrGroup interface {
	Go(func() error)
}

// Deps holds the external dependencies needed by Initialize.
//
// EthBackendRPC, MiningRPC, and StateDiffClient must be created in
// backend.go before the txpool is initialised. All other deps are
// available after txpool init.
type Deps struct {
	// Pre-created before txpool init.
	EthBackendRPC   *privateapi.EthBackendServer
	MiningRPC       *privateapi.MiningServer
	StateDiffClient *direct.StateDiffClientDirect

	// Available after txpool init.
	ChainDB          kv.TemporalRwDB
	ChainConfig      *chain.Config
	BlockReader      services.FullBlockReader
	TxPoolGrpcServer txpoolproto.TxpoolServer
	Engine           rules.EngineReader
	PolygonBridge    bridgeReader // nil for non-Bor
	DirsLog          string       // config.Dirs.Log for MCP server
	Logger           log.Logger
}

// BuildApiListDeps holds deps needed to build the full JSON-RPC API list.
type BuildApiListDeps struct {
	ChainKv         kv.TemporalRoDB
	Engine          rules.EngineReader
	BlockReader     services.FullBlockReader
	PolygonBridge   bridgeReader        // nil for non-Bor
	HeimdallService spanProducersReader // nil for non-Bor
	Logger          log.Logger
}

// StartDeps holds deps needed to start background goroutines.
type StartDeps struct {
	Ctx                          context.Context
	Eg                           ErrGroup
	EngineBackendRPC             *engineapi.EngineServer // nil when Bor without single slot finality
	ChainConfig                  *chain.Config
	PolygonPosSingleSlotFinality bool
	ChainDB                      kv.TemporalRoDB
	BlockReader                  services.FullBlockReader
	Engine                       rules.EngineReader
	Logger                       log.Logger
}

// Configure stores configuration. Must be called before Initialize.
func (p *Provider) Configure(httpCfg *httpcfg.HttpCfg, mcpAddress string) {
	p.httpCfgInput = *httpCfg
	p.mcpAddress = mcpAddress
}

// Initialize wires together the embedded RPC services.
//
// deps.EthBackendRPC, deps.MiningRPC, and deps.StateDiffClient must
// already be created by the caller (before txpool init). The remaining
// deps (TxPoolGrpcServer etc.) are used here to create EmbeddedServices
// and the JSON-RPC API objects.
//
// After this call: EthBackendRPC, MiningRPC, StateDiffClient,
// EthRpcClient, TxPoolRpcClient, MiningRpcClient, RpcDaemonStateCache,
// RpcFilters, EthApi, McpServer, ExecModuleCache, and HttpCfg are all set.
func (p *Provider) Initialize(ctx context.Context, deps Deps) error {
	logger := deps.Logger

	// Store pre-created backend servers.
	p.EthBackendRPC = deps.EthBackendRPC
	p.MiningRPC = deps.MiningRPC
	p.StateDiffClient = deps.StateDiffClient

	p.ExecModuleCache = &execmodule.Cache{}
	httpCfg := p.httpCfgInput
	httpCfg.StateCache.LocalCache = p.ExecModuleCache
	p.HttpCfg = httpCfg

	ethRpcClient, txPoolRpcClient, miningRpcClient, rpcDaemonStateCache, rpcFilters := rpcdaemoncli.EmbeddedServices(
		ctx,
		deps.ChainDB,
		httpCfg.StateCache,
		httpCfg.RpcFiltersConfig,
		deps.BlockReader,
		p.EthBackendRPC,
		deps.TxPoolGrpcServer,
		p.MiningRPC,
		p.StateDiffClient,
		logger,
	)
	p.EthRpcClient = ethRpcClient
	p.TxPoolRpcClient = txPoolRpcClient
	p.MiningRpcClient = miningRpcClient
	p.RpcDaemonStateCache = rpcDaemonStateCache
	p.RpcFilters = rpcFilters

	baseApi := jsonrpc.NewBaseApi(
		p.RpcFilters,
		p.RpcDaemonStateCache,
		deps.BlockReader,
		httpCfg.WithDatadir,
		httpCfg.EvmCallTimeout,
		deps.Engine,
		httpCfg.Dirs,
		deps.PolygonBridge,
		httpCfg.BlockRangeLimit,
		httpCfg.GetLogsMaxResults,
	)
	p.Connections = jsonrpc.NewConnections(baseApi)

	ethApiConfig := &jsonrpc.EthApiConfig{
		GasCap:                      httpCfg.Gascap,
		FeeCap:                      httpCfg.Feecap,
		ReturnDataLimit:             httpCfg.ReturnDataLimit,
		AllowUnprotectedTxs:         httpCfg.AllowUnprotectedTxs,
		MaxGetProofRewindBlockCount: httpCfg.MaxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    httpCfg.WebsocketSubscribeLogsChannelSize,
		RpcTxSyncDefaultTimeout:     httpCfg.RpcTxSyncDefaultTimeout,
		RpcTxSyncMaxTimeout:         httpCfg.RpcTxSyncMaxTimeout,
	}
	p.EthApi = jsonrpc.NewEthAPI(
		p.Connections.BaseApi,
		deps.ChainDB,
		p.EthRpcClient,
		p.TxPoolRpcClient,
		p.MiningRpcClient,
		ethApiConfig,
		logger,
	)

	erigonApi := jsonrpc.NewErigonAPI(p.Connections.BaseApi, deps.ChainDB, p.EthRpcClient)
	otsApi := jsonrpc.NewOtterscanAPI(p.Connections.BaseApi, deps.ChainDB, httpCfg.OtsMaxPageSize)

	p.McpServer = mcp.NewErigonMCPServer(p.EthApi, erigonApi, otsApi, deps.DirsLog)

	if p.mcpAddress != "" {
		go func() {
			logger.Info("serve MCP on", "addr", p.mcpAddress)
			if mcpErr := p.McpServer.ServeSSE(p.mcpAddress); mcpErr != nil {
				logger.Error("mcpServer.ServeSSE", "err", mcpErr)
			}
		}()
	}

	return nil
}

// BuildApiList registers the standard JSON-RPC API set on Connections.
// Call after the engine server is available (i.e. from Init()).
// Additional namespaces from other components may be registered on
// Connections before or after this call, but always before Start.
func (p *Provider) BuildApiList(deps BuildApiListDeps) {
	jsonrpc.PopulateConnections(
		p.Connections,
		deps.ChainKv,
		p.EthRpcClient,
		p.TxPoolRpcClient,
		p.MiningRpcClient,
		deps.BlockReader,
		&p.HttpCfg.SharedApiConfig,
		deps.Engine,
		deps.Logger,
		deps.PolygonBridge,
		deps.HeimdallService,
	)
}

// Start launches the background RPC and engine goroutines.
// Call from Init() after BuildApiList (and after any additional
// conn.Register calls from other components).
func (p *Provider) Start(deps StartDeps) {
	logger := deps.Logger
	apiList := p.Connections.APIs()
	httpCfg := p.HttpCfg

	deps.Eg.Go(func() error {
		err := rpcdaemoncli.StartRpcServer(deps.Ctx, &httpCfg, apiList, logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("cli.StartRpcServer error", "err", err)
		}
		return err
	})

	if deps.ChainConfig.Bor == nil || deps.PolygonPosSingleSlotFinality {
		if deps.EngineBackendRPC != nil {
			deps.Eg.Go(func() error {
				defer logger.Debug("[EngineServer] goroutine terminated")
				err := deps.EngineBackendRPC.Start(
					deps.Ctx,
					&httpCfg,
					deps.ChainDB,
					deps.BlockReader,
					p.RpcFilters,
					p.RpcDaemonStateCache,
					deps.Engine,
					p.EthRpcClient,
					p.MiningRpcClient,
				)
				if err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("[EngineServer] background goroutine failed", "err", err)
				}
				return err
			})
		}
	}
}
