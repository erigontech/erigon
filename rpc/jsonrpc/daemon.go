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

package jsonrpc

import (
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/clique"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func NewEthApiConfig(cfg *httpcfg.SharedApiConfig) *EthApiConfig {
	return &EthApiConfig{
		GasCap:                      cfg.Gascap,
		FeeCap:                      cfg.Feecap,
		ReturnDataLimit:             cfg.ReturnDataLimit,
		AllowUnprotectedTxs:         cfg.AllowUnprotectedTxs,
		MaxGetProofRewindBlockCount: cfg.MaxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    cfg.WebsocketSubscribeLogsChannelSize,
		RpcTxSyncDefaultTimeout:     cfg.RpcTxSyncDefaultTimeout,
		RpcTxSyncMaxTimeout:         cfg.RpcTxSyncMaxTimeout,
	}
}

// PopulateConnections registers the standard JSON-RPC API set on conn using
// conn.BaseApi as the shared base. The enabled namespaces are taken from
// cfg.API; graphql is registered unconditionally when cfg.GraphQLEnabled is set.
//
// External components that want to expose additional namespaces should call
// conn.Register(...) directly, either before or after PopulateConnections.
func PopulateConnections(
	conn *Connections,
	db kv.TemporalRoDB,
	eth rpchelper.ApiBackend,
	txPool txpoolproto.TxpoolClient,
	mining txpoolproto.MiningClient,
	blockReader services.FullBlockReader,
	cfg *httpcfg.SharedApiConfig,
	engine rules.EngineReader,
	logger log.Logger,
	bridge bridgeReader,
	spans spanProducersReader,
) {
	base := conn.BaseApi
	ethImpl := NewEthAPI(base, db, eth, txPool, mining, NewEthApiConfig(cfg), logger)
	erigonImpl := NewErigonAPI(base, db, eth)
	txpoolImpl := NewTxPoolAPI(base, db, txPool)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(base, db, eth, cfg.Gascap, cfg.GethCompatibility)
	traceImpl := NewTraceAPI(base, db, cfg)
	web3Impl := NewWeb3APIImpl(eth)
	adminImpl := NewAdminAPI(eth)
	parityImpl := NewParityAPIImpl(base, db)

	var borImpl *BorImpl

	type lazy interface {
		HasEngine() bool
		Engine() rules.EngineReader
	}

	switch eng := engine.(type) {
	case *bor.Bor:
		borImpl = NewBorAPI(base, db, spans)
	case lazy:
		if _, ok := eng.Engine().(*bor.Bor); !eng.HasEngine() || ok {
			borImpl = NewBorAPI(base, db, spans)
		}
	}

	otsImpl := NewOtterscanAPI(base, db, cfg.OtsMaxPageSize)
	internalImpl := NewInternalAPI(base, db)
	gqlImpl := NewGraphQLAPI(base, db)
	overlayImpl := NewOverlayAPI(base, db, cfg.Gascap, cfg.OverlayGetLogsTimeout, cfg.OverlayReplayBlockTimeout, otsImpl)

	if cfg.GraphQLEnabled {
		conn.Register(rpc.API{
			Namespace: "graphql",
			Public:    true,
			Service:   GraphQLAPI(gqlImpl),
			Version:   "1.0",
		})
	}

	for _, enabledAPI := range cfg.API {
		switch enabledAPI {
		case "eth":
			conn.Register(rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(ethImpl),
				Version:   "1.0",
			})
		case "debug":
			conn.Register(rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(debugImpl),
				Version:   "1.0",
			})
		case "net":
			conn.Register(rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})
		case "txpool":
			conn.Register(rpc.API{
				Namespace: "txpool",
				Public:    true,
				Service:   TxPoolAPI(txpoolImpl),
				Version:   "1.0",
			})
		case "web3":
			conn.Register(rpc.API{
				Namespace: "web3",
				Public:    true,
				Service:   Web3API(web3Impl),
				Version:   "1.0",
			})
		case "trace":
			conn.Register(rpc.API{
				Namespace: "trace",
				Public:    true,
				Service:   TraceAPI(traceImpl),
				Version:   "1.0",
			})
		case "db": /* Deprecated */
			dbImpl := NewDBAPIImpl() /* deprecated */
			conn.Register(rpc.API{
				Namespace: "db",
				Public:    true,
				Service:   DBAPI(dbImpl),
				Version:   "1.0",
			})
		case "erigon":
			conn.Register(rpc.API{
				Namespace: "erigon",
				Public:    true,
				Service:   ErigonAPI(erigonImpl),
				Version:   "1.0",
			})
		case "bor":
			if borImpl != nil {
				conn.Register(rpc.API{
					Namespace: "bor",
					Public:    true,
					Service:   BorAPI(borImpl),
					Version:   "1.0",
				})
			}
		case "admin":
			conn.Register(rpc.API{
				Namespace: "admin",
				Public:    false,
				Service:   AdminAPI(adminImpl),
				Version:   "1.0",
			})
		case "parity":
			conn.Register(rpc.API{
				Namespace: "parity",
				Public:    false,
				Service:   ParityAPI(parityImpl),
				Version:   "1.0",
			})
		case "ots":
			conn.Register(rpc.API{
				Namespace: "ots",
				Public:    true,
				Service:   OtterscanAPI(otsImpl),
				Version:   "1.0",
			})
		case "internal":
			conn.Register(rpc.API{
				Namespace: "internal",
				Public:    true,
				Service:   InternalAPI(internalImpl),
				Version:   "1.0",
			})
		case "clique":
			conn.Register(clique.NewCliqueAPI(db, engine, blockReader))
		case "overlay":
			conn.Register(rpc.API{
				Namespace: "overlay",
				Public:    true,
				Service:   OverlayAPI(overlayImpl),
				Version:   "1.0",
			})
		}
	}
}

// APIList describes the list of available RPC apis.
// It is a convenience wrapper around NewConnections + PopulateConnections.
func APIList(db kv.TemporalRoDB, eth rpchelper.ApiBackend, txPool txpoolproto.TxpoolClient, mining txpoolproto.MiningClient,
	filters *rpchelper.Filters, stateCache kvcache.Cache,
	blockReader services.FullBlockReader, cfg *httpcfg.SharedApiConfig, engine rules.EngineReader,
	logger log.Logger, bridge bridgeReader, spans spanProducersReader,
) []rpc.API {
	base := NewBaseApi(filters, stateCache, blockReader, cfg.WithDatadir, cfg.EvmCallTimeout, engine, cfg.Dirs, bridge, cfg.BlockRangeLimit, cfg.GetLogsMaxResults)
	conn := NewConnections(base)
	PopulateConnections(conn, db, eth, txPool, mining, blockReader, cfg, engine, logger, bridge, spans)
	return conn.APIs()
}
