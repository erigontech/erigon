package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/rpc"
)

// APIList describes the list of available RPC apis
func APIList(ctx context.Context, db kv.RoDB,
	eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, filters *filters.Filters,
	stateCache kvcache.Cache,
	cfg cli.Flags, customAPIList []rpc.API) []rpc.API {
	var defaultAPIList []rpc.API

	base := NewBaseApi(filters, stateCache, cfg.SingleNodeMode)
	if cfg.TevmEnabled {
		base.EnableTevmExperiment()
	}
	ethImpl := NewEthAPI(base, db, eth, txPool, mining, cfg.Gascap)
	erigonImpl := NewErigonAPI(base, db)
	txpoolImpl := NewTxPoolAPI(base, db, txPool)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(base, db, cfg.Gascap)
	traceImpl := NewTraceAPI(base, db, &cfg)
	web3Impl := NewWeb3APIImpl(eth)
	dbImpl := NewDBAPIImpl() /* deprecated */

	for _, enabledAPI := range cfg.API {
		switch enabledAPI {
		case "eth":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(ethImpl),
				Version:   "1.0",
			})
		case "debug":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(debugImpl),
				Version:   "1.0",
			})
		case "net":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})
		case "txpool":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "txpool",
				Public:    true,
				Service:   TxPoolAPI(txpoolImpl),
				Version:   "1.0",
			})
		case "web3":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "web3",
				Public:    true,
				Service:   Web3API(web3Impl),
				Version:   "1.0",
			})
		case "trace":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "trace",
				Public:    true,
				Service:   TraceAPI(traceImpl),
				Version:   "1.0",
			})
		case "db": /* Deprecated */
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "db",
				Public:    true,
				Service:   DBAPI(dbImpl),
				Version:   "1.0",
			})
		case "erigon":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "erigon",
				Public:    true,
				Service:   ErigonAPI(erigonImpl),
				Version:   "1.0",
			})
		}
	}

	return append(defaultAPIList, customAPIList...)
}
