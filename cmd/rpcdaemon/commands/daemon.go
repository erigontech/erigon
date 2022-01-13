package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/rpc"
)

// APIList describes the list of available RPC apis
func APIList(ctx context.Context, db kv.RoDB,
	eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, filters *filters.Filters,
	stateCache kvcache.Cache,
	blockReader interfaces.BlockAndTxnReader,
	cfg cli.Flags, customAPIList []rpc.API) []rpc.API {
	var defaultAPIList []rpc.API

	base := NewBaseApi(filters, stateCache, blockReader, cfg.SingleNodeMode)
	if cfg.TevmEnabled {
		base.EnableTevmExperiment()
	}
	ethImpl := NewEthAPI(base, db, eth, txPool, mining, cfg.Gascap)
	erigonImpl := NewErigonAPI(base, db, eth)
	starknetImpl := NewStarknetAPI(base, db, txPool)
	txpoolImpl := NewTxPoolAPI(base, db, txPool)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(base, db, cfg.Gascap)
	traceImpl := NewTraceAPI(base, db, &cfg)
	web3Impl := NewWeb3APIImpl(eth)
	dbImpl := NewDBAPIImpl() /* deprecated */
	engineImpl := NewEngineAPI(base, db, eth)
	adminImpl := NewAdminAPI(eth)
	parityImpl := NewParityAPIImpl(db)

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
		case "starknet":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "starknet",
				Public:    true,
				Service:   StarknetAPI(starknetImpl),
				Version:   "1.0",
			})
		case "engine":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "engine",
				Public:    true,
				Service:   EngineAPI(engineImpl),
				Version:   "1.0",
			})
		case "admin":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "admin",
				Public:    false,
				Service:   AdminAPI(adminImpl),
				Version:   "1.0",
			})
		case "parity":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "parity",
				Public:    false,
				Service:   ParityAPI(parityImpl),
				Version:   "1.0",
			})
		}
	}

	return append(defaultAPIList, customAPIList...)
}
