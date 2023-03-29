package commands

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// APIList describes the list of available RPC apis
func APIList(db kv.RoDB, borDb kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient,
	filters *rpchelper.Filters, stateCache kvcache.Cache,
	blockReader services.FullBlockReader, agg *libstate.AggregatorV3, cfg httpcfg.HttpCfg, engine consensus.EngineReader,
) (list []rpc.API) {
	base := NewBaseApi(filters, stateCache, blockReader, agg, cfg.WithDatadir, cfg.EvmCallTimeout, engine)
	ethImpl := NewEthAPI(base, db, eth, txPool, mining, cfg.Gascap, cfg.ReturnDataLimit)
	erigonImpl := NewErigonAPI(base, db, eth)
	txpoolImpl := NewTxPoolAPI(base, db, txPool)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(base, db, cfg.Gascap)
	traceImpl := NewTraceAPI(base, db, &cfg)
	web3Impl := NewWeb3APIImpl(eth)
	dbImpl := NewDBAPIImpl() /* deprecated */
	adminImpl := NewAdminAPI(eth)
	parityImpl := NewParityAPIImpl(db)
	borImpl := NewBorAPI(base, db, borDb) // bor (consensus) specific
	otsImpl := NewOtterscanAPI(base, db)
	gqlImpl := NewGraphQLAPI(base, db)

	if cfg.GraphQLEnabled {
		list = append(list, rpc.API{
			Namespace: "graphql",
			Public:    true,
			Service:   GraphQLAPI(gqlImpl),
			Version:   "1.0",
		})
	}

	for _, enabledAPI := range cfg.API {
		switch enabledAPI {
		case "eth":
			list = append(list, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(ethImpl),
				Version:   "1.0",
			})
		case "debug":
			list = append(list, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(debugImpl),
				Version:   "1.0",
			})
		case "net":
			list = append(list, rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})
		case "txpool":
			list = append(list, rpc.API{
				Namespace: "txpool",
				Public:    true,
				Service:   TxPoolAPI(txpoolImpl),
				Version:   "1.0",
			})
		case "web3":
			list = append(list, rpc.API{
				Namespace: "web3",
				Public:    true,
				Service:   Web3API(web3Impl),
				Version:   "1.0",
			})
		case "trace":
			list = append(list, rpc.API{
				Namespace: "trace",
				Public:    true,
				Service:   TraceAPI(traceImpl),
				Version:   "1.0",
			})
		case "db": /* Deprecated */
			list = append(list, rpc.API{
				Namespace: "db",
				Public:    true,
				Service:   DBAPI(dbImpl),
				Version:   "1.0",
			})
		case "erigon":
			list = append(list, rpc.API{
				Namespace: "erigon",
				Public:    true,
				Service:   ErigonAPI(erigonImpl),
				Version:   "1.0",
			})
		case "bor":
			list = append(list, rpc.API{
				Namespace: "bor",
				Public:    true,
				Service:   BorAPI(borImpl),
				Version:   "1.0",
			})
		case "admin":
			list = append(list, rpc.API{
				Namespace: "admin",
				Public:    false,
				Service:   AdminAPI(adminImpl),
				Version:   "1.0",
			})
		case "parity":
			list = append(list, rpc.API{
				Namespace: "parity",
				Public:    false,
				Service:   ParityAPI(parityImpl),
				Version:   "1.0",
			})
		case "ots":
			list = append(list, rpc.API{
				Namespace: "ots",
				Public:    true,
				Service:   OtterscanAPI(otsImpl),
				Version:   "1.0",
			})
		}
	}

	return list
}

func AuthAPIList(db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient,
	filters *rpchelper.Filters, stateCache kvcache.Cache, blockReader services.FullBlockReader,
	agg *libstate.AggregatorV3,
	cfg httpcfg.HttpCfg, engine consensus.EngineReader,
) (list []rpc.API) {
	base := NewBaseApi(filters, stateCache, blockReader, agg, cfg.WithDatadir, cfg.EvmCallTimeout, engine)

	ethImpl := NewEthAPI(base, db, eth, txPool, mining, cfg.Gascap, cfg.ReturnDataLimit)
	engineImpl := NewEngineAPI(base, db, eth, cfg.InternalCL)

	list = append(list, rpc.API{
		Namespace: "eth",
		Public:    true,
		Service:   EthAPI(ethImpl),
		Version:   "1.0",
	}, rpc.API{
		Namespace: "engine",
		Public:    true,
		Service:   EngineAPI(engineImpl),
		Version:   "1.0",
	})

	return list
}
