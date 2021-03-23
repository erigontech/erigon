package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// APIList describes the list of available RPC apis
func APIList(db ethdb.Database, eth core.ApiBackend, filters *filters.Filters, cfg cli.Flags, customAPIList []rpc.API) []rpc.API {
	var defaultAPIList []rpc.API

	ethImpl := NewEthAPI(db, eth, cfg.Gascap, filters)
	tgImpl := NewTgAPI(db)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(db, cfg.Gascap)
	traceImpl := NewTraceAPI(db, &cfg)
	web3Impl := NewWeb3APIImpl()
	dbImpl := NewDBAPIImpl()   /* deprecated */
	shhImpl := NewSHHAPIImpl() /* deprecated */

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
		case "db":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "db",
				Public:    true,
				Service:   DBAPI(dbImpl),
				Version:   "1.0",
			})
		case "shh":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "shh",
				Public:    true,
				Service:   SHHAPI(shhImpl),
				Version:   "1.0",
			})
		case "tg":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "tg",
				Public:    true,
				Service:   TgAPI(tgImpl),
				Version:   "1.0",
			})
		}
	}

	return append(defaultAPIList, customAPIList...)
}
