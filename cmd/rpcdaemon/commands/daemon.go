package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
)

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByNumber(api.dbReader, uint64(number.Int64()))
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", number.Int64())
	}

	additionalFields["totalDifficulty"] = rawdb.ReadTd(api.dbReader, block.Hash(), uint64(number.Int64()))
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

func APIList(db ethdb.KV, eth ethdb.Backend, cfg cli.Flags, customApiList []rpc.API) []rpc.API {
	var defaultAPIList []rpc.API

	dbReader := ethdb.NewObjectDatabase(db)
	apiImpl := NewAPI(db, dbReader, eth, cfg.Gascap)
	netImpl := NewNetAPIImpl(eth)
	dbgAPIImpl := NewPrivateDebugAPI(db, dbReader)

	for _, enabledAPI := range cfg.API {
		switch enabledAPI {
		case "eth":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(apiImpl),
				Version:   "1.0",
			})
		case "debug":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(dbgAPIImpl),
				Version:   "1.0",
			})
		case "net":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})

		}
	}

	// validation
	availableNamespaces := map[string]bool{}
	for _, v := range customApiList {
		availableNamespaces[v.Namespace] = true
	}
	for _, v := range defaultAPIList {
		availableNamespaces[v.Namespace] = true
	}

	for i := range cfg.API {
		_, ok := availableNamespaces[cfg.API[i]]
		if !ok {
			log.Error("Unrecognised", "api", cfg.API[i])
		}
	}

	return append(defaultAPIList, customApiList...)
}
