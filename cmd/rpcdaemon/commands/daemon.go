package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter/ethapi"
)

// GetBlockByNumber see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbynumber
// see internal/ethapi.PublicBlockChainAPI.GetBlockByNumber
func (api *APIImpl) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]interface{}, error) {
	var blockNum uint64
	if number == rpc.LatestBlockNumber || number == rpc.PendingBlockNumber {
		var err error
		blockNum, _, err = stages.GetStageProgress(api.dbReader, stages.Execution)
		if err != nil {
			return nil, fmt.Errorf("getting latest block number: %v", err)
		}
	} else if number == rpc.EarliestBlockNumber {
		blockNum = 0
	} else {
		blockNum = uint64(number.Int64())
	}
	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByNumber(api.dbReader, blockNum)
	if block == nil {
		return nil, fmt.Errorf("block not found: %d", blockNum)
	}

	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(api.dbReader, block.Hash(), blockNum))
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}

// GetBlockByHash see https://github.com/ethereum/wiki/wiki/JSON-RPC#eth_getblockbyhash
// see internal/ethapi.PublicBlockChainAPI.GetBlockByHash
func (api *APIImpl) GetBlockByHash(ctx context.Context, hash common.Hash, fullTx bool) (map[string]interface{}, error) {
	additionalFields := make(map[string]interface{})

	block := rawdb.ReadBlockByHash(api.dbReader, hash)
	if block == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}
	number := block.NumberU64()

	additionalFields["totalDifficulty"] = (*hexutil.Big)(rawdb.ReadTd(api.dbReader, block.Hash(), blockNum))
	response, err := ethapi.RPCMarshalBlock(block, true, fullTx, additionalFields)

	if err == nil && int64(number) == rpc.PendingBlockNumber.Int64() {
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
	traceAPIImpl := NewTraceAPI(db, dbReader, cfg.MaxTraces)

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
		case "trace":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "trace",
				Public:    true,
				Service:   TraceAPI(traceAPIImpl),
				Version:   "1.0",
			})
		}
	}

	return append(defaultAPIList, customApiList...)
}
