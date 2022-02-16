package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/rpc"
)

// GetHeaderByNumber implements erigon_getHeaderByNumber. Returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).
func (api *ErigonImpl) GetHeaderByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (*types.Header, error) {
	// Pending block is only known by the miner
	if blockNumber == rpc.PendingBlockNumber {
		block := api.pendingBlock()
		if block == nil {
			return nil, nil
		}
		return block.Header(), nil
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, err := getBlockNumber(blockNumber, tx)
	if err != nil {
		return nil, err
	}

	header := rawdb.ReadHeaderByNumber(tx, blockNum)
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNum)
	}

	return header, nil
}

// GetHeaderByHash implements erigon_getHeaderByHash. Returns a block's header given a block's hash.
func (api *ErigonImpl) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}

func (api *ErigonImpl) GetBlockByTimeStamp(ctx context.Context, timeStamp uint64, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	currentHeader := rawdb.ReadCurrentHeader(tx)
	currenttHeaderTime := currentHeader.Time

	var lowestNumber uint64 = 0
	highestNumber := currentHeader.Number.Uint64()
	middleNumber := (highestNumber + lowestNumber) / 2

	firstHeader := rawdb.ReadHeaderByNumber(tx, lowestNumber)
	firstHeaderTime := firstHeader.Time

	middleHeader := rawdb.ReadHeaderByNumber(tx, middleNumber)

	if currenttHeaderTime == timeStamp {
		blockResponse, err := buildBlockResponse(tx, highestNumber, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	if firstHeaderTime == timeStamp {
		blockResponse, err := buildBlockResponse(tx, lowestNumber, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	if middleHeader.Time == timeStamp {
		blockResponse, err := buildBlockResponse(tx, middleNumber, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	for lowestNumber < highestNumber {

		if middleHeader.Time < timeStamp {
			lowestNumber = middleNumber + 1
		}

		if middleHeader.Time > timeStamp {
			highestNumber = middleNumber - 1
		}

		if middleHeader.Time == timeStamp {
			blockResponse, err := buildBlockResponse(tx, middleNumber, fullTx)
			if err != nil {
				return nil, err
			}
			return blockResponse, nil
		}

		middleNumber = (highestNumber + lowestNumber) / 2
		middleHeader = rawdb.ReadHeaderByNumber(tx, middleNumber)
		if middleHeader == nil {
			return nil, nil
		}

	}

	blockResponse, err := buildBlockResponse(tx, highestNumber, fullTx)
	if err != nil {
		return nil, err
	}
	return blockResponse, nil

}

func buildBlockResponse(db kv.Tx, blockNum uint64, fullTx bool) (map[string]interface{}, error) {
	block, err := rawdb.ReadBlockByNumber(db, blockNum)
	if err != nil {
		return nil, err
	}

	if block == nil {
		return nil, nil
	}

	response, err := ethapi.RPCMarshalBlock(block, true, fullTx)

	if err == nil && rpc.BlockNumber(block.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}
	return response, err
}
