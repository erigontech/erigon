package commands

import (
	"context"
	"fmt"
	"sort"

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

func (api *ErigonImpl) GetBlockByTimestamp(ctx context.Context, timeStamp uint64, fullTx bool) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	currentHeader := rawdb.ReadCurrentHeader(tx)
	currenttHeaderTime := currentHeader.Time
	highestNumber := currentHeader.Number.Uint64()

	firstHeader := rawdb.ReadHeaderByNumber(tx, 0)
	firstHeaderTime := firstHeader.Time

	if currenttHeaderTime <= timeStamp {
		blockResponse, err := buildBlockResponse(tx, highestNumber, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	if firstHeaderTime >= timeStamp {
		blockResponse, err := buildBlockResponse(tx, 0, fullTx)
		if err != nil {
			return nil, err
		}

		return blockResponse, nil
	}

	blockNum := sort.Search(int(currentHeader.Number.Uint64()), func(blockNum int) bool {
		currentHeader := rawdb.ReadHeaderByNumber(tx, uint64(blockNum))

		return currentHeader.Time >= timeStamp
	})

	resultingHeader := rawdb.ReadHeaderByNumber(tx, uint64(blockNum))

	if resultingHeader.Time > timeStamp {
		response, err := buildBlockResponse(tx, uint64(blockNum)-1, fullTx)
		if err != nil {
			return nil, err
		}
		return response, nil
	}

	response, err := buildBlockResponse(tx, uint64(blockNum), fullTx)
	if err != nil {
		return nil, err
	}

	return response, nil
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
