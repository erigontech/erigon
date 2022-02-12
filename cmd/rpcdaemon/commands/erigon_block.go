package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
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

func (api *ErigonImpl) GetBlockByTimeStamp(ctx context.Context, timeStamp uint64) (types.Block, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return types.Block{}, err
	}
	defer tx.Rollback()

	currentHeader := rawdb.ReadCurrentHeader(tx)
	currenttHeaderTime := currentHeader.Time

	firstHeader := rawdb.ReadHeaderByNumber(tx, 1)
	firstHeaderTime := firstHeader.Time

	if currenttHeaderTime == timeStamp {
		block := *rawdb.ReadCurrentBlock(tx)
		return block, nil
	}

	if firstHeaderTime == timeStamp {
		block, err := rawdb.ReadBlockByNumber(tx, 1)
		if err != nil {
			return types.Block{}, err
		}
		return *block, nil
	}

	return types.Block{}, nil

}
