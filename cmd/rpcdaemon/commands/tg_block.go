package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// GetHeaderByNumber implements tg_getHeaderByNumber. Returns a block's header given a block number ignoring the block's transaction and uncle list (may be faster).
func (api *TgImpl) GetHeaderByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (*types.Header, error) {
	// Pending block is only known by the miner
	if blockNumber == rpc.PendingBlockNumber {
		block := api.pending.Block()
		return block.Header(), nil
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header := rawdb.ReadHeaderByNumber(ethdb.NewRoTxDb(tx), uint64(blockNumber.Int64()))
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNumber.Int64())
	}

	return header, nil
}

// GetHeaderByHash implements tg_getHeaderByHash. Returns a block's header given a block's hash.
func (api *TgImpl) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(ethdb.NewRoTxDb(tx), hash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("block header not found: %s", hash.String())
	}

	return header, nil
}
