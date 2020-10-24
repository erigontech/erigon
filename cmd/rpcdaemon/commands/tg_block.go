package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// GetHeaderByNumber returns a block's header by number
func (api *TgImpl) GetHeaderByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (*types.Header, error) {
	tx, err := api.dbReader.Begin(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	header := rawdb.ReadHeaderByNumber(tx, uint64(blockNumber.Int64()))
	if header == nil {
		return nil, fmt.Errorf("block header not found: %d", blockNumber.Int64())
	}

	return header, nil
}

// GetHeaderByHash returns a block's header by hash
func (api *TgImpl) GetHeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	tx, err := api.dbReader.Begin(ctx, false)
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
