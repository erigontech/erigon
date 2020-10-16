package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// GetLogsByHash returns all logs in a block
func (api *TgImpl) GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	tx, err := api.db.Begin(ctx, nil, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	number := rawdb.ReadHeaderNumber(tx, hash)
	if number == nil {
		return nil, fmt.Errorf("block not found: %x", hash)
	}
	receipts, err := getReceipts(ctx, tx, *number, hash)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}
