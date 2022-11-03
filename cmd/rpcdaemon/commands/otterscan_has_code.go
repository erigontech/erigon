package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

func (api *OtterscanAPIImpl) HasCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, fmt.Errorf("hasCode cannot open tx: %w", err)
	}
	defer tx.Rollback()

	blockNumber, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return false, err
	}

	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return false, err
	}
	return !acc.IsEmptyCodeHash(), nil
}
