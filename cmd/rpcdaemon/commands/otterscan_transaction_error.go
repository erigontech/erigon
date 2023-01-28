package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

func (api *OtterscanAPIImpl) GetTransactionError(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	result, err := api.runTracer(ctx, tx, hash, nil)
	if err != nil {
		return nil, err
	}

	return result.Revert(), nil
}
