package rpcservices

import (
	"context"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
)

func (back *RemoteBackend) TxnEffectiveGasPricePercentage(ctx context.Context, tx kv.Tx, txnHash libcommon.Hash) (uint8, error) {
	return back.blockReader.TxnEffectiveGasPricePercentage(ctx, tx, txnHash)
}
