package rpcservices

import (
	"context"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
)

func (back *RemoteBackend) TxnEffectiveGasPricePercentage(ctx context.Context, tx kv.Tx, txnHash libcommon.Hash) (uint8, error) {
	return back.blockReader.TxnEffectiveGasPricePercentage(ctx, tx, txnHash)
}
