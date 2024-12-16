package shutter

import (
	"context"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider"
)

var _ txnprovider.TxnProvider = Pool{}

type Pool struct {
}

func (p Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	//
	// TODO
	//
	return nil, nil
}
