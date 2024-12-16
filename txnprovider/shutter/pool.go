package shutter

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider"
)

var _ txnprovider.TxnProvider = (*Pool)(nil)

type Pool struct {
	logger               log.Logger
	config               Config
	secondaryTxnProvider txnprovider.TxnProvider
}

func NewPool(logger log.Logger, config Config, secondaryTxnProvider txnprovider.TxnProvider) *Pool {
	return &Pool{
		logger:               logger.New("component", "shutter"),
		config:               config,
		secondaryTxnProvider: secondaryTxnProvider,
	}
}

func (p Pool) Run(ctx context.Context) error {
	p.logger.Info("running pool")
	//
	// TODO - start pool, sentinel listeners for keyper decryption keys and other necessary background goroutines
	//        blocks until all sub-components have shutdown or have error-ed
	//
	return nil
}

func (p Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	//
	// TODO - implement shutter spec
	//        1) fetch corresponding txns for current slot and fill the remaining gas
	//           with the secondary txn provider (devp2p)
	//        2) if no decryption keys arrive for current slot then return empty transactions
	//
	return p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
}
