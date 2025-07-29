package router

import (
	"context"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/pool3"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// TODO Add code to manage all external interactions

// Add transaction
// Req: TXN RLP, Res: Code

// Print stats
// [] txn_stat

// Notify peers of new transactions

type PoolRouter struct {
	ctx           context.Context // Context used for cancellation and closing of the fetcher
	logger        log.Logger
	sentryClients []sentry.SentryClient // sentry clients that will be used for accessing the network
	PoolManager   *pool3.PoolManager

	QueuedTxns         []hexutil.Bytes
	PoolManagerChannel chan hexutil.Bytes
	ExternalChannel    chan hexutil.Bytes
	UnprocessedTxns    chan *txpool.TxnSlot

	KnownTxnHashes map[common.Hash]struct{} // Short term cache used for filtering out known txns
}

func New() *PoolRouter {
	return &PoolRouter{
		QueuedTxns:         make([]hexutil.Bytes, 0, 100),
		PoolManagerChannel: make(chan hexutil.Bytes, 100),
		ExternalChannel:    make(chan hexutil.Bytes, 100),
	}
}

func NewWithManager(dbRef kv.TemporalRwDB, config *chain.Config) *PoolRouter {
	poolRouter := &PoolRouter{
		QueuedTxns:         make([]hexutil.Bytes, 0, 100),
		PoolManagerChannel: make(chan hexutil.Bytes, 100),
		ExternalChannel:    make(chan hexutil.Bytes, 100),
		UnprocessedTxns:    make(chan *txpool.TxnSlot, 1000),
		PoolManager:        pool3.NewPoolManager(dbRef, config),
	}
	poolRouter.PoolManager.UnProcessedTxnsChan = &poolRouter.UnprocessedTxns
	return poolRouter
}

func (f *PoolRouter) FilterKnownIdHashes(hashes []common.Hash) []common.Hash {
	ret := make([]common.Hash, 0, len(hashes))
	for _, h := range hashes {
		if _, ok := f.KnownTxnHashes[h]; ok {
			continue
		}
		ret = append(ret, h)
	}
	if len(ret) == 0 {
		return nil
	}
	return f.PoolManager.FilterKnownIdHashes(ret)
}