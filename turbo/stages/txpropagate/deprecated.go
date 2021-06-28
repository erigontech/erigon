package txpropagate

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core"
)

const txChanSize int = 4096

func BroadcastNewTxsToNetworks(ctx context.Context, txPool *core.TxPool, s *download.ControlServerImpl) {
	defer debug.LogPanic()

	txsCh := make(chan core.NewTxsEvent, txChanSize)
	txsSub := txPool.SubscribeNewTxsEvent(txsCh)
	defer txsSub.Unsubscribe()

	for {
		select {
		case e := <-txsCh:
			s.BroadcastNewTxs(context.Background(), e.Txs)
		case <-txsSub.Err():
			return
		case <-ctx.Done():
			return
		}
	}
}
