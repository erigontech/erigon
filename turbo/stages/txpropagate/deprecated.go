package txpropagate

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
)

const txChanSize int = 4096

func SendPendingTxsToRpcDaemon(ctx context.Context, txPool *core.TxPool, notifier *remotedbserver.Events) {
	defer debug.LogPanic()
	if notifier == nil {
		return
	}

	txsCh := make(chan core.NewTxsEvent, txChanSize)
	txsSub := txPool.SubscribeNewTxsEvent(txsCh)
	defer txsSub.Unsubscribe()

	for {
		select {
		case e := <-txsCh:
			notifier.OnNewPendingTxs(e.Txs)
		case <-txsSub.Err():
			return
		case <-ctx.Done():
			return
		}
	}
}

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
