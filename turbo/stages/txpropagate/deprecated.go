package txpropagate

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core"
)

const txChanSize int = 4096

// BroadcastPendingTxsToNetwork - does send to p2p:
// - new txs
// - all pooled txs to recently connected peers
// - all local pooled txs to random peers periodically
func BroadcastPendingTxsToNetwork(ctx context.Context, txPool *core.TxPool, recentPeers *RecentlyConnectedPeers, s *download.ControlServerImpl) {
	defer debug.LogPanic()

	txsCh := make(chan core.NewTxsEvent, txChanSize)
	txsSub := txPool.SubscribeNewTxsEvent(txsCh)
	defer txsSub.Unsubscribe()

	syncToNewPeersEvery := time.NewTicker(2 * time.Minute)
	defer syncToNewPeersEvery.Stop()

	broadcastLocalTransactionsEvery := time.NewTicker(5 * time.Minute)
	defer broadcastLocalTransactionsEvery.Stop()

	pooledTxHashes := make([]common.Hash, 128)

	for {
		select {
		case <-txsSub.Err():
			return
		case <-ctx.Done():
			return
		case e := <-txsCh: // new txs
			pooledTxHashes = pooledTxHashes[:0]
			for i := range e.Txs {
				pooledTxHashes = append(pooledTxHashes, e.Txs[i].Hash())
			}
			s.BroadcastPooledTxs(ctx, pooledTxHashes)
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := recentPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			pooledTxHashes = txPool.AppendHashes(pooledTxHashes[:0])
			s.PropagatePooledTxsToPeersList(ctx, newPeers, pooledTxHashes)
		case <-broadcastLocalTransactionsEvery.C: // periodically broadcast local txs to random peers
			pooledTxHashes = txPool.AppendLocalHashes(pooledTxHashes[:0])
			s.BroadcastPooledTxs(ctx, pooledTxHashes)
		}
	}
}

type RecentlyConnectedPeers struct {
	lock  sync.RWMutex
	peers []*types.H512
}

func (l *RecentlyConnectedPeers) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.peers)
}
func (l *RecentlyConnectedPeers) AddPeer(p *types.H512) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}
func (l *RecentlyConnectedPeers) GetAndClean() []*types.H512 {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}
