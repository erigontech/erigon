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

func BroadcastNewTxsToNetworks(ctx context.Context, txPool *core.TxPool, recentPeers *RecentlyConnectedPeers, s *download.ControlServerImpl) {
	defer debug.LogPanic()

	txsCh := make(chan core.NewTxsEvent, txChanSize)
	txsSub := txPool.SubscribeNewTxsEvent(txsCh)
	defer txsSub.Unsubscribe()

	syncToNewPeersEvery := time.NewTicker(5 * time.Minute)
	defer syncToNewPeersEvery.Stop()

	flatPendingHashes := make([]common.Hash, 128)

	for {
		select {
		case e := <-txsCh:
			s.BroadcastNewTxs(context.Background(), e.Txs)
		case <-txsSub.Err():
			return
		case <-txsSub.Err():
			return
		case <-syncToNewPeersEvery.C:
			flatPendingHashes = txPool.AppendHashes(flatPendingHashes[:0])
			s.PropagatePooledTxsToPeersList(context.Background(), recentPeers.GetAndClean(), flatPendingHashes)
		case <-ctx.Done():
			return
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
