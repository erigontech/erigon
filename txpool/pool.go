/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

// Pool is interface for the transaction pool
// This interface exists for the convinience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(hash []byte) bool

	NotifyNewPeer(peerID PeerID)
}

type PoolImpl struct {
	recentlyConnectedPeers     *recentlyConnectedPeers
	lastTxPropagationTimestamp time.Time
}

func NewPool() *PoolImpl {
	return &PoolImpl{
		recentlyConnectedPeers: &recentlyConnectedPeers{},
	}
}

// Loop - does:
// send pending txs to p2p:
//      - new txs
//      - all pooled txs to recently connected peers
//      - all local pooled txs to random peers periodically
// promote/demote transactions
// reorgs
func (p *PoolImpl) Loop(ctx context.Context, send *Send, timings Timings) {
	propagateAllNewTxsEvery := time.NewTicker(timings.propagateAllNewTxsEvery)
	defer propagateAllNewTxsEvery.Stop()

	syncToNewPeersEvery := time.NewTicker(timings.syncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()

	broadcastLocalTransactionsEvery := time.NewTicker(timings.broadcastLocalTransactionsEvery)
	defer broadcastLocalTransactionsEvery.Stop()

	localTxHashes := make([]byte, 0, 128)
	remoteTxHashes := make([]byte, 0, 128)

	for {
		select {
		case <-ctx.Done():
			return
		case <-propagateAllNewTxsEvery.C: // new txs
			last := p.lastTxPropagationTimestamp
			p.lastTxPropagationTimestamp = time.Now()

			// first broadcast all local txs to all peers, then non-local to random sqrt(peersAmount) peers
			localTxHashes = localTxHashes[:0]
			p.FillLocalHashesSince(last, localTxHashes)
			send.BroadcastLocalPooledTxs(localTxHashes)

			remoteTxHashes = remoteTxHashes[:0]
			p.FillRemoteHashesSince(last, remoteTxHashes)
			send.BroadcastRemotePooledTxs(remoteTxHashes)
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			p.FillRemoteHashes(remoteTxHashes[:0])
			send.PropagatePooledTxsToPeersList(newPeers, remoteTxHashes)
		case <-broadcastLocalTransactionsEvery.C: // periodically broadcast local txs to random peers
			p.FillLocalHashes(localTxHashes[:0])
			send.BroadcastLocalPooledTxs(localTxHashes)
		}
	}
}

func (p *PoolImpl) FillLocalHashesSince(since time.Time, to []byte)  {}
func (p *PoolImpl) FillRemoteHashesSince(since time.Time, to []byte) {}
func (p *PoolImpl) FillLocalHashes(to []byte)                        {}
func (p *PoolImpl) FillRemoteHashes(to []byte)                       {}

// recentlyConnectedPeers does buffer IDs of recently connected good peers
// then sync of pooled Transaction can happen to all of then at once
// DoS protection and performance saving
// it doesn't track if peer disconnected, it's fine
type recentlyConnectedPeers struct {
	lock  sync.RWMutex
	peers []*types.H512
}

func (l *recentlyConnectedPeers) AddPeer(p *types.H512) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []*types.H512 {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}
