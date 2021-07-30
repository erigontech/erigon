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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	_ "github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
)

// Pool is interface for the transaction pool
// This interface exists for the convinience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(hash []byte) bool

	NotifyNewPeer(peerID PeerID)
}

// SubPoolMarker ordered bitset responsible to sort transactions by sub-pools. Bits meaning:
// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means this transaction will never be included into this particular chain.
// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

func NewSubPoolMarker(enoughFeeCapProtocol, noNonCeGaps, enoughBalance, enoughFeeCapBlock, isLocal bool) SubPoolMarker {
	var s SubPoolMarker
	if enoughFeeCapProtocol {
		s |= 1 << 4
	}
	if noNonCeGaps {
		s |= 1 << 3
	}
	if enoughBalance {
		s |= 1 << 2
	}
	if enoughFeeCapBlock {
		s |= 1 << 1
	}
	if isLocal {
		s |= 1 << 0
	}
	return s
}

// MetaTx holds transaction and some metadata
type MetaTx struct {
	SubPool    SubPoolMarker
	Tx         *TxSlot
	bestIndex  int
	worstIndex int
}

type BestQueue []*MetaTx

func (mt *MetaTx) Less(than *MetaTx) bool {
	if mt.SubPool != than.SubPool {
		return mt.SubPool < than.SubPool
	}
	// means that strict nonce ordering of transactions from the same sender must be observed.
	if mt.Tx.sender != than.Tx.sender {
		return bytes.Compare(mt.Tx.sender[:], than.Tx.sender[:]) < 0
	}
	if mt.Tx.nonce != than.Tx.nonce {
		return mt.Tx.nonce < than.Tx.nonce
	}
	return false
}

func (p BestQueue) Len() int           { return len(p) }
func (p BestQueue) Less(i, j int) bool { return !p[i].Less(p[j]) } // We want Pop to give us the highest, not lowest, priority so we use greater than here.
func (p BestQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].bestIndex = i
	p[j].bestIndex = j
}
func (p *BestQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*MetaTx)
	item.bestIndex = n
	*p = append(*p, item)
}

func (p *BestQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil      // avoid memory leak
	item.bestIndex = -1 // for safety
	*p = old[0 : n-1]
	return item
}

type WorstQueue []*MetaTx

func (p WorstQueue) Len() int           { return len(p) }
func (p WorstQueue) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p WorstQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].worstIndex = i
	p[j].worstIndex = j
}
func (p *WorstQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*MetaTx)
	item.worstIndex = n
	*p = append(*p, x.(*MetaTx))
}
func (p *WorstQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil       // avoid memory leak
	item.worstIndex = -1 // for safety
	*p = old[0 : n-1]
	return item
}

type SubPool struct {
	best  *BestQueue
	worst *WorstQueue
}

func NewSubPool() *SubPool {
	return &SubPool{best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}
func (p *SubPool) Best() *MetaTx {
	if len(*p.best) == 0 {
		return nil
	}
	return (*p.best)[0]
}
func (p *SubPool) Worst() *MetaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *SubPool) PopBest() *MetaTx {
	i := heap.Pop(p.best).(*MetaTx)
	heap.Remove(p.worst, i.worstIndex)
	return i
}
func (p *SubPool) PopWorst() *MetaTx {
	i := heap.Pop(p.worst).(*MetaTx)
	heap.Remove(p.best, i.bestIndex)
	return i
}
func (p *SubPool) Len() int { return p.best.Len() }
func (p *SubPool) Add(i *MetaTx) {
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}
func (p *SubPool) DebugPrint() {
	for i, it := range *p.best {
		fmt.Printf("best: %d, %d, %d\n", i, it.SubPool, it.bestIndex)
	}
	for i, it := range *p.worst {
		fmt.Printf("worst: %d, %d, %d\n", i, it.SubPool, it.worstIndex)
	}
}

const PendingSubPoolLimit = 1024
const BaseFeeSubPoolLimit = 1024
const QueuedSubPoolLimit = 1024

func PromoteStep(pending, baseFee, queued *SubPool) {
	pending.EnforceInvariants()
	baseFee.EnforceInvariants()
	queued.EnforceInvariants()

	//1. If top element in the worst green queue has SubPool != 0b1111 (binary), it needs to be removed from the green pool.
	//   If SubPool < 0b1000 (not satisfying minimum fee), discard.
	//   If SubPool == 0b1110, demote to the yellow pool, otherwise demote to the red pool.
	for worst := pending.Worst(); pending.Len() > 0; worst = pending.Worst() {
		if worst.SubPool >= 0b11110 {
			break
		}
		if worst.SubPool >= 0b11100 {
			baseFee.Add(pending.PopWorst())
			continue
		}
		if worst.SubPool >= 0b10000 {
			queued.Add(pending.PopWorst())
			continue
		}
		pending.PopWorst()
	}

	//2. If top element in the worst green queue has SubPool == 0b1111, but there is not enough room in the pool, discard.
	for worst := pending.Worst(); pending.Len() > PendingSubPoolLimit; worst = pending.Worst() {
		if worst.SubPool >= 0b11111 { // TODO: here must 'SubPool == 0b1111' or 'SubPool <= 0b1111' ?
			break
		}
		pending.PopWorst()
	}

	//3. If the top element in the best yellow queue has SubPool == 0b1111, promote to the green pool.
	for best := baseFee.Best(); baseFee.Len() > 0; best = baseFee.Best() {
		if best.SubPool < 0b11110 {
			break
		}
		pending.Add(baseFee.PopBest())
	}

	//4. If the top element in the worst yellow queue has SubPool != 0x1110, it needs to be removed from the yellow pool.
	//   If SubPool < 0b1000 (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
	for worst := baseFee.Worst(); baseFee.Len() > 0; worst = baseFee.Worst() {
		if worst.SubPool >= 0b11100 {
			break
		}
		if worst.SubPool >= 0b10000 {
			queued.Add(baseFee.PopWorst())
			continue
		}
		baseFee.PopWorst()
	}

	//5. If the top element in the worst yellow queue has SubPool == 0x1110, but there is not enough room in the pool, discard.
	for worst := baseFee.Worst(); baseFee.Len() > BaseFeeSubPoolLimit; worst = baseFee.Worst() {
		if worst.SubPool >= 0b11110 {
			break
		}
		baseFee.PopWorst()
	}

	//6. If the top element in the best red queue has SubPool == 0x1110, promote to the yellow pool. If SubPool == 0x1111, promote to the green pool.
	for best := queued.Best(); queued.Len() > 0; best = queued.Best() {
		if best.SubPool < 0b11100 {
			break
		}
		if best.SubPool < 0b11110 {
			baseFee.Add(queued.PopBest())
			continue
		}

		pending.Add(queued.PopBest())
	}

	//7. If the top element in the worst red queue has SubPool < 0b1000 (not satisfying minimum fee), discard.
	for worst := queued.Worst(); queued.Len() > 0; worst = queued.Worst() {
		if worst.SubPool >= 0b10000 {
			break
		}

		queued.PopWorst()
	}

	//8. If the top element in the worst red queue has SubPool >= 0b100, but there is not enough room in the pool, discard.
	for _ = queued.Worst(); queued.Len() > QueuedSubPoolLimit; _ = queued.Worst() {
		queued.PopWorst()
	}
}

func CheckInvariants(pending, baseFee, queued *SubPool) {
	//1. If top element in the worst green queue has SubPool != 0b1111 (binary), it needs to be removed from the green pool.
	//   If SubPool < 0b1000 (not satisfying minimum fee), discard.
	//   If SubPool == 0b1110, demote to the yellow pool, otherwise demote to the red pool.
	for worst := pending.Worst(); pending.Len() > 0; worst = pending.Worst() {
		if worst.SubPool >= 0b11110 {
			break
		}
		if worst.SubPool >= 0b11100 {
			baseFee.Add(pending.PopWorst())
			continue
		}
		if worst.SubPool >= 0b11000 {
			queued.Add(pending.PopWorst())
			continue
		}
		pending.PopWorst()
	}

	//2. If top element in the worst green queue has SubPool == 0b1111, but there is not enough room in the pool, discard.
	for worst := pending.Worst(); pending.Len() > PendingSubPoolLimit; worst = pending.Worst() {
		if worst.SubPool >= 0b11110 { // TODO: here must 'SubPool == 0b1111' or 'SubPool <= 0b1111' ?
			break
		}
		pending.PopWorst()
	}

	//3. If the top element in the best yellow queue has SubPool == 0b1111, promote to the green pool.
	for best := baseFee.Best(); baseFee.Len() > 0; best = baseFee.Best() {
		if best.SubPool < 0b11110 {
			break
		}
		pending.Add(baseFee.PopWorst())
	}

	//4. If the top element in the worst yellow queue has SubPool != 0x1110, it needs to be removed from the yellow pool.
	//   If SubPool < 0b1000 (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
	for worst := baseFee.Worst(); baseFee.Len() > 0; worst = baseFee.Worst() {
		if worst.SubPool >= 0b11100 {
			break
		}
		if worst.SubPool >= 0b11000 {
			queued.Add(baseFee.PopWorst())
			continue
		}
		baseFee.PopWorst()
	}

	//5. If the top element in the worst yellow queue has SubPool == 0x1110, but there is not enough room in the pool, discard.
	for worst := baseFee.Worst(); baseFee.Len() > BaseFeeSubPoolLimit; worst = baseFee.Worst() {
		if worst.SubPool >= 0b11110 {
			break
		}
		baseFee.PopWorst()
	}

	//6. If the top element in the best red queue has SubPool == 0x1110, promote to the yellow pool. If SubPool == 0x1111, promote to the green pool.
	for best := queued.Best(); queued.Len() > 0; best = queued.Best() {
		if best.SubPool < 0b11100 {
			break
		}
		if best.SubPool < 0b11110 {
			baseFee.Add(queued.PopWorst())
			continue
		}

		pending.Add(queued.PopWorst())
	}

	//7. If the top element in the worst red queue has SubPool < 0b1000 (not satisfying minimum fee), discard.
	for worst := queued.Worst(); queued.Len() > 0; worst = queued.Worst() {
		if worst.SubPool >= 0b10000 {
			break
		}

		queued.PopWorst()
	}

	//8. If the top element in the worst red queue has SubPool >= 0b100, but there is not enough room in the pool, discard.
	for worst := queued.Worst(); queued.Len() > QueuedSubPoolLimit; worst = queued.Worst() {
		if worst.SubPool >= 0b10000 {
			break
		}

		queued.PopWorst()
	}
}

// Below is a draft code, will convert it to Loop and LoopStep funcs later

type PoolImpl struct {
	recentlyConnectedPeers     *recentlyConnectedPeers
	lastTxPropagationTimestamp time.Time
	logger                     log.Logger
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
			initialAmount := len(localTxHashes)
			sentToPeers := send.BroadcastLocalPooledTxs(localTxHashes)
			if initialAmount == 1 {
				p.logger.Info("local tx propagated", "to_peers_amount", sentToPeers, "tx_hash", localTxHashes)
			} else {
				p.logger.Info("local txs propagated", "to_peers_amount", sentToPeers, "txs_amount", initialAmount)
			}

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
	peers []PeerID
}

func (l *recentlyConnectedPeers) AddPeer(p PeerID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []PeerID {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}
