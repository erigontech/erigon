// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"context"
	"time"

	"github.com/go-stack/stack"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/time/rate"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Bound both retained payloads and fresh-hash simulation work per peer.
// These gossip limits do not apply to local submissions or block execution.
const (
	remoteFrameQueueLimit = 64
	remoteFramePeerLimit  = 4
	remoteFramePeerRate   = 16 // frame transactions per second
)

type remoteFrameTxn struct {
	txn    *TxnSlot
	sender common.Address
	source remoteSource
}

// The queue has one consumer owned by Run. Pending hashes remain known while
// simulation runs, including after the ordinary remote queue has been drained.
// The maps and queue writes require the pool lock.
type remoteFrameQueue struct {
	ready      chan remoteFrameTxn
	pending    map[string]struct{}
	peerCounts map[[64]byte]int
	peerRates  *simplelru.LRU[[64]byte, *rate.Limiter]
}

func newRemoteFrameQueue() (remoteFrameQueue, error) {
	peerRates, err := simplelru.NewLRU[[64]byte, *rate.Limiter](1024, nil)
	if err != nil {
		return remoteFrameQueue{}, err
	}
	return remoteFrameQueue{
		ready:      make(chan remoteFrameTxn, remoteFrameQueueLimit),
		pending:    make(map[string]struct{}),
		peerCounts: make(map[[64]byte]int),
		peerRates:  peerRates,
	}, nil
}

func (s remoteSource) peerKey() [64]byte {
	if s.peerID == nil {
		return [64]byte{}
	}
	return gointerfaces.ConvertH512ToHash(s.peerID)
}

func (q *remoteFrameQueue) reserve(txn *TxnSlot, source remoteSource, now time.Time) bool {
	peer := source.peerKey()
	if len(q.pending) >= remoteFrameQueueLimit || q.peerCounts[peer] >= remoteFramePeerLimit {
		return false
	}
	limiter, ok := q.peerRates.Get(peer)
	if !ok {
		limiter = rate.NewLimiter(remoteFramePeerRate, remoteFramePeerLimit)
		q.peerRates.Add(peer, limiter)
	}
	if !limiter.AllowN(now, 1) {
		return false
	}
	q.pending[string(txn.IDHash[:])] = struct{}{}
	q.peerCounts[peer]++
	return true
}

func (q *remoteFrameQueue) release(txn *TxnSlot, source remoteSource) {
	delete(q.pending, string(txn.IDHash[:]))
	peer := source.peerKey()
	if q.peerCounts[peer]--; q.peerCounts[peer] == 0 {
		delete(q.peerCounts, peer)
	}
}

// Ordinary transactions are admitted before dispatching frames to the worker.
// The returned reasons refer only to ordinary transactions; frame admission is asynchronous.
func (p *TxPool) processRemoteBatch(ctx context.Context) ([]txpoolcfg.DiscardReason, error) {
	p.lock.Lock()
	txns := *p.unprocessedRemoteTxns
	sources := p.unprocessedRemotePeers
	p.lock.Unlock()

	// Run is the only consumer. Keep this prefix in the input queue during admission
	// so duplicate detection still covers it and new arrivals can append.
	var ordinary TxnSlots
	var ordinarySources []remoteSource
	var frames []remoteFrameTxn
	for i, txn := range txns.Txns {
		if txn.TxType() == FrameTxnType {
			frames = append(frames, remoteFrameTxn{txn, txns.Senders.AddressAt(i), sources[i]})
		} else {
			ordinary.Append(txn, txns.Senders.At(i), false)
			ordinarySources = append(ordinarySources, sources[i])
		}
	}
	var reasons []txpoolcfg.DiscardReason
	if len(ordinary.Txns) > 0 {
		var err error
		reasons, err = p.addNewTxns(ctx, ordinary, ordinarySources)
		if err != nil {
			return reasons, err
		}
	}
	kzgOffenders := make(map[[64]byte]struct{})
	if len(frames) > 0 {
		for i, reason := range reasons {
			if reason == txpoolcfg.UnmatchedBlobTxExt {
				kzgOffenders[ordinarySources[i].peerKey()] = struct{}{}
			}
		}
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, frame := range frames {
		if _, offender := kzgOffenders[frame.source.peerKey()]; offender {
			p.remoteFrames.release(frame.txn, frame.source)
			continue
		}
		select {
		case p.remoteFrames.ready <- frame:
		default:
			p.remoteFrames.release(frame.txn, frame.source)
		}
	}
	p.removeProcessedRemoteTxns(len(txns.Txns))
	return reasons, nil
}

func (p *TxPool) runRemoteFrames(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame := <-p.remoteFrames.ready:
			if ctx.Err() != nil {
				return
			}
			p.processRemoteFrame(ctx, frame)
		}
	}
}

func (p *TxPool) processRemoteFrame(ctx context.Context, frame remoteFrameTxn) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("[txpool] remote frame admission panic", "hash", frame.txn.IDHash, "err", r, "stack", stack.Trace().String())
		}
		p.lock.Lock()
		p.remoteFrames.release(frame.txn, frame.source)
		p.lock.Unlock()
	}()
	var txns TxnSlots
	txns.Append(frame.txn, frame.sender[:], false)
	_, err := p.addNewTxns(ctx, txns, []remoteSource{frame.source})
	if err != nil && ctx.Err() == nil {
		p.logger.Warn("[txpool] remote frame admission failed", "hash", frame.txn.IDHash, "err", err)
	}
}
