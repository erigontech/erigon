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

package follower

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

// SyncMode is where boot takes the history below L1 finality from.
type SyncMode string

const (
	// SyncL1Only derives the whole history from L1.
	SyncL1Only SyncMode = "l1_only"
	// SyncPeers takes the history below the root L1 finalized from L2 peers.
	SyncPeers SyncMode = "peers"
)

// Phase is how far a follower is from following L1.
type Phase string

const (
	// PhaseBooting: boot has not yet chosen where derivation starts.
	PhaseBooting Phase = "booting"
	// PhaseWaitingForFinality: L1 names no finalized root yet, having no
	// finalized block or failing to read the registry at it.
	PhaseWaitingForFinality Phase = "waiting_for_finality"
	// PhaseSyncing: the history below the finalized root is being fetched
	// from L2 peers.
	PhaseSyncing Phase = "syncing"
	// PhaseWaitingForPeers: no L2 peer serves the history.
	PhaseWaitingForPeers Phase = "waiting_for_peers"
	// PhaseDeriving: boot derives the settlements up to the L1 head.
	PhaseDeriving Phase = "deriving"
	// PhaseFollowing: boot is done; the follower derives each new L1 block.
	PhaseFollowing Phase = "following"
)

// HeadSource is what moved the head last.
type HeadSource string

const (
	// HeadDerivation: blocks derived from L1 settlements.
	HeadDerivation HeadSource = "derivation"
	// HeadHistorySync: the sync from L2 peers to the root L1 finalized.
	HeadHistorySync HeadSource = "history_sync"
	// HeadSignedHead: a head the sequencer signed.
	HeadSignedHead HeadSource = "signed_head"
	// HeadSequencerRPC: the sequencer's latest block over JSON-RPC.
	HeadSequencerRPC HeadSource = "sequencer_rpc"
)

// Anchor is L1's finalized block F and the root H_F the registry holds for
// the rollup at F, as boot read them.
type Anchor struct {
	L1Block     uint64
	L1BlockHash common.Hash
	// Root is H_F, nil when the rollup is not registered at F: its history
	// then starts at L2 genesis.
	Root *common.Hash
	// L2Block is H_F's number once the local chain holds it.
	L2Block *uint64
}

func anchorOf(c l1.Commitment) *Anchor {
	a := &Anchor{L1Block: c.Finalized.Number, L1BlockHash: c.Finalized.Hash}
	if c.Outcome == l1.CommitmentAnchor {
		root := c.Root
		a.Root = &root
	} else {
		var genesis uint64
		a.L2Block = &genesis
	}
	return a
}

// Status returns the follower's progress as of its latest step, with its
// peers and signed heads as of the call. It is safe to call concurrently
// with Run.
func (f *Follower) Status() Status {
	f.statusMu.Lock()
	s := f.status
	f.statusMu.Unlock()
	if f.peers != nil {
		s.Peers = f.peers.latest()
	}
	if f.cfg.OverlayPeerCount != nil {
		n := uint64(max(f.cfg.OverlayPeerCount(), 0))
		s.OverlayPeers = &n
	}
	if f.heads != nil {
		s.SignedHeads = f.heads.tracker.Candidates()
	}
	return s
}

func (f *Follower) setPhase(p Phase) {
	f.statusMu.Lock()
	f.status.Phase = p
	f.statusMu.Unlock()
}

func (f *Follower) setAnchor(a *Anchor) {
	f.statusMu.Lock()
	f.status.Anchor = a
	f.statusMu.Unlock()
}

// anchorReached records number as the height of the anchor's root.
func (f *Follower) anchorReached(number uint64) {
	f.statusMu.Lock()
	defer f.statusMu.Unlock()
	if f.status.Anchor == nil {
		return
	}
	// A copy: Status hands out the pointer.
	a := *f.status.Anchor
	a.L2Block = &number
	f.status.Anchor = &a
}

func (f *Follower) setHeadSource(s HeadSource) {
	f.statusMu.Lock()
	f.status.HeadSource = s
	f.statusMu.Unlock()
}

// moving makes s the source of the head moves until the returned func
// restores the previous one. Every step that may move the head names its
// source; a move outside them is reported as made by none.
func (f *Follower) moving(s HeadSource) func() {
	prev := f.watch.mover
	f.watch.mover = s
	return func() { f.watch.mover = prev }
}

// headWatch is the chain of the follower and of its deriver: it reports
// each move of the head with the source that made it. Every move runs on
// the follower's goroutine.
type headWatch struct {
	deriver.Chain
	f     *Follower
	mover HeadSource
	head  common.Hash
}

// seed takes the current head as moved by nobody since start.
func (w *headWatch) seed(ctx context.Context) error {
	h, err := w.Chain.Head(ctx)
	if err != nil {
		return err
	}
	w.head = h.Hash()
	return nil
}

func (w *headWatch) moved(head common.Hash) {
	if head == w.head {
		return
	}
	w.head = head
	w.f.setHeadSource(w.mover)
}

func (w *headWatch) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	if err := w.Chain.ForkChoice(ctx, head, safe, finalized); err != nil {
		return err
	}
	w.moved(head)
	return nil
}

func (w *headWatch) Extend(ctx context.Context, parent *types.Header, in deriver.HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	b, err := w.Chain.Extend(ctx, parent, in, txs, safe, finalized)
	if err == nil {
		w.moved(b.Block.Hash())
	}
	return b, err
}

// peerCountTimeout bounds a count of the devp2p peers.
const peerCountTimeout = time.Second

// peerCount is the latest count of the connected devp2p peers. Counting asks
// the sentry, so it runs in the background, at most once every interval and
// within peerCountTimeout, and readers take the latest count.
type peerCount struct {
	count func(context.Context) (uint64, error)
	every time.Duration
	n     atomic.Uint64

	mu      sync.Mutex
	started time.Time
	running bool
}

// latest returns the latest count and starts another when it is due.
func (p *peerCount) latest() uint64 {
	p.refresh()
	return p.n.Load()
}

// refresh starts a count unless one runs or the last started less than
// every ago.
func (p *peerCount) refresh() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running || time.Since(p.started) < p.every {
		return
	}
	p.running, p.started = true, time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), peerCountTimeout)
		defer cancel()
		if n, err := p.count(ctx); err == nil {
			p.n.Store(n)
		}
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()
}

// run refreshes the count every interval until ctx ends, through boot and
// long derivation steps.
func (p *peerCount) run(ctx context.Context) {
	t := time.NewTicker(p.every)
	defer t.Stop()
	for {
		p.refresh()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
