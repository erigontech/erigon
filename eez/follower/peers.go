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
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/types"
)

// ErrPeerHistoryRefused reports a sync from L2 peers whose blocks the local
// chain refuses.
var ErrPeerHistoryRefused = errors.New("the local chain refuses the history from L2 peers")

// statusInterval is the shortest interval between two lines saying why boot
// still waits.
const statusInterval = 30 * time.Second

// every tells when a repeated line is due: at once, then at most every d.
type every struct {
	d    time.Duration
	last time.Time
}

func (e *every) due() bool {
	now := time.Now()
	if !e.last.IsZero() && now.Sub(e.last) < e.d {
		return false
	}
	e.last = now
	return true
}

// bootFromPeers syncs the local chain to the root that L1's finalized block
// F holds, from the chain's L2 peers, seeds derivation at F and returns the
// L1 block to scan from. It waits while L1 cannot name the root or no peer
// serves it, and never derives that history from L1.
func (f *Follower) bootFromPeers(ctx context.Context) (uint64, error) {
	defer f.moving(HeadHistorySync)()
	waiting, syncing := every{d: statusInterval}, every{d: statusInterval}
	// The block the root's chain must keep: the local finalized block's
	// parent for the first sync, the last root for later ones.
	var bound *types.Header
	for {
		c, err := l1.FinalizedCommitment(ctx, f.reader, f.l2.Registry, f.l2.RollupID)
		if err != nil {
			return 0, err
		}
		if c.Outcome == l1.CommitmentWait {
			f.setPhase(PhaseWaitingForFinality)
			if waiting.due() {
				f.logger.Warn("[eez] waiting for L1 to name the finalized L2 root", "event", "eez.node.sync.waiting", "cause", c.Cause)
			}
			if err := pause(ctx, f.cfg.Poll); err != nil {
				return 0, err
			}
			continue
		}
		f.setAnchor(anchorOf(c))
		var number uint64
		local := c.Outcome == l1.CommitmentSeedGenesis
		if local {
			// No history precedes F; the execution module cannot replay below
			// a finalized block of another deployment.
			if err := f.checkGenesisFinalized(ctx, c.Finalized.Number); err != nil {
				return 0, err
			}
		} else if number, local, err = f.chain.CanonicalNumber(ctx, c.Root); err != nil {
			return 0, err
		}
		if local {
			return max(c.Finalized.Number, 1), f.seedAt(ctx, c.Finalized, number)
		}

		f.setPhase(PhaseSyncing)
		if bound == nil {
			if bound, err = f.rewindToFinalized(ctx); err != nil {
				return 0, refused(err)
			}
		}
		if _, err := f.cfg.Peers.Fetch(ctx, c.Root, FetchOptions{Inserted: f.extend(bound)}); err != nil {
			if errors.Is(err, ErrNotOnL1Chain) || errors.Is(err, deriver.ErrForkChoiceRefused) || ctx.Err() != nil {
				return 0, refused(err)
			}
			f.setPhase(PhaseWaitingForPeers)
			if syncing.due() {
				var head uint64
				if h, err := f.chain.Head(ctx); err == nil {
					head = h.Number.Uint64()
				}
				f.logger.Warn("[eez] syncing L2 history from peers to the L1-finalized root", "event", "eez.node.sync.waiting_for_peers",
					"peers", f.cfg.Peers.Peers(), "head", head, "l1Finalized", c.Finalized.Number, "target", c.Root, "err", err)
			}
			if err := pause(ctx, f.cfg.Poll); err != nil {
				return 0, err
			}
			continue
		}
		// A stored root needs no download, so the check on the first
		// downloaded block may not have run.
		if err := f.checkExtends(ctx, c.Root, bound); err != nil {
			return 0, err
		}
		if err := f.chain.ForkChoice(ctx, c.Root, c.Root, c.Root); err != nil {
			return 0, refused(err)
		}
		if bound, err = f.chain.HeaderByHash(ctx, c.Root); err != nil {
			return 0, err
		}
	}
}

// refused marks a fork choice the execution module refused during a sync
// from peers as ErrPeerHistoryRefused: no peer can mend it.
func refused(err error) error {
	if errors.Is(err, deriver.ErrForkChoiceRefused) {
		return fmt.Errorf("%w: %w", ErrPeerHistoryRefused, err)
	}
	return err
}

// checkGenesisFinalized returns ErrNotOnL1Chain when the local chain
// finalized a block above genesis although the rollup has no history before
// L1 block l1Finalized.
func (f *Follower) checkGenesisFinalized(ctx context.Context, l1Finalized uint64) error {
	hash, err := f.chain.Finalized(ctx)
	if err != nil || hash == (common.Hash{}) {
		return err
	}
	n, _, err := f.chain.CanonicalNumber(ctx, hash)
	if err != nil || n == 0 {
		return err
	}
	return fmt.Errorf("%w: the rollup has no history before L1 block %d, the local chain finalized block %d %x", ErrNotOnL1Chain, l1Finalized, n, hash)
}

// checkExtends returns ErrNotOnL1Chain unless the stored block hash
// descends from bound, a canonical block.
func (f *Follower) checkExtends(ctx context.Context, hash common.Hash, bound *types.Header) error {
	for at := hash; ; {
		n, ok, err := f.chain.CanonicalNumber(ctx, at)
		if err != nil {
			return err
		}
		if ok {
			if n < bound.Number.Uint64() {
				return fmt.Errorf("%w: block %x leaves the chain at block %d, below block %d %x", ErrNotOnL1Chain,
					hash, n, bound.Number.Uint64(), bound.Hash())
			}
			return nil
		}
		h, err := f.chain.HeaderByHash(ctx, at)
		if err != nil {
			return err
		}
		if h == nil {
			return fmt.Errorf("block %x is not stored", at)
		}
		at = h.ParentHash
	}
}

// rewindToFinalized makes the local finalized block, genesis before any,
// the head, and its parent finalized, so that a block L1 settled in its
// place may replace it. It returns the block a sync from peers must keep:
// that parent, or genesis.
func (f *Follower) rewindToFinalized(ctx context.Context) (*types.Header, error) {
	hash, err := f.chain.Finalized(ctx)
	if err != nil {
		return nil, err
	}
	var number uint64
	if hash != (common.Hash{}) {
		var ok bool
		if number, ok, err = f.chain.CanonicalNumber(ctx, hash); err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("finalized block %x is not canonical", hash)
		}
	}
	h, err := f.chain.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, err
	}
	if err := f.chain.ForkChoice(ctx, h.Hash(), h.Hash(), h.Hash()); err != nil {
		return nil, err
	}
	f.logger.Info("[eez] syncing L2 history from peers", "event", "eez.node.sync.rewound", "finalized", number)
	if number == 0 {
		return h, nil
	}
	p, err := f.chain.HeaderByNumber(ctx, number-1)
	if err != nil {
		return nil, err
	}
	if err := f.chain.ForkChoice(ctx, h.Hash(), p.Hash(), p.Hash()); err != nil {
		return nil, err
	}
	return p, nil
}

// extend returns the callback of a sync from peers that must keep bound: it
// stops a chain that leaves bound before executing any of it, and every
// LoopBlockLimit inserted blocks executes them and finalizes the latest, an
// ancestor of the root L1 finalized.
func (f *Follower) extend(bound *types.Header) func(context.Context, []*types.Block) error {
	var started bool
	var pending uint64
	return func(ctx context.Context, blocks []*types.Block) error {
		// From the first block, not its parent: the downloader can start at
		// a block it stores.
		if !started {
			if err := f.checkExtends(ctx, blocks[0].Hash(), bound); err != nil {
				return err
			}
			started = true
		}
		if pending += uint64(len(blocks)); f.cfg.LoopBlockLimit == 0 || pending < f.cfg.LoopBlockLimit {
			return nil
		}
		pending = 0
		// Finalized with the head: a restart rewinds no further than the
		// execution module keeps changesets for.
		last := blocks[len(blocks)-1].Hash()
		return f.chain.ForkChoice(ctx, last, last, last)
	}
}

// seedAt starts derivation after L1 block finalized, whose settlements end
// at L2 block number, and makes finalized the latest finalized L1 block.
func (f *Follower) seedAt(ctx context.Context, finalized l1.Header, number uint64) error {
	r := deriver.Record{L1Block: finalized.Number, L1BlockHash: finalized.Hash, L1Time: finalized.Time, L1MixDigest: finalized.MixDigest, LastL2Block: number}
	if err := f.deriver.Seed(ctx, r); err != nil {
		return err
	}
	f.anchorReached(number)
	f.floor, f.l1Finalized = finalized.Number, max(f.l1Finalized, finalized.Number)
	f.logger.Info("[eez] deriving from L1 finality", "event", "eez.node.sync.seeded", "l1Block", finalized.Number, "l2Block", number)
	return nil
}

// pause waits d or until ctx ends.
func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
