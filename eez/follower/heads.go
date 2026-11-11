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
	"math"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/seqsig"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

// maxLoggedRejections bounds the remembered rejections: a rogue signer can
// mint new hashes forever.
const maxLoggedRejections = 256

// BlockFetcher fetches blocks by hash from L2 peers into the local chain
// without moving its fork choice.
type BlockFetcher interface {
	Fetch(ctx context.Context, hash common.Hash, opts FetchOptions) (*types.Header, error)
	// Peers returns the number of connected L2 peers.
	Peers() uint64
}

// signedHeads makes the head the highest-version signed head that descends
// from the safe block, fetching its missing ancestors from L2 peers. Safe and
// finalized stay derivation's: blocks are inserted, then the head alone
// moves, and derivation reconciles the head's blocks with what L1 settles.
type signedHeads struct {
	tracker seqsig.Tracker
	chain   deriver.Chain
	deriver *deriver.Deriver
	fetcher BlockFetcher
	// retry is the interval between decisions without a new head; request
	// bounds each peer request of a fetch.
	retry, request time.Duration
	// bad holds the targets that failed validation, not executed again;
	// logged the rejections logged, so a head retried every tick warns once.
	bad    bounded[common.Hash]
	logged bounded[rejection]
	// noPeers and ahead are the waits the last decision ended in, logged on
	// entry and at debug while they last.
	noPeers, ahead bool
	logger         log.Logger
}

// observe tracks h, a head the overlay verified. It is safe to call
// concurrently with follow.
func (s *signedHeads) observe(h *seqsig.VerifiedHead) {
	if o, _ := s.tracker.Observe(h, uint64(time.Now().UnixMilli())); o != seqsig.ObservedNew {
		s.logger.Debug("[eez] signed head not tracked", "number", h.Head.Number, "hash", h.Head.BlockHash, "observation", o)
	}
}

// follow moves the head toward the highest-version candidate that descends
// from safe. Each fetch inserts blocks the next decision finds local, so a
// decision is repeated after every fetch, which also checks the hold again
// before the import.
func (s *signedHeads) follow(ctx context.Context) error {
	in := seqsig.Inputs{Candidates: s.tracker.Candidates(), Fetched: map[common.Hash][]*types.Block{}}
	if len(in.Candidates) == 0 {
		return nil
	}
	in.Peers = s.peerCount()
	local := chainView{ctx: ctx, chain: s.chain, deriver: s.deriver}
	var fetched int
	for {
		in.Indexed = s.deriver.Cursor()
		d, err := seqsig.Decide(local, in)
		if err != nil {
			return err
		}
		for _, skip := range d.Skipped {
			s.reject(skip.Head, skip.Err)
		}
		switch d.Action {
		case seqsig.Fetch:
			n, err := s.fetchBlocks(ctx, d.From, d.Count)
			fetched += n
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				loadMetrics().headFetchFailed.Inc()
				// Unknown ancestry is not a conflict: a lower version must
				// not win in its place.
				s.report(d.Target, err, in.Indexed)
				return nil
			}
			// A block fetched is stored or not served: it is not asked for
			// again within this decision.
			in.Fetched[d.From] = nil
			continue
		case seqsig.Adopt:
			return s.move(ctx, d.Target, d.Blocks, fetched, in.Indexed)
		case seqsig.Rewind:
			// A target the node has may be a canonical ancestor or the tip
			// of blocks a fetch inserted.
			return s.move(ctx, d.Target, nil, fetched, in.Indexed)
		}
		s.report(d.Target, d.Err, in.Indexed)
		return nil
	}
}

// move inserts blocks and makes target the head, keeping safe and finalized.
func (s *signedHeads) move(ctx context.Context, target *seqsig.VerifiedHead, blocks []*types.Block, fetched int, indexed uint64) error {
	h := target.Head
	if s.bad.has(h.BlockHash) {
		return nil
	}
	prev, err := s.chain.Head(ctx)
	if err == nil && len(blocks) > 0 {
		err = s.chain.Insert(ctx, blocks)
	}
	if err == nil {
		err = s.deriver.ForkChoiceTo(ctx, h.BlockHash)
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Anything but a block that fails validation may pass on a retry.
		if errors.Is(err, deriver.ErrBadBlock) {
			s.bad.add(h.BlockHash)
		}
		s.report(target, err, indexed)
		return nil
	}
	s.report(target, nil, indexed)
	s.countMove(ctx, prev)
	s.logger.Info("[eez] head follows the signed head", "event", "eez.node.follower.head.advanced",
		"number", h.Number, "hash", h.BlockHash, "imported", fetched+len(blocks))
	return nil
}

// countMove counts a move of the head from prev: adopted while prev stays
// canonical, rewound when the move dropped it, to an ancestor or another
// branch.
func (s *signedHeads) countMove(ctx context.Context, prev *types.Header) {
	hash, ok, err := s.chain.CanonicalHash(ctx, prev.Number.Uint64())
	if err != nil {
		return
	}
	if m := loadMetrics(); ok && hash == prev.Hash() {
		m.headAdopted.Inc()
	} else {
		m.headRewound.Inc()
	}
}

// fetchBlocks fetches the blocks up to hash the local chain lacks, at most
// seqsig.MaxAncestryWalk of them, count per request, and returns how many it
// inserted.
func (s *signedHeads) fetchBlocks(ctx context.Context, hash common.Hash, count uint64) (int, error) {
	var inserted int
	_, err := s.fetcher.Fetch(ctx, hash, FetchOptions{
		Limit:   seqsig.MaxAncestryWalk,
		Batch:   count,
		Timeout: s.request,
		Inserted: func(_ context.Context, blocks []*types.Block) error {
			inserted += len(blocks)
			return nil
		},
	})
	if errors.Is(err, execp2p.ErrChainLengthExceedsLimit) {
		err = fmt.Errorf("%w: %w", seqsig.ErrUnverifiable, err)
	}
	return inserted, err
}

// peerCount returns the connected L2 peers a fetch can ask, none without a
// fetcher.
func (s *signedHeads) peerCount() int {
	if s.fetcher == nil {
		return 0
	}
	return int(min(s.fetcher.Peers(), math.MaxInt))
}

// report logs why a decision on target ends with reason, nil when it moved or
// needs to move nothing.
func (s *signedHeads) report(target *seqsig.VerifiedHead, reason error, indexed uint64) {
	noPeers := errors.Is(reason, seqsig.ErrNoPeers) || errors.Is(reason, ErrNoPeers)
	ahead := errors.Is(reason, seqsig.ErrAhead)
	switch {
	case reason == nil:
	case noPeers:
		s.wait(&s.noPeers, target, reason, log.LvlWarn, "[eez] signed head not followed", "eez.node.follower.head.rejected")
	case ahead:
		loadMetrics().headAhead.Inc()
		s.wait(&s.ahead, target, reason, log.LvlInfo, "[eez] signed head not followed yet, waiting for derivation", "eez.node.follower.head.ahead")
	case errors.Is(reason, seqsig.ErrHeld):
		loadMetrics().headHeld.Inc()
		s.logger.Debug("[eez] safe lags the L1 batch index, holding the head", "event", "eez.node.follower.head.held",
			"indexed", indexed, "safe", s.deriver.Safe())
	default:
		s.reject(target, reason)
	}
	s.noPeers, s.ahead = noPeers, ahead
}

// wait logs that target waits for reason: at lvl when the wait begins, at
// debug for each further head while it lasts.
func (s *signedHeads) wait(waiting *bool, target *seqsig.VerifiedHead, reason error, lvl log.Lvl, msg, event string) {
	h := target.Head
	if !s.logged.add(rejection{h.BlockHash, reason.Error()}) {
		return
	}
	if *waiting {
		lvl = log.LvlDebug
	}
	s.logger.Log(lvl, msg, "event", event, "number", h.Number, "hash", h.BlockHash, "reason", reason)
}

// reject logs that head is not followed, once per head and reason.
func (s *signedHeads) reject(head *seqsig.VerifiedHead, reason error) {
	if h := head.Head; s.logged.add(rejection{h.BlockHash, reason.Error()}) {
		loadMetrics().headRejected.Inc()
		s.logger.Warn("[eez] signed head not followed", "event", "eez.node.follower.head.rejected",
			"number", h.Number, "hash", h.BlockHash, "reason", reason)
	}
}

// bounded is a set that starts over once it holds maxLoggedRejections
// entries.
type bounded[K comparable] struct {
	m map[K]struct{}
}

// add adds k and reports whether it is new.
func (b *bounded[K]) add(k K) bool {
	if b.has(k) {
		return false
	}
	if b.m == nil || len(b.m) >= maxLoggedRejections {
		b.m = make(map[K]struct{})
	}
	b.m[k] = struct{}{}
	return true
}

func (b *bounded[K]) has(k K) bool {
	_, ok := b.m[k]
	return ok
}

type rejection struct {
	hash   common.Hash
	reason string
}

// chainView is the local chain as the signed-head fork choice reads it, with
// derivation's safe block.
type chainView struct {
	ctx     context.Context
	chain   deriver.Chain
	deriver *deriver.Deriver
}

func (v chainView) Head() (seqsig.BlockID, error) {
	h, err := v.chain.Head(v.ctx)
	if err != nil {
		return seqsig.BlockID{}, err
	}
	return seqsig.BlockID{Number: h.Number.Uint64(), Hash: h.Hash()}, nil
}

func (v chainView) Safe() (seqsig.BlockID, bool, error) {
	n := v.deriver.Safe()
	hash, ok, err := v.chain.CanonicalHash(v.ctx, n)
	if err != nil {
		return seqsig.BlockID{}, false, err
	}
	// Derivation keeps its safe block canonical; without one every head
	// would pass for a descendant.
	if !ok {
		return seqsig.BlockID{}, false, fmt.Errorf("safe block %d is not canonical", n)
	}
	return seqsig.BlockID{Number: n, Hash: hash}, true, nil
}

func (v chainView) Header(hash common.Hash) (uint64, common.Hash, bool, error) {
	h, err := v.chain.HeaderByHash(v.ctx, hash)
	if err != nil || h == nil {
		return 0, common.Hash{}, false, err
	}
	return h.Number.Uint64(), h.ParentHash, true, nil
}

func (v chainView) CanonicalHash(number uint64) (common.Hash, bool, error) {
	return v.chain.CanonicalHash(v.ctx, number)
}
