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

package bscsync

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

// A generalized fork of the deleted polygon/sync block downloader
// (downloadBlocksUsingWaypoints): parallel, multi-peer, forward range fetch.
// The waypoint work-unit + Merkle-root trust anchor are replaced by fixed
// block-number ranges + structural verification (verifyChain). Everything else
// — the RAM-bounded worker count, backoff — mirrors bor.

// estBlockSize is a conservative per-block over-estimate (matching bor's
// waypoint sizing) used to bound worker count: a worker buffers a whole range,
// so RAM/worker ≈ rangeSize × estBlockSize.
const estBlockSize = 1 * datasize.MB

var (
	rangeSize     = dbg.EnvUint("BSC_BLOCK_RANGE_SIZE", 1024)
	rangeDeadline = dbg.EnvDuration("BSC_BLOCK_RANGE_DEADLINE", 2*time.Minute)
)

// insertFunc persists an ascending, gap-free batch of blocks. It is the seam
// bor filled with store.InsertBlocks; here it wraps the exec-module client.
type insertFunc func(ctx context.Context, blocks []*types.Block) error

type blockRange struct {
	from, to uint64 // inclusive
	deadline time.Duration
	failed   map[p2p.PeerId]bool
}

type rangeResult struct {
	r        blockRange
	peer     p2p.PeerId
	blocks   []*types.Block
	took     time.Duration
	timedOut bool
	err      error
}

func maxRangesByRAM() int {
	return estimate.EstimatedRamPerWorker(datasize.ByteSize(rangeSize) * estBlockSize).WorkersByRAMOnly()
}

// downloadRanges fetches [from,target()] forward in parallel range-chunks, calling
// insert with each assembled ascending batch. target is evaluated after every range result. parent is the header at from-1
// (used to verify the first range links to the persisted chain); it may be nil
// only at genesis. It returns when the cursor passes the target or ctx is
// cancelled. Peer backoff uses peerBackoff (poll for peers, not bor's 1m
// steady-state wait).
func downloadRanges(ctx context.Context, logger log.Logger, svc rangeFetcher, from uint64, target func() uint64, parent *types.Header, maxRanges int, insert insertFunc) error {
	fetchCtx, cancelFetches := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancelFetches()
	results := make(chan rangeResult, maxRanges)

	prev := parent
	fetchStart := time.Now()
	var done uint64
	cursor, next := from, from
	var queued []blockRange
	held := make(map[uint64]rangeResult)
	busy := make(map[p2p.PeerId]bool)
	rates := make(map[p2p.PeerId]float64)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		to := target()
		if cursor > to {
			return nil
		}

		for len(queued)+len(busy)+len(held) < maxRanges && next <= to {
			end := min(next+rangeSize-1, to)
			queued = append(queued, blockRange{from: next, to: end, deadline: rangeDeadline})
			next = end + 1
		}
		waiting := queued[:0]
		for _, r := range queued {
			var peer *p2p.PeerId
			if r.from <= to {
				peer = fastestIdlePeer(svc.ListPeersMayHaveBlockNum(r.from), busy, r.failed, rates)
			}
			if peer == nil {
				waiting = append(waiting, r)
				continue
			}
			if r.to > to {
				waiting = append(waiting, blockRange{from: to + 1, to: r.to, deadline: r.deadline})
				r.to = to
			}
			busy[*peer] = true
			wg.Go(func() {
				select {
				case results <- fetchRange(fetchCtx, svc, r, peer):
				case <-fetchCtx.Done():
				}
			})
		}
		queued = waiting

		if len(busy) == 0 {
			logger.Warn("[bsc] no peers for range, backing off", "from", cursor, "to", to, "backoff", peerBackoff)
			for i := range queued {
				queued[i].failed = nil
			}
			if err := common.Sleep(ctx, peerBackoff); err != nil {
				return err
			}
			continue
		}

		var res rangeResult
		select {
		case res = <-results:
		case <-ctx.Done():
			return ctx.Err()
		}
		delete(busy, res.peer)
		if res.err != nil {
			logger.Debug("[bsc] range fetch failed, will retry", "from", res.r.from, "to", res.r.to, "peer", res.peer.String(), "timedOut", res.timedOut, "err", res.err)
			if res.timedOut {
				res.r.deadline *= 2
			}
			delete(rates, res.peer)
			queued = requeue(queued, res.r, res.peer)
			continue
		}
		rates[res.peer] = float64(len(res.blocks)) / res.took.Seconds()
		held[res.r.from] = res

		var batch []*types.Block
		for h, ok := held[cursor]; ok; h, ok = held[cursor] {
			delete(held, cursor)
			if err := verifyChain(prev, h.blocks); err != nil {
				logger.Debug("[bsc] range verify failed, will retry", "from", h.r.from, "to", h.r.to, "peer", h.peer.String(), "err", err)
				delete(rates, h.peer)
				queued = requeue(queued, h.r, h.peer)
				break
			}
			batch = append(batch, h.blocks...)
			prev = h.blocks[len(h.blocks)-1].HeaderNoCopy()
			cursor = h.r.to + 1
		}
		if len(batch) == 0 {
			continue
		}
		if err := insert(ctx, batch); err != nil {
			return err
		}

		done += uint64(len(batch))
		logger.Info("[bsc] downloaded blocks",
			"to", cursor-1, "inFlight", len(busy), "held", len(held), "peers", len(svc.ListPeersMayHaveBlockNum(cursor)),
			"blk/s", uint64(float64(done)/time.Since(fetchStart).Seconds()))
	}
}

// fetchRange cancels on the deadline instead of using a context deadline: a
// context.DeadlineExceeded makes TrackingFetcher mark the peer as missing the range.
func fetchRange(ctx context.Context, svc rangeFetcher, r blockRange, peer *p2p.PeerId) rangeResult {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(r.deadline, cancel)
	start := time.Now()
	blocks, err := fetchRangeFromPeer(ctx, svc, r.from, r.to, peer)
	return rangeResult{r: r, peer: *peer, blocks: blocks, took: time.Since(start), timedOut: !timer.Stop(), err: err}
}

func fastestIdlePeer(peers []*p2p.PeerId, busy, failed map[p2p.PeerId]bool, rates map[p2p.PeerId]float64) *p2p.PeerId {
	var best *p2p.PeerId
	for _, peer := range peers {
		if busy[*peer] || failed[*peer] {
			continue
		}
		if best == nil || rates[*peer] > rates[*best] {
			best = peer
		}
	}
	return best
}

func requeue(queued []blockRange, r blockRange, peer p2p.PeerId) []blockRange {
	if r.failed == nil {
		r.failed = make(map[p2p.PeerId]bool)
	}
	r.failed[peer] = true
	i, _ := slices.BinarySearchFunc(queued, r.from, func(q blockRange, from uint64) int { return cmp.Compare(q.from, from) })
	return slices.Insert(queued, i, r)
}
