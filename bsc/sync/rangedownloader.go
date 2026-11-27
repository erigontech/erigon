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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

// A generalized fork of the deleted polygon/sync block downloader
// (downloadBlocksUsingWaypoints): parallel, multi-peer, forward range fetch.
// The waypoint work-unit + Merkle-root trust anchor are replaced by fixed
// block-number ranges + structural verification (verifyChain). Everything else
// — the worker pool, gap truncation, per-window peer refresh, RAM-bounded
// worker count, backoff — mirrors bor.

// estBlockSize is a conservative per-block over-estimate (matching bor's
// waypoint sizing) used to bound worker count: a worker buffers a whole range,
// so RAM/worker ≈ rangeSize × estBlockSize.
const estBlockSize = 1 * datasize.MB

// insertFunc persists an ascending, gap-free batch of blocks and reports whether
// the batch was committed as the canonical head. It is the seam bor filled with
// store.InsertBlocks; here it wraps the exec-module client.
type insertFunc func(ctx context.Context, blocks []*types.Block) (committed bool, err error)

// badBlockError reports that execution rejected an inserted block; lastValid is
// the canonical head that execution kept.
type badBlockError struct {
	lastValid *types.Header
}

func (e *badBlockError) Error() string {
	return fmt.Sprintf("bad block after %d (%x)", e.lastValid.Number.Uint64(), e.lastValid.Hash())
}

type rangeService interface {
	ListPeersMayHaveBlockNum(blockNum uint64) []*p2p.PeerId
	FetchHeaders(ctx context.Context, start, end uint64, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error)
	FetchBodies(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error)
	Penalize(ctx context.Context, peerId *p2p.PeerId) error
}

type blockRange struct {
	from, to uint64 // inclusive
}

// rangeSource records the peer that sent an inserted range.
type rangeSource struct {
	blockRange
	peer *p2p.PeerId
}

// downloadRanges fetches [from,to] forward in parallel range-chunks, calling
// insert with each assembled ascending batch. parent is the header at from-1
// (used to verify the first range links to the persisted chain); it may be nil
// only at genesis. It returns when the range is fully inserted or ctx is
// cancelled. Peer backoff uses peerBackoff (poll for peers, not bor's 1m
// steady-state wait).
func downloadRanges(ctx context.Context, logger log.Logger, svc rangeService, from, to uint64, parent *types.Header, insert insertFunc) error {
	maxWorkers := estimate.EstimatedRamPerWorker(datasize.ByteSize(rangeSize) * estBlockSize).WorkersByRAMOnly()
	prev := parent
	fetchStart := time.Now()
	var done uint64
	var uncommitted []rangeSource

	// Ranges are generated lazily from a cursor rather than materialized up front:
	// a full-chain sync (0 → ~126M at 1024/range) would otherwise pre-allocate
	// ~123k range structs. nextFrom is the first not-yet-persisted block.
	for nextFrom := from; nextFrom <= to; {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		peers := svc.ListPeersMayHaveBlockNum(to)
		if len(peers) == 0 {
			logger.Warn("[bsc] no peers for range, backing off", "from", nextFrom, "to", to, "backoff", peerBackoff)
			if err := common.Sleep(ctx, peerBackoff); err != nil {
				return err
			}
			continue
		}

		remaining := (to-nextFrom)/rangeSize + 1
		numWorkers := min(uint64(maxWorkers), uint64(len(peers)), remaining)

		// Build just this batch of ranges (numWorkers of them) from the cursor.
		batch := make([]blockRange, 0, numWorkers)
		f := nextFrom
		for range numWorkers {
			t := min(f+rangeSize-1, to)
			batch = append(batch, blockRange{from: f, to: t})
			f = t + 1
		}

		blockBatches := make([][]*types.Block, len(batch))
		var wg sync.WaitGroup
		for i, r := range batch {
			i, r, peerID := i, r, peers[i]
			wg.Go(func() {
				blocks, err := fetchRangeFromPeer(ctx, svc, r.from, r.to, peerID)
				if err != nil {
					logger.Debug("[bsc] range fetch failed, will retry", "from", r.from, "to", r.to, "peer", peerID, "err", err)
					return // leaves blockBatches[i] nil → treated as a gap
				}
				blockBatches[i] = blocks
			})
		}
		wg.Wait()

		// Assemble in order; verify each range links to the previous (boundary +
		// intra-range + body checks via verifyChain). Stop at the first gap or
		// verification failure so we never insert a hole or a fork; the cursor
		// rewinds to the start of the failed range for retry.
		var assembled []*types.Block
		advanceTo := f // past the whole batch, unless a gap rewinds it
		for i, r := range batch {
			blocks := blockBatches[i]
			if len(blocks) == 0 {
				advanceTo = r.from
				break
			}
			if err := verifyChain(prev, blocks); err != nil {
				logger.Debug("[bsc] range verify failed, will retry", "from", r.from, "to", r.to, "peer", peers[i], "err", err)
				penalize(ctx, logger, svc, peers[i])
				advanceTo = r.from
				break
			}
			assembled = append(assembled, blocks...)
			uncommitted = append(uncommitted, rangeSource{blockRange{r.from, blocks[len(blocks)-1].NumberU64()}, peers[i]})
			prev = blocks[len(blocks)-1].HeaderNoCopy()
		}
		nextFrom = advanceTo

		if len(assembled) == 0 {
			continue
		}

		committed, err := insert(ctx, assembled)
		if bad, ok := errors.AsType[*badBlockError](err); ok {
			badNum := bad.lastValid.Number.Uint64() + 1
			logger.Warn("[bsc] execution rejected block, will retry", "block", badNum, "err", err)
			for _, src := range uncommitted {
				if src.from <= badNum && badNum <= src.to {
					penalize(ctx, logger, svc, src.peer)
				}
			}
			uncommitted = nil
			prev = bad.lastValid
			nextFrom = badNum
			continue
		}
		if err != nil {
			return err
		}
		if committed {
			uncommitted = nil
		}

		done += uint64(len(assembled))
		logger.Info("[bsc] downloaded blocks",
			"to", assembled[len(assembled)-1].NumberU64(),
			"workers", len(batch), "peers", len(peers),
			"blk/s", uint64(float64(done)/time.Since(fetchStart).Seconds()))
	}
	return nil
}

func penalize(ctx context.Context, logger log.Logger, svc rangeService, peer *p2p.PeerId) {
	if err := svc.Penalize(ctx, peer); err != nil {
		logger.Debug("[bsc] failed to penalize peer", "peer", peer, "err", err)
	}
}
