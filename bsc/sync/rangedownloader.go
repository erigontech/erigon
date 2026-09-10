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
	"sync"
	"time"

	"github.com/c2h5oh/datasize"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
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

// insertFunc persists an ascending, gap-free batch of blocks. It is the seam
// bor filled with store.InsertBlocks; here it wraps the exec-module client.
type insertFunc func(ctx context.Context, blocks []*types.Block) error

type blockRange struct {
	from, to uint64 // inclusive
}

type rangeDownloader struct {
	logger       log.Logger
	svc          *bscp2p.Service
	rangeSize    uint64
	maxWorkers   int
	retryBackOff time.Duration
}

func newRangeDownloader(logger log.Logger, svc *bscp2p.Service, rangeSize uint64) *rangeDownloader {
	return &rangeDownloader{
		logger:       logger,
		svc:          svc,
		rangeSize:    rangeSize,
		maxWorkers:   estimate.EstimatedRamPerWorker(datasize.ByteSize(rangeSize) * estBlockSize).WorkersByRAMOnly(),
		retryBackOff: peerBackoff, // poll for peers, not bor's 1m steady-state wait
	}
}

// download fetches [from,to] forward in parallel range-chunks, calling insert
// with each assembled ascending batch. parent is the header at from-1 (used to
// verify the first range links to the persisted chain); it may be nil only at
// genesis. It returns when the range is fully inserted or ctx is cancelled.
func (d *rangeDownloader) download(ctx context.Context, from, to uint64, parent *types.Header, insert insertFunc) error {
	prev := parent
	fetchStart := time.Now()
	var done uint64

	// Ranges are generated lazily from a cursor rather than materialized up front:
	// a full-chain sync (0 → ~126M at 1024/range) would otherwise pre-allocate
	// ~123k range structs. nextFrom is the first not-yet-persisted block.
	for nextFrom := from; nextFrom <= to; {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		peers := d.svc.ListPeersMayHaveBlockNum(to)
		if len(peers) == 0 {
			d.logger.Warn("[bsc] no peers for range, backing off", "from", nextFrom, "to", to, "backoff", d.retryBackOff)
			if err := common.Sleep(ctx, d.retryBackOff); err != nil {
				return err
			}
			continue
		}

		remaining := (to-nextFrom)/d.rangeSize + 1
		numWorkers := min(uint64(d.maxWorkers), uint64(len(peers)), remaining)

		// Build just this batch of ranges (numWorkers of them) from the cursor.
		batch := make([]blockRange, 0, numWorkers)
		f := nextFrom
		for range numWorkers {
			t := min(f+d.rangeSize-1, to)
			batch = append(batch, blockRange{from: f, to: t})
			f = t + 1
		}

		blockBatches := make([][]*types.Block, len(batch))
		var wg sync.WaitGroup
		for i, r := range batch {
			i, r, peerID := i, r, peers[i]
			wg.Go(func() {
				blocks, err := fetchRangeFromPeer(ctx, d.svc, r.from, r.to, peerID)
				if err != nil {
					d.logger.Debug("[bsc] range fetch failed, will retry", "from", r.from, "to", r.to, "peer", peerID, "err", err)
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
				d.logger.Debug("[bsc] range verify failed, will retry", "from", r.from, "to", r.to, "err", err)
				advanceTo = r.from
				break
			}
			assembled = append(assembled, blocks...)
			prev = blocks[len(blocks)-1].HeaderNoCopy()
		}
		nextFrom = advanceTo

		if len(assembled) == 0 {
			continue
		}

		if err := insert(ctx, assembled); err != nil {
			return err
		}

		done += uint64(len(assembled))
		d.logger.Info("[bsc] downloaded blocks",
			"to", assembled[len(assembled)-1].NumberU64(),
			"workers", len(batch), "peers", len(peers),
			"blk/s", uint64(float64(done)/time.Since(fetchStart).Seconds()))
	}
	return nil
}
