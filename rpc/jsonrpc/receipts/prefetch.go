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

package receipts

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// DEBUG ONLY: prefetch outcome counters and timings, logged per window of dbgEvery Receipts calls.
var (
	dbgStarted, dbgBeginFail, dbgCanonFail, dbgReadErr, dbgIncomplete, dbgServed, dbgCalls          atomic.Int64
	dbgWaitZero, dbgSchedNs, dbgBeginNs, dbgCanonNs, dbgReadNs, dbgPrefetchNs, dbgHashNs, dbgWaitNs atomic.Int64
	// DbgBlockNs is the blockWithSenders time of GetBlockReceipts.
	DbgBlockNs atomic.Int64
	dbgPrev    [16]int64
)

const dbgEvery = 2000

func dbgLog() {
	n := dbgCalls.Add(1)
	if n%dbgEvery != 0 {
		return
	}
	cur := [16]int64{n, dbgStarted.Load(), dbgBeginFail.Load(), dbgCanonFail.Load(), dbgReadErr.Load(), dbgIncomplete.Load(),
		dbgServed.Load(), dbgWaitZero.Load(), dbgSchedNs.Load(), dbgBeginNs.Load(), dbgCanonNs.Load(), dbgReadNs.Load(),
		dbgPrefetchNs.Load(), dbgHashNs.Load(), dbgWaitNs.Load(), DbgBlockNs.Load()}
	var d [16]int64
	for i := range cur {
		d[i] = cur[i] - dbgPrev[i]
	}
	dbgPrev = cur
	calls, started := max(d[0], 1), max(d[1], 1)
	us := func(ns, cnt int64) int64 { return ns / cnt / 1000 }
	log.Info("[dbg] receipts prefetch window", "calls", n, "started", d[1], "beginFail", d[2], "canonFail", d[3],
		"readErr", d[4], "incomplete", d[5], "served", d[6], "waitZero", d[7],
		"schedUs", us(d[8], started), "beginUs", us(d[9], started), "canonUs", us(d[10], started), "readUs", us(d[11], started),
		"prefetchUs", us(d[12], started), "blockUs", us(d[15], calls), "hashUs", us(d[13], calls), "waitUs", us(d[14], calls))
}

// ReceiptsPrefetch reads the persisted receipts of one block on its own read tx,
// while the caller reads the block on its tx.
type ReceiptsPrefetch struct {
	cancel   context.CancelFunc
	done     chan struct{}
	receipts types.Receipts
}

// PrefetchReceipts starts the read of the persisted receipts of the block blockNum/blockHash.
// It returns nil when the receipts would not be served from the persistent cache.
func (g *Generator) PrefetchReceipts(ctx context.Context, cfg *chain.Config, db kv.TemporalRoDB, blockNum uint64, blockHash common.Hash, opts eth.ReceiptsOpts) *ReceiptsPrefetch {
	if !PersistedReceiptsServed() || PostStateCalculated(cfg, blockNum, opts.CommitmentHistoryEnabled, g.blockReader) {
		return nil
	}
	if g.receiptsCache.Contains(blockHash) {
		return nil
	}
	dbgStarted.Add(1)
	created := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	p := &ReceiptsPrefetch{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		start := time.Now()
		dbgSchedNs.Add(int64(start.Sub(created)))
		defer func() { dbgPrefetchNs.Add(int64(time.Since(start))) }()
		// The request already holds a read-tx slot: waiting for a second one can deadlock.
		prefetchTx, err := db.BeginTemporalRo(kv.WithNonBlockingAcquire(ctx))
		dbgBeginNs.Add(int64(time.Since(start)))
		if err != nil {
			dbgBeginFail.Add(1)
			return
		}
		defer prefetchTx.Rollback()
		view := g.filters.WithTemporalOverlay(prefetchTx)
		// The prefetch tx opens after the request tx: serve only the block the request resolved.
		tc := time.Now()
		canonical, ok, err := g.blockReader.CanonicalHash(ctx, view, blockNum)
		dbgCanonNs.Add(int64(time.Since(tc)))
		if err != nil || !ok || canonical != blockHash {
			dbgCanonFail.Add(1)
			return
		}
		tr := time.Now()
		receipts, err := rawdb.ReadBlockReceiptsCacheV2(ctx, view, blockNum, g.txNumReader)
		dbgReadNs.Add(int64(time.Since(tr)))
		if err != nil {
			dbgReadErr.Add(1)
			return
		}
		p.receipts = receipts
	}()
	return p
}

// Receipts returns the prefetched receipts completed with the fields of block,
// or nil when the caller must read them itself.
func (p *ReceiptsPrefetch) Receipts(block *types.Block) types.Receipts {
	if p == nil {
		return nil
	}
	defer dbgLog()
	transactions := block.Transactions()
	t0 := time.Now()
	for _, txn := range transactions {
		txn.Hash()
	}
	t1 := time.Now()
	dbgHashNs.Add(int64(t1.Sub(t0)))
	select {
	case <-p.done:
		dbgWaitZero.Add(1)
	default:
		<-p.done
	}
	dbgWaitNs.Add(int64(time.Since(t1)))
	if len(p.receipts) != len(transactions) {
		dbgIncomplete.Add(1)
		return nil
	}
	dbgServed.Add(1)
	blockHash, blockNum := block.Hash(), block.NumberU64()
	for i, x := range p.receipts {
		x.DeriveFieldsV4ForCachedReceipt(blockHash, blockNum, transactions[i].Hash(), false)
	}
	return p.receipts
}

// Close stops the prefetch and waits until its read tx is closed.
func (p *ReceiptsPrefetch) Close() {
	if p == nil {
		return
	}
	p.cancel()
	<-p.done
}
