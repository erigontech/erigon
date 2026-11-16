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

// DEBUG ONLY: prefetch outcome counters, logged every dbgEvery Receipts calls.
var (
	dbgNotServed, dbgInLRU, dbgStarted, dbgBeginFail, dbgCanonErr, dbgCanonMissing, dbgCanonMismatch atomic.Int64
	dbgReadErr, dbgIncomplete, dbgServed, dbgCalls, dbgWaitNs, dbgWaitZero, dbgHashNs, dbgPrefetchNs atomic.Int64
	dbgCtxDone                                                                                       atomic.Int64
)

const dbgEvery = 5000

func dbgLog() {
	n := dbgCalls.Add(1)
	if n%dbgEvery != 0 {
		return
	}
	log.Info("[dbg] receipts prefetch", "calls", n, "notServed", dbgNotServed.Load(), "inLRU", dbgInLRU.Load(),
		"started", dbgStarted.Load(), "beginFail", dbgBeginFail.Load(), "canonErr", dbgCanonErr.Load(),
		"canonMissing", dbgCanonMissing.Load(), "canonMismatch", dbgCanonMismatch.Load(), "readErr", dbgReadErr.Load(),
		"ctxDone", dbgCtxDone.Load(), "incomplete", dbgIncomplete.Load(), "served", dbgServed.Load(),
		"avgWaitUs", dbgWaitNs.Load()/n/1000, "waitZero", dbgWaitZero.Load(),
		"avgHashUs", dbgHashNs.Load()/n/1000, "avgPrefetchUs", dbgPrefetchNs.Load()/max(dbgStarted.Load(), 1)/1000)
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
		dbgNotServed.Add(1)
		return nil
	}
	if g.receiptsCache.Contains(blockHash) {
		dbgInLRU.Add(1)
		return nil
	}
	dbgStarted.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	p := &ReceiptsPrefetch{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		start := time.Now()
		defer func() { dbgPrefetchNs.Add(int64(time.Since(start))) }()
		// The request already holds a read-tx slot: waiting for a second one can deadlock.
		prefetchTx, err := db.BeginTemporalRo(kv.WithNonBlockingAcquire(ctx))
		if err != nil {
			dbgBeginFail.Add(1)
			return
		}
		defer prefetchTx.Rollback()
		view := g.filters.WithTemporalOverlay(prefetchTx)
		// The prefetch tx opens after the request tx: serve only the block the request resolved.
		canonical, ok, err := g.blockReader.CanonicalHash(ctx, view, blockNum)
		if err != nil || !ok || canonical != blockHash {
			switch {
			case err != nil:
				dbgCanonErr.Add(1)
			case !ok:
				dbgCanonMissing.Add(1)
			default:
				dbgCanonMismatch.Add(1)
			}
			return
		}
		receipts, err := rawdb.ReadBlockReceiptsCacheV2(ctx, view, blockNum, g.txNumReader)
		if err != nil {
			if ctx.Err() != nil {
				dbgCtxDone.Add(1)
			}
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
