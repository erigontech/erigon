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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

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
	if !PersistedReceiptsServed() || PostStateCalculated(cfg, blockNum, opts.CommitmentHistoryEnabled, g.blockReader) ||
		g.receiptsCache.Contains(blockHash) {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &ReceiptsPrefetch{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		// The request already holds a read-tx slot: waiting for a second one can deadlock.
		prefetchTx, err := db.BeginTemporalRo(kv.WithNonBlockingAcquire(ctx))
		if err != nil {
			return
		}
		defer prefetchTx.Rollback()
		view := g.filters.WithTemporalOverlay(prefetchTx)
		// The prefetch tx opens after the request tx: serve only the block the request resolved.
		canonical, ok, err := g.blockReader.CanonicalHash(ctx, view, blockNum)
		if err != nil || !ok || canonical != blockHash {
			return
		}
		if receipts, err := rawdb.ReadBlockReceiptsCacheV2(ctx, view, blockNum, g.txNumReader); err == nil {
			p.receipts = receipts
		}
	}()
	return p
}

// Receipts returns the prefetched receipts completed with the fields of block,
// or nil when the caller must read them itself.
func (p *ReceiptsPrefetch) Receipts(block *types.Block) types.Receipts {
	if p == nil {
		return nil
	}
	// Hashing the transactions overlaps with reading the receipts.
	for _, txn := range block.Transactions() {
		txn.Hash()
	}
	<-p.done
	return rawdb.DeriveCachedBlockReceipts(block, p.receipts)
}

// Close stops the prefetch and waits until its read tx is closed.
func (p *ReceiptsPrefetch) Close() {
	if p == nil {
		return
	}
	p.cancel()
	<-p.done
}
