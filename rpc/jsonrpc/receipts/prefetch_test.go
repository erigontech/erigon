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

package receipts_test

import (
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const prefetchTestTxs = 16

func mockForPrefetch(t *testing.T, txsPerBlock int) *execmoduletester.ExecModuleTester {
	signer := types.LatestSignerForChainID(nil)
	return mockWithGenerator(t, 1, func(i int, block *blockgen.BlockGen) {
		for range txsPerBlock {
			txn, err := types.SignTx(
				types.NewTransaction(block.TxNonce(testAddr), testAddr, uint256.NewInt(1), params.TxGas, nil, nil),
				*signer, testKey,
			)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	}, execmoduletester.WithEnableDomain(kv.RCacheDomain))
}

func newPrefetchGenerator(m *execmoduletester.ExecModuleTester, events *shards.Events) *receipts.Generator {
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)
	return receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute, ff)
}

func servePersistedReceipts(t *testing.T) {
	prev := dbg.AssertEnabled
	dbg.AssertEnabled = false // assertions re-execute instead of serving the persistent cache
	t.Cleanup(func() { dbg.AssertEnabled = prev })
}

func readBlock(t *testing.T, m *execmoduletester.ExecModuleTester, tx kv.TemporalTx, blockNum uint64) *types.Block {
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	require.NotNil(t, block)
	return block
}

func TestPrefetchReceiptsMatchesPersistedReceipts(t *testing.T) {
	servePersistedReceipts(t)
	m := mockForPrefetch(t, prefetchTestTxs)
	gen := newPrefetchGenerator(m, shards.NewEvents())

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	block := readBlock(t, m, tx, 1)

	want, err := rawdb.ReadReceiptsCacheV2(tx, block, m.BlockReader.TxnumReader())
	require.NoError(t, err)
	require.Len(t, want, prefetchTestTxs)

	p := gen.PrefetchReceipts(m.Ctx, m.ChainConfig, m.DB, tx, 1, block.Hash(), eth.ReceiptsOpts{})
	defer p.Close()
	got := p.Receipts(block)
	require.Equal(t, want, got)
}

type fullReadTxLimiterDB struct {
	kv.TemporalRoDB
	nonBlocking []bool
}

func (db *fullReadTxLimiterDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.nonBlocking = append(db.nonBlocking, kv.IsNonBlockingAcquire(ctx))
	return nil, kv.ErrReadTxLimitExceeded
}

// TestPrefetchReceiptsDoesNotWaitForAReadTxSlot pins that the prefetch tx does not wait for the
// read-tx limiter: the request already holds a slot, so waiting can deadlock once every slot is
// held by a request that waits for its own prefetch.
func TestPrefetchReceiptsDoesNotWaitForAReadTxSlot(t *testing.T) {
	servePersistedReceipts(t)
	m := mockForPrefetch(t, prefetchTestTxs)
	gen := newPrefetchGenerator(m, shards.NewEvents())

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	block := readBlock(t, m, tx, 1)

	db := &fullReadTxLimiterDB{TemporalRoDB: m.DB}
	p := gen.PrefetchReceipts(m.Ctx, m.ChainConfig, db, tx, 1, block.Hash(), eth.ReceiptsOpts{})
	defer p.Close()
	require.Nil(t, p.Receipts(block))
	require.Equal(t, []bool{true}, db.nonBlocking)
}

// TestPrefetchReceiptsRejectsAnotherCanonicalBlock pins that the prefetch tx serves receipts only
// when it sees the requested block as canonical: it opens after the request tx, so a reorg in
// between can put another block at the same height.
func TestPrefetchReceiptsRejectsAnotherCanonicalBlock(t *testing.T) {
	servePersistedReceipts(t)
	m := mockForPrefetch(t, prefetchTestTxs)
	gen := newPrefetchGenerator(m, shards.NewEvents())

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	block := readBlock(t, m, tx, 1)

	reorgedHash := common.Hash{0x01}
	p := gen.PrefetchReceipts(m.Ctx, m.ChainConfig, m.DB, tx, 1, reorgedHash, eth.ReceiptsOpts{})
	defer p.Close()
	got := p.Receipts(block)
	require.Nil(t, got)
}

type countingDB struct {
	kv.TemporalRoDB
	begins int
}

func (db *countingDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.begins++
	return db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic // the prefetch owns and closes the tx
}

func TestPrefetchReceiptsSkipsReceiptsNotReadFromThePersistentCache(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, m *execmoduletester.ExecModuleTester, gen *receipts.Generator, tx kv.TemporalTx, block *types.Block) (*chain.Config, eth.ReceiptsOpts)
	}{
		{"assertions re-execute", func(t *testing.T, m *execmoduletester.ExecModuleTester, _ *receipts.Generator, _ kv.TemporalTx, _ *types.Block) (*chain.Config, eth.ReceiptsOpts) {
			dbg.AssertEnabled = true
			return m.ChainConfig, eth.ReceiptsOpts{}
		}},
		{"pre-Byzantium post state", func(t *testing.T, m *execmoduletester.ExecModuleTester, _ *receipts.Generator, _ kv.TemporalTx, _ *types.Block) (*chain.Config, eth.ReceiptsOpts) {
			cfg := m.ChainConfig.Copy()
			cfg.ByzantiumBlock = common.NewUint64(1_000)
			return cfg, eth.ReceiptsOpts{CommitmentHistoryEnabled: true}
		}},
		{"already in the receipts LRU", func(t *testing.T, m *execmoduletester.ExecModuleTester, gen *receipts.Generator, tx kv.TemporalTx, block *types.Block) (*chain.Config, eth.ReceiptsOpts) {
			_, err := gen.GetReceipts(m.Ctx, m.ChainConfig, tx, block, eth.ReceiptsOpts{})
			require.NoError(t, err)
			return m.ChainConfig, eth.ReceiptsOpts{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servePersistedReceipts(t)
			m := mockForPrefetch(t, prefetchTestTxs)
			gen := newPrefetchGenerator(m, shards.NewEvents())
			tx, err := m.DB.BeginTemporalRo(m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			block := readBlock(t, m, tx, 1)
			cfg, opts := tc.setup(t, m, gen, tx, block)

			db := &countingDB{TemporalRoDB: m.DB}
			p := gen.PrefetchReceipts(m.Ctx, cfg, db, tx, 1, block.Hash(), opts)
			defer p.Close()
			require.Nil(t, p)
			require.Zero(t, db.begins)
		})
	}
}

type cancelOnlySeekDB struct {
	kv.TemporalRoDB
	opened chan *cancelOnlySeekTx
}

func (db *cancelOnlySeekDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic // the prefetch owns and closes the tx
	if err != nil {
		return nil, err
	}
	seekTx := &cancelOnlySeekTx{TemporalTx: tx, ctx: ctx, started: make(chan struct{})}
	db.opened <- seekTx
	return seekTx, nil
}

// cancelOnlySeekTx blocks every history read until the prefetch is cancelled.
type cancelOnlySeekTx struct {
	kv.TemporalTx
	ctx        context.Context
	started    chan struct{}
	seeks      int
	rolledBack bool
}

func (tx *cancelOnlySeekTx) HistorySeek(domain kv.Domain, key []byte, ts uint64) ([]byte, bool, error) {
	tx.seeks++
	if tx.seeks == 1 {
		close(tx.started)
	}
	<-tx.ctx.Done()
	return nil, false, tx.ctx.Err()
}

func (tx *cancelOnlySeekTx) Rollback() {
	tx.rolledBack = true
	tx.TemporalTx.Rollback()
}

// TestPrefetchReceiptsCloseStopsTheRead pins that Close cancels the read and returns only after
// the prefetch tx is closed, so a request that ends early leaves no read tx behind.
func TestPrefetchReceiptsCloseStopsTheRead(t *testing.T) {
	servePersistedReceipts(t)
	m := mockForPrefetch(t, prefetchTestTxs)
	gen := newPrefetchGenerator(m, shards.NewEvents())

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	block := readBlock(t, m, tx, 1)

	db := &cancelOnlySeekDB{TemporalRoDB: m.DB, opened: make(chan *cancelOnlySeekTx, 1)}
	p := gen.PrefetchReceipts(m.Ctx, m.ChainConfig, db, tx, 1, block.Hash(), eth.ReceiptsOpts{})
	require.NotNil(t, p)
	prefetchTx := <-db.opened
	<-prefetchTx.started
	p.Close()
	require.True(t, prefetchTx.rolledBack)
	require.Equal(t, 1, prefetchTx.seeks)
}

func TestPrefetchReceiptsSkipsSmallBlocks(t *testing.T) {
	servePersistedReceipts(t)
	m := mockForPrefetch(t, 2)
	gen := newPrefetchGenerator(m, shards.NewEvents())

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	block := readBlock(t, m, tx, 1)

	db := &countingDB{TemporalRoDB: m.DB}
	p := gen.PrefetchReceipts(m.Ctx, m.ChainConfig, db, tx, 1, block.Hash(), eth.ReceiptsOpts{})
	defer p.Close()
	require.Nil(t, p)
	require.Zero(t, db.begins)
}
