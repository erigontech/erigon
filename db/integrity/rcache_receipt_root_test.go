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

package integrity_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func enableHistoricalRCache(t *testing.T) {
	saved := statecfg.Schema.RCacheDomain
	statecfg.EnableHistoricalRCache()
	t.Cleanup(func() { statecfg.Schema.RCacheDomain = saved })
}

func newRCacheChain(t *testing.T, txsPerBlock []int, skip map[uint64]bool) (kv.TemporalRwDB, *freezeblocks.BlockReader) {
	t.Helper()
	ctx := t.Context()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(16))
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles())

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer doms.Close()
	putter := doms.AsPutDel(tx)

	txNum := uint64(0)
	for b, n := range txsPerBlock {
		receipts := make(types.Receipts, n)
		for i := range receipts {
			receipts[i] = &types.Receipt{Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: uint64(21_000 * (i + 1))}
		}
		write := func(r *types.Receipt) {
			if !skip[txNum] {
				require.NoError(t, rawdb.WriteReceiptCacheV2(putter, r, txNum))
			}
			txNum++
		}
		write(nil)
		for _, r := range receipts {
			write(r)
		}
		write(nil)
		require.NoError(t, rawdbv3.TxNums.Append(tx, uint64(b), txNum-1))
		h := &types.Header{Number: *uint256.NewInt(uint64(b)), ReceiptHash: types.DeriveSha(receipts)}
		require.NoError(t, rawdb.WriteHeader(tx, h))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, h.Hash(), uint64(b)))
	}
	require.NoError(t, doms.Flush(ctx, tx))
	doms.Close()
	require.NoError(t, tx.Commit())
	return db, br
}

func TestReceiptRootIntegrity(t *testing.T) {
	enableHistoricalRCache(t)

	txsPerBlock := []int{0, 2, 0, 0, 1, 0, 3, 0}

	tests := []struct {
		name  string
		skip  []uint64
		block uint64
	}{
		{name: "complete"},
		{name: "hole over empty block", skip: []uint64{8, 9}, block: 3},
		{name: "hole over block with txs", skip: []uint64{10, 11, 12}, block: 4},
		{name: "missing system txNum in block with txs", skip: []uint64{10}, block: 4},
		{name: "hole in last block", skip: []uint64{20}, block: 7},
		{name: "missing final system txNum in last block", skip: []uint64{21}, block: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			logger := log.New()
			ctx := t.Context()
			skip := map[uint64]bool{}
			for _, txNum := range tt.skip {
				skip[txNum] = true
			}
			db, br := newRCacheChain(t, txsPerBlock, skip)

			sc := integrity.SamplerCfg{Seed: 1, SampleRatio: 1}
			rangeErr := integrity.CheckRCacheRootAtBlkRange(ctx, sc, db, br, chain.AllProtocolChanges, 1, uint64(len(txsPerBlock)), true, logger)
			autoErr := integrity.CheckReceiptRootIntegrity(ctx, sc, db, br, chain.AllProtocolChanges, true, logger)
			if len(tt.skip) == 0 {
				require.NoError(t, rangeErr)
				require.NoError(t, autoErr)
				return
			}
			require.ErrorIs(t, rangeErr, integrity.ErrIntegrity)
			require.ErrorIs(t, autoErr, integrity.ErrIntegrity)
			require.ErrorIs(t, integrity.CheckRCacheRootAtBlk(ctx, db, br, chain.AllProtocolChanges, tt.block, true, logger), integrity.ErrIntegrity)
		})
	}
}

func TestReceiptRootIntegrity_FilesOnlyTip(t *testing.T) {
	enableHistoricalRCache(t)
	ctx := t.Context()

	db, br := newRCacheChain(t, []int{0, 2, 0, 0, 2, 1, 0, 0}, nil)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, agg.BuildFiles2(ctx, db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, table := range db.Debug().DomainTables(kv.RCacheDomain) {
			if err := tx.ClearTable(table); err != nil {
				return err
			}
		}
		return nil
	}))

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.Equal(t, uint64(16), tx.Debug().DomainProgress(kv.RCacheDomain))
	tx.Rollback()

	require.NoError(t, integrity.CheckReceiptRootIntegrity(ctx, integrity.SamplerCfg{Seed: 1, SampleRatio: 1}, db, br, chain.AllProtocolChanges, true, log.New()))
}

func TestReceiptRootIntegrity_GenesisWithoutRCache(t *testing.T) {
	enableHistoricalRCache(t)
	ctx := t.Context()
	logger := log.New()

	db, br := newRCacheChain(t, []int{0, 2, 0}, map[uint64]bool{0: true, 1: true})

	sc := integrity.SamplerCfg{Seed: 1, SampleRatio: 1}
	require.NoError(t, integrity.CheckRCacheRootAtBlkRange(ctx, sc, db, br, chain.AllProtocolChanges, 0, 3, true, logger))
	require.NoError(t, integrity.CheckRCacheRootAtBlk(ctx, db, br, chain.AllProtocolChanges, 0, true, logger))
}

type inexactRCacheDebugTx struct {
	kv.TemporalDebugTx
}

func (inexactRCacheDebugTx) DomainVisibleEnd(kv.Domain) (uint64, bool) { return 0, false }
func (inexactRCacheDebugTx) DomainProgress(kv.Domain) uint64           { return 16 }
func (inexactRCacheDebugTx) TxNumsInFiles(...kv.Domain) uint64         { return 0 }

type blockFilesTemporalTx interface {
	kv.TemporalTx
	freezeblocks.HasBlockFilesRoTx
}

type inexactRCacheTx struct {
	blockFilesTemporalTx
}

func (tx inexactRCacheTx) Debug() kv.TemporalDebugTx {
	return inexactRCacheDebugTx{tx.blockFilesTemporalTx.Debug()}
}

func TestRCacheEndBlockNum_InexactVisibleEnd(t *testing.T) {
	ctx := t.Context()
	db, br := newRCacheChain(t, []int{0, 2, 0, 0, 2, 1, 0, 0}, nil)

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	end, err := integrity.RCacheEndBlockNum(ctx, inexactRCacheTx{tx.(blockFilesTemporalTx)}, br.TxnumReader())
	require.NoError(t, err)
	require.Equal(t, uint64(5), end)
}
