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

package storage

import (
	"context"
	"encoding/binary"
	"github.com/holiman/uint256"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

// seedHeaderAt writes a minimal Header + HeaderNumber + TD + body
// entry for (blockNum, hash) into the supplied tx. Used to fabricate
// "leaked" forward block data in DB-reset tests.
func seedHeaderAt(t *testing.T, tx kv.RwTx, blockNum uint64, hash common.Hash) {
	t.Helper()
	require.NoError(t, rawdb.WriteHeaderNumber(tx, hash, blockNum))
	// Body must have TxCount >= 1: TruncateBlocks computes
	// LastSystemTx = baseTxnID + txCount - 1 (BaseTxnID.LastSystemTx
	// in execution/types/block.go). With TxCount=0 that underflows
	// to MaxUint64 and the tx-delete loop iterates MaxUint64 times.
	// Give each fixture block one tx with a unique BaseTxnID so the
	// delete loops don't overlap across blocks.
	require.NoError(t, rawdb.WriteBodyForStorage(tx, hash, blockNum, &types.BodyForStorage{
		BaseTxnID: types.BaseTxnID(blockNum * 8),
		TxCount:   1,
	}))
	require.NoError(t, rawdb.WriteTd(tx, hash, blockNum, *uint256.NewInt(blockNum * 10)))
	// Write into kv.Headers directly: TruncateBlocks iterates kv.Headers
	// by composite key (block_num_u64 || hash); the row value is the
	// header RLP but TruncateBlocks doesn't inspect it.
	headerKey := make([]byte, 8+len(hash))
	binary.BigEndian.PutUint64(headerKey[:8], blockNum)
	copy(headerKey[8:], hash[:])
	require.NoError(t, tx.Put(kv.Headers, headerKey, []byte{0x80}))
}

// TestDeleteHeaderNumbersPastBlock_WipesOrphans pins the new
// HeaderNumber-orphan sweep that fixes the OtterSync wedge:
// kv.Headers can carry forward block headers (and their hash→number
// map entries) that arrived via NewPayload between when mode B
// committed and when the process was killed. rawdb.TruncateBlocks
// wipes kv.Headers itself but does NOT touch kv.HeaderNumber —
// orphans there break hash-based lookups after the unwind.
//
// deleteHeaderNumbersPastBlock walks kv.Headers from toBlock+1,
// extracts each hash from the composite key, and removes the
// corresponding kv.HeaderNumber row.
func TestDeleteHeaderNumbersPastBlock_WipesOrphans(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	hashAt := func(n uint64) common.Hash {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], n)
		copy(h[24:], "orphan-seed-pad")
		return h
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(100)
	for n := uint64(95); n <= 105; n++ {
		seedHeaderAt(t, tx, n, hashAt(n))
	}

	for n := uint64(95); n <= 105; n++ {
		got, err := tx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.NotEmpty(t, got, "fixture: HeaderNumber should be seeded for block %d", n)
	}

	require.NoError(t, deleteHeaderNumbersPastBlock(tx, toBlock))

	for n := uint64(95); n <= 100; n++ {
		got, err := tx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.NotEmpty(t, got, "HeaderNumber at block %d (≤ toBlock) must survive the sweep", n)
	}
	for n := uint64(101); n <= 105; n++ {
		got, err := tx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.Empty(t, got, "HeaderNumber at block %d (> toBlock) must be deleted", n)
	}
}

// TestUnwindDBPastBlock_PreservesHeadersForBlocksUpToTarget pins the
// other half of the cold-start invariant: rows ≤ toBlock must SURVIVE
// the wipe. Live regression on hoodi: after mode B + restart, kv.Headers
// held only genesis — TruncateBlocks looked like it was wiping
// everything, not just past toBlock. If the underlying rawdb helpers
// have a seek-ordering bug interacting with the 40-byte composite key
// (8-byte block + 32-byte hash) vs an 8-byte ForEach prefix, this test
// surfaces it directly.
func TestUnwindDBPastBlock_PreservesHeadersForBlocksUpToTarget(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	hashAt := func(n uint64) common.Hash {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], n)
		copy(h[24:], "preserve-test")
		return h
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(100)
	for n := uint64(0); n <= 110; n++ {
		seedHeaderAt(t, tx, n, hashAt(n))
	}

	require.NoError(t, deleteHeaderNumbersPastBlock(tx, toBlock))
	require.NoError(t, rawdb.TruncateBlocks(ctx, tx, toBlock+1))
	require.NoError(t, rawdb.TruncateTd(tx, toBlock+1))

	// Survival check: every block 0..toBlock must still have a kv.Headers
	// row at its composite key (num || hash).
	for n := uint64(0); n <= toBlock; n++ {
		hk := make([]byte, 8+32)
		binary.BigEndian.PutUint64(hk[:8], n)
		copy(hk[8:], func() []byte { h := hashAt(n); return h[:] }())
		got, err := tx.GetOne(kv.Headers, hk)
		require.NoError(t, err)
		require.NotEmpty(t, got,
			"kv.Headers at block %d (≤ toBlock=%d) MUST survive — composite key %x",
			n, toBlock, hk)
	}
}

// TestUnwindDBPastBlock_CoreTablesAreEmptyPastTarget exercises the
// composite block-data wipe added to mode-B's unwindDBPastBlock —
// TruncateBlocks + TruncateTd + deleteHeaderNumbersPastBlock — and
// pins the cold-start invariant: after the unwind, the writable DB
// must carry no rows past toBlock in any block-data table.
//
// This is the regression for the OtterSync wedge observed on hoodi:
// snapshots ended at 2,911,999 while kv.Headers retained rows at
// 2,914,976+ from CL NewPayloads pushed after the unwind committed.
// firstNonGenesisCheck in stage_snapshots refuses to start.
//
// Doesn't run unwindDBPastBlock itself (that requires a non-nil
// BlockReader resolving a canonical hash at toBlock — too heavy for
// a focused unit test); calls the three helpers in the same order
// the function does, against a memdb seeded with leaked forward
// data, and asserts the invariant. The unwindDBPastBlock wiring is
// verified by inspection — the call site has a single linear
// sequence with the three helpers below.
func TestUnwindDBPastBlock_CoreTablesAreEmptyPastTarget(t *testing.T) {
	t.Parallel()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	ctx := context.Background()

	hashAt := func(n uint64) common.Hash {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], n)
		copy(h[24:], "leak-seed-pad")
		return h
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const toBlock = uint64(100)
	for n := uint64(0); n <= 105; n++ {
		seedHeaderAt(t, tx, n, hashAt(n))
	}

	require.NoError(t, deleteHeaderNumbersPastBlock(tx, toBlock))
	require.NoError(t, rawdb.TruncateBlocks(ctx, tx, toBlock+1))
	require.NoError(t, rawdb.TruncateTd(tx, toBlock+1))

	tables := []string{kv.Headers, kv.BlockBody, kv.HeaderTD}
	for _, table := range tables {
		c, err := tx.Cursor(table) //nolint:gocritic // explicit close at end of loop body; defer would accumulate cursors across the loop
		require.NoError(t, err)
		past := 0
		for k, _, err := c.First(); k != nil && err == nil; k, _, err = c.Next() {
			if len(k) >= 8 && binary.BigEndian.Uint64(k[:8]) > toBlock {
				past++
			}
		}
		c.Close()
		require.Zero(t, past, "%s must have no rows past toBlock=%d after the wipe", table, toBlock)
	}

	// HeaderNumber orphans: hash-keyed, so verify directly via lookup.
	for n := uint64(101); n <= 105; n++ {
		got, err := tx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.Empty(t, got, "HeaderNumber at block %d must be empty after the wipe", n)
	}

	for n := uint64(0); n <= toBlock; n++ {
		got, err := tx.GetOne(kv.HeaderNumber, func() []byte { h := hashAt(n); return h[:] }())
		require.NoError(t, err)
		require.NotEmpty(t, got, "HeaderNumber at block %d (≤ toBlock) must survive", n)
	}
}
