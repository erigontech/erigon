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

package rawdb_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

func writeCanonicalBlocks(t *testing.T, tx kv.RwTx, n uint64) {
	t.Helper()
	for blockNum := uint64(1); blockNum <= n; blockNum++ {
		h := common.BigToHash(new(big.Int).SetUint64(blockNum))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, h, blockNum))
		_, err := rawdb.WriteRawBody(tx, h, blockNum, &types.RawBody{Transactions: [][]byte{{0x01}}})
		require.NoError(t, err)
	}
}

// TestAppendCanonicalTxNumsFromTip_FillsGap pins the behaviour the
// forkchoice path depends on: an append whose start sits above the
// txNums tip resumes from the tip instead of failing.
func TestAppendCanonicalTxNumsFromTip_FillsGap(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)
	writeCanonicalBlocks(t, tx, 10)

	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1, nil))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 5))

	require.Error(t, rawdb.AppendCanonicalTxNums(tx, 8, nil),
		"the plain append must still reject a gap — callers that truncated to from-1 rely on it")
	require.NoError(t, rawdb.AppendCanonicalTxNumsFromTip(tx, 8, nil))

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(10), last)
	// Strictly increasing maxTxNum proves each block in the gap got its
	// own entry rather than inheriting a lower block's value.
	for blockNum := uint64(5); blockNum <= 10; blockNum++ {
		require.Greater(t, mustMaxTxNum(t, tx, blockNum), mustMaxTxNum(t, tx, blockNum-1),
			"block %d missing from the txNums index", blockNum)
	}
}

// TestAppendCanonicalTxNumsFromTip_ContiguousStart is the baseline: with
// from exactly at tip+1 the wrapper appends without retrying.
func TestAppendCanonicalTxNumsFromTip_ContiguousStart(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)
	writeCanonicalBlocks(t, tx, 10)

	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1, nil))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 5))
	require.NoError(t, rawdb.AppendCanonicalTxNumsFromTip(tx, 5, nil))

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(10), last)
}

// TestAppendCanonicalTxNumsFromTip_UnfillableGapIsNotSuccess pins that a
// gap the wrapper cannot fill is surfaced rather than reported as success.
//
// The forkchoice decides canonicality through the block reader, which falls
// through to the header segments, so a block whose only canonical record is
// in a .seg file stops the walk-back. The forkchoice then writes an MDBX
// marker for that block alone and appends from it. Retrying from the txNums
// tip lands on a block whose marker lives only in the files, and the append
// loop — which reads canonical hashes straight from MDBX — stops there.
// Reporting that as success leaves txNums pinned below the head, so
// execution finds nothing to do for every later block and the head never
// moves again.
func TestAppendCanonicalTxNumsFromTip_UnfillableGapIsNotSuccess(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)
	writeCanonicalBlocks(t, tx, 5)
	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1, nil))

	// Blocks 6 and 7 are canonical only in the files; block 8 is the
	// forkchoice head, the one block the walk-back marked in MDBX.
	head := common.BigToHash(new(big.Int).SetUint64(8))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, head, 8))
	_, err := rawdb.WriteRawBody(tx, head, 8, &types.RawBody{Transactions: [][]byte{{0x01}}})
	require.NoError(t, err)

	appendErr := rawdb.AppendCanonicalTxNumsFromTip(tx, 8, nil)

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Error(t, appendErr,
		"appended nothing (txNums tip still %d, asked to append from 8) yet reported success — "+
			"the caller commits a forkchoice whose txNums never advance and the node wedges", last)
}

// TestAppendCanonicalTxNumsFromTip_PrunedBodyIsNotSuccess pins the wedge
// seen on hoodi under --prune.mode=minimal: retirement runs ahead of
// execution, PruneBlocks drops the bodies of retired blocks from the db,
// and the append — which reads bodies from the db alone — stops on the
// first pruned body. Canonical markers are still there, so nothing raises
// a gap and the caller is told the append succeeded while txNums never
// moved. Execution then finds no work and the head stops for good.
func TestAppendCanonicalTxNumsFromTip_PrunedBodyIsNotSuccess(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)
	writeCanonicalBlocks(t, tx, 10)
	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1, nil))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 6))

	// Blocks 6..10 are retired: canonical markers stay, bodies are pruned.
	for blockNum := uint64(6); blockNum <= 10; blockNum++ {
		rawdb.DeleteBody(tx, common.BigToHash(new(big.Int).SetUint64(blockNum)), blockNum)
	}

	appendErr := rawdb.AppendCanonicalTxNumsFromTip(tx, 6, nil)

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Error(t, appendErr,
		"appended nothing (txNums tip still %d) yet reported success — the forkchoice commits and the node wedges", last)
}

// TestAppendCanonicalTxNums_PrunedBodyResolvedFromReader pins the cure for
// the minimal-prune wedge: given a reader that serves the retired bodies the
// db no longer holds, the append must cross the retirement horizon and carry
// txNums up to the canonical tip.
func TestAppendCanonicalTxNums_PrunedBodyResolvedFromReader(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)
	writeCanonicalBlocks(t, tx, 10)
	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1, nil))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 6))

	// Retire blocks 6..10: keep what the segments would serve, drop the db copy.
	retired := map[uint64]*types.BodyForStorage{}
	for blockNum := uint64(6); blockNum <= 10; blockNum++ {
		h := common.BigToHash(new(big.Int).SetUint64(blockNum))
		b, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(blockNum, h))
		require.NoError(t, err)
		require.NotNil(t, b)
		retired[blockNum] = b
		rawdb.DeleteBody(tx, h, blockNum)
	}

	fromSegments := func(blockNum uint64, _ common.Hash) (*types.BodyForStorage, error) {
		return retired[blockNum], nil
	}
	require.NoError(t, rawdb.AppendCanonicalTxNumsFromTip(tx, 6, fromSegments))

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(10), last,
		"the reader serves the retired bodies, so txNums must reach the canonical tip")
	for blockNum := uint64(7); blockNum <= 10; blockNum++ {
		require.Greater(t, mustMaxTxNum(t, tx, blockNum), mustMaxTxNum(t, tx, blockNum-1),
			"block %d missing from the txNums index", blockNum)
	}
}

func mustMaxTxNum(t *testing.T, tx kv.Tx, blockNum uint64) uint64 {
	t.Helper()
	v, err := rawdbv3.TxNums.Max(t.Context(), tx, blockNum)
	require.NoError(t, err)
	return v
}
