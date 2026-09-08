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

	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 5))

	require.Error(t, rawdb.AppendCanonicalTxNums(tx, 8),
		"the plain append must still reject a gap — callers that truncated to from-1 rely on it")
	require.NoError(t, rawdb.AppendCanonicalTxNumsFromTip(tx, 8))

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

	require.NoError(t, rawdb.AppendCanonicalTxNums(tx, 1))
	require.NoError(t, rawdbv3.TxNums.Truncate(tx, 5))
	require.NoError(t, rawdb.AppendCanonicalTxNumsFromTip(tx, 5))

	last, _, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	require.Equal(t, uint64(10), last)
}

func mustMaxTxNum(t *testing.T, tx kv.Tx, blockNum uint64) uint64 {
	t.Helper()
	v, err := rawdbv3.TxNums.Max(t.Context(), tx, blockNum)
	require.NoError(t, err)
	return v
}
