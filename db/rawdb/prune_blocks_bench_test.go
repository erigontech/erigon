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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkPruneBlocks(b *testing.B) {
	const blocks = 256
	for _, txsPerBlock := range []int{1, 16, 200} {
		b.Run(fmt.Sprintf("txs=%d", txsPerBlock), func(b *testing.B) {
			benchPruneBlocks(b, blocks, txsPerBlock)
		})
	}
}

func benchPruneBlocks(b *testing.B, blocks, txsPerBlock int) {
	db := mdbxtest.NewTestDB(b, dbcfg.ChainDB)
	txs := make([][]byte, txsPerBlock)
	for i := range txs {
		txs[i] = make([]byte, 200)
		txs[i][0] = byte(i)
	}

	b.ResetTimer()
	for range b.N {
		pruneBlocksOnce(b, db, blocks, txs)
	}
}

func pruneBlocksOnce(b *testing.B, db kv.RwDB, blocks int, txs [][]byte) {
	b.StopTimer()
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()

	for n := 1; n <= blocks; n++ {
		h := &types.Header{Number: *uint256.NewInt(uint64(n))}
		require.NoError(b, rawdb.WriteHeader(tx, h))
		_, err = rawdb.WriteRawBody(tx, h.Hash(), uint64(n), &types.RawBody{Transactions: txs})
		require.NoError(b, err)
	}
	b.StartTimer()

	deleted, err := rawdb.PruneBlocks(tx, uint64(blocks+1), blocks)

	b.StopTimer()
	require.NoError(b, err)
	require.Equal(b, blocks, deleted)
	left, err := tx.Count(kv.EthTx)
	require.NoError(b, err)
	require.Zero(b, left)
}
