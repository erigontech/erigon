// Copyright 2024 The Erigon Authors
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

package membatchwithdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

// The FCU block overlay is a MemoryMutation over a read-only tx, and rawdb's
// block truncation runs range deletes on it, so it must satisfy
// kv.HasDeleteRange rather than fall back to a per-key scan.
func TestMemoryMutationDeleteRange(t *testing.T) {
	// initializeDbNonDupSort seeds AAAA, CAAA, CBAA, CCAA in the underlying tx;
	// BAAA is overlay-only, so the range delete has to span both views.
	newBatch := func(t *testing.T) *membatchwithdb.MemoryMutation {
		t.Helper()
		_, rwTx := newTestTx(t)
		initializeDbNonDupSort(rwTx)
		batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
		require.NoError(t, err)
		t.Cleanup(batch.Close)
		require.NoError(t, batch.Put(kv.HeaderNumber, []byte("BAAA"), []byte("overlay")))
		return batch
	}

	has := func(t *testing.T, batch *membatchwithdb.MemoryMutation, key string) bool {
		t.Helper()
		v, err := batch.GetOne(kv.HeaderNumber, []byte(key))
		require.NoError(t, err)
		return v != nil
	}

	t.Run("implements kv.HasDeleteRange", func(t *testing.T) {
		var batch kv.RwTx = newBatch(t)
		_, ok := batch.(kv.HasDeleteRange)
		require.True(t, ok)
	})

	t.Run("deletes [from, to) across overlay and underlying tx", func(t *testing.T) {
		batch := newBatch(t)
		n, err := batch.DeleteRange(kv.HeaderNumber, []byte("BAAA"), []byte("CBAA"))
		require.NoError(t, err)
		require.EqualValues(t, 2, n) // BAAA (overlay) + CAAA (underlying)

		require.True(t, has(t, batch, "AAAA"))  // below from: kept
		require.False(t, has(t, batch, "BAAA")) // overlay-only: gone
		require.False(t, has(t, batch, "CAAA")) // underlying: gone
		require.True(t, has(t, batch, "CBAA"))  // exclusive upper bound: kept
		require.True(t, has(t, batch, "CCAA"))
	})

	t.Run("DeleteBefore removes every key < to", func(t *testing.T) {
		batch := newBatch(t)
		n, err := batch.DeleteBefore(kv.HeaderNumber, []byte("CBAA"))
		require.NoError(t, err)
		require.EqualValues(t, 3, n) // AAAA, BAAA, CAAA

		require.False(t, has(t, batch, "AAAA"))
		require.False(t, has(t, batch, "CAAA"))
		require.True(t, has(t, batch, "CBAA"))
		require.True(t, has(t, batch, "CCAA"))
	})

	t.Run("DeleteAfter removes every key >= from", func(t *testing.T) {
		batch := newBatch(t)
		n, err := batch.DeleteAfter(kv.HeaderNumber, []byte("CAAA"))
		require.NoError(t, err)
		require.EqualValues(t, 3, n) // CAAA, CBAA, CCAA

		require.True(t, has(t, batch, "AAAA"))
		require.True(t, has(t, batch, "BAAA"))
		require.False(t, has(t, batch, "CAAA"))
		require.False(t, has(t, batch, "CCAA"))
	})

	// kv.HasDeleteRange specifies that on DupSort tables the count is (key,value)
	// pairs and a key goes with all of its values, so the overlay must agree with
	// the mdbx backends rather than counting distinct keys.
	t.Run("dupsort counts (key,value) pairs", func(t *testing.T) {
		_, rwTx := newTestTx(t)
		require.NoError(t, rwTx.Put(kv.TblAccountIdx, []byte("AAAA"), []byte("v1")))
		require.NoError(t, rwTx.Put(kv.TblAccountIdx, []byte("AAAA"), []byte("v2")))
		require.NoError(t, rwTx.Put(kv.TblAccountIdx, []byte("BBBB"), []byte("v1")))

		batch, err := membatchwithdb.NewMemoryBatch(rwTx, "", log.Root())
		require.NoError(t, err)
		t.Cleanup(batch.Close)

		n, err := batch.DeleteBefore(kv.TblAccountIdx, []byte("BBBB"))
		require.NoError(t, err)
		require.EqualValues(t, 2, n) // AAAA's two values, not one key

		v, err := batch.GetOne(kv.TblAccountIdx, []byte("AAAA"))
		require.NoError(t, err)
		require.Nil(t, v) // every value of the key is gone
	})

	t.Run("nil bounds clear the table", func(t *testing.T) {
		batch := newBatch(t)
		n, err := batch.DeleteAfter(kv.HeaderNumber, nil)
		require.NoError(t, err)
		require.EqualValues(t, 5, n)
		require.False(t, has(t, batch, "AAAA"))
		require.False(t, has(t, batch, "CCAA"))
	})
}
