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

package state

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// TestPutShadowHistorySynth_LandsInBothTables locks in the invariant
// that a shadow write paired via putShadowHistorySynth produces BOTH
// a History.KeysTable entry (txN → K) AND a History.ValuesTable entry
// (K, txN, V). Retire's History.collate reads these to build .ef/.v;
// without them, .kv gets K but .ef does not — the mode-D wrong-root
// failure shape.
func TestPutShadowHistorySynth_LandsInBothTables(t *testing.T) {
	t.Parallel()
	db, d := testDbAndDomain(t, log.New())

	const targetTxN = uint64(1000)
	testKey := []byte("shadow-key-01")
	testVal := []byte("shadow-value")

	err := db.Update(t.Context(), func(tx kv.RwTx) error {
		return putShadowHistorySynth(tx, d, testKey, testVal, targetTxN)
	})
	require.NoError(t, err)

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], targetTxN)

		keysCursor, err := tx.CursorDupSort(d.History.KeysTable)
		require.NoError(t, err)
		defer keysCursor.Close()
		gotTxKey, gotKey, err := keysCursor.SeekBothExact(txKey[:], testKey)
		require.NoError(t, err)
		require.Equal(t, txKey[:], gotTxKey, "History.KeysTable missing (txN=%d, K=%x)", targetTxN, testKey)
		require.Equal(t, testKey, gotKey)

		if d.History.HistoryLargeValues {
			vk := append(append([]byte{}, testKey...), txKey[:]...)
			gotVal, err := tx.GetOne(d.History.ValuesTable, vk)
			require.NoError(t, err)
			require.Equal(t, testVal, gotVal)
			return nil
		}
		valsCursor, err := tx.CursorDupSort(d.History.ValuesTable)
		require.NoError(t, err)
		defer valsCursor.Close()
		expected := append(append([]byte{}, txKey[:]...), testVal...)
		gotDup, err := valsCursor.SeekBothRange(testKey, txKey[:])
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected, gotDup),
			"History.ValuesTable missing (K=%x, txN=%d, V=%x); got=%x", testKey, targetTxN, testVal, gotDup)
		return nil
	}))
}

// TestPutShadowHistorySynth_NoopForHistoryDisabled ensures the helper
// does not write to nil-history tables for domains that disable
// history (commitment, rcache). Prevents a spurious "history disabled
// but got a synth write" corruption.
func TestPutShadowHistorySynth_NoopForHistoryDisabled(t *testing.T) {
	t.Parallel()
	db, d := testDbAndDomain(t, log.New())
	d.HistoryDisabled = true

	err := db.Update(t.Context(), func(tx kv.RwTx) error {
		return putShadowHistorySynth(tx, d, []byte("k"), []byte("v"), 42)
	})
	require.NoError(t, err)

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], 42)
		c, err := tx.CursorDupSort(d.History.KeysTable)
		require.NoError(t, err)
		defer c.Close()
		gotTxKey, gotKey, err := c.SeekBothExact(txKey[:], []byte("k"))
		require.NoError(t, err)
		require.Nil(t, gotTxKey)
		require.Nil(t, gotKey)
		return nil
	}))
}
