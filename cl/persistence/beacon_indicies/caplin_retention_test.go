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

package beacon_indicies

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestCaplinRetentionRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		want clparams.CaplinRetention
	}{
		{name: "archive", want: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeArchive, Blocks: clparams.CaplinAllFloor(), Sidecars: clparams.CaplinAllFloor(), States: clparams.CaplinAllFloor()}},
		{name: "spec", want: clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec}},
		{
			name: "pruned distance",
			want: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinDistanceFloor(10),
				Sidecars: clparams.CaplinDistanceFloor(20),
				States:   clparams.CaplinDistanceFloor(30),
			},
		},
		{
			name: "pruned slot",
			want: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinSlotFloor(100),
				Sidecars: clparams.CaplinSlotFloor(200),
				States:   clparams.CaplinSlotFloor(300),
			},
		},
		{
			name: "pruned all",
			want: clparams.CaplinRetention{
				Mode:     clparams.CaplinPruneModePruned,
				Blocks:   clparams.CaplinAllFloor(),
				Sidecars: clparams.CaplinAllFloor(),
				States:   clparams.CaplinAllFloor(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			t.Cleanup(db.Close)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return WriteCaplinRetention(tx, test.want)
			}))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				got, found, err := ReadCaplinRetention(tx)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, test.want, got)
				return nil
			}))
		})
	}
}

func TestReadCaplinRetentionAbsent(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		got, found, err := ReadCaplinRetention(tx)
		require.NoError(t, err)
		require.False(t, found)
		require.Zero(t, got)
		return nil
	}))
}

func TestReadCaplinRetentionRejectsInvalidValue(t *testing.T) {
	for _, test := range []struct {
		name  string
		value []byte
		want  string
	}{
		{name: "wrong length", value: make([]byte, 1), want: "invalid Caplin retention value length"},
		{name: "unknown version", value: append([]byte{2}, make([]byte, caplinRetentionValueLength-1)...), want: "unknown Caplin retention value version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			t.Cleanup(db.Close)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return tx.Put(kv.DatabaseInfo, caplinRetentionKey, test.value)
			}))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				_, found, err := ReadCaplinRetention(tx)
				require.ErrorContains(t, err, test.want)
				require.False(t, found)
				return nil
			}))
		})
	}
}

func TestEmptyRetentionValueIsCorrupt(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.DatabaseInfo, caplinRetentionKey, []byte{})
	}))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		exists, err := tx.Has(kv.DatabaseInfo, caplinRetentionKey)
		require.NoError(t, err)
		require.True(t, exists)
		_, found, err := ReadCaplinRetention(tx)
		if err == nil {
			t.Fatalf("present zero-byte value: found=%t err=%v", found, err)
		}
		return nil
	}))
}
