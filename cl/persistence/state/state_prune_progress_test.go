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

package state_accessors

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestStatePruneProgress(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	got, err := ReadStatePruneProgress(tx, kv.BlockRoot)
	require.NoError(t, err)
	require.Zero(t, got)

	require.NoError(t, SetStatePruneProgress(tx, kv.BlockRoot, 123))
	require.NoError(t, SetStatePruneProgress(tx, kv.StateRoot, 456))

	got, err = ReadStatePruneProgress(tx, kv.BlockRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(123), got)

	got, err = ReadStatePruneProgress(tx, kv.StateRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(456), got)

	got, err = ReadStatePruneProgress(tx, kv.EpochData)
	require.NoError(t, err)
	require.Zero(t, got)
}
