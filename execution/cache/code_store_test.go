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

package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestCodeStore_TwoTierAndEvict(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	code := []byte{0x60, 0x80, 0x60, 0x40, 0x52}
	hash := crypto.Keccak256(code)

	// Write path populates both tiers.
	cs := NewCodeStore(1<<20, 1<<20)
	require.NoError(t, cs.PutByHash(tx, hash, code))
	got, ok := cs.GetByHash(tx, hash)
	require.True(t, ok)
	require.Equal(t, code, got)

	// A fresh store (empty mem) serves from the MDBX backing tier.
	cs2 := NewCodeStore(1<<20, 1<<20)
	got, ok = cs2.GetByHash(tx, hash)
	require.True(t, ok, "must serve from the persistent TblCodeCache backing")
	require.Equal(t, code, got)

	// Unknown codehash misses cleanly.
	_, ok = cs2.GetByHash(tx, crypto.Keccak256([]byte{0xff}))
	require.False(t, ok)

	// Evict prunes the backing when over the table cap.
	small := NewCodeStore(1<<20, 128)
	for i := range 64 {
		c := []byte{byte(i), byte(i >> 8), 0xaa, 0xbb}
		require.NoError(t, small.PutByHash(tx, crypto.Keccak256(c), c))
	}
	require.NoError(t, small.Evict(tx))
	require.LessOrEqual(t, small.tableSizeBytes.Load(), int64(128))

	// Restart scenario: a fresh store (tableSizeBytes=0) over an already-full
	// backing must still prune — seed the size from the table, don't grow
	// unbounded. Without the seed, Evict's under-cap early return would skip.
	restarted := NewCodeStore(1<<20, 128)
	require.Zero(t, restarted.tableSizeBytes.Load())
	require.NoError(t, restarted.Evict(tx))
	require.LessOrEqual(t, restarted.tableSizeBytes.Load(), int64(128),
		"a fresh store must seed its size from the backing and prune, not grow unbounded across restarts")
}
