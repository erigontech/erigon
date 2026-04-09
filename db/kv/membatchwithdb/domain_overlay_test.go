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

package membatchwithdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

// TestDomainOverlay verifies that DomainPut writes are visible through
// GetLatest on a MemoryMutation, and that nested overlays (child on
// parent) properly chain domain reads.
func TestDomainOverlay(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	db := temporaltest.NewTestDB(t, dirs)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	// Create parent overlay and write a domain entry.
	parent, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	require.NoError(t, err)
	defer parent.Close()

	key := []byte("test-account-key-00000000000000000000")
	val := []byte{0x42, 0x43}
	require.NoError(t, parent.DomainPut(kv.AccountsDomain, key, val, 0, nil))

	// Read back from parent overlay.
	got, _, err := parent.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val, got, "parent overlay should return the written value")

	// Create child overlay on top of parent — reads should chain through.
	child, err := membatchwithdb.NewMemoryBatch(parent, "", logger)
	require.NoError(t, err)
	defer child.Close()

	got2, _, err := child.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val, got2, "child overlay should see parent's domain write")

	// Write to child, verify it shadows the parent.
	val2 := []byte{0x99}
	require.NoError(t, child.DomainPut(kv.AccountsDomain, key, val2, 0, nil))

	got3, _, err := child.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val2, got3, "child should return its own write")

	// Parent should still have the original value.
	got4, _, err := parent.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val, got4, "parent should be unaffected by child write")

	// DomainDel in child — verify deletion is visible.
	require.NoError(t, child.DomainDel(kv.AccountsDomain, key, 0, nil))
	got5, _, err := child.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Nil(t, got5, "child should return nil after DomainDel")

	// Parent still has the original value.
	got6, _, err := parent.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val, got6, "parent should still have value after child DomainDel")
}

// TestDomainDelPrefix verifies that DomainDelPrefix marks keys as deleted
// across the overlay chain (this overlay + parent overlays).
func TestDomainDelPrefix(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	db := temporaltest.NewTestDB(t, dirs)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	prefix := []byte{0xAA, 0xBB}
	keyA := append(common.Copy(prefix), 0x01)
	keyB := append(common.Copy(prefix), 0x02)
	keyC := []byte{0xCC, 0x01} // different prefix — should survive

	// Parent overlay has keyA, keyB (matching prefix) and keyC (different prefix).
	parent, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	require.NoError(t, err)
	defer parent.Close()

	require.NoError(t, parent.DomainPut(kv.StorageDomain, keyA, []byte{1}, 0, nil))
	require.NoError(t, parent.DomainPut(kv.StorageDomain, keyB, []byte{2}, 0, nil))
	require.NoError(t, parent.DomainPut(kv.StorageDomain, keyC, []byte{3}, 0, nil))

	// Child overlay adds keyD with the same prefix, then deletes the prefix.
	child, err := membatchwithdb.NewMemoryBatch(parent, "", logger)
	require.NoError(t, err)
	defer child.Close()

	keyD := append(common.Copy(prefix), 0x03)
	require.NoError(t, child.DomainPut(kv.StorageDomain, keyD, []byte{4}, 0, nil))

	require.NoError(t, child.DomainDelPrefix(kv.StorageDomain, prefix, 0))

	// All prefix-matching keys should be invisible through child.
	for _, k := range [][]byte{keyA, keyB, keyD} {
		v, _, err := child.GetLatest(kv.StorageDomain, k)
		require.NoError(t, err)
		require.Nil(t, v, "key %x should be deleted after DomainDelPrefix", k)
	}

	// keyC has a different prefix — should still be visible.
	v, _, err := child.GetLatest(kv.StorageDomain, keyC)
	require.NoError(t, err)
	require.Equal(t, []byte{3}, v, "keyC should survive DomainDelPrefix")

	// Parent overlay should be unaffected.
	va, _, err := parent.GetLatest(kv.StorageDomain, keyA)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, va, "parent should still have keyA")
}

// TestHasPrefixWithDeletions verifies that HasPrefix skips deleted keys
// and finds surviving keys in the same overlay.
func TestHasPrefixWithDeletions(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	db := temporaltest.NewTestDB(t, dirs)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	prefix := []byte{0xDD, 0xEE}
	key1 := append(common.Copy(prefix), 0x01)
	key2 := append(common.Copy(prefix), 0x02)

	// Parent overlay has both keys.
	parent, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	require.NoError(t, err)
	defer parent.Close()

	require.NoError(t, parent.DomainPut(kv.StorageDomain, key1, []byte{0x10}, 0, nil))
	require.NoError(t, parent.DomainPut(kv.StorageDomain, key2, []byte{0x20}, 0, nil))

	// Child overlay deletes key1 — HasPrefix should still find key2.
	child, err := membatchwithdb.NewMemoryBatch(parent, "", logger)
	require.NoError(t, err)
	defer child.Close()

	require.NoError(t, child.DomainDel(kv.StorageDomain, key1, 0, nil))

	_, _, found, err := child.HasPrefix(kv.StorageDomain, prefix)
	require.NoError(t, err)
	require.True(t, found, "HasPrefix should find key2 after key1 is deleted")

	// Delete key2 as well — now HasPrefix should return false.
	require.NoError(t, child.DomainDel(kv.StorageDomain, key2, 0, nil))

	_, _, found2, err := child.HasPrefix(kv.StorageDomain, prefix)
	require.NoError(t, err)
	require.False(t, found2, "HasPrefix should return false when all matching keys are deleted")
}

// TestHasPrefixNestedOverlay verifies that HasPrefix finds keys written
// in a parent overlay when queried from a child.
func TestHasPrefixNestedOverlay(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	db := temporaltest.NewTestDB(t, dirs)

	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()

	parent, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	require.NoError(t, err)
	defer parent.Close()

	prefix := []byte{0xF0, 0xF1}
	key := append(common.Copy(prefix), 0x42)
	val := []byte{0xBE, 0xEF}
	require.NoError(t, parent.DomainPut(kv.StorageDomain, key, val, 0, nil))

	// Child overlay — should see parent's key via HasPrefix.
	child, err := membatchwithdb.NewMemoryBatch(parent, "", logger)
	require.NoError(t, err)
	defer child.Close()

	k, v, found, err := child.HasPrefix(kv.StorageDomain, prefix)
	require.NoError(t, err)
	require.True(t, found, "child should find parent's key via HasPrefix")
	require.Equal(t, key, k)
	require.Equal(t, val, v)

	// Delete the key in the child — should now return false.
	require.NoError(t, child.DomainDel(kv.StorageDomain, key, 0, nil))

	_, _, found2, err := child.HasPrefix(kv.StorageDomain, prefix)
	require.NoError(t, err)
	require.False(t, found2, "child should not find deleted parent key")

	// Parent should still see it.
	_, _, found3, err := parent.HasPrefix(kv.StorageDomain, prefix)
	require.NoError(t, err)
	require.True(t, found3, "parent should still see its own key")
}
