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

package membatchwithdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// newTestOverlay creates a minimal MemoryMutation for domain overlay testing.
// No temporal DB or aggregator — just the in-memory overlay layer.
func newTestOverlay(parent *MemoryMutation) *MemoryMutation {
	mem := newMemStore()
	memDB := &memStoreDB{store: mem}
	m := &MemoryMutation{
		memDb:          memDB,
		memTx:          mem,
		deletedEntries: make(map[string]map[string]struct{}),
		deletedDups:    map[string]map[string]map[string]struct{}{},
		clearedTables:  make(map[string]struct{}),
	}
	if parent != nil {
		m.db = parent
	}
	return m
}

// TestDomainOverlay verifies that DomainPut writes are visible through
// GetLatest on a MemoryMutation, and that nested overlays (child on
// parent) properly chain domain reads.
func TestDomainOverlay(t *testing.T) {
	t.Parallel()

	// Create parent overlay and write a domain entry.
	parent := newTestOverlay(nil)
	defer parent.Close()

	key := []byte("test-account-key-00000000000000000000")
	val := []byte{0x42, 0x43}
	require.NoError(t, parent.DomainPut(kv.AccountsDomain, key, val, 0, nil))

	// Read back from parent overlay.
	got, _, err := parent.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Equal(t, val, got, "parent overlay should return the written value")

	// Create child overlay on top of parent — reads should chain through.
	child := newTestOverlay(parent)
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

	// FlushDomains — merge child into parent.
	require.NoError(t, child.FlushDomains(parent))
	got7, _, err := parent.GetLatest(kv.AccountsDomain, key)
	require.NoError(t, err)
	require.Nil(t, got7, "after FlushDomains, parent should see child's delete")
}
