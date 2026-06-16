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

package freezeblocks

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// These tests pin the merge-deletion notification contract that
// Inventory relies on to stay in sync with disk. The wedge: in iter
// 1 mode_a2 of the 2026-06-15 soak, Provider.Unwind tried to open
// v1.1-003019-003020-headers.seg (a per-step retire sub-chunk) but
// the file no longer existed on disk — a merge had consolidated it
// into v1.1-003010-003020-headers.seg. Inventory still held the
// stale sub-chunk entry because BlockRetire.MergeBlocks never fired
// NotifyOnFilesDelete, so Provider's onDelete-callback hook never
// ran inv.RemoveFile.

// recordingNotifier is a kv.SnapshotNotifier stub that records every
// NotifyOnFilesChange / NotifyOnFilesDelete call. Used to assert
// that BlockRetire fires the delete notification when sub-chunks are
// consumed by a merge.
type recordingNotifier struct {
	mu             sync.Mutex
	changed        []string
	deleted        []string
	registeredOnCh kv.OnFilesChange
	registeredOnDe kv.OnFilesChange
}

func (n *recordingNotifier) OnFilesChange(onChange, onDelete kv.OnFilesChange) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.registeredOnCh = onChange
	n.registeredOnDe = onDelete
}

func (n *recordingNotifier) NotifyOnFilesChange(names []string) {
	n.mu.Lock()
	n.changed = append(n.changed, names...)
	cb := n.registeredOnCh
	n.mu.Unlock()
	if cb != nil {
		cb(names)
	}
}

func (n *recordingNotifier) NotifyOnFilesDelete(names []string) {
	n.mu.Lock()
	n.deleted = append(n.deleted, names...)
	cb := n.registeredOnDe
	n.mu.Unlock()
	if cb != nil {
		cb(names)
	}
}

func (n *recordingNotifier) snapshotDeleted() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, len(n.deleted))
	copy(out, n.deleted)
	return out
}

func TestBlockRetire_NotifyDeletedSnapshotFiles_FiresOnFilesDelete(t *testing.T) {
	t.Parallel()
	notifier := &recordingNotifier{}
	br := &BlockRetire{filesNotifier: notifier}

	deleted := []string{
		"v1.1-003018-003019-headers.seg",
		"v1.1-003018-003019-bodies.seg",
		"v1.1-003018-003019-transactions.seg",
	}
	br.NotifyDeletedSnapshotFiles(deleted)

	require.Equal(t, deleted, notifier.snapshotDeleted(),
		"NotifyDeletedSnapshotFiles must propagate names through NotifyOnFilesDelete so the Provider's onDelete hook can run inv.RemoveFile — without this Inventory keeps stale sub-chunk entries after a merge")
}

func TestBlockRetire_NotifyDeletedSnapshotFiles_NilNotifier(t *testing.T) {
	t.Parallel()
	// No notifier wired (CLI tool / standalone use). Must not panic.
	br := &BlockRetire{}
	br.NotifyDeletedSnapshotFiles([]string{"v1.1-003018-003019-headers.seg"})
}

func TestBlockRetire_NotifyDeletedSnapshotFiles_EmptyList(t *testing.T) {
	t.Parallel()
	notifier := &recordingNotifier{}
	br := &BlockRetire{filesNotifier: notifier}

	br.NotifyDeletedSnapshotFiles(nil)
	br.NotifyDeletedSnapshotFiles([]string{})

	require.Empty(t, notifier.snapshotDeleted(),
		"empty-list calls are no-ops — no spurious empty Delete notifications fired")
}

// TestBlockRetire_NotifyDeletedSnapshotFiles_DrivesInventoryRemoval
// pins the wiring end-to-end: the recording notifier mirrors how the
// production temporal DB dispatches NotifyOnFilesDelete to the
// onDelete callback. We register a callback that removes from a tiny
// in-test inventory map, fire the delete via BlockRetire, and assert
// the inventory entries are gone. The Provider's real onDelete hook
// (node/components/storage/provider.go) does the same thing with
// snapshot.Inventory.RemoveFile.
func TestBlockRetire_NotifyDeletedSnapshotFiles_DrivesInventoryRemoval(t *testing.T) {
	t.Parallel()
	inventory := map[string]bool{
		"v1.1-003010-003011-headers.seg":      true,
		"v1.1-003010-003011-bodies.seg":       true,
		"v1.1-003010-003011-transactions.seg": true,
		"v1.1-003011-003012-headers.seg":      true,
		"v1.1-003011-003012-bodies.seg":       true,
		"v1.1-003011-003012-transactions.seg": true,
		// Post-merge superset:
		"v1.1-003010-003020-headers.seg":      true,
		"v1.1-003010-003020-bodies.seg":       true,
		"v1.1-003010-003020-transactions.seg": true,
	}
	var invMu sync.Mutex
	removeFromInventory := func(names []string) {
		invMu.Lock()
		defer invMu.Unlock()
		for _, name := range names {
			delete(inventory, name)
		}
	}

	notifier := &recordingNotifier{}
	notifier.OnFilesChange(nil, removeFromInventory)
	br := &BlockRetire{filesNotifier: notifier}

	// Merge consolidated the two 003010-003011 and 003011-003012
	// sub-chunks into 003010-003020. Fire the delete notify for the
	// sub-chunks; the merged superset stays.
	br.NotifyDeletedSnapshotFiles([]string{
		"v1.1-003010-003011-headers.seg",
		"v1.1-003010-003011-bodies.seg",
		"v1.1-003010-003011-transactions.seg",
		"v1.1-003011-003012-headers.seg",
		"v1.1-003011-003012-bodies.seg",
		"v1.1-003011-003012-transactions.seg",
	})

	invMu.Lock()
	defer invMu.Unlock()
	require.NotContains(t, inventory, "v1.1-003010-003011-headers.seg",
		"sub-chunk must be removed from inventory after merge")
	require.NotContains(t, inventory, "v1.1-003011-003012-headers.seg",
		"sub-chunk must be removed from inventory after merge")
	require.Contains(t, inventory, "v1.1-003010-003020-headers.seg",
		"merged superset must remain in inventory")
}
