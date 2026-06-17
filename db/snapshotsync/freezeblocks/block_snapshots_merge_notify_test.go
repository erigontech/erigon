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

func (n *recordingNotifier) snapshotChanged() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, len(n.changed))
	copy(out, n.changed)
	return out
}

// NotifyMergedSnapshotFiles mirrors NotifyDeletedSnapshotFiles on the
// add direction. After a block-snapshot merge produces a wider .seg
// (e.g. v1.1-003020-003030 from 10 sub-chunks), the merged name must
// flow through NotifyOnFilesChange so the Provider's onChange hook
// runs inv.AddFile and Inventory ends up with the merged entry.
// Without this, Inventory loses sight of the file entirely (the
// sub-chunk entries are removed by NotifyDeletedSnapshotFiles but the
// merged entry is never added), straddleBlockFileForType returns nil
// at unwind time, the rebuild loop has no work, and the stale 10k
// chunk stays on disk past the new EL head — the wedge live-caught
// 2026-06-17 5-iter soak v11 iter 1 mode_b: EL head=3,027,899,
// snapshot file v1.1-003020-003030 on disk, Caplin's historical-
// download skipped blocks 3,027,900..3,029,999 because the EL's
// FrozenBlocks view picked up the stale file, gap-bridging failed.

func TestBlockRetire_NotifyMergedSnapshotFiles_FiresOnFilesChange(t *testing.T) {
	t.Parallel()
	notifier := &recordingNotifier{}
	br := &BlockRetire{filesNotifier: notifier}

	merged := []string{
		"v1.1-003020-003030-headers.seg",
		"v1.1-003020-003030-bodies.seg",
		"v1.1-003020-003030-transactions.seg",
	}
	br.NotifyMergedSnapshotFiles(merged)

	require.Equal(t, merged, notifier.snapshotChanged(),
		"NotifyMergedSnapshotFiles must propagate names through NotifyOnFilesChange so the Provider's onChange hook can run inv.AddFile — without this Inventory has no entry for the merged file and straddleBlockFileForType returns nil at unwind time")
}

func TestBlockRetire_NotifyMergedSnapshotFiles_NilNotifier(t *testing.T) {
	t.Parallel()
	br := &BlockRetire{}
	br.NotifyMergedSnapshotFiles([]string{"v1.1-003020-003030-headers.seg"})
}

func TestBlockRetire_NotifyMergedSnapshotFiles_EmptyList(t *testing.T) {
	t.Parallel()
	notifier := &recordingNotifier{}
	br := &BlockRetire{filesNotifier: notifier}

	br.NotifyMergedSnapshotFiles(nil)
	br.NotifyMergedSnapshotFiles([]string{})

	require.Empty(t, notifier.snapshotChanged())
}

// TestBlockRetire_NotifyMergedSnapshotFiles_DrivesInventoryAdd
// mirrors the delete-side end-to-end test: a recording notifier
// dispatches NotifyOnFilesChange to the onChange callback the way
// the production temporal DB does. We register a callback that adds
// to a tiny in-test inventory map, fire the change via BlockRetire,
// and assert the merged entry appears.
func TestBlockRetire_NotifyMergedSnapshotFiles_DrivesInventoryAdd(t *testing.T) {
	t.Parallel()
	inventory := map[string]bool{}
	var invMu sync.Mutex
	addToInventory := func(names []string) {
		invMu.Lock()
		defer invMu.Unlock()
		for _, name := range names {
			inventory[name] = true
		}
	}

	notifier := &recordingNotifier{}
	notifier.OnFilesChange(addToInventory, nil)
	br := &BlockRetire{filesNotifier: notifier}

	br.NotifyMergedSnapshotFiles([]string{
		"v1.1-003020-003030-headers.seg",
		"v1.1-003020-003030-bodies.seg",
		"v1.1-003020-003030-transactions.seg",
	})

	invMu.Lock()
	defer invMu.Unlock()
	require.Contains(t, inventory, "v1.1-003020-003030-headers.seg",
		"merged file must land in inventory")
	require.Contains(t, inventory, "v1.1-003020-003030-bodies.seg")
	require.Contains(t, inventory, "v1.1-003020-003030-transactions.seg")
}

// TestBlockRetire_MergeNotifyConvertsFullPathsToBasenames pins the
// path-normalisation contract: MergeBlocks captures names from
// merger.Merge's onMerge / onDelete callbacks (which use full paths
// via newDirtySegment.FilePath() / FilePaths) and from
// RemoveOverlaps (which uses relative paths). Both must be reduced
// to basenames before flowing through NotifyOnFilesChange /
// NotifyOnFilesDelete — otherwise the Provider's inv.AddFile /
// RemoveFile entries get keyed on a non-basename and later
// straddleBlockFileForType's filepath.Join(snapDir, e.Name) produces
// a doubled path (live-caught 2026-06-17 soak v13 iter 2 mode_b:
// `open old /erigon/tmp/.../snapshots/erigon/tmp/.../snapshots/v1.1-003020-003030-headers.seg`).
//
// The notification helpers themselves don't transform — they just
// forward. The transformation lives at the capture site in
// MergeBlocks (verified end-to-end by the soak; this test pins the
// helper contract that whatever the caller hands in, it is forwarded
// unchanged so callers know they own normalisation).
func TestBlockRetire_NotifyMergedSnapshotFiles_ForwardsNamesUnchanged(t *testing.T) {
	t.Parallel()
	notifier := &recordingNotifier{}
	br := &BlockRetire{filesNotifier: notifier}

	// Mix of basenames and full paths. The helper should forward
	// verbatim — caller is responsible for normalisation.
	in := []string{
		"v1.1-003020-003030-headers.seg",
		"/somewhere/absolute/v1.1-003020-003030-bodies.seg",
	}
	br.NotifyMergedSnapshotFiles(in)
	require.Equal(t, in, notifier.snapshotChanged())
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
