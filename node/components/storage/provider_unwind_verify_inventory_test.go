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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// These tests pin the mirror of findInventoryOrphansPastBlock: AFTER an
// unwind commits, no Inventory entry may cover blocks past toBlock.
// findInventoryOrphansPastBlock catches the IN direction (file on disk
// not in inventory); these check the OUT direction (entry in inventory
// past target, regardless of disk state).
//
// Symptom this catches: iter 3 of the 2026-06-14 mode-B soak reported
// inv_extras=3 — three Inventory entries past target=2,984,451 that
// were left over from an in-flight Retire whose ctx wasn't cancelled
// before the unwind ran. The cancel-before-unwind fix (SetHead +
// BlockRetire.CancelInFlight) should prevent the symptom; this check
// is a defense-in-depth assertion run inside the same tx so any
// regression rolls the unwind back instead of committing silent corruption.

func TestFindInventoryEntriesPastBlock_NoInventory(t *testing.T) {
	t.Parallel()
	got, err := (&Provider{}).findInventoryEntriesPastBlock(2_900_000)
	require.NoError(t, err)
	require.Nil(t, got, "bare provider (no Inventory) returns no entries — clean no-op")
}

func TestFindInventoryEntriesPastBlock_AllEntriesAtOrBelowTarget(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	for _, fe := range []*snapshot.FileEntry{
		{Name: "v1.1-002800-002900-headers.seg", FromBlock: 2_800_000, ToBlock: 2_900_000, Local: true},
		{Name: "v1.1-002900-002910-headers.seg", FromBlock: 2_900_000, ToBlock: 2_910_000, Local: true},
		{Name: "v1.1-002910-002984-headers.seg", FromBlock: 2_910_000, ToBlock: 2_984_451, Local: true},
	} {
		require.NoError(t, inv.AddFile(fe))
	}
	p := &Provider{Inventory: inv}
	got, err := p.findInventoryEntriesPastBlock(2_984_451)
	require.NoError(t, err)
	require.Empty(t, got, "every entry covers blocks at or below toBlock — none past")
}

func TestFindInventoryEntriesPastBlock_EntriesPastTargetSurface(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	// In-range entries (must NOT surface):
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{
		Name: "v1.1-002800-002900-headers.seg", FromBlock: 2_800_000, ToBlock: 2_900_000, Local: true,
	}))
	// Past-target entries — exactly the iter 3 inv_extras=3 wedge:
	pastTarget := []*snapshot.FileEntry{
		{Name: "v1.1-002990-002991-headers.seg", FromBlock: 2_990_000, ToBlock: 2_991_000, Local: true},
		{Name: "v1.1-002991-002992-bodies.seg", FromBlock: 2_991_000, ToBlock: 2_992_000, Local: true},
		{Name: "v1.1-003007-003008-transactions.seg", FromBlock: 3_007_000, ToBlock: 3_008_000, Local: true},
	}
	for _, fe := range pastTarget {
		require.NoError(t, inv.AddFile(fe))
	}
	p := &Provider{Inventory: inv}
	got, err := p.findInventoryEntriesPastBlock(2_984_451)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{
			"v1.1-002990-002991-headers.seg",
			"v1.1-002991-002992-bodies.seg",
			"v1.1-003007-003008-transactions.seg",
		},
		got,
		"every entry strictly past toBlock must surface — this would have caught iter 3's inv_extras=3 wedge at commit time instead of in the recovery phase 1800s later")
}

func TestFindInventoryEntriesPastBlock_StraddleEntryIsKept(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	// A straddle file whose ToBlock > targetBlock but FromBlock ≤
	// targetBlock is a legitimate boundary file — Provider.Unwind has
	// dedicated straddle-rebuild logic for this case. The post-unwind
	// check must NOT flag it.
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{
		Name: "v1.1-002980-002990-headers.seg", FromBlock: 2_980_000, ToBlock: 2_990_000, Local: true,
	}))
	p := &Provider{Inventory: inv}
	got, err := p.findInventoryEntriesPastBlock(2_984_451)
	require.NoError(t, err)
	require.Empty(t, got,
		"a straddle entry whose FromBlock <= toBlock must not surface — only entries entirely past toBlock are inv_extras")
}
