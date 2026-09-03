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

package flow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// blockEntry builds a peer block-file entry the way the manifest paths
// do: PopulateFromName fills the BLOCK axis for files with no Domain
// and leaves FromStep/ToStep at zero ("step unknown until a commitment
// binding establishes it" — see snapshot.FileEntry's field docs).
func blockEntry(t *testing.T, name string) *snapshot.FileEntry {
	t.Helper()
	e := &snapshot.FileEntry{Name: name}
	require.True(t, snapshot.PopulateFromName(e), "fixture name must parse: %s", name)
	require.Zero(t, e.FromStep, "block files carry no step axis — fixture must reflect that")
	return e
}

// localBlockInventory returns an inventory holding one local block file
// covering [from, to), which is what establishes the local tip.
func localBlockInventory(t *testing.T, name string) *snapshot.Inventory {
	t.Helper()
	inv := snapshot.NewInventory()
	e := blockEntry(t, name)
	e.Local = true
	require.NoError(t, inv.AddFile(e))
	return inv
}

// TestFilterPeerBlockEntriesByLocalTip_KeepsEntriesAboveTip pins the
// filter's stated purpose: drop peer entries overlapping what we
// already hold, keep the ones above our tip so peer-driven catch-up
// still fetches the blocks we are missing.
//
// The filter compares against a tip derived from local ToBlock, so the
// entry side must be read on the same axis. Reading FromStep instead
// compares a step against a block number — and since block files never
// carry a step axis, every entry reads as 0, falls at-or-below any
// non-zero tip, and the filter drops the entire peer manifest.
func TestFilterPeerBlockEntriesByLocalTip_KeepsEntriesAboveTip(t *testing.T) {
	t.Parallel()
	// Local holds [3400000, 3410000); tip is 3410000.
	inv := localBlockInventory(t, "v1.1-003400-003410-headers.seg")

	above := blockEntry(t, "v1.1-003410-003420-headers.seg")
	got := filterPeerBlockEntriesByLocalTip([]*snapshot.FileEntry{above}, inv)

	require.Len(t, got, 1,
		"entry starting at our tip is the next range we lack — dropping it kills peer-driven catch-up")
	require.Equal(t, "v1.1-003410-003420-headers.seg", got[0].Name)
}

// TestFilterPeerBlockEntriesByLocalTip_DropsOverlappingEntries pins the
// complement: an entry starting below our tip overlaps what we hold and
// would re-materialise visible-set drift, so it must be dropped.
func TestFilterPeerBlockEntriesByLocalTip_DropsOverlappingEntries(t *testing.T) {
	t.Parallel()
	inv := localBlockInventory(t, "v1.1-003400-003410-headers.seg")

	overlapping := blockEntry(t, "v1.1-003400-003500-headers.seg")
	got := filterPeerBlockEntriesByLocalTip([]*snapshot.FileEntry{overlapping}, inv)

	require.Empty(t, got,
		"a pre-unwind wide file starting below our tip must not be re-fetched")
}

// TestFilterPeerBlockEntriesByLocalTip_ColdStartPassesThrough pins the
// bootstrap escape: with no local block files there is no tip, so every
// peer entry survives and first-time sync can fetch the whole set.
func TestFilterPeerBlockEntriesByLocalTip_ColdStartPassesThrough(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()

	entries := []*snapshot.FileEntry{
		blockEntry(t, "v1.1-000000-000500-headers.seg"),
		blockEntry(t, "v1.1-003410-003420-headers.seg"),
	}
	got := filterPeerBlockEntriesByLocalTip(entries, inv)

	require.Len(t, got, 2, "cold start (no local block files) must be a pass-through")
}
