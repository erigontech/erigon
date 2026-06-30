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

// TestLocalKVRanges_FiltersNonLocalEntries pins the iter-3-mode_b
// regression: Inventory.AllDomainFiles returns BOTH local entries and
// peer-discovered (Local=false) entries from peer chain.toml.v2
// manifests. The post-2026-06-30 regen loop only acts on disk via
// os.Rename / dir.RemoveFile, so any non-local entry must be filtered
// out BEFORE classification — otherwise the planner classifies a
// non-existent path and FinalizeUnwind logs noisy "no such file"
// warnings while the file the planner intended to regen stays
// untouched.
//
// Failure shape on hoodi iter-3 mode_b (depth 30k):
//   - Local inventory had accounts.280-282.kv (this node's truncated
//     file from iter-2's regen).
//   - Peer chain.toml.v2 advertised accounts.280-284.kv (a different
//     peer with a broader file). The consumer registered it as a non-
//     local Inventory entry.
//   - regenerateBoundaryStepFiles iterated AllDomainFiles, classified
//     both, tried to regen the .280-284.kv source — file doesn't
//     exist locally → noisy rename failures → real state stays stale
//     → first post-unwind block 3,092,684 wedged with 17,100-gas
//     mismatch.
func TestLocalKVRanges_FiltersNonLocalEntries(t *testing.T) {
	t.Parallel()
	entries := []*snapshot.FileEntry{
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 256, ToStep: 272, Local: true, Name: "v2.0-accounts.256-272.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 280, ToStep: 282, Local: true, Name: "v2.0-accounts.280-282.kv"},
		// Peer-discovered entry — exists in peer's chain.toml.v2 but
		// no file locally. Must NOT appear in the filtered output.
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 280, ToStep: 284, Local: false, Name: "v2.0-accounts.280-284.kv"},
	}

	files, ranges := localKVRanges(entries)

	require.Len(t, files, 3, "only the 3 Local=true entries must survive the filter")
	require.Len(t, ranges, 3, "ranges slice must be parallel to files slice")
	for _, f := range files {
		require.True(t, f.Local, "every returned entry must have Local=true")
		require.NotEqual(t, "v2.0-accounts.280-284.kv", f.Name,
			"peer-discovered .280-284 entry must be excluded — its file isn't on disk locally")
	}
	require.Equal(t, []stateFileRange{
		{FromStep: 0, ToStep: 256},
		{FromStep: 256, ToStep: 272},
		{FromStep: 280, ToStep: 282},
	}, ranges)
}

// TestLocalKVRanges_SkipsNonKVKinds verifies that the filter only keeps
// .kv files. Accessor/index sidecar entries (Kind != KindKV) live in
// the same per-domain bucket but must never reach the regen planner.
func TestLocalKVRanges_SkipsNonKVKinds(t *testing.T) {
	t.Parallel()
	entries := []*snapshot.FileEntry{
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.kv"},
		// Accessors (.bt/.kvi/.kvei) live under KindAccessor — must be
		// ignored. History (.v) and inverted-index (.ef) are KindHistory
		// and KindIdx respectively, also non-KV.
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindAccessor, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.bt"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.v"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindIdx, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.ef"},
	}

	files, ranges := localKVRanges(entries)

	require.Len(t, files, 1, "only the single .kv entry survives the kind filter")
	require.Equal(t, snapshot.KindKV, files[0].Kind)
	require.Equal(t, []stateFileRange{{FromStep: 0, ToStep: 256}}, ranges)
}

// TestLocalKVRanges_HandlesNilAndEmpty defensively covers two edge
// cases: an empty input slice (early-chain domain with nothing
// retired) and a nil entry (paranoia — shouldn't happen but a nil
// dereference would crash FinalizeUnwind).
func TestLocalKVRanges_HandlesNilAndEmpty(t *testing.T) {
	t.Parallel()

	files, ranges := localKVRanges(nil)
	require.Empty(t, files)
	require.Empty(t, ranges)

	entries := []*snapshot.FileEntry{
		nil,
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.kv"},
		nil,
	}
	files, ranges = localKVRanges(entries)
	require.Len(t, files, 1, "nil entries must be silently skipped")
	require.Len(t, ranges, 1)
}

// TestLocalKVRanges_Iter3HoodiLayout replays the EXACT on-disk +
// Inventory state from the 2026-06-30 iter-3 mode_b wedge. With both
// the local .280-282.kv AND the peer-discovered .280-284.kv present
// in Inventory, only the 7 local entries must reach the planner. The
// planner can then correctly classify .280-282.kv as actionRegenInPlace
// at stepBoundary=282 and leave .280-284 untouched (we don't own it).
func TestLocalKVRanges_Iter3HoodiLayout(t *testing.T) {
	t.Parallel()
	entries := []*snapshot.FileEntry{
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 256, Local: true, Name: "v2.0-accounts.0-256.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 256, ToStep: 272, Local: true, Name: "v2.0-accounts.256-272.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 272, ToStep: 276, Local: true, Name: "v2.0-accounts.272-276.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 272, ToStep: 280, Local: true, Name: "v2.0-accounts.272-280.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 276, ToStep: 278, Local: true, Name: "v2.0-accounts.276-278.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 278, ToStep: 279, Local: true, Name: "v2.0-accounts.278-279.kv"},
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 280, ToStep: 282, Local: true, Name: "v2.0-accounts.280-282.kv"},
		// Peer-claimed broader file. Pre-fix this entry's path
		// resolved against the local snapDir → "no such file" failure
		// in FinalizeUnwind.
		{Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 280, ToStep: 284, Local: false, Name: "v2.0-accounts.280-284.kv"},
	}

	files, ranges := localKVRanges(entries)
	require.Len(t, files, 7, "exactly the 7 local .kv entries — peer-claimed .280-284 must be excluded")

	classified := planStateFileActions(ranges, 282)
	require.Equal(t, []stateFileRange{
		{0, 256}, {256, 272}, {272, 276}, {272, 280}, {276, 278}, {278, 279},
	}, classified.keep, "every local file below the boundary stays put")
	require.Equal(t, []stateFileRange{{280, 282}}, classified.regen,
		"the boundary-aligned local file regens in place; peer's .280-284 is NOT considered")
	require.Equal(t, []bool{true}, classified.inPlace)
	require.Empty(t, classified.remove,
		"nothing locally entirely past boundary — the peer's .280-284 must NOT trigger a removal")
}
