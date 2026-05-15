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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapcfg"
)

// TestHeldRanges_BlockKind_Contiguous covers the happy path: a
// manifest advertising contiguous headers ranges produces a sorted
// sequence of HeldRange entries with no merging.
func TestHeldRanges_BlockKind_Contiguous(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "01"},
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "02"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "03"},
	}

	got := HeldRanges(items, "headers")
	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},        // ParseFileName converts K-units → blocks
		{From: 1_000_000, To: 2_000_000},
		{From: 2_000_000, To: 3_000_000},
	}, got,
		"contiguous block ranges return sorted, distinct entries (no merging — sparsity preserved by construction)")
}

// TestHeldRanges_BlockKind_Sparse pins the core sparse property:
// a peer holding non-contiguous ranges has them stay non-contiguous
// in the output. This is the virtualization-readiness invariant.
func TestHeldRanges_BlockKind_Sparse(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "01"},
		preverified.Item{Name: "v1.1-002000-003000-headers.seg", Hash: "02"}, // gap at 1000-2000
		preverified.Item{Name: "v1.1-005000-006000-headers.seg", Hash: "03"}, // bigger gap 3000-5000
	}

	got := HeldRanges(items, "headers")
	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
		{From: 2_000_000, To: 3_000_000},
		{From: 5_000_000, To: 6_000_000},
	}, got,
		"sparse advertisements stay sparse — no implicit fill of gaps")
}

// TestHeldRanges_KindFiltering pins that only entries matching the
// requested kind are returned. A manifest advertising headers, bodies,
// and transactions returns only the requested type.
func TestHeldRanges_KindFiltering(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "01"},
		preverified.Item{Name: "v1.1-000000-001000-bodies.seg", Hash: "02"},
		preverified.Item{Name: "v1.1-000000-001000-transactions.seg", Hash: "03"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "04"},
	}

	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
		{From: 1_000_000, To: 2_000_000},
	}, HeldRanges(items, "headers"))

	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
	}, HeldRanges(items, "bodies"))

	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
	}, HeldRanges(items, "transactions"))
}

// TestHeldRanges_StateKind pins that state-domain entries return
// their step ranges (NOT block ranges). The same helper covers
// canonical (chain.toml) and per-node (chain.<enr>.toml) inputs;
// the kind argument disambiguates.
func TestHeldRanges_StateKind(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "01"},
		preverified.Item{Name: "domain/v2.0-commitment.8192-8704.kv", Hash: "02"},
		preverified.Item{Name: "domain/v2.0-accounts.0-8192.kv", Hash: "03"},
	}

	require.Equal(t, []HeldRange{
		{From: 0, To: 8192},
		{From: 8192, To: 8704},
	}, HeldRanges(items, "commitment"),
		"state-domain ranges are in STEP units (not converted to blocks) — different unit from block kinds")

	require.Equal(t, []HeldRange{
		{From: 0, To: 8192},
	}, HeldRanges(items, "accounts"))
}

// TestHeldRanges_ExtensionDeduplication pins that .seg + .idx +
// .torrent entries for the same range collapse to a single HeldRange.
// Callers want to know which ranges are covered, not how many torrent
// files cover them.
func TestHeldRanges_ExtensionDeduplication(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "01"},
		preverified.Item{Name: "v1.1-000000-001000-headers.idx", Hash: "02"},
		preverified.Item{Name: "v1.1-001000-002000-headers.seg", Hash: "03"},
	}

	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
		{From: 1_000_000, To: 2_000_000},
	}, HeldRanges(items, "headers"),
		"per-range deduplication: .seg primary file defines the range; .idx/.torrent for same range are collapsed")
}

// TestHeldRanges_CLDataIgnored pins that beaconblocks / blobsidecars
// / caplin-prefixed entries don't appear in results for EL kinds —
// even if the kind name happens to substring-match (e.g. there's no
// kind called "blobsidecars" but a defensive caller asking for it
// must not get any results either).
func TestHeldRanges_CLDataIgnored(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "01"},
		preverified.Item{Name: "v1.1-013500-014000-beaconblocks.seg", Hash: "c01"},
		preverified.Item{Name: "v1.1-013500-014000-blobsidecars.seg", Hash: "c02"},
		preverified.Item{Name: "caplin/v1.1-000000-000010-beaconblocks.seg", Hash: "c03"},
	}

	require.Equal(t, []HeldRange{
		{From: 0, To: 1_000_000},
	}, HeldRanges(items, "headers"),
		"CL entries don't leak into EL kind results")

	require.Empty(t, HeldRanges(items, "beaconblocks"),
		"asking for a CL kind returns empty — these are filtered at the isCLData boundary, not just by name mismatch")
}

// TestHeldRanges_EmptyManifest pins the zero case.
func TestHeldRanges_EmptyManifest(t *testing.T) {
	t.Parallel()

	require.Nil(t, HeldRanges(snapcfg.PreverifiedItems{}, "headers"))
	require.Nil(t, HeldRanges(snapcfg.PreverifiedItems{}, ""))
}

// TestHeldRanges_UnknownKind pins that asking for a kind that doesn't
// exist in the manifest returns empty (not error, not panic). Allows
// callers to probe defensively.
func TestHeldRanges_UnknownKind(t *testing.T) {
	t.Parallel()

	items := snapcfg.PreverifiedItems{
		preverified.Item{Name: "v1.1-000000-001000-headers.seg", Hash: "01"},
		preverified.Item{Name: "domain/v2.0-commitment.0-8192.kv", Hash: "02"},
	}

	require.Empty(t, HeldRanges(items, "nonexistent"))
}
