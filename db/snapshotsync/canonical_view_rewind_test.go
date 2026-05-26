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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapcfg"
)

// TestDemoteByRewindPredicate_KeepsBlockFilesAtOrBelowToBlock pins the
// block-axis predicate. ParseRange applies the *1000 step→block
// convention to block-file names, so `v1.0-000000-000500-headers.seg`
// has range [0, 500_000). A rewind to ToBlock=499_999 retains the
// file (last covered block == ToBlock); a rewind to ToBlock=499_998
// demotes it (block 499_999 is above ToBlock).
func TestDemoteByRewindPredicate_KeepsBlockFilesAtOrBelowToBlock(t *testing.T) {
	t.Parallel()

	pred := DemoteByRewindPredicate(499_999)
	require.False(t, pred("v1.0-000000-000500-headers.seg"),
		"last-covered-block == ToBlock → keep")

	pred = DemoteByRewindPredicate(499_998)
	require.True(t, pred("v1.0-000000-000500-headers.seg"),
		"file's last block (499_999) is above ToBlock → drop")
}

// TestDemoteByRewindPredicate_DropsPostRewindBlockFiles confirms that
// every block file whose range starts above the rewind target is
// demoted.
func TestDemoteByRewindPredicate_DropsPostRewindBlockFiles(t *testing.T) {
	t.Parallel()

	pred := DemoteByRewindPredicate(500_000)

	// Strictly after the rewind point.
	require.True(t, pred("v1.0-000500-001000-headers.seg"))
	require.True(t, pred("v1.0-001000-002000-bodies.seg"))
	require.True(t, pred("v1.0-002000-003000-transactions.seg"))
	// Straddling the rewind point — drop, no partial-file surgery.
	require.True(t, pred("v1.0-000000-001000-headers.seg"))
}

// TestDemoteByRewindPredicate_LeavesStateFilesAlone confirms that
// state-domain files (step-axis ranges) are not touched by the
// block-axis predicate. A step→block-aware caller composes via Or.
func TestDemoteByRewindPredicate_LeavesStateFilesAlone(t *testing.T) {
	t.Parallel()

	pred := DemoteByRewindPredicate(100)
	require.False(t, pred("v2.0-accounts.0-2048.kv"),
		"state-domain files have step-axis ranges; block-axis predicate must skip them")
	require.False(t, pred("v2.0-storage.0-2048.kv"))
	require.False(t, pred("v2.0-commitment.0-2048.kv"))
}

// TestDemoteByRewindPredicate_DropsUnparseableNames confirms the
// defensive default: an unparseable name in the canonical observation
// set is not given the benefit of the doubt during a rewind. The
// predicate returns false (don't drop) for unparseable names ONLY
// because Demote with no info is the safer default; the false return
// is intentional and documented — the unparseable file just keeps
// behaving normally until a real predicate covers it.
func TestDemoteByRewindPredicate_DefensiveOnUnparseable(t *testing.T) {
	t.Parallel()

	pred := DemoteByRewindPredicate(100)
	require.False(t, pred("salt-state.txt"), "non-range file → ignored, not dropped")
	require.False(t, pred("random-garbage"), "unparseable → ignored, not dropped")
}

// TestOr_ComposesPredicates pins the composition helper used to stack
// the block-axis rewind predicate with a caller-supplied step-axis
// predicate.
func TestOr_ComposesPredicates(t *testing.T) {
	t.Parallel()

	always := func(string) bool { return true }
	never := func(string) bool { return false }

	never2 := func(string) bool { return false }
	require.True(t, Or(never, always)("x"))
	require.True(t, Or(always, never)("x"))
	require.False(t, Or(never, never2)("x"))
	require.True(t, Or(nil, always)("x"), "nil predicate is a clean no-op")
	require.False(t, Or()("x"), "no predicates → never drops")
}

// TestCanonicalView_DemoteByRewind_EndToEnd pins the canonical
// demotion path end-to-end: a promoted entry whose range exceeds the
// rewind target is dropped via the predicate, and the canonical set
// no longer carries it.
func TestCanonicalView_DemoteByRewind_EndToEnd(t *testing.T) {
	t.Parallel()

	genesis := snapcfg.PreverifiedItems{
		{Name: "v1.0-000000-000500-headers.seg", Hash: "g1"},
	}
	v := NewCanonicalView(genesis, snapcfg.QuorumConfig{F: 0.5, QFloor: 2})

	// Two verified issuers each advertise both pre-rewind and post-rewind files.
	preRewind := snapcfg.PreverifiedItems{
		{Name: "v1.0-000500-001000-headers.seg", Hash: "h1"},
	}
	postRewind := snapcfg.PreverifiedItems{
		{Name: "v1.0-001000-002000-headers.seg", Hash: "h2"},
	}
	merged := append(append(snapcfg.PreverifiedItems(nil), preRewind...), postRewind...)

	v.Observe([]byte("issuer-1"), merged, time.Now())
	v.Observe([]byte("issuer-2"), merged, time.Now())

	canonicalBefore := v.Canonical()
	require.Len(t, canonicalBefore, 3, "genesis + 2 promoted entries")

	// Rewind to block 800_000 — files entirely or partially above
	// must demote.
	// v1.0-000500-001000 → range [500_000, 1_000_000), straddles → drop
	// v1.0-001000-002000 → range [1_000_000, 2_000_000), entirely above → drop
	dropped := v.Demote(DemoteByRewindPredicate(800_000))
	require.Equal(t, 2, dropped, "both promoted post-rewind entries must demote")

	canonicalAfter := v.Canonical()
	require.Len(t, canonicalAfter, 1, "only genesis remains")
	require.Equal(t, "v1.0-000000-000500-headers.seg", canonicalAfter[0].Name)
}
