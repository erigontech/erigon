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

package fileset

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Each test pins one of the observed wedge scenarios from the
// mode-B soak history. The test names map 1:1 to the table in
// .claude/plans/time-to-get-back-generic-mist.md.

// ---------- StalePastTip ----------

// TestStalePastTip_PastOrphan: file `003050-003060` after an unwind
// to chunk-aligned tip 3,041,000 is entirely past tip — stale.
func TestStalePastTip_PastOrphan(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_050_000, To: 3_060_000}},
	}
	require.Equal(t, []int{0}, StalePastTip(items, 3_041_000))
}

// TestStalePastTip_NarrowStraddler is the iter-5 soak wedge fix
// (2026-06-27). A narrow file `003040-003050` straddles tip
// 3,041,000: From ≤ tip but To > tip. The pre-fix predicate
// `From > toBlock` skipped this; the new predicate `To > tip`
// catches it.
func TestStalePastTip_NarrowStraddler(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_040_000, To: 3_050_000}},
	}
	require.Equal(t, []int{0}, StalePastTip(items, 3_041_000))
}

// TestStalePastTip_RebuildExactlyAtTip: the canonical rebuild output
// `003000-003041` has To = tip = 3,041,000. To > tip is false; kept.
func TestStalePastTip_RebuildExactlyAtTip(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_041_000}},
	}
	require.Empty(t, StalePastTip(items, 3_041_000))
}

// TestStalePastTip_EntirelyBelowTip: original 10k chunks below tip
// remain on disk (Inventory will exclude any subsumed by a wider
// rebuild via the maximality rule — that's a separate pass).
func TestStalePastTip_EntirelyBelowTip(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_010_000}},
		{Range: Range{From: 3_010_000, To: 3_020_000}},
	}
	require.Empty(t, StalePastTip(items, 3_041_000))
}

// ---------- StaleNonMaximal ----------

// TestStaleNonMaximal_SubsumedByWide (M-A): a narrow file inside a
// wider one. Default direction = narrower stale. Models the original
// 10k chunk surviving under the iter-4 wide rebuild.
func TestStaleNonMaximal_SubsumedByWide(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_041_000}}, // wide (kept)
		{Range: Range{From: 3_000_000, To: 3_010_000}}, // narrow (stale)
	}
	require.Equal(t, []int{1}, sortInts(StaleNonMaximal(items)))
}

// TestStaleNonMaximal_UnionCoverByRegens (M-B): the 2026-06-25
// v2.0-accounts wedge as a unit test. Broad pre-mode-B file `272-280`
// co-exists with narrow boundary-regen files `272-276`, `276-278`,
// `278-280`. The narrows collectively tile the broad's range. With
// regen tagging, the broad is the stale one — the narrower tiling is
// the canonical refinement.
func TestStaleNonMaximal_UnionCoverByRegens(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 272, To: 280}},                        // broad pre-mode-B (stale)
		{Range: Range{From: 272, To: 276}, ProducedByRegen: true}, // narrow regen
		{Range: Range{From: 276, To: 278}, ProducedByRegen: true}, // narrow regen
		{Range: Range{From: 278, To: 280}, ProducedByRegen: true}, // narrow regen
	}
	require.Equal(t, []int{0}, sortInts(StaleNonMaximal(items)))
}

// TestStaleNonMaximal_UnionCoverWithoutRegenTags: same topology but
// no regen tagging — default M-A direction takes over. The narrows
// are proper subsets of the broad, so they're stale; broad survives.
// This is the merge case: merge output supersedes original chunks.
func TestStaleNonMaximal_UnionCoverWithoutRegenTags(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 272, To: 280}},
		{Range: Range{From: 272, To: 276}},
		{Range: Range{From: 276, To: 278}},
		{Range: Range{From: 278, To: 280}},
	}
	require.Equal(t, []int{1, 2, 3}, sortInts(StaleNonMaximal(items)))
}

// TestStaleNonMaximal_NoOverlap: independent ranges, all maximal.
func TestStaleNonMaximal_NoOverlap(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 100, To: 200}},
		{Range: Range{From: 300, To: 400}},
	}
	require.Empty(t, StaleNonMaximal(items))
}

// TestStaleNonMaximal_IdenticalRangeNotStrictSubset: identical
// ranges are not "proper" subsets — neither is removed by M-A. (In
// practice the file system can't hold two files with identical names,
// so this case shouldn't arise; the test pins the predicate boundary.)
func TestStaleNonMaximal_IdenticalRangeNotStrictSubset(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 100, To: 200}},
		{Range: Range{From: 100, To: 200}},
	}
	require.Empty(t, StaleNonMaximal(items))
}

// ---------- CullPlan ----------

// TestCullPlan_iter5SoakLayout is the iter-5 soak wedge as a single
// pinned scenario. The on-disk snapshot dir at the wedge point held:
//   - wide rebuild output  `003000-003041` (kept)
//   - 4 original 10k chunks  `003000-003010` ... `003030-003040`
//     (narrower than the wide rebuild; default M-A direction → stale)
//   - narrow straddler      `003040-003050` (straddles tip → past-tip stale)
//   - past-tip orphan       `003050-003060` (entirely past tip → stale)
//
// With CullPlan({all}, tip=3_041_000) the wide rebuild and nothing
// else survives. Removing the 4 below-tip narrows is the
// maximality cleanup the existing site-by-site code didn't do; the
// straddler + past-tip orphan are the tip rule.
func TestCullPlan_iter5SoakLayout(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_041_000}}, // 0 — wide rebuild (kept)
		{Range: Range{From: 3_000_000, To: 3_010_000}}, // 1 — narrow, stale (M-A)
		{Range: Range{From: 3_010_000, To: 3_020_000}}, // 2 — narrow, stale (M-A)
		{Range: Range{From: 3_020_000, To: 3_030_000}}, // 3 — narrow, stale (M-A)
		{Range: Range{From: 3_030_000, To: 3_040_000}}, // 4 — narrow, stale (M-A)
		{Range: Range{From: 3_040_000, To: 3_050_000}}, // 5 — straddler, stale (past-tip)
		{Range: Range{From: 3_050_000, To: 3_060_000}}, // 6 — orphan, stale (past-tip)
	}
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, sortInts(CullPlan(items, 3_041_000)))
}

// TestCullPlan_Idempotent: running the plan, removing the items,
// then running again on the survivors yields an empty plan.
func TestCullPlan_Idempotent(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_041_000}},
		{Range: Range{From: 3_000_000, To: 3_010_000}},
		{Range: Range{From: 3_040_000, To: 3_050_000}},
	}
	removed := CullPlan(items, 3_041_000)
	keep := make([]Tagged, 0, len(items)-len(removed))
	gone := make(map[int]struct{}, len(removed))
	for _, idx := range removed {
		gone[idx] = struct{}{}
	}
	for i, it := range items {
		if _, isGone := gone[i]; !isGone {
			keep = append(keep, it)
		}
	}
	require.Empty(t, CullPlan(keep, 3_041_000),
		"second pass on the survivors must reach fixpoint")
}

// TestCullPlan_AllMaximalBelowTip: a healthy contiguous chain of
// non-overlapping files below tip — nothing stale.
func TestCullPlan_AllMaximalBelowTip(t *testing.T) {
	t.Parallel()
	items := []Tagged{
		{Range: Range{From: 3_000_000, To: 3_010_000}},
		{Range: Range{From: 3_010_000, To: 3_020_000}},
		{Range: Range{From: 3_020_000, To: 3_030_000}},
	}
	require.Empty(t, CullPlan(items, 3_041_000))
}

// TestCullPlan_Empty: degenerate input.
func TestCullPlan_Empty(t *testing.T) {
	t.Parallel()
	require.Empty(t, CullPlan(nil, 3_041_000))
}

func sortInts(in []int) []int {
	out := append([]int(nil), in...)
	sort.Ints(out)
	return out
}
