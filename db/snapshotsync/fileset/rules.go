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

// Package fileset provides the pure topology rules a snapshot file set
// must satisfy. Both block-snapshot files (range = block numbers) and
// state-domain files (range = txNums or steps) use the same rules with
// the appropriate unit.
//
// Two invariants:
//
//  1. Tip: no item I with I.To > canonicalTip. Catches both past-tip
//     orphans (I.From ≥ tip) AND straddlers (I.From < tip < I.To).
//
//  2. Maximality: no item I subsumed by another. The wider item wins;
//     the narrower (proper subset) is removed. Merge output supersedes
//     the original chunks under this rule. Mode-B's boundary-step
//     regen does not produce overlapping ranges — its output is
//     written under a truncated filename matching its actual coverage
//     and the original broad file is removed in the same transaction
//     (see node/components/storage/provider_unwind_state_regen_wire.go)
//     — so the rule never has to disambiguate broad-vs-truncated co-
//     existence at runtime.
package fileset

// Range is a half-open interval [From, To).
type Range struct {
	From uint64
	To   uint64
}

// Tagged carries a Range for use in the rule predicates. The struct
// exists so the rules can grow new tiebreaker tags in the future
// without changing every call site; today there are no tags.
type Tagged struct {
	Range
}

// StalePastTip returns the indices of items violating the tip
// invariant: item I is stale iff I.To > tip.
//
// One predicate covers both past-tip orphans (I.From ≥ tip) and
// straddlers (I.From < tip < I.To). Items entirely at or below the
// tip (I.To ≤ tip) are kept.
func StalePastTip(items []Tagged, tip uint64) []int {
	var out []int
	for i, it := range items {
		if it.To > tip {
			out = append(out, i)
		}
	}
	return out
}

// StaleNonMaximal returns the indices of items violating the
// maximality invariant: item I is stale if it is a proper subset of
// some other item J (J.From ≤ I.From, J.To ≥ I.To, at least one
// inequality strict). The wider J wins; the narrower I is removed.
//
// This is the M-A case from the rules' documented two-shape model.
// The M-B "union-cover" case (a wider untagged item exactly tiled by
// narrower regen-tagged items) is no longer in scope: mode-B's
// regen path writes its output under a filename matching its actual
// coverage and atomically removes the original broad file in the
// same transaction, so the disambiguation the M-B branch was meant
// to perform at runtime is enforced at the write layer instead.
func StaleNonMaximal(items []Tagged) []int {
	stale := make(map[int]struct{})
	for i, ii := range items {
		for j, jj := range items {
			if i == j {
				continue
			}
			subset := jj.From <= ii.From && jj.To >= ii.To
			strict := jj.From < ii.From || jj.To > ii.To
			if subset && strict {
				stale[i] = struct{}{}
				break
			}
		}
	}
	out := make([]int, 0, len(stale))
	for i := range stale {
		out = append(out, i)
	}
	return out
}

// CullPlan returns the indices of items to remove, iterated to a
// fixpoint: StalePastTip ∪ StaleNonMaximal, then re-applied on the
// surviving set until no more items are flagged. Idempotent.
//
// Removing a wider item via StalePastTip can expose a narrower item
// that previously hid behind it as past-tip — running once isn't
// always enough. A fixed-point loop with a bound on iterations keeps
// the function total and easy to reason about.
func CullPlan(items []Tagged, tip uint64) []int {
	keep := make([]int, len(items))
	for i := range items {
		keep[i] = i
	}
	for iter := 0; iter < len(items)+1; iter++ {
		surv := make([]Tagged, 0, len(keep))
		survIdx := make([]int, 0, len(keep))
		for _, idx := range keep {
			surv = append(surv, items[idx])
			survIdx = append(survIdx, idx)
		}
		flagged := make(map[int]struct{})
		for _, k := range StalePastTip(surv, tip) {
			flagged[k] = struct{}{}
		}
		for _, k := range StaleNonMaximal(surv) {
			flagged[k] = struct{}{}
		}
		if len(flagged) == 0 {
			break
		}
		next := make([]int, 0, len(keep)-len(flagged))
		for k, origIdx := range survIdx {
			if _, gone := flagged[k]; gone {
				continue
			}
			next = append(next, origIdx)
		}
		keep = next
	}
	keepSet := make(map[int]struct{}, len(keep))
	for _, k := range keep {
		keepSet[k] = struct{}{}
	}
	removed := make([]int, 0, len(items)-len(keep))
	for i := range items {
		if _, kept := keepSet[i]; !kept {
			removed = append(removed, i)
		}
	}
	return removed
}
