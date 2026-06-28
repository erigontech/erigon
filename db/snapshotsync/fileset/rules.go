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
//  2. Maximality: no item I subsumed by another, with the winner
//     direction tagged. The default direction is "wider wins, narrower
//     removed" (merge output supersedes original chunks). Items tagged
//     ProducedByRegen=true reverse the direction when they exactly tile
//     a wider untagged item (boundary regens supersede pre-regen broad
//     files).
package fileset

// Range is a half-open interval [From, To).
type Range struct {
	From uint64
	To   uint64
}

// Tagged carries a Range plus metadata that drives the maximality
// tiebreaker. ProducedByRegen=true marks an item as the output of a
// boundary-step regeneration; such items win against a broader,
// untagged predecessor that their union exactly tiles.
type Tagged struct {
	Range
	ProducedByRegen bool
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
// maximality invariant.
//
// M-A (single-dominator): I is a proper subset of some J in the set
// (J.From ≤ I.From, J.To ≥ I.To, at least one inequality strict). I is
// stale unless the tiebreaker reverses it.
//
// M-B (union-cover): I is exactly tiled — without gaps or overlaps —
// by a set of items {J_k} all tagged ProducedByRegen=true. Then I is
// stale (the regen tiling wins).
//
// Tiebreaker for M-A: when I is a proper subset of J and exactly one
// of them is ProducedByRegen, the non-regen file is stale. When both
// or neither is ProducedByRegen, the narrower (proper subset) is
// stale — the default "wider wins" rule.
func StaleNonMaximal(items []Tagged) []int {
	stale := make(map[int]struct{})

	// M-A pass: pairwise proper-subset check with tagged tiebreaker.
	for i, ii := range items {
		for j, jj := range items {
			if i == j {
				continue
			}
			// Is i a (possibly improper) subset of j?
			subset := jj.From <= ii.From && jj.To >= ii.To
			strict := jj.From < ii.From || jj.To > ii.To
			if !subset || !strict {
				continue
			}
			// i is a strict subset of j. Choose the loser.
			if ii.ProducedByRegen && !jj.ProducedByRegen {
				// regen i (narrower) wins; j (wider, untagged) is stale.
				stale[j] = struct{}{}
			} else {
				// default direction: narrower (i) is stale.
				stale[i] = struct{}{}
			}
		}
	}

	// M-B pass: for each untagged item I, look for a set of
	// regen-tagged items whose ranges exactly tile [I.From, I.To)
	// with no gaps. If such a tiling exists, I is stale.
	for i, it := range items {
		if it.ProducedByRegen {
			continue
		}
		if _, already := stale[i]; already {
			continue
		}
		if exactTileByRegens(items, it.Range, i) {
			stale[i] = struct{}{}
		}
	}

	out := make([]int, 0, len(stale))
	for i := range stale {
		out = append(out, i)
	}
	return out
}

// exactTileByRegens reports whether the subset of `items` (excluding
// index `skipIdx`) that is tagged ProducedByRegen and lies fully
// within target exactly tiles target with no gaps and no overlaps.
func exactTileByRegens(items []Tagged, target Range, skipIdx int) bool {
	tiles := make([]Range, 0)
	for k, it := range items {
		if k == skipIdx {
			continue
		}
		if !it.ProducedByRegen {
			continue
		}
		if it.From < target.From || it.To > target.To {
			continue
		}
		tiles = append(tiles, it.Range)
	}
	if len(tiles) == 0 {
		return false
	}
	for i := 1; i < len(tiles); i++ {
		for j := i; j > 0 && tiles[j-1].From > tiles[j].From; j-- {
			tiles[j-1], tiles[j] = tiles[j], tiles[j-1]
		}
	}
	if tiles[0].From != target.From {
		return false
	}
	cursor := tiles[0].To
	for k := 1; k < len(tiles); k++ {
		if tiles[k].From != cursor {
			return false
		}
		cursor = tiles[k].To
	}
	return cursor == target.To
}

// CullPlan returns the indices of items to remove, iterated to a
// fixpoint: StalePastTip ∪ StaleNonMaximal, then re-applied on the
// surviving set until no more items are flagged. Idempotent.
//
// Removing a wider item via M-B can expose a narrower item that
// previously hid behind it as past-tip — running once isn't always
// enough. A fixed-point loop with a bound on iterations keeps the
// function total and easy to reason about.
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
