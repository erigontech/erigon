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

package snapshot

import "sort"

// StepRange represents a half-open interval [From, To) of aggregator steps.
type StepRange struct {
	From uint64
	To   uint64
}

// Len returns the number of steps in the range.
func (r StepRange) Len() uint64 {
	if r.To <= r.From {
		return 0
	}
	return r.To - r.From
}

// Contains returns true if step is within [From, To).
func (r StepRange) Contains(step uint64) bool {
	return step >= r.From && step < r.To
}

// Overlaps returns true if the two ranges share any steps.
func (r StepRange) Overlaps(other StepRange) bool {
	return r.From < other.To && other.From < r.To
}

// Covers returns true if r fully contains other.
func (r StepRange) Covers(other StepRange) bool {
	return r.From <= other.From && r.To >= other.To
}

// StepRanges is a sorted, non-overlapping list of step ranges.
// The zero value is an empty range set.
type StepRanges []StepRange

// Normalize sorts and merges overlapping/adjacent ranges.
func (rs StepRanges) Normalize() StepRanges {
	if len(rs) <= 1 {
		return rs
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].From < rs[j].From })

	merged := StepRanges{rs[0]}
	for _, r := range rs[1:] {
		last := &merged[len(merged)-1]
		if r.From <= last.To {
			// Overlapping or adjacent — extend.
			if r.To > last.To {
				last.To = r.To
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// Coverage returns the total number of steps covered.
func (rs StepRanges) Coverage() uint64 {
	var total uint64
	for _, r := range rs {
		total += r.Len()
	}
	return total
}

// Contains returns true if the given step is covered by any range.
func (rs StepRanges) Contains(step uint64) bool {
	for _, r := range rs {
		if r.Contains(step) {
			return true
		}
		if step < r.From {
			return false // sorted, no point continuing
		}
	}
	return false
}

// IsComplete returns true if the ranges cover [from, to) without gaps.
func (rs StepRanges) IsComplete(from, to uint64) bool {
	norm := rs.Normalize()
	for _, r := range norm {
		if r.From <= from && r.To >= to {
			return true
		}
		if r.From <= from && r.To > from {
			from = r.To
		}
	}
	return from >= to
}

// Gaps returns the uncovered ranges within [from, to).
func (rs StepRanges) Gaps(from, to uint64) StepRanges {
	norm := rs.Normalize()
	var gaps StepRanges
	cursor := from
	for _, r := range norm {
		if r.From >= to {
			break
		}
		if r.From > cursor {
			gaps = append(gaps, StepRange{cursor, min(r.From, to)})
		}
		if r.To > cursor {
			cursor = r.To
		}
	}
	if cursor < to {
		gaps = append(gaps, StepRange{cursor, to})
	}
	return gaps
}

// GapsAgainst returns ranges that other covers but rs does not.
// Useful for: "what does this peer have that I'm missing?"
func (rs StepRanges) GapsAgainst(other StepRanges) StepRanges {
	otherNorm := other.Normalize()
	if len(otherNorm) == 0 {
		return nil
	}
	// Find the full extent of other, then compute our gaps within that extent.
	from := otherNorm[0].From
	to := otherNorm[len(otherNorm)-1].To

	ourGaps := rs.Gaps(from, to)

	// Intersect our gaps with other's coverage (only report gaps where other actually has data).
	var result StepRanges
	for _, gap := range ourGaps {
		for _, r := range otherNorm {
			if r.From >= gap.To {
				break
			}
			if r.To <= gap.From {
				continue
			}
			// Intersection of gap and r.
			intFrom := max(gap.From, r.From)
			intTo := min(gap.To, r.To)
			if intFrom < intTo {
				result = append(result, StepRange{intFrom, intTo})
			}
		}
	}
	return result
}

// Union returns the combined coverage of rs and other.
func (rs StepRanges) Union(other StepRanges) StepRanges {
	combined := make(StepRanges, 0, len(rs)+len(other))
	combined = append(combined, rs...)
	combined = append(combined, other...)
	return combined.Normalize()
}
