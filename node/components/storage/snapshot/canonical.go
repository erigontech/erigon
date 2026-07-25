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

// CanonicalLayout computes the deterministic file layout for a given step count.
//
// The layout uses power-of-2 aligned files: at each position, the largest
// power-of-2 file that fits and is aligned is used. This produces a minimal
// set of files that covers [0, steps) without gaps or overlaps.
//
// The result is deterministic: given the same step count, all nodes produce
// the same file boundaries. Deeper merges replace files but never invalidate
// them — [0-4096) replaces [0-2048) + [2048-4096), but both were correct.
//
// maxMergeSize caps the largest file. A node that hasn't completed deep merges
// uses a smaller cap, producing more files at a shallower merge level.
// Pass 0 for no cap (unlimited merging).
func CanonicalLayout(steps uint64, maxMergeSize uint64) StepRanges {
	if steps == 0 {
		return nil
	}

	var files StepRanges
	var pos uint64
	for pos < steps {
		remaining := steps - pos

		// Largest power-of-2 that pos is aligned to.
		var maxAligned uint64
		if pos == 0 {
			maxAligned = 1 << 40 // effectively unlimited for pos=0
		} else {
			maxAligned = pos & -pos
		}

		// Largest power-of-2 that fits in remaining.
		maxFit := uint64(1)
		for maxFit*2 <= remaining {
			maxFit *= 2
		}

		size := min(maxAligned, maxFit)
		if maxMergeSize > 0 && size > maxMergeSize {
			size = maxMergeSize
		}

		files = append(files, StepRange{pos, pos + size})
		pos += size
	}

	return files
}

// CanonicalLevel returns the merge depth (largest file size) in a canonical layout.
// Useful for advertising in ENR: "my deepest merge is N steps".
func CanonicalLevel(layout StepRanges) uint64 {
	var maxSize uint64
	for _, r := range layout {
		if s := r.Len(); s > maxSize {
			maxSize = s
		}
	}
	return maxSize
}

// MissingMerges computes the merges needed to go from the current file layout
// to a target layout. Each returned range represents a merge that should be
// executed: combine all current files overlapping that range into one file.
//
// The returned ranges are ordered deepest-first (largest merges first),
// which is the correct execution order for convergence.
func MissingMerges(current, target StepRanges) StepRanges {
	// Build a set of current ranges for fast lookup.
	currentSet := make(map[StepRange]bool, len(current))
	for _, r := range current {
		currentSet[r] = true
	}

	// Find target ranges that don't exist in current.
	var missing StepRanges
	for _, r := range target {
		if !currentSet[r] {
			missing = append(missing, r)
		}
	}

	// Sort deepest first (largest ranges first) for correct merge ordering.
	sortByLenDesc(missing)
	return missing
}

func sortByLenDesc(rs StepRanges) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Len() > rs[j-1].Len(); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// IsCanonical returns true if the given file layout matches any valid canonical
// layout (at any merge depth) for the given step count. A layout is canonical
// if every file is at a power-of-2 boundary and the files cover [0, steps)
// without gaps.
func IsCanonical(layout StepRanges, steps uint64) bool {
	if len(layout) == 0 {
		return steps == 0
	}

	// Must cover [0, steps) completely.
	if !layout.IsComplete(0, steps) {
		return false
	}

	// Each file must be power-of-2 sized and aligned.
	for _, r := range layout {
		size := r.Len()
		if size == 0 {
			return false
		}
		// Power of 2 check.
		if size&(size-1) != 0 {
			return false
		}
		// Alignment check: From must be divisible by size.
		if r.From%size != 0 {
			return false
		}
	}

	return true
}
