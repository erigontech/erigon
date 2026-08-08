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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// TestDomain_RetireSubsumedV4Items — the retire path removes v4
// dirtyFiles items whose range starts at step*stepSize and ends
// within [step*stepSize, (step+1)*stepSize]. Non-v4 files (step-
// aligned endTxNum) and v4 items anchored at a different step are
// left alone.
//
// Runs against an empty in-memory Domain (no MDBX writes needed —
// we're pinning the dirtyFiles filter logic, not the on-disk
// interaction).
func TestDomain_RetireSubsumedV4Items(t *testing.T) {
	t.Parallel()

	// Minimal Domain with just the fields the retire path reads.
	// stepSize comes from the embedded *History; dirtyFiles is a btree
	// of *FilesItem. retireSubsumedV4Items reads startTxNum, endTxNum,
	// decompressor from items and calls the package's retire() helper.
	const stepSize = uint64(1000)
	const step = kv.Step(3)
	d := &Domain{
		History: &History{
			InvertedIndex: &InvertedIndex{stepSize: stepSize},
		},
		dirtyFiles: newDirtyFiles(),
	}

	// Case matrix — three items go into dirtyFiles:
	//   A: step-aligned .3-4 (startTxNum=3000, endTxNum=4000)  — NOT a v4, MUST survive
	//   B: v4 at target step (startTxNum=3000, endTxNum=3500)  — v4, MUST be retired
	//   C: v4 at target step (startTxNum=3000, endTxNum=3999)  — v4, MUST be retired
	//   D: v4 at DIFFERENT step (startTxNum=2000, endTxNum=2500) — v4 for step 2, NOT step 3, MUST survive
	//   E: v4 at target step but endTxNum > (step+1)*stepSize (startTxNum=3000, endTxNum=4500) — spills past step, MUST survive
	itemA := &FilesItem{startTxNum: 3000, endTxNum: 4000}
	itemB := &FilesItem{startTxNum: 3000, endTxNum: 3500}
	itemC := &FilesItem{startTxNum: 3000, endTxNum: 3999}
	itemD := &FilesItem{startTxNum: 2000, endTxNum: 2500}
	itemE := &FilesItem{startTxNum: 3000, endTxNum: 4500}
	for _, it := range []*FilesItem{itemA, itemB, itemC, itemD, itemE} {
		d.dirtyFiles.Set(it)
	}

	retired := d.retireSubsumedV4Items(step)

	// Expected retired set: B and C (v4, in-step).
	require.Len(t, retired, 2)
	retiredSet := map[*FilesItem]struct{}{retired[0]: {}, retired[1]: {}}
	require.Contains(t, retiredSet, itemB)
	require.Contains(t, retiredSet, itemC)

	// dirtyFiles now contains only A, D, E.
	var remaining []*FilesItem
	d.dirtyFiles.Scan(func(it *FilesItem) bool {
		remaining = append(remaining, it)
		return true
	})
	require.Len(t, remaining, 3)
	remainingSet := map[*FilesItem]struct{}{}
	for _, it := range remaining {
		remainingSet[it] = struct{}{}
	}
	require.Contains(t, remainingSet, itemA)
	require.Contains(t, remainingSet, itemD)
	require.Contains(t, remainingSet, itemE)

	// canDelete flag is set on retired items so reclaimRetiredLocked
	// physically unlinks them.
	require.True(t, itemB.canDelete.Load())
	require.True(t, itemC.canDelete.Load())
	require.False(t, itemA.canDelete.Load())
	require.False(t, itemD.canDelete.Load())
	require.False(t, itemE.canDelete.Load())
}

// TestDomain_RetireSubsumedV4Items_Empty — no v4 items in dirtyFiles
// returns nil without touching dirtyFiles.
func TestDomain_RetireSubsumedV4Items_Empty(t *testing.T) {
	t.Parallel()

	const stepSize = uint64(1000)
	d := &Domain{
		History: &History{
			InvertedIndex: &InvertedIndex{stepSize: stepSize},
		},
		dirtyFiles: newDirtyFiles(),
	}
	itemA := &FilesItem{startTxNum: 3000, endTxNum: 4000} // step-aligned only
	d.dirtyFiles.Set(itemA)

	retired := d.retireSubsumedV4Items(kv.Step(3))
	require.Empty(t, retired)
	require.False(t, itemA.canDelete.Load())
}
