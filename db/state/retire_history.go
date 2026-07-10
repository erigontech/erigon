// Copyright 2024 The Erigon Authors
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
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// entirelyBeforeStep returns the dirty source files backing the visible files entirely
// below cutoff step — selecting over visible (not dirty), so retire drops only what the
// node serves.
func entirelyBeforeStep(files visibleFiles, stepSize uint64, cutoff kv.Step) (outs []*FilesItem) {
	for _, f := range files {
		if _, endStep := f.src.StepRange(stepSize); endStep <= cutoff {
			outs = append(outs, f.src)
		}
	}
	return outs
}

// filesBeforeStep selects (does not mutate) the .ef/.efi dirty files entirely below cutoff.
// mutate detaches them from dirtyFiles and must run under dirtyFilesLock.
func (iit *InvertedIndexRoTx) filesBeforeStep(cutoff kv.Step) (deleted []string, outs []*FilesItem, mutate func()) {
	outs = entirelyBeforeStep(iit.files, iit.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(iit.ii.dirs.Snap)...)
	}
	mutate = func() { retire(iit.ii.dirtyFiles, outs, iit.ii.FilenameBase, retireReasonAged, iit.ii.logger) }
	return deleted, outs, mutate
}

// filesBeforeStep selects History (.v) and its InvertedIndex (.ef) files together, so the
// two never diverge. mutate must run under dirtyFilesLock.
func (ht *HistoryRoTx) filesBeforeStep(cutoff kv.Step) (deleted []string, outs []*FilesItem, mutate func()) {
	iDeleted, iOuts, iMutate := ht.iit.filesBeforeStep(cutoff)
	deleted = append(deleted, iDeleted...)
	outs = append(outs, iOuts...)

	hOuts := entirelyBeforeStep(ht.files, ht.stepSize, cutoff)
	for _, out := range hOuts {
		deleted = append(deleted, out.FilePaths(ht.h.dirs.Snap)...)
	}
	outs = append(outs, hOuts...)
	mutate = func() {
		iMutate()
		retire(ht.h.dirtyFiles, hOuts, ht.h.FilenameBase, retireReasonAged, ht.h.logger)
	}
	return deleted, outs, mutate
}

// Retire drops old visible History+InvertedIndex files below their per-domain cutoff.
// Reads visible only — invisible garbage is the merge clean-up's job (cleanAfterMerge /
// RemoveOverlaps). Physical deletion is deferred until no reader pins the retired generation.
func (a *Aggregator) Retire(ctx context.Context, cutoffs kv.RetireCutoffs) (retiredCount int, err error) {
	if cutoffs.IsNoop() {
		return 0, nil
	}

	// Paranoia: retire deletes files. A cutoff reaching the visible tip means the retention
	// window collapsed and every file would be retired — almost always a caller bug (a distance
	// computed as ~0). Refuse rather than wipe the node's history. The tip is the ceiling the
	// visible files retire selects over are clamped to.
	if tip := a.dirtyFilesEndTxNumMinimax(); tip > 0 {
		cutoff := cutoffs.Default
		for _, c := range cutoffs.PerDomain {
			cutoff = max(cutoff, c)
		}
		if cutoff >= tip {
			return 0, fmt.Errorf("retire refused: cutoff %d >= visible tip %d (retention window too small; would retire all files)", cutoff, tip)
		}
	}

	at := a.BeginFilesRo()
	defer at.Close()

	// Select (lock-free): read the visible files backing each entity below its cutoff.
	var deleted []string
	var retired []*FilesItem
	var mutations []func()
	selectEntity := func(deletedNames []string, outs []*FilesItem, mutate func()) {
		deleted = append(deleted, deletedNames...)
		retired = append(retired, outs...)
		mutations = append(mutations, mutate)
	}
	for _, dt := range at.d {
		if dt.d.Disable || dt.d.SnapshotsDisabled || dt.d.HistoryDisabled {
			continue
		}
		cutoffTxNum := cutoffs.Default
		if txNum, ok := cutoffs.PerDomain[dt.name]; ok {
			cutoffTxNum = txNum
		}
		cutoffStep := kv.Step(cutoffTxNum / dt.stepSize)
		if cutoffStep == 0 {
			continue
		}
		selectEntity(dt.ht.filesBeforeStep(cutoffStep))
	}
	for _, iit := range at.standaloneIIs() {
		if iit.ii.Disable {
			continue
		}
		cutoffStep := kv.Step(cutoffs.Default / iit.stepSize)
		if cutoffStep == 0 {
			continue
		}
		selectEntity(iit.filesBeforeStep(cutoffStep))
	}

	if len(retired) == 0 {
		return 0, nil
	}

	// Mutate (locked): detach from dirtyFiles, notify the seeder, and publish a new visible
	// generation — atomically, so readers observe one cross-entity-consistent step.
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	for _, mutate := range mutations {
		mutate()
	}
	a.onFilesDelete(deleted)
	a.recalcVisibleFiles(retired)

	mxRetiredHistoryFiles.AddInt(len(retired))
	a.logger.Info("[snapshots] retired old history files", "removed", len(retired), "cutoffs", cutoffs.String(a.StepSize()))
	return len(retired), nil
}
