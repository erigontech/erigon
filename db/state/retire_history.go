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

// agedFiles is one entity's dirty files selected for retirement, carried from the lock-free
// selection pass to the locked detach pass.
type agedFiles struct {
	dirtyFiles   *DirtyFiles
	filenameBase string
	files        []*FilesItem
}

// filesBeforeStep selects (does not mutate) the .ef/.efi dirty files entirely below cutoff.
func (iit *InvertedIndexRoTx) filesBeforeStep(cutoff kv.Step) (deleted []string, aged agedFiles) {
	outs := entirelyBeforeStep(iit.files, iit.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(iit.ii.dirs.Snap)...)
	}
	return deleted, agedFiles{iit.ii.dirtyFiles, iit.ii.FilenameBase, outs}
}

// filesBeforeStep selects History (.v) and its InvertedIndex (.ef) files together, so the
// two never diverge.
func (ht *HistoryRoTx) filesBeforeStep(cutoff kv.Step) (deleted []string, aged []agedFiles) {
	iDeleted, iAged := ht.iit.filesBeforeStep(cutoff)
	deleted = append(deleted, iDeleted...)

	outs := entirelyBeforeStep(ht.files, ht.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(ht.h.dirs.Snap)...)
	}
	return deleted, []agedFiles{iAged, {ht.h.dirtyFiles, ht.h.FilenameBase, outs}}
}

// visibleFilesMaxEndTxNum is the highest endTxNum among the visible history/II files — the
// tip of what retire selects over. Read from the pinned visible bundle (not dirtyFiles), and
// naturally 0 only when there are no visible files at all (an empty entity contributes nothing).
func (at *AggregatorRoTx) visibleFilesMaxEndTxNum() (tip uint64) {
	for _, dt := range at.d {
		if dt == nil {
			continue
		}
		if f := dt.ht.files; len(f) > 0 {
			tip = max(tip, f[len(f)-1].endTxNum)
		}
	}
	for _, iit := range at.standaloneIIs() {
		if iit == nil {
			continue
		}
		if f := iit.files; len(f) > 0 {
			tip = max(tip, f[len(f)-1].endTxNum)
		}
	}
	return tip
}

// Retire drops old visible History+InvertedIndex files below their per-domain cutoff.
// Reads visible only — invisible garbage is the merge clean-up's job (cleanAfterMerge /
// RemoveOverlaps). Physical deletion is deferred until no reader pins the retired generation.
func (at *AggregatorRoTx) Retire(ctx context.Context, cutoffs kv.RetireCutoffs) (retiredCount int, err error) {
	if cutoffs.IsNoop() {
		return 0, nil
	}

	// Paranoia: retire deletes files. A cutoff reaching the visible tip means the retention
	// window collapsed and every file would be retired — almost always a caller bug (a distance
	// computed as ~0). Refuse rather than wipe the node's history.
	if tip := at.visibleFilesMaxEndTxNum(); tip > 0 {
		cutoff := cutoffs.Default
		for _, c := range cutoffs.PerDomain {
			cutoff = max(cutoff, c)
		}
		if cutoff >= tip {
			return 0, fmt.Errorf("retire refused: cutoff %d >= visible tip %d (retention window too small; would retire all files)", cutoff, tip)
		}
	}

	// Select over visible files (no lock — at.d/at.iis are the pinned visible generation).
	var deleted []string
	var aged []agedFiles
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
		if len(dt.ht.files) == 0 || cutoffTxNum <= dt.ht.files[0].startTxNum {
			continue
		}
		d, agedList := dt.ht.filesBeforeStep(cutoffStep)
		deleted = append(deleted, d...)
		aged = append(aged, agedList...)
	}
	for _, iit := range at.standaloneIIs() {
		if iit.ii.Disable {
			continue
		}
		cutoffStep := kv.Step(cutoffs.Default / iit.stepSize)
		if cutoffStep == 0 {
			continue
		}
		d, agedList := iit.filesBeforeStep(cutoffStep)
		deleted = append(deleted, d...)
		aged = append(aged, agedList)
	}

	var retired []*FilesItem
	for _, agedList := range aged {
		retired = append(retired, agedList.files...)
	}
	if len(retired) == 0 {
		return 0, nil
	}

	// Detach from dirtyFiles + publish the new visible generation atomically. Only this needs
	// the lock: it mutates the dirtyFiles btrees that recalcVisibleFiles reads.
	at.a.dirtyFilesLock.Lock()
	defer at.a.dirtyFilesLock.Unlock()
	for _, agedList := range aged {
		retire(agedList.dirtyFiles, agedList.files, agedList.filenameBase, retireReasonAged, at.a.logger)
	}
	at.a.onFilesDelete(deleted)
	at.a.recalcVisibleFiles(retired)

	mxRetiredHistoryFiles.AddInt(len(retired))
	at.a.logger.Info("[snapshots] retired old history files", "removed", len(retired), "cutoffs", cutoffs.String(at.a.StepSize()))
	return len(retired), nil
}
