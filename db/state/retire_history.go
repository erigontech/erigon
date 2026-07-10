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

	"github.com/erigontech/erigon/db/kv"
)

// entirelyBeforeStep returns the dirty source files backing the visible files entirely
// below cutoff step. It selects over visible (not dirty) so retire only ever drops files
// the node actually serves; invisible garbage (subsumed/overlapping) is left to the merge
// clean-up path (cleanAfterMerge / RemoveOverlaps).
func entirelyBeforeStep(files visibleFiles, stepSize uint64, cutoff kv.Step) (outs []*FilesItem) {
	for _, f := range files {
		if _, endStep := f.src.StepRange(stepSize); endStep <= cutoff {
			outs = append(outs, f.src)
		}
	}
	return outs
}

// retireBeforeStep removes .ef/.efi dirty files entirely below cutoff.
func (iit *InvertedIndexRoTx) retireBeforeStep(cutoff kv.Step) (deleted []string, retired []*FilesItem) {
	outs := entirelyBeforeStep(iit.files, iit.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(iit.ii.dirs.Snap)...)
	}
	retire(iit.ii.dirtyFiles, outs, iit.ii.FilenameBase, retireReasonAged, iit.ii.logger)
	retired = append(retired, outs...)
	return deleted, retired
}

// retireBeforeStep removes History (.v) and its InvertedIndex (.ef) files
// together, so the two never diverge.
func (ht *HistoryRoTx) retireBeforeStep(cutoff kv.Step) (deleted []string, retired []*FilesItem) {
	iNames, iRetired := ht.iit.retireBeforeStep(cutoff)
	deleted = append(deleted, iNames...)
	retired = append(retired, iRetired...)

	outs := entirelyBeforeStep(ht.files, ht.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(ht.h.dirs.Snap)...)
	}
	retire(ht.h.dirtyFiles, outs, ht.h.FilenameBase, retireReasonAged, ht.h.logger)
	retired = append(retired, outs...)
	return deleted, retired
}

// Retire drops old visible History+InvertedIndex files entirely below their per-domain
// cutoff — it reads/deletes only visible files, so it touches just what the node serves.
// Invisible garbage (subsumed/overlapping) is not retire's concern; it is dropped by the
// separate merge clean-up (cleanAfterMerge / RemoveOverlaps). Physical deletion is deferred
// until no reader pins the retired generation.
func (a *Aggregator) Retire(ctx context.Context, cutoffs kv.RetireCutoffs) (retiredCount int, err error) {
	if cutoffs.IsNoop() {
		return 0, nil
	}
	at := a.BeginFilesRo()
	defer at.Close()

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	var deleted []string
	var retired []*FilesItem
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
		names, r := dt.ht.retireBeforeStep(cutoffStep)
		deleted = append(deleted, names...)
		retired = append(retired, r...)
	}
	for _, iit := range at.standaloneIIs() {
		if iit.ii.Disable {
			continue
		}
		cutoffStep := kv.Step(cutoffs.Default / iit.stepSize)
		if cutoffStep == 0 {
			continue
		}
		names, r := iit.retireBeforeStep(cutoffStep)
		deleted = append(deleted, names...)
		retired = append(retired, r...)
	}

	if len(retired) == 0 {
		return 0, nil
	}

	a.onFilesDelete(deleted)
	a.recalcVisibleFiles(retired)

	mxRetiredHistoryFiles.AddInt(len(retired))
	a.logger.Info("[snapshots] retired old history files", "removed", len(retired), "cutoffs", cutoffs.String(a.StepSize()))
	return len(retired), nil
}
