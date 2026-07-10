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

	"github.com/erigontech/erigon/common/dbg"
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
	return deleted, []agedFiles{iAged, agedFiles{ht.h.dirtyFiles, ht.h.FilenameBase, outs}}
}

// Retire drops old visible History+InvertedIndex files below their per-domain cutoff.
// Reads visible only — invisible garbage is the merge clean-up's job (cleanAfterMerge /
// RemoveOverlaps). Physical deletion is deferred until no reader pins the retired generation.
func (at *AggregatorRoTx) Retire(ctx context.Context, cutoffs kv.RetireCutoffs) (retiredCount int, err error) {
	if dbg.NoRetire() || cutoffs.IsNoop() {
		return 0, nil
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
		if len(dt.ht.files) == 0 {
			continue
		}
		if cutoffTxNum <= dt.ht.files.StartTxNum() {
			continue
		}
		if tip := dt.ht.files.EndTxNum(); cutoffTxNum >= tip {
			return 0, fmt.Errorf("retire refused: cutoff %d >= %s tip %d (would retire all its files)", cutoffTxNum, dt.name, tip)
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
		if len(iit.files) == 0 {
			continue
		}
		if cutoffs.Default <= iit.files.StartTxNum() {
			continue
		}
		if tip := iit.files.EndTxNum(); cutoffs.Default >= tip {
			return 0, fmt.Errorf("retire refused: cutoff %d >= %s tip %d (would retire all its files)", cutoffs.Default, iit.ii.FilenameBase, tip)
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

	// get lock only if have something to retire
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
