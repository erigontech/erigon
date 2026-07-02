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

// entirelyBeforeStep returns dirty files entirely below cutoff step.
func entirelyBeforeStep(dirtyFiles *DirtyFiles, stepSize uint64, cutoff kv.Step) (outs []*FilesItem) {
	iter := dirtyFiles.Iter()
	defer iter.Release()
	for ok := iter.First(); ok; ok = iter.Next() {
		item := iter.Item()
		if _, endStep := item.StepRange(stepSize); endStep <= cutoff {
			outs = append(outs, item)
		}
	}
	return outs
}

// retireBeforeStep removes .ef/.efi dirty files entirely below cutoff.
func (iit *InvertedIndexRoTx) retireBeforeStep(cutoff kv.Step) (deleted []string, retired []*FilesItem) {
	outs := entirelyBeforeStep(iit.ii.dirtyFiles, iit.stepSize, cutoff)
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

	outs := entirelyBeforeStep(ht.h.dirtyFiles, ht.stepSize, cutoff)
	for _, out := range outs {
		deleted = append(deleted, out.FilePaths(ht.h.dirs.Snap)...)
	}
	retire(ht.h.dirtyFiles, outs, ht.h.FilenameBase, retireReasonAged, ht.h.logger)
	retired = append(retired, outs...)
	return deleted, retired
}

// RetireOldHistoryFiles retires History+InvertedIndex files entirely below
// cutoffStep. Physical deletion is deferred until no reader still pins the
// retired generation.
func (a *Aggregator) RetireOldHistoryFiles(ctx context.Context, cutoffStep kv.Step) (retiredCount int, err error) {
	if cutoffStep == 0 {
		return 0, nil
	}
	at := a.BeginFilesRo()
	defer at.Close()

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()

	var deleted []string
	var retired []*FilesItem
	for _, dt := range at.d {
		// commitment.history and rcache have special cli flags: --prune.include-commitment-history --persist.receipt
		// if they enabled they are never pruned - it's current logic. we will change it in future PR's - but for now keep them
		// See: https://github.com/erigontech/erigon/issues/21306 'step 4'
		if dt.name == kv.CommitmentDomain || dt.name == kv.RCacheDomain {
			continue
		}
		if dt.d.Disable || dt.d.SnapshotsDisabled || dt.d.HistoryDisabled {
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
	a.logger.Info("[snapshots] retired old history files", "removed", len(retired), "cutoffStep", cutoffStep)
	return len(retired), nil
}
