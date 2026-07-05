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

// HistoryRetireCutoffs is the step below which frozen files are retired, per
// domain (PerDomain, falling back to Default for other domains and standalone
// indices); a 0 cutoff keeps the entity.
type HistoryRetireCutoffs struct {
	Default   kv.Step
	PerDomain map[kv.Domain]kv.Step
}

func (c HistoryRetireCutoffs) forDomain(name kv.Domain) kv.Step {
	if step, ok := c.PerDomain[name]; ok {
		return step
	}
	return c.Default
}

// IsNoop reports whether every cutoff is 0, so nothing would be retired.
func (c HistoryRetireCutoffs) IsNoop() bool {
	if c.Default != 0 {
		return false
	}
	for _, step := range c.PerDomain {
		if step != 0 {
			return false
		}
	}
	return true
}

// RetireOldHistoryFiles retires History+InvertedIndex files entirely below their
// cutoff; physical deletion is deferred until no reader pins the retired generation.
func (a *Aggregator) RetireOldHistoryFiles(ctx context.Context, cutoffs HistoryRetireCutoffs) (retiredCount int, err error) {
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
		cutoffStep := cutoffs.forDomain(dt.name)
		if cutoffStep == 0 {
			continue
		}
		names, r := dt.ht.retireBeforeStep(cutoffStep)
		deleted = append(deleted, names...)
		retired = append(retired, r...)
	}
	for _, iit := range at.standaloneIIs() {
		if iit.ii.Disable || cutoffs.Default == 0 {
			continue
		}
		names, r := iit.retireBeforeStep(cutoffs.Default)
		deleted = append(deleted, names...)
		retired = append(retired, r...)
	}

	if len(retired) == 0 {
		return 0, nil
	}

	a.onFilesDelete(deleted)
	a.recalcVisibleFiles(retired)

	mxRetiredHistoryFiles.AddInt(len(retired))
	a.logger.Info("[snapshots] retired old history files", "removed", len(retired), "defaultStep", cutoffs.Default)
	return len(retired), nil
}
