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
	"context"
	"errors"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func NewPBinRangeWriterWithLimitsForTest(aggregator *Aggregator, domain kv.Domain, endTxNum uint64, maxOps, maxBytes int) (*PBinRangeWriter, error) {
	if maxOps <= 0 || maxBytes <= 0 {
		return nil, errors.New("pbin range writer: invalid batch limits")
	}
	return newPBinRangeWriter(aggregator, domain, endTxNum, PBinRangeWriterLimits{MaxOps: maxOps, MaxBytes: maxBytes})
}

func NewPBinRangeWriterForResume(aggregator *Aggregator, domain kv.Domain, baseTxNum, endTxNum uint64) (*PBinRangeWriter, error) {
	return newPBinRangeWriterForResume(aggregator, domain, baseTxNum, endTxNum, nil, 0, nil)
}

func VerifyPBinDomainRootWithStats(ctx context.Context, tx kv.TemporalTx, aggregator *Aggregator, domain kv.Domain, stats *pbt.VerifyStats) (common.Hash, error) {
	return verifyPBinDomainRoot(ctx, tx, aggregator, domain, stats)
}

func (a *Aggregator) DirtyFilesLockFreeForTest() bool {
	if !a.dirtyFilesLock.TryLock() {
		return false
	}
	a.dirtyFilesLock.Unlock()
	return true
}

func (w *PBinRangeWriter) CheckRowStampsForTest() func() int {
	lookups := 0
	w.stampTracker = func() pbinStampTracker {
		return &pbinCheckedStampTracker{got: newPBinRowStampTracker(), want: newPBinReferenceRowStampTracker(), lookups: &lookups}
	}
	return func() int { return lookups }
}

func (w *PBinRangeWriter) SetPipelineHookForTest(hook func(string, int) error) {
	w.pipelineHook = hook
}

func (w *PBinRangeWriter) SetResumeSourceLayoutForTest() {
	w.resumeSourceLayout = true
}

func (w *PBinRangeWriter) SetWorkersForTest(workers int) {
	w.workers = workers
}

func (w *PBinRangeWriter) SetRowLaneSkipBufferForTest(size uint64) {
	for i := range w.ranges {
		w.ranges[i].rows.skip.Close()
		w.ranges[i].rows.skip = etl.NewCollector("pbin-range-"+w.ranges[i].rows.name+"-skip", w.ranges[i].rows.tmpDir, etl.NewSortableBuffer(datasize.ByteSize(size)), log.Root()).MergeInBackground(true)
	}
}

func (w *PBinRangeWriter) RowLaneTempDirsForTest() []string {
	dirs := make([]string, 0, len(w.ranges))
	for i := range w.ranges {
		dirs = append(dirs, w.ranges[i].rows.tmpDir)
	}
	return dirs
}
