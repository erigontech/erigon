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
	"fmt"
	"math"
	"slices"

	"github.com/erigontech/erigon/db/kv"
)

func (a *Aggregator) StepAlignedEnd(stepSize uint64) (uint64, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	return at.stepAlignedEnd(stepSize)
}

func (a *Aggregator) AlignFilesToStepSize(ctx context.Context, stepSize uint64) (uint64, error) {
	for _, d := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		if a.isDomainFrozen(d) || a.CommitmentDomainStopped(d) {
			return 0, fmt.Errorf("%s domain is frozen or stopped, its files cannot be merged", d)
		}
	}
	end, err := a.StepAlignedEnd(stepSize)
	if err != nil {
		return 0, err
	}
	for {
		merged, err := a.mergeStepAligned(ctx, stepSize, end)
		if err != nil {
			return 0, err
		}
		if !merged {
			return end, nil
		}
	}
}

func (a *Aggregator) mergeStepAligned(ctx context.Context, stepSize, end uint64) (bool, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	r, err := at.stepAlignedMergeRanges(stepSize, end)
	if err != nil {
		return false, err
	}
	if !r.any() {
		a.cleanAfterMerge(nil)
		return false, nil
	}
	outs, err := at.filesInRange(r)
	if err != nil {
		return false, err
	}
	in, err := at.mergeFiles(ctx, outs, r)
	if err != nil {
		in.Close()
		return false, err
	}
	a.integrateMergedDirtyFiles(in)
	return true, nil
}

func (at *AggregatorRoTx) stepAlignedFileLists() []visibleFiles {
	var lists []visibleFiles
	for _, d := range at.d {
		if d == nil || !d.d.Enabled {
			continue
		}
		lists = append(lists, d.files, d.ht.files, d.ht.iit.files)
	}
	for _, ii := range at.iis {
		if ii == nil || !ii.ii.Enabled {
			continue
		}
		lists = append(lists, ii.files)
	}
	return lists
}

func (at *AggregatorRoTx) stepAlignedEnd(stepSize uint64) (uint64, error) {
	lists := at.stepAlignedFileLists()
	end := uint64(math.MaxUint64)
	for _, files := range lists {
		if len(files) == 0 {
			continue
		}
		if start := files.StartTxNum(); start%stepSize != 0 {
			return 0, fmt.Errorf("%s starts at txNum %d, not a multiple of %d", files[0].Fullpath(), start, stepSize)
		}
		end = min(end, files.EndTxNum())
	}
	var candidates []uint64
	for _, files := range lists {
		for _, f := range files {
			if f.endTxNum <= end && f.endTxNum%stepSize == 0 {
				candidates = append(candidates, f.endTxNum)
			}
		}
	}
	slices.Sort(candidates)
	end = 0
	for _, c := range slices.Backward(candidates) {
		if !slices.ContainsFunc(lists, func(files visibleFiles) bool {
			return slices.ContainsFunc(files, func(f visibleFile) bool { return f.startTxNum < c && c < f.endTxNum })
		}) {
			end = c
			break
		}
	}
	for _, files := range lists {
		for i := 1; i < len(files) && files[i].startTxNum < end; i++ {
			if prev := files[i-1].endTxNum; files[i].startTxNum != prev {
				return 0, fmt.Errorf("%s: no visible file for txNum %d-%d", files[i].Fullpath(), prev, files[i].startTxNum)
			}
		}
	}
	if com := at.d[kv.CommitmentDomain]; com != nil && com.d.Enabled {
		for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
			if dt := at.d[d]; dt != nil && dt.d.Enabled && !slices.Equal(fileRangesUpTo(dt.files, end), fileRangesUpTo(com.files, end)) {
				return 0, fmt.Errorf("%s and commitment files differ below txNum %d", d, end)
			}
		}
	}
	return end, nil
}

func fileRangesUpTo(files visibleFiles, end uint64) (ranges [][2]uint64) {
	for _, f := range files {
		if f.endTxNum <= end {
			ranges = append(ranges, [2]uint64{f.startTxNum, f.endTxNum})
		}
	}
	return ranges
}

func stepAlignedMergeRange(files visibleFiles, stepSize, end uint64) MergeRange {
	var from uint64
	n := 0
	for _, f := range files {
		if f.endTxNum > end {
			break
		}
		if n == 0 {
			from = f.startTxNum
		}
		n++
		if f.endTxNum%stepSize == 0 {
			if n > 1 {
				return MergeRange{needMerge: true, from: from, to: f.endTxNum}
			}
			n = 0
		}
	}
	return MergeRange{}
}

func (at *AggregatorRoTx) stepAlignedMergeRanges(stepSize, end uint64) (*Ranges, error) {
	r := &Ranges{}
	for id, d := range at.d {
		if d == nil || !d.d.Enabled {
			continue
		}
		r.domain[id] = DomainRanges{name: d.name, aggStep: d.stepSize, values: stepAlignedMergeRange(d.files, stepSize, end)}
		if r.domain[id].values.needMerge {
			continue
		}
		history := stepAlignedMergeRange(d.ht.files, stepSize, end)
		index := stepAlignedMergeRange(d.ht.iit.files, stepSize, end)
		if history.needMerge && !(index.needMerge && history.Equal(&index)) {
			return nil, fmt.Errorf("%s history merge %d-%d has no index merge over the same range", d.name, history.from, history.to)
		}
		r.domain[id].history = HistoryRanges{history: history, index: index}
	}
	if r.anyDomainValues() {
		return r, nil
	}
	for id, ii := range at.iis {
		if ii == nil || !ii.ii.Enabled {
			continue
		}
		mr := stepAlignedMergeRange(ii.files, stepSize, end)
		mr.name = ii.name.String()
		r.invertedIndex[id] = &mr
	}
	return r, nil
}
