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

func (a *Aggregator) StepAlignedEnd(stepSize uint64) (end, stoppedEnd uint64, err error) {
	if a.CommitmentDomainStopped(kv.CommitmentDomain) {
		return 0, 0, fmt.Errorf("%s domain is stopped, its files cannot be merged apart from accounts and storage", kv.CommitmentDomain)
	}
	at := a.BeginFilesRo()
	defer at.Close()
	return at.stepAlignedEnd(stepSize, a.stoppedCommitmentDomains())
}

func (a *Aggregator) stoppedCommitmentDomains() (stopped []kv.Domain) {
	for _, d := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		if a.CommitmentDomainStopped(d) {
			stopped = append(stopped, d)
		}
	}
	return stopped
}

func (a *Aggregator) AlignFilesToStepSize(ctx context.Context, stepSize uint64) (end, stoppedEnd uint64, err error) {
	for _, d := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		if a.isDomainFrozen(d) {
			return 0, 0, fmt.Errorf("%s domain is frozen, its files cannot be merged", d)
		}
	}
	stopped := a.stoppedCommitmentDomains()
	end, stoppedEnd, err = a.StepAlignedEnd(stepSize)
	if err != nil {
		return 0, 0, err
	}
	for _, d := range stopped {
		a.stoppedCommitment[d].Store(false)
		defer a.StopCommitmentDomain(d)
	}
	for {
		merged, err := a.mergeStepAligned(ctx, stepSize, end, stoppedEnd, stopped)
		if err != nil {
			return 0, 0, err
		}
		if !merged {
			return end, stoppedEnd, nil
		}
	}
}

func (a *Aggregator) mergeStepAligned(ctx context.Context, stepSize, end, stoppedEnd uint64, stopped []kv.Domain) (bool, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	r, err := at.stepAlignedMergeRanges(stepSize, end, stoppedEnd, stopped)
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

func (at *AggregatorRoTx) stepAlignedFileLists(stopped []kv.Domain, ofStopped bool) []visibleFiles {
	var lists []visibleFiles
	for id, d := range at.d {
		if d == nil || !d.d.Enabled || slices.Contains(stopped, kv.Domain(id)) != ofStopped {
			continue
		}
		lists = append(lists, d.files, d.ht.files, d.ht.iit.files)
	}
	if ofStopped {
		return lists
	}
	for _, ii := range at.iis {
		if ii == nil || !ii.ii.Enabled {
			continue
		}
		lists = append(lists, ii.files)
	}
	return lists
}

func (at *AggregatorRoTx) stepAlignedEnd(stepSize uint64, stopped []kv.Domain) (end, stoppedEnd uint64, err error) {
	if end, err = stepAlignedListsEnd(at.stepAlignedFileLists(stopped, false), stepSize, math.MaxUint64); err != nil {
		return 0, 0, err
	}
	if com := at.d[kv.CommitmentDomain]; com != nil && com.d.Enabled {
		for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
			if dt := at.d[d]; dt != nil && dt.d.Enabled && !slices.Equal(fileRangesUpTo(dt.files, end), fileRangesUpTo(com.files, end)) {
				return 0, 0, fmt.Errorf("%s and commitment files differ below txNum %d", d, end)
			}
		}
	}
	if len(stopped) == 0 {
		return end, end, nil
	}
	if stoppedEnd, err = stepAlignedListsEnd(at.stepAlignedFileLists(stopped, true), stepSize, end); err != nil {
		return 0, 0, err
	}
	return end, stoppedEnd, nil
}

func stepAlignedListsEnd(lists []visibleFiles, stepSize, limit uint64) (uint64, error) {
	end := limit
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

func (at *AggregatorRoTx) stepAlignedMergeRanges(stepSize, end, stoppedEnd uint64, stopped []kv.Domain) (*Ranges, error) {
	r := &Ranges{}
	for id, d := range at.d {
		if d == nil || !d.d.Enabled {
			continue
		}
		domainEnd := end
		if slices.Contains(stopped, kv.Domain(id)) {
			domainEnd = stoppedEnd
		}
		r.domain[id] = DomainRanges{name: d.name, aggStep: d.stepSize, values: stepAlignedMergeRange(d.files, stepSize, domainEnd)}
		if r.domain[id].values.needMerge {
			continue
		}
		history := stepAlignedMergeRange(d.ht.files, stepSize, domainEnd)
		index := stepAlignedMergeRange(d.ht.iit.files, stepSize, domainEnd)
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
