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
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/c2h5oh/datasize"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/db/kv"
)

func (a *Aggregator) StepAlignedEnd(stepSize uint64) (uint64, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	return at.stepAlignedEnd(stepSize)
}

func (a *Aggregator) AlignFilesToStepSize(ctx context.Context, stepSize uint64) (uint64, error) {
	end, err := a.StepAlignedEnd(stepSize)
	if err != nil {
		return 0, err
	}
	a.PresetOfflineMerge()
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
	merges, err := at.stepAlignedMerges(stepSize, end)
	at.Close()
	if err != nil {
		return false, err
	}
	if len(merges) == 0 {
		a.cleanAfterMerge(nil)
		return false, nil
	}
	slices.SortStableFunc(merges, func(x, y stepAlignedMerge) int { return cmp.Compare(y.size, x.size) })
	var input int64
	for _, m := range merges {
		input += m.size
	}
	compressWorkers := max(1, a.d[kv.AccountsDomain].CompressCfg.Workers)
	limit := max(1, estimate.CompressSnapshot.WorkersByRAMOnly()/(compressWorkers*max(1, a.workers.getMerge())))
	a.logger.Info("[snapshots] step alignment", "merges", len(merges), "input", datasize.ByteSize(input).HR(), "concurrent", limit, "compress_workers", compressWorkers)
	var g errgroup.Group
	g.SetLimit(limit)
	for _, m := range merges {
		g.Go(func() error {
			in, mergeErr := a.mergeStepAlignedRange(ctx, m)
			if mergeErr != nil {
				return mergeErr
			}
			a.integrateMergedDirtyFiles(in)
			return nil
		})
	}
	err = g.Wait()
	return err == nil, err
}

func (a *Aggregator) mergeStepAlignedRange(ctx context.Context, m stepAlignedMerge) (*MergeResult, error) {
	start := time.Now()
	at := a.BeginFilesRo()
	defer at.Close()
	outs, err := at.filesInRange(m.r)
	if err != nil {
		return nil, err
	}
	in, err := at.mergeFiles(ctx, outs, m.r)
	if err != nil {
		return in, err
	}
	a.logger.Info("[snapshots] step alignment merged", "files", m.r.String(), "size", datasize.ByteSize(m.size).HR(), "took", time.Since(start))
	return in, nil
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

func stepAlignedMergeRanges(files visibleFiles, stepSize, end uint64) (ranges []MergeRange) {
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
				ranges = append(ranges, MergeRange{needMerge: true, from: from, to: f.endTxNum})
			}
			n = 0
		}
	}
	return ranges
}

func filesSizeIn(files visibleFiles, mr MergeRange) (size int64) {
	for _, f := range files {
		if mr.from <= f.startTxNum && f.endTxNum <= mr.to {
			size += f.src.decompressor.Size()
		}
	}
	return size
}

type stepAlignedMerge struct {
	size int64
	r    *Ranges
}

func (at *AggregatorRoTx) stepAlignedMerges(stepSize, end uint64) ([]stepAlignedMerge, error) {
	var merges []stepAlignedMerge
	add := func(size int64, set func(r *Ranges)) {
		r := &Ranges{}
		for id, d := range at.d {
			if d != nil {
				r.domain[id] = DomainRanges{name: d.name, aggStep: d.stepSize}
			}
		}
		set(r)
		merges = append(merges, stepAlignedMerge{size: size, r: r})
	}
	coupled := make(map[[2]uint64]int)
	for id, d := range at.d {
		if d == nil || !d.d.Enabled {
			continue
		}
		for _, mr := range stepAlignedMergeRanges(d.files, stepSize, end) {
			size, set := filesSizeIn(d.files, mr), func(r *Ranges) { r.domain[id].values = mr }
			if !slices.Contains([]kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CommitmentDomain}, kv.Domain(id)) {
				add(size, set)
				continue
			}
			key := [2]uint64{mr.from, mr.to}
			if i, ok := coupled[key]; ok {
				set(merges[i].r)
				merges[i].size += size
				continue
			}
			coupled[key] = len(merges)
			add(size, set)
		}
		history := stepAlignedMergeRanges(d.ht.files, stepSize, end)
		for _, index := range stepAlignedMergeRanges(d.ht.iit.files, stepSize, end) {
			hr := HistoryRanges{index: index}
			if i := slices.IndexFunc(history, func(h MergeRange) bool { return h.Equal(&index) }); i >= 0 {
				hr.history = index
				history = slices.Delete(history, i, i+1)
			}
			add(filesSizeIn(d.ht.files, index)+filesSizeIn(d.ht.iit.files, index), func(r *Ranges) { r.domain[id].history = hr })
		}
		if len(history) > 0 {
			return nil, fmt.Errorf("%s history merge %d-%d has no index merge over the same range", d.name, history[0].from, history[0].to)
		}
	}
	for id, ii := range at.iis {
		if ii == nil || !ii.ii.Enabled {
			continue
		}
		for _, mr := range stepAlignedMergeRanges(ii.files, stepSize, end) {
			mr.name = ii.name.String()
			add(filesSizeIn(ii.files, mr), func(r *Ranges) { r.invertedIndex[id] = &mr })
		}
	}
	return merges, nil
}
