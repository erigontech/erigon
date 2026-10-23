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
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type PBinRangeWriter struct {
	aggregator   *Aggregator
	domain       kv.Domain
	endTxNum     uint64
	leafStamp    uint64
	ranges       []pbinRange
	maxOps       int
	maxBytes     int
	workers      int
	state        []byte
	stateInFiles bool
	workerViews  atomic.Int32
	stampTracker func() pbinStampTracker
	pipelineHook func(string, int) error
}

type PBinRangeWriterLimits struct {
	MaxOps              int
	MaxBytes            int
	NoRangePastFrontier bool
	Workers             int
}

const (
	pbinRangeWriterMaxOps   = 100_000
	pbinRangeWriterMaxBytes = 64 << 20
)

type pbinRange struct {
	start uint64
	end   uint64
	rows  *pbinRowLanes
}

type pbinStampSplit struct {
	bitLen int16
	stamp  uint64
}

type pbinStampTracker interface {
	observe(key []byte, stamp uint64) error
	advance(lookahead [][]byte) error
	clearClosed()
	stamp(key []byte) (uint64, error)
}

const (
	pbinPartitionBits = 12
	pbinBucketBits    = 264
)

type pbinRowStampTracker struct {
	partitions map[pbinPartition]*pbinPartitionStamps
	upper      map[string]uint64
	maximum    uint64
}

type pbinPartitionStamps struct {
	open     []pbinStampSplit
	closed   map[string]uint64
	previous eip8297.Bitpath
	last     uint64
}

func newPBinRowStampTracker() *pbinRowStampTracker {
	return &pbinRowStampTracker{partitions: make(map[pbinPartition]*pbinPartitionStamps), upper: make(map[string]uint64)}
}

func (t *pbinRowStampTracker) clone() pbinStampTracker {
	cloned := &pbinRowStampTracker{
		partitions: make(map[pbinPartition]*pbinPartitionStamps, len(t.partitions)),
		upper:      maps.Clone(t.upper),
		maximum:    t.maximum,
	}
	for key, stamps := range t.partitions {
		partition := &pbinPartitionStamps{
			open:     append([]pbinStampSplit(nil), stamps.open...),
			closed:   maps.Clone(stamps.closed),
			previous: stamps.previous,
			last:     stamps.last,
		}
		cloned.partitions[key] = partition
	}
	return cloned
}

type pbinRangeWriterWrite struct {
	data []byte
	prev []byte
}

type pbinRangeWriterDetached struct {
	key  []byte
	data []byte
	prev []byte
}

type pbinRangeWriterOverlay struct {
	commitment.PatriciaContext
	writes   map[string]pbinRangeWriterWrite
	views    *atomic.Int32
	release  func([]byte)
	write    func([]byte, []byte, []byte) error
	finished func() error
}

type pbinStampTrackerClone interface {
	clone() pbinStampTracker
}

type pbinFoldBatch struct {
	batch     []pbt.Op
	stamps    []uint64
	lookahead [][]byte
	final     bool
	index     int
	tracker   pbinStampTracker
	rows      []pbinRangeWriterDetached
}

type pbinFoldPipeline struct {
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	submit      chan *pbinFoldBatch
	detached    chan *pbinFoldBatchResult
	completed   chan int
	tracker     pbinStampTracker
	compute     func(context.Context, *pbinFoldBatch) error
	process     func(*pbinFoldBatchResult) error
	hook        func(string, int) error
	stopOnce    sync.Once
	failureLock sync.Mutex
	failure     pbinFoldFailure
	wg          sync.WaitGroup
}

type pbinFoldBatchResult struct {
	batch *pbinFoldBatch
	rows  []pbinRangeWriterDetached
}

type pbinFoldFailure struct {
	index int
	err   error
}

func newPBinFoldPipeline(ctx context.Context, tracker pbinStampTracker, compute func(context.Context, *pbinFoldBatch) error, process func(*pbinFoldBatchResult) error, hook func(string, int) error) (*pbinFoldPipeline, bool) {
	clone, ok := tracker.(pbinStampTrackerClone)
	if !ok {
		return nil, false
	}
	pipelineCtx, cancel := context.WithCancel(ctx)
	p := &pbinFoldPipeline{
		ctx:       pipelineCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
		submit:    make(chan *pbinFoldBatch, 1),
		detached:  make(chan *pbinFoldBatchResult, 1),
		completed: make(chan int, 2),
		tracker:   clone.clone(),
		compute:   compute,
		process:   process,
		hook:      hook,
	}
	p.wg.Add(2)
	go p.computeLoop()
	go p.processLoop()
	return p, true
}

func (p *pbinFoldPipeline) stop() {
	p.stopOnce.Do(func() {
		p.cancel()
		close(p.done)
	})
}

func (p *pbinFoldPipeline) fail(index int, err error) {
	if err == nil {
		return
	}
	p.failureLock.Lock()
	if p.failure.err == nil || index < p.failure.index {
		p.failure = pbinFoldFailure{index: index, err: err}
	}
	p.failureLock.Unlock()
	p.stop()
}

func (p *pbinFoldPipeline) err() error {
	p.failureLock.Lock()
	defer p.failureLock.Unlock()
	if p.failure.err != nil {
		return p.failure.err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *pbinFoldPipeline) waitCompleted(index int) error {
	select {
	case completed := <-p.completed:
		if completed != index {
			return fmt.Errorf("pbin fold pipeline: completed batch %d, want %d", completed, index)
		}
		return p.err()
	case <-p.done:
		return p.err()
	}
}

func (p *pbinFoldPipeline) submitBatch(batch []pbt.Op, stamps []uint64, lookahead [][]byte, final bool, index int) error {
	if index >= 2 {
		if err := p.waitCompleted(index - 2); err != nil {
			return err
		}
		p.tracker.clearClosed()
	}
	for i := range batch {
		if err := p.tracker.observe(batch[i].Key, stamps[i]); err != nil {
			p.fail(index, err)
			return err
		}
	}
	if err := p.tracker.advance(lookahead); err != nil {
		p.fail(index, err)
		return err
	}
	if p.hook != nil {
		if err := p.hook("stage1", index); err != nil {
			p.fail(index, err)
			return err
		}
	}
	cloned, ok := p.tracker.(pbinStampTrackerClone)
	if !ok {
		err := fmt.Errorf("pbin fold pipeline: tracker cannot be cloned")
		p.fail(index, err)
		return err
	}
	queued := &pbinFoldBatch{
		batch:     clonePBinOps(batch),
		stamps:    append([]uint64(nil), stamps...),
		lookahead: clonePBinKeys(lookahead),
		final:     final,
		index:     index,
		tracker:   cloned.clone(),
	}
	p.tracker.clearClosed()
	select {
	case p.submit <- queued:
		return nil
	case <-p.done:
		return p.err()
	}
}

func (p *pbinFoldPipeline) computeLoop() {
	defer p.wg.Done()
	defer close(p.detached)
	for {
		select {
		case batch, ok := <-p.submit:
			if !ok {
				return
			}
			if p.hook != nil {
				if err := p.hook("stage2", batch.index); err != nil {
					p.fail(batch.index, err)
					return
				}
			}
			if err := p.compute(p.ctx, batch); err != nil {
				p.fail(batch.index, err)
				return
			}
			result := &pbinFoldBatchResult{batch: batch, rows: batch.rows}
			select {
			case p.detached <- result:
			case <-p.done:
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *pbinFoldPipeline) processLoop() {
	defer p.wg.Done()
	for {
		select {
		case result, ok := <-p.detached:
			if !ok {
				return
			}
			if p.hook != nil {
				if err := p.hook("stage3", result.batch.index); err != nil {
					p.fail(result.batch.index, err)
					return
				}
			}
			sort.Slice(result.rows, func(i, j int) bool { return bytes.Compare(result.rows[i].key, result.rows[j].key) < 0 })
			if processErr := p.process(result); processErr != nil {
				p.fail(result.batch.index, processErr)
				return
			}
			result.batch.tracker.clearClosed()
			select {
			case p.completed <- result.batch.index:
			case <-p.done:
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *pbinFoldPipeline) close() error {
	close(p.submit)
	p.wg.Wait()
	return p.err()
}

func (o *pbinRangeWriterOverlay) Absent(key []byte) bool {
	_, held := o.writes[string(key)]
	return !held
}

func (o *pbinRangeWriterOverlay) DecorateWorkerContext(worker commitment.PatriciaContext) commitment.PatriciaContext {
	o.views.Add(1)
	return &pbinRangeWriterView{PatriciaContext: worker, overlay: o}
}

type pbinRangeWriterView struct {
	commitment.PatriciaContext
	overlay *pbinRangeWriterOverlay
}

func (v *pbinRangeWriterView) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if write, ok := v.overlay.writes[string(prefix)]; ok {
		return bytes.Clone(write.data), 0, nil
	}
	return v.PatriciaContext.Branch(prefix)
}

func (v *pbinRangeWriterView) PutBranch(prefix, _, _ []byte) error {
	return fmt.Errorf("pbin range writer: worker write of row %x during a round", prefix)
}

func (v *pbinRangeWriterView) Absent(key []byte) bool {
	return v.overlay.Absent(key)
}

func (o *pbinRangeWriterOverlay) FlushFinished(lookahead [][]byte) error {
	rows, err := o.detach(pbinRangeWriterSkip(lookahead))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := o.write(row.key, row.data, row.prev); err != nil {
			return err
		}
	}
	return o.finished()
}

func (o *pbinRangeWriterOverlay) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if write, ok := o.writes[string(prefix)]; ok {
		return bytes.Clone(write.data), 0, nil
	}
	return o.PatriciaContext.Branch(prefix)
}

func (o *pbinRangeWriterOverlay) PutBranch(prefix, data, prevData []byte) error {
	key := string(prefix)
	write := o.writes[key]
	if write.data == nil {
		write.prev = bytes.Clone(prevData)
	}
	write.data = bytes.Clone(data)
	o.writes[key] = write
	return nil
}

func (o *pbinRangeWriterOverlay) Flush() error {
	rows, err := o.detach(nil)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := o.write(row.key, row.data, row.prev); err != nil {
			return err
		}
	}
	return o.finished()
}

func pbinRangeWriterSkip(lookahead [][]byte) func([]byte) (bool, error) {
	nextPaths := make([]eip8297.Bitpath, len(lookahead))
	for i, key := range lookahead {
		nextPaths[i] = eip8297.PathFromBits(key, int16(len(key)*8))
	}
	return func(key []byte) (bool, error) {
		if bytes.Equal(key, pbt.GlobalRootKey()) {
			return true, nil
		}
		path, err := eip8297.DecodeBitPath(key)
		if err != nil {
			return false, err
		}
		for i := range nextPaths {
			if path.BitLen <= nextPaths[i].BitLen && eip8297.CommonPrefixBitsAt(&path, 0, &nextPaths[i]) == path.BitLen {
				return true, nil
			}
		}
		return false, nil
	}
}

func (o *pbinRangeWriterOverlay) detach(skip func([]byte) (bool, error)) ([]pbinRangeWriterDetached, error) {
	keys := make([]string, 0, len(o.writes))
	for key := range o.writes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]pbinRangeWriterDetached, 0, len(keys))
	for _, key := range keys {
		if skip != nil {
			skipped, err := skip([]byte(key))
			if err != nil {
				return nil, err
			}
			if skipped {
				continue
			}
		}
		write := o.writes[key]
		if err := o.PatriciaContext.PutBranch([]byte(key), write.data, write.prev); err != nil {
			return nil, err
		}
		rows = append(rows, pbinRangeWriterDetached{key: bytes.Clone([]byte(key)), data: bytes.Clone(write.data), prev: bytes.Clone(write.prev)})
		o.release([]byte(key))
		delete(o.writes, key)
	}
	return rows, nil
}

func NewPBinRangeWriter(aggregator *Aggregator, domain kv.Domain, endTxNum uint64) (*PBinRangeWriter, error) {
	return newPBinRangeWriter(aggregator, domain, endTxNum, PBinRangeWriterLimits{MaxOps: pbinRangeWriterMaxOps, MaxBytes: pbinRangeWriterMaxBytes})
}

func NewPBinRangeWriterWithinFiles(aggregator *Aggregator, domain kv.Domain, endTxNum uint64) (*PBinRangeWriter, error) {
	return newPBinRangeWriter(aggregator, domain, endTxNum, PBinRangeWriterLimits{MaxOps: pbinRangeWriterMaxOps, MaxBytes: pbinRangeWriterMaxBytes, NoRangePastFrontier: true})
}

func newPBinRangeWriter(aggregator *Aggregator, domain kv.Domain, endTxNum uint64, limits PBinRangeWriterLimits) (*PBinRangeWriter, error) {
	if aggregator == nil {
		return nil, fmt.Errorf("pbin range writer: nil aggregator")
	}
	if domain != kv.CommitmentDomain && domain != kv.CommitmentBinDomain {
		return nil, fmt.Errorf("pbin range writer: invalid domain %s", domain)
	}
	if aggregator.d[domain] == nil || !aggregator.d[domain].Enabled {
		return nil, fmt.Errorf("pbin range writer: domain %s is unavailable", domain)
	}
	workers := limits.Workers
	if workers <= 0 {
		workers = dbg.PBTWorkers
	}
	workers = max(workers, 1)
	at := aggregator.BeginFilesRo()
	defer at.Close()
	files := pbinAccountFiles(at.Files(kv.AccountsDomain))
	if len(files) == 0 {
		if endTxNum == 0 {
			return &PBinRangeWriter{aggregator: aggregator, domain: domain, maxOps: limits.MaxOps, maxBytes: limits.MaxBytes, workers: workers}, nil
		}
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("0-%d", endTxNum+1))
		if err != nil {
			return nil, err
		}
		return &PBinRangeWriter{
			aggregator:   aggregator,
			domain:       domain,
			endTxNum:     endTxNum,
			leafStamp:    endTxNum,
			maxOps:       limits.MaxOps,
			maxBytes:     limits.MaxBytes,
			workers:      workers,
			stateInFiles: !limits.NoRangePastFrontier,
			ranges: []pbinRange{{
				start: 0,
				end:   endTxNum + 1,
				rows:  rows,
			}},
		}, nil
	}
	if endTxNum == 0 {
		endTxNum = files.EndRootNum() - 1
	}
	ranges := make([]pbinRange, 0, len(files))
	for _, file := range files {
		start, end := file.StartRootNum(), file.EndRootNum()
		if start > endTxNum {
			break
		}
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", start, end))
		if err != nil {
			for i := range ranges {
				ranges[i].rows.close()
			}
			return nil, err
		}
		ranges = append(ranges, pbinRange{
			start: start,
			end:   end,
			rows:  rows,
		})
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("pbin range writer: no account range contains %d", endTxNum)
	}
	leafStamp := min(ranges[len(ranges)-1].end-1, endTxNum)
	stateInFiles := !limits.NoRangePastFrontier || endTxNum < ranges[len(ranges)-1].end
	if ranges[len(ranges)-1].end <= endTxNum && !limits.NoRangePastFrontier {
		if endTxNum == ^uint64(0) {
			return nil, fmt.Errorf("pbin range writer: end txNum is too large")
		}
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", ranges[len(ranges)-1].end, endTxNum+1))
		if err != nil {
			for i := range ranges {
				ranges[i].rows.close()
			}
			return nil, err
		}
		ranges = append(ranges, pbinRange{
			start: ranges[len(ranges)-1].end,
			end:   endTxNum + 1,
			rows:  rows,
		})
	}
	return &PBinRangeWriter{aggregator: aggregator, domain: domain, endTxNum: endTxNum, leafStamp: leafStamp, ranges: ranges, maxOps: limits.MaxOps, maxBytes: limits.MaxBytes, workers: workers, stateInFiles: stateInFiles}, nil
}

func (w *PBinRangeWriter) PBinLeafStamp() uint64 {
	return w.leafStamp
}

func (w *PBinRangeWriter) PBinCommitmentState() []byte {
	return bytes.Clone(w.state)
}

func (w *PBinRangeWriter) PBinCommitmentStateInFiles() bool {
	return w.stateInFiles
}

func pbinAccountFiles(files kv.VisibleFiles) kv.VisibleFiles {
	accountFiles := make(kv.VisibleFiles, 0, len(files))
	for _, file := range files {
		if strings.HasSuffix(file.Fullpath(), ".kv") {
			accountFiles = append(accountFiles, file)
		}
	}
	return accountFiles
}

func (w *PBinRangeWriter) WriteAtBlock(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, leaves func(func(PBinLeaf) error) error, blockNum uint64) (common.Hash, error) {
	if w == nil || w.aggregator == nil {
		return common.Hash{}, fmt.Errorf("pbin range writer: nil writer")
	}
	if tx == nil || domains == nil || leaves == nil {
		return common.Hash{}, fmt.Errorf("pbin range writer: missing input")
	}
	if domains.GetCommitmentCtx().CommitmentDomain() != w.domain {
		return common.Hash{}, fmt.Errorf("pbin range writer: shared domains use %s, want %s", domains.GetCommitmentCtx().CommitmentDomain(), w.domain)
	}
	defer w.closeRanges()
	fresh, err := w.freshDomain(tx)
	if err != nil {
		return common.Hash{}, err
	}
	if !fresh {
		return common.Hash{}, fmt.Errorf("pbin range writer: target domain %s already holds rows; the range writer builds only into an empty domain", w.domain)
	}
	var tracker pbinStampTracker = newPBinRowStampTracker()
	if w.stampTracker != nil {
		tracker = w.stampTracker()
	}
	var (
		overlay  *pbinRangeWriterOverlay
		pipeline *pbinFoldPipeline
		root     []byte
		seen     bool
		state    []byte
	)
	foldProgress := NewPBTProgress(PBTProgressConfig{Message: "PBT fold progress", Phase: "fold batches", TempDir: w.aggregator.Dirs().Tmp})
	defer foldProgress.Done()
	onRow := func(rowTracker pbinStampTracker, key, data []byte) error {
		if commitment.IsCommitmentStateKey(key) {
			state = bytes.Clone(data)
			return nil
		}
		stamp, stampErr := rowTracker.stamp(key)
		if stampErr != nil {
			return stampErr
		}
		rangeIndex, rangeErr := w.rangeForStamp(stamp)
		if rangeErr != nil {
			return rangeErr
		}
		route, collectErr := w.ranges[rangeIndex].rows.collect(key, data)
		if collectErr != nil {
			return collectErr
		}
		if w.pipelineHook != nil {
			return w.pipelineHook(route, 0)
		}
		return nil
	}
	serialVisit := func(batch []pbt.Op, stamps []uint64, lookahead [][]byte, final bool) error {
		for i, op := range batch {
			if observeErr := tracker.observe(op.Key, stamps[i]); observeErr != nil {
				return observeErr
			}
		}
		if advanceErr := tracker.advance(lookahead); advanceErr != nil {
			return advanceErr
		}
		domains.GetCommitmentCtx().SetPBinOps(batch)
		var computeErr error
		root, computeErr = domains.GetCommitmentCtx().ComputeCommitmentWithDiff(ctx, tx, final, blockNum, w.endTxNum, "pbin-range-writer", nil, nil, nil, func(inner commitment.PatriciaContext) commitment.PatriciaContext {
			overlay.PatriciaContext = inner
			return overlay
		})
		if computeErr != nil {
			return computeErr
		}
		if final {
			return overlay.Flush()
		}
		return overlay.FlushFinished(lookahead)
	}
	overlay = &pbinRangeWriterOverlay{
		writes: make(map[string]pbinRangeWriterWrite),
		views:  &w.workerViews,
		write: func(key, data, _ []byte) error {
			return onRow(tracker, key, data)
		},
		finished: func() error {
			tracker.clearClosed()
			return nil
		},
		release: func(key []byte) {
			domains.GetMemBatch().(*TemporalMemBatch).ForgetLatest(w.domain, key)
		},
	}
	pipeline, _ = newPBinFoldPipeline(ctx, tracker,
		func(pipelineCtx context.Context, batch *pbinFoldBatch) error {
			domains.GetCommitmentCtx().SetPBinOps(batch.batch)
			var computeErr error
			root, computeErr = domains.GetCommitmentCtx().ComputeCommitmentWithDiff(pipelineCtx, tx, batch.final, blockNum, w.endTxNum, "pbin-range-writer", nil, nil, nil, func(inner commitment.PatriciaContext) commitment.PatriciaContext {
				overlay.PatriciaContext = inner
				return overlay
			})
			if computeErr != nil {
				return computeErr
			}
			if batch.final {
				batch.rows, computeErr = overlay.detach(nil)
			} else {
				batch.rows, computeErr = overlay.detach(pbinRangeWriterSkip(batch.lookahead))
			}
			return computeErr
		},
		func(result *pbinFoldBatchResult) error {
			for _, row := range result.rows {
				if rowErr := onRow(result.batch.tracker, row.key, row.data); rowErr != nil {
					return rowErr
				}
			}
			return nil
		}, w.pipelineHook)
	batchIndex := 0
	visit := func(batch []pbt.Op, stamps []uint64, lookahead [][]byte, final bool) error {
		foldProgress.Add(1)
		seen = true
		if pipeline == nil {
			return serialVisit(batch, stamps, lookahead, final)
		}
		index := batchIndex
		batchIndex++
		return pipeline.submitBatch(batch, stamps, lookahead, final, index)
	}
	stream := func(emit func(pbt.Op, uint64) error) error {
		progress := NewPBTProgress(PBTProgressConfig{Message: "PBT range writer progress", Phase: "range leaf stream", TempDir: w.aggregator.Dirs().Tmp})
		var progressDone, progressReported uint64
		defer func() {
			if progressDone != progressReported {
				progress.Add(progressDone - progressReported)
			}
			progress.Done()
		}()
		return leaves(func(leaf PBinLeaf) error {
			progressDone++
			if progressDone-progressReported >= 4096 {
				progress.Add(progressDone - progressReported)
				progressReported = progressDone
			}
			if len(leaf.Value) != eip8297.ValueLength {
				return fmt.Errorf("pbin range writer: leaf %x has value length %d", leaf.Key, len(leaf.Value))
			}
			var value [eip8297.ValueLength]byte
			copy(value[:], leaf.Value)
			return emit(pbt.Op{Key: bytes.Clone(leaf.Key), Value: value}, leaf.Stamp)
		})
	}
	streamErr := pbinForEachRebuildOpStream(w.aggregator.Dirs().Tmp, w.maxOps, w.maxBytes, visit, stream)
	if streamErr != nil && pipeline != nil {
		pipeline.stop()
	}
	var pipelineErr error
	if pipeline != nil {
		pipelineErr = pipeline.close()
	}
	if streamErr != nil {
		return common.Hash{}, streamErr
	}
	if pipelineErr != nil {
		return common.Hash{}, pipelineErr
	}
	if !seen {
		root = append([]byte(nil), eip8297.EmptyTreeHash[:]...)
	}
	if len(state) == 0 {
		trie, ok := domains.GetCommitmentCtx().Trie().(commitment.StatefulTrie)
		if !ok {
			return common.Hash{}, fmt.Errorf("pbin range writer: trie does not support state encoding")
		}
		trieState, encodeErr := trie.EncodeCurrentState(nil)
		if encodeErr != nil {
			return common.Hash{}, encodeErr
		}
		state, err = commitmentdb.NewCommitmentState(w.endTxNum, blockNum, trieState).Encode()
		if err != nil {
			return common.Hash{}, err
		}
	}
	w.state = bytes.Clone(state)
	if err := w.buildFiles(ctx, state); err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(root), nil
}

func (w *PBinRangeWriter) freshDomain(tx kv.Tx) (bool, error) {
	at := w.aggregator.BeginFilesRo()
	defer at.Close()
	inFiles, err := at.DebugRangeLatestFromFiles(w.domain, nil, nil, 1)
	if err != nil {
		return false, err
	}
	defer inFiles.Close()
	if inFiles.HasNext() {
		return false, nil
	}
	firstKey, err := kv.FirstKey(tx, w.aggregator.d[w.domain].ValuesTable)
	if err != nil {
		return false, err
	}
	return len(firstKey) == 0, nil
}

func (w *PBinRangeWriter) rangeForStamp(stamp uint64) (int, error) {
	for i := range w.ranges {
		if stamp >= w.ranges[i].start && stamp < w.ranges[i].end {
			return i, nil
		}
	}
	return 0, fmt.Errorf("pbin range writer: stamp %d is outside accounts files", stamp)
}

func (w *PBinRangeWriter) closeRanges() {
	for i := range w.ranges {
		w.ranges[i].rows.close()
	}
}

const (
	pbinRangeBuildMemoryCap  uint64 = 4 * uint64(datasize.GB)
	pbinRangeBuildMemoryUnit uint64 = uint64(datasize.MB)
)

type pbinRangeBuildPlan struct {
	index           int
	stepFrom        kv.Step
	stepTo          kv.Step
	valuesPath      string
	idtPath         string
	kviPath         string
	rows            uint64
	budget          uint64
	accessorWorkers int
	static          StaticFiles
}

type pbinRangeBuildLimiter struct {
	mu       sync.Mutex
	wake     chan struct{}
	capacity uint64
	used     uint64
}

func newPBinRangeBuildLimiter(capacity uint64) *pbinRangeBuildLimiter {
	return &pbinRangeBuildLimiter{wake: make(chan struct{}), capacity: capacity}
}

func (l *pbinRangeBuildLimiter) acquire(ctx context.Context, weight uint64) error {
	for {
		l.mu.Lock()
		if l.used == 0 || l.used+weight <= l.capacity {
			l.used += weight
			l.mu.Unlock()
			return nil
		}
		wake := l.wake
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func (l *pbinRangeBuildLimiter) release(weight uint64) {
	l.mu.Lock()
	l.used -= weight
	close(l.wake)
	l.wake = make(chan struct{})
	l.mu.Unlock()
}

func pbinRangeBuildWorkers(rows []uint64, workers int) []int {
	result := make([]int, len(rows))
	if len(result) == 0 {
		return result
	}
	workers = max(workers, 1)
	for i := range result {
		result[i] = 1
	}
	if workers <= len(result) {
		return result
	}
	remaining := workers - len(result)
	var total uint64
	for _, rowCount := range rows {
		total += rowCount
	}
	if total == 0 {
		for i := 0; remaining > 0; i, remaining = i+1, remaining-1 {
			result[i%len(result)]++
		}
		return result
	}
	type fraction struct {
		index int
		value float64
	}
	fractions := make([]fraction, 0, len(rows))
	for i, rowCount := range rows {
		share := float64(remaining) * float64(rowCount) / float64(total)
		whole := int(share)
		result[i] += whole
		remaining -= whole
		fractions = append(fractions, fraction{index: i, value: share - float64(whole)})
	}
	sort.Slice(fractions, func(i, j int) bool { return fractions[i].value > fractions[j].value })
	for i := 0; remaining > 0; i, remaining = i+1, remaining-1 {
		result[fractions[i%len(fractions)].index]++
	}
	return result
}

func pbinRangeBuildBudget(rows uint64, compressorWorkers, accessorWorkers int) uint64 {
	compressor := uint64(max(compressorWorkers, 1)) * 64 * uint64(datasize.MB)
	recsplit := uint64(max(accessorWorkers, 1)) * 64 * uint64(datasize.MB)
	skip := max(uint64(datasize.MB), rows*512)
	return compressor + recsplit + skip
}

func (w *PBinRangeWriter) buildFiles(ctx context.Context, state []byte) error {
	defer w.closeRanges()
	domain := w.aggregator.d[w.domain]
	stepSize := w.aggregator.StepSize()
	rowCounts := make([]uint64, len(w.ranges))
	for i := range w.ranges {
		rowCounts[i] = w.ranges[i].rows.count()
	}
	accessorWorkers := pbinRangeBuildWorkers(rowCounts, w.workers)
	plans := make([]pbinRangeBuildPlan, len(w.ranges))
	for i := range plans {
		stepFrom := kv.Step(w.ranges[i].start / stepSize)
		stepTo := kv.Step(w.ranges[i].end / stepSize)
		valuesPath := domain.kvNewFilePath(stepFrom, stepTo)
		plans[i] = pbinRangeBuildPlan{
			index:           i,
			stepFrom:        stepFrom,
			stepTo:          stepTo,
			valuesPath:      valuesPath,
			idtPath:         filepath.Join(domain.dirs.Tmp, filepath.Base(valuesPath)+".idt"),
			kviPath:         domain.kviAccessorNewFilePath(stepFrom, stepTo),
			rows:            rowCounts[i],
			accessorWorkers: accessorWorkers[i],
		}
		plans[i].budget = pbinRangeBuildBudget(rowCounts[i], domain.CompressCfg.Workers, accessorWorkers[i])
	}
	buildProgress := NewPBTProgress(PBTProgressConfig{
		Message: "PBT range writer progress",
		Phase:   "range builds",
		Total:   uint64(len(plans)),
		TempDir: w.aggregator.Dirs().Tmp,
		Fields:  []any{"memory_cap_bytes", pbinRangeBuildMemoryCap, "ranges_in_flight", 0},
	})
	defer buildProgress.Done()
	limiter := newPBinRangeBuildLimiter(max(pbinRangeBuildMemoryCap/pbinRangeBuildMemoryUnit, uint64(1)))
	var inFlight atomic.Int32
	setBuildProgress := func() {
		buildProgress.SetFields("memory_cap_bytes", pbinRangeBuildMemoryCap, "ranges_in_flight", inFlight.Load())
		buildProgress.Add(0)
	}
	cleanup := func() {
		for i := range plans {
			plans[i].static.CleanupOnError()
			_ = dir.RemoveFile(plans[i].valuesPath)
			_ = dir.RemoveFile(plans[i].kviPath)
			_ = dir.RemoveFile(plans[i].idtPath)
		}
		w.closeRanges()
	}
	var firstRealError error
	var firstErrorMu sync.Mutex
	recordError := func(err error) {
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		firstErrorMu.Lock()
		if firstRealError == nil {
			firstRealError = err
		}
		firstErrorMu.Unlock()
	}
	g, buildCtx := errgroup.WithContext(ctx)
	for i := range plans {
		plan := &plans[i]
		weight := max(uint64(1), (plan.budget+pbinRangeBuildMemoryUnit-1)/pbinRangeBuildMemoryUnit)
		g.Go(func() error {
			if err := limiter.acquire(buildCtx, weight); err != nil {
				return err
			}
			inFlight.Add(1)
			setBuildProgress()
			defer func() {
				inFlight.Add(-1)
				limiter.release(weight)
				setBuildProgress()
			}()
			if w.pipelineHook != nil {
				if err := w.pipelineHook("range-build-start", plan.index); err != nil {
					recordError(err)
					return err
				}
			}
			err := w.buildRange(buildCtx, domain, state, plan)
			if err == nil && w.pipelineHook != nil {
				err = w.pipelineHook("range-build-done", plan.index)
			}
			if err == nil {
				buildProgress.Add(1)
			}
			if err != nil {
				recordError(err)
			}
			return err
		})
	}
	waitErr := g.Wait()
	firstErrorMu.Lock()
	realErr := firstRealError
	firstErrorMu.Unlock()
	if realErr != nil {
		cleanup()
		return realErr
	}
	if waitErr != nil {
		cleanup()
		return waitErr
	}
	w.aggregator.dirtyFilesLock.Lock()
	for i := range plans {
		domain.integrateDirtyFiles(plans[i].static, w.ranges[i].start, w.ranges[i].end)
	}
	w.aggregator.recalcVisibleFiles(nil)
	w.aggregator.dirtyFilesLock.Unlock()
	return nil
}

func (w *PBinRangeWriter) buildRange(buildCtx context.Context, domain *Domain, state []byte, plan *pbinRangeBuildPlan) error {
	valuesComp, err := seg.NewCompressor(buildCtx, domain.FilenameBase+".domain.convert", plan.valuesPath, domain.dirs.Tmp, domain.CompressCfg, log.LvlTrace, domain.logger)
	if err != nil {
		return err
	}
	collation := Collation{valuesComp: valuesComp, valuesPath: plan.valuesPath}
	writer := seg.NewWriter(valuesComp, seg.CompressNone)
	if w.stateInFiles && plan.index == len(w.ranges)-1 && len(state) != 0 {
		if _, collectErr := w.ranges[plan.index].rows.collect(commitment.KeyCommitmentState, state); collectErr != nil {
			valuesComp.Close()
			return collectErr
		}
	}
	rangeProgress := newPbtRangeProgress(w.aggregator.Dirs().Tmp, fmt.Sprintf("range %d-%d", plan.stepFrom, plan.stepTo))
	if w.pipelineHook != nil {
		rangeProgress.hook = func(stage string) error { return w.pipelineHook("range-"+stage, plan.index) }
	}
	rows := w.ranges[plan.index].rows
	if err = rows.finalize(); err != nil {
		valuesComp.Close()
		return err
	}
	rowProgress := rangeProgress.stage("rows", rows.count())
	rowProgress.SetFields("lane_bytes", rows.laneBytes(), "lane_buffer_bytes", pbinRowLaneBufferBytes, "skip_rows", rows.skipRows(), "skip_share", rows.skipShare())
	rowProgress.Add(rows.count())
	rowProgress.Done()
	kvProgress := rangeProgress.batch(".kv", rows.count())
	err = rows.load(buildCtx, func(key, value []byte) error {
		if _, writeErr := writer.Write(key); writeErr != nil {
			return writeErr
		}
		if _, valueErr := writer.Write(value); valueErr != nil {
			return valueErr
		}
		kvProgress.Add(1)
		return nil
	})
	kvProgress.Done()
	if err != nil {
		valuesComp.Close()
		return err
	}
	collation.valuesCount = valuesComp.Count() / 2
	static, err := domain.buildFileRangeWithWorkers(buildCtx, plan.stepFrom, plan.stepTo, collation, background.NewProgressSet(), "", rangeProgress, plan.accessorWorkers)
	if err != nil {
		return err
	}
	if err := rows.cleanup(); err != nil {
		static.CleanupOnError()
		return err
	}
	plan.static = static
	return nil
}

func (t *pbinRowStampTracker) partition(path *eip8297.Bitpath) *pbinPartitionStamps {
	key := pbinPartition{byte(path.Words[0] >> 56), byte(path.Words[0]>>52) & 0x0f}
	stamps := t.partitions[key]
	if stamps == nil {
		stamps = &pbinPartitionStamps{closed: make(map[string]uint64)}
		t.partitions[key] = stamps
	}
	return stamps
}

func (t *pbinRowStampTracker) observe(key []byte, stamp uint64) error {
	if len(key) == 0 {
		return fmt.Errorf("pbin range writer: empty leaf key")
	}
	path := eip8297.PathFromBits(key, int16(len(key)*8))
	if path.BitLen < pbinPartitionBits {
		return fmt.Errorf("pbin range writer: leaf key %x is shorter than a partition", key)
	}
	for bitLen := int16(4); bitLen < pbinPartitionBits; bitLen += 4 {
		prefix := path
		prefix.Truncate(bitLen)
		name := string(eip8297.EncodeBitPath(&prefix))
		t.upper[name] = max(t.upper[name], stamp)
	}
	t.maximum = max(t.maximum, stamp)
	t.partition(&path).observe(&path, stamp)
	return nil
}

func (p *pbinPartitionStamps) observe(path *eip8297.Bitpath, stamp uint64) {
	if len(p.open) == 0 {
		p.open = append(p.open, pbinStampSplit{bitLen: pbinPartitionBits})
	} else {
		window := eip8297.CommonPrefixBitsAt(&p.previous, 0, path) / 4 * 4
		p.closeDeeper(window)
		if top := &p.open[len(p.open)-1]; top.bitLen == window {
			top.stamp = max(top.stamp, p.last)
		} else {
			p.open = append(p.open, pbinStampSplit{bitLen: window, stamp: p.last})
		}
	}
	if byte(path.Words[0]>>56) == eip8297.StorageZone && p.open[len(p.open)-1].bitLen < pbinBucketBits {
		p.open = append(p.open, pbinStampSplit{bitLen: pbinBucketBits})
	}
	p.previous = *path
	p.last = stamp
}

func (t *pbinRowStampTracker) advance(lookahead [][]byte) error {
	live := make(map[*pbinPartitionStamps]struct{}, len(lookahead))
	for _, nextKey := range lookahead {
		if len(nextKey) == 0 {
			return fmt.Errorf("pbin range writer: missing lookahead key")
		}
		nextPath := eip8297.PathFromBits(nextKey, int16(len(nextKey)*8))
		stamps := t.partition(&nextPath)
		stamps.closeDeeper(eip8297.CommonPrefixBitsAt(&stamps.previous, 0, &nextPath))
		live[stamps] = struct{}{}
	}
	for _, stamps := range t.partitions {
		if _, ok := live[stamps]; !ok {
			stamps.closeDeeper(-1)
		}
	}
	return nil
}

func (t *pbinRowStampTracker) clearClosed() {
	for _, stamps := range t.partitions {
		clear(stamps.closed)
	}
}

func (p *pbinPartitionStamps) closeDeeper(bitLen int16) {
	carry := p.last
	for len(p.open) > 0 && (bitLen < 0 || p.open[len(p.open)-1].bitLen > bitLen) {
		split := p.open[len(p.open)-1]
		carry = max(carry, split.stamp)
		prefix := p.previous
		prefix.Truncate(split.bitLen)
		key := string(eip8297.EncodeBitPath(&prefix))
		p.closed[key] = max(p.closed[key], carry)
		p.open = p.open[:len(p.open)-1]
	}
	p.last = carry
}

func (t *pbinRowStampTracker) stamp(key []byte) (uint64, error) {
	if bytes.Equal(key, pbt.GlobalRootKey()) {
		return t.maximum, nil
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return 0, err
	}
	if path.BitLen < pbinPartitionBits {
		if stamp, ok := t.upper[string(key)]; ok {
			return stamp, nil
		}
		return 0, fmt.Errorf("pbin range writer: row %x has no leaf stamp", key)
	}
	if stamp, ok := t.partition(&path).closed[string(key)]; ok {
		return stamp, nil
	}
	return 0, fmt.Errorf("pbin range writer: row %x has no leaf stamp", key)
}
