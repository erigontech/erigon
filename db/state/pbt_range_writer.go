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
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

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
	aggregator         *Aggregator
	domain             kv.Domain
	endTxNum           uint64
	leafStamp          uint64
	ranges             []pbinRange
	maxOps             int
	maxBytes           int
	workers            int
	state              []byte
	stateInFiles       bool
	resume             bool
	resumeSourceLayout bool
	stampTracker       func() pbinStampTracker
	pipelineHook       func(string, int) error
	absentProbe        PBinAbsentProbeFunc
}

type PBinRangeWriterLimits struct {
	MaxOps              int
	MaxBytes            int
	NoRangePastFrontier bool
	Workers             int
	AbsentProbe         PBinAbsentProbeFunc
}

type PBinAbsentProbeFunc func(func([]byte) bool)

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
	base     bool
	release  func([]byte)
	write    func([]byte, []byte, []byte) error
	finished func() error
}

type pbinDirectBranchWriter interface {
	PutBranchDirect([]byte, []byte, []byte) error
}

type pbinStampTrackerClone interface {
	clone() pbinStampTracker
}

type pbinFoldBatch struct {
	batch     []pbt.Op
	lookahead [][]byte
	final     bool
	index     int
	tracker   pbinStampTracker
	rows      []pbinRangeWriterDetached
}

type pbinFoldPipeline struct {
	ctx         context.Context
	cancel      context.CancelFunc
	submit      chan *pbinFoldBatch
	detached    chan *pbinFoldBatch
	completed   chan int
	tracker     pbinStampTracker
	compute     func(context.Context, *pbinFoldBatch) error
	process     func(*pbinFoldBatch) error
	hook        func(string, int) error
	failureLock sync.Mutex
	failure     pbinFoldFailure
	wg          sync.WaitGroup
}

type pbinFoldFailure struct {
	index int
	err   error
}

func newPBinFoldPipeline(ctx context.Context, tracker pbinStampTracker, compute func(context.Context, *pbinFoldBatch) error, process func(*pbinFoldBatch) error, hook func(string, int) error) (*pbinFoldPipeline, bool) {
	clone, ok := tracker.(pbinStampTrackerClone)
	if !ok {
		return nil, false
	}
	pipelineCtx, cancel := context.WithCancel(ctx)
	p := &pbinFoldPipeline{
		ctx:       pipelineCtx,
		cancel:    cancel,
		submit:    make(chan *pbinFoldBatch, 1),
		detached:  make(chan *pbinFoldBatch, 1),
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

func (p *pbinFoldPipeline) fail(index int, err error) {
	if err == nil {
		return
	}
	p.failureLock.Lock()
	if p.failure.err == nil || index < p.failure.index {
		p.failure = pbinFoldFailure{index: index, err: err}
	}
	p.failureLock.Unlock()
	p.cancel()
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
	case <-p.ctx.Done():
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
		lookahead: clonePBinKeys(lookahead),
		final:     final,
		index:     index,
		tracker:   cloned.clone(),
	}
	p.tracker.clearClosed()
	select {
	case p.submit <- queued:
		return nil
	case <-p.ctx.Done():
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
			select {
			case p.detached <- batch:
			case <-p.ctx.Done():
				return
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *pbinFoldPipeline) processLoop() {
	defer p.wg.Done()
	for {
		select {
		case batch, ok := <-p.detached:
			if !ok {
				return
			}
			if p.hook != nil {
				if err := p.hook("stage3", batch.index); err != nil {
					p.fail(batch.index, err)
					return
				}
			}
			if processErr := p.process(batch); processErr != nil {
				p.fail(batch.index, processErr)
				return
			}
			batch.tracker.clearClosed()
			select {
			case p.completed <- batch.index:
			case <-p.ctx.Done():
				return
			}
		case <-p.ctx.Done():
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
	if _, held := o.writes[string(key)]; held {
		return false
	}
	if !o.base {
		return true
	}
	data, _, err := o.PatriciaContext.Branch(key)
	return err != nil || len(data) == 0
}

func (o *pbinRangeWriterOverlay) DecorateWorkerContext(worker commitment.PatriciaContext) commitment.PatriciaContext {
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
	return o.flush(pbinRangeWriterSkip(lookahead))
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
	return o.flush(nil)
}

func (o *pbinRangeWriterOverlay) flush(skip func([]byte) (bool, error)) error {
	rows, err := o.detach(skip)
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
	keys := slices.Sorted(maps.Keys(o.writes))
	rows := make([]pbinRangeWriterDetached, 0, len(keys))
	putter, direct := o.PatriciaContext.(pbinDirectBranchWriter)
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
		var err error
		if direct {
			err = putter.PutBranchDirect([]byte(key), write.data, write.prev)
		} else {
			err = o.PatriciaContext.PutBranch([]byte(key), write.data, write.prev)
		}
		if err != nil {
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

func newPBinRangeWriterForResume(aggregator *Aggregator, domain kv.Domain, baseTxNum, endTxNum uint64, baseState []byte, baseStateEnd uint64, sourceRanges [][2]uint64) (*PBinRangeWriter, error) {
	if aggregator == nil {
		return nil, fmt.Errorf("pbin resume range writer: nil aggregator")
	}
	if domain != kv.CommitmentDomain && domain != kv.CommitmentBinDomain {
		return nil, fmt.Errorf("pbin resume range writer: invalid domain %s", domain)
	}
	if endTxNum < baseTxNum {
		return nil, fmt.Errorf("pbin resume range writer: end txNum %d is before base txNum %d", endTxNum, baseTxNum)
	}
	stepSize := aggregator.StepSize()
	if stepSize == 0 {
		return nil, fmt.Errorf("pbin resume range writer: step size is zero")
	}
	at := aggregator.BeginFilesRo()
	defer at.Close()
	baseFiles := pbinAccountFiles(at.Files(domain))
	if len(baseFiles) == 0 {
		return nil, fmt.Errorf("pbin resume range writer: base domain %s has no files", domain)
	}
	for _, file := range baseFiles {
		if file.StartRootNum() > baseTxNum && visibleFileHasRows(file) {
			return nil, fmt.Errorf("pbin resume range writer: target %s file range %d-%d is past selected base txNum %d", domain, file.StartRootNum(), file.EndRootNum(), baseTxNum)
		}
	}
	writer := &PBinRangeWriter{
		aggregator:   aggregator,
		domain:       domain,
		endTxNum:     endTxNum,
		leafStamp:    endTxNum,
		maxOps:       pbinRangeWriterMaxOps,
		maxBytes:     pbinRangeWriterMaxBytes,
		workers:      dbg.PBTWorkers,
		stateInFiles: true,
		resume:       true,
	}
	if len(baseState) == 0 {
		state, found, _, stateEnd, err := at.DebugGetLatestFromFiles(domain, commitment.KeyCommitmentState, ^uint64(0))
		if err != nil {
			return nil, fmt.Errorf("pbin resume range writer: read base state: %w", err)
		}
		if !found || stateEnd < baseTxNum {
			return nil, fmt.Errorf("pbin resume range writer: base domain %s has no state at txNum %d", domain, baseTxNum)
		}
		if len(state) < 16 {
			return nil, fmt.Errorf("pbin resume range writer: target %s state is invalid", domain)
		}
		stateTx, _ := commitmentdb.DecodeTxBlockNums(state)
		if stateTx > baseTxNum {
			return nil, fmt.Errorf("pbin resume range writer: target %s state is past selected base txNum %d at %d", domain, baseTxNum, stateTx)
		}
		if len(sourceRanges) != 0 {
			if rangeErr := pbinValidateResumeBaseRanges(baseFiles, sourceRanges, baseTxNum); rangeErr != nil {
				return nil, rangeErr
			}
			ranges, rangeErr := pbinResumeOutputRanges(aggregator, sourceRanges, baseTxNum)
			if rangeErr != nil {
				return nil, rangeErr
			}
			writer.resumeSourceLayout = true
			writer.ranges = ranges
			return writer, nil
		}
		start := max(stateEnd, baseTxNum)
		end := max(endTxNum+1, start+stepSize)
		end = ((end + stepSize - 1) / stepSize) * stepSize
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", start, end))
		if err != nil {
			return nil, err
		}
		writer.ranges = []pbinRange{{start: start, end: end, rows: rows}}
		return writer, nil
	}
	files := baseFiles
	accountFiles := pbinAccountFiles(at.Files(kv.AccountsDomain))
	if len(accountFiles) != 0 {
		files = accountFiles
	}
	stateEnd := baseStateEnd
	if stateEnd == 0 {
		stateEnd = files.EndRootNum()
	}
	if len(baseState) < 16 {
		return nil, fmt.Errorf("pbin resume range writer: target %s state is invalid", domain)
	}
	stateTx, _ := commitmentdb.DecodeTxBlockNums(baseState)
	if stateTx > baseTxNum {
		return nil, fmt.Errorf("pbin resume range writer: target %s state is past selected base txNum %d at %d", domain, baseTxNum, stateTx)
	}
	if stateEnd <= baseTxNum {
		return nil, fmt.Errorf("pbin resume range writer: base domain %s has no state at txNum %d", domain, baseTxNum)
	}
	if len(sourceRanges) != 0 {
		if err := pbinValidateResumeBaseRanges(baseFiles, sourceRanges, baseTxNum); err != nil {
			return nil, err
		}
		ranges, err := pbinResumeOutputRanges(aggregator, sourceRanges, baseTxNum)
		if err != nil {
			return nil, err
		}
		writer.resumeSourceLayout = true
		writer.ranges = ranges
		return writer, nil
	}
	start := max(stateEnd, baseTxNum)
	ranges := make([]pbinRange, 0, len(files))
	for _, file := range files {
		fileStart, fileEnd := file.StartRootNum(), file.EndRootNum()
		if fileEnd <= start {
			continue
		}
		fileStart = max(fileStart, start)
		fileEnd = max(fileEnd, fileStart+stepSize)
		if fileEnd > endTxNum {
			fileEnd = (endTxNum + stepSize) / stepSize * stepSize
		}
		if fileEnd <= fileStart {
			continue
		}
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", fileStart, fileEnd))
		if err != nil {
			for i := range ranges {
				ranges[i].rows.close()
			}
			return nil, err
		}
		ranges = append(ranges, pbinRange{start: fileStart, end: fileEnd, rows: rows})
	}
	if len(ranges) == 0 {
		end := max(endTxNum+1, start+stepSize)
		end = ((end + stepSize - 1) / stepSize) * stepSize
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", start, end))
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, pbinRange{start: start, end: end, rows: rows})
	}
	last := &ranges[len(ranges)-1]
	if last.end < endTxNum+1 {
		end := ((endTxNum + 1 + stepSize - 1) / stepSize) * stepSize
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", last.end, end))
		if err != nil {
			for i := range ranges {
				ranges[i].rows.close()
			}
			return nil, err
		}
		ranges = append(ranges, pbinRange{start: last.end, end: end, rows: rows})
	}
	writer.ranges = ranges
	return writer, nil
}

func pbinValidateResumeBaseRanges(baseFiles kv.VisibleFiles, sourceRanges [][2]uint64, baseTxNum uint64) error {
	for i, sourceRange := range sourceRanges {
		if sourceRange[0] >= sourceRange[1] {
			return fmt.Errorf("pbin resume range writer: invalid source commitment range %d-%d", sourceRange[0], sourceRange[1])
		}
		if i != 0 && sourceRange[0] < sourceRanges[i-1][1] {
			return fmt.Errorf("pbin resume range writer: overlapping source commitment ranges %d-%d and %d-%d", sourceRanges[i-1][0], sourceRanges[i-1][1], sourceRange[0], sourceRange[1])
		}
	}
	for _, file := range baseFiles {
		if file.StartRootNum() > baseTxNum || !visibleFileHasRows(file) {
			continue
		}
		fileStart, fileEnd := file.StartRootNum(), file.EndRootNum()
		contained := false
		for i, sourceRange := range sourceRanges {
			if fileStart >= sourceRange[0] && fileEnd <= sourceRange[1] {
				contained = true
				break
			}
			if fileStart < sourceRange[1] && fileEnd > sourceRange[0] {
				if i+1 < len(sourceRanges) && fileEnd > sourceRanges[i+1][0] {
					return fmt.Errorf("pbin resume range writer: base bin file range %d-%d straddles source commitment ranges %d-%d and %d-%d", fileStart, fileEnd, sourceRange[0], sourceRange[1], sourceRanges[i+1][0], sourceRanges[i+1][1])
				}
				return fmt.Errorf("pbin resume range writer: base bin file range %d-%d is outside source commitment range %d-%d", fileStart, fileEnd, sourceRange[0], sourceRange[1])
			}
		}
		if contained {
			continue
		}
		return fmt.Errorf("pbin resume range writer: base bin file range %d-%d is outside source commitment layout", fileStart, fileEnd)
	}
	return nil
}

func pbinResumeOutputRanges(aggregator *Aggregator, sourceRanges [][2]uint64, baseTxNum uint64) ([]pbinRange, error) {
	ranges := make([]pbinRange, 0, len(sourceRanges))
	for _, sourceRange := range sourceRanges {
		if sourceRange[1] <= baseTxNum {
			continue
		}
		rows, err := newPBinRowLanes(aggregator.Dirs().Tmp, fmt.Sprintf("%d-%d", sourceRange[0], sourceRange[1]))
		if err != nil {
			for i := range ranges {
				ranges[i].rows.close()
			}
			return nil, err
		}
		ranges = append(ranges, pbinRange{start: sourceRange[0], end: sourceRange[1], rows: rows})
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("pbin resume range writer: source commitment ranges end at or before selected base txNum %d", baseTxNum)
	}
	return ranges, nil
}

func visibleFileHasRows(file kv.VisibleFile) bool {
	visible, ok := file.(visibleFile)
	return ok && visible.src != nil && visible.src.decompressor != nil && visible.src.decompressor.Count() != 0
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
		if endTxNum == ^uint64(0) {
			return nil, fmt.Errorf("pbin range writer: end txNum is too large")
		}
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
	if endTxNum == ^uint64(0) && !limits.NoRangePastFrontier {
		return nil, fmt.Errorf("pbin range writer: end txNum is too large")
	}
	if endTxNum == 0 {
		endTxNum = files.EndRootNum() - 1
	}
	ranges := make([]pbinRange, 0, len(files))
	success := false
	defer func() {
		if !success {
			for i := range ranges {
				ranges[i].rows.close()
			}
		}
	}()
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
	success = true
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
	var err error
	if w.resume {
		err = w.resumeDomain(tx)
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		var fresh bool
		fresh, err = w.freshDomain(tx)
		if err != nil {
			return common.Hash{}, err
		}
		if !fresh {
			return common.Hash{}, fmt.Errorf("pbin range writer: target domain %s already holds rows; the range writer builds only into an empty domain", w.domain)
		}
	}
	var tracker pbinStampTracker
	if !w.resume || w.resumeSourceLayout {
		tracker = newPBinRowStampTracker()
		if w.stampTracker != nil {
			tracker = w.stampTracker()
		}
	}
	var (
		overlay  *pbinRangeWriterOverlay
		pipeline *pbinFoldPipeline
		root     []byte
		seen     bool
		state    []byte
	)
	probeDone := false
	runProbe := func() {
		if w.absentProbe != nil && !probeDone {
			w.absentProbe(overlay.Absent)
			probeDone = true
		}
	}
	foldProgress := NewPBTProgress(PBTProgressConfig{Message: "PBT fold progress", Phase: "fold batches", TempDir: w.aggregator.Dirs().Tmp})
	defer foldProgress.Done()
	onRow := func(rowTracker pbinStampTracker, key, data []byte) error {
		if commitment.IsCommitmentStateKey(key) {
			state = bytes.Clone(data)
			return nil
		}
		rangeIndex := len(w.ranges) - 1
		if rowTracker != nil {
			stamp, stampErr := rowTracker.stamp(key)
			if stampErr != nil {
				return stampErr
			}
			var rangeErr error
			rangeIndex, rangeErr = w.rangeForStamp(stamp)
			if rangeErr != nil {
				return rangeErr
			}
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
		if tracker != nil {
			for i, op := range batch {
				if observeErr := tracker.observe(op.Key, stamps[i]); observeErr != nil {
					return observeErr
				}
			}
			if advanceErr := tracker.advance(lookahead); advanceErr != nil {
				return advanceErr
			}
		}
		domains.GetCommitmentCtx().SetPBinOps(batch)
		var computeErr error
		root, computeErr = domains.GetCommitmentCtx().ComputeCommitmentWithDiff(ctx, tx, final, blockNum, w.endTxNum, "pbin-range-writer", nil, nil, nil, func(inner commitment.PatriciaContext) commitment.PatriciaContext {
			overlay.PatriciaContext = inner
			runProbe()
			return overlay
		})
		if computeErr != nil {
			return computeErr
		}
		if final {
			if flushErr := overlay.Flush(); flushErr != nil {
				return flushErr
			}
			return nil
		}
		return overlay.FlushFinished(lookahead)
	}
	overlay = &pbinRangeWriterOverlay{
		writes: make(map[string]pbinRangeWriterWrite),
		base:   w.resume,
		write: func(key, data, _ []byte) error {
			return onRow(tracker, key, data)
		},
		finished: func() error {
			if tracker != nil {
				tracker.clearClosed()
			}
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
				runProbe()
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
		func(batch *pbinFoldBatch) error {
			for _, row := range batch.rows {
				if rowErr := onRow(batch.tracker, row.key, row.data); rowErr != nil {
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
		batch := &pbtProgressBatch{progress: progress, threshold: 4096}
		defer batch.Done()
		return leaves(func(leaf PBinLeaf) error {
			batch.Add(1)
			if len(leaf.Value) != eip8297.ValueLength {
				return fmt.Errorf("pbin range writer: leaf %x has value length %d", leaf.Key, len(leaf.Value))
			}
			var value [eip8297.ValueLength]byte
			copy(value[:], leaf.Value)
			op := pbt.Op{Key: bytes.Clone(leaf.Key), Value: value}
			return emit(op, leaf.Stamp)
		})
	}
	streamErr := pbinForEachRebuildOpStream(w.aggregator.Dirs().Tmp, w.maxOps, w.maxBytes, visit, stream)
	if streamErr != nil && pipeline != nil {
		pipeline.cancel()
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
		if w.resume {
			root, err = domains.GetCommitmentCtx().Trie().RootHash()
			if err != nil {
				return common.Hash{}, err
			}
		} else {
			root = append([]byte(nil), eip8297.EmptyTreeHash[:]...)
		}
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
	if w.resumeSourceLayout {
		if err := w.aggregator.RemoveOverlapsAfterMerge(); err != nil {
			return common.Hash{}, err
		}
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

func (w *PBinRangeWriter) resumeDomain(tx kv.Tx) error {
	at := w.aggregator.BeginFilesRo()
	defer at.Close()
	inFiles, err := at.DebugRangeLatestFromFiles(w.domain, nil, nil, 1)
	if err != nil {
		return err
	}
	defer inFiles.Close()
	if !inFiles.HasNext() {
		return fmt.Errorf("pbin resume range writer: base domain %s has no rows", w.domain)
	}
	firstKey, err := kv.FirstKey(tx, w.aggregator.d[w.domain].ValuesTable)
	if err != nil {
		return err
	}
	if len(firstKey) != 0 {
		return fmt.Errorf("pbin resume range writer: base domain %s has MDBX rows", w.domain)
	}
	return nil
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
	budget          uint64
	accessorWorkers int
	static          StaticFiles
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
		dstDir := ""
		if w.resumeSourceLayout {
			dstDir = domain.dirs.Tmp
		}
		valuesPath := domain.kvNewFilePathIn(dstDir, stepFrom, stepTo)
		if w.resumeSourceLayout {
			valuesPath = filepath.Join(domain.dirs.Tmp, fmt.Sprintf("pbin-resume-delta-%d-%d-%d.kv", stepFrom, stepTo, i))
		}
		plans[i] = pbinRangeBuildPlan{
			index:           i,
			stepFrom:        stepFrom,
			stepTo:          stepTo,
			valuesPath:      valuesPath,
			idtPath:         filepath.Join(domain.dirs.Tmp, filepath.Base(valuesPath)+".idt"),
			kviPath:         domain.kviAccessorNewFilePathIn(dstDir, stepFrom, stepTo),
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
	memoryCapacity := max(pbinRangeBuildMemoryCap/pbinRangeBuildMemoryUnit, uint64(1))
	memoryLimiter := semaphore.NewWeighted(int64(memoryCapacity))
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
	g.SetLimit(max(1, w.workers))
	for i := range plans {
		plan := &plans[i]
		weight := max(uint64(1), (plan.budget+pbinRangeBuildMemoryUnit-1)/pbinRangeBuildMemoryUnit)
		weight = min(weight, memoryCapacity)
		g.Go(func() error {
			if err := memoryLimiter.Acquire(buildCtx, int64(weight)); err != nil {
				return err
			}
			inFlight.Add(1)
			setBuildProgress()
			defer func() {
				inFlight.Add(-1)
				memoryLimiter.Release(int64(weight))
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
	if w.resumeSourceLayout {
		for i := range plans {
			if w.pipelineHook != nil {
				if err := w.pipelineHook("resume-merge", i); err != nil {
					cleanup()
					return err
				}
			}
			merged, err := w.mergeResumeRange(ctx, domain, plans[i].static, w.ranges[i].start, w.ranges[i].end)
			if err != nil {
				cleanup()
				return err
			}
			plans[i].static = merged
		}
	}
	w.aggregator.dirtyFilesLock.Lock()
	defer w.aggregator.dirtyFilesLock.Unlock()
	for i := range plans {
		domain.integrateDirtyFiles(plans[i].static, w.ranges[i].start, w.ranges[i].end)
	}
	w.aggregator.recalcVisibleFiles(nil)
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
	dstDir := ""
	if w.resumeSourceLayout {
		dstDir = domain.dirs.Tmp
	}
	static, err := domain.buildFileRangeWithWorkers(buildCtx, plan.stepFrom, plan.stepTo, collation, background.NewProgressSet(), dstDir, rangeProgress, plan.accessorWorkers)
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

func (w *PBinRangeWriter) mergeResumeRange(ctx context.Context, domain *Domain, delta StaticFiles, start, end uint64) (StaticFiles, error) {
	deltaItem := newFilesItem(start, end+1)
	deltaItem.decompressor = delta.valuesDecomp
	deltaItem.index = delta.valuesIdx
	deltaItem.bindex = delta.valuesBt
	deltaItem.existence = delta.existenceFilter
	ranges := &Ranges{}
	ranges.domain[w.domain] = NewDomainRanges(w.domain, *NewMergeRange(w.domain.String(), true, start, end), HistoryRanges{}, w.aggregator.StepSize())
	at := w.aggregator.BeginFilesRo()
	defer at.Close()
	visible, err := at.filesInRange(ranges)
	if err != nil {
		deltaItem.closeFilesAndRemove()
		return StaticFiles{}, err
	}
	inputs := append(visible.DomainFiles(w.domain), deltaItem)
	merged, _, _, err := at.d[w.domain].mergeFiles(ctx, inputs, nil, nil, ranges.domain[w.domain], nil, true, background.NewProgressSet())
	if err != nil {
		deltaItem.closeFilesAndRemove()
		return StaticFiles{}, err
	}
	deltaItem.closeFilesAndRemove()
	return StaticFiles{valuesDecomp: merged.decompressor, valuesIdx: merged.index, valuesBt: merged.bindex, existenceFilter: merged.existence}, nil
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
