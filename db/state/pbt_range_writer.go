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
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
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
	state        []byte
	stateInFiles bool
	workerViews  atomic.Int32
	stampTracker func() pbinStampTracker
}

type PBinRangeWriterLimits struct {
	MaxOps              int
	MaxBytes            int
	NoRangePastFrontier bool
}

const (
	pbinRangeWriterMaxOps   = 100_000
	pbinRangeWriterMaxBytes = 64 << 20
)

type pbinRange struct {
	start     uint64
	end       uint64
	collector *etl.Collector
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

type pbinRangeWriterWrite struct {
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
	nextPaths := make([]eip8297.Bitpath, len(lookahead))
	for i, key := range lookahead {
		nextPaths[i] = eip8297.PathFromBits(key, int16(len(key)*8))
	}
	return o.flush(func(key []byte) (bool, error) {
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
	})
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
	keys := make([]string, 0, len(o.writes))
	for key := range o.writes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if skip != nil {
			skipped, err := skip([]byte(key))
			if err != nil {
				return err
			}
			if skipped {
				continue
			}
		}
		write := o.writes[key]
		if err := o.write([]byte(key), write.data, write.prev); err != nil {
			return err
		}
		if err := o.PatriciaContext.PutBranch([]byte(key), write.data, write.prev); err != nil {
			return err
		}
		o.release([]byte(key))
		delete(o.writes, key)
	}
	return o.finished()
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
	at := aggregator.BeginFilesRo()
	defer at.Close()
	files := pbinAccountFiles(at.Files(kv.AccountsDomain))
	if len(files) == 0 {
		if endTxNum == 0 {
			return &PBinRangeWriter{aggregator: aggregator, domain: domain, maxOps: limits.MaxOps, maxBytes: limits.MaxBytes}, nil
		}
		return &PBinRangeWriter{
			aggregator:   aggregator,
			domain:       domain,
			endTxNum:     endTxNum,
			leafStamp:    endTxNum,
			maxOps:       limits.MaxOps,
			maxBytes:     limits.MaxBytes,
			stateInFiles: !limits.NoRangePastFrontier,
			ranges: []pbinRange{{
				start:     0,
				end:       endTxNum + 1,
				collector: etl.NewCollector("pbin-range-writer", aggregator.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true),
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
		ranges = append(ranges, pbinRange{
			start:     start,
			end:       end,
			collector: etl.NewCollector("pbin-range-writer", aggregator.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true),
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
		ranges = append(ranges, pbinRange{
			start:     ranges[len(ranges)-1].end,
			end:       endTxNum + 1,
			collector: etl.NewCollector("pbin-range-writer", aggregator.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).SortAndFlushInBackground(true),
		})
	}
	return &PBinRangeWriter{aggregator: aggregator, domain: domain, endTxNum: endTxNum, leafStamp: leafStamp, ranges: ranges, maxOps: limits.MaxOps, maxBytes: limits.MaxBytes, stateInFiles: stateInFiles}, nil
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
		overlay *pbinRangeWriterOverlay
		root    []byte
		seen    bool
		state   []byte
	)
	foldProgress := NewPBTProgress(PBTProgressConfig{Message: "PBT fold progress", Phase: "fold batches", TempDir: w.aggregator.Dirs().Tmp})
	defer foldProgress.Done()
	onRow := func(key, data, _ []byte) error {
		if commitment.IsCommitmentStateKey(key) {
			state = bytes.Clone(data)
			return nil
		}
		stamp, stampErr := tracker.stamp(key)
		if stampErr != nil {
			return stampErr
		}
		rangeIndex, rangeErr := w.rangeForStamp(stamp)
		if rangeErr != nil {
			return rangeErr
		}
		return w.ranges[rangeIndex].collector.Collect(key, data)
	}
	visit := func(batch []pbt.Op, stamps []uint64, lookahead [][]byte, final bool) error {
		foldProgress.Add(1)
		seen = true
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
		write:  onRow,
		finished: func() error {
			tracker.clearClosed()
			return nil
		},
		release: func(key []byte) {
			domains.GetMemBatch().(*TemporalMemBatch).ForgetLatest(w.domain, key)
		},
	}
	stream := func(emit func(pbt.Op, uint64) error) error {
		progress := NewPBTProgress(PBTProgressConfig{Message: "PBT range writer progress", Phase: "range leaf stream", TempDir: w.aggregator.Dirs().Tmp})
		defer progress.Done()
		return leaves(func(leaf PBinLeaf) error {
			progress.Add(1)
			if len(leaf.Value) != eip8297.ValueLength {
				return fmt.Errorf("pbin range writer: leaf %x has value length %d", leaf.Key, len(leaf.Value))
			}
			var value [eip8297.ValueLength]byte
			copy(value[:], leaf.Value)
			return emit(pbt.Op{Key: bytes.Clone(leaf.Key), Value: value}, leaf.Stamp)
		})
	}
	if streamErr := pbinForEachRebuildOpStream(w.aggregator.Dirs().Tmp, w.maxOps, w.maxBytes, visit, stream); streamErr != nil {
		return common.Hash{}, streamErr
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
		w.ranges[i].collector.Close()
	}
}

func (w *PBinRangeWriter) buildFiles(ctx context.Context, state []byte) error {
	defer w.closeRanges()
	domain := w.aggregator.d[w.domain]
	stepSize := w.aggregator.StepSize()
	for i := range w.ranges {
		stepFrom := kv.Step(w.ranges[i].start / stepSize)
		stepTo := kv.Step(w.ranges[i].end / stepSize)
		valuesPath := domain.kvNewFilePath(stepFrom, stepTo)
		valuesComp, err := seg.NewCompressor(ctx, domain.FilenameBase+".domain.convert", valuesPath, domain.dirs.Tmp, domain.CompressCfg, log.LvlTrace, domain.logger)
		if err != nil {
			return err
		}
		collation := Collation{valuesComp: valuesComp, valuesPath: valuesPath}
		writer := seg.NewWriter(valuesComp, seg.CompressNone)
		if w.stateInFiles && i == len(w.ranges)-1 && len(state) != 0 {
			if collectErr := w.ranges[i].collector.Collect(commitment.KeyCommitmentState, state); collectErr != nil {
				valuesComp.Close()
				return collectErr
			}
		}
		rangeProgress := newPbtRangeProgress(w.aggregator.Dirs().Tmp, fmt.Sprintf("range %d-%d", stepFrom, stepTo))
		err = w.ranges[i].collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			if _, writeErr := writer.Write(key); writeErr != nil {
				return writeErr
			}
			_, valueErr := writer.Write(value)
			return valueErr
		}, etl.TransformArgs{Progress: rangeProgress.stage(".kv", w.ranges[i].collector.Collected())})
		if err != nil {
			valuesComp.Close()
			return err
		}
		collation.valuesCount = valuesComp.Count() / 2
		static, err := domain.buildFileRange(ctx, stepFrom, stepTo, collation, background.NewProgressSet(), "", rangeProgress)
		if err != nil {
			return err
		}
		w.aggregator.dirtyFilesLock.Lock()
		domain.integrateDirtyFiles(static, w.ranges[i].start, w.ranges[i].end)
		w.aggregator.recalcVisibleFiles(nil)
		w.aggregator.dirtyFilesLock.Unlock()
	}
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
