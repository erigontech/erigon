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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
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

type pbinStampBranch struct {
	path  eip8297.Bitpath
	stamp uint64
}

type pbinRowStampTracker struct {
	open     []pbinStampBranch
	closed   map[string]uint64
	previous eip8297.Bitpath
	maximum  uint64
}

type pbinRangeWriterWrite struct {
	data []byte
	prev []byte
}

type pbinRangeWriterOverlay struct {
	commitment.PatriciaContext
	writes   map[string]pbinRangeWriterWrite
	release  func([]byte)
	write    func([]byte, []byte, []byte) error
	finished func() error
}

func (o *pbinRangeWriterOverlay) FlushFinished(nextKey []byte) error {
	nextPath := eip8297.PathFromBits(nextKey, int16(len(nextKey)*8))
	return o.flush(func(key []byte) (bool, error) {
		if bytes.Equal(key, pbt.GlobalRootKey()) {
			return true, nil
		}
		path, err := eip8297.DecodeBitPath(key)
		if err != nil {
			return false, err
		}
		return path.BitLen <= nextPath.BitLen && eip8297.CommonPrefixBitsAt(&path, 0, &nextPath) == path.BitLen, nil
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
	tracker := &pbinRowStampTracker{closed: make(map[string]uint64)}
	stampFile, err := os.CreateTemp(w.aggregator.Dirs().Tmp, "pbin-range-stamps-")
	if err != nil {
		return common.Hash{}, err
	}
	defer func() {
		_ = stampFile.Close()
		_ = dir.RemoveFile(stampFile.Name())
	}()
	stampWriter := bufio.NewWriter(stampFile)
	var stampReader *bufio.Reader
	var (
		overlay *pbinRangeWriterOverlay
		root    []byte
		seen    bool
		state   []byte
	)
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
	visit := func(batch []pbt.Op, nextKey []byte, final bool) error {
		seen = true
		for _, op := range batch {
			var stampBytes [8]byte
			if _, readErr := io.ReadFull(stampReader, stampBytes[:]); readErr != nil {
				return readErr
			}
			stamp := binary.BigEndian.Uint64(stampBytes[:])
			if observeErr := tracker.observe(op.Key, stamp); observeErr != nil {
				return observeErr
			}
		}
		if final {
			tracker.closeDeeper(-1)
		} else if advanceErr := tracker.advance(nextKey); advanceErr != nil {
			return advanceErr
		}
		domains.GetCommitmentCtx().SetPBinOps(batch)
		var computeErr error
		root, computeErr = domains.GetCommitmentCtx().ComputeCommitmentWithDiffAndReader(ctx, tx, final, blockNum, w.endTxNum, "pbin-range-writer", nil, nil, nil, func(inner commitment.PatriciaContext) commitment.PatriciaContext {
			overlay.PatriciaContext = inner
			return overlay
		})
		if computeErr != nil {
			return computeErr
		}
		if final {
			return overlay.Flush()
		}
		return overlay.FlushFinished(nextKey)
	}
	overlay = &pbinRangeWriterOverlay{
		writes: make(map[string]pbinRangeWriterWrite),
		write:  onRow,
		finished: func() error {
			clear(tracker.closed)
			return nil
		},
		release: func(key []byte) {
			domains.GetMemBatch().(*TemporalMemBatch).ForgetLatest(w.domain, key)
		},
	}
	stream := func(emit func(pbt.Op) error) error {
		progress := newPbinStreamProgress("PBT range writer progress", "", "range writer")
		streamErr := leaves(func(leaf PBinLeaf) error {
			progress.leaf(leaf.Key)
			if len(leaf.Value) != eip8297.ValueLength {
				return fmt.Errorf("pbin range writer: leaf %x has value length %d", leaf.Key, len(leaf.Value))
			}
			var stampBytes [8]byte
			binary.BigEndian.PutUint64(stampBytes[:], leaf.Stamp)
			if _, writeErr := stampWriter.Write(stampBytes[:]); writeErr != nil {
				return writeErr
			}
			var value [eip8297.ValueLength]byte
			copy(value[:], leaf.Value)
			return emit(pbt.Op{Key: bytes.Clone(leaf.Key), Value: value})
		})
		if streamErr == nil {
			if streamErr = stampWriter.Flush(); streamErr == nil {
				streamErr = stampFile.Sync()
			}
			if streamErr == nil {
				if _, streamErr = stampFile.Seek(0, io.SeekStart); streamErr == nil {
					stampReader = bufio.NewReader(stampFile)
				}
			}
		}
		return streamErr
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
		err = w.ranges[i].collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			if _, writeErr := writer.Write(key); writeErr != nil {
				return writeErr
			}
			_, valueErr := writer.Write(value)
			return valueErr
		}, etl.TransformArgs{})
		if err != nil {
			valuesComp.Close()
			return err
		}
		collation.valuesCount = valuesComp.Count() / 2
		static, err := domain.buildFileRange(ctx, stepFrom, stepTo, collation, background.NewProgressSet(), "")
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

func (t *pbinRowStampTracker) observe(key []byte, stamp uint64) error {
	if len(key) == 0 {
		return fmt.Errorf("pbin range writer: empty leaf key")
	}
	path := eip8297.PathFromBits(key, int16(len(key)*8))
	commonBits := eip8297.CommonPrefixBitsAt(&t.previous, 0, &path)
	t.closeDeeper(commonBits)
	for i := range t.open {
		t.open[i].stamp = max(t.open[i].stamp, stamp)
	}
	start := int16(4)
	if len(t.open) > 0 {
		start = t.open[len(t.open)-1].path.BitLen + 4
	}
	for bitLen := start; bitLen <= path.BitLen; bitLen += 4 {
		prefix := path
		prefix.Truncate(bitLen)
		t.open = append(t.open, pbinStampBranch{path: prefix, stamp: stamp})
	}
	t.maximum = max(t.maximum, stamp)
	t.previous = path
	return nil
}

func (t *pbinRowStampTracker) advance(nextKey []byte) error {
	if len(nextKey) == 0 {
		return fmt.Errorf("pbin range writer: missing lookahead key")
	}
	nextPath := eip8297.PathFromBits(nextKey, int16(len(nextKey)*8))
	commonBits := eip8297.CommonPrefixBitsAt(&t.previous, 0, &nextPath)
	t.closeDeeper(commonBits)
	return nil
}

func (t *pbinRowStampTracker) closeDeeper(bitLen int16) {
	for len(t.open) > 0 && (bitLen < 0 || t.open[len(t.open)-1].path.BitLen > bitLen) {
		t.closeBranch(t.open[len(t.open)-1])
		t.open = t.open[:len(t.open)-1]
	}
}

func (t *pbinRowStampTracker) closeBranch(branch pbinStampBranch) {
	key := string(eip8297.EncodeBitPath(&branch.path))
	t.closed[key] = max(t.closed[key], branch.stamp)
}

func (t *pbinRowStampTracker) stamp(key []byte) (uint64, error) {
	if bytes.Equal(key, pbt.GlobalRootKey()) {
		return t.maximum, nil
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return 0, err
	}
	if stamp, ok := t.closed[string(key)]; ok {
		return stamp, nil
	}
	for i := range t.open {
		if t.open[i].path == path {
			return t.open[i].stamp, nil
		}
	}
	return 0, fmt.Errorf("pbin range writer: row %x has no leaf stamp", key)
}
