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
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type PBinRangeWriter struct {
	aggregator *Aggregator
	domain     kv.Domain
	endTxNum   uint64
	ranges     []pbinRange
}

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
	havePrev bool
	maximum  uint64
}

func NewPBinRangeWriter(aggregator *Aggregator, domain kv.Domain, endTxNum uint64) (*PBinRangeWriter, error) {
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
	files := at.Files(kv.AccountsDomain)
	if len(files) == 0 {
		return nil, fmt.Errorf("pbin range writer: accounts files are empty")
	}
	if endTxNum == 0 {
		endTxNum = files.EndRootNum()
	}
	if endTxNum != files.EndRootNum() {
		return nil, fmt.Errorf("pbin range writer: end txNum %d does not match accounts files %d", endTxNum, files.EndRootNum())
	}
	ranges := make([]pbinRange, 0, len(files))
	seenRanges := make(map[[2]uint64]struct{}, len(files))
	for _, file := range files {
		start, end := file.StartRootNum(), file.EndRootNum()
		if end > endTxNum {
			break
		}
		key := [2]uint64{start, end}
		if _, ok := seenRanges[key]; ok {
			continue
		}
		seenRanges[key] = struct{}{}
		ranges = append(ranges, pbinRange{
			start:     start,
			end:       end,
			collector: etl.NewCollector("pbin-range-writer", aggregator.Dirs().Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()),
		})
	}
	if len(ranges) == 0 || ranges[len(ranges)-1].end != endTxNum {
		for i := range ranges {
			ranges[i].collector.Close()
		}
		return nil, fmt.Errorf("pbin range writer: no complete account ranges through %d", endTxNum)
	}
	return &PBinRangeWriter{aggregator: aggregator, domain: domain, endTxNum: endTxNum, ranges: ranges}, nil
}

func (w *PBinRangeWriter) Write(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, leaves func(func(PBinLeaf) error) error) (common.Hash, error) {
	if w == nil || w.aggregator == nil {
		return common.Hash{}, fmt.Errorf("pbin range writer: nil writer")
	}
	if tx == nil || domains == nil || leaves == nil {
		return common.Hash{}, fmt.Errorf("pbin range writer: missing input")
	}
	if domains.GetCommitmentCtx().CommitmentDomain() != w.domain {
		return common.Hash{}, fmt.Errorf("pbin range writer: shared domains use %s, want %s", domains.GetCommitmentCtx().CommitmentDomain(), w.domain)
	}
	tracker := &pbinRowStampTracker{closed: make(map[string]uint64)}
	stampFile, err := os.CreateTemp(w.aggregator.Dirs().Tmp, "pbin-range-stamps-")
	if err != nil {
		w.closeRanges()
		return common.Hash{}, err
	}
	defer func() {
		_ = stampFile.Close()
		_ = dir.RemoveFile(stampFile.Name())
	}()
	var (
		overlay *pbinRebuildOverlay
		root    []byte
		seen    bool
		state   []byte
	)
	onRow := func(key, data, _ []byte) error {
		if commitment.IsCommitmentStateKey(key) {
			state = bytes.Clone(data)
			return nil
		}
		stamp, err := tracker.stamp(key)
		if err != nil {
			return err
		}
		rangeIndex, err := w.rangeForStamp(stamp)
		if err != nil {
			return err
		}
		return w.ranges[rangeIndex].collector.Collect(key, data)
	}
	visit := func(batch []pbt.Op, nextKey []byte, final bool) error {
		seen = true
		for _, op := range batch {
			var stampBytes [8]byte
			if _, err := io.ReadFull(stampFile, stampBytes[:]); err != nil {
				return err
			}
			stamp := binary.BigEndian.Uint64(stampBytes[:])
			if err := tracker.observe(op.Key, stamp); err != nil {
				return err
			}
		}
		if final {
			tracker.finish()
		} else if err := tracker.advance(nextKey); err != nil {
			return err
		}
		domains.GetCommitmentCtx().SetPBinOps(batch)
		var current *pbinRebuildOverlay
		var err error
		root, err = domains.GetCommitmentCtx().ComputeCommitmentWithDiffAndReader(ctx, tx, final, 0, w.endTxNum, "pbin-range-writer", nil, nil, nil, func(inner commitment.PatriciaContext) commitment.PatriciaContext {
			if overlay == nil {
				overlay = newPBinRebuildOverlay()
			}
			current = overlay.withInner(inner).withWrite(onRow).withFinished(func() error {
				tracker.clearClosed()
				return nil
			})
			return current
		})
		if err != nil {
			return err
		}
		if final {
			return current.Flush()
		}
		return current.FlushFinished(nextKey)
	}
	stream := func(emit func(pbt.Op) error) error {
		err := leaves(func(leaf PBinLeaf) error {
			if len(leaf.Value) != eip8297.ValueLength {
				return fmt.Errorf("pbin range writer: leaf %x has value length %d", leaf.Key, len(leaf.Value))
			}
			var stampBytes [8]byte
			binary.BigEndian.PutUint64(stampBytes[:], leaf.Stamp)
			if _, err := stampFile.Write(stampBytes[:]); err != nil {
				return err
			}
			var value [eip8297.ValueLength]byte
			copy(value[:], leaf.Value)
			return emit(pbt.Op{Key: bytes.Clone(leaf.Key), Value: value})
		})
		if err == nil {
			if err = stampFile.Sync(); err == nil {
				_, err = stampFile.Seek(0, io.SeekStart)
			}
		}
		return err
	}
	if err := pbinForEachRebuildOpStreamLookaheadAfterWithSample(w.aggregator.Dirs().Tmp, pbinRebuildMaxOps, pbinRebuildMaxBytes, nil, visit, stream, nil); err != nil {
		w.closeRanges()
		return common.Hash{}, err
	}
	if !seen {
		root = append([]byte(nil), eip8297.EmptyTreeHash[:]...)
	}
	if len(state) == 0 && seen {
		w.closeRanges()
		return common.Hash{}, fmt.Errorf("pbin range writer: commitment state is missing")
	}
	if err := w.buildFiles(ctx, state); err != nil {
		w.closeRanges()
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
		if i == len(w.ranges)-1 && len(state) != 0 {
			if _, stateKeyErr := writer.Write(commitment.KeyCommitmentState); stateKeyErr != nil {
				valuesComp.Close()
				return stateKeyErr
			}
			if _, stateValueErr := writer.Write(state); stateValueErr != nil {
				valuesComp.Close()
				return stateValueErr
			}
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
	if t.havePrev {
		commonBits := eip8297.CommonPrefixBitsAt(&t.previous, 0, &path)
		for len(t.open) > 0 && t.open[len(t.open)-1].path.BitLen > commonBits {
			t.closeBranch(t.open[len(t.open)-1])
			t.open = t.open[:len(t.open)-1]
		}
	}
	for i := range t.open {
		if path.HasPrefix(&t.open[i].path) && stamp > t.open[i].stamp {
			t.open[i].stamp = stamp
		}
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
	if stamp > t.maximum {
		t.maximum = stamp
	}
	t.previous = path
	t.havePrev = true
	return nil
}

func (t *pbinRowStampTracker) advance(nextKey []byte) error {
	if len(nextKey) == 0 {
		return fmt.Errorf("pbin range writer: missing lookahead key")
	}
	if !t.havePrev {
		return nil
	}
	nextPath := eip8297.PathFromBits(nextKey, int16(len(nextKey)*8))
	commonBits := eip8297.CommonPrefixBitsAt(&t.previous, 0, &nextPath)
	for len(t.open) > 0 && t.open[len(t.open)-1].path.BitLen > commonBits {
		t.closeBranch(t.open[len(t.open)-1])
		t.open = t.open[:len(t.open)-1]
	}
	return nil
}

func (t *pbinRowStampTracker) finish() {
	for len(t.open) > 0 {
		t.closeBranch(t.open[len(t.open)-1])
		t.open = t.open[:len(t.open)-1]
	}
}

func (t *pbinRowStampTracker) closeBranch(branch pbinStampBranch) {
	key := string(eip8297.EncodeBitPath(&branch.path))
	if old, ok := t.closed[key]; !ok || branch.stamp > old {
		t.closed[key] = branch.stamp
	}
}

func (t *pbinRowStampTracker) clearClosed() {
	clear(t.closed)
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
