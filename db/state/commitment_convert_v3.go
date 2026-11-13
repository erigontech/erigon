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
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

const commitmentV3Batch = 1024

var commitmentV3BatchBytes = 1 << 20

type kvBatch struct {
	buf   []byte
	pairs [][2][]byte
}

func (b *kvBatch) reset() {
	b.buf, b.pairs = b.buf[:0], b.pairs[:0]
}

const commitmentV3Collectors = 4

type convertedBatch struct {
	buf  []byte
	ents [][3]int
}

func (b *convertedBatch) emit(k, v []byte, kind v3.LegacyKind) {
	k0 := len(b.buf)
	b.buf = append(b.buf, k...)
	v0 := len(b.buf)
	b.buf = append(append(b.buf, byte(kind)), v...)
	b.ents = append(b.ents, [3]int{k0, v0, len(b.buf)})
}

func getConvertedBatch() *convertedBatch {
	res := convertedBatchPool.Get().(*convertedBatch)
	res.buf, res.ents = res.buf[:0], res.ents[:0]
	return res
}

type convertedSink struct {
	ctx context.Context
	out chan<- *convertedBatch
	res *convertedBatch
}

func (s *convertedSink) send() error {
	if len(s.res.ents) == 0 {
		return nil
	}
	select {
	case s.out <- s.res:
		s.res = getConvertedBatch()
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *convertedSink) emit(k, v []byte, kind v3.LegacyKind) error {
	if len(s.res.ents) != 0 && len(s.res.buf)+len(k)+1+len(v) > commitmentV3BatchBytes {
		if err := s.send(); err != nil {
			return err
		}
	}
	s.res.emit(k, v, kind)
	return nil
}

var (
	kvBatchPool        = sync.Pool{New: func() any { return &kvBatch{pairs: make([][2][]byte, 0, commitmentV3Batch)} }}
	convertedBatchPool = sync.Pool{New: func() any { return &convertedBatch{} }}
)

func newV3Collectors(name, tmpDir string, workers int, logger log.Logger) ([]*etl.Collector, func()) {
	collectors := make([]*etl.Collector, min(workers, commitmentV3Collectors))
	for i := range collectors {
		collectors[i] = etl.NewCollector(name, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger).SortAndFlushInBackground(true)
	}
	return collectors, func() {
		for _, coll := range collectors {
			coll.Close()
		}
	}
}

func mergeLoadGroups(ctx context.Context, name string, collectors []*etl.Collector, flush func(key []byte, group []v3.LegacyEntry) error) error {
	var groupKey, groupVals []byte
	var group []v3.LegacyEntry
	flushGroup := func() error {
		if len(group) == 0 {
			return nil
		}
		return flush(groupKey, group)
	}
	err := etl.MergeLoad(name, collectors, func(k, v []byte) error {
		if len(group) == 0 || !bytes.Equal(k, groupKey) {
			if err := flushGroup(); err != nil {
				return err
			}
			groupKey, groupVals, group = append(groupKey[:0], k...), groupVals[:0], group[:0]
		}
		start := len(groupVals)
		groupVals = append(groupVals, v[1:]...)
		group = append(group, v3.LegacyEntry{Kind: v3.LegacyKind(v[0]), Value: groupVals[start:]})
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return err
	}
	return flushGroup()
}

func goWorkers(g *errgroup.Group, n int, work func() error, done func()) {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return work()
		})
	}
	g.Go(func() error {
		wg.Wait()
		done()
		return nil
	})
}

func collectConverted(g *errgroup.Group, out <-chan *convertedBatch, collectors []*etl.Collector) {
	for _, coll := range collectors {
		g.Go(func() error {
			for res := range out {
				for _, e := range res.ents {
					if err := coll.Collect(res.buf[e[0]:e[1]], res.buf[e[1]:e[2]]); err != nil {
						return err
					}
				}
				convertedBatchPool.Put(res)
			}
			return nil
		})
	}
}

func convertCommitmentFileV3(
	ctx context.Context,
	at *AggregatorRoTx,
	file VisibleFile,
	dstDir string,
	fileIdx, fileTotal int,
	grandTotalKeys, processedKeys uint64,
	logger log.Logger,
) (sizeDelta int64, ki uint64, err error) {
	st, err := detectFileState(at, file, 48)
	if err != nil && !errors.Is(err, errNoNonStateSamples) {
		return 0, 0, fmt.Errorf("convertCommitmentFileV3 %q: %w", file.Fullpath(), err)
	}
	if st.squeezed {
		return 0, 0, fmt.Errorf("convertCommitmentFileV3 %q: branches reference plain keys by offset; unsqueeze first with `commitment convert` without --squeeze", file.Fullpath())
	}

	stepSize := at.StepSize()
	startTxNum, endTxNum := file.StartRootNum(), file.EndRootNum()
	if endTxNum == 0 || endTxNum < startTxNum {
		return 0, 0, fmt.Errorf("convertCommitmentFileV3 %q: invalid range %d..%d", file.Fullpath(), startTxNum, endTxNum)
	}
	stepFrom, stepTo := kv.Step(startTxNum/stepSize), kv.Step(endTxNum/stepSize)
	incremental := stepFrom > 0

	vf, ok := file.(visibleFile)
	if !ok || vf.src == nil || vf.src.decompressor == nil {
		return 0, 0, fmt.Errorf("convertCommitmentFileV3 %q: source has no decompressor", file.Fullpath())
	}
	d := at.d[kv.CommitmentDomain].d
	srcCompression := d.Compression
	if vf.src.StepCount(stepSize) < DomainMinStepsToCompress {
		srcCompression = seg.CompressNone
	}

	baseName := filepath.Base(file.Fullpath())
	fileStart := time.Now()
	workers := max(1, runtime.GOMAXPROCS(0))
	view, err := vf.src.decompressor.OpenSequentialView()
	if err != nil {
		return 0, 0, fmt.Errorf("convertCommitmentFileV3 %q: %w", file.Fullpath(), err)
	}
	defer view.Close()
	collectors, closeCollectors := newV3Collectors("commitment_convert_v3", at.Dirs().Tmp, workers, logger)
	defer closeCollectors()
	in := make(chan *kvBatch, workers*2)
	out := make(chan *convertedBatch, workers*2)
	var read atomic.Uint64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(in)
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		reader := seg.NewReader(view.MakeGetter(), srcCompression)
		keysCompressed, valsCompressed := srcCompression.Has(seg.CompressKeys), srcCompression.Has(seg.CompressVals)
		batch := kvBatchPool.Get().(*kvBatch)
		batch.reset()
		send := func() error {
			select {
			case in <- batch:
				batch = kvBatchPool.Get().(*kvBatch)
				batch.reset()
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		for reader.HasNext() {
			k := readOwned(reader, batch, keysCompressed)
			if !reader.HasNext() {
				return fmt.Errorf("truncated at key %x", k)
			}
			n := read.Add(1)
			v := readOwned(reader, batch, valsCompressed)
			if bytes.Equal(k, commitment.KeyCommitmentState) && len(v) >= 8 {
				if txNum := binary.BigEndian.Uint64(v); txNum != endTxNum-1 {
					return fmt.Errorf("commitment state txNum %d differs from file end txNum %d", txNum, endTxNum-1)
				}
			}
			batch.pairs = append(batch.pairs, [2][]byte{k, v})
			if len(batch.pairs) == commitmentV3Batch {
				if sendErr := send(); sendErr != nil {
					return sendErr
				}
			}
			select {
			case <-logEvery.C:
				logger.Info(fmt.Sprintf("[commitment_convert] v3 file=%s %s key/s at %s/%s %s",
					baseName, formatRate(n, time.Since(fileStart)),
					common.PrettyCounter(processedKeys+n), common.PrettyCounter(grandTotalKeys),
					buildPhase1Prefix(fileIdx, fileTotal, processedKeys+n, grandTotalKeys)))
			default:
			}
		}
		if len(batch.pairs) == 0 {
			return nil
		}
		return send()
	})

	goWorkers(g, workers, func() error {
		wat := at.a.BeginFilesRo()
		defer wat.Close()
		vals := &legacyFileValues{accounts: wat.d[kv.AccountsDomain], storage: wat.d[kv.StorageDomain], maxStep: stepTo - 1}
		conv := v3.NewLegacyConverter(vals, st.keysV2, incremental)
		delta := &legacyDelta{commitment: wat.d[kv.CommitmentDomain], before: stepFrom - 1, after: stepTo - 1, keysV2: st.keysV2}
		reach := &legacyReach{keysV2: st.keysV2, read: func(key []byte) ([]byte, error) { return delta.read(key, delta.after) }}
		delta.reachBefore = &legacyReach{keysV2: st.keysV2, read: func(key []byte) ([]byte, error) { return delta.read(key, delta.before) }}
		sink := &convertedSink{ctx: gctx, out: out, res: getConvertedBatch()}
		for batch := range in {
			for _, p := range batch.pairs {
				if convErr := convertLegacyPairV3(p[0], p[1], incremental, conv, reach, delta, sink.emit); convErr != nil {
					return convErr
				}
			}
			kvBatchPool.Put(batch)
			if sendErr := sink.send(); sendErr != nil {
				return sendErr
			}
		}
		return nil
	}, func() { close(out) })
	collectConverted(g, out, collectors)
	if err = g.Wait(); err != nil {
		return 0, read.Load(), fmt.Errorf("convertCommitmentFileV3 %q: %w", file.Fullpath(), err)
	}
	ki = read.Load()
	collected := time.Now()

	valuesPath := d.kvNewFilePathIn(dstDir, stepFrom, stepTo)
	valuesComp, err := seg.NewCompressor(ctx, d.FilenameBase+".domain.convert", valuesPath, d.dirs.Tmp, d.CompressCfg, log.LvlTrace, d.logger)
	if err != nil {
		return 0, ki, fmt.Errorf("convertCommitmentFileV3 %q: compressor: %w", file.Fullpath(), err)
	}
	collation := Collation{valuesComp: valuesComp, valuesPath: valuesPath}
	closeCollation := true
	defer func() {
		if closeCollation {
			collation.Close()
		}
	}()
	writer := seg.NewWriter(valuesComp, srcCompression)
	var written uint64
	err = mergeLoadGroups(ctx, "commitment_convert_v3", collectors, func(groupKey []byte, group []v3.LegacyEntry) error {
		value, write, resolveErr := v3.ResolveLegacy(groupKey, group)
		if resolveErr != nil || !write {
			return resolveErr
		}
		written++
		if _, writeErr := writer.Write(groupKey); writeErr != nil {
			return writeErr
		}
		_, writeErr := writer.Write(value)
		return writeErr
	})
	if err != nil {
		return 0, ki, fmt.Errorf("convertCommitmentFileV3 %q: resolve: %w", file.Fullpath(), err)
	}
	collation.valuesCount = valuesComp.Count() / 2
	resolved := time.Now()

	legacyAccessors := d.Accessors
	d.Accessors = statecfg.CommitmentV3Accessors
	closeCollation = false
	static, err := d.buildFileRange(ctx, stepFrom, stepTo, collation, background.NewProgressSet(), dstDir)
	d.Accessors = legacyAccessors
	if err != nil {
		return 0, ki, fmt.Errorf("convertCommitmentFileV3 %q: build: %w", file.Fullpath(), err)
	}
	static.CleanupOnError()

	delta, pct, err := commitmentFileSizeDelta(file.Fullpath(), valuesPath)
	if err != nil {
		return 0, ki, fmt.Errorf("convertCommitmentFileV3 %q: size delta: %w", file.Fullpath(), err)
	}
	elapsed := time.Since(fileStart)
	logger.Info(fmt.Sprintf(
		"[commitment_convert] v3 file done %s legacy=%s records=%s sizeDelta=%.1f%% in %s (%s key/s) collect=%s write=%s build=%s %s",
		baseName, common.PrettyCounter(ki), common.PrettyCounter(written), pct,
		elapsed.Round(time.Millisecond), formatRate(ki, elapsed),
		collected.Sub(fileStart).Round(time.Millisecond), resolved.Sub(collected).Round(time.Millisecond), time.Since(resolved).Round(time.Millisecond),
		buildPhase1Prefix(fileIdx, fileTotal, processedKeys+ki, grandTotalKeys)))
	return delta, ki, nil
}

func readOwned(reader *seg.Reader, batch *kvBatch, compressed bool) []byte {
	if !compressed {
		word, _ := reader.Next(nil)
		return word
	}
	start := len(batch.buf)
	batch.buf, _ = reader.Next(batch.buf)
	return batch.buf[start:]
}

func convertLegacyPairV3(k, v []byte, incremental bool, conv *v3.LegacyConverter, reach *legacyReach, delta *legacyDelta, emit v3.LegacyEmitFunc) error {
	if commitment.IsCommitmentStateKey(k) {
		if !bytes.Equal(k, commitment.KeyCommitmentState) {
			return fmt.Errorf("unexpected state key %x in a legacy file", k)
		}
		state, err := v3.ConvertLegacyState(v)
		if err != nil {
			return err
		}
		var prev []byte
		if incremental {
			if prev, err = delta.read(k, delta.before); err != nil {
				return err
			}
		}
		if err := emit(commitment.KeyCommitmentV3State, state, v3.LegacyDirect); err != nil {
			return err
		}
		return conv.EmitRootExtension(v, prev, emit)
	}
	live, err := reach.reachable(k, v)
	if err != nil {
		return err
	}
	if !incremental {
		if !live {
			return nil
		}
		return conv.Convert(k, v, nil, emit)
	}
	prev, err := delta.read(k, delta.before)
	if err != nil {
		return err
	}
	if !live {
		if err := conv.Tombstone(k, emit); err != nil {
			return err
		}
	} else if err := conv.Convert(k, v, prev, emit); err != nil {
		return err
	}
	return delta.children(k, prev, v, live, conv, reach, emit)
}

type legacyDelta struct {
	commitment         *DomainRoTx
	before, after      kv.Step
	keysV2             bool
	reachBefore        *legacyReach
	stack, kids, prevs []legacyChild
}

func (w *legacyDelta) read(key []byte, step kv.Step) ([]byte, error) {
	v, _, _, _, err := w.commitment.getLatestFromFiles(key, nil, step)
	return v, err
}

func (w *legacyDelta) children(k, prev, v []byte, live bool, conv *v3.LegacyConverter, reach *legacyReach, emit v3.LegacyEmitFunc) error {
	prefix, err := decodeLegacyPrefix(k, w.keysV2)
	if err != nil {
		return err
	}
	if w.prevs, err = appendLegacyChildren(w.prevs[:0], prefix, prev); err != nil {
		return err
	}
	w.kids = w.kids[:0]
	if live {
		if w.kids, err = appendLegacyChildren(w.kids, prefix, v); err != nil {
			return err
		}
	}
	liveBefore := false
	if len(w.prevs) != 0 {
		if liveBefore, err = w.reachBefore.reachable(k, prev); err != nil {
			return err
		}
	}
	for _, c := range w.kids {
		if liveBefore && slices.ContainsFunc(w.prevs, func(p legacyChild) bool { return p.hash == c.hash && bytes.Equal(p.path, c.path) }) {
			continue
		}
		if err := w.reemit(c.path, conv, emit); err != nil {
			return err
		}
	}
	for _, p := range w.prevs {
		if slices.ContainsFunc(w.kids, func(c legacyChild) bool { return bytes.Equal(p.path, c.path) }) {
			continue
		}
		if err := w.drop(p.path, conv, reach, emit); err != nil {
			return err
		}
	}
	return nil
}

func (w *legacyDelta) walk(path []byte, visit func(key, value, old []byte) (descend bool, err error)) error {
	w.stack = append(w.stack[:0], legacyChild{path: path})
	for len(w.stack) > 0 {
		path := w.stack[len(w.stack)-1].path
		w.stack = w.stack[:len(w.stack)-1]
		key := encodeLegacyPrefix(path, w.keysV2)
		value, err := w.read(key, w.after)
		if err != nil {
			return err
		}
		old, err := w.read(key, w.before)
		if err != nil {
			return err
		}
		if len(value) == 0 || !bytes.Equal(value, old) {
			continue
		}
		descend, err := visit(key, value, old)
		if err != nil {
			return err
		}
		if descend {
			if w.stack, err = appendLegacyChildren(w.stack, path, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *legacyDelta) reemit(path []byte, conv *v3.LegacyConverter, emit v3.LegacyEmitFunc) error {
	return w.walk(path, func(key, value, old []byte) (bool, error) {
		if err := conv.Convert(key, value, old, emit); err != nil {
			return false, err
		}
		liveBefore, err := w.reachBefore.reachable(key, old)
		return !liveBefore, err
	})
}

func (w *legacyDelta) drop(path []byte, conv *v3.LegacyConverter, reach *legacyReach, emit v3.LegacyEmitFunc) error {
	return w.walk(path, func(key, value, _ []byte) (bool, error) {
		live, err := reach.reachable(key, value)
		if err != nil || live {
			return false, err
		}
		if err := conv.Tombstone(key, emit); err != nil {
			return false, err
		}
		return true, nil
	})
}

type legacyReach struct {
	read   func(key []byte) ([]byte, error)
	keysV2 bool
	path   []byte
	chain  []legacyReachNode
	hashed [64]byte

	rootExt []byte
}

type legacyReachNode struct {
	depth int
	value []byte
}

func (r *legacyReach) reachable(legacyKey, value []byte) (bool, error) {
	if commitment.BranchData(value).ChildCount() == 0 {
		return true, nil
	}
	path, err := decodeLegacyPrefix(legacyKey, r.keysV2)
	if err != nil || len(path) == 0 {
		return err == nil, err
	}
	common := 0
	for common < len(path) && common < len(r.path) && path[common] == r.path[common] {
		common++
	}
	keep := 0
	for keep < len(r.chain) && r.chain[keep].depth <= common {
		keep++
	}
	r.chain = r.chain[:keep]
	r.path = append(r.path[:0], path...)
	if len(r.chain) == 0 {
		root, err := r.read(encodeLegacyPrefix(nil, r.keysV2))
		if err != nil {
			return false, err
		}
		r.chain = append(r.chain, legacyReachNode{0, bytes.Clone(root)})
		r.rootExt = nil
		if commitment.BranchData(root).ChildCount() == 0 {
			state, err := r.read(commitment.KeyCommitmentState)
			if err != nil || len(state) == 0 {
				return false, err
			}
			if r.rootExt, _, err = v3.LegacyRootExtension(state); err != nil {
				return false, err
			}
		}
	}
	for {
		n := r.chain[len(r.chain)-1]
		if n.depth == len(path) {
			return true, nil
		}
		var next int
		var ok bool
		var err error
		if n.depth == 0 && len(r.rootExt) != 0 {
			next, ok = len(r.rootExt), bytes.HasPrefix(path, r.rootExt)
		} else {
			next, ok, err = r.childDepth(n.value, path, n.depth)
		}
		if err != nil || !ok || next > len(path) {
			return false, err
		}
		if next == len(path) {
			r.chain = append(r.chain, legacyReachNode{next, bytes.Clone(value)})
			return true, nil
		}
		child, err := r.read(encodeLegacyPrefix(path[:next], r.keysV2))
		if err != nil || commitment.BranchData(child).ChildCount() == 0 {
			return false, err
		}
		r.chain = append(r.chain, legacyReachNode{next, bytes.Clone(child)})
	}
}

func (r *legacyReach) childDepth(branch, path []byte, depth int) (next int, ok bool, err error) {
	if len(branch) < 4 {
		return 0, false, nil
	}
	nib := int(path[depth])
	err = commitment.BranchData(branch).ForEachCell(func(i int, cell commitment.BranchCell) error {
		if i != nib {
			return nil
		}
		ext, from := cell.Extension, depth+1
		switch {
		case len(cell.AccountAddr) != 0:
			if len(path) < 64 || len(cell.StorageAddr) != 0 || len(cell.Hash) != length.Hash {
				return nil
			}
			h := keccak.Sum256(cell.AccountAddr)
			nibbles.Expand(h[:], r.hashed[:])
			if !bytes.Equal(r.hashed[:], path[:64]) {
				return nil
			}
			from = 64
		case len(cell.StorageAddr) != 0 || len(cell.Hash) != length.Hash:
			return nil
		}
		end := min(from+len(ext), len(path))
		next, ok = from+len(ext), bytes.Equal(ext[:end-from], path[from:end])
		return nil
	})
	return next, ok, err
}

type legacyFileValues struct {
	accounts, storage *DomainRoTx
	maxStep           kv.Step
}

func (v *legacyFileValues) Account(plainKey []byte) ([]byte, error) {
	enc, _, _, _, err := v.accounts.getLatestFromFiles(plainKey, nil, v.maxStep)
	return enc, err
}

func (v *legacyFileValues) Storage(plainKey []byte) ([]byte, error) {
	enc, _, _, _, err := v.storage.getLatestFromFiles(plainKey, nil, v.maxStep)
	return enc, err
}

type hashedBatch struct {
	buf     []byte
	records []hashedRecord
	expects []hashedExpect
}

type hashedRecord struct {
	k0, k1, e0, e1 int
	hash           [32]byte
}

type hashedExpect struct {
	k0, k1 int
	hash   [32]byte
}

func (b *hashedBatch) expect(key, hash []byte) error {
	k0 := len(b.buf)
	b.buf = append(b.buf, key...)
	e := hashedExpect{k0: k0, k1: len(b.buf)}
	copy(e.hash[:], hash)
	b.expects = append(b.expects, e)
	return nil
}

var hashedBatchPool = sync.Pool{New: func() any { return &hashedBatch{} }}

func newHashedBatch() *hashedBatch {
	b := hashedBatchPool.Get().(*hashedBatch)
	b.buf, b.records, b.expects = b.buf[:0], b.records[:0], b.expects[:0]
	return b
}

const commitmentV3VerifyGroup = 4

func VerifyCommitmentV3(ctx context.Context, a *Aggregator, asOf []uint64, history bool, logger log.Logger) error {
	started := time.Now()
	if (history || len(asOf) > 0) && statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Hist.HistoryDisabled {
		return errors.New("commitment history is disabled")
	}
	points := append([]uint64{math.MaxUint64}, asOf...)
	if history {
		samples, err := commitmentV3HistorySamples(a)
		if err != nil {
			return err
		}
		points = append(points, samples...)
	}
	defer a.MadvNormal().DisableReadAhead()
	groups := (len(points) + commitmentV3VerifyGroup - 1) / commitmentV3VerifyGroup
	logged := time.Now()
	for gi := range groups {
		group := points[gi*commitmentV3VerifyGroup : min((gi+1)*commitmentV3VerifyGroup, len(points))]
		groupStarted := time.Now()
		checks, err := FoldCommitmentV3(ctx, a, group, func(done, total int) {
			if time.Since(logged) < 30*time.Second {
				return
			}
			logged = time.Now()
			logger.Info("[commitment verify] progress", "group", fmt.Sprintf("%d/%d", gi+1, groups), "ranges", fmt.Sprintf("%d/%d", done, total),
				"progress", fmt.Sprintf("%.1f%%", 100*(float64(gi)+float64(done)/float64(total))/float64(groups)), "elapsed", time.Since(started).Round(time.Second))
		})
		if err != nil {
			return err
		}
		for i, c := range checks {
			if group[i] != math.MaxUint64 && c.TxNum >= group[i] {
				return fmt.Errorf("%s: no commitment history covers txNum %d", asOfLabel(group[i]), group[i])
			}
			logger.Info("[commitment verify] ok", "asOf", asOfLabel(group[i]), "block", c.BlockNum, "txNum", c.TxNum, "root", fmt.Sprintf("%x", c.Root),
				"records", common.PrettyCounter(c.Records), "took", time.Since(groupStarted).Round(time.Second), "done", fmt.Sprintf("%d/%d", gi*commitmentV3VerifyGroup+i+1, len(points)))
		}
	}
	logger.Info("[commitment verify] done", "points", len(points), "history", history, "took", time.Since(started).Round(time.Millisecond))
	return nil
}

type CommitmentV3Check struct {
	Root                     []byte
	BlockNum, TxNum, Records uint64
}

var (
	commitmentV3FoldRanges  uint64 = 8192
	commitmentV3FoldChunk          = 4096
	commitmentV3RangeChunks        = 64
)

type foldChunk struct {
	batch *hashedBatch
	state []byte
}

func FoldCommitmentV3(ctx context.Context, a *Aggregator, asOf []uint64, progress func(done, total int)) ([]CommitmentV3Check, error) {
	at := a.BeginFilesRo()
	ranges := commitmentV3KeyRanges(at.d[kv.CommitmentDomain], commitmentV3FoldRanges)
	at.Close()
	workers := max(1, runtime.GOMAXPROCS(0))
	type task struct {
		r      [2][]byte
		chunks []chan foldChunk
	}
	in := make(chan task)
	orders := make([]chan chan foldChunk, len(asOf))
	for j := range orders {
		orders[j] = make(chan chan foldChunk, workers*2)
	}
	states := make([][]byte, len(asOf))
	folds := make([]CommitmentV3Check, len(asOf))
	orphans := make([]uint64, len(asOf))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer func() {
			close(in)
			for _, o := range orders {
				close(o)
			}
		}()
		for _, r := range ranges {
			t := task{r: r, chunks: make([]chan foldChunk, len(asOf))}
			for j := range asOf {
				t.chunks[j] = make(chan foldChunk, commitmentV3RangeChunks)
				select {
				case orders[j] <- t.chunks[j]:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			select {
			case in <- t:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for range workers {
		g.Go(func() error {
			at := a.BeginFilesRo()
			defer at.Close()
			dt := at.d[kv.CommitmentDomain]
			hasher := v3.NewRecordHasher()
			for t := range in {
				for j, txNum := range asOf {
					err := foldCommitmentV3Range(gctx, dt, txNum, t.r, hasher, func(c foldChunk) error {
						select {
						case t.chunks[j] <- c:
							return nil
						case <-gctx.Done():
							return gctx.Err()
						}
					})
					close(t.chunks[j])
					if err != nil {
						return fmt.Errorf("%s: %w", asOfLabel(txNum), err)
					}
				}
			}
			return nil
		})
	}
	for j, txNum := range asOf {
		g.Go(func() error {
			m := v3.NewRecordMatcher()
			done := 0
			for chunks := range orders[j] {
				for {
					var c foldChunk
					var ok bool
					select {
					case c, ok = <-chunks:
					case <-gctx.Done():
						return gctx.Err()
					}
					if !ok {
						break
					}
					if c.state != nil {
						states[j] = c.state
					}
					b := c.batch
					for _, r := range b.records {
						for _, e := range b.expects[r.e0:r.e1] {
							if err := m.Expect(b.buf[e.k0:e.k1], e.hash[:]); err != nil {
								return fmt.Errorf("%s: %w", asOfLabel(txNum), err)
							}
						}
						if err := m.Record(b.buf[r.k0:r.k1], r.hash); err != nil {
							return fmt.Errorf("%s: %w", asOfLabel(txNum), err)
						}
					}
					hashedBatchPool.Put(b)
				}
				done++
				if j == 0 && progress != nil {
					progress(done, len(ranges))
				}
			}
			root, records, n, err := m.Finish()
			if err != nil {
				return fmt.Errorf("%s: %w", asOfLabel(txNum), err)
			}
			folds[j], orphans[j] = CommitmentV3Check{Root: root[:], Records: records}, n
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	for j, txNum := range asOf {
		c := &folds[j]
		var stateRoot []byte
		var err error
		if c.BlockNum, c.TxNum, stateRoot, err = commitment.DecodeCommitmentV3State(states[j]); err != nil {
			return nil, fmt.Errorf("%s: state %x: %w", asOfLabel(txNum), states[j], err)
		}
		if !bytes.Equal(c.Root, stateRoot) {
			return nil, fmt.Errorf("%s: records fold to root %x, state at block %d records %x", asOfLabel(txNum), c.Root, c.BlockNum, stateRoot)
		}
		if orphans[j] != 0 {
			return nil, fmt.Errorf("%s: %d orphan records (block %d, %d records)", asOfLabel(txNum), orphans[j], c.BlockNum, c.Records)
		}
	}
	return folds, nil
}

func asOfLabel(txNum uint64) string {
	if txNum == math.MaxUint64 {
		return "as of latest"
	}
	return fmt.Sprintf("as of txNum %d", txNum)
}

func commitmentV3KeyRanges(dt *DomainRoTx, n uint64) [][2][]byte {
	var bt *btindex.BtIndex
	var dec *seg.Decompressor
	for _, f := range dt.files {
		if b := f.src.bindex; b != nil && (bt == nil || b.KeyCount() > bt.KeyCount()) {
			bt, dec = b, f.src.decompressor
		}
	}
	var ranges [][2][]byte
	var from []byte
	if bt != nil && bt.KeyCount() > n {
		r := dt.dataReader(dec)
		for i := range n - 1 {
			c := bt.OrdinalLookup(r, (i+1)*bt.KeyCount()/n)
			if c == nil {
				continue
			}
			to := bytes.Clone(c.Key())
			c.Close()
			if bytes.Compare(to, from) <= 0 {
				continue
			}
			ranges = append(ranges, [2][]byte{from, to})
			from = to
		}
	}
	return append(ranges, [2][]byte{from, nil})
}

func foldCommitmentV3Range(ctx context.Context, dt *DomainRoTx, asOf uint64, r [2][]byte, hasher *v3.RecordHasher, send func(foldChunk) error) error {
	latest, err := dt.DebugRangeLatestFromFiles(r[0], r[1], -1)
	if err != nil {
		return err
	}
	var it stream.KV = latest
	if asOf != math.MaxUint64 {
		hist := &HistoryRangeAsOfFiles{hc: dt.ht, from: r[0], toPrefix: r[1], startTxNum: asOf, limit: kv.Unlim, orderAscend: order.Asc, ctx: ctx, logger: dt.ht.h.logger}
		if err = hist.init(dt.ht.iit.files); err != nil {
			hist.Close()
			latest.Close()
			return err
		}
		it = stream.UnionKV(hist, latest, -1)
	}
	defer it.Close()
	var state []byte
	res := newHashedBatch()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if len(v) == 0 {
			continue
		}
		if bytes.Equal(k, commitment.KeyCommitmentV3State) {
			state = bytes.Clone(v)
			continue
		}
		k0 := len(res.buf)
		res.buf = append(res.buf, k...)
		rec := hashedRecord{k0: k0, k1: len(res.buf), e0: len(res.expects)}
		if rec.hash, err = hasher.Hash(k, v, res.expect); err != nil {
			return err
		}
		rec.e1 = len(res.expects)
		res.records = append(res.records, rec)
		if len(res.records) >= commitmentV3FoldChunk {
			if err := send(foldChunk{batch: res}); err != nil {
				return err
			}
			res = newHashedBatch()
		}
	}
	return send(foldChunk{batch: res, state: state})
}
