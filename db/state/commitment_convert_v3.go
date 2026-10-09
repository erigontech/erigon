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
	"sync"
	"sync/atomic"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

const commitmentV3Batch = 1024

type kvBatch struct {
	seq      uint64
	buf      []byte
	pairs    [][2][]byte
	shadowed []bool
}

func (b *kvBatch) reset() {
	b.buf, b.pairs, b.shadowed = b.buf[:0], b.pairs[:0], b.shadowed[:0]
}

func (b *kvBatch) own(word []byte) []byte {
	start := len(b.buf)
	b.buf = append(b.buf, word...)
	return b.buf[start:]
}

const commitmentV3Collectors = 4

type convertedBatch struct {
	buf  []byte
	ents [][3]int
}

func (b *convertedBatch) emit(k, v []byte, kind v3.LegacyKind) error {
	k0 := len(b.buf)
	b.buf = append(b.buf, k...)
	v0 := len(b.buf)
	b.buf = append(append(b.buf, byte(kind)), v...)
	b.ents = append(b.ents, [3]int{k0, v0, len(b.buf)})
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
	if !file.Version().Less(version.V3_0) {
		return 0, 0, errSkip
	}
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
	dt := at.d[kv.CommitmentDomain]
	d := dt.d
	srcCompression := commitmentFileCompression(d, vf, stepSize)
	lastStep := kv.Step(dt.files[len(dt.files)-1].endTxNum / stepSize)

	baseName := filepath.Base(file.Fullpath())
	fileStart := time.Now()
	workers := max(1, runtime.GOMAXPROCS(0))
	collectors, closeCollectors := newV3Collectors("commitment_convert_v3", at.Dirs().Tmp, workers, logger)
	defer closeCollectors()
	in := make(chan *kvBatch, workers*2)
	out := make(chan *convertedBatch, workers*2)
	var read atomic.Uint64
	var shadowed uint64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(in)
		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		reader := seg.NewReader(vf.src.decompressor.MakeGetter(), srcCompression)
		keysCompressed, valsCompressed := srcCompression.Has(seg.CompressKeys), srcCompression.Has(seg.CompressVals)
		newer := newNewerCommitmentKeys(dt, endTxNum, stepSize)
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
			isShadowed := !commitment.IsCommitmentStateKey(k) && newer.contains(k)
			if isShadowed {
				shadowed++
			}
			v := readOwned(reader, batch, valsCompressed)
			batch.pairs = append(batch.pairs, [2][]byte{k, v})
			batch.shadowed = append(batch.shadowed, isShadowed)
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

	var converting sync.WaitGroup
	for range workers {
		converting.Add(1)
		g.Go(func() error {
			defer converting.Done()
			wat := at.a.BeginFilesRo()
			defer wat.Close()
			vals := &legacyFileValues{accounts: wat.d[kv.AccountsDomain], storage: wat.d[kv.StorageDomain], maxStep: lastStep - 1}
			conv := v3.NewLegacyConverter(vals, st.keysV2, incremental)
			prevs := wat.d[kv.CommitmentDomain]
			reach := &legacyReach{dt: prevs, maxStep: lastStep - 1, keysV2: st.keysV2}
			for batch := range in {
				res := convertedBatchPool.Get().(*convertedBatch)
				res.buf, res.ents = res.buf[:0], res.ents[:0]
				for i, p := range batch.pairs {
					if convErr := convertLegacyPairV3(p[0], p[1], batch.shadowed[i], incremental, stepFrom, prevs, conv, reach, res.emit); convErr != nil {
						return convErr
					}
				}
				kvBatchPool.Put(batch)
				select {
				case out <- res:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		converting.Wait()
		close(out)
		return nil
	})
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
	compress := seg.CompressNone
	if stepTo-stepFrom > DomainMinStepsToCompress {
		compress = d.Compression
	}
	writer := seg.NewWriter(valuesComp, compress)
	var groupKey, groupVals []byte
	var group []v3.LegacyEntry
	var written uint64
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
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
	}
	err = etl.MergeLoad("commitment_convert_v3", collectors, func(k, v []byte) error {
		if len(group) == 0 || !bytes.Equal(k, groupKey) {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			groupKey, groupVals, group = append(groupKey[:0], k...), groupVals[:0], group[:0]
		}
		start := len(groupVals)
		groupVals = append(groupVals, v[1:]...)
		group = append(group, v3.LegacyEntry{Kind: v3.LegacyKind(v[0]), Value: groupVals[start:]})
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})
	if err == nil {
		err = flush()
	}
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
		"[commitment_convert] v3 file done %s legacy=%s shadowed=%s records=%s sizeDelta=%.1f%% in %s (%s key/s) collect=%s write=%s build=%s %s",
		baseName, common.PrettyCounter(ki), common.PrettyCounter(shadowed), common.PrettyCounter(written), pct,
		elapsed.Round(time.Millisecond), formatRate(ki, elapsed),
		collected.Sub(fileStart).Round(time.Millisecond), resolved.Sub(collected).Round(time.Millisecond), time.Since(resolved).Round(time.Millisecond),
		buildPhase1Prefix(fileIdx, fileTotal, processedKeys+ki, grandTotalKeys)))
	return delta, ki, nil
}

func commitmentFileCompression(d *Domain, f visibleFile, stepSize uint64) seg.FileCompression {
	if f.src.StepCount(stepSize) < DomainMinStepsToCompress {
		return seg.CompressNone
	}
	return d.Compression
}

type newerCommitmentKeys struct {
	readers []*seg.Reader
	keys    [][]byte
}

func newNewerCommitmentKeys(dt *DomainRoTx, fromTxNum, stepSize uint64) *newerCommitmentKeys {
	s := &newerCommitmentKeys{}
	for _, f := range dt.files {
		if f.startTxNum < fromTxNum {
			continue
		}
		s.readers = append(s.readers, seg.NewReader(f.src.decompressor.MakeGetter(), commitmentFileCompression(dt.d, f, stepSize)))
		s.keys = append(s.keys, nil)
		s.advance(len(s.readers) - 1)
	}
	return s
}

func (s *newerCommitmentKeys) advance(i int) {
	if !s.readers[i].HasNext() {
		s.readers[i] = nil
		return
	}
	s.keys[i], _ = s.readers[i].Next(s.keys[i][:0])
	s.readers[i].Skip()
}

func (s *newerCommitmentKeys) contains(k []byte) bool {
	for i := range s.readers {
		for s.readers[i] != nil && bytes.Compare(s.keys[i], k) < 0 {
			s.advance(i)
		}
		if s.readers[i] != nil && bytes.Equal(s.keys[i], k) {
			return true
		}
	}
	return false
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

func convertLegacyPairV3(k, v []byte, shadowed, incremental bool, stepFrom kv.Step, prevs *DomainRoTx, conv *v3.LegacyConverter, reach *legacyReach, emit v3.LegacyEmitFunc) error {
	if commitment.IsCommitmentStateKey(k) {
		if !bytes.Equal(k, commitment.KeyCommitmentState) {
			return fmt.Errorf("unexpected state key %x in a legacy file", k)
		}
		state, err := v3.ConvertLegacyState(v)
		if err != nil {
			return err
		}
		return emit(commitment.KeyCommitmentV3State, state, v3.LegacyDirect)
	}
	if shadowed {
		return conv.ConvertShadowed(k, v, emit)
	}
	if len(v) >= 4 && binary.BigEndian.Uint16(v[2:4]) != 0 {
		if live, err := reach.reachable(k, v); err != nil || !live {
			return err
		}
	}
	var prev []byte
	if incremental {
		var err error
		if prev, _, _, _, err = prevs.getLatestFromFiles(k, nil, stepFrom-1); err != nil {
			return err
		}
	}
	return conv.Convert(k, v, prev, emit)
}

type legacyReach struct {
	dt      *DomainRoTx
	maxStep kv.Step
	keysV2  bool
	path    []byte
	chain   []legacyReachNode
	hashed  [64]byte
}

type legacyReachNode struct {
	depth int
	value []byte
}

func (r *legacyReach) reachable(legacyKey, value []byte) (bool, error) {
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
		root, _, _, _, err := r.dt.getLatestFromFiles(encodeLegacyPrefix(nil, r.keysV2), nil, r.maxStep)
		if err != nil {
			return false, err
		}
		r.chain = append(r.chain, legacyReachNode{0, bytes.Clone(root)})
	}
	for {
		n := r.chain[len(r.chain)-1]
		if n.depth == len(path) {
			return true, nil
		}
		next, ok, err := r.childDepth(n.value, path, n.depth)
		if err != nil || !ok || next > len(path) {
			return false, err
		}
		if next == len(path) {
			r.chain = append(r.chain, legacyReachNode{next, bytes.Clone(value)})
			return true, nil
		}
		child, _, _, _, err := r.dt.getLatestFromFiles(encodeLegacyPrefix(path[:next], r.keysV2), nil, r.maxStep)
		if err != nil || len(child) < 4 || binary.BigEndian.Uint16(child[2:4]) == 0 {
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
	seq     uint64
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

func verifyCommitmentV3Files(ctx context.Context, a *Aggregator, logger log.Logger) error {
	started := time.Now()
	c, err := FoldCommitmentV3(ctx, a, math.MaxUint64)
	if err != nil {
		return err
	}
	logger.Info("[commitment_convert] v3 records verified", "root", fmt.Sprintf("%x", c.Root), "block", c.BlockNum, "txNum", c.TxNum,
		"records", common.PrettyCounter(c.Records), "orphans", common.PrettyCounter(c.Orphans), "took", time.Since(started).Round(time.Millisecond))
	return nil
}

func DebugCommitmentV3RootAsOf(ctx context.Context, a *Aggregator, txNum uint64) ([]byte, error) {
	c, err := FoldCommitmentV3(ctx, a, txNum)
	return c.Root, err
}

type CommitmentV3Check struct {
	Root                              []byte
	BlockNum, TxNum, Records, Orphans uint64
}

func FoldCommitmentV3(ctx context.Context, a *Aggregator, asOfTxNum uint64) (CommitmentV3Check, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	dt := at.d[kv.CommitmentDomain]
	latest, err := dt.DebugRangeLatestFromFiles(nil, nil, -1)
	if err != nil {
		return CommitmentV3Check{}, err
	}
	var it stream.KV = latest
	if asOfTxNum != math.MaxUint64 {
		hist := &HistoryRangeAsOfFiles{hc: dt.ht, startTxNum: asOfTxNum, limit: kv.Unlim, orderAscend: order.Asc, ctx: ctx, logger: dt.ht.h.logger}
		if err = hist.init(dt.ht.iit.files); err != nil {
			hist.Close()
			latest.Close()
			return CommitmentV3Check{}, err
		}
		it = stream.UnionKV(hist, latest, -1)
	}
	defer it.Close()
	f, err := foldCommitmentV3Records(ctx, it)
	if err != nil {
		return CommitmentV3Check{}, err
	}
	blockNum, txNum, err := f.checkState()
	if err != nil {
		return CommitmentV3Check{}, err
	}
	return CommitmentV3Check{Root: f.root[:], BlockNum: blockNum, TxNum: txNum, Records: f.records, Orphans: f.orphans}, nil
}

type commitmentV3Fold struct {
	root             [32]byte
	state            []byte
	records, orphans uint64
}

func (f commitmentV3Fold) checkState() (blockNum, txNum uint64, err error) {
	blockNum, txNum, stateRoot, err := commitment.DecodeCommitmentV3State(f.state)
	if err != nil {
		return 0, 0, fmt.Errorf("state %x: %w", f.state, err)
	}
	if !bytes.Equal(f.root[:], stateRoot) {
		return 0, 0, fmt.Errorf("records fold to root %x, state at block %d records %x", f.root, blockNum, stateRoot)
	}
	return blockNum, txNum, nil
}

func foldCommitmentV3Records(ctx context.Context, it stream.KV) (commitmentV3Fold, error) {
	workers := max(1, runtime.GOMAXPROCS(0))
	in := make(chan *kvBatch, workers*2)
	out := make(chan *hashedBatch, workers*2)
	var state []byte
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(in)
		var seq uint64
		batch := kvBatchPool.Get().(*kvBatch)
		batch.reset()
		send := func() error {
			batch.seq = seq
			seq++
			select {
			case in <- batch:
				batch = kvBatchPool.Get().(*kvBatch)
				batch.reset()
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		for it.HasNext() {
			k, v, nextErr := it.Next()
			if nextErr != nil {
				return nextErr
			}
			if len(v) == 0 {
				continue
			}
			if bytes.Equal(k, commitment.KeyCommitmentV3State) {
				state = bytes.Clone(v)
				continue
			}
			batch.pairs = append(batch.pairs, [2][]byte{batch.own(k), batch.own(v)})
			if len(batch.pairs) == commitmentV3Batch {
				if sendErr := send(); sendErr != nil {
					return sendErr
				}
			}
		}
		if len(batch.pairs) == 0 {
			return nil
		}
		return send()
	})
	var hashing sync.WaitGroup
	for range workers {
		hashing.Add(1)
		g.Go(func() error {
			defer hashing.Done()
			hasher := v3.NewRecordHasher()
			for batch := range in {
				res := hashedBatchPool.Get().(*hashedBatch)
				res.seq, res.buf, res.records, res.expects = batch.seq, res.buf[:0], res.records[:0], res.expects[:0]
				for _, p := range batch.pairs {
					k0 := len(res.buf)
					res.buf = append(res.buf, p[0]...)
					r := hashedRecord{k0: k0, k1: len(res.buf), e0: len(res.expects)}
					hash, hashErr := hasher.Hash(p[0], p[1], res.expect)
					if hashErr != nil {
						return hashErr
					}
					r.hash, r.e1 = hash, len(res.expects)
					res.records = append(res.records, r)
				}
				kvBatchPool.Put(batch)
				select {
				case out <- res:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		hashing.Wait()
		close(out)
		return nil
	})
	matcher := v3.NewRecordMatcher()
	g.Go(func() error {
		waiting := map[uint64]*hashedBatch{}
		var next uint64
		for res := range out {
			waiting[res.seq] = res
			for b, ok := waiting[next]; ok; b, ok = waiting[next] {
				delete(waiting, next)
				next++
				for _, r := range b.records {
					for _, e := range b.expects[r.e0:r.e1] {
						if matchErr := matcher.Expect(b.buf[e.k0:e.k1], e.hash[:]); matchErr != nil {
							return matchErr
						}
					}
					if matchErr := matcher.Record(b.buf[r.k0:r.k1], r.hash); matchErr != nil {
						return matchErr
					}
				}
				hashedBatchPool.Put(b)
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return commitmentV3Fold{}, err
	}
	root, records, orphans, err := matcher.Finish()
	if err != nil {
		return commitmentV3Fold{}, err
	}
	return commitmentV3Fold{root: root, state: state, records: records, orphans: orphans}, nil
}
