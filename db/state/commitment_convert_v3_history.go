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
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
)

const commitmentV3HistoryBatch = 256

type commitmentV3HistoryStage struct {
	history, idx, accessor string
}

func newCommitmentV3HistoryStage(dirs datadir.Dirs) commitmentV3HistoryStage {
	root := filepath.Join(dirs.Snap, "rebuild")
	return commitmentV3HistoryStage{
		history:  filepath.Join(root, "history"),
		idx:      filepath.Join(root, "idx"),
		accessor: filepath.Join(root, "accessor"),
	}
}

type commitmentHistoryRange struct {
	fromStep, toStep kv.Step
}

type historyChainBatch struct {
	buf    []byte
	keys   [][2]int
	spans  [][2]int
	txNums []uint64
	vals   [][2]int
}

func (b *historyChainBatch) reset() {
	b.buf, b.keys, b.spans, b.txNums, b.vals = b.buf[:0], b.keys[:0], b.spans[:0], b.txNums[:0], b.vals[:0]
}

func (b *historyChainBatch) span(p []byte) [2]int {
	start := len(b.buf)
	b.buf = append(b.buf, p...)
	return [2]int{start, len(b.buf)}
}

var historyChainBatchPool = sync.Pool{New: func() any { return &historyChainBatch{} }}

type historyVersion struct {
	buf      []byte
	ents     []historyVersionEntry
	accounts []accountStorage
}

type historyVersionEntry struct {
	k0, k1, v1 int
	kind       v3.LegacyKind
}

const (
	storageNone byte = iota
	storageSingleton
	storageExtension
	storageBranch
)

type accountStorage struct {
	addrHash [32]byte
	class    byte
}

func (h *historyVersion) reset() { h.buf, h.ents, h.accounts = h.buf[:0], h.ents[:0], h.accounts[:0] }

func (h *historyVersion) parseAccounts(legacyValue []byte) error {
	if len(legacyValue) < 4 || binary.BigEndian.Uint16(legacyValue[2:4]) == 0 {
		return nil
	}
	return commitment.BranchData(legacyValue).ForEachCell(func(_ int, c commitment.BranchCell) error {
		if len(c.AccountAddr) == 0 {
			return nil
		}
		a := accountStorage{addrHash: crypto.Keccak256Hash(c.AccountAddr)}
		switch {
		case len(c.StorageAddr) != 0:
			a.class = storageSingleton
		case len(c.Extension) != 0:
			a.class = storageExtension
		case len(c.Hash) == length.Hash:
			a.class = storageBranch
		}
		h.accounts = append(h.accounts, a)
		return nil
	})
}

func (h *historyVersion) account(addrHash [32]byte) (accountStorage, bool) {
	for _, a := range h.accounts {
		if a.addrHash == addrHash {
			return a, true
		}
	}
	return accountStorage{}, false
}

func (h *historyVersion) index(key []byte) int {
	return slices.IndexFunc(h.ents, func(e historyVersionEntry) bool { return bytes.Equal(h.key(e), key) })
}

func (h *historyVersion) hasValue(key, value []byte) bool {
	i := h.index(key)
	return i >= 0 && bytes.Equal(h.value(h.ents[i]), value)
}

func (h *historyVersion) valueOf(key []byte) []byte {
	if i := h.index(key); i >= 0 {
		return h.value(h.ents[i])
	}
	return nil
}

func (h *historyVersion) add(k, v []byte, kind v3.LegacyKind) error {
	k0 := len(h.buf)
	h.buf = append(append(h.buf, k...), v...)
	h.ents = append(h.ents, historyVersionEntry{k0: k0, k1: k0 + len(k), v1: len(h.buf), kind: kind})
	return nil
}

func (h *historyVersion) key(e historyVersionEntry) []byte   { return h.buf[e.k0:e.k1] }
func (h *historyVersion) value(e historyVersionEntry) []byte { return h.buf[e.k1:e.v1] }

type legacyHistoryValues struct {
	accounts, storage *DomainRoTx
	asOf              uint64
	cache             map[string]historyValue
}

type historyValue struct {
	value       []byte
	from, until uint64
}

func (v *legacyHistoryValues) Account(plainKey []byte) ([]byte, error) {
	return v.lookup(v.accounts, plainKey)
}

func (v *legacyHistoryValues) Storage(plainKey []byte) ([]byte, error) {
	return v.lookup(v.storage, plainKey)
}

func (v *legacyHistoryValues) lookup(dt *DomainRoTx, plainKey []byte) ([]byte, error) {
	if c, ok := v.cache[string(plainKey)]; ok && c.from <= v.asOf && v.asOf <= c.until {
		return c.value, nil
	}
	found, nextWrite, err := dt.ht.iit.seekInFiles(plainKey, v.asOf)
	if err != nil {
		return nil, err
	}
	var value []byte
	until := uint64(1<<64 - 1)
	if found {
		if value, _, err = dt.ht.historyValueAt(plainKey, nextWrite, v.asOf); err != nil {
			return nil, err
		}
		until = nextWrite
	} else if value, _, _, _, err = dt.getLatestFromFiles(plainKey, nil, kv.NoStepBound); err != nil {
		return nil, err
	}
	if len(v.cache) >= 1<<20 {
		clear(v.cache)
	}
	c := historyValue{value: bytes.Clone(value), from: v.asOf, until: until}
	v.cache[string(plainKey)] = c
	return c.value, nil
}

func appendEscapedKey(dst, key []byte) []byte {
	for _, b := range key {
		if b == 0 {
			dst = append(dst, 0, 0xff)
			continue
		}
		dst = append(dst, b)
	}
	return append(dst, 0, 0)
}

func appendUnescapedKey(dst, escaped []byte) ([]byte, error) {
	for i := 0; i < len(escaped); i++ {
		if escaped[i] != 0 {
			dst = append(dst, escaped[i])
			continue
		}
		if i+1 >= len(escaped) {
			return nil, fmt.Errorf("escaped key %x is truncated", escaped)
		}
		i++
		switch escaped[i] {
		case 0xff:
			dst = append(dst, 0)
		case 0:
			if i+1 != len(escaped) {
				return nil, fmt.Errorf("escaped key %x has bytes after its terminator", escaped)
			}
			return dst, nil
		default:
			return nil, fmt.Errorf("escaped key %x has an invalid escape", escaped)
		}
	}
	return nil, fmt.Errorf("escaped key %x has no terminator", escaped)
}

func seekFileSeq(iit *InvertedIndexRoTx, i int, key []byte, seq *multiencseq.SequenceReader) bool {
	hi, lo := iit.hashKey(key)
	offset, ok := iit.statelessIdxReader(i).TwoLayerLookupByHash(hi, lo)
	if !ok {
		return false
	}
	g := iit.statelessGetter(i)
	g.Reset(offset)
	if g.MatchCmp(key) != 0 {
		return false
	}
	encoded, _ := g.Next(nil)
	seq.Reset(iit.files[i].startTxNum, encoded)
	return true
}

func lastWriteBefore(iit *InvertedIndexRoTx, key []byte, txNum uint64) (uint64, bool, error) {
	var seq multiencseq.SequenceReader
	var it multiencseq.SequenceIterator
	for i, f := range slices.Backward(iit.files) {
		if f.startTxNum >= txNum || !seekFileSeq(iit, i, key, &seq) {
			continue
		}
		if f.endTxNum <= txNum {
			return seq.Max(), true, nil
		}
		var last uint64
		found := false
		for it.Reset(&seq, 0); it.HasNext(); {
			n, err := it.Next()
			if err != nil {
				return 0, false, err
			}
			if n >= txNum {
				break
			}
			last, found = n, true
		}
		if found {
			return last, true, nil
		}
	}
	return 0, false, nil
}

func appendFileTxNums(dst []uint64, iit *InvertedIndexRoTx, from, to uint64, key []byte) ([]uint64, error) {
	i := slices.IndexFunc(iit.files, func(f visibleFile) bool { return f.startTxNum == from && f.endTxNum == to })
	var seq multiencseq.SequenceReader
	if i < 0 || !seekFileSeq(iit, i, key, &seq) {
		return dst, nil
	}
	var it multiencseq.SequenceIterator
	for it.Reset(&seq, 0); it.HasNext(); {
		txNum, err := it.Next()
		if err != nil {
			return dst, err
		}
		dst = append(dst, txNum)
	}
	return dst, nil
}

func encodeLegacyPrefix(path []byte, keysV2 bool) []byte {
	if keysV2 {
		return nibbles.EncodeKeyV2(path)
	}
	return nibbles.HexToCompact(path)
}

func decodeLegacyPrefix(key []byte, keysV2 bool) ([]byte, error) {
	if keysV2 {
		return nibbles.DecodeKeyV2(key)
	}
	if len(key) == 0 || key[0]>>4 > 1 {
		return nil, fmt.Errorf("%x is not a compact branch prefix", key)
	}
	return nibbles.CompactToHex(key), nil
}

type legacyChild struct {
	path []byte
	hash [32]byte
}

func appendLegacyChildren(dst []legacyChild, prefix, value []byte) ([]legacyChild, error) {
	if len(value) < 4 || binary.BigEndian.Uint16(value[2:4]) == 0 {
		return dst, nil
	}
	err := commitment.BranchData(value).ForEachCell(func(nib int, c commitment.BranchCell) error {
		if len(c.Hash) != length.Hash || len(c.StorageAddr) != 0 {
			return nil
		}
		var child legacyChild
		if len(c.AccountAddr) != 0 {
			child.path = append(commitment.KeyToNibblizedHash(c.AccountAddr), c.Extension...)
		} else {
			child.path = append(append(append(make([]byte, 0, len(prefix)+1+len(c.Extension)), prefix...), byte(nib)), c.Extension...)
		}
		copy(child.hash[:], c.Hash)
		dst = append(dst, child)
		return nil
	})
	return dst, err
}

func legacyCommitTxNums(dt *DomainRoTx, fromTxNum, toTxNum uint64) ([]uint64, error) {
	iit := dt.ht.iit
	var commits []uint64
	last, found, err := lastWriteBefore(iit, commitment.KeyCommitmentState, fromTxNum)
	if err != nil {
		return nil, err
	}
	if found {
		commits = append(commits, last)
	}
	return appendFileTxNums(commits, iit, fromTxNum, toTxNum, commitment.KeyCommitmentState)
}

func convertStateOrEmpty(legacy []byte) ([]byte, error) {
	if len(legacy) == 0 {
		return nil, nil
	}
	return v3.ConvertLegacyState(legacy)
}

func stateRootsDiffer(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) != len(b)
	}
	_, _, rootA, errA := commitment.DecodeCommitmentV3State(a)
	_, _, rootB, errB := commitment.DecodeCommitmentV3State(b)
	return errA != nil || errB != nil || !bytes.Equal(rootA, rootB)
}

func legacyCommitmentAsOf(dt *DomainRoTx, key []byte, txNum uint64) ([]byte, error) {
	v, found, err := dt.ht.historySeekInFiles(key, txNum)
	if err != nil {
		return nil, err
	}
	if !found {
		if v, _, _, _, err = dt.getLatestFromFiles(key, nil, kv.NoStepBound); err != nil {
			return nil, err
		}
	}
	return bytes.Clone(v), nil
}

func resolveLegacyHistory(key []byte, entries []v3.LegacyEntry) ([]byte, error) {
	var value []byte
	for _, e := range entries {
		if len(e.Value) == 0 {
			continue
		}
		if value != nil && !bytes.Equal(e.Value, value) {
			return nil, fmt.Errorf("%w: two history values for %x", v3.ErrLegacyConflict, key)
		}
		value = e.Value
	}
	return value, nil
}

func detectHistoryKeysV2(ii *InvertedIndex, ef *seg.Decompressor, samples int) (bool, error) {
	keys := ef.Count() / 2
	if keys == 0 {
		return false, nil
	}
	stride := max(1, keys/samples)
	r := seg.NewReader(ef.MakeGetter(), ii.Compression)
	var pairs []sampledPair
	for i := 0; r.HasNext(); i++ {
		if i%stride != 0 {
			r.Skip()
			r.Skip()
			continue
		}
		k, _ := r.Next(nil)
		r.Skip()
		pairs = append(pairs, sampledPair{k: k})
	}
	keysV2, err := detectKeyEncoding(pairs)
	if errors.Is(err, errNoNonStateSamples) {
		return false, nil
	}
	return keysV2, err
}

func pendingCommitmentHistoryV3(dt *DomainRoTx) ([]visibleFile, []visibleFile, error) {
	var vs, efs []visibleFile
	for _, v := range dt.ht.files {
		if !v.Version().Less(version.V3_0) {
			continue
		}
		j := slices.IndexFunc(dt.ht.iit.files, func(ef visibleFile) bool { return ef.startTxNum == v.startTxNum && ef.endTxNum == v.endTxNum })
		if j < 0 {
			return nil, nil, fmt.Errorf("commitment history %s has no .ef of the same range", v.Fullpath())
		}
		vs, efs = append(vs, v), append(efs, dt.ht.iit.files[j])
	}
	return vs, efs, nil
}

func convertCommitmentHistoryV3(ctx context.Context, at *AggregatorRoTx, stage commitmentV3HistoryStage, logger log.Logger) ([]commitmentHistoryRange, error) {
	dt := at.d[kv.CommitmentDomain]
	if dt.ht.h.SnapshotsDisabled {
		return nil, nil
	}
	vs, efs, err := pendingCommitmentHistoryV3(dt)
	if err != nil || len(vs) == 0 {
		return nil, err
	}
	for _, d := range []string{stage.history, stage.idx, stage.accessor} {
		if err := dir.RemoveAll(d); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	started := time.Now()
	ranges := make([]commitmentHistoryRange, 0, len(vs))
	for i := range vs {
		r, err := convertCommitmentHistoryFileV3(ctx, at, vs[i], efs[i], stage, i+1, len(vs), logger)
		if err != nil {
			return nil, fmt.Errorf("convertCommitmentHistoryFileV3 %q: %w", vs[i].Fullpath(), err)
		}
		ranges = append(ranges, r)
	}
	logger.Info(fmt.Sprintf("[commitment_convert] v3 history: converted %d files in %s", len(ranges), time.Since(started).Round(time.Millisecond)))
	return ranges, nil
}

func convertCommitmentHistoryFileV3(ctx context.Context, at *AggregatorRoTx, vFile, efFile visibleFile, stage commitmentV3HistoryStage, fileIdx, fileTotal int, logger log.Logger) (commitmentHistoryRange, error) {
	dt := at.d[kv.CommitmentDomain]
	h := dt.ht.h
	ii := h.InvertedIndex
	stepSize := at.StepSize()
	fromTxNum, toTxNum := vFile.startTxNum, vFile.endTxNum
	r := commitmentHistoryRange{fromStep: kv.Step(fromTxNum / stepSize), toStep: kv.Step(toTxNum / stepSize)}
	baseName := filepath.Base(vFile.Fullpath())
	fileStart := time.Now()

	keysV2, keysErr := detectHistoryKeysV2(ii, efFile.src.decompressor, 48)
	if keysErr != nil {
		return r, keysErr
	}
	pageValues := vFile.src.decompressor.CompressedPageValuesCount()
	if vFile.src.decompressor.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		pageValues = h.HistoryValuesOnCompressedPage
	}

	workers := max(1, runtime.GOMAXPROCS(0))
	collectors, closeCollectors := newV3Collectors("commitment_convert_v3_history", at.Dirs().Tmp, workers, logger)
	defer closeCollectors()
	in := make(chan *historyChainBatch, workers*2)
	out := make(chan *convertedBatch, workers*2)
	var chains, versions atomic.Uint64

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(in)
		vView, openErr := vFile.src.decompressor.OpenSequentialView()
		if openErr != nil {
			return openErr
		}
		defer vView.Close()
		efView, openErr := efFile.src.decompressor.OpenSequentialView()
		if openErr != nil {
			return openErr
		}
		defer efView.Close()
		efReader := seg.NewReader(efView.MakeGetter(), ii.Compression)
		vReader := seg.NewPagedReader(seg.NewReader(vView.MakeGetter(), h.Compression), pageValues, true)
		var seq multiencseq.SequenceReader
		var it multiencseq.SequenceIterator
		var keyBuf, efBuf, valBuf []byte
		batch := historyChainBatchPool.Get().(*historyChainBatch)
		batch.reset()
		send := func() error {
			select {
			case in <- batch:
				batch = historyChainBatchPool.Get().(*historyChainBatch)
				batch.reset()
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		for efReader.HasNext() {
			keyBuf, _ = efReader.Next(keyBuf[:0])
			if !efReader.HasNext() {
				return fmt.Errorf("%s truncated at key %x", efFile.Fullpath(), keyBuf)
			}
			efBuf, _ = efReader.Next(efBuf[:0])
			key := batch.span(keyBuf)
			start := len(batch.txNums)
			seq.Reset(fromTxNum, efBuf)
			it.Reset(&seq, 0)
			for it.HasNext() {
				txNum, err := it.Next()
				if err != nil {
					return err
				}
				if !vReader.HasNext() {
					return fmt.Errorf("%s has no value for %x at txNum %d", baseName, keyBuf, txNum)
				}
				var v []byte
				_, v, valBuf, _ = vReader.Next2(valBuf[:0])
				batch.txNums = append(batch.txNums, txNum)
				batch.vals = append(batch.vals, batch.span(v))
			}
			batch.keys = append(batch.keys, key)
			batch.spans = append(batch.spans, [2]int{start, len(batch.txNums)})
			if len(batch.keys) == commitmentV3HistoryBatch {
				if sendErr := send(); sendErr != nil {
					return sendErr
				}
			}
		}
		if vReader.HasNext() {
			return fmt.Errorf("%s has values past the last .ef entry", baseName)
		}
		if len(batch.keys) == 0 {
			return nil
		}
		return send()
	})

	commits, commitsErr := legacyCommitTxNums(dt, fromTxNum, toTxNum)
	if commitsErr != nil {
		return r, commitsErr
	}
	leavesAsOf := func(txNum uint64) uint64 {
		i, _ := slices.BinarySearch(commits, txNum)
		if i == 0 {
			return 0
		}
		return commits[i-1] + 1
	}
	rootLegacyKey := encodeLegacyPrefix(nil, keysV2)

	var converting sync.WaitGroup
	for range workers {
		converting.Add(1)
		g.Go(func() error {
			defer converting.Done()
			wat := at.a.BeginFilesRo()
			defer wat.Close()
			wdt := wat.d[kv.CommitmentDomain]
			vals := &legacyHistoryValues{accounts: wat.d[kv.AccountsDomain], storage: wat.d[kv.StorageDomain], cache: map[string]historyValue{}}
			conv := v3.NewLegacyConverter(vals, keysV2, false)
			var cur, nextVersion, childCur, childNext, scratch historyVersion
			var beforeChildren, afterChildren []legacyChild
			var key, rootKey []byte
			var res *convertedBatch
			emit := func(k []byte, txNum uint64, v []byte, kind v3.LegacyKind) error {
				key = binary.BigEndian.AppendUint64(appendEscapedKey(key[:0], k), txNum)
				return res.emit(key, v, kind)
			}
			convert := func(legacyKey, value []byte, asOf uint64, dst *historyVersion) error {
				dst.reset()
				vals.asOf = asOf
				if convErr := conv.Convert(legacyKey, value, nil, dst.add); convErr != nil {
					return fmt.Errorf("legacy %x as of txNum %d: %w", legacyKey, asOf, convErr)
				}
				if parseErr := dst.parseAccounts(value); parseErr != nil {
					return fmt.Errorf("legacy %x as of txNum %d: accounts: %w", legacyKey, asOf, parseErr)
				}
				return nil
			}
			branchRootAsOf := func(addrHash [32]byte, txNum uint64) ([]byte, error) {
				path := make([]byte, 64)
				nibbles.Expand(addrHash[:], path)
				legacyKey := encodeLegacyPrefix(path, keysV2)
				value, err := legacyCommitmentAsOf(wdt, legacyKey, txNum)
				if err != nil {
					return nil, err
				}
				if err := convert(legacyKey, value, leavesAsOf(txNum), &scratch); err != nil {
					return nil, err
				}
				root := scratch.valueOf(rootKey)
				if len(root) == 0 {
					return nil, fmt.Errorf("account %x has a storage branch before txNum %d but legacy %x holds none", addrHash, txNum, legacyKey)
				}
				return root, nil
			}
			emitBoundary := func(txNum uint64, cur, next *historyVersion) error {
				for _, e := range cur.ents {
					if e.kind == v3.LegacySynthesized {
						continue
					}
					k, v := cur.key(e), cur.value(e)
					if e.kind == v3.LegacyDirect && next.hasValue(k, v) {
						continue
					}
					if err := emit(k, txNum, v, e.kind); err != nil {
						return err
					}
				}
				for _, a := range cur.accounts {
					rootKey = v3.StorageNodeKey(a.addrHash, nil, rootKey[:0])
					after, inNext := next.account(a.addrHash)
					var before []byte
					switch a.class {
					case storageSingleton, storageExtension:
						before = cur.valueOf(rootKey)
						if inNext && after.class != storageNone && after.class != storageBranch && bytes.Equal(before, next.valueOf(rootKey)) {
							continue
						}
					case storageBranch:
						if inNext && after.class == storageBranch {
							continue
						}
						var err error
						if before, err = branchRootAsOf(a.addrHash, txNum); err != nil {
							return err
						}
					default:
						continue
					}
					if err := emit(rootKey, txNum, before, v3.LegacySynthesized); err != nil {
						return err
					}
				}
				for _, a := range next.accounts {
					if a.class == storageNone {
						continue
					}
					if b, inCur := cur.account(a.addrHash); inCur && b.class != storageNone {
						continue
					}
					rootKey = v3.StorageNodeKey(a.addrHash, nil, rootKey[:0])
					if err := emit(rootKey, txNum, nil, v3.LegacySynthesized); err != nil {
						return err
					}
				}
				return nil
			}
			var childChanged func(childKey []byte, txNum uint64, persists bool) error
			childChanged = func(childKey []byte, txNum uint64, persists bool) error {
				found, written, err := wdt.ht.iit.seekInFiles(childKey, txNum)
				if err != nil {
					return err
				}
				if found && written == txNum {
					return nil
				}
				value, err := legacyCommitmentAsOf(wdt, childKey, txNum)
				if err != nil {
					return err
				}
				if len(value) < 4 || binary.BigEndian.Uint16(value[2:4]) == 0 {
					return fmt.Errorf("legacy %x is referenced at txNum %d but holds no record", childKey, txNum)
				}
				if convErr := convert(childKey, value, leavesAsOf(txNum), &childCur); convErr != nil {
					return convErr
				}
				childNext.reset()
				if persists {
					if convErr := convert(childKey, value, txNum+1, &childNext); convErr != nil {
						return convErr
					}
				}
				if err = emitBoundary(txNum, &childCur, &childNext); err != nil || persists {
					return err
				}
				prefix, err := decodeLegacyPrefix(childKey, keysV2)
				if err != nil {
					return err
				}
				removed, err := appendLegacyChildren(nil, prefix, value)
				if err != nil {
					return fmt.Errorf("children of removed %x at txNum %d: %w", childKey, txNum, err)
				}
				for _, c := range removed {
					if err := childChanged(encodeLegacyPrefix(c.path, keysV2), txNum, false); err != nil {
						return err
					}
				}
				return nil
			}
			childrenChanged := func(prefix, before, after []byte, txNum uint64) error {
				var err error
				if beforeChildren, err = appendLegacyChildren(beforeChildren[:0], prefix, before); err != nil {
					return fmt.Errorf("children of %x before txNum %d: %w", prefix, txNum, err)
				}
				if afterChildren, err = appendLegacyChildren(afterChildren[:0], prefix, after); err != nil {
					return fmt.Errorf("children of %x after txNum %d: %w", prefix, txNum, err)
				}
				for _, b := range beforeChildren {
					j := slices.IndexFunc(afterChildren, func(a legacyChild) bool { return bytes.Equal(a.path, b.path) })
					if j >= 0 && afterChildren[j].hash == b.hash {
						continue
					}
					if err := childChanged(encodeLegacyPrefix(b.path, keysV2), txNum, j >= 0); err != nil {
						return err
					}
				}
				for _, a := range afterChildren {
					if slices.ContainsFunc(beforeChildren, func(b legacyChild) bool { return bytes.Equal(a.path, b.path) }) {
						continue
					}
					if err := childChanged(encodeLegacyPrefix(a.path, keysV2), txNum, true); err != nil {
						return err
					}
				}
				return nil
			}
			for batch := range in {
				res = convertedBatchPool.Get().(*convertedBatch)
				res.buf, res.ents = res.buf[:0], res.ents[:0]
				for c, span := range batch.spans {
					legacyKey := batch.buf[batch.keys[c][0]:batch.keys[c][1]]
					txNums := batch.txNums[span[0]:span[1]]
					value := func(i int) []byte { return batch.buf[batch.vals[span[0]+i][0]:batch.vals[span[0]+i][1]] }
					after := func(i int) ([]byte, error) {
						if i+1 < len(txNums) {
							return value(i + 1), nil
						}
						return legacyCommitmentAsOf(wdt, legacyKey, toTxNum)
					}
					chains.Add(1)
					versions.Add(uint64(len(txNums)))
					if commitment.IsCommitmentStateKey(legacyKey) {
						for i, txNum := range txNums {
							before, err := convertStateOrEmpty(value(i))
							if err != nil {
								return fmt.Errorf("state before txNum %d: %w", txNum, err)
							}
							if emitErr := emit(commitment.KeyCommitmentV3State, txNum, before, v3.LegacyDirect); emitErr != nil {
								return emitErr
							}
							next, err := after(i)
							if err != nil {
								return err
							}
							afterState, err := convertStateOrEmpty(next)
							if err != nil {
								return fmt.Errorf("state after txNum %d: %w", txNum, err)
							}
							if stateRootsDiffer(before, afterState) {
								if err := childChanged(rootLegacyKey, txNum, true); err != nil {
									return err
								}
							}
						}
						continue
					}
					prefix, err := decodeLegacyPrefix(legacyKey, keysV2)
					if err != nil {
						return err
					}
					for i, txNum := range txNums {
						next, err := after(i)
						if err != nil {
							return err
						}
						if err := convert(legacyKey, value(i), leavesAsOf(txNum), &cur); err != nil {
							return err
						}
						if err := convert(legacyKey, next, txNum+1, &nextVersion); err != nil {
							return err
						}
						if err := emitBoundary(txNum, &cur, &nextVersion); err != nil {
							return err
						}
						if err := childrenChanged(prefix, value(i), next, txNum); err != nil {
							return err
						}
					}
				}
				historyChainBatchPool.Put(batch)
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
	if err := g.Wait(); err != nil {
		return r, err
	}
	collected := time.Now()

	vPath := filepath.Join(stage.history, filepath.Base(h.vNewFilePath(r.fromStep, r.toStep)))
	viPath := filepath.Join(stage.accessor, filepath.Base(h.vAccessorNewFilePath(r.fromStep, r.toStep)))
	efPath := filepath.Join(stage.idx, filepath.Base(ii.efNewFilePath(r.fromStep, r.toStep)))
	efiPath := filepath.Join(stage.accessor, filepath.Base(ii.efAccessorNewFilePath(r.fromStep, r.toStep)))
	vComp, compErr := seg.NewCompressor(ctx, "commitment_convert_v3 .v", vPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, logger)
	if compErr != nil {
		return r, compErr
	}
	defer vComp.Close()
	efComp, compErr := seg.NewCompressor(ctx, "commitment_convert_v3 .ef", efPath, h.dirs.Tmp, ii.CompressorCfg, log.LvlTrace, logger)
	if compErr != nil {
		return r, compErr
	}
	defer efComp.Close()
	vWriter := h.dataWriter(ctx, vComp)
	efWriter := ii.dataWriter(efComp, false)

	var (
		groupKey, histKey, efBytes []byte
		curKey, nextKey            []byte
		group                      []v3.LegacyEntry
		groupVals                  []byte
		keyTxNums                  []uint64
		builder                    multiencseq.SequenceBuilder
		records, keys              uint64
	)
	finishKey := func() error {
		if len(keyTxNums) == 0 {
			return nil
		}
		builder.Reset(fromTxNum, uint64(len(keyTxNums)), keyTxNums[len(keyTxNums)-1])
		for _, txNum := range keyTxNums {
			builder.AddOffset(txNum)
		}
		builder.Build()
		efBytes = builder.AppendBytes(efBytes[:0])
		keys++
		if _, err := efWriter.Write(curKey); err != nil {
			return err
		}
		_, err := efWriter.Write(efBytes)
		return err
	}
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		value, err := resolveLegacyHistory(groupKey, group)
		if err != nil {
			return err
		}
		escaped, txNum := groupKey[:len(groupKey)-8], binary.BigEndian.Uint64(groupKey[len(groupKey)-8:])
		if nextKey, err = appendUnescapedKey(nextKey[:0], escaped); err != nil {
			return err
		}
		if !bytes.Equal(nextKey, curKey) || len(keyTxNums) == 0 {
			if err := finishKey(); err != nil {
				return err
			}
			curKey, keyTxNums = append(curKey[:0], nextKey...), keyTxNums[:0]
		}
		keyTxNums = append(keyTxNums, txNum)
		records++
		histKey = historyKey(txNum, curKey, histKey)
		return vWriter.Add(histKey, value)
	}
	loadErr := etl.MergeLoad("commitment_convert_v3_history", collectors, func(k, v []byte) error {
		if len(group) == 0 || !bytes.Equal(k, groupKey) {
			if err := flush(); err != nil {
				return err
			}
			groupKey, groupVals, group = append(groupKey[:0], k...), groupVals[:0], group[:0]
		}
		start := len(groupVals)
		groupVals = append(groupVals, v[1:]...)
		group = append(group, v3.LegacyEntry{Kind: v3.LegacyKind(v[0]), Value: groupVals[start:]})
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})
	if loadErr == nil {
		loadErr = flush()
	}
	if loadErr == nil {
		loadErr = finishKey()
	}
	if loadErr != nil {
		return r, fmt.Errorf("resolve: %w", loadErr)
	}
	if err := vWriter.Compress(); err != nil {
		return r, err
	}
	if err := efWriter.Compress(); err != nil {
		return r, err
	}
	vComp.Close()
	efComp.Close()
	written := time.Now()

	ps := background.NewProgressSet()
	efDecomp, err := seg.NewDecompressor(efPath)
	if err != nil {
		return r, err
	}
	defer efDecomp.Close()
	vDecomp, err := seg.NewDecompressor(vPath)
	if err != nil {
		return r, err
	}
	defer vDecomp.Close()
	if err := ii.buildMapAccessorAt(ctx, efiPath, efDecomp, ps); err != nil {
		return r, err
	}
	if err := h.buildVI(ctx, viPath, vDecomp, efDecomp, fromTxNum, ps); err != nil {
		return r, err
	}

	legacySize := vFile.src.decompressor.Size() + efFile.src.decompressor.Size()
	elapsed := time.Since(fileStart)
	logger.Info(fmt.Sprintf(
		"[commitment_convert] v3 history file done %s legacy keys=%s entries=%s -> keys=%s entries=%s .v+.ef %s -> %s in %s (%s entries/s) collect=%s write=%s build=%s (%d/%d files)",
		baseName, common.PrettyCounter(chains.Load()), common.PrettyCounter(versions.Load()),
		common.PrettyCounter(keys), common.PrettyCounter(records),
		common.ByteCount(uint64(legacySize)), common.ByteCount(uint64(vDecomp.Size()+efDecomp.Size())),
		elapsed.Round(time.Millisecond), formatRate(versions.Load(), elapsed),
		collected.Sub(fileStart).Round(time.Millisecond), written.Sub(collected).Round(time.Millisecond), time.Since(written).Round(time.Millisecond),
		fileIdx, fileTotal))
	return r, nil
}

func convertHistoryPhase3(dirs datadir.Dirs, backupRoot string, ranges []commitmentHistoryRange) (int, error) {
	moved := 0
	for _, pair := range [][2]string{{dirs.SnapHistory, "history"}, {dirs.SnapIdx, "idx"}, {dirs.SnapAccessors, "accessor"}} {
		dst := filepath.Join(backupRoot, pair[1])
		if err := os.MkdirAll(dst, 0o755); err != nil {
			return moved, err
		}
		for _, r := range ranges {
			matches, err := filepath.Glob(filepath.Join(pair[0], fmt.Sprintf("*-commitment.%d-%d.*", r.fromStep, r.toStep)))
			if err != nil {
				return moved, err
			}
			for _, src := range matches {
				if err := os.Rename(src, filepath.Join(dst, filepath.Base(src))); err != nil {
					return moved, err
				}
				moved++
			}
		}
	}
	return moved, nil
}

func convertHistoryPhase4(dirs datadir.Dirs, stage commitmentV3HistoryStage) (int, error) {
	moved := 0
	for _, pair := range [][2]string{{stage.history, dirs.SnapHistory}, {stage.idx, dirs.SnapIdx}, {stage.accessor, dirs.SnapAccessors}} {
		n, err := convertPhase4(pair[0], pair[1])
		moved += n
		if err != nil {
			return moved, err
		}
		if err := dir.RemoveAll(pair[0]); err != nil {
			return moved, err
		}
	}
	return moved, nil
}

func verifyCommitmentV3History(ctx context.Context, a *Aggregator, ranges []commitmentHistoryRange, logger log.Logger) error {
	started := time.Now()
	at := a.BeginFilesRo()
	iit := at.d[kv.CommitmentDomain].ht.iit
	stepSize := at.StepSize()
	var samples []uint64
	for _, r := range ranges {
		txNums, err := appendFileTxNums(nil, iit, uint64(r.fromStep)*stepSize, uint64(r.toStep)*stepSize, commitment.KeyCommitmentV3State)
		if err != nil {
			at.Close()
			return err
		}
		if len(txNums) == 0 {
			at.Close()
			return fmt.Errorf("commitment history %d-%d has no state entries", r.fromStep, r.toStep)
		}
		samples = append(samples, txNums[len(txNums)/2]+1, txNums[len(txNums)-1]+1)
	}
	at.Close()
	for _, txNum := range samples {
		if _, err := DebugCommitmentV3RootAsOf(ctx, a, txNum); err != nil {
			return fmt.Errorf("as of txNum %d: %w", txNum, err)
		}
	}
	logger.Info("[commitment_convert] v3 history verified", "files", len(ranges), "samples", len(samples), "took", time.Since(started).Round(time.Millisecond))
	return nil
}
