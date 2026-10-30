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

package pbt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func (t *Trie) newVerifier() *Trie {
	verifier := &Trie{
		ctx:                   t.ctx,
		rootKey:               bytes.Clone(t.rootKey),
		rootPath:              t.rootPath,
		bucketMode:            t.bucketMode,
		upperOnly:             t.upperOnly,
		suppressRoot:          t.suppressRoot,
		suppressBucketRecords: t.suppressBucketRecords,
		verifyOnly:            true,
		verifyProgress:        t.verifyProgress,
		verifyStats:           t.verifyStats,
	}
	if t.ownedPrefix != nil {
		prefix := *t.ownedPrefix
		verifier.ownedPrefix = &prefix
	}
	if len(t.upperStops) != 0 {
		verifier.upperStops = append([]eip8297.Bitpath(nil), t.upperStops...)
	}
	return verifier
}

func (t *Trie) Verify() error {
	return t.VerifyContext(context.Background(), 1, nil)
}

func (t *Trie) VerifyContext(ctx context.Context, workers int, factory commitment.TrieContextFactory) error {
	if t.ctx == nil {
		return fmt.Errorf("nil Patricia context")
	}
	verifier := t.newVerifier()
	_, hasRangePlanner := verifier.ctx.(verifyRangeTaskPreparer)
	if workers > 0 && (factory != nil || hasRangePlanner) {
		if err := verifier.verifyChains(ctx, workers, factory); err != nil {
			return err
		}
	}
	if err := verifier.verify(); err != nil {
		return err
	}
	if checker, ok := verifier.ctx.(interface{ VerifyKeyCount(uint64) error }); ok {
		if err := checker.VerifyKeyCount(verifier.verifyReferencedKeys); err != nil {
			return err
		}
	}
	t.root = verifier.root
	t.rootLoaded = verifier.rootLoaded
	t.foldedRoot = verifier.foldedRoot
	t.foldedRootReady = verifier.foldedRootReady
	return nil
}

const verifyChainBits = 16

type VerifyRangeReader interface {
	ReadVerifyRange(from, to []byte, emit func([]byte, []byte) error) (uint64, error)
}

type verifyRangeTaskPreparer interface {
	PrepareVerifyRanges(starts [][]byte) error
}

type verifyUpperRowPreparer interface {
	PrepareVerifyUpper(rootKey []byte, upperBits int16) (map[string][]byte, error)
}

type VerifyStats struct {
	PointLookups       atomic.Uint64
	ExactRangeReads    atomic.Uint64
	RangeTasks         atomic.Uint64
	RangeReads         atomic.Uint64
	RangeTaskReads     atomic.Uint64
	RangeReadsAtOffset atomic.Uint64
	RangeRows          atomic.Uint64
	UpperRowsLoaded    atomic.Uint64
	TaskWorkers        atomic.Uint64
	TaskRowsLoaded     atomic.Uint64
	TaskRowsReleased   atomic.Uint64
	LiveRows           atomic.Uint64
	LiveBytes          atomic.Uint64
	PeakRows           atomic.Uint64
	PeakBytes          atomic.Uint64
	MaxTaskRows        atomic.Uint64
	UpperRowsAtStart   atomic.Uint64
	liveProgress       func(uint64, uint64)
}

func (t *Trie) SetVerifyStats(stats *VerifyStats) {
	t.verifyStats = stats
	if setter, ok := t.ctx.(interface{ SetVerifyRangeOffsetCounter(*atomic.Uint64) }); ok {
		if stats == nil {
			setter.SetVerifyRangeOffsetCounter(nil)
		} else {
			setter.SetVerifyRangeOffsetCounter(&stats.RangeReadsAtOffset)
		}
	}
}

func (s *VerifyStats) SetLiveProgress(progress func(uint64, uint64)) {
	s.liveProgress = progress
}

func (s *VerifyStats) holdRows(rows, bytes uint64) {
	liveRows := s.LiveRows.Add(rows)
	liveBytes := s.LiveBytes.Add(bytes)
	for peak := s.PeakRows.Load(); liveRows > peak && !s.PeakRows.CompareAndSwap(peak, liveRows); peak = s.PeakRows.Load() {
	}
	for peak := s.PeakBytes.Load(); liveBytes > peak && !s.PeakBytes.CompareAndSwap(peak, liveBytes); peak = s.PeakBytes.Load() {
	}
	if s.liveProgress != nil {
		s.liveProgress(liveRows, liveBytes)
	}
}

func (s *VerifyStats) recordTaskRows(rows uint64) {
	for maximum := s.MaxTaskRows.Load(); rows > maximum && !s.MaxTaskRows.CompareAndSwap(maximum, rows); maximum = s.MaxTaskRows.Load() {
	}
}

func (s *VerifyStats) releaseRows(rows, bytes uint64) {
	if rows != 0 {
		s.LiveRows.Add(0 - rows)
	}
	if bytes != 0 {
		s.LiveBytes.Add(0 - bytes)
	}
	if s.liveProgress != nil {
		s.liveProgress(s.LiveRows.Load(), s.LiveBytes.Load())
	}
}

type verifiedChain struct {
	row            *rowNode
	result         FoldResult
	err            error
	referencedKeys uint64
}

func (t *Trie) verifyChain(path eip8297.Bitpath) (chain verifiedChain) {
	defer t.releaseVerifyRows()
	chain.err = t.loadVerifyRows(path)
	if chain.err == nil {
		chain.row, chain.err = t.loadRow(path)
	}
	if chain.err == nil {
		chain.result, chain.err = t.verifyRow(chain.row)
	}
	if chain.err == nil {
		chain.err = t.verifyRowsComplete()
	}
	if chain.err == nil {
		chain.referencedKeys = t.verifyReferencedKeys
	}
	return chain
}

func (t *Trie) trackBucketKeys() {
	if t.verifiedBucketKeys != nil {
		return
	}
	if _, ok := t.ctx.(interface{ Records() map[string][]byte }); ok {
		t.verifiedBucketKeys = make(map[string]struct{})
	}
}

func (t *Trie) verifyKeyOwnedByTask(key []byte) bool {
	if len(t.verifiedChains) == 0 {
		return false
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return false
	}
	for taskKey := range t.verifiedChains {
		taskPath, err := eip8297.DecodeBitPath([]byte(taskKey))
		if err == nil && path.BitLen >= taskPath.BitLen && path.HasPrefix(&taskPath) {
			return true
		}
	}
	return false
}

func (t *Trie) verifyChains(ctx context.Context, workers int, factory commitment.TrieContextFactory) error {
	if preparer, ok := t.ctx.(verifyUpperRowPreparer); ok {
		upperRows, err := preparer.PrepareVerifyUpper(t.rootRecordKey(), verifyChainBits)
		if err != nil {
			return err
		}
		t.verifyUpperRows = upperRows
		if t.verifyStats != nil {
			t.verifyStats.UpperRowsLoaded.Add(uint64(len(upperRows)))
		}
	}
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if root.form != RowRoot || root.row == nil || root.row.path.BitLen >= verifyChainBits {
		return nil
	}
	var paths []eip8297.Bitpath
	upperBuckets := make(map[string]struct{})
	if err := t.collectChainPaths(root.row, &paths, upperBuckets); err != nil {
		return nil
	}
	if len(paths) < 2 {
		return nil
	}
	if t.verifyStats != nil {
		t.verifyStats.TaskWorkers.Store(uint64(min(workers, len(paths))))
	}
	t.trackBucketKeys()
	progress := t.verifyProgress
	if progress != nil {
		var mu sync.Mutex
		progress = func(key []byte) {
			mu.Lock()
			defer mu.Unlock()
			t.verifyProgress(key)
		}
	}
	keys := make([]string, len(paths))
	starts := make([][]byte, len(paths))
	for i := range paths {
		key, err := rowKeyForPath(&paths[i])
		if err != nil {
			return nil
		}
		start, _ := verifyRangeBounds(&paths[i])
		keys[i] = string(key)
		starts[i] = start
	}
	if preparer, ok := t.ctx.(verifyRangeTaskPreparer); ok {
		rangeStarts := make([][]byte, 0, len(starts)+len(upperBuckets))
		rangeStarts = append(rangeStarts, starts...)
		for key := range upperBuckets {
			rangeStarts = append(rangeStarts, []byte(key))
		}
		if err := preparer.PrepareVerifyRanges(rangeStarts); err != nil {
			return err
		}
	}
	if err := t.loadUpperBucketRows(upperBuckets); err != nil {
		return err
	}
	if factory == nil || workers <= 1 {
		if t.verifyStats != nil {
			t.verifyStats.TaskWorkers.Store(1)
		}
		t.trackBucketKeys()
		chains := make([]verifiedChain, len(paths))
		bucketKeys := make([]map[string]struct{}, len(paths))
		for i := range paths {
			worker := t.newVerifier()
			worker.ctx = t.ctx
			worker.verifyProgress = t.verifyProgress
			if t.verifiedBucketKeys != nil {
				worker.verifiedBucketKeys = make(map[string]struct{})
			}
			chain := worker.verifyChain(paths[i])
			if chain.err != nil {
				return chain.err
			}
			chains[i] = chain
			bucketKeys[i] = worker.verifiedBucketKeys
		}
		t.verifiedChains = make(map[string]verifiedChain, len(chains))
		for i := range chains {
			t.verifiedChains[keys[i]] = chains[i]
			t.verifyReferencedKeys += chains[i].referencedKeys
			for key := range bucketKeys[i] {
				t.verifiedBucketKeys[key] = struct{}{}
			}
		}
		return nil
	}
	chains := make([]verifiedChain, len(paths))
	bucketKeys := make([]map[string]struct{}, len(paths))
	var next atomic.Int64
	var firstFailed atomic.Int64
	firstFailed.Store(int64(len(paths)))
	g, gctx := errgroup.WithContext(ctx)
	for range min(workers, len(paths)) {
		g.Go(func() error {
			view, cleanup := factory(gctx)
			if cleanup != nil {
				defer cleanup()
			}
			if view == nil {
				return errors.New("trie context factory returned a nil context")
			}
			for {
				if err := gctx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1)) - 1
				if i >= len(paths) {
					return nil
				}
				if int64(i) > firstFailed.Load() {
					continue
				}
				worker := t.newVerifier()
				worker.ctx = view
				worker.verifyProgress = progress
				if t.verifiedBucketKeys != nil {
					worker.verifiedBucketKeys = make(map[string]struct{})
				}
				chain := worker.verifyChain(paths[i])
				if chain.err != nil {
					for failed := firstFailed.Load(); int64(i) < failed && !firstFailed.CompareAndSwap(failed, int64(i)); failed = firstFailed.Load() {
					}
				}
				chains[i] = chain
				bucketKeys[i] = worker.verifiedBucketKeys
			}
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	t.verifiedChains = make(map[string]verifiedChain, len(chains))
	for i := range chains {
		if int64(i) > firstFailed.Load() {
			continue
		}
		t.verifiedChains[keys[i]] = chains[i]
		t.verifyReferencedKeys += chains[i].referencedKeys
		for key := range bucketKeys[i] {
			t.verifiedBucketKeys[key] = struct{}{}
		}
	}
	return nil
}

func (t *Trie) verifyRangeReader() (VerifyRangeReader, bool) {
	reader, ok := t.ctx.(VerifyRangeReader)
	if !ok {
		return nil, false
	}
	if checker, ok := reader.(interface{ HasVerifyRange() bool }); ok && !checker.HasVerifyRange() {
		return nil, false
	}
	return reader, true
}

func (t *Trie) loadVerifyRows(path eip8297.Bitpath) error {
	reader, ok := t.verifyRangeReader()
	if !ok {
		return nil
	}
	if t.verifyStats != nil {
		t.verifyStats.RangeTasks.Add(1)
		t.verifyStats.RangeTaskReads.Add(1)
	}
	from, to := verifyRangeBounds(&path)
	rows := make(map[string][]byte)
	var taskRows, taskBytes, heldRows, heldBytes uint64
	hold := func() {
		if t.verifyStats != nil && taskRows != heldRows {
			t.verifyStats.holdRows(taskRows-heldRows, taskBytes-heldBytes)
		}
		heldRows, heldBytes = taskRows, taskBytes
	}
	count, err := reader.ReadVerifyRange(from, to, func(key, value []byte) error {
		if commitment.IsCommitmentStateKey(key) {
			return nil
		}
		rowPath, decodeErr := eip8297.DecodeBitPath(key)
		if decodeErr != nil || rowPath.BitLen < path.BitLen || !rowPath.HasPrefix(&path) {
			return nil
		}
		copied := bytes.Clone(value)
		rows[string(key)] = copied
		taskRows++
		taskBytes += uint64(len(key) + len(copied))
		if taskBytes-heldBytes >= 1<<20 {
			hold()
		}
		return nil
	})
	hold()
	if t.verifyStats != nil {
		t.verifyStats.RangeReads.Add(1)
		t.verifyStats.RangeRows.Add(count)
	}
	if err != nil {
		if t.verifyStats != nil {
			t.verifyStats.releaseRows(taskRows, taskBytes)
		}
		return err
	}
	t.verifyRows = rows
	t.verifyRowsUsed = make(map[string]struct{}, len(rows))
	t.verifyRowsExact = false
	t.verifyTaskRows = taskRows
	t.verifyTaskBytes = taskBytes
	t.verifyTaskLoaded = true
	if t.verifyStats != nil {
		t.verifyStats.TaskRowsLoaded.Add(1)
		t.verifyStats.recordTaskRows(t.verifyTaskRows)
	}
	return nil
}

func (t *Trie) releaseVerifyRows() {
	if !t.verifyTaskLoaded {
		return
	}
	if t.verifyStats != nil {
		t.verifyStats.TaskRowsReleased.Add(1)
		t.verifyStats.releaseRows(t.verifyTaskRows, t.verifyTaskBytes)
	}
	t.verifyRows = nil
	t.verifyRowsUsed = nil
	t.verifyRowsExact = false
	t.verifyTaskRows = 0
	t.verifyTaskBytes = 0
	t.verifyTaskLoaded = false
}

func (t *Trie) loadVerifyExact(key []byte) error {
	reader, ok := t.verifyRangeReader()
	if !ok {
		return nil
	}
	if t.verifyStats != nil {
		t.verifyStats.ExactRangeReads.Add(1)
	}
	if t.verifyStats != nil {
		t.verifyStats.RangeTasks.Add(1)
	}
	from := bytes.Clone(key)
	to := append(bytes.Clone(key), 0)
	rows := make(map[string][]byte, 1)
	count, err := reader.ReadVerifyRange(from, to, func(rowKey, value []byte) error {
		if bytes.Equal(rowKey, key) {
			rows[string(rowKey)] = bytes.Clone(value)
		}
		return nil
	})
	if t.verifyStats != nil {
		t.verifyStats.RangeReads.Add(1)
		t.verifyStats.RangeRows.Add(count)
	}
	if err != nil {
		return err
	}
	t.verifyRows = rows
	if t.verifyRowsUsed == nil {
		t.verifyRowsUsed = make(map[string]struct{}, len(rows))
	}
	t.verifyRowsExact = true
	return nil
}

func (t *Trie) verifyRowsComplete() error {
	for key := range t.verifyRows {
		if _, ok := t.verifyRowsUsed[key]; !ok {
			return fmt.Errorf("unreferenced verification row %x", []byte(key))
		}
	}
	if !t.verifyRowsExact {
		t.verifyReferencedKeys += uint64(len(t.verifyRows))
	}
	return nil
}

func (t *Trie) verifyBranch(key []byte) ([]byte, kv.Step, error) {
	if t.verifyOnly && t.verifyUpperRows != nil {
		if data, ok := t.verifyUpperRows[string(key)]; ok {
			if t.verifyUpperRowsUsed == nil {
				t.verifyUpperRowsUsed = make(map[string]struct{})
			}
			if _, used := t.verifyUpperRowsUsed[string(key)]; !used {
				t.verifyUpperRowsUsed[string(key)] = struct{}{}
				if !t.verifyKeyOwnedByTask(key) {
					t.verifyReferencedKeys++
				}
			}
			return data, 0, nil
		}
	}
	if t.verifyOnly {
		data, ok := t.verifyRows[string(key)]
		if !ok && (t.verifyRows == nil || t.verifyRowsExact) {
			if err := t.loadVerifyExact(key); err != nil {
				return nil, 0, err
			}
			data, ok = t.verifyRows[string(key)]
		}
		if t.verifyRows != nil {
			if ok {
				keyString := string(key)
				if _, used := t.verifyRowsUsed[keyString]; !used {
					t.verifyRowsUsed[keyString] = struct{}{}
					if t.verifyRowsExact && !commitment.IsCommitmentStateKey(key) && !t.verifyKeyOwnedByTask(key) {
						t.verifyReferencedKeys++
					}
				}
			}
			return data, 0, nil
		}
	}
	if t.verifyOnly && t.verifyStats != nil {
		t.verifyStats.PointLookups.Add(1)
	}
	data, step, err := t.ctx.Branch(key)
	if t.verifyOnly {
		data = bytes.Clone(data)
	}
	return data, step, err
}

func verifyRangeBounds(path *eip8297.Bitpath) ([]byte, []byte) {
	from := path.AppendPackedBits(nil)
	to := append([]byte(nil), from...)
	if used := path.BitLen % 8; used != 0 {
		to[len(to)-1] |= byte(0xff >> uint(used))
	}
	to = append(to, bytes.Repeat([]byte{0xff}, eip8297.StorageKeyLength+1-len(to))...)
	return from, to
}

func (t *Trie) collectChainPaths(row *rowNode, paths *[]eip8297.Bitpath, upperBuckets map[string]struct{}) error {
	for slot := range row.cells {
		cell := row.cell(slot)
		if err := collectUpperBucketKey(row, slot, cell, upperBuckets); err != nil {
			return err
		}
		if cell.Kind != BranchCell {
			continue
		}
		path, err := rowChildPath(row, slot, cell.Prefix, branchSplit(row, cell))
		if err != nil {
			return err
		}
		if path.BitLen >= verifyChainBits {
			*paths = append(*paths, path)
			continue
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return err
		}
		if err := t.collectChainPaths(child, paths, upperBuckets); err != nil {
			return err
		}
	}
	return nil
}

func collectUpperBucketKey(row *rowNode, slot int, cell *rowCell, upperBuckets map[string]struct{}) error {
	var path eip8297.Bitpath
	switch cell.Kind {
	case LeafCell:
		if cell.Key[0] != eip8297.StorageZone {
			return nil
		}
		var err error
		if path, err = keyPath(cell.Key); err != nil {
			return err
		}
	case BranchCell:
		path = branchPath(row, slot, cell)
		if path.BitLen < 264 || pathByte(&path) != eip8297.StorageZone {
			return nil
		}
	default:
		return nil
	}
	bucketPath := path.Slice(0, 264)
	key, err := EncodeRowKey(&bucketPath)
	if err != nil {
		return err
	}
	upperBuckets[string(key)] = struct{}{}
	return nil
}

func (t *Trie) loadUpperBucketRows(upperBuckets map[string]struct{}) error {
	reader, ok := t.verifyRangeReader()
	if !ok || t.verifyUpperRows == nil {
		return nil
	}
	for bucketKey := range upperBuckets {
		key := []byte(bucketKey)
		if t.verifyStats != nil {
			t.verifyStats.RangeTasks.Add(1)
			t.verifyStats.RangeTaskReads.Add(1)
		}
		count, err := reader.ReadVerifyRange(key, append(bytes.Clone(key), 0), func(rowKey, value []byte) error {
			if bytes.Equal(rowKey, key) {
				t.verifyUpperRows[bucketKey] = bytes.Clone(value)
			}
			return nil
		})
		if t.verifyStats != nil {
			t.verifyStats.RangeReads.Add(1)
			t.verifyStats.RangeRows.Add(count)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) verifyBranchChild(row *rowNode, slot int) (*rowNode, FoldResult, error) {
	if t.verifiedChains != nil && row.path.BitLen < verifyChainBits {
		cell := row.cell(slot)
		path, err := rowChildPath(row, slot, cell.Prefix, branchSplit(row, cell))
		if err != nil {
			return nil, FoldResult{}, err
		}
		if path.BitLen >= verifyChainBits {
			key, err := rowKeyForPath(&path)
			if err != nil {
				return nil, FoldResult{}, err
			}
			chain, ok := t.verifiedChains[string(key)]
			if !ok {
				return nil, FoldResult{}, fmt.Errorf("chain row %x was not verified", key)
			}
			return chain.row, chain.result, chain.err
		}
	}
	child, err := t.loadBranchChild(row, slot)
	if err != nil {
		return nil, FoldResult{}, err
	}
	result, err := t.verifyRow(child)
	return child, result, err
}

func (t *Trie) verify() error {
	if t.verifyStats != nil {
		t.verifyStats.UpperRowsAtStart.Store(t.verifyStats.LiveRows.Load())
	}
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	t.trackBucketKeys()
	if root.row == nil && root.form == RowRoot {
		return t.verifyBuckets()
	}
	var verifyErr error
	switch root.form {
	case LeafRoot:
		verifyErr = t.verifyRootRecord()
		if verifyErr == nil && root.leaf.Key[0] == eip8297.StorageZone {
			path, err := keyPath(root.leaf.Key)
			if err != nil {
				verifyErr = err
			} else {
				bucketPath := path.Slice(0, 264)
				verifyErr = t.verifyBucketRecordPath(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: root.leaf})
			}
		}
	case ExtRoot:
		if err := t.verifyRootRecord(); err != nil {
			verifyErr = err
			break
		}
		row, err := t.extTopRow(root)
		if err != nil {
			verifyErr = err
			break
		}
		result, err := t.verifyRow(row)
		if err != nil {
			verifyErr = err
			break
		}
		if result.Split != root.self.BitLen || result.Left != root.left || result.Right != root.right {
			verifyErr = fmt.Errorf("root extension does not match its top row")
			break
		}
		wantSelf, err := rowTopPrefix(row, result.Split)
		if err != nil {
			verifyErr = err
			break
		}
		if root.self != wantSelf {
			verifyErr = fmt.Errorf("root extension prefix does not match its top row")
		}
		if verifyErr == nil && root.self.BitLen >= 264 && pathByte(&root.self) == eip8297.StorageZone {
			bucketPath := root.self.Slice(0, 264)
			verifyErr = t.verifyBucketBranch(&bucketPath, &root.self, row, root.left, root.right)
		}
	case RowRoot:
		_, verifyErr = t.verifyRow(root.row)
	default:
		verifyErr = fmt.Errorf("unknown root form %d", root.form)
	}
	if verifyErr != nil {
		return verifyErr
	}
	return t.verifyBuckets()
}

func (t *Trie) verifyRootRecord() error {
	record := t.rootRecord()
	rootKey := t.rootRecordKey()
	data, err := EncodeRecord(rootKey, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, t.root.raw) {
		return fmt.Errorf("root record is not canonical")
	}
	decoded, err := DecodeRecord(rootKey, data)
	if err != nil {
		return err
	}
	canonical, err := EncodeRecord(rootKey, &decoded)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, canonical) {
		return fmt.Errorf("root record round trip changed its value")
	}
	return nil
}

func (t *Trie) verifyRow(row *rowNode) (FoldResult, error) {
	if t.verifyProgress != nil {
		t.verifyProgress(row.key)
	}
	record := row.record()
	data, err := EncodeRecord(row.key, &record)
	if err != nil {
		return FoldResult{}, err
	}
	if len(row.raw) != 0 && !bytes.Equal(data, row.raw) && !row.dirty {
		return FoldResult{}, fmt.Errorf("stored row %x changed without a mutation", row.key)
	}
	decoded, err := DecodeRecord(row.key, data)
	if err != nil {
		return FoldResult{}, err
	}
	canonical, err := EncodeRecord(row.key, &decoded)
	if err != nil {
		return FoldResult{}, err
	}
	if !bytes.Equal(data, canonical) {
		return FoldResult{}, fmt.Errorf("row %x is not canonical", row.key)
	}
	for slot := range row.cells {
		cell := row.cell(slot)
		if cell.Kind == LeafCell {
			if row.path.BitLen < 264 && cell.Key[0] == eip8297.StorageZone {
				path, err := keyPath(cell.Key)
				if err != nil {
					return FoldResult{}, err
				}
				bucketPath := path.Slice(0, 264)
				if err := t.verifyBucketRecordPath(&bucketPath, bucketDescriptor{form: LeafRoot, leaf: *cell.Cell}); err != nil {
					return FoldResult{}, err
				}
			}
			continue
		}
		if cell.Kind != BranchCell {
			continue
		}
		child, childResult, err := t.verifyBranchChild(row, slot)
		if err != nil {
			return FoldResult{}, err
		}
		top, err := rowTopPrefix(child, childResult.Split)
		if err != nil {
			return FoldResult{}, err
		}
		wantPrefix := top.Slice(row.path.BitLen+4, childResult.Split)
		if cell.Prefix != wantPrefix || cell.Left != childResult.Left || cell.Right != childResult.Right {
			return FoldResult{}, fmt.Errorf("row %x cell %d does not match its child", row.key, slot)
		}
		full := branchPath(row, slot, cell)
		if row.path.BitLen < 264 && full.BitLen >= 264 && pathByte(&full) == eip8297.StorageZone {
			bucketPath := full.Slice(0, 264)
			if err := t.verifyBucketBranch(&bucketPath, &full, child, cell.Left, cell.Right); err != nil {
				return FoldResult{}, err
			}
		}
		cell.child = nil
		child.parent = nil
	}
	return rowFoldResult(row)
}

func (t *Trie) verifyBuckets() error {
	records, ok := t.ctx.(interface{ Records() map[string][]byte })
	if !ok {
		return nil
	}
	for key, data := range records.Records() {
		if len(data) == 0 {
			continue
		}
		if _, err := bucketPathForKey([]byte(key)); err == nil {
			if _, found := t.verifiedBucketKeys[key]; !found {
				return fmt.Errorf("orphan bucket record %x", []byte(key))
			}
		}
	}
	return nil
}

func (t *Trie) verifyBucketBranch(bucketPath, branchPath *eip8297.Bitpath, row *rowNode, left, right common.Hash) error {
	if branchPath.BitLen < bucketPath.BitLen+4 {
		return t.verifyBucketRecordPath(bucketPath, bucketDescriptor{form: RowRoot, row: row})
	}
	return t.verifyBucketRecordPath(bucketPath, bucketDescriptor{form: ExtRoot, self: branchPath.Slice(bucketPath.BitLen, branchPath.BitLen), left: left, right: right})
}

func (t *Trie) verifyBucketRecordPath(path *eip8297.Bitpath, descriptor bucketDescriptor) error {
	key, err := EncodeRowKey(path)
	if err != nil {
		return err
	}
	if t.verifiedBucketKeys != nil {
		if _, found := t.verifiedBucketKeys[string(key)]; found {
			return nil
		}
	}
	if err := t.verifyBucketRecord(key, descriptor); err != nil {
		return err
	}
	if t.verifiedBucketKeys != nil {
		t.verifiedBucketKeys[string(key)] = struct{}{}
	}
	return nil
}

func (t *Trie) verifyBucketRecord(key []byte, descriptor bucketDescriptor) error {
	data, _, err := t.verifyBranch(key)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("bucket record %x is missing", key)
	}
	record := descriptor.record()
	wantData, err := EncodeRecord(key, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, wantData) {
		return fmt.Errorf("bucket record %x does not match its upper cell", key)
	}
	if _, err := DecodeRecord(key, data); err != nil {
		return err
	}
	return nil
}
