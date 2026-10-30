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
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/stretchr/testify/require"
)

type pbinVerifyMemoryContext struct {
	records map[string][]byte
	reads   atomic.Int64
}

func (c *pbinVerifyMemoryContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads.Add(1)
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *pbinVerifyMemoryContext) PutBranch(key, data, _ []byte) error {
	if len(data) == 0 {
		delete(c.records, string(key))
		return nil
	}
	c.records[string(key)] = bytes.Clone(data)
	return nil
}

func (c *pbinVerifyMemoryContext) Account([]byte) (*commitment.Update, error) {
	return nil, nil
}

func (c *pbinVerifyMemoryContext) Storage([]byte) (*commitment.Update, error) {
	return nil, nil
}

func TestPBinVerifierWithoutRecordsDoesNotAllocateBucketKeyIndex(t *testing.T) {
	ctx := &pbinVerifyMemoryContext{records: make(map[string][]byte)}
	trie := NewTrie(ctx)
	_, err := trie.Process([]Op{{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)}})
	require.NoError(t, err)
	verifier := trie.newVerifier()
	require.NoError(t, verifier.verify())
	require.Nil(t, verifier.verifiedBucketKeys)
}

type pbinRangeContext struct {
	commitment.PatriciaContext
	records   map[string][]byte
	emitted   *atomic.Uint64
	finalized *atomic.Uint64
	afterEmit func(uint64, uint64)
}

func (c *pbinRangeContext) ReadVerifyRange(from, to []byte, emit func([]byte, []byte) error) (uint64, error) {
	keys := make([]string, 0, len(c.records))
	for key := range c.records {
		if bytes.Compare([]byte(key), from) >= 0 && bytes.Compare([]byte(key), to) < 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var count, size uint64
	for _, key := range keys {
		data := bytes.Clone(c.records[key])
		if c.finalized != nil {
			counter := c.finalized
			runtime.SetFinalizer(&data[0], func(*byte) { counter.Add(1) })
		}
		if err := emit([]byte(key), data); err != nil {
			return count, err
		}
		count++
		size += uint64(len(key) + len(data))
		if c.emitted != nil {
			c.emitted.Add(1)
		}
		if c.afterEmit != nil {
			c.afterEmit(count, size)
		}
	}
	return count, nil
}

func TestPBinReleasedTaskRowsUnreachable(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	base := &trieTestContext{records: corpus.snapshot}
	verifier := NewTrie(base).newVerifier()
	stats := new(VerifyStats)
	verifier.SetVerifyStats(stats)
	var emitted, finalized atomic.Uint64
	require.NoError(t, verifier.verifyChains(t.Context(), 2, func(context.Context) (commitment.PatriciaContext, func()) {
		return &pbinRangeContext{PatriciaContext: base, records: corpus.snapshot, emitted: &emitted, finalized: &finalized}, nil
	}))
	require.Greater(t, len(verifier.verifiedChains), 2)
	require.Positive(t, emitted.Load())
	require.Equal(t, stats.TaskRowsLoaded.Load(), stats.TaskRowsReleased.Load())
	require.Zero(t, stats.LiveRows.Load())
	for range 10 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	retained := emitted.Load() - finalized.Load()
	runtime.KeepAlive(verifier)
	require.Zero(t, retained, "released task row buffers remain reachable: retained=%d, emitted=%d, finalized=%d, rows_live=%d", retained, emitted.Load(), finalized.Load(), stats.LiveRows.Load())
	require.NoError(t, verifier.verify())
}

func TestPBinLiveRowsDuringLoad(t *testing.T) {
	const (
		holdChunkBytes = 1 << 20
		rowValueBytes  = 1 << 19
	)
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	base := &trieTestContext{records: corpus.snapshot}
	verifier := NewTrie(base).newVerifier()
	root, err := verifier.loadRoot()
	require.NoError(t, err)
	var paths []eip8297.Bitpath
	require.NoError(t, verifier.collectChainPaths(root.row, &paths, make(map[string]struct{})))
	require.NotEmpty(t, paths)
	stats := new(VerifyStats)
	verifier.SetVerifyStats(stats)
	records := make(map[string][]byte)
	for key := range corpus.snapshot {
		path, err := eip8297.DecodeBitPath([]byte(key))
		if err == nil && path.BitLen >= paths[0].BitLen && path.HasPrefix(&paths[0]) {
			records[key] = make([]byte, rowValueBytes)
		}
	}
	var actualRows, actualBytes, liveRows, liveBytes uint64
	verifier.ctx = &pbinRangeContext{PatriciaContext: base, records: records, afterEmit: func(rows, size uint64) {
		actualRows, actualBytes = rows, size
		liveRows, liveBytes = stats.LiveRows.Load(), stats.LiveBytes.Load()
	}}
	require.NoError(t, verifier.loadVerifyRows(paths[0]))
	defer verifier.releaseVerifyRows()
	require.Positive(t, actualRows)
	require.Greater(t, actualBytes, uint64(holdChunkBytes))
	require.Equal(t, actualRows, verifier.verifyTaskRows)
	require.Less(t, actualRows-liveRows, uint64(holdChunkBytes/rowValueBytes), "during range load: rows_live=%d, retained_rows=%d; bytes_live=%d, retained_bytes=%d", liveRows, actualRows, liveBytes, actualBytes)
	require.Less(t, actualBytes-liveBytes, uint64(holdChunkBytes), "during range load: rows_live=%d, retained_rows=%d; bytes_live=%d, retained_bytes=%d", liveRows, actualRows, liveBytes, actualBytes)
	require.Equal(t, verifier.verifyTaskRows, stats.LiveRows.Load())
	require.Equal(t, verifier.verifyTaskBytes, stats.LiveBytes.Load())
}
