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
	"maps"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var errVerifyChainReadsSerialized = errors.New("no second chain row read arrived while the first was in progress")

type verifyRendezvousContext struct {
	commitment.PatriciaContext
	arrived *atomic.Int32
	met     chan struct{}
	wait    time.Duration
}

func (c *verifyRendezvousContext) Branch(key []byte) ([]byte, kv.Step, error) {
	if path, err := eip8297.DecodeBitPath(key); err == nil && path.BitLen >= 12 {
		if c.arrived.Add(1) == 2 {
			close(c.met)
		}
		select {
		case <-c.met:
		case <-time.After(c.wait):
			return nil, 0, errVerifyChainReadsSerialized
		}
	}
	return c.PatriciaContext.Branch(key)
}

func TestVerifyReadsChainRowsConcurrently(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	base := newParallelRoundBase(corpus.snapshot)
	var arrived atomic.Int32
	met := make(chan struct{})
	wait := parallelRoundGateWait()
	main := &verifyRendezvousContext{PatriciaContext: base.main, arrived: &arrived, met: met, wait: wait}
	err := NewTrie(main).VerifyContext(t.Context(), 4, func(ctx context.Context) (commitment.PatriciaContext, func()) {
		view, cleanup := base.view(ctx)
		return &verifyRendezvousContext{PatriciaContext: view, arrived: &arrived, met: met, wait: wait}, cleanup
	})
	require.NotErrorIs(t, err, errVerifyChainReadsSerialized)
	require.NoError(t, err)
}

func verifyParallelRows(t *testing.T, records map[string][]byte) []string {
	t.Helper()
	var keys []string
	for key, data := range records {
		path, err := eip8297.DecodeBitPath([]byte(key))
		if err != nil || path.BitLen <= 12 || path.BitLen >= 264 || len(data) == 0 {
			continue
		}
		record, err := DecodeRecord([]byte(key), data)
		require.NoError(t, err)
		if record.Form == RowRoot {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	require.NotEmpty(t, keys)
	return keys
}

func verifyBothWays(t *testing.T, records map[string][]byte) (sequential, parallel error) {
	t.Helper()
	var sequentialCalls, parallelCalls atomic.Int32
	trie := NewTrie(&trieTestContext{records: maps.Clone(records)})
	trie.SetVerifyProgress(func([]byte) { sequentialCalls.Add(1) })
	sequential = trie.Verify()
	base := newParallelRoundBase(maps.Clone(records))
	trie = NewTrie(base.main)
	trie.SetVerifyProgress(func([]byte) { parallelCalls.Add(1) })
	parallel = trie.VerifyContext(t.Context(), 4, base.view)
	require.Greater(t, base.servedViews(), 1, "the verify read no worker views")
	if sequential == nil {
		require.Equal(t, sequentialCalls.Load(), parallelCalls.Load(), "progress calls")
	}
	return sequential, parallel
}

func TestVerifyParallelMatchesSequential(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	t.Run("valid", func(t *testing.T) {
		sequential, parallel := verifyBothWays(t, corpus.snapshot)
		require.NoError(t, sequential)
		require.NoError(t, parallel)
	})
	t.Run("corrupt row", func(t *testing.T) {
		records := maps.Clone(corpus.snapshot)
		rows := verifyParallelRows(t, records)
		key := rows[len(rows)-1]
		record, err := DecodeRecord([]byte(key), records[key])
		require.NoError(t, err)
		corrupted := false
		for slot := range record.Cells {
			if record.Cells[slot].Kind == BranchCell {
				record.Cells[slot].Left[0]++
				corrupted = true
				break
			}
			if record.Cells[slot].Kind == LeafCell {
				record.Cells[slot].Value[1]++
				corrupted = true
				break
			}
		}
		require.True(t, corrupted)
		records[key], err = EncodeRecord([]byte(key), &record)
		require.NoError(t, err)
		sequential, parallel := verifyBothWays(t, records)
		require.Error(t, sequential)
		require.EqualError(t, parallel, sequential.Error())
	})
	t.Run("missing row", func(t *testing.T) {
		records := maps.Clone(corpus.snapshot)
		rows := verifyParallelRows(t, records)
		delete(records, rows[len(rows)-1])
		sequential, parallel := verifyBothWays(t, records)
		require.ErrorContains(t, sequential, "is missing")
		require.EqualError(t, parallel, sequential.Error())
	})
	t.Run("orphan bucket", func(t *testing.T) {
		records := maps.Clone(corpus.snapshot)
		orphan := newTrieTestContext()
		orphanKey := eip8297.TreeKeyStorage(bytes.Repeat([]byte{0xee}, 20), storageSlot(64))
		_, err := NewTrie(orphan).Process([]Op{{Key: orphanKey, Value: eip8297.EncodeStorageValue([]byte{2})}})
		require.NoError(t, err)
		bucket, err := bucketKeyForStorage(orphanKey)
		require.NoError(t, err)
		require.Empty(t, records[string(bucket)])
		records[string(bucket)] = bytes.Clone(orphan.records[string(bucket)])
		sequential, parallel := verifyBothWays(t, records)
		require.ErrorContains(t, sequential, "orphan bucket record")
		require.EqualError(t, parallel, sequential.Error())
	})
}

func TestVerifyParallelMissingUpperRowKeepsTraversalOrder(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	records := maps.Clone(corpus.snapshot)
	var upper []string
	for key, data := range records {
		path, err := eip8297.DecodeBitPath([]byte(key))
		if err == nil && path.BitLen > 0 && path.BitLen < 12 && len(data) != 0 {
			upper = append(upper, key)
		}
	}
	sort.Strings(upper)
	require.NotEmpty(t, upper)
	rows := verifyParallelRows(t, records)
	delete(records, upper[len(upper)-1])
	delete(records, rows[0])
	sequential := NewTrie(&trieTestContext{records: maps.Clone(records)}).Verify()
	require.ErrorContains(t, sequential, "is missing")
	base := newParallelRoundBase(records)
	parallel := NewTrie(base.main).VerifyContext(t.Context(), 4, base.view)
	require.EqualError(t, parallel, sequential.Error())
}
