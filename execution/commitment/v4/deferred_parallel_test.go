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

package v4

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

func countingFactory(c *shardedContext, calls *atomic.Int32) commitment.TrieContextFactory {
	return func(ctx context.Context) (commitment.PatriciaContext, func()) {
		calls.Add(1)
		return c.factory(ctx)
	}
}

func storeSnapshot(c *shardedContext) map[string]string {
	out := make(map[string]string)
	for i := range c.shards {
		c.shards[i].mu.Lock()
		for k, v := range c.shards[i].branches {
			out[k] = string(v)
		}
		c.shards[i].mu.Unlock()
	}
	return out
}

func TestDeferredProcessBuildsWorkerContexts(t *testing.T) {
	entries := benchEntries("storage", 256)
	var calls atomic.Int32
	c := newShardedContext()
	tr := &Trie{}
	tr.ResetContext(c)
	tr.SetTrieContextFactory(countingFactory(c, &calls))
	tr.SetDeferCommitmentUpdates(true)
	defer tr.Release()

	u := benchUpdatesIn(t.TempDir(), commitment.ModeCollect, entries)
	_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Greater(t, calls.Load(), int32(1),
		"deferred rounds must still build per-worker contexts for the storage phase")
}

func TestDeferredProcessMatchesInlineProcess(t *testing.T) {
	entries := benchEntries("storage", 256)

	inlineCtx := newShardedContext()
	inline := &Trie{}
	inline.ResetContext(inlineCtx)
	inline.SetTrieContextFactory(inlineCtx.factory)
	defer inline.Release()
	inlineRoot, err := inline.Process(context.Background(),
		benchUpdatesIn(t.TempDir(), commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	deferredCtx := newShardedContext()
	deferred := &Trie{}
	deferred.ResetContext(deferredCtx)
	deferred.SetTrieContextFactory(deferredCtx.factory)
	deferred.SetDeferCommitmentUpdates(true)
	defer deferred.Release()
	deferredRoot, err := deferred.Process(context.Background(),
		benchUpdatesIn(t.TempDir(), commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	require.Equal(t, inlineRoot, deferredRoot)

	apply := deferred.TakeDeferredUpdates()
	require.NotNil(t, apply)
	require.NoError(t, apply(deferredCtx.PutBranch))

	require.Equal(t, storeSnapshot(inlineCtx), storeSnapshot(deferredCtx))
}

type workerBarrier struct {
	mu      sync.Mutex
	seen    int
	want    int
	release chan struct{}
	timeout time.Duration
}

func (b *workerBarrier) arrive() error {
	b.mu.Lock()
	b.seen++
	if b.seen == b.want {
		close(b.release)
	}
	b.mu.Unlock()

	select {
	case <-b.release:
		return nil
	case <-time.After(b.timeout):
		b.mu.Lock()
		seen := b.seen
		b.mu.Unlock()
		return fmt.Errorf("only %d of %d storage workers ever claimed a task", seen, b.want)
	}
}

type barrierContext struct {
	*shardedContext
	barrier *workerBarrier
	arrived bool
}

func (c *barrierContext) Branch(key []byte) ([]byte, kv.Step, error) {
	if !c.arrived {
		c.arrived = true
		if err := c.barrier.arrive(); err != nil {
			return nil, 0, err
		}
	}
	return c.shardedContext.Branch(key)
}

func TestStoragePhaseSpreadsTasksAcrossWorkers(t *testing.T) {
	const workers = 4
	inner := newShardedContext()
	barrier := &workerBarrier{want: workers, release: make(chan struct{}), timeout: 15 * time.Second}

	tr := &Trie{scheduleWorkers: workers}
	tr.ResetContext(inner)
	tr.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) {
		return &barrierContext{shardedContext: inner, barrier: barrier}, nil
	})
	defer tr.Release()

	u := benchUpdatesIn(t.TempDir(), commitment.ModeCollect, benchEntries("storage", 2*workers))
	_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
}
