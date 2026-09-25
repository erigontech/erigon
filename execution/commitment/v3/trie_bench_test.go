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

package v3

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/erigontech/erigon/db/kv"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func benchUpdatesIn(dir string, mode commitment.Mode, entries []parityUpdate) *commitment.Updates {
	u := commitment.NewUpdates(mode, dir, commitment.KeyToHexNibbleHash)
	for _, e := range entries {
		u.TouchPlainKeyDirect(string(e.key), e.update)
	}
	return u
}

func benchAddr(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: 20}, i)
}

func benchSlot(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-slot", Size: 32}, i)
}

func benchEntries(shape string, n int) []parityUpdate {
	seed := int64(0)
	switch shape {
	case "accounts", "storage":
	case "whale":
		seed = 424242
	case "whale_mixed":
		seed = 99
	default:
		shape = "storage"
	}
	c, err := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: shape, Count: n})
	if err != nil {
		panic(err)
	}
	return parityEntries(c.Rounds[0])
}

func BenchmarkFoldV3VsHPH(b *testing.B) {
	ctxb := context.Background()
	for _, shape := range []string{"accounts", "storage", "whale"} {
		for _, n := range []int{10000, 100000} {
			entries := benchEntries(shape, n)

			b.Run(fmt.Sprintf("%s/%d/v3", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				for range b.N {
					b.StopTimer()
					c := newShardedContext()
					tr := &Trie{}
					tr.ResetContext(c)
					tr.SetTrieContextFactory(c.factory)
					b.StartTimer()
					u := benchUpdatesIn(dir, commitment.ModeCollect, entries)
					if _, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					tr.Release()
					b.StartTimer()
				}
			})

			b.Run(fmt.Sprintf("%s/%d/hph", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				for range b.N {
					b.StopTimer()
					c := newShardedContext()
					tr := commitment.NewHexPatriciaHashed(length.Addr, c, commitment.DefaultTrieConfig())
					b.StartTimer()
					u := benchUpdatesIn(dir, commitment.ModeUpdate, entries)
					if _, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					tr.Release()
					b.StartTimer()
				}
			})

			b.Run(fmt.Sprintf("%s/%d/parallel", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				for range b.N {
					b.StopTimer()
					c := newShardedContext()
					tr := commitment.NewParallelPatriciaHashed(c.factory, length.Addr, commitment.DefaultTrieConfig())
					b.StartTimer()
					u := benchUpdatesIn(dir, commitment.ModeParallel, entries)
					if _, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					tr.Release()
					b.StartTimer()
				}
			})
		}
	}
}

type shardedShard struct {
	mu       sync.Mutex
	branches map[string][]byte
	_        [24]byte
}

type shardedContext struct {
	shards [64]shardedShard
	empty  commitment.Update
}

func newShardedContext() *shardedContext {
	c := &shardedContext{empty: commitment.Update{Flags: commitment.DeleteUpdate}}
	for i := range c.shards {
		c.shards[i].branches = make(map[string][]byte)
	}
	return c
}

func (c *shardedContext) shard(key []byte) *shardedShard {
	h := uint32(2166136261)
	for _, b := range key {
		h = (h ^ uint32(b)) * 16777619
	}
	return &c.shards[h&63]
}

func (c *shardedContext) Branch(key []byte) ([]byte, kv.Step, error) {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.branches[string(key)]), 0, nil
}

func (c *shardedContext) PutBranch(key, data, _ []byte) error {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (c *shardedContext) Account([]byte) (*commitment.Update, error) { return c.empty.Copy(), nil }
func (c *shardedContext) Storage([]byte) (*commitment.Update, error) { return c.empty.Copy(), nil }

func (c *shardedContext) factory(context.Context) (commitment.PatriciaContext, func()) { return c, nil }

var _ commitment.PatriciaContext = (*shardedContext)(nil)

func BenchmarkV3Workers(b *testing.B) {
	ctxb := context.Background()
	for _, shape := range []string{"storage", "whale"} {
		entries := benchEntries(shape, 100000)
		for _, w := range []int{1, 2, 4, 8, 18, 36} {
			b.Run(fmt.Sprintf("%s/%d", shape, w), func(b *testing.B) {
				dir := b.TempDir()
				for range b.N {
					b.StopTimer()
					c := newShardedContext()
					tr := &Trie{scheduleWorkers: w}
					tr.ResetContext(c)
					tr.SetTrieContextFactory(c.factory)
					b.StartTimer()
					u := benchUpdatesIn(dir, commitment.ModeCollect, entries)
					if _, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{}); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					tr.Release()
					b.StartTimer()
				}
			})
		}
	}
}
