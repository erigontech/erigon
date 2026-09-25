// Copyright 2026 The Erigon Authors
// This file is part of the Erigon project.
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
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/erigontech/erigon/db/kv"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

func benchUpdatesIn(dir string, mode commitment.Mode, entries []parityUpdate) *commitment.Updates {
	u := commitment.NewUpdates(mode, dir, commitment.KeyToHexNibbleHash)
	for _, e := range entries {
		u.TouchPlainKeyDirect(string(e.key), e.update)
	}
	return u
}

func benchAddr(i int) []byte {
	a := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(a[:8], uint64(i)*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(a[8:16], uint64(i))
	return a
}

func benchSlot(i int) []byte {
	s := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(s[:8], uint64(i)*0xC2B2AE3D27D4EB4F)
	binary.BigEndian.PutUint64(s[8:16], uint64(i))
	return s
}

func benchEntries(shape string, n int) []parityUpdate {
	switch shape {
	case "accounts":
		out := make([]parityUpdate, n)
		for i := range out {
			out[i] = parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)}
		}
		return out
	case "storage":
		out := make([]parityUpdate, 0, n*2)
		for i := range n {
			out = append(out,
				parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)},
				parityUpdate{key: append(benchAddr(i), benchSlot(i)...), update: storageParityUpdate(i)})
		}
		return out
	case "whale":
		rnd := rand.New(rand.NewSource(424242))
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		out := make([]parityUpdate, 0, n+1)
		out = append(out, parityUpdate{key: addr, update: accountParityUpdate(1)})
		for i := range n {
			slot := make([]byte, length.Hash)
			rnd.Read(slot)
			out = append(out, parityUpdate{key: append(append([]byte{}, addr...), slot...), update: storageParityUpdate(i)})
		}
		return out
	case "whale_mixed":
		rnd := rand.New(rand.NewSource(99))
		out := make([]parityUpdate, 0, n+2000)
		for i := range 1000 {
			out = append(out, parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)})
		}
		waddr := make([]byte, length.Addr)
		rnd.Read(waddr)
		out = append(out, parityUpdate{key: waddr, update: accountParityUpdate(7)})
		for i := range n {
			slot := make([]byte, length.Hash)
			rnd.Read(slot)
			out = append(out, parityUpdate{key: append(append([]byte{}, waddr...), slot...), update: storageParityUpdate(i)})
		}
		for i := range 1000 {
			out = append(out, parityUpdate{key: benchAddr(500000 + i), update: accountParityUpdate(i)})
		}
		return out
	default:
		out := make([]parityUpdate, 0, n*2)
		for i := range n {
			out = append(out,
				parityUpdate{key: benchAddr(i), update: accountParityUpdate(i)},
				parityUpdate{key: append(benchAddr(i), benchSlot(i)...), update: storageParityUpdate(i)})
		}
		return out
	}
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
