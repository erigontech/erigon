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
	"maps"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func benchUpdatesIn(dir string, mode commitment.Mode, ops []commitmenttest.Op, updates []*commitment.Update) *commitment.Updates {
	u := commitment.NewUpdates(mode, dir, commitment.KeyToHexNibbleHash)
	for i, op := range ops {
		u.TouchPlainKeyDirect(string(op.Key), updates[i])
	}
	return u
}

func benchAddr(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: 20}, i)
}

func benchSlot(i int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-slot", Size: 32}, i)
}

func benchEntries(shape string, n int) []commitmenttest.Op {
	seed := int64(0)
	switch shape {
	case "accounts", "storage":
	case "whale":
		seed = 424242
	default:
		shape = "storage"
	}
	c := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: shape, Count: n})
	return c.Rounds[0]
}

func BenchmarkFoldV3VsHPH(b *testing.B) {
	ctxb := context.Background()
	for _, shape := range []string{"accounts", "storage", "whale"} {
		for _, n := range []int{10000, 100000} {
			entries := benchEntries(shape, n)
			updates := updatesOf(entries)

			b.Run(fmt.Sprintf("%s/%d/v3", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				timedProcess(b, func() (func() error, func()) {
					c := newShardedContext()
					tr := &Trie{}
					tr.ResetContext(c)
					tr.SetTrieContextFactory(c.factory)
					return func() error {
						u := benchUpdatesIn(dir, commitment.ModeCollect, entries, updates)
						_, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{})
						return err
					}, tr.Release
				})
			})

			b.Run(fmt.Sprintf("%s/%d/hph", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				timedProcess(b, func() (func() error, func()) {
					c := newShardedContext()
					tr := commitment.NewHexPatriciaHashed(length.Addr, c, commitment.DefaultTrieConfig())
					return func() error {
						u := benchUpdatesIn(dir, commitment.ModeUpdate, entries, updates)
						_, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{})
						return err
					}, tr.Release
				})
			})

			b.Run(fmt.Sprintf("%s/%d/parallel", shape, n), func(b *testing.B) {
				dir := b.TempDir()
				timedProcess(b, func() (func() error, func()) {
					c := newShardedContext()
					tr := commitment.NewParallelPatriciaHashed(c.factory, length.Addr, commitment.DefaultTrieConfig())
					return func() error {
						u := benchUpdatesIn(dir, commitment.ModeParallel, entries, updates)
						_, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{})
						return err
					}, tr.Release
				})
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
		updates := updatesOf(entries)
		for _, w := range []int{1, 2, 4, 8, 18, 36} {
			b.Run(fmt.Sprintf("%s/%d", shape, w), func(b *testing.B) {
				dir := b.TempDir()
				timedProcess(b, func() (func() error, func()) {
					c := newShardedContext()
					tr := &Trie{scheduleWorkers: w}
					tr.ResetContext(c)
					tr.SetTrieContextFactory(c.factory)
					return func() error {
						u := benchUpdatesIn(dir, commitment.ModeCollect, entries, updates)
						_, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{})
						return err
					}, tr.Release
				})
			})
		}
	}
}

func timedProcess(b *testing.B, prepare func() (process func() error, release func())) {
	for range b.N {
		b.StopTimer()
		process, release := prepare()
		b.StartTimer()
		if err := process(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		release()
		b.StartTimer()
	}
}

func BenchmarkV3Incremental(b *testing.B) {
	const contracts = 100000
	seed := make([]commitmenttest.Op, 0, contracts*5)
	next := make([]commitmenttest.Op, 0, contracts)
	for i := range contracts {
		addr := benchAddr(i)
		seed = append(seed, parityAccount(addr, i))
		for j := range 4 {
			seed = append(seed, commitmenttest.StorageAt(append(bytes.Clone(addr), benchSlot(i*4+j)...), i+j))
		}
		if i%5 == 0 {
			next = append(next,
				parityAccount(addr, i+1),
				commitmenttest.StorageAt(append(bytes.Clone(addr), benchSlot(i*4)...), i+7),
				commitmenttest.StorageAt(append(bytes.Clone(addr), benchSlot(contracts*4+i)...), i+9))
		}
	}
	for i := contracts; i < contracts+contracts/20; i++ {
		next = append(next, parityAccount(benchAddr(i), i))
	}

	base := newShardedContext()
	tr := &Trie{}
	tr.ResetContext(base)
	tr.SetTrieContextFactory(base.factory)
	if _, err := tr.Process(context.Background(), benchUpdatesIn(b.TempDir(), commitment.ModeCollect, seed, updatesOf(seed)), "", nil, commitment.WarmupConfig{}); err != nil {
		b.Fatal(err)
	}
	tr.Release()

	dir := b.TempDir()
	nextUpdates := updatesOf(next)
	b.ReportAllocs()
	b.ResetTimer()
	timedProcess(b, func() (func() error, func()) {
		c := newShardedContext()
		for s := range base.shards {
			c.shards[s].branches = maps.Clone(base.shards[s].branches)
		}
		tr := &Trie{}
		tr.ResetContext(c)
		tr.SetTrieContextFactory(c.factory)
		u := benchUpdatesIn(dir, commitment.ModeCollect, next, nextUpdates)
		return func() error {
			_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
			return err
		}, tr.Release
	})
}

type discardWrites struct{ commitment.PatriciaContext }

func (discardWrites) PutBranch([]byte, []byte, []byte) error { return nil }

func BenchmarkWitness(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	bed := newWitnessBed(b, witnessState(rng, witnessShape{accounts: 20000, slots: 8}))
	var keys witnessKeys
	for range 1000 {
		keys.plain = append(keys.plain, bed.accounts[rng.Intn(len(bed.accounts))], bed.slots[rng.Intn(len(bed.slots))])
	}
	for range 20 {
		keys.plain = append(keys.plain, craftKey(rng, length.Addr, nil, accountNibbles))
	}
	var batch []commitmenttest.Op
	for i := range 1200 {
		switch {
		case i < 800:
			batch = append(batch, commitmenttest.Op{Key: bed.accounts[rng.Intn(len(bed.accounts))], Account: randomWitnessAccount(rng)})
		case i < 1100:
			batch = append(batch, commitmenttest.DeleteAt(bed.slots[rng.Intn(len(bed.slots))]))
		default:
			addr := bed.accounts[rng.Intn(len(bed.accounts))]
			batch = append(batch, commitmenttest.Op{Key: append(bytes.Clone(addr), craftKey(rng, length.Hash, nil, slotNibbles(addr))...), Storage: randomWitnessValue(rng)})
		}
	}
	ctx := context.Background()
	dir := b.TempDir()
	for _, exclusion := range []bool{false, true} {
		name := map[bool]string{false: "canonical", true: "exclusion"}[exclusion]
		b.Run("nodes/"+name+"/hph", func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.hphMem.Open(ctx)
				hph := commitment.NewHexPatriciaHashed(length.Addr, reader, commitment.DefaultTrieConfig())
				require.NoError(b, hph.SetState(bed.hphState))
				u := commitment.NewUpdates(commitment.ModeUpdate, dir, commitment.KeyToHexNibbleHash)
				keys.touch(u)
				byHash, proved, root, err := hph.WitnessesByHash(ctx, u, exclusion)
				require.NoError(b, err)
				_, err = trie.WitnessNodesForKeysByHash(byHash, root, proved)
				require.NoError(b, err)
				u.Close()
			}
		})
		b.Run("nodes/"+name+"/v3", func(b *testing.B) {
			for b.Loop() {
				tr := bed.openV3(ctx)
				_, _, err := tr.RestoreState(bed.v3State)
				require.NoError(b, err)
				u := commitment.NewUpdates(commitment.ModeCollect, dir, commitment.KeyToHexNibbleHash)
				keys.touch(u)
				byHash, proved, root, err := tr.WitnessesByHash(ctx, u, exclusion)
				require.NoError(b, err)
				_, err = trie.WitnessNodesForKeysByHash(byHash, root, proved)
				require.NoError(b, err)
				u.Close()
			}
		})
	}

	bed.hphMem.Apply(batch)
	for _, trace := range []bool{false, true} {
		suffix := map[bool]string{false: "process", true: "process+trace"}[trace]
		b.Run("collapse/hph/"+suffix, func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.hphMem.Open(ctx)
				hph := commitment.NewHexPatriciaHashed(length.Addr, discardWrites{reader}, commitment.DefaultTrieConfig())
				require.NoError(b, hph.SetState(bed.hphState))
				if trace {
					hph.SetCollapseTracer(func([]byte, []byte) {})
				}
				u := commitment.NewUpdates(commitment.ModeUpdate, dir, commitment.KeyToHexNibbleHash)
				for _, op := range batch {
					u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
				}
				_, err := hph.Process(ctx, u, "", nil, commitment.WarmupConfig{})
				require.NoError(b, err)
				u.Close()
			}
		})
		b.Run("collapse/v3/"+suffix, func(b *testing.B) {
			for b.Loop() {
				reader, _ := bed.v3Mem.Open(ctx)
				tr := &Trie{}
				tr.ResetContext(discardWrites{reader})
				_, _, err := tr.RestoreState(bed.v3State)
				require.NoError(b, err)
				if trace {
					tr.SetCollapseTracer(func([]byte, []byte) {})
				}
				u := commitment.NewUpdates(commitment.ModeCollect, dir, commitment.KeyToHexNibbleHash)
				for _, op := range batch {
					u.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
				}
				_, err = tr.Process(ctx, u, "", nil, commitment.WarmupConfig{})
				require.NoError(b, err)
				u.Close()
			}
		})
	}
	items := make([]feedEntry, len(batch))
	for i, op := range batch {
		items[i] = feedEntry{plainKey: string(op.Key), hashedKey: commitment.KeyToHexNibbleHash(op.Key), update: runner.Update(op)}
	}
	slices.SortFunc(items, compareFeed)
	b.Run("collapse/v3/trace-only", func(b *testing.B) {
		reader, _ := bed.v3Mem.Open(ctx)
		for b.Loop() {
			require.NoError(b, traceCollapses(reader, items, func([]byte, []byte) {}))
		}
	})
}
