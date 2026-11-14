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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func BenchmarkFoldV3VsHPH(b *testing.B) {
	ctxb := context.Background()
	for _, shape := range []string{"accounts", "storage", "whale"} {
		for _, n := range []int{10000, 100000} {
			entries := benchEntries(shape, n)
			updates := updatesOf(entries)

			for _, arm := range []struct {
				name string
				mode commitment.Mode
				open func(*shardedContext) commitment.Trie
			}{
				{"v3", commitment.ModeCollect, func(c *shardedContext) commitment.Trie { return newFactoryTrie(0, c, c.factory) }},
				{"hph", commitment.ModeUpdate, func(c *shardedContext) commitment.Trie {
					return commitment.NewHexPatriciaHashed(length.Addr, c, commitment.DefaultTrieConfig())
				}},
				{"parallel", commitment.ModeParallel, func(c *shardedContext) commitment.Trie {
					return commitment.NewParallelPatriciaHashed(c.factory, length.Addr, commitment.DefaultTrieConfig())
				}},
			} {
				b.Run(fmt.Sprintf("%s/%d/%s", shape, n, arm.name), func(b *testing.B) {
					dir := b.TempDir()
					timedProcess(b, func() (func() error, func()) {
						tr := arm.open(newShardedContext())
						return func() error {
							u := benchUpdatesIn(dir, arm.mode, entries, updates)
							_, err := tr.Process(ctxb, u, "", nil, commitment.WarmupConfig{})
							return err
						}, tr.Release
					})
				})
			}
		}
	}
}

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
					tr := newFactoryTrie(w, c, c.factory)
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
	tr := newFactoryTrie(0, base, base.factory)
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
		tr := newFactoryTrie(0, c, c.factory)
		u := benchUpdatesIn(dir, commitment.ModeCollect, next, nextUpdates)
		return func() error {
			_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
			return err
		}, tr.Release
	})
}

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
			batch = append(batch, commitmenttest.Op{Key: append(bytes.Clone(addr), craftKey(rng, length.Hash, nil, slotNibbles(addr))...), Storage: commitmenttest.RandomStorage(rng)})
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
