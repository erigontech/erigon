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
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type parallelRoundView struct {
	records map[string][]byte
	reads   int
}

func (v *parallelRoundView) Branch(key []byte) ([]byte, kv.Step, error) {
	v.reads++
	return v.records[string(key)], 0, nil
}

func (v *parallelRoundView) PutBranch(key, _, _ []byte) error {
	return fmt.Errorf("write %x to a read-only view", key)
}

func (v *parallelRoundView) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (v *parallelRoundView) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

type parallelRoundBase struct {
	snapshot map[string][]byte
	main     *trieTestContext
	mu       sync.Mutex
	views    []*parallelRoundView
}

func newParallelRoundBase(snapshot map[string][]byte) *parallelRoundBase {
	return &parallelRoundBase{snapshot: snapshot, main: &trieTestContext{records: maps.Clone(snapshot)}}
}

func (b *parallelRoundBase) trie() *Trie {
	trie := NewTrie(b.main)
	trie.SetTrieContextFactory(b.view)
	return trie
}

func (b *parallelRoundBase) view(context.Context) (commitment.PatriciaContext, func()) {
	view := &parallelRoundView{records: b.snapshot}
	b.mu.Lock()
	b.views = append(b.views, view)
	b.mu.Unlock()
	return view, nil
}

func (b *parallelRoundBase) servedViews() int {
	served := 0
	for _, view := range b.views {
		if view.reads != 0 {
			served++
		}
	}
	return served
}

type parallelRoundShape struct {
	accounts, code, storage, buckets, bigBuckets, bigMin, bigMax int
}

var parallelRoundTestShape = parallelRoundShape{accounts: 400, code: 100, storage: 500, buckets: 100, bigBuckets: 2, bigMin: 30, bigMax: 60}

type parallelRoundCorpus struct {
	keys     [][]byte
	stems    [][]byte
	snapshot map[string][]byte
}

func parallelRoundValue(r *rand.Rand, key []byte) (v [eip8297.ValueLength]byte) {
	r.Read(v[:])
	v[0] |= 1
	if key[0] == eip8297.AccountZone && key[33] == eip8297.BasicDataLeafKey {
		copy(v[:4], []byte{0, 0, 0, 0})
		v[31] |= 1
	}
	return v
}

func parallelRoundStem(r *rand.Rand) []byte {
	s := make([]byte, 32)
	r.Read(s)
	return s
}

func parallelRoundStorageKey(stem []byte, slot int) []byte {
	g := sha256.Sum256(append(bytes.Clone(stem), byte(slot>>8), byte(slot>>16), byte(slot>>24)))
	k := make([]byte, 0, 66)
	k = append(k, eip8297.StorageZone)
	k = append(k, stem...)
	k = append(k, g[:]...)
	return append(k, byte(slot))
}

func sortParallelRoundOps(ops []Op) []Op {
	slices.SortFunc(ops, func(a, b Op) int { return bytes.Compare(opKey(a), opKey(b)) })
	return ops
}

func newParallelRoundCorpus(tb testing.TB, shape parallelRoundShape) *parallelRoundCorpus {
	tb.Helper()
	r := rand.New(rand.NewSource(1))
	c := &parallelRoundCorpus{}
	var ops []Op
	add := func(k []byte) {
		c.keys = append(c.keys, k)
		ops = append(ops, Op{Key: k, Value: parallelRoundValue(r, k)})
	}
	for range shape.accounts / 2 {
		s := parallelRoundStem(r)
		add(eip8297.TreeKey(eip8297.AccountZone, s, 0))
		add(eip8297.TreeKey(eip8297.AccountZone, s, 1))
	}
	for range shape.code {
		add(eip8297.TreeKey(eip8297.CodeZone, parallelRoundStem(r), byte(r.Intn(256))))
	}
	sizes := make([]int, shape.buckets)
	total := 0
	for i := range shape.bigBuckets {
		sizes[i] = shape.bigMin + r.Intn(shape.bigMax-shape.bigMin+1)
		total += sizes[i]
	}
	for i := shape.bigBuckets; i < shape.buckets; i++ {
		sizes[i] = 1
		total++
	}
	for total < shape.storage {
		i := shape.bigBuckets + int(float64(shape.buckets-shape.bigBuckets)*r.Float64()*r.Float64()*r.Float64())
		if sizes[i] < 10 {
			sizes[i]++
			total++
		}
	}
	for _, size := range sizes {
		s := parallelRoundStem(r)
		c.stems = append(c.stems, s)
		for j := range size {
			add(parallelRoundStorageKey(s, j))
		}
	}
	require.Len(tb, ops, shape.accounts+shape.code+total)
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process(sortParallelRoundOps(ops))
	require.NoError(tb, err)
	c.snapshot = ctx.records
	return c
}

func (c *parallelRoundCorpus) updates(r *rand.Rand, k, inserts int) []Op {
	seen := make(map[int]struct{}, k)
	ops := make([]Op, 0, k)
	for len(ops) < k-inserts {
		i := r.Intn(len(c.keys))
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		ops = append(ops, Op{Key: c.keys[i], Value: parallelRoundValue(r, c.keys[i])})
	}
	for range inserts {
		var key []byte
		switch x := r.Float64(); {
		case x < 0.4:
			key = eip8297.TreeKey(eip8297.AccountZone, parallelRoundStem(r), byte(r.Intn(2)))
		case x < 0.5:
			key = eip8297.TreeKey(eip8297.CodeZone, parallelRoundStem(r), byte(r.Intn(256)))
		default:
			key = parallelRoundStorageKey(c.stems[r.Intn(len(c.stems))], 1_000_000+r.Intn(1_000_000))
		}
		ops = append(ops, Op{Key: key, Value: parallelRoundValue(r, key)})
	}
	return sortParallelRoundOps(ops)
}

func (c *parallelRoundCorpus) churn(r *rand.Rand, k, drops int) []Op {
	dropped := make(map[string]struct{}, drops)
	ops := make([]Op, 0, k+drops)
	for len(dropped) < drops {
		key := c.keys[r.Intn(len(c.keys))]
		if key[0] == eip8297.CodeZone {
			continue
		}
		if _, ok := dropped[string(key[:33])]; !ok {
			dropped[string(key[:33])] = struct{}{}
			ops = append(ops, Drop(key[:33]))
		}
	}
	seen := make(map[int]struct{}, k)
	for len(seen) < k {
		i := r.Intn(len(c.keys))
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		key := c.keys[i]
		if _, ok := dropped[string(key[:33])]; ok {
			continue
		}
		if r.Intn(3) == 0 {
			ops = append(ops, Op{Key: key})
		} else {
			ops = append(ops, Op{Key: key, Value: parallelRoundValue(r, key)})
		}
	}
	return sortParallelRoundOps(ops)
}

func parallelRoundConvertOps(r *rand.Rand, k int) []Op {
	ops := make([]Op, 0, k)
	for range k {
		s := parallelRoundStem(r)
		s[0] = 0x30 | s[0]&0x0f
		key := eip8297.TreeKey(eip8297.AccountZone, s, 0)
		ops = append(ops, Op{Key: key, Value: parallelRoundValue(r, key)})
	}
	return sortParallelRoundOps(ops)
}

var errParallelRoundSerial = errors.New("no second task started while the first task was in progress")

func holdFirstTaskForSecond(trie *Trie) {
	var started atomic.Int32
	both := make(chan struct{})
	trie.SetPhaseHook(func(_ phaseTask, op *Op) error {
		if op != nil {
			return nil
		}
		if started.Add(1) == 2 {
			close(both)
		}
		select {
		case <-both:
			return nil
		case <-time.After(5 * time.Second):
			return errParallelRoundSerial
		}
	})
}

func TestParallelRoundMatchesSequential(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	cases := []struct {
		name       string
		base       map[string][]byte
		concurrent bool
		ops        func(*rand.Rand) []Op
	}{
		{name: "tip", base: corpus.snapshot, concurrent: true, ops: func(r *rand.Rand) []Op { return corpus.updates(r, 200, 20) }},
		{name: "reexec", base: corpus.snapshot, concurrent: true, ops: func(r *rand.Rand) []Op { return corpus.updates(r, 600, 0) }},
		{name: "convert", base: map[string][]byte{}, ops: func(r *rand.Rand) []Op { return parallelRoundConvertOps(r, 300) }},
		{name: "churn", base: corpus.snapshot, concurrent: true, ops: func(r *rand.Rand) []Op { return corpus.churn(r, 300, 12) }},
	}
	for _, tc := range cases {
		for _, workers := range []int{2, 4, 8} {
			t.Run(fmt.Sprintf("%s/workers=%d", tc.name, workers), func(t *testing.T) {
				r := rand.New(rand.NewSource(int64(100 + workers)))
				snapshot := tc.base
				for round := range 3 {
					ops := tc.ops(r)
					sequential := &trieTestContext{records: maps.Clone(snapshot)}
					wantRoot, err := NewTrie(sequential).Process(ops)
					require.NoError(t, err, "round=%d", round)

					base := newParallelRoundBase(snapshot)
					trie := base.trie()
					if tc.concurrent {
						holdFirstTaskForSecond(trie)
					}
					gotRoot, err := trie.ProcessParallelContext(t.Context(), ops, workers)
					require.NotErrorIs(t, err, errParallelRoundSerial, "round=%d", round)
					require.NoError(t, err, "round=%d", round)
					require.Equal(t, wantRoot, gotRoot, "round=%d", round)
					require.Equal(t, sequential.records, base.main.records, "round=%d", round)
					require.Positive(t, base.servedViews(), "round=%d views that served reads", round)
					snapshot = sequential.records
				}
			})
		}
	}
}
