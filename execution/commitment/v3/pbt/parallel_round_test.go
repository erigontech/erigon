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
	"flag"
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

var parallelRoundGateStart = time.Now()

func parallelRoundGateWait() time.Duration {
	wait := time.Minute
	if timeout := flag.Lookup("test.timeout"); timeout != nil {
		if limit, ok := timeout.Value.(flag.Getter).Get().(time.Duration); ok && limit > 0 {
			wait = min(wait, (limit-time.Since(parallelRoundGateStart))/2)
		}
	}
	return wait
}

func holdFirstTaskForSecond(trie *Trie) {
	holdFirstTaskForSecondWithin(trie, parallelRoundGateWait())
}

func holdFirstTaskForSecondWithin(trie *Trie, wait time.Duration) {
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
		case <-time.After(wait):
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
					snapshot = requireParallelRoundMatches(t, snapshot, tc.ops(r), workers, tc.concurrent, fmt.Sprintf("round=%d", round))
				}
			})
		}
	}
}

func requireParallelRoundMatches(t *testing.T, snapshot map[string][]byte, ops []Op, workers int, concurrent bool, label string) map[string][]byte {
	t.Helper()
	sequential := &trieTestContext{records: maps.Clone(snapshot)}
	wantRoot, err := NewTrie(sequential).Process(ops)
	require.NoError(t, err, label)
	base := newParallelRoundBase(snapshot)
	trie := base.trie()
	if concurrent {
		holdFirstTaskForSecond(trie)
	}
	gotRoot, err := trie.ProcessParallelContext(t.Context(), ops, workers)
	require.NotErrorIs(t, err, errParallelRoundSerial, label)
	require.NoError(t, err, label)
	require.Equal(t, wantRoot, gotRoot, label)
	require.Equal(t, sequential.records, base.main.records, label)
	require.Positive(t, base.servedViews(), "%s: views that served reads", label)
	return base.main.records
}

func parallelRoundKey(zone, first, tail, subIndex byte) []byte {
	stem := make([]byte, 32)
	stem[0] = first
	stem[31] = tail
	return eip8297.TreeKey(zone, stem, subIndex)
}

func parallelRoundRowKey(t *testing.T, path eip8297.Bitpath) []byte {
	t.Helper()
	key, err := EncodeRowKey(&path)
	require.NoError(t, err)
	return key
}

func parallelRoundRecord(t *testing.T, records map[string][]byte, key []byte) Record {
	t.Helper()
	data, ok := records[string(key)]
	require.True(t, ok, "record %x is missing", key)
	record, err := DecodeRecord(key, data)
	require.NoError(t, err)
	return record
}

func parallelRoundCells(record *Record) int {
	cells := 0
	for i := range record.Cells {
		if record.Cells[i].Kind != EmptyCell {
			cells++
		}
	}
	return cells
}

func runParallelRoundCollapse(t *testing.T, initial, ops []Op, before, after func(*testing.T, map[string][]byte)) {
	t.Helper()
	seed := newTrieTestContext()
	requireProcess(t, seed, sortParallelRoundOps(initial))
	before(t, seed.records)
	ops = sortParallelRoundOps(ops)
	for _, workers := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			after(t, requireParallelRoundMatches(t, seed.records, ops, workers, true, "collapse"))
		})
	}
}

func parallelRoundChainCollapseCorpus() (initial, ops []Op) {
	a1, a2 := parallelRoundKey(eip8297.AccountZone, 0x10, 1, 0), parallelRoundKey(eip8297.AccountZone, 0x11, 2, 0)
	b1, b2 := parallelRoundKey(eip8297.AccountZone, 0x20, 3, 0), parallelRoundKey(eip8297.AccountZone, 0x21, 4, 0)
	initial = []Op{{Key: a1, Value: testTrieValue(1)}, {Key: a2, Value: testTrieValue(2)}, {Key: b1, Value: testTrieValue(3)}, {Key: b2, Value: testTrieValue(4)}}
	ops = []Op{Drop(a1[:33]), Drop(a2[:33]), {Key: b1, Value: testTrieValue(9)}}
	return initial, ops
}

func TestParallelRoundDropsCollapseUpperRowOntoChangedChain(t *testing.T) {
	initial, ops := parallelRoundChainCollapseCorpus()
	zoneRow := parallelRoundRowKey(t, eip8297.PathFromBits([]byte{eip8297.AccountZone}, 8))
	changed := chainPrefix(eip8297.AccountZone, 2)
	runParallelRoundCollapse(t, initial, ops, func(t *testing.T, records map[string][]byte) {
		record := parallelRoundRecord(t, records, zoneRow)
		require.Equal(t, RowRoot, record.Form)
		require.Equal(t, 2, parallelRoundCells(&record))
	}, func(t *testing.T, records map[string][]byte) {
		require.NotContains(t, records, string(zoneRow))
		root := parallelRoundRecord(t, records, GlobalRootKey())
		require.Equal(t, ExtRoot, root.Form)
		require.True(t, pathHasPrefix(&root.SelfExt, &changed), "root extension %d bits does not reach the changed chain", root.SelfExt.BitLen)
	})
}

func TestParallelRoundEmptiedBucketCollapsesStorageChain(t *testing.T) {
	stemX := append([]byte{eip8297.StorageZone, 0x30}, make([]byte, 31)...)
	stemY := append([]byte{eip8297.StorageZone, 0x31}, make([]byte, 31)...)
	x1, x2 := storageKeyWithSuffix(stemX, 0x10, 1), storageKeyWithSuffix(stemX, 0x20, 2)
	y1, y2 := storageKeyWithSuffix(stemY, 0x10, 1), storageKeyWithSuffix(stemY, 0x20, 2)
	account := parallelRoundKey(eip8297.AccountZone, 0x10, 1, 0)
	initial := []Op{{Key: x1, Value: testTrieValue(1)}, {Key: x2, Value: testTrieValue(2)}, {Key: y1, Value: testTrieValue(3)}, {Key: y2, Value: testTrieValue(4)}, {Key: account, Value: testTrieValue(5)}}
	ops := []Op{{Key: x1}, {Key: x2}, {Key: y1, Value: testTrieValue(9)}}
	chainRow := parallelRoundRowKey(t, chainPrefix(eip8297.StorageZone, 3))
	bucketX := parallelRoundRowKey(t, eip8297.PathFromBits(stemX, 264))
	bucketY := parallelRoundRowKey(t, eip8297.PathFromBits(stemY, 264))
	runParallelRoundCollapse(t, initial, ops, func(t *testing.T, records map[string][]byte) {
		record := parallelRoundRecord(t, records, chainRow)
		require.Equal(t, 2, parallelRoundCells(&record))
		require.Contains(t, records, string(bucketX))
	}, func(t *testing.T, records map[string][]byte) {
		require.NotContains(t, records, string(chainRow))
		require.NotContains(t, records, string(bucketX))
		require.Contains(t, records, string(bucketY))
	})
}

func parallelRoundSplitBucketCorpus() (stem []byte, initial, ops []Op) {
	stem = append([]byte{eip8297.StorageZone, 0x40}, make([]byte, 31)...)
	for _, first := range []byte{0x10, 0x20, 0x30} {
		for last := range byte(4) {
			initial = append(initial, Op{Key: storageKeyWithSuffix(stem, first+last, last+1), Value: testTrieValue(first + last)})
		}
	}
	initial = append(initial, Op{Key: parallelRoundKey(eip8297.AccountZone, 0x10, 1, 0), Value: testTrieValue(5)})
	for _, first := range []byte{0x10, 0x20} {
		for last := range byte(4) {
			ops = append(ops, Op{Key: storageKeyWithSuffix(stem, first+last, last+1)})
		}
	}
	ops = append(ops, Op{Key: storageKeyWithSuffix(stem, 0x30, 1), Value: testTrieValue(99)})
	return stem, initial, ops
}

func TestParallelRoundSplitBucketKeepsOneSubtask(t *testing.T) {
	stem, initial, ops := parallelRoundSplitBucketCorpus()
	plan, err := buildPhasePlan(sortParallelRoundOps(slices.Clone(ops)))
	require.NoError(t, err)
	subtasks := 0
	for _, task := range plan.tasks {
		if task.kind == phaseBucketSubtask {
			subtasks++
		}
	}
	require.Equal(t, 3, subtasks)
	bucket := parallelRoundRowKey(t, eip8297.PathFromBits(stem, 264))
	runParallelRoundCollapse(t, initial, ops, func(t *testing.T, records map[string][]byte) {
		record := parallelRoundRecord(t, records, bucket)
		require.Equal(t, RowRoot, record.Form)
		require.Equal(t, 3, parallelRoundCells(&record))
	}, func(t *testing.T, records map[string][]byte) {
		require.Equal(t, ExtRoot, parallelRoundRecord(t, records, bucket).Form)
	})
}

func TestParallelRoundJoinRefusesTaskOwnedRecords(t *testing.T) {
	initial, chainOps := parallelRoundChainCollapseCorpus()
	stem, bucketInitial, bucketOps := parallelRoundSplitBucketCorpus()
	cases := []struct {
		name    string
		initial []Op
		ops     []Op
		join    func(phaseTask) bool
		owner   func(phaseTask) bool
		key     []byte
	}{
		{
			name:    "global",
			initial: initial,
			ops:     chainOps,
			join:    func(task phaseTask) bool { return task.kind == phaseJoin },
			owner: func(task phaseTask) bool {
				return task.kind == phaseChain && task.prefix == chainPrefix(eip8297.AccountZone, 2)
			},
			key: parallelRoundRowKey(t, chainPrefix(eip8297.AccountZone, 2)),
		},
		{
			name:    "bucket",
			initial: bucketInitial,
			ops:     bucketOps,
			join:    func(task phaseTask) bool { return task.kind == phaseBucket && len(task.dependencies) != 0 },
			owner: func(task phaseTask) bool {
				return task.kind == phaseBucketSubtask && task.prefix == eip8297.PathFromBits(append(slices.Clone(stem), 0x30), 268)
			},
			key: parallelRoundRowKey(t, eip8297.PathFromBits(append(slices.Clone(stem), 0x30), 268)),
		},
	}
	for _, tc := range cases {
		for _, write := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write=%v", tc.name, write), func(t *testing.T) {
				seed := newTrieTestContext()
				requireProcess(t, seed, sortParallelRoundOps(tc.initial))
				require.Contains(t, seed.records, string(tc.key))
				ops := sortParallelRoundOps(slices.Clone(tc.ops))
				plan, err := buildPhasePlan(ops)
				require.NoError(t, err)
				owner := slices.IndexFunc(plan.tasks, tc.owner)
				require.NotEqual(t, -1, owner)
				trie := newParallelRoundBase(seed.records).trie()
				joined := false
				trie.joinHook = func(task phaseTask, ctx commitment.PatriciaContext) error {
					if !tc.join(task) {
						return nil
					}
					joined = true
					if write {
						return ctx.PutBranch(tc.key, []byte{1}, seed.records[string(tc.key)])
					}
					_, _, err := ctx.Branch(tc.key)
					return err
				}
				_, err = trie.ProcessParallelContext(t.Context(), ops, 2)
				require.True(t, joined)
				require.ErrorContains(t, err, fmt.Sprintf("record %x is owned by task %d", tc.key, owner))
			})
		}
	}
}
