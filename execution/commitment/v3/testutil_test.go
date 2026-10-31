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
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func testAccountUpdate(value commitmenttest.AccountValue) *commitment.Update {
	return runner.Update(commitmenttest.Op{Account: &value})
}

func storageUpdate(value []byte) *commitment.Update {
	return runner.Update(commitmenttest.Op{Storage: value})
}

func updatesOf(ops []commitmenttest.Op) []*commitment.Update {
	updates := make([]*commitment.Update, len(ops))
	for i, op := range ops {
		updates[i] = runner.Update(op)
	}
	return updates
}

func openTestTrie(ctx context.Context, spec runner.RunSpec) (commitment.Trie, error) {
	if spec.Mode != commitment.ModeCollect {
		return runner.OpenHPH(ctx, spec)
	}
	tr := &Trie{scheduleWorkers: spec.Workers}
	reader, _ := spec.Memory.Open(ctx)
	tr.ResetContext(reader)
	if !spec.NoContextFactory {
		tr.SetTrieContextFactory(spec.Memory.Open)
	}
	return tr, nil
}

func TestSharedRunnerCatalogue(t *testing.T) {
	for _, tc := range []struct {
		id    string
		kind  string
		count int
	}{
		{"E58-E101/process-100k-accounts", "accounts", 100000},
		{"E58-E101/process-100k-storage", "storage", 100000},
	} {
		t.Run(tc.id, func(t *testing.T) {
			c := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: tc.kind, Count: tc.count})
			c.ID = tc.id
			runner.Run(t, c, runner.RunSpec{Name: "v3", Mode: commitment.ModeCollect, Context: runner.ContextSpec{ForbidStateReads: true}}, openTestTrie)
		})
	}
}

func TestProcessFeedReadsNoState(t *testing.T) {
	c := commitmenttest.Generate(commitmenttest.MathRand(424242), commitmenttest.SequenceSpec{Kind: "whale", Count: 200})
	memory := runner.NewMemory(runner.ContextSpec{ForbidStateReads: true})
	engine, err := openTestTrie(context.Background(), runner.RunSpec{Mode: commitment.ModeCollect, Memory: memory})
	require.NoError(t, err)
	defer engine.Release()
	_, err = engine.(*Trie).ProcessFeed(context.Background(), feedOf(c.Rounds[0]), nil)
	require.NoError(t, err)
	require.Zero(t, memory.Counts().AccountReads)
	require.Zero(t, memory.Counts().StorageReads)
}

func newNode(plane byte, path ...byte) *node {
	n := fork(path)
	n.plane = plane
	return n
}

func (n *node) addLeaf(nib int, fill byte, value ...byte) []byte {
	full := append(append(bytes.Clone(n.path), byte(nib)), bytes.Repeat([]byte{fill}, 63-len(n.path))...)
	n.setLeaf(nib, packPath(full[len(n.path)+1:], nil), value)
	return full
}

func materializeNode(path []byte, plane byte, spec *commitmenttest.RecordSpec) *node {
	n := fork(path)
	n.plane = plane
	for nib := range 16 {
		if spec.ChildMask&(1<<nib) == 0 {
			continue
		}
		if spec.LeafMask&(1<<nib) != 0 {
			n.setLeaf(nib, spec.Leaves[nib].Suffix, spec.Leaves[nib].Value)
		} else {
			ext := spec.Extensions[nib]
			if len(ext) != 0 {
				ext = unpackPath(ext[1:], int(ext[0]), nil)
			}
			n.setStoredChild(nib, spec.Hashes[nib], ext)
		}
	}
	return n
}

func runStorageTask(ctx commitment.PatriciaContext, task storageTask) ([32]byte, error) {
	root, parts, err := runStorageTaskWithPlan(ctx, task, foldPlan{})
	if err != nil {
		return [32]byte{}, err
	}
	return root, applyDeltas(parts, ctx.PutBranch)
}

func entryOf(path []byte, update *commitment.Update) storageEntry {
	entry, err := storageEntryOf(path, update)
	if err != nil {
		panic(err)
	}
	return entry
}

func slotPath(prefix ...byte) []byte {
	paths := commitmenttest.Paths(commitmenttest.Shape{Prefixes: [][]byte{prefix}})
	return paths[0]
}

func phaseAStorageUpdate(value []byte) *commitment.Update { return storageUpdate(value) }

func storageTaskFor(addr [32]byte, ops []commitmenttest.Op) storageTask {
	task := storageTask{addrHash: addr, entries: make([]storageEntry, len(ops))}
	for i, op := range ops {
		task.entries[i] = entryOf(op.Key, runner.Update(op))
	}
	return task
}

func storageRound(t *testing.T, ctx commitment.PatriciaContext, addr [32]byte, ops []commitmenttest.Op) [32]byte {
	t.Helper()
	root, err := runStorageTask(ctx, storageTaskFor(addr, ops))
	require.NoError(t, err)
	return root
}

func storageOps(paths [][]byte) []commitmenttest.Op {
	ops := make([]commitmenttest.Op, len(paths))
	for i, path := range paths {
		ops[i] = commitmenttest.Op{Key: path, Storage: commitmenttest.Storage(commitmenttest.StorageSpec{Path: path})}
	}
	return ops
}

func exactLeaves(t *testing.T, ctx commitment.PatriciaContext, plane byte, addr [32]byte) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	var visit func([]byte)
	visit = func(path []byte) {
		key := AccountNodeKey(path, nil)
		if plane == planeStorage {
			key = StorageNodeKey(addr, path, nil)
		}
		data, _, err := ctx.Branch(key)
		data = bytes.Clone(data)
		require.NoError(t, err)
		if len(data) == 0 {
			require.Empty(t, path, "missing record at path %x", path)
			return
		}
		depth := len(path)
		require.NoError(t, Validate(data, depth), "invalid record at %x", path)
		r := Record{data: data, depth: depth}
		if r.isLeafRoot() {
			out[string(unpackPath(data[1:33], 64, nil))] = bytes.Clone(data[34:])
			return
		}
		l := r.layout()
		if depth == 0 && l.selfExtLen != 0 {
			visit(unpackPath(r.SelfExt()[1:], l.selfExtLen, nil))
			return
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if l.child&bit == 0 {
				continue
			}
			child := append(bytes.Clone(path), byte(nib))
			if l.leaf&bit == 0 {
				visit(append(child, decodeExtension(r.extAt(l, nib))...))
				continue
			}
			suffix, value := r.leafAt(l, nib)
			child = append(child, unpackPath(suffix, 64-depth-1, nil)...)
			require.NotContains(t, out, string(child), "duplicate leaf %x", child)
			out[string(child)] = bytes.Clone(value)
		}
	}
	visit(nil)
	return out
}

func liveStorageRecords(records map[string][]byte) map[string][]byte {
	for key, value := range records {
		if len(value) == 0 {
			delete(records, key)
		}
	}
	return records
}

func requireLiveRecords(t *testing.T, want, got map[string][]byte, label string) {
	t.Helper()
	want, got = liveStorageRecords(want), liveStorageRecords(got)
	for key, value := range got {
		wantValue, ok := want[key]
		require.True(t, ok, "%s: record %x is not in a trie rebuilt from the same state (%d records, rebuilt %d)", label, key, len(got), len(want))
		require.Equal(t, wantValue, value, "%s: record %x differs from the rebuilt trie's", label, key)
	}
	require.Len(t, got, len(want), "%s: record count", label)
}

func AccountNodeKey(path []byte, dst []byte) []byte {
	return nodeKey(tagAccountNode, nil, path, dst)
}

func testUpdates(t testing.TB, mode commitment.Mode, ops []commitmenttest.Op) *commitment.Updates {
	t.Helper()
	updates := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
	t.Cleanup(updates.Close)
	for _, op := range ops {
		updates.TouchPlainKeyDirect(string(op.Key), runner.Update(op))
	}
	return updates
}

func requireStorageState(t *testing.T, ctx commitment.PatriciaContext, addr [32]byte, state commitmenttest.State) {
	t.Helper()
	want := make(map[string][]byte, len(state))
	for _, op := range state.Ops() {
		want[string(op.Key)] = op.Storage
	}
	require.Equal(t, want, exactLeaves(t, ctx, planeStorage, addr))
}

func feedOf(ops []commitmenttest.Op) *commitment.Feed {
	feed := &commitment.Feed{Keys: len(ops)}
	index := make(map[string]int)
	for _, e := range ops {
		addr := string(e.Key[:length.Addr])
		at, ok := index[addr]
		if !ok {
			at = len(feed.Accounts)
			index[addr] = at
			feed.Accounts = append(feed.Accounts, commitment.FeedAccount{Hash: keccak.Sum256(e.Key[:length.Addr])})
		}
		account := &feed.Accounts[at]
		if len(e.Key) == length.Addr {
			account.Update = runner.Update(e)
			continue
		}
		slot := commitment.FeedSlot{Hash: keccak.Sum256(e.Key[length.Addr:])}
		if u := runner.Update(e); !u.Deleted() {
			slot.Value = u.Storage[:u.StorageLen]
		}
		account.Slots = append(account.Slots, slot)
	}
	return feed
}

type v3Config struct {
	trie     *Trie
	workers  int
	deferred bool
	feed     bool
	wrap     func(commitment.PatriciaContext) commitment.PatriciaContext
}

func runV3(t *testing.T, ctx commitment.PatriciaContext, cfg v3Config, rounds ...[]commitmenttest.Op) ([][]byte, []string, int32) {
	t.Helper()
	var calls atomic.Int32
	tr := cfg.trie
	if tr == nil {
		tr = &Trie{}
		defer tr.Release()
	}
	tr.scheduleWorkers = cfg.workers
	tr.ResetContext(ctx)
	tr.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) {
		calls.Add(1)
		if cfg.wrap != nil {
			return cfg.wrap(ctx), nil
		}
		return ctx, nil
	})
	tr.SetDeferCommitmentUpdates(cfg.deferred)
	var roots [][]byte
	var deltas []string
	for _, round := range rounds {
		var root []byte
		var err error
		if cfg.feed {
			root, err = tr.ProcessFeed(context.Background(), feedOf(round), nil)
		} else {
			updates := benchUpdatesIn(t.TempDir(), commitment.ModeCollect, round, updatesOf(round))
			root, err = tr.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			updates.Close()
		}
		require.NoError(t, err)
		roots = append(roots, root)
		taken := len(deltas)
		for _, part := range tr.TakeDeferredDeltas() {
			for _, d := range part {
				deltas = append(deltas, string(d.Key)+"|"+string(d.Data)+"|"+string(d.Prev))
				require.NoError(t, ctx.PutBranch(d.Key, d.Data, d.Prev))
			}
		}
		require.Equal(t, cfg.deferred, len(deltas) > taken)
	}
	slices.Sort(deltas)
	return roots, deltas, calls.Load()
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

func requireSameRuns(t *testing.T, base []commitmenttest.Op, a, b v3Config, rounds ...[]commitmenttest.Op) [][]byte {
	t.Helper()
	seeded := newShardedContext()
	if base != nil {
		runV3(t, seeded, v3Config{workers: 1}, base)
	}
	run := func(cfg v3Config) ([][]byte, map[string]string, []string) {
		c := newShardedContext()
		for i := range c.shards {
			c.shards[i].branches = maps.Clone(seeded.shards[i].branches)
		}
		roots, deltas, _ := runV3(t, c, cfg, rounds...)
		return roots, storeSnapshot(c), deltas
	}
	rootsA, storeA, deltasA := run(a)
	rootsB, storeB, deltasB := run(b)
	for i := 1; i < len(rootsA); i++ {
		require.NotEqual(t, rootsA[i-1], rootsA[i])
	}
	require.Equal(t, rootsA, rootsB)
	require.Equal(t, storeA, storeB)
	if a.deferred == b.deferred {
		require.Equal(t, deltasA, deltasB)
	}
	return rootsA
}

func fullAccountUpdate(nonce, balance uint64, codeHash common.Hash) commitment.Update {
	return *testAccountUpdate(commitmenttest.Account(commitmenttest.AccountSpec{Nonce: nonce, Balance: balance, CodeHash: codeHash}))
}

type mockContext struct {
	branches     map[string][]byte
	branchBuf    []byte
	branchCalls  [][]byte
	putCalls     int
	accountCalls int
	storageCalls int
}

func newMockContext() *mockContext {
	return &mockContext{branches: make(map[string][]byte)}
}

func (m *mockContext) Branch(key []byte) ([]byte, kv.Step, error) {
	m.branchCalls = append(m.branchCalls, bytes.Clone(key))
	data, ok := m.branches[string(key)]
	if !ok {
		return nil, 0, nil
	}
	if m.branchBuf == nil {
		m.branchBuf = make([]byte, 0, len(data))
	}
	m.branchBuf = append(m.branchBuf[:0], data...)
	return m.branchBuf, 0, nil
}

func (m *mockContext) PutBranch(key, data, _ []byte) error {
	m.putCalls++
	m.branches[string(key)] = bytes.Clone(data)
	return nil
}

func (m *mockContext) Account([]byte) (*commitment.Update, error) {
	m.accountCalls++
	return &commitment.Update{}, nil
}

func (m *mockContext) Storage([]byte) (*commitment.Update, error) {
	m.storageCalls++
	return &commitment.Update{}, nil
}

var _ commitment.PatriciaContext = (*mockContext)(nil)

type phaseAInput struct {
	hashedKey []byte
	plainKey  []byte
	update    *commitment.Update
}

func partition(stream []phaseAInput) ([]storageTask, []accountEntry) {
	sorted := slices.Clone(stream)
	slices.SortStableFunc(sorted, func(a, b phaseAInput) int {
		return bytes.Compare(a.hashedKey, b.hashedKey)
	})
	p := &partitioner{}
	for _, item := range sorted {
		if err := p.add(item.hashedKey, item.update); err != nil {
			panic(err)
		}
	}
	return p.done()
}
