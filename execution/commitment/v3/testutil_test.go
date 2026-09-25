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
	"math/bits"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
	"github.com/stretchr/testify/require"
)

func testAccountUpdate(value commitmenttest.AccountValue) *commitment.Update {
	return runner.Update(commitmenttest.Op{Account: &value})
}

func storageUpdate(value []byte) *commitment.Update {
	return runner.Update(commitmenttest.Op{Storage: value})
}

func parityEntries(ops []commitmenttest.Op) []parityUpdate {
	entries := make([]parityUpdate, len(ops))
	for i, op := range ops {
		entries[i] = parityUpdate{key: op.Key, update: runner.Update(op)}
	}
	return entries
}

func incrementalEntries(ops []commitmenttest.Op) []incrementalOp {
	entries := make([]incrementalOp, len(ops))
	for i, op := range ops {
		entries[i] = incrementalOp{key: op.Key, update: runner.Update(op), read: op.Read}
	}
	return entries
}

func openTestTrie(ctx context.Context, spec runner.RunSpec) (runner.Engine, error) {
	if spec.Mode != commitment.ModeCollect {
		return runner.OpenHPH(ctx, spec)
	}
	tr := &Trie{scheduleWorkers: spec.Workers}
	reader, _ := spec.Memory.Open(ctx)
	tr.ResetContext(reader)
	tr.SetTrieContextFactory(spec.Memory.Open)
	return tr, nil
}

func TestSharedRunnerCatalogue(t *testing.T) {
	for _, tc := range []struct {
		id    string
		kind  string
		count int
	}{
		{"E102/process-100k-accounts", "accounts", 100000},
		{"E102/process-100k-storage", "storage", 100000},
		{"E103/differential-zero-state-reads", "incremental", 0},
	} {
		t.Run(tc.id, func(t *testing.T) {
			c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: tc.kind, Count: tc.count})
			require.NoError(t, err)
			c.ID = tc.id
			c.Assertions = commitmenttest.Assertions{ProcessNoError: true, ZeroAccountReads: tc.kind == "incremental", ZeroStorageReads: tc.kind == "incremental", StateReadEngines: []string{"v3"}}
			require.True(t, c.Assertions.ProcessNoError)
			runner.Run(t, c, runner.RunSpec{Name: "v3", Mode: commitment.ModeCollect, Context: runner.ContextSpec{ForbidStateReads: true}}, openTestTrie)
		})
	}
}

func TestSharedRunnerDifferential(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	c.Assertions = commitmenttest.Assertions{ProcessNoError: true, ZeroAccountReads: true, ZeroStorageReads: true, StateReadEngines: []string{"v3"}}
	runner.Compare(t, c, []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Context: runner.ContextSpec{ForbidStateReads: true}}, {Name: "hph", Mode: commitment.ModeUpdate}, {Name: "parallel", Mode: commitment.ModeParallel}}, openTestTrie)
}

func TestSharedRunnerFeed(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(424242), commitmenttest.SequenceSpec{Kind: "whale", Count: 200})
	require.NoError(t, err)
	feed, err := runner.Feed(c.Rounds[0])
	require.NoError(t, err)
	require.Equal(t, feedOf(parityEntries(c.Rounds[0])), feed)
	memory := runner.NewMemory(runner.ContextSpec{ForbidStateReads: true})
	engine, err := openTestTrie(context.Background(), runner.RunSpec{Mode: commitment.ModeCollect, Memory: memory})
	require.NoError(t, err)
	defer engine.Release()
	root, err := engine.(*Trie).ProcessFeed(context.Background(), feed, nil)
	require.NoError(t, err)
	got := runner.Run(t, c, runner.RunSpec{Name: "v3", Mode: commitment.ModeCollect, Context: runner.ContextSpec{ForbidStateReads: true}}, openTestTrie)
	require.Equal(t, got.Rounds[0].Root, root)
	require.Equal(t, got.Rounds[0].Records, memory.Records())
	require.Zero(t, memory.Counts().AccountReads)
	require.Zero(t, memory.Counts().StorageReads)
}

func TestSharedRunnerReload(t *testing.T) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	c.Assertions.ZeroAccountReads, c.Assertions.ZeroStorageReads = true, true
	got := runner.Run(t, c, runner.RunSpec{Name: "v3", Mode: commitment.ModeCollect, Reload: true}, openTestTrie)
	require.Len(t, got.Rounds, 3)
	for i, round := range got.Rounds {
		require.NotEmpty(t, round.State)
		require.Equal(t, uint64(i+1), round.BlockNum)
		require.Equal(t, uint64(i+1), round.TxNum)
	}
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
	paths, err := commitmenttest.Paths(commitmenttest.Shape{Plane: "storage", Prefixes: [][]byte{prefix}})
	if err != nil {
		panic(err)
	}
	return paths[0]
}

func slotValue(path []byte) []byte {
	return commitmenttest.Storage(commitmenttest.StorageSpec{Path: path})
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
		ops[i] = commitmenttest.Op{Key: path, Storage: slotValue(path)}
	}
	return ops
}

func exactStorage(t *testing.T, ctx commitment.PatriciaContext, addr [32]byte) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	var walk func(*node)
	walk = func(n *node) {
		if n == nil {
			return
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 {
				continue
			}
			path := append(bytes.Clone(n.path), byte(nib))
			if n.leafMask&bit != 0 {
				suffix, value := n.leafAt(nib)
				path = append(path, unpackPath(suffix, 64-len(n.path)-1, nil)...)
				require.NotContains(t, out, string(path))
				out[string(path)] = bytes.Clone(value)
				continue
			}
			child := n.child(nib)
			if child == nil {
				path = append(path, n.childExtAt(nib)...)
				if len(n.path) != 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == 0 {
					path = bytes.Clone(n.path)
				}
				loaded, err := unfold(ctx, path, planeStorage, addr[:])
				require.NoError(t, err, "unfold %x", path)
				require.NotNil(t, loaded, "unfold %x", path)
				loaded.path = path
				child = loaded
			}
			walk(child)
		}
	}
	root, err := unfold(ctx, nil, planeStorage, addr[:])
	require.NoError(t, err)
	walk(root)
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

func testUpdates(t *testing.T, mode commitment.Mode, ops []commitmenttest.Op) *commitment.Updates {
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
	require.Equal(t, want, exactStorage(t, ctx, addr))
}
