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
	"context"
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
	for _, c := range commitmenttest.Corpus() {
		t.Run(c.ID, func(t *testing.T) {
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
