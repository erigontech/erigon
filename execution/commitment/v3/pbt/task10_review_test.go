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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBinReviewExplicitNegativeThresholdKeepsOldPlanner(t *testing.T) {
	ops := grainSortedOps(grainStorageOps(0x30, 129, false))
	sequential := newTrieTestContext()
	wantRoot, err := NewTrie(sequential).Process(ops)
	require.NoError(t, err)
	base := newParallelRoundBase(map[string][]byte{})
	trie := base.trie()
	var subtasks atomic.Int32
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if op == nil && task.kind == phaseBucketSubtask {
			subtasks.Add(1)
		}
		return nil
	})
	gotRoot, err := trie.ProcessParallelWithThreshold(ops, 2, -1)
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	require.Equal(t, sequential.records, base.main.records)
	if got := subtasks.Load(); got != 0 {
		t.Fatalf("explicit threshold -1 must keep the old unsplit planner: want 0 bucket subtasks, got %d", got)
	}
}
