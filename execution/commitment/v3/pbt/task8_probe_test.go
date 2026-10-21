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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinReviewEmptyDeclarationAcrossRounds(t *testing.T) {
	base := newParallelRoundBase(map[string][]byte{})
	base.answerAbsent()
	trie := base.trie()
	_, err := trie.ProcessParallelContext(t.Context(), parallelRoundConvertOps(rand.New(rand.NewSource(9)), 300), 4)
	require.NoError(t, err)
	base.snapshot = base.main.records
	key := GlobalRootKey()
	require.NotEmpty(t, base.main.records[string(key)])
	round := trie.newPhaseContext(base.main, nil)
	err = round.PutBranch(key, []byte{0x42}, nil)
	if err == nil {
		t.Fatalf("round 2 accepted nil prev over stored root: Absent=%v, stored bytes=%d", base.main.Absent(key), len(base.main.records[string(key)]))
	}
	require.ErrorContains(t, err, "previous record mismatch")
}

func TestPBinReviewPhaseHookPreservesCallerMerge(t *testing.T) {
	ops := []Op{{Key: parallelRoundKey(eip8297.AccountZone, 0x10, 1, 0), merge: &feedMerge{kind: mergeBasicData, nonce: 7}}}
	trie := NewTrie(newTrieTestContext())
	trie.SetPhaseHook(func(_ phaseTask, op *Op) error { op.merge.nonce = 99; return nil })
	task := phaseTask{ops: ops}
	require.NoError(t, trie.phaseHookOps(&task))
	if ops[0].merge.nonce != 7 {
		t.Fatalf("phase hook changed caller merge nonce: want 7, got %d", ops[0].merge.nonce)
	}
}
