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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinReviewPhaseHookPreservesCallerKeyBytes(t *testing.T) {
	first := parallelRoundKey(eip8297.CodeZone, 0x10, 1, 1)
	second := parallelRoundKey(eip8297.CodeZone, 0x20, 2, 1)
	ops := []Op{{Key: first, Value: testTrieValue(1)}, {Key: second, Value: testTrieValue(2)}}
	original := bytes.Clone(first)
	base := newParallelRoundBase(map[string][]byte{})
	trie := base.trie()
	trie.SetPhaseHook(func(_ phaseTask, op *Op) error {
		if op != nil {
			op.Key[len(op.Key)-1] = 2
		}
		return nil
	})
	_, err := trie.ProcessParallel(ops, 2)
	require.NoError(t, err)
	if !bytes.Equal(original, ops[0].Key) {
		t.Fatalf("phase hook changed caller key suffix: want %d, got %d", original[len(original)-1], ops[0].Key[len(ops[0].Key)-1])
	}
}
