// Copyright 2024 The Erigon Authors
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

package commitment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// Strict (reth-equivalent) witness oracle: root equality is necessary-not-sufficient,
// so each accessed key must also strictly resolve — present keys fully materialized,
// absent keys diverging at a materialized node, never a bare HashNode on the path.

func assertPresentStrict(t *testing.T, wt *trie.Trie, plainKey []byte) {
	t.Helper()
	require.True(t, witnessMaterializesNodeAt(wt.RootNode, KeyToHexNibbleHash(plainKey)),
		"present key %x must be materialized on-path", plainKey)
}

func storageKey(account, slot []byte) []byte {
	return append(common.Copy(account), slot...)
}

func benchWitnessTrie(b *testing.B) (*HexPatriciaHashed, [][]byte) {
	b.Helper()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	accounts := buildWitnessCorpus(b, ms, hph, 128, 4)
	return hph, accounts[:16]
}

func BenchmarkWitnesses(b *testing.B) {
	ctx := context.Background()
	hph, targets := benchWitnessTrie(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		for _, a := range targets {
			toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
		}
		_, _, _, err := hph.Witnesses(ctx, toWitness, false, "")
		toWitness.Close()
		require.NoError(b, err)
	}
}
