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

package eip8297

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
)

func TestStreamRootBuilderMatchesReference(t *testing.T) {
	for _, suite := range []string{HashKeccak, HashBlake3} {
		t.Run(suite, func(t *testing.T) {
			previous := HashSuiteName()
			t.Cleanup(func() { require.NoError(t, SetHashSuite(previous)) })
			require.NoError(t, SetHashSuite(suite))
			entries := streamRootEntries()
			corpora := streamRootCorpora(entries)
			sum := streamRootHash(suite)
			for _, corpus := range corpora {
				t.Run(corpus.name, func(t *testing.T) {
					builder, err := NewStreamRootBuilder(sum)
					require.NoError(t, err)
					for _, entry := range corpus.entries {
						require.NoError(t, builder.Add(entry.Key, entry.Value))
					}
					require.Equal(t, StateRootWithHash(corpus.entries, sum), builder.RootHash())
				})
			}
		})
	}
}

func TestStreamRootBuilderEmpty(t *testing.T) {
	for _, suite := range []string{HashKeccak, HashBlake3} {
		t.Run(suite, func(t *testing.T) {
			previous := HashSuiteName()
			t.Cleanup(func() { require.NoError(t, SetHashSuite(previous)) })
			require.NoError(t, SetHashSuite(suite))
			builder, err := NewStreamRootBuilder(streamRootHash(suite))
			require.NoError(t, err)
			require.Equal(t, EmptyTreeHash, builder.RootHash())
		})
	}
}

func TestStreamRootBuilderRejectsInvalidLeaves(t *testing.T) {
	key := TreeKeyAccount(referenceAddress(1), BasicDataLeafKey)
	value := streamRootValue(1)

	_, err := NewStreamRootBuilder(nil)
	require.ErrorIs(t, err, ErrStreamRootHash)

	tests := []struct {
		name string
		call func(*StreamRootBuilder) error
	}{
		{name: "unsorted", call: func(builder *StreamRootBuilder) error {
			require.NoError(t, builder.Add(key, value))
			return builder.Add(append([]byte(nil), key[:len(key)-1]...), value)
		}},
		{name: "duplicate", call: func(builder *StreamRootBuilder) error {
			require.NoError(t, builder.Add(key, value))
			return builder.Add(key, value)
		}},
		{name: "value length", call: func(builder *StreamRootBuilder) error {
			return builder.Add(key, value[:ValueLength-1])
		}},
		{name: "zero value", call: func(builder *StreamRootBuilder) error {
			return builder.Add(key, make([]byte, ValueLength))
		}},
		{name: "prefix", call: func(builder *StreamRootBuilder) error {
			shortKey := key[:len(key)-1]
			require.NoError(t, builder.Add(shortKey, value))
			return builder.Add(key, value)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder, err := NewStreamRootBuilder(streamRootHash(HashKeccak))
			require.NoError(t, err)
			require.Error(t, test.call(builder))
		})
	}
}

func streamRootEntries() []Entry {
	states := make([]State, 0, 258)
	for i := range 2 {
		address := referenceAddress(uint64(100 + i))
		var balance uint256.Int
		balance.SetUint64(uint64(i + 1))
		states = append(states, State{
			Address: address,
			Nonce:   uint64(i + 1),
			Balance: balance,
			Slots: map[string][]byte{
				string(referenceSlot(0)):   {byte(i + 1)},
				string(referenceSlot(64)):  {byte(i + 2)},
				string(referenceSlot(256)): {byte(i + 3)},
			},
			Code: []byte{byte(i + 1), 0x01, 0x02},
		})
	}
	for chunks := 1; chunks <= 256; chunks++ {
		code := make([]byte, chunks*ChunkDataLen)
		for i := range code {
			code[i] = byte((i + chunks) % 251)
			if code[i] == 0 {
				code[i] = 1
			}
		}
		states = append(states, State{Address: referenceAddress(uint64(1000 + chunks)), Code: code})
	}
	return sortStreamEntries(EmbedState([][]State{states}))
}

func streamRootCorpora(entries []Entry) []struct {
	name    string
	entries []Entry
} {
	corpora := []struct {
		name    string
		entries []Entry
	}{
		{name: "all zones and code stems", entries: entries},
		{name: "single leaf", entries: entries[:1]},
		{name: "three leaves", entries: entries[:3]},
		{name: "one account", entries: sortStreamEntries(referenceOneAccountCorpus().entries)},
		{name: "deep shared prefix", entries: sortStreamEntries(referenceDeepSharedPrefixCorpus().entries)},
	}
	rnd := rand.New(rand.NewSource(0x8297))
	for range 8 {
		selected := make([]Entry, 1+rnd.Intn(len(entries)))
		for j, index := range rnd.Perm(len(entries))[:len(selected)] {
			selected[j] = entries[index]
		}
		corpora = append(corpora, struct {
			name    string
			entries []Entry
		}{name: "random", entries: sortStreamEntries(selected)})
	}
	return corpora
}

func sortStreamEntries(entries []Entry) []Entry {
	entries = slices.Clone(entries)
	slices.SortFunc(entries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
	return entries
}

func streamRootValue(seed byte) []byte {
	value := make([]byte, ValueLength)
	value[ValueLength-1] = seed
	return value
}

func streamRootHash(suite string) HashFn {
	if suite == HashBlake3 {
		return func(data []byte) common.Hash { return common.Hash(blake3.Sum256(data)) }
	}
	return func(data []byte) common.Hash { return keccak.Sum256(data) }
}
