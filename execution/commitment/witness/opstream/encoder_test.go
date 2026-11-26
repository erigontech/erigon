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

package opstream_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/commitment/witness/opstream"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestEncodeMatchesTrieWitness(t *testing.T) {
	code := []byte{0x60, 0x00, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	designator := append([]byte{0xef, 0x01, 0x00}, make([]byte, len(common.Address{}))...)

	state := trie.New(common.Hash{})

	storageAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
	storageHash := crypto.Keccak256Hash(storageAddress[:])
	storageSlot := common.HexToHash("0x01")
	storageKey := crypto.Keccak256Hash(storageSlot[:])
	contract := accounts.NewAccount()
	contract.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	state.UpdateAccount(storageHash[:], &contract)
	state.Update(append(append([]byte{}, storageHash[:]...), storageKey[:]...), []byte{0x42})

	plainAddress := common.HexToAddress("0x2000000000000000000000000000000000000002")
	plainHash := crypto.Keccak256Hash(plainAddress[:])
	plain := accounts.NewAccount()
	plain.Nonce = 3
	state.UpdateAccount(plainHash[:], &plain)

	missingCodeAddress := common.HexToAddress("0x3000000000000000000000000000000000000003")
	missingCodeHash := crypto.Keccak256Hash(missingCodeAddress[:])
	missingCodeAccount := accounts.NewAccount()
	missingCodeAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x01}))
	state.UpdateAccount(missingCodeHash[:], &missingCodeAccount)

	emptyStorageAddress := common.HexToAddress("0x4000000000000000000000000000000000000004")
	emptyStorageHash := crypto.Keccak256Hash(emptyStorageAddress[:])
	emptyStorageAccount := accounts.NewAccount()
	emptyStorageAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	state.UpdateAccount(emptyStorageHash[:], &emptyStorageAccount)

	designatorAddress := common.HexToAddress("0x5000000000000000000000000000000000000005")
	designatorHash := crypto.Keccak256Hash(designatorAddress[:])
	designatorAccount := accounts.NewAccount()
	designatorAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(designator))
	state.UpdateAccount(designatorHash[:], &designatorAccount)

	rootExtension := trie.New(common.Hash{})
	rootAccountA := accounts.NewAccount()
	rootAccountB := accounts.NewAccount()
	rootExtension.UpdateAccount(common.Hex2Bytes("0x123400"), &rootAccountA)
	rootExtension.UpdateAccount(common.Hex2Bytes("0x123500"), &rootAccountB)

	cases := []struct {
		name      string
		tr        *trie.Trie
		codeReads map[common.Hash][]byte
	}{
		{name: "empty", tr: trie.New(common.Hash{})},
		{name: "single leaf", tr: stateForSingleLeaf()},
		{name: "root extension", tr: rootExtension},
		{
			name: "accounts storage and code",
			tr:   state,
			codeReads: map[common.Hash][]byte{
				storageHash:      code,
				emptyStorageHash: code,
				designatorHash:   designator,
			},
		},
		{name: "code-bearing account without code read", tr: stateForAccount(missingCodeHash, missingCodeAccount)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, err := tc.tr.RLPEncode()
			require.NoError(t, err)
			want := trieWitnessBytes(t, nodes, tc.codeReads)
			gotWitness, err := opstream.Encode(nodes, tc.codeReads)
			require.NoError(t, err)
			got := witnessBytes(t, gotWitness)
			if !bytes.Equal(got, want) {
				t.Fatalf("encoded witness differs: got %x, want %x", got, want)
			}
		})
	}
}

func TestEncodeRejectsCodeHashMismatch(t *testing.T) {
	key := crypto.Keccak256Hash([]byte("code-account"))
	code := []byte{0x60, 0x01}
	account := accounts.NewAccount()
	account.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	tr := stateForAccount(key, account)
	nodes, err := tr.RLPEncode()
	require.NoError(t, err)
	_, err = opstream.Encode(nodes, map[common.Hash][]byte{
		key: {0x60, 0x02},
	})
	if err == nil {
		t.Fatal("expected code hash mismatch")
	}
}

func TestEncodeRejectsNodeTrailingData(t *testing.T) {
	nodes, err := stateForSingleLeaf().RLPEncode()
	require.NoError(t, err)
	nodes[0] = append(nodes[0], 0)
	if _, err := opstream.Encode(nodes, nil); err == nil {
		t.Fatal("expected trailing node data error")
	}
}

func TestEncodeRejectsCodeReadForEmptyCodeHash(t *testing.T) {
	addr := common.HexToAddress("0x1234")
	key := crypto.Keccak256Hash(addr[:])
	account := accounts.NewAccount()
	account.Nonce = 1
	nodes, err := stateForAccount(key, account).RLPEncode()
	require.NoError(t, err)
	old, err := trie.RLPDecode(nodes)
	require.NoError(t, err)
	code := []byte{0x60, 0x01}
	if acc, ok := old.GetAccount(key[:]); !ok || acc == nil {
		t.Fatal("missing account in reference trie")
	}
	if err := old.UpdateAccountCode(key[:], trie.CodeNode(code)); err == nil || !strings.Contains(err.Error(), "inserted code mismatch") {
		t.Fatalf("reference code mismatch error = %v", err)
	}
	_, err = opstream.Encode(nodes, map[common.Hash][]byte{
		key: code,
	})
	if err == nil || !strings.Contains(err.Error(), "inserted code mismatch") {
		t.Fatalf("Encode error = %v, want code hash mismatch", err)
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	tr := trie.New(common.Hash{})
	first := accounts.NewAccount()
	first.Nonce = 1
	second := accounts.NewAccount()
	second.Nonce = 2
	tr.UpdateAccount([]byte("first"), &first)
	tr.UpdateAccount([]byte("second"), &second)
	nodes, err := tr.RLPEncode()
	require.NoError(t, err)

	witness, err := opstream.Encode(nodes, nil)
	require.NoError(t, err)
	encoded := witnessBytes(t, witness)
	decoded, err := opstream.NewWitnessFromReader(bytes.NewReader(encoded), false)
	require.NoError(t, err)
	rebuilt, err := trie.BuildTrieFromWitness(decoded, false)
	require.NoError(t, err)
	if got, want := rebuilt.Hash(), tr.Hash(); got != want {
		t.Fatalf("round-trip root = %x, want %x", got, want)
	}
}

func stateForSingleLeaf() *trie.Trie {
	tr := trie.New(common.Hash{})
	account := accounts.NewAccount()
	key := crypto.Keccak256Hash([]byte("single-leaf"))
	tr.UpdateAccount(key[:], &account)
	tr.Update(append(append([]byte{}, key[:]...), make([]byte, 32)...), []byte{0x01})
	return tr
}

func stateForAccount(key common.Hash, account accounts.Account) *trie.Trie {
	tr := trie.New(common.Hash{})
	tr.UpdateAccount(key[:], &account)
	return tr
}

func trieWitnessBytes(t *testing.T, nodes [][]byte, codeReads map[common.Hash][]byte) []byte {
	t.Helper()
	tr, err := trie.RLPDecode(nodes)
	require.NoError(t, err)
	for key, code := range codeReads {
		if len(code) == 0 {
			continue
		}
		if err := tr.UpdateAccountCode(key[:], trie.CodeNode(code)); err != nil {
			t.Fatal(err)
		}
	}
	witness, err := tr.ExtractWitness(false, nil)
	require.NoError(t, err)
	return witnessBytes(t, witness)
}

func witnessBytes(t *testing.T, witness *opstream.Witness) []byte {
	t.Helper()
	var output bytes.Buffer
	if _, err := witness.WriteInto(&output); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
