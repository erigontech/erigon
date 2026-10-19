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

package stateless

import (
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	alice = common.HexToAddress("0xa11ce")
	bob   = common.HexToAddress("0xb0b")
	slot1 = common.Hash{1}
	slot2 = common.Hash{2}
)

func eoa() *accounts.Account {
	return &accounts.Account{Nonce: 1, Root: empty.RootHash, CodeHash: accounts.EmptyCodeHash}
}

func slotKey(addr common.Address, slot common.Hash) []byte {
	return dbutils.GenerateCompositeTrieKey(crypto.Keccak256Hash(addr[:]), crypto.Keccak256Hash(slot[:]))
}

// stateTrie returns the trie of accounts, with storage for those slots has.
func stateTrie(accs []common.Address, slots map[common.Address][]common.Hash) *trie.Trie {
	tr := trie.New(common.Hash{})
	for _, addr := range accs {
		addrHash := crypto.Keccak256Hash(addr[:])
		acc := eoa()
		tr.UpdateAccount(addrHash[:], acc)
		for i, slot := range slots[addr] {
			tr.Update(slotKey(addr, slot), []byte{byte(i + 1)})
		}
		if len(slots[addr]) > 0 {
			_, acc.Root, _ = tr.DeepHash(addrHash[:])
			tr.UpdateAccount(addrHash[:], acc)
		}
	}
	return tr
}

// proofNodes returns the nodes proving keys in tr, without duplicates.
func proofNodes(t *testing.T, tr *trie.Trie, storage bool, keys ...[]byte) [][]byte {
	t.Helper()
	var nodes [][]byte
	for _, key := range keys {
		proof, err := tr.Prove(key, 0, storage)
		require.NoError(t, err)
		for _, n := range proof {
			if !slices.ContainsFunc(nodes, func(m []byte) bool { return slices.Equal(m, n) }) {
				nodes = append(nodes, n)
			}
		}
	}
	return nodes
}

// TestDeleteNeedsSibling covers deletions that reduce a branch to one child:
// a witness without that child's node cannot give the canonical root.
func TestDeleteNeedsSibling(t *testing.T) {
	aliceHash, bobHash := crypto.Keccak256Hash(alice[:]), crypto.Keccak256Hash(bob[:])

	t.Run("account", func(t *testing.T) {
		tr := stateTrie([]common.Address{alice, bob}, nil)
		want := stateTrie([]common.Address{bob}, nil).Hash()
		for _, tc := range []struct {
			nodes [][]byte
			err   error
		}{
			{proofNodes(t, tr, false, aliceHash[:], bobHash[:]), nil},
			{proofNodes(t, tr, false, aliceHash[:]), ErrIncompleteWitness},
		} {
			ws, err := newWitnessState(tc.nodes, nil, tr.Hash())
			require.NoError(t, err)
			require.NoError(t, ws.DeleteAccount(accounts.InternAddress(alice), nil))
			root, err := ws.flush()
			require.ErrorIs(t, err, tc.err)
			if tc.err == nil {
				require.Equal(t, want, root)
			}
		}
	})

	t.Run("storage", func(t *testing.T) {
		tr := stateTrie([]common.Address{alice}, map[common.Address][]common.Hash{alice: {slot1, slot2}})
		// Slot 2 keeps its value 2.
		wantTrie := trie.New(common.Hash{})
		acc := eoa()
		wantTrie.UpdateAccount(aliceHash[:], acc)
		wantTrie.Update(slotKey(alice, slot2), []byte{2})
		_, acc.Root, _ = wantTrie.DeepHash(aliceHash[:])
		wantTrie.UpdateAccount(aliceHash[:], acc)
		want := wantTrie.Hash()

		for _, tc := range []struct {
			nodes [][]byte
			err   error
		}{
			{proofNodes(t, tr, true, slotKey(alice, slot1), slotKey(alice, slot2)), nil},
			{proofNodes(t, tr, true, slotKey(alice, slot1)), ErrIncompleteWitness},
		} {
			ws, err := newWitnessState(tc.nodes, nil, tr.Hash())
			require.NoError(t, err)
			addr := accounts.InternAddress(alice)
			acc, err := ws.ReadAccountData(addr)
			require.NoError(t, err)
			require.NoError(t, ws.UpdateAccountData(addr, nil, acc))
			require.NoError(t, ws.WriteAccountStorage(addr, 0, accounts.InternKey(slot1), *uint256.NewInt(1), uint256.Int{}))
			root, err := ws.flush()
			require.ErrorIs(t, err, tc.err)
			if tc.err == nil {
				require.Equal(t, want, root)
			}
		}
	})
}
