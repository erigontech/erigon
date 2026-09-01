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

package state_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// mineKey searches a counter space for a key whose hashed trie path starts with the given nibbles,
// so a fixture can pin the shape of the trie instead of taking whatever keccak hands out.
func mineKey(t *testing.T, keyLen int, hash func([]byte) []byte, path []byte) []byte {
	t.Helper()
	key := make([]byte, keyLen)
	for counter := uint64(0); counter < 1<<26; counter++ {
		binary.BigEndian.PutUint64(key[keyLen-8:], counter)
		if bytes.Equal(hash(key)[:len(path)], path) {
			return bytes.Clone(key)
		}
	}
	t.Fatalf("no key of length %d hashes to path %x", keyLen, path)
	return nil
}

func mineAddr(t *testing.T, path ...byte) []byte {
	t.Helper()
	return mineKey(t, length.Addr, commitment.KeyToHexNibbleHash, path)
}

func mineSlot(t *testing.T, path ...byte) []byte {
	t.Helper()
	return mineKey(t, length.Hash, commitment.KeyToNibblizedHash, path)
}

// depthFixture pins an account trie four levels deep with a two-level storage subtree hanging off
// its deepest account, plus a mirror subtree that shares a cache slot with the first:
//
//	root {0,2,5,a,f} -> [0] {0,1} -> [0,0] {0,1} -> [0,0,0] {0,1}
//	                 -> [2] -> [2,0] -> [2,0,0] {0,1}
//	deep0 storage: root {0,3} -> [0] {0,1}
//
// The record keys of the two depth-4 branches are 00f08n and 20f08n. They differ only in the byte
// a compact key reserves for flags, which is what makes the mirror worth building.
type depthFixture struct {
	deep0, deep1     []byte // under [0,0,0]: the branch that makes the account trie four levels deep
	mirror0, mirror1 []byte // under [2,0,0]
	mid              []byte // under [0,0]
	upper            []byte // under [0]
	wide0            []byte
	wide1            []byte
	wide2            []byte

	slot00, slot01, slot3 []byte // deep0 storage: [0,0], [0,1], [3]
	lone                  []byte // a slot for mid, kept alone so it stays hoisted
}

func newDepthFixture(t *testing.T) *depthFixture {
	t.Helper()
	return &depthFixture{
		deep0:   mineAddr(t, 0, 0, 0, 0),
		deep1:   mineAddr(t, 0, 0, 0, 1),
		mirror0: mineAddr(t, 2, 0, 0, 0),
		mirror1: mineAddr(t, 2, 0, 0, 1),
		mid:     mineAddr(t, 0, 0, 1),
		upper:   mineAddr(t, 0, 1),
		wide0:   mineAddr(t, 5),
		wide1:   mineAddr(t, 0xa),
		wide2:   mineAddr(t, 0xf),

		slot00: mineSlot(t, 0, 0),
		slot01: mineSlot(t, 0, 1),
		slot3:  mineSlot(t, 3),
		lone:   mineSlot(t, 7),
	}
}

func depthAccount(addr []byte, nonce, balance uint64) acceptanceEntry {
	account := accounts.Account{
		Nonce:    nonce,
		Balance:  *uint256.NewInt(balance),
		CodeHash: accounts.EmptyCodeHash,
	}
	return acceptanceEntry{domain: kv.AccountsDomain, key: bytes.Clone(addr), value: accounts.SerialiseV3(&account)}
}

func depthStorage(addr, slot []byte, value uint64) acceptanceEntry {
	key := make([]byte, 0, length.Addr+length.Hash)
	key = append(key, addr...)
	key = append(key, slot...)
	return acceptanceEntry{domain: kv.StorageDomain, key: key, value: uint256.NewInt(value).Bytes()}
}

// depthBatches grows the trie a level at a time, so each batch reads back a node that an earlier
// batch wrote. A single fresh build would never exercise the incremental read path. The last two
// batches touch the mirror subtree and then the first one, so the first one reads a node the mirror
// wrote after it.
func depthBatches(f *depthFixture) [][]acceptanceEntry {
	return [][]acceptanceEntry{
		{
			depthAccount(f.wide0, 1, 100), depthAccount(f.wide1, 1, 101),
			depthAccount(f.upper, 1, 102),
		},
		{
			depthAccount(f.mid, 1, 200), depthAccount(f.deep0, 1, 201),
			depthStorage(f.deep0, f.slot00, 1),
		},
		{
			depthAccount(f.deep1, 1, 300), depthAccount(f.wide2, 1, 301),
			depthAccount(f.mirror0, 1, 302), depthAccount(f.mirror1, 1, 303),
			depthStorage(f.deep0, f.slot01, 2), depthStorage(f.deep0, f.slot3, 3),
			depthStorage(f.mid, f.lone, 4),
		},
		{
			depthAccount(f.mirror0, 2, 400), depthAccount(f.mirror1, 2, 401),
		},
		{
			depthAccount(f.deep0, 2, 500), depthAccount(f.upper, 2, 501),
			depthStorage(f.deep0, f.slot00, 5),
		},
	}
}

// A four-level account trie with a two-level storage subtree is the smallest shape that reads a v3
// node written by an earlier batch through a parent that is itself a v3 node. Every shallower
// fixture in the suite reaches its accounts from the root in one hop.
func TestCommitmentV3DeepTrieMatchesLegacy(t *testing.T) {
	f := newDepthFixture(t)
	batches := depthBatches(f)

	legacyDB, legacyAgg := newAcceptanceDB(t, 4, 2)
	legacyAgg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, false)
	v3DB, v3Agg := newAcceptanceDB(t, 4, 2)
	v3Agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)

	for batchNumber, batch := range batches {
		txNum := uint64(batchNumber + 1)
		wantRoot := applyAcceptanceBatch(t, legacyDB, batch, txNum)
		gotRoot := applyAcceptanceBatch(t, v3DB, batch, txNum)
		require.Equalf(t, wantRoot, gotRoot, "batch %d root", txNum)
	}

	require.Equal(t, recomputeAcceptanceRoot(t, legacyDB), recomputeAcceptanceRoot(t, v3DB), "recomputed root")
}
