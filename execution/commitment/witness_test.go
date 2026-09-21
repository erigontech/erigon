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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func nodeSet(nodes [][]byte) map[string]struct{} {
	m := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		m[string(n)] = struct{}{}
	}
	return m
}

func TestWitnessNodesForKeys_ByHashEquivalence(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name                            string
		accts, slots, touch             int
		touchStorage, exclude, prefixes bool
	}{
		{"acct-only-legacy", 128, 4, 16, false, true, false},
		{"acct+storage-legacy", 128, 4, 16, true, true, false},
		{"acct+storage-canonical", 256, 8, 24, true, false, false},
		{"single-touch-legacy", 64, 4, 1, true, true, false},
		{"partial-prefixes-legacy", 128, 4, 16, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := NewMockState(t)
			hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			hph.SetTraceWriter(nil)
			addrs := buildWitnessCorpus(t, ms, hph, tc.accts, tc.slots)

			toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
			defer toWitness.Close()
			touchSlots := 0
			if tc.touchStorage {
				touchSlots = tc.slots
			}
			touchAccountsSlots(toWitness, addrs[:tc.touch], touchSlots)
			// collapse siblings reach the fold as hashed-key prefixes: one inside the account trie, one a nibble into storage
			touchPrefixes := func(u *Updates) {
				if !tc.prefixes {
					return
				}
				for _, a := range addrs[tc.touch : tc.touch+4] {
					u.TouchHashedKey(KeyToHexNibbleHash(a)[:3])
					u.TouchHashedKey(KeyToHexNibbleHash(storageKey(a, slotHashBytes(0)))[:65])
				}
			}
			touchPrefixes(toWitness)
			// the read-only fold runs first: the full fold leaves deferred branch updates behind
			indexedUpdates := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
			defer indexedUpdates.Close()
			touchAccountsSlots(indexedUpdates, addrs[:tc.touch], touchSlots)
			touchPrefixes(indexedUpdates)
			byHash, indexedKeys, root, err := hph.WitnessesByHash(ctx, indexedUpdates, tc.exclude)
			require.NoError(t, err)
			indexed, err := trie.WitnessNodesForKeysByHash(byHash, root, indexedKeys)
			require.NoError(t, err)

			full, provedKeys, _, err := hph.Witnesses(ctx, toWitness, tc.exclude)
			require.NoError(t, err)

			wt, err := trie.RLPDecode(full)
			require.NoError(t, err)
			want, err := wt.WitnessNodesForKeys(provedKeys)
			require.NoError(t, err)
			got, err := trie.WitnessNodesForKeysFromNodes(full, provedKeys)
			require.NoError(t, err)

			ws, gs := nodeSet(want), nodeSet(got)
			var missing, extra int
			for k := range ws {
				if _, ok := gs[k]; !ok {
					missing++
				}
			}
			for k := range gs {
				if _, ok := ws[k]; !ok {
					extra++
				}
			}
			t.Logf("want=%d got=%d missing(in want not got)=%d extra(in got not want)=%d", len(want), len(got), missing, extra)
			require.Zero(t, missing, "byHash prune missing nodes present in RLPDecode prune")
			require.Zero(t, extra, "byHash prune has extra nodes")

			require.Equal(t, ws, nodeSet(indexed), "the read-only indexed fold and prune must give the RLPDecode prune's nodes")
		})
	}
}

func TestWitnessNodesForKeys_AbsentSlotStopsAtBlindedChild(t *testing.T) {
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)

	addrPlain, _ := generateKeyWithHashedPrefix([]byte{0}, length.Addr)
	addrHex := common.Bytes2Hex(addrPlain)
	builder := NewUpdateBuilder().Balance(addrHex, 1)
	slots := make([][]byte, 16)
	for n := range 16 {
		slotPlain, _ := generateKeyWithHashedPrefix([]byte{byte(n)}, length.Hash)
		slots[n] = slotPlain
		builder.Storage(addrHex, common.Bytes2Hex(slotPlain), fmt.Sprintf("%064x", n+1))
	}
	plainKeys, updates := builder.Build()
	processBatch(t, ms, hph, plainKeys, updates)

	absentSlot, _ := generateKeyWithHashedPrefix([]byte{1}, length.Hash)

	toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	defer toWitness.Close()
	toWitness.TouchPlainKey(string(addrPlain), nil, toWitness.TouchAccount)
	toWitness.TouchPlainKey(string(storageKey(addrPlain, slots[0])), nil, toWitness.TouchStorage)
	toWitness.TouchPlainKey(string(storageKey(addrPlain, absentSlot)), nil, toWitness.TouchStorage)

	nodes, provedKeys, _, err := hph.Witnesses(ctx, toWitness, false)
	require.NoError(t, err)

	wt, err := trie.RLPDecode(nodes)
	require.NoError(t, err)

	_, err = wt.WitnessNodesForKeys(provedKeys)
	require.NoError(t, err, "prune must stop at a blinded child, not error on *trie.HashNode")

	storageProofKey := append(crypto.Keccak256(addrPlain), crypto.Keccak256(absentSlot)...)
	require.NotPanics(t, func() {
		_, _ = wt.Prove(crypto.Keccak256(addrPlain), 0, false)
		_, _ = wt.Prove(storageProofKey, 0, true)
	}, "Prove must not panic on a blinded *trie.HashNode (eth_getProof path)")
}

type capturedNode struct{ rlp, hash string }

type recordingTracer struct{ nodes []capturedNode }

func (r *recordingTracer) onNode(rlp, hash []byte) {
	r.nodes = append(r.nodes, capturedNode{rlp: string(rlp), hash: string(hash)})
}

func Test_witness_capture(t *testing.T) {
	var w witness
	var sink bytes.Buffer

	require.False(t, w.active())
	require.Same(t, &sink, w.leafWriter(&sink))
	w.emitLeaf([]byte("x"))
	w.beginBranch([]byte("y"))
	w.writeBranch([]byte("z"))
	w.emitBranch([]byte("w"))

	rec := &recordingTracer{}
	w.tracer = rec
	require.True(t, w.active())

	lw := w.leafWriter(&sink)
	_, _ = lw.Write([]byte("leaf-rlp"))
	w.emitLeaf([]byte("leaf-hash"))

	w.beginBranch([]byte("pre"))
	w.writeBranch([]byte("-slot1"))
	w.writeBranch([]byte("-slot2"))
	w.emitBranch([]byte("branch-hash"))

	require.Equal(t, []capturedNode{
		{rlp: "leaf-rlp", hash: "leaf-hash"},
		{rlp: "pre-slot1-slot2", hash: "branch-hash"},
	}, rec.nodes)

	w.reset()
	require.False(t, w.active())
}

func Test_WitnessTracer_CapturedNodesReconstructRoot(t *testing.T) {
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)
	hph.memoizationOff = true

	builder := NewUpdateBuilder()
	extAccts, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 2, 3)
	for i, a := range extAccts {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for i := range 16 {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		builder.Balance(common.Bytes2Hex(a), uint64(100+i))
	}
	storer := extAccts[0]
	slots, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 2, 3)
	for _, sk := range slots {
		builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}

	plainKeys, updates := builder.Build()

	c := newWitnessNodeSet()
	hph.witness.tracer = c
	root := processBatch(t, ms, hph, plainKeys, updates)
	require.NotEmpty(t, c.byHash, "tracer must capture nodes")

	nodeSet, err := c.nodes(root)
	require.NoError(t, err)
	tr, err := trie.RLPDecode(nodeSet)
	require.NoError(t, err)
	require.Equal(t, root, tr.Root(), "captured node-set must reconstruct the commitment root")
}

func witnessResolvesAbsence(n trie.Node, key []byte, pos int) bool {
	switch x := n.(type) {
	case nil:
		return true
	case trie.ValueNode:
		// reaching a value means the key is present; only a value short of the full key
		// length (a divergent leaf) proves absence
		return pos < len(key)
	case *trie.AccountNode:
		return witnessResolvesAbsence(x.Storage, key, pos)
	case *trie.ShortNode:
		matchlen := nibbles.CommonPrefixLen(key[pos:], x.Key)
		if matchlen == len(x.Key) || x.Key[matchlen] == 16 {
			return witnessResolvesAbsence(x.Val, key, pos+matchlen)
		}
		_, isHash := x.Val.(*trie.HashNode)
		return !isHash
	case *trie.FullNode:
		child := x.Children[key[pos]]
		if child == nil {
			return true
		}
		return witnessResolvesAbsence(child, key, pos+1)
	case *trie.HashNode:
		return false
	default:
		return false
	}
}

func witnessNodeAtPath(n trie.Node, key []byte, pos int) trie.Node {
	if pos == len(key) {
		return n
	}
	switch x := n.(type) {
	case *trie.AccountNode:
		return witnessNodeAtPath(x.Storage, key, pos)
	case *trie.ShortNode:
		k := x.Key
		if len(k) > 0 && k[len(k)-1] == 16 {
			k = k[:len(k)-1]
		}
		if len(key)-pos < len(k) || nibbles.CommonPrefixLen(key[pos:], k) < len(k) {
			return nil
		}
		return witnessNodeAtPath(x.Val, key, pos+len(k))
	case *trie.FullNode:
		return witnessNodeAtPath(x.Children[key[pos]], key, pos+1)
	default:
		return n
	}
}

func witnessMaterializesNodeAt(root trie.Node, key []byte) bool {
	n := witnessNodeAtPath(root, key, 0)
	if n == nil {
		return false
	}
	_, blinded := n.(*trie.HashNode)
	return !blinded
}

func touchUpdates(touchAccounts, touchStorage [][]byte) *Updates {
	u := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	for _, a := range touchAccounts {
		u.TouchPlainKey(string(a), nil, u.TouchAccount)
	}
	for _, s := range touchStorage {
		u.TouchPlainKey(string(s), nil, u.TouchStorage)
	}
	return u
}

func Test_Witnesses_ExclusionAcrossFoldedExtension(t *testing.T) {
	acctPlains, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 2, 6)
	acctPlain := acctPlains[0]
	storPlain, storHashed := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 5, 2)

	builder := NewUpdateBuilder()
	for i, a := range acctPlains {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for _, sk := range storPlain {
		builder.Storage(common.Bytes2Hex(acctPlain), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}
	plainKeys, updates := builder.Build()

	shared := storHashed[0]
	absentPrefix := []byte{shared[0], shared[1], (shared[2] + 1) & 0xf}
	absentSlotPlain, _ := generateKeyWithHashedPrefix(absentPrefix, length.Hash)
	absentStorageKey := storageKey(acctPlain, absentSlotPlain)

	hph, root := processFreshTrie(t, plainKeys, updates)
	setB, _, rootW, err := hph.Witnesses(context.Background(),
		touchUpdates([][]byte{acctPlain}, [][]byte{absentStorageKey}), true)
	require.NoError(t, err)
	require.Equal(t, root, rootW)

	decoded, err := trie.RLPDecode(setB)
	require.NoError(t, err)
	require.Equal(t, root, decoded.Root(), "Witnesses set must reconstruct the root")

	assertPresentStrict(t, decoded, acctPlain)
	require.True(t, witnessResolvesAbsence(decoded.RootNode, KeyToHexNibbleHash(absentStorageKey), 0),
		"Witnesses must materialize the diverging branch to prove the absent slot")
}

func assertPresentStrict(t *testing.T, wt *trie.Trie, plainKey []byte) {
	t.Helper()
	require.True(t, witnessMaterializesNodeAt(wt.RootNode, KeyToHexNibbleHash(plainKey)),
		"present key %x must be materialized on-path", plainKey)
}

func storageKey(account, slot []byte) []byte {
	return append(bytes.Clone(account), slot...)
}

func Test_WitnessNodesByHash_ReadOnlyFold(t *testing.T) {
	accts, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 1, 24)
	slots, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 1, 12)
	builder := NewUpdateBuilder()
	for i, a := range accts {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for _, sk := range slots {
		builder.Storage(common.Bytes2Hex(accts[0]), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}
	plainKeys, updates := builder.Build()
	hph, root := processFreshTrie(t, plainKeys, updates)
	ms := hph.ctx.(*MockState)

	proven := [][]byte{accts[0], accts[5]}
	provenSlots := [][]byte{storageKey(accts[0], slots[0]), storageKey(accts[0], slots[7])}
	full, _, _, err := hph.Witnesses(context.Background(), touchUpdates(proven, provenSlots), false)
	require.NoError(t, err)
	fullTrie, err := trie.RLPDecode(full)
	require.NoError(t, err)
	writes := ms.putBranches
	_, _, _, err = hph.WitnessesByHash(context.Background(), touchUpdates(proven, provenSlots), false)
	require.Error(t, err, "pending deferred updates would be flushed by the fold")
	require.Equal(t, writes, ms.putBranches)

	require.NoError(t, hph.branchEncoder.ApplyDeferredUpdates(16, ms.PutBranch))
	hph.branchEncoder.ClearDeferred()
	writes = ms.putBranches
	byHash, _, rootRO, err := hph.WitnessesByHash(context.Background(), touchUpdates(proven, provenSlots), false)
	require.NoError(t, err)
	require.Equal(t, root, rootRO)
	require.Equal(t, writes, ms.putBranches, "a read-only fold writes no branch")
	require.Empty(t, hph.branchEncoder.deferred, "a read-only fold queues no deferred update")
	require.Less(t, len(byHash), len(full), "nodes off the proven paths are referenced by hash")

	for _, a := range proven {
		key := crypto.Keccak256(a)
		want, err := fullTrie.Prove(key, 0, false)
		require.NoError(t, err)
		got, _, err := trie.ProofFromNodes(byHash, root, key)
		require.NoError(t, err)
		require.Equal(t, want, got, "account %x", a)
	}
	accountProof, accountRLP, err := trie.ProofFromNodes(byHash, root, crypto.Keccak256(accts[0]))
	require.NoError(t, err)
	var acc accounts.Account
	require.NoError(t, acc.DecodeForHashing(accountRLP))
	for _, sk := range []int{0, 7} {
		fullKey := append(crypto.Keccak256(accts[0]), crypto.Keccak256(slots[sk])...)
		want, err := fullTrie.Prove(fullKey, len(accountProof), true)
		require.NoError(t, err)
		got, _, err := trie.ProofFromNodes(byHash, acc.Root[:], crypto.Keccak256(slots[sk]))
		require.NoError(t, err)
		require.Equal(t, want, got, "slot %x", slots[sk])
	}
}
