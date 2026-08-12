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
)

func nodeSet(nodes [][]byte) map[string]struct{} {
	m := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		m[string(n)] = struct{}{}
	}
	return m
}

// TestWitnessNodesForKeys_ByHashEquivalence asserts the byHash-walk prune returns
// exactly the same lean node set as RLPDecode + WitnessNodesForKeys, across account,
// account+storage, and canonical (no exclusion) shapes.
func TestWitnessNodesForKeys_ByHashEquivalence(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name                  string
		accts, slots, touch   int
		touchStorage, exclude bool
	}{
		{"acct-only-legacy", 128, 4, 16, false, true},
		{"acct+storage-legacy", 128, 4, 16, true, true},
		{"acct+storage-canonical", 256, 8, 24, true, false},
		{"single-touch-legacy", 64, 4, 1, true, true},
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
			full, provedKeys, _, err := hph.Witnesses(ctx, toWitness, tc.exclude, "")
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
		})
	}
}

// RLPDecode rebuilds blinded children as *trie.HashNode; a proved key that steps
// onto one (an absent slot diverging at a canonical-mode branch) must stop cleanly
// in both the prune and Prove, never panic on the pointer type.
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

	nodes, provedKeys, _, err := hph.Witnesses(ctx, toWitness, false, "")
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

// Test_witness_capture exercises the witness helper directly: an inactive witness
// passes the keccak writer through untouched and emits nothing, while an active one
// tees leaf bytes through leafBuf and accumulates a branch from its prefix and slots.
func Test_witness_capture(t *testing.T) {
	var w witness
	var sink bytes.Buffer

	// inactive: passthrough writer, emits are no-ops, no panic on nil tracer
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

// Test_WitnessTracer_CapturedNodesReconstructRoot proves the fold-time tap captures
// the exact consensus node bytes: decoding the full captured node-set rebuilds the
// commitment root. memoizationOff forces every node to be re-hashed so the capture is
// complete.
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

// witnessResolvesAbsence walks the witness trie following key the way a strict stateless
// verifier does: every node on the path, including the child of a divergent extension, must
// be materialized. Unlike trie.Get it does not accept a bare HashNode behind a divergent
// extension as proof of absence.
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

// witnessNodeAtPath returns the witness node reached after consuming the whole
// hashed path, descending account→storage and through extension/branch nodes
// (terminator-aware). It is used to assert what a strict verifier finds at a
// collapse sibling's prefix — a materialized branch rather than a bare HashNode.
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

// witnessMaterializesNodeAt reports whether the witness holds a materialized
// (present, non-blinded) node at the end of the hashed path. A strict verifier
// descending to a collapse sibling's prefix must find a real branch/leaf there,
// not a bare HashNode it cannot re-form the collapsing branch from.
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

// Test_Witnesses_ExclusionAcrossFoldedExtension drives Witnesses() in legacy mode
// on the #21810 shape (absent slot diverging inside a folded storage extension)
// and asserts the captured set proves absence — the diverging branch is
// materialized during positioning.
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
		touchUpdates([][]byte{acctPlain}, [][]byte{absentStorageKey}), true, "")
	require.NoError(t, err)
	require.Equal(t, root, rootW)

	decoded, err := trie.RLPDecode(setB)
	require.NoError(t, err)
	require.Equal(t, root, decoded.Root(), "Witnesses set must reconstruct the root")

	assertPresentStrict(t, decoded, acctPlain)
	require.True(t, witnessResolvesAbsence(decoded.RootNode, KeyToHexNibbleHash(absentStorageKey), 0),
		"Witnesses must materialize the diverging branch to prove the absent slot")
}

// Strict (reth-equivalent) witness oracle: root equality is necessary-not-sufficient,
// so each accessed key must also strictly resolve — present keys fully materialized,
// absent keys diverging at a materialized node, never a bare HashNode on the path.

func assertPresentStrict(t *testing.T, wt *trie.Trie, plainKey []byte) {
	t.Helper()
	require.True(t, witnessMaterializesNodeAt(wt.RootNode, KeyToHexNibbleHash(plainKey)),
		"present key %x must be materialized on-path", plainKey)
}

func storageKey(account, slot []byte) []byte {
	return append(bytes.Clone(account), slot...)
}

func benchWitnessTrie(b *testing.B) (*HexPatriciaHashed, [][]byte) {
	b.Helper()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)
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
