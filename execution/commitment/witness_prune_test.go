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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
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
			hph.SetTrace(false)
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
	hph.SetTrace(false)

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
