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
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(t, err)

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
