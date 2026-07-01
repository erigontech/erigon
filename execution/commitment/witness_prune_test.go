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
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// A proved key whose path steps onto a blinded child must stop cleanly in the prune,
// not fall through to the error default. RLPDecode reconstructs blinded children as
// *trie.HashNode (pointer), so WitnessNodesForKeys must match the pointer type — the
// canonical exclusion-proof case (an absent slot diverging at a branch).
func TestWitnessNodesForKeys_AbsentSlotStopsAtBlindedChild(t *testing.T) {
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)

	// One contract account whose storage carries a slot at every top storage-nibble,
	// so its storage root is a 16-way branch; witnessing a single slot leaves the
	// other children blinded.
	addrPlain, _ := generateKeyWithHashedPrefix([]byte{0}, length.Addr)
	addrHex := common.Bytes2Hex(addrPlain)
	builder := NewUpdateBuilder().Balance(addrHex, 1)
	slots := make([][]byte, 16)
	for n := 0; n < 16; n++ {
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

	// An absent slot whose hashed path routes to storage-nibble 1 — a sibling we do
	// not witness, so canonical mode leaves it a blinded *HashNode.
	absentSlot, _ := generateKeyWithHashedPrefix([]byte{1}, length.Hash)

	toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	defer toWitness.Close()
	toWitness.TouchPlainKey(string(addrPlain), nil, toWitness.TouchAccount)
	toWitness.TouchPlainKey(string(storageKey(addrPlain, slots[0])), nil, toWitness.TouchStorage)
	toWitness.TouchPlainKey(string(storageKey(addrPlain, absentSlot)), nil, toWitness.TouchStorage)

	nodes, provedKeys, _, err := hph.Witnesses(ctx, toWitness, false /* produceExclusionProofs: canonical */, "")
	require.NoError(t, err)

	wt, err := trie.RLPDecode(nodes)
	require.NoError(t, err)

	_, err = wt.WitnessNodesForKeys(provedKeys)
	require.NoError(t, err, "prune must stop at a blinded child, not error on *trie.HashNode")
}
