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

package trie

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
)

// A contract touched for its account fields only (no storage slot accessed) keeps a
// non-empty but blinded (HashNode) storage subtrie in the witness trie. ExtractWitness
// must serialize that blinded root as a hash op rather than erroring, even though the
// account path is retained.
func TestExtractWitness_RetainedAccountBlindedStorage(t *testing.T) {
	tr := newEmpty()
	addrHash := crypto.Keccak256(common.FromHex("0x00112233445566778899aabbccddeeff00112233"))
	acc := testAccount(1, u256.N100, withRoot(common.BytesToHash([]byte("non-empty storage root"))))
	tr.UpdateAccount(addrHash, acc)

	sn, ok := tr.RootNode.(*ShortNode)
	require.True(t, ok, "root should be a ShortNode wrapping the account leaf")
	an, ok := sn.Val.(*AccountNode)
	require.True(t, ok, "leaf should be an AccountNode")
	an.Storage = &HashNode{hash: crypto.Keccak256([]byte("blinded storage subtrie"))}

	rlb := NewRetainListBuilder()
	rlb.AddTouch(addrHash)
	rl := rlb.Build(false)

	w, err := tr.ExtractWitness(false, rl)
	require.NoError(t, err)
	require.NotNil(t, w)
}
