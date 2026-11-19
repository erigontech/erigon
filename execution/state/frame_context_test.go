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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestFrameContextLazyHashes(t *testing.T) {
	var ibs IntraBlockState
	tx := &types.FrameTransaction{
		NonceKeys:  []uint256.Int{{1}, {2}},
		Frames:     []types.Frame{{Data: []byte{3}}},
		Signatures: []types.FrameSignature{{Signature: []byte{4}}},
	}
	ctx := ibs.NewFrameContext(tx, uint256.Int{})
	require.Zero(t, ctx.signingHash, "construction must not compute the signing hash")
	require.Zero(t, ctx.nonceKeysHash, "construction must not compute the nonce-keys hash")

	wantSigningHash := tx.SigningHash(nil)
	signingHash := ctx.SigningHash()
	require.Equal(t, wantSigningHash, signingHash)
	require.Zero(t, ctx.nonceKeysHash, "reading the signing hash must not compute the nonce-keys hash")
	wantNonceKeysHash := tx.NonceKeysHash()
	nonceKeysHash := ctx.NonceKeysHash()
	require.Equal(t, wantNonceKeysHash, nonceKeysHash)

	allocs := testing.AllocsPerRun(100, func() {
		signingHash = ctx.SigningHash()
		nonceKeysHash = ctx.NonceKeysHash()
	})
	require.Zero(t, allocs, "cached reads must not allocate for hashing again")
	require.Equal(t, wantSigningHash, signingHash)
	require.Equal(t, wantNonceKeysHash, nonceKeysHash)
}
