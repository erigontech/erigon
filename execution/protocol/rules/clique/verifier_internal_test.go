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

package clique

import (
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestVerifySealRejectsRecentSignerBeforeLimit(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	signatures, err := lru.New[common.Hash, accounts.Address](1)
	require.NoError(t, err)
	engine := &Clique{FakeDiff: true, signatures: signatures}
	snapshot := &Snapshot{
		Signers: map[accounts.Address]struct{}{
			signer: {},
			accounts.InternAddress(common.Address{1}): {},
			accounts.InternAddress(common.Address{2}): {},
			accounts.InternAddress(common.Address{3}): {},
		},
		Recents: map[uint64]accounts.Address{1: signer},
	}
	header := &types.Header{
		Number: *uint256.NewInt(2),
		Extra:  make([]byte, ExtraVanity+ExtraSeal),
	}
	sealHash := SealHash(header)
	signature, err := crypto.Sign(sealHash[:], key)
	require.NoError(t, err)
	copy(header.Extra[ExtraVanity:], signature)

	require.ErrorIs(t, engine.verifySeal(nil, header, snapshot), ErrRecentlySigned)
}
