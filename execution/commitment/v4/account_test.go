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

package v4

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestAccountLeafRoundTrip(t *testing.T) {
	codeHash := common.HexToHash("0x1234")
	storageRoot := common.HexToHash("0x5678")
	tests := []struct {
		name        string
		update      commitment.Update
		storageRoot []byte
		wantCode    []byte
		wantRoot    []byte
	}{
		{name: "eoa", update: commitment.Update{CodeHash: empty.CodeHash}, wantRoot: empty.RootHash[:]},
		{name: "contract with storage", update: commitment.Update{Nonce: 3, Balance: *uint256.NewInt(99), CodeHash: codeHash}, storageRoot: storageRoot[:], wantCode: codeHash[:], wantRoot: storageRoot[:]},
		{name: "contract with code and no storage", update: commitment.Update{Nonce: 7, CodeHash: codeHash}, wantCode: codeHash[:], wantRoot: empty.RootHash[:]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeAccountLeaf(&tt.update, tt.storageRoot, nil)
			nonce, balance, gotCode, gotRoot, err := decodeAccountLeaf(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.update.Nonce, nonce)
			require.Equal(t, 0, balance.Cmp(&tt.update.Balance))
			require.Equal(t, tt.wantCode, gotCode)
			require.Equal(t, tt.wantRoot, gotRoot)
		})
	}
}

func TestAccountLeafElisionFlags(t *testing.T) {
	codeHash := common.HexToHash("0x1234")
	for _, balance := range []uint64{0, 99} {
		for _, code := range []common.Hash{empty.CodeHash, codeHash} {
			u := &commitment.Update{Balance: *uint256.NewInt(balance), CodeHash: code}
			encoded := encodeAccountLeaf(u, nil, nil)
			_, gotBalance, gotCode, _, err := decodeAccountLeaf(encoded)
			require.NoError(t, err)
			require.Equal(t, balance, gotBalance.Uint64())
			if code == empty.CodeHash {
				require.Empty(t, gotCode)
			} else {
				require.Equal(t, code[:], gotCode)
			}
		}
	}
}

func TestAccountConsensusRLPMatchesAccountRLP(t *testing.T) {
	codeHash := common.HexToHash("0x1234")
	storageRoot := common.HexToHash("0x5678")
	for _, balance := range []uint64{0, 99} {
		for _, code := range []common.Hash{empty.CodeHash, codeHash} {
			acc := accounts.NewAccount()
			acc.Nonce = 7
			acc.Balance.SetUint64(balance)
			acc.Root = storageRoot
			acc.CodeHash = accounts.InternCodeHash(code)
			got := accountConsensusRLP(acc.Nonce, &acc.Balance, acc.Root[:], code[:], nil)
			require.Equal(t, acc.RLP(), got)
		}
	}

	acc := accounts.NewAccount()
	got := accountConsensusRLP(0, &acc.Balance, nil, nil, nil)
	accRoot := empty.RootHash
	acc.Root = accRoot
	require.Equal(t, acc.RLP(), got)
}

func TestDecodeAccountLeafRejectsMalformedBodies(t *testing.T) {
	u := &commitment.Update{Nonce: 1, Balance: *uint256.NewInt(2), CodeHash: common.HexToHash("0x1234")}
	encoded := encodeAccountLeaf(u, nil, nil)
	for i := range encoded {
		_, _, _, _, err := decodeAccountLeaf(encoded[:i])
		require.Error(t, err)
	}

	_, _, _, _, err := decodeAccountLeaf([]byte{0x80, 0})
	require.ErrorIs(t, err, errAccountLeafFlags)

	invalidCode := append([]byte{accountHasCodeHash, 0}, make([]byte, length.Hash)...)
	_, _, _, _, err = decodeAccountLeaf(invalidCode)
	require.ErrorIs(t, err, errAccountLeafFlags)

	trailing := append(bytes.Clone(encodeAccountLeaf(&commitment.Update{}, nil, nil)), 1)
	_, _, _, _, err = decodeAccountLeaf(trailing)
	require.ErrorIs(t, err, errAccountLeafTrailing)
}
