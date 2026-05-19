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

package accounts

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func TestSerialiseV3_Empty(t *testing.T) {
	t.Parallel()
	a := Account{}
	enc := SerialiseV3(&a)

	var decoded Account
	require.NoError(t, DeserialiseV3(&decoded, enc))
	require.Equal(t, a.Nonce, decoded.Nonce)
	require.Equal(t, uint256.Int{}, decoded.Balance)
	require.Equal(t, EmptyCodeHash, decoded.CodeHash)
}

func TestSerialiseV3_NonEmpty(t *testing.T) {
	t.Parallel()
	a := Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(1000),
		CodeHash: InternCodeHash(common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3}))),
	}
	enc := SerialiseV3(&a)

	var decoded Account
	require.NoError(t, DeserialiseV3(&decoded, enc))
	require.Equal(t, a.Nonce, decoded.Nonce)
	require.True(t, a.Balance.Eq(&decoded.Balance))
	require.Equal(t, a.CodeHash, decoded.CodeHash)
}

// TestDeserialiseV3_LegacyFormatWithTrailingIncarnation verifies the tolerant
// decoder accepts pre-incarnation-removal rows that carry an extra trailing
// `[incLen][inc]` section. Such rows still live in frozen AccountsDomain
// snapshot files until those snapshots are rebuilt.
func TestDeserialiseV3_LegacyFormatWithTrailingIncarnation(t *testing.T) {
	t.Parallel()
	// Build a row by hand in the legacy 4-section format:
	//   [nonceLen=1][nonce=7][balLen=2][bal=0x03e8][codeLen=32][codeHash][incLen=1][inc=0x05]
	codeHash := common.BytesToHash(crypto.Keccak256([]byte{1, 2, 3}))
	legacy := []byte{
		1, 7, // nonce=7
		2, 0x03, 0xe8, // balance=1000
		32, // codeHash length
	}
	legacy = append(legacy, codeHash[:]...)
	legacy = append(legacy, 1, 5) // legacy trailing incarnation=5 — must be ignored

	var decoded Account
	require.NoError(t, DeserialiseV3(&decoded, legacy))
	require.Equal(t, uint64(7), decoded.Nonce)
	require.Equal(t, uint64(1000), decoded.Balance.Uint64())
	require.Equal(t, InternCodeHash(codeHash), decoded.CodeHash)
}
