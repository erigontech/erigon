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

package types

import (
	"bytes"
	"io"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func sampleSetCodeTx() *SetCodeTransaction {
	to := common.HexToAddress("0x0000000000000000000000000000000000000002")
	return &SetCodeTransaction{
		DynamicFeeTransaction: DynamicFeeTransaction{
			CommonTx: CommonTx{
				Nonce:    1,
				GasLimit: 21_000,
				To:       &to,
				Value:    *uint256.NewInt(0),
			},
			ChainID:    *uint256.NewInt(1),
			TipCap:     *uint256.NewInt(1),
			FeeCap:     *uint256.NewInt(2),
			AccessList: AccessList{},
		},
		Authorizations: []Authorization{{
			ChainID: *uint256.NewInt(1),
			Address: common.HexToAddress("0x0000000000000000000000000000000000000003"),
			Nonce:   0,
			YParity: 0,
			R:       *uint256.NewInt(1),
			S:       *uint256.NewInt(1),
		}},
	}
}

func TestParseAndAddressToDelegation(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000000003"))

	code := AddressToDelegation(addr)
	require.Len(t, code, DelegateDesignationCodeSize)

	got, ok := ParseDelegation(code)
	require.True(t, ok)
	require.Equal(t, addr.Value(), got.Value())

	// Wrong length.
	_, ok = ParseDelegation(make([]byte, DelegateDesignationCodeSize-1))
	require.False(t, ok)

	// Right length, wrong prefix.
	_, ok = ParseDelegation(make([]byte, DelegateDesignationCodeSize))
	require.False(t, ok)
}

func TestSetCodeTx_Basics(t *testing.T) {
	t.Parallel()
	tx := sampleSetCodeTx()
	require.Equal(t, uint8(SetCodeTxType), tx.Type())
	require.Empty(t, tx.GetBlobHashes())
	require.Len(t, tx.GetAuthorizations(), 1)
	require.Same(t, tx, tx.Unwrap())
	require.Positive(t, tx.EncodingSize())
}

func TestSetCodeTx_Copy(t *testing.T) {
	t.Parallel()
	tx := sampleSetCodeTx()
	cp := tx.copy()
	require.Len(t, cp.Authorizations, 1)

	tx.Authorizations[0].Nonce = 99
	require.NotEqual(t, tx.Authorizations[0].Nonce, cp.Authorizations[0].Nonce)
}

func TestSetCodeTx_HashAndSigningHash(t *testing.T) {
	t.Parallel()
	tx := sampleSetCodeTx()

	h := tx.Hash()
	require.NotEqual(t, common.Hash{}, h)
	require.Equal(t, h, tx.Hash()) // cached

	require.NotEqual(t, common.Hash{}, tx.SigningHash(big.NewInt(1)))
}

func TestSetCodeTx_MarshalDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	tx := sampleSetCodeTx()

	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	b := buf.Bytes()
	require.Equal(t, uint8(SetCodeTxType), b[0])

	var got SetCodeTransaction
	require.NoError(t, got.DecodeRLP(rlp.NewStream(bytes.NewReader(b[1:]), 0)))
	require.Equal(t, tx.Nonce, got.Nonce)
	require.Equal(t, tx.GasLimit, got.GasLimit)
	require.Equal(t, *tx.To, *got.To)
	require.Len(t, got.Authorizations, 1)

	// EncodeRLP (envelope form) also succeeds.
	var env bytes.Buffer
	require.NoError(t, tx.EncodeRLP(&env))
	require.NotEmpty(t, env.Bytes())
}

func TestSetCodeTx_NilToErrors(t *testing.T) {
	t.Parallel()
	tx := sampleSetCodeTx()
	tx.To = nil
	require.ErrorIs(t, tx.MarshalBinary(io.Discard), ErrNilToFieldTx)
	require.ErrorIs(t, tx.EncodeRLP(io.Discard), ErrNilToFieldTx)
}
