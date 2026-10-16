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
package protocol

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// minimalSystemTx is the transaction eez-rollup0 a270a06 pins in
// crates/eez-primitives/src/tests.rs (native_wire_sender_hash_and_storage_round_trip).
const minimalSystemTx = "0x76dd0180944200000000000000000000000000000000000007808401020304"

// TestSystemTxVectors is T-RLP-1 on the reference client's byte vectors.
func TestSystemTxVectors(t *testing.T) {
	tx := SystemTx{ChainID: 1, To: EEZL2Address, Input: []byte{1, 2, 3, 4}}
	enc, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, common.FromHex(minimalSystemTx), enc)
	h, err := tx.Hash()
	require.NoError(t, err)
	require.Equal(t, crypto.Keccak256Hash(enc), h)
	dec, err := DecodeSystemTx(enc)
	require.NoError(t, err)
	require.Equal(t, tx, *dec)

	golden := common.FromHex(goldenInboundSystemTx)
	dec, err = DecodeSystemTx(golden)
	require.NoError(t, err)
	require.Equal(t, uint64(1), dec.ChainID)
	require.Equal(t, uint64(7), dec.Nonce)
	require.True(t, dec.Value.IsZero())
	require.Equal(t, goldenInboundCalldata(t), dec.Input)
	enc, err = dec.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, golden, enc)
}

func TestSystemTxRejects(t *testing.T) {
	enc := common.FromHex(minimalSystemTx)
	for n := range len(enc) {
		_, err := DecodeSystemTx(enc[:n])
		require.ErrorIs(t, err, ErrSystemTx, "prefix of %d bytes", n)
	}
	_, err := DecodeSystemTx(append(bytes.Clone(enc), 0))
	require.ErrorIs(t, err, ErrSystemTx)

	wrongType := bytes.Clone(enc)
	wrongType[0] = 0x02
	_, err = DecodeSystemTx(wrongType)
	require.ErrorIs(t, err, ErrSystemTx)

	// Byte 10 lies in the target address.
	wrongTarget := bytes.Clone(enc)
	wrongTarget[10] ^= 1
	_, err = DecodeSystemTx(wrongTarget)
	require.ErrorIs(t, err, ErrSystemTx)
	_, err = (&SystemTx{To: common.HexToAddress("0x01")}).MarshalBinary()
	require.ErrorIs(t, err, ErrSystemTx)

	type signed struct {
		ChainID, Nonce uint64
		To             common.Address
		Value          uint256.Int
		Input          []byte
		V, R, S        uint256.Int
	}
	body, err := rlp.EncodeToBytes(&signed{ChainID: 1, To: EEZL2Address, Input: []byte{1}, V: *uint256.NewInt(27)})
	require.NoError(t, err)
	_, err = DecodeSystemTx(append([]byte{SystemTxType}, body...))
	require.ErrorIs(t, err, ErrSystemTx)

	// A nonce of 0 encoded as 0x00 instead of 0x80 is not canonical RLP.
	nonCanonical := bytes.Clone(enc)
	require.Equal(t, byte(0x80), nonCanonical[3])
	nonCanonical[3] = 0x00
	_, err = DecodeSystemTx(nonCanonical)
	require.ErrorIs(t, err, ErrSystemTx)
}
