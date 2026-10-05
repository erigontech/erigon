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

package pbt

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestOperationCodecPreservesMerge(t *testing.T) {
	var balance uint256.Int
	balance.SetUint64(17)
	op := Op{
		Key:   []byte{1, 2, 3},
		Value: [32]byte{4},
		merge: &feedMerge{kind: mergeBasicData, nonce: 9, balance: balance, codeHash: common.HexToHash("0x1234")},
	}
	encoded, err := EncodeOp(op)
	require.NoError(t, err)
	decoded, err := DecodeOp(encoded)
	require.NoError(t, err)
	require.Equal(t, op.Key, decoded.Key)
	require.Equal(t, op.Value, decoded.Value)
	require.NotNil(t, decoded.merge)
	require.Equal(t, op.merge.kind, decoded.merge.kind)
	require.Equal(t, op.merge.nonce, decoded.merge.nonce)
	require.True(t, op.merge.balance.Eq(&decoded.merge.balance))
	require.Equal(t, op.merge.codeHash, decoded.merge.codeHash)
	require.True(t, bytes.Equal(op.Drop, decoded.Drop))
}
