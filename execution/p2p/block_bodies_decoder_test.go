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

package p2p

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestDecodeBlockBodiesResponseMatchesHeaderBeforeDecoding(t *testing.T) {
	tests := []struct {
		name string
		body *types.Body
	}{
		{
			name: "uncles",
			body: &types.Body{Uncles: []*types.Header{newMockBlockHeaders(1)[0]}},
		},
		{
			name: "withdrawals",
			body: &types.Body{Withdrawals: []*types.Withdrawal{
				{Index: 1, Validator: 2, Address: common.Address{3}, Amount: 4},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := newMockHeaderForBody(1, test.body)
			encoded, err := rlp.EncodeToBytes(test.body)
			require.NoError(t, err)

			bodies, err := decodeBlockBodiesResponse(encoded, []*types.Header{header})
			require.NoError(t, err)
			require.Len(t, bodies, 1)
			require.NoError(t, bodies[0].MatchesHeader(header))
		})
	}
}

func TestDecodeBlockBodiesResponseRejectsMismatchBeforeTransactionDecode(t *testing.T) {
	transactions := rlpTestList([]byte{rlp.EmptyListCode})
	body := rlpTestList(append(transactions, rlp.EmptyListCode))
	header := newMockHeaderForBody(1, &types.Body{})

	decoded, err := decodeBlockBodiesResponse(body, []*types.Header{header})
	require.ErrorIs(t, err, &ErrBodyDoesNotMatchHeader{})
	require.False(t, rlp.IsInvalidRLPError(err))
	require.Nil(t, decoded)
}

func TestDecodeBlockBodiesResponseRejectsTransactionAmplification(t *testing.T) {
	const transactionCount = 4096

	transaction := append([]byte{0xc9}, bytes.Repeat([]byte{0x80}, 9)...)
	transactions := rlpTestList(bytes.Repeat(transaction, transactionCount))
	body := rlpTestList(append(transactions, rlp.EmptyListCode))
	header := newMockHeaderForBody(1, &types.Body{})

	decoded, err := decodeBlockBodiesResponse(body, []*types.Header{header})
	require.ErrorIs(t, err, &ErrBodyDoesNotMatchHeader{})
	require.Nil(t, decoded)
}

func TestDecodeBlockBodiesResponseRejectsExcessBeforeDecoding(t *testing.T) {
	body, err := rlp.EncodeToBytes(&types.Body{})
	require.NoError(t, err)
	header := newMockHeaderForBody(1, &types.Body{})

	_, err = decodeBlockBodiesResponse(append(body, 0x80), []*types.Header{header})
	require.ErrorIs(t, err, &ErrTooManyBodies{})
}

func TestDecodeBlockBodiesResponseEmpty(t *testing.T) {
	header := newMockHeaderForBody(1, &types.Body{})
	decoded, err := decodeBlockBodiesResponse(nil, []*types.Header{header})
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func rlpTestList(content []byte) []byte {
	encoded := make([]byte, rlp.ListPrefixLen(len(content))+len(content))
	prefixLen := rlp.EncodeListPrefixToBuf(len(content), encoded)
	copy(encoded[prefixLen:], content)
	return encoded
}
