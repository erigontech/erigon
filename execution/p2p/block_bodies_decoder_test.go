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

func TestDecodeBlockBodiesResponseMatchesHeader(t *testing.T) {
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

func TestDecodeBlockBodiesResponseRejectsMalformedTransactionBeforeMismatch(t *testing.T) {
	transactions := rlpTestList([]byte{rlp.EmptyStringCode})
	body := rlpTestList(append(transactions, rlp.EmptyListCode))
	header := newMockHeaderForBody(1, &types.Body{})

	decoded, err := decodeBlockBodiesResponse(body, []*types.Header{header})
	require.ErrorContains(t, err, "short input: 0")
	require.NotErrorIs(t, err, ErrBodyDoesNotMatchHeader)
	require.Nil(t, decoded)
}

func TestDecodeBlockBodiesResponseRejectsWithdrawalPresenceMismatch(t *testing.T) {
	for _, test := range []struct {
		name     string
		expected []*types.Withdrawal
		actual   []*types.Withdrawal
	}{
		{name: "unexpected", actual: []*types.Withdrawal{}},
		{name: "missing", expected: []*types.Withdrawal{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := newMockHeaderForBody(1, &types.Body{Withdrawals: test.expected})
			encoded, err := rlp.EncodeToBytes(&types.Body{Withdrawals: test.actual})
			require.NoError(t, err)

			decoded, err := decodeBlockBodiesResponse(encoded, []*types.Header{header})
			require.ErrorIs(t, err, ErrBodyDoesNotMatchHeader)
			require.False(t, rlp.IsInvalidRLPError(err))
			require.Nil(t, decoded)
		})
	}
}

func TestDecodeBlockBodiesResponseRejectsTrailingFieldBeforeMismatch(t *testing.T) {
	header := newMockHeaderForBody(1, &types.Body{})
	header.TxHash = common.Hash{1}
	body := rlpTestList(bytes.Repeat([]byte{rlp.EmptyListCode}, 4))

	decoded, err := decodeBlockBodiesResponse(body, []*types.Header{header})
	require.ErrorIs(t, err, ErrInvalidBodyRLP)
	require.True(t, rlp.IsInvalidRLPError(err))
	require.NotErrorIs(t, err, ErrBodyDoesNotMatchHeader)
	require.Nil(t, decoded)
}

func rlpTestList(content []byte) []byte {
	encoded := make([]byte, rlp.ListPrefixLen(len(content))+len(content))
	prefixLen := rlp.EncodeListPrefixToBuf(len(content), encoded)
	copy(encoded[prefixLen:], content)
	return encoded
}
