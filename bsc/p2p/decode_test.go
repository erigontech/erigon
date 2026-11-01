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

package bscp2p

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

var sidecars = rlp.RawValue{0xc3, 0xc2, 0x01, 0x02}

func TestDecodeBlockBodySkipsSidecars(t *testing.T) {
	t.Parallel()

	body := testBody()
	got, err := decodeBlockBody(rlp.NewBytesStream(withExtraField(t, encode(t, body), sidecars)))
	require.NoError(t, err)
	require.Len(t, got.Transactions, 1)
	require.Equal(t, body.Transactions[0].Hash(), got.Transactions[0].Hash())
}

func TestDecodeBlockBodyWithoutSidecars(t *testing.T) {
	t.Parallel()

	body := testBody()
	got, err := decodeBlockBody(rlp.NewBytesStream(encode(t, body)))
	require.NoError(t, err)
	require.Len(t, got.Transactions, 1)
	require.Equal(t, body.Transactions[0].Hash(), got.Transactions[0].Hash())
}

func TestDecodeBlockBodyRejectsFieldsAfterSidecars(t *testing.T) {
	t.Parallel()

	body := withExtraField(t, withExtraField(t, encode(t, testBody()), sidecars), sidecars)
	_, err := decodeBlockBody(rlp.NewBytesStream(body))
	require.Error(t, err)
}

func TestDecodeNewBlockSkipsSidecars(t *testing.T) {
	t.Parallel()

	body := testBody()
	block := types.NewBlock(&types.Header{Number: *uint256.NewInt(1)}, body.Transactions, nil, nil, body.Withdrawals, nil)
	packet := withExtraField(t, encode(t, &eth.NewBlockPacket{Block: block, TD: *uint256.NewInt(2)}), sidecars)

	got, err := decodeNewBlock(packet)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), got.Block.Hash())
	require.Equal(t, uint64(2), got.TD.Uint64())
}

func TestDecodeNewBlockWithoutSidecars(t *testing.T) {
	t.Parallel()

	block := types.NewBlock(&types.Header{Number: *uint256.NewInt(1)}, nil, nil, nil, nil, nil)
	got, err := decodeNewBlock(encode(t, &eth.NewBlockPacket{Block: block, TD: *uint256.NewInt(2)}))
	require.NoError(t, err)
	require.Equal(t, block.Hash(), got.Block.Hash())
}

func testBody() *types.Body {
	txn := types.NewTransaction(0, common.HexToAddress("0x1"), uint256.NewInt(1), 21000, uint256.NewInt(1), nil)
	return &types.Body{Transactions: []types.Transaction{txn}, Withdrawals: []*types.Withdrawal{}}
}

func encode(t *testing.T, v any) []byte {
	t.Helper()
	b, err := rlp.EncodeToBytes(v)
	require.NoError(t, err)
	return b
}

// withExtraField appends one element to an encoded RLP list.
func withExtraField(t *testing.T, list []byte, extra rlp.RawValue) []byte {
	t.Helper()
	content, _, err := rlp.SplitList(list)
	require.NoError(t, err)
	var elems []rlp.RawValue
	for len(content) > 0 {
		_, _, rest, err := rlp.Split(content)
		require.NoError(t, err)
		elems = append(elems, rlp.RawValue(content[:len(content)-len(rest)]))
		content = rest
	}
	return encode(t, append(elems, extra))
}
