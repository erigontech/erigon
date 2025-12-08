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

package eth1_utils

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func makeBlock(txCount, uncleCount, withdrawalCount int) *types.Block {
	var (
		key, _      = crypto.GenerateKey()
		txs         = make([]types.Transaction, txCount)
		receipts    = make([]*types.Receipt, len(txs))
		signer      = types.LatestSigner(chain.TestChainConfig)
		uncles      = make([]*types.Header, uncleCount)
		withdrawals = make([]*types.Withdrawal, withdrawalCount)
	)
	header := &types.Header{
		Difficulty: math.BigPow(11, 11),
		Number:     math.BigPow(2, 9),
		GasLimit:   12345678,
		GasUsed:    1476322,
		Time:       9876543,
		Extra:      []byte("test block"),
	}
	for i := range txs {
		amount, _ := uint256.FromBig(math.BigPow(2, int64(i)))
		price := uint256.NewInt(300000)
		data := make([]byte, 100)
		tx := types.NewTransaction(uint64(i), common.Address{}, amount, 123457, price, data)
		signedTx, err := types.SignTx(tx, *signer, key)
		if err != nil {
			panic(err)
		}
		txs[i] = signedTx
		receipts[i] = types.NewReceipt(false, tx.GetGasLimit())
	}
	for i := range uncles {
		uncles[i] = &types.Header{
			Difficulty: math.BigPow(11, 11),
			Number:     math.BigPow(2, 9),
			GasLimit:   12345678,
			GasUsed:    1476322,
			Time:       9876543,
			Extra:      []byte("test uncle"),
		}
	}
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{
			Index:     uint64(i),
			Validator: uint64(i),
			Amount:    uint64(10 * i),
		}
	}
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{
			Index:     uint64(i),
			Validator: uint64(i),
			Amount:    uint64(10 * i),
		}
	}
	return types.NewBlock(header, txs, uncles, receipts, withdrawals)
}

func TestBlockRpcConversion(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	testBlock := makeBlock(50, 2, 3)

	// header conversions
	rpcHeader := HeaderToHeaderRPC(testBlock.Header())
	roundTripHeader, err := HeaderRpcToHeader(rpcHeader)
	if err != nil {
		panic(err)
	}

	deep.CompareUnexportedFields = true
	require.Nil(deep.Equal(testBlock.HeaderNoCopy(), roundTripHeader))

	// body conversions
	rpcBlock := ConvertBlockToRPC(testBlock)
	roundTripBody, err := ConvertRawBlockBodyFromRpc(rpcBlock.Body)
	if err != nil {
		panic(err)
	}
	testBlockRaw := testBlock.RawBody()
	require.NotEmpty(testBlockRaw.Transactions)
	require.NotEmpty(testBlockRaw.Uncles)
	require.NotEmpty(testBlockRaw.Withdrawals)
	require.Nil(deep.Equal(testBlockRaw, roundTripBody))
}

func TestBigIntConversion(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	val := math.BigPow(2, 32)
	rpcVal := ConvertBigIntToRpc(val)
	roundTripVal := ConvertBigIntFromRpc(rpcVal)
	require.Equal(val, roundTripVal)
}
