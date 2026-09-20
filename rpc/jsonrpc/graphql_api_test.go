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

package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// TestGetAccountStorage_InvalidSlot checks that malformed slot strings are
// rejected with InvalidParamsError before any DB access occurs.
func TestGetAccountStorage_InvalidSlot(t *testing.T) {
	api := &GraphQLAPIImpl{} // db is nil; validation must fire before BeginTemporalRo

	tests := []struct {
		name string
		slot string
	}{
		{"non-hex string", "not-hex"},
		{"too long (33 bytes)", "0x" + strings.Repeat("ab", 33)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := api.GetAccountStorage(context.Background(), common.Address{}, tt.slot, rpc.BlockNumber(0))
			if _, ok := errors.AsType[*rpc.InvalidParamsError](err); !ok {
				t.Errorf("expected *rpc.InvalidParamsError, got %T: %v", err, err)
			}
		})
	}
}

// The graphql_ block responses also go out over JSON-RPC, so the four keys and
// their encodings are a wire contract rather than an internal detail.
func TestMarshalWithdrawalsEncoding(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(marshalWithdrawals(types.Withdrawals{{
		Index:     19_000_042,
		Validator: 881_234,
		Address:   common.HexToAddress("0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"),
		Amount:    63_012_345,
	}}))
	require.NoError(t, err)
	assert.JSONEq(t, `[{"index":"0x121eaea","validator":"0xd7252",`+
		`"address":"0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f","amount":"0x3c17df9"}]`, string(got))

	// A withdrawal amount above 2^63 stays a plain quantity.
	got, err = json.Marshal(marshalWithdrawals(types.Withdrawals{{Amount: hexutil.Uint64(1) << 63}}))
	require.NoError(t, err)
	assert.Contains(t, string(got), `"amount":"0x8000000000000000"`)
}

func TestMarshalWithdrawalsEmpty(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(marshalWithdrawals(nil))
	require.NoError(t, err)
	assert.Equal(t, "[]", string(got))
}

// NewGraphQLReceipt and ethapi.NewRPCTransaction describe the same transaction,
// so a type one reports fee caps for and the other does not is graphql and
// eth_getTransactionByHash disagreeing.
func TestGraphQLReceiptFeeCapsMatchRPCTransaction(t *testing.T) {
	t.Parallel()

	feeCap, tipCap := uint256.NewInt(1000), uint256.NewInt(10)
	// Fresh values per fixture: types.CommonTx holds an atomic hash cache and
	// copylocks rejects copying one out of a variable.
	commonTx := func() types.CommonTx { return types.CommonTx{GasLimit: 21_000} }
	dynamicFee := func() types.DynamicFeeTransaction {
		return types.DynamicFeeTransaction{
			CommonTx: commonTx(),
			ChainID:  *uint256.NewInt(1),
			TipCap:   *tipCap,
			FeeCap:   *feeCap,
		}
	}

	for _, tt := range []struct {
		name string
		txn  types.Transaction
	}{
		{"legacy", &types.LegacyTx{CommonTx: commonTx(), GasPrice: *uint256.NewInt(100)}},
		{"accessList", &types.AccessListTx{
			LegacyTx: types.LegacyTx{CommonTx: commonTx(), GasPrice: *uint256.NewInt(100)},
			ChainID:  *uint256.NewInt(1),
		}},
		{"dynamicFee", &types.DynamicFeeTransaction{
			CommonTx: commonTx(), ChainID: *uint256.NewInt(1), TipCap: *tipCap, FeeCap: *feeCap,
		}},
		{"blob", &types.BlobTx{
			DynamicFeeTransaction: dynamicFee(),
			MaxFeePerBlobGas:      *uint256.NewInt(50),
		}},
		{"setCode", &types.SetCodeTransaction{DynamicFeeTransaction: dynamicFee()}},
		{"accountAbstraction", &types.AccountAbstractionTransaction{
			NonceKey:      uint256.NewInt(0),
			ChainID:       uint256.NewInt(1),
			Tip:           tipCap,
			FeeCap:        feeCap,
			BuilderFee:    uint256.NewInt(0),
			GasLimit:      21_000,
			SenderAddress: accounts.InternAddress(common.HexToAddress("0x01")),
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			chainConfig := &chain.Config{ChainID: uint256.NewInt(1)}
			header := &types.Header{Number: *uint256.NewInt(1)}
			receipt := &types.Receipt{Type: tt.txn.Type(), BlockNumber: uint256.NewInt(1)}

			want := ethapi.NewRPCTransaction(tt.txn, header.Hash(), 0, 1, 0, nil)
			got := NewGraphQLReceipt(receipt, tt.txn, chainConfig, header)

			assert.Equal(t, (*uint256.Int)(want.MaxFeePerGas), got.MaxFeePerGas, "maxFeePerGas")
			assert.Equal(t, (*uint256.Int)(want.MaxPriorityFeePerGas), got.MaxPriorityFeePerGas, "maxPriorityFeePerGas")
		})
	}
}
