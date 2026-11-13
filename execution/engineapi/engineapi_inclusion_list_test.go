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

package engineapi

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func newGetInclusionListClient(t *testing.T, inclusionList func(context.Context) (types.Transactions, error)) *rpc.Client {
	t.Helper()
	return newEngineInProcClient(t, &EngineServer{logger: log.New(), config: &chain.Config{BogotaTime: common.NewUint64(0)}, executionService: &stubExecutionModule{inclusionListFunc: inclusionList}})
}

func TestGetInclusionListV1(t *testing.T) {
	t.Parallel()

	txnWithData := func(nonce uint64, dataLen int) types.Transaction {
		return types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(0), 21_000, uint256.NewInt(1), make([]byte, dataLen))
	}
	half := int(params.MaxTransactionsBytesPerInclusionListEIP7805) / 2
	a, b, c := txnWithData(0, 100), txnWithData(1, half), txnWithData(2, half)
	blob := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: 3, GasLimit: 21_000, To: &common.Address{1}}},
		BlobVersionedHashes:   []common.Hash{{0x01}},
	}

	for _, tc := range []struct {
		name string
		txns types.Transactions
		want types.Transactions
	}{
		{"encodes_transactions", types.Transactions{a, b}, types.Transactions{a, b}},
		{"skips_transactions_over_byte_limit", types.Transactions{b, c, a}, types.Transactions{b, a}},
		{"skips_blob_transactions", types.Transactions{a, blob}, types.Transactions{a}},
		{"empty", nil, types.Transactions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newGetInclusionListClient(t, func(context.Context) (types.Transactions, error) {
				return tc.txns, nil
			})

			var raw json.RawMessage
			require.NoError(t, client.CallContext(t.Context(), &raw, "engine_getInclusionListV1"))
			var result []hexutil.Bytes
			require.NoError(t, json.Unmarshal(raw, &result))
			require.NotNil(t, result, "an empty inclusion list must be [] rather than null")

			want, err := types.MarshalTransactionsBinary(tc.want)
			require.NoError(t, err)
			require.Len(t, result, len(want))
			total := 0
			for i := range want {
				require.Equal(t, hexutil.Bytes(want[i]), result[i])
				total += len(result[i])
			}
			require.LessOrEqual(t, total, int(params.MaxTransactionsBytesPerInclusionListEIP7805))
		})
	}
}

func TestGetInclusionListV1JSONRPCClient(t *testing.T) {
	t.Parallel()

	txns := types.Transactions{types.NewTransaction(0, common.Address{1}, uint256.NewInt(0), 21_000, uint256.NewInt(1), nil)}
	client := &JsonRpcClient{rpcClient: newGetInclusionListClient(t, func(context.Context) (types.Transactions, error) {
		return txns, nil
	})}

	result, err := client.GetInclusionListV1(t.Context())
	require.NoError(t, err)
	want, err := types.MarshalTransactionsBinary(txns)
	require.NoError(t, err)
	require.Equal(t, []hexutil.Bytes{want[0]}, result)
}

func TestGetInclusionListV1RejectsBeforeBogota(t *testing.T) {
	t.Parallel()

	called := false
	srv := &EngineServer{
		logger: log.New(),
		config: &chain.Config{BogotaTime: common.NewUint64(math.MaxUint64)},
		executionService: &stubExecutionModule{inclusionListFunc: func(context.Context) (types.Transactions, error) {
			called = true
			return nil, nil
		}},
	}

	result, err := srv.GetInclusionListV1(t.Context())

	require.Nil(t, result)
	var unsupported *rpc.UnsupportedForkError
	require.ErrorAs(t, err, &unsupported)
	require.False(t, called)
}
