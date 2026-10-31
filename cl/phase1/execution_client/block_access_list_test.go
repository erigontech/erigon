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

package execution_client

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestDecodeAndValidateBlockAccessListBoundsChangeAllocations(t *testing.T) {
	changes := make([][2]uint64, 100_000)
	for i := range changes {
		changes[i] = [2]uint64{uint64(i), 1}
	}
	checkBlockAccessListChangeAllocations(t, changes, "block access index")
}

func TestDecodeAndValidateBlockAccessListBoundsDuplicateChangeAllocations(t *testing.T) {
	changes := make([][2]uint64, 100_000)
	for i := range changes {
		changes[i] = [2]uint64{0, 1}
	}
	checkBlockAccessListChangeAllocations(t, changes, "indices must be strictly increasing")
}

func checkBlockAccessListChangeAllocations(t *testing.T, changes [][2]uint64, wantError string) {
	t.Helper()
	for _, tt := range []struct {
		name  string
		field int
	}{
		{name: "storage", field: 1},
		{name: "balance", field: 3},
		{name: "nonce", field: 4},
		{name: "code", field: 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields := []any{common.Address{}, []any{}, []any{}, []any{}, []any{}, []any{}}
			fields[tt.field] = changes
			if tt.field == 1 {
				fields[tt.field] = []any{[]any{uint64(0), changes}}
			}
			raw, err := rlp.EncodeToBytes([]any{fields})
			require.NoError(t, err)
			payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
			payload.GasLimit = 60_000_000
			require.NoError(t, payload.BlockAccessList.SetBytes(raw))
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err = DecodeAndValidateBlockAccessList(payload)
			runtime.ReadMemStats(&after)
			require.ErrorContains(t, err, wantError)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
		})
	}
}

func TestDecodeAndValidateBlockAccessList(t *testing.T) {
	t.Parallel()
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc0}))

	bal, err := DecodeAndValidateBlockAccessList(payload)
	require.NoError(t, err)
	require.NotNil(t, bal)
	require.Empty(t, bal.BlockAccessList())
	raw, err := bal.Bytes()
	require.NoError(t, err)
	require.Equal(t, []byte{0xc0}, raw)

	require.NoError(t, payload.BlockAccessList.SetBytes([]byte{0xc2, 0x01, 0x02}))
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.Error(t, err)

	oversized, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{
		Address: common.Address{1},
	}})
	require.NoError(t, err)
	require.NoError(t, payload.BlockAccessList.SetBytes(oversized))
	payload.GasLimit = types.BalItemCost - 1
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.ErrorContains(t, err, "block access list too large")

	withNonceChangeAt := func(index uint32) {
		encoded, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{{
			Address:      common.Address{1},
			NonceChanges: []*types.NonceChange{{Index: index}},
		}})
		require.NoError(t, err)
		require.NoError(t, payload.BlockAccessList.SetBytes(encoded))
	}
	payload.GasLimit = types.BalItemCost
	withNonceChangeAt(2)
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.ErrorContains(t, err, "block access index", "a payload without a transaction list only has the two system indices")

	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions([][]byte{{1}})
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.NoError(t, err, "len(transactions)+1 is the post-execution index")
	withNonceChangeAt(3)
	_, err = DecodeAndValidateBlockAccessList(payload)
	require.ErrorContains(t, err, "block access index")
}
