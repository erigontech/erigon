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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// BenchmarkCreateAccessListRejectsAuthorizations measures refusing a list that
// cannot afford its own intrinsic gas. Cost grows with the list because decoding
// it does, but the per-entry share stays in the tens of nanoseconds. Recovering an
// authority is an ECDSA operation costing hundreds of times that, so a check moved
// after the recovery loop shows up here as a per-entry cost in the microseconds.
func BenchmarkCreateAccessListRejectsAuthorizations(b *testing.B) {
	m, bankAddress, _, receiverAddress := chainWithDeployedContractAndConfig(b, chain.AllProtocolChanges)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	key, err := crypto.GenerateKey()
	require.NoError(b, err)
	auth, err := types.SignAuthorization(key, *uint256.NewInt(1337), receiverAddress, 0)
	require.NoError(b, err)

	for _, size := range []int{1024, 16384, 65536} {
		auths := make([]types.JsonAuthorization, size)
		for i := range auths {
			auths[i] = types.JsonAuthorization{}.FromAuthorization(auth)
		}
		args := ethapi.CallArgs{
			From:              &bankAddress,
			To:                &receiverAddress,
			AuthorizationList: auths,
		}

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := api.CreateAccessList(context.Background(), args, nil, nil, nil); err == nil {
					b.Fatal("the list must not fit the gas cap, else this measures the wrong path")
				}
			}
		})
	}
}
