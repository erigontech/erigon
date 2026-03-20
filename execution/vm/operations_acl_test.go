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

package vm

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestGasCallEIP7702StaticValueTransferDoesNotWarmAddresses(t *testing.T) {
	t.Parallel()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	t.Cleanup(db.Close)
	t.Cleanup(tx.Rollback)

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(domains.Close)

	err = rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	statedb := state.New(state.NewReaderV3(domains.AsGetter(tx)))

	caller := accounts.InternAddress(common.HexToAddress("0x1001"))
	contractAddr := accounts.InternAddress(common.HexToAddress("0x2002"))
	callee := accounts.InternAddress(common.HexToAddress("0x3003"))
	delegate := accounts.InternAddress(common.HexToAddress("0x4004"))

	statedb.CreateAccount(caller, true)
	statedb.CreateAccount(contractAddr, true)
	statedb.CreateAccount(callee, true)
	statedb.SetCode(callee, types.AddressToDelegation(delegate))

	blockCtx := evmtypes.BlockContext{
		BlockNumber: 1,
		Time:        0,
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	evm := NewEVM(blockCtx, evmtypes.TxContext{Origin: caller}, statedb, chain.AllProtocolChanges, Config{})

	rules := evm.ChainRules()
	statedb.Prepare(rules, caller, accounts.ZeroAddress, contractAddr, ActivePrecompiles(rules), nil, nil)
	evm.readOnly = true

	scope := &CallContext{
		Contract: *NewContract(caller, caller, contractAddr, uint256.Int{}),
	}
	scope.Stack.push(*uint256.NewInt(0))                                 // out size
	scope.Stack.push(*uint256.NewInt(0))                                 // out offset
	scope.Stack.push(*uint256.NewInt(0))                                 // in size
	scope.Stack.push(*uint256.NewInt(0))                                 // in offset
	scope.Stack.push(*uint256.NewInt(1))                                 // value
	scope.Stack.push(*new(uint256.Int).SetBytes(callee.Value().Bytes())) // address
	scope.Stack.push(*uint256.NewInt(100000))                            // requested call gas

	gas, err := gasCallEIP7702(evm, scope, 100000, 0)
	require.ErrorIs(t, err, ErrWriteProtection)
	require.Zero(t, gas)
	require.False(t, statedb.AddressInAccessList(callee))
	require.False(t, statedb.AddressInAccessList(delegate))
}
