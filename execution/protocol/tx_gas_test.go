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

package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/types"
)

// leftBinder binds every transaction to the gas its block admits.
type leftBinder struct{ rules.EngineReader }

func (leftBinder) BindTxGas(txn types.Transaction, gasLeft uint64) (types.Transaction, error) {
	if txn.GetGasLimit() == gasLeft {
		return txn, nil
	}
	return &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: gasLeft}}, nil
}

// TestCheckTxGas checks that a gas limit has to be the one its engine binds it
// to with what the block admits, from Amsterdam also in state gas.
func TestCheckTxGas(t *testing.T) {
	tx := func(gas uint64) types.Transaction { return &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: gas}} }
	gp := NewBlockGasPool(20_000_000, 10_000_000, 0)
	require.NoError(t, CheckTxGas(leftBinder{}, false, tx(20_000_000), gp))
	require.ErrorIs(t, CheckTxGas(leftBinder{}, false, tx(10_000_000), gp), ErrGasLimitPosition)
	// From Amsterdam the block's state gas also bounds what it admits.
	require.NoError(t, CheckTxGas(leftBinder{}, true, tx(10_000_000), gp))
	require.ErrorIs(t, CheckTxGas(leftBinder{}, true, tx(20_000_000), gp), ErrGasLimitPosition)
	// Engines without the rule, and no engine, accept any limit.
	require.NoError(t, CheckTxGas(ethash.NewFaker(), true, tx(1), gp))
	require.NoError(t, CheckTxGas(nil, true, tx(1), gp))
}
