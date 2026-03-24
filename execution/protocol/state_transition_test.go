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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// newTestEVM creates a minimal EVM suitable for state transition tests.
// Uses NoBaseFee + zero gas prices so no sender balance is required.
func newTestEVM(ibs *state.IntraBlockState, cfg *chain.Config, blockGasLimit uint64) *vm.EVM {
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}
	txCtx := evmtypes.TxContext{}
	return vm.NewEVM(blockCtx, txCtx, ibs, cfg, vm.Config{NoBaseFee: true})
}

// newSimpleTransferMsg creates a zero-value transfer message with the given gas limit.
func newSimpleTransferMsg(from, to accounts.Address, gas uint64, checkGas bool) *types.Message {
	return types.NewMessage(
		from, to, 0, uint256.NewInt(0), gas,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		nil, nil,
		false, // checkNonce
		false, // checkTransaction
		checkGas,
		false, // isFree
		nil,   // maxFeePerBlobGas
	)
}

// TestEIP7825_GasPoolPreservedOnReject verifies that when a transaction is
// rejected by the EIP-7825 gas limit cap, the block gas pool is NOT depleted.
//
// Regression test for the bug where buyGas() debited the pool before the cap
// check, and on rejection the debit was never reversed — exhausting the pool
// mid-block and causing subsequent valid transactions to fail with "gas limit
// reached".
func TestEIP7825_GasPoolPreservedOnReject(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	cfg := chain.TestChainOsakaConfig

	t.Run("rejected tx preserves gas pool", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		evm := newTestEVM(ibs, cfg, blockGasLimit)
		msg := newSimpleTransferMsg(sender, recipient, params.MaxTxnGasLimit+1, true)
		gp := new(GasPool).AddGas(blockGasLimit)

		st := NewStateTransition(evm, msg, gp)
		_, err := st.TransitionDb(true, false)

		require.ErrorIs(t, err, ErrGasLimitTooHigh)
		require.Equal(t, uint64(blockGasLimit), gp.Gas(),
			"gas pool must be unchanged after EIP-7825 rejection")
	})

	t.Run("valid tx debits gas pool normally", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		evm := newTestEVM(ibs, cfg, blockGasLimit)
		msg := newSimpleTransferMsg(sender, recipient, params.MaxTxnGasLimit, true)
		gp := new(GasPool).AddGas(blockGasLimit)

		st := NewStateTransition(evm, msg, gp)
		result, err := st.TransitionDb(true, false)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Less(t, gp.Gas(), uint64(blockGasLimit),
			"gas pool must be debited for a valid transaction")
	})

	t.Run("consecutive txs after rejection", func(t *testing.T) {
		// Simulate the original bug scenario: a rejected tx should not prevent
		// a subsequent valid tx from succeeding due to pool exhaustion.
		ibs := state.New(state.NewNoopReader())
		gp := new(GasPool).AddGas(blockGasLimit)

		// First: a tx that exceeds the cap — must be rejected without touching pool.
		evm1 := newTestEVM(ibs, cfg, blockGasLimit)
		msg1 := newSimpleTransferMsg(sender, recipient, params.MaxTxnGasLimit+1, true)
		st1 := NewStateTransition(evm1, msg1, gp)
		_, err := st1.TransitionDb(true, false)
		require.ErrorIs(t, err, ErrGasLimitTooHigh)

		poolAfterReject := gp.Gas()
		require.Equal(t, uint64(blockGasLimit), poolAfterReject,
			"gas pool must be unchanged after rejected tx")

		// Second: a valid tx that fits within the cap and remaining pool.
		evm2 := newTestEVM(ibs, cfg, blockGasLimit)
		msg2 := newSimpleTransferMsg(sender, recipient, 100_000, true)
		st2 := NewStateTransition(evm2, msg2, gp)
		result, err := st2.TransitionDb(true, false)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Less(t, gp.Gas(), poolAfterReject,
			"second tx must succeed and debit the gas pool")
	})
}
