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

		st := NewTxnExecutor(evm, msg, gp)
		_, err := st.Execute(true, false)

		require.ErrorIs(t, err, ErrGasLimitTooHigh)
		require.Equal(t, uint64(blockGasLimit), gp.Gas(),
			"gas pool must be unchanged after EIP-7825 rejection")
	})

	t.Run("valid tx debits gas pool normally", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		evm := newTestEVM(ibs, cfg, blockGasLimit)
		msg := newSimpleTransferMsg(sender, recipient, params.MaxTxnGasLimit, true)
		gp := new(GasPool).AddGas(blockGasLimit)

		st := NewTxnExecutor(evm, msg, gp)
		result, err := st.Execute(true, false)

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
		st1 := NewTxnExecutor(evm1, msg1, gp)
		_, err := st1.Execute(true, false)
		require.ErrorIs(t, err, ErrGasLimitTooHigh)

		poolAfterReject := gp.Gas()
		require.Equal(t, uint64(blockGasLimit), poolAfterReject,
			"gas pool must be unchanged after rejected tx")

		// Second: a valid tx that fits within the cap and remaining pool.
		evm2 := newTestEVM(ibs, cfg, blockGasLimit)
		msg2 := newSimpleTransferMsg(sender, recipient, 100_000, true)
		st2 := NewTxnExecutor(evm2, msg2, gp)
		result, err := st2.Execute(true, false)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Less(t, gp.Gas(), poolAfterReject,
			"second tx must succeed and debit the gas pool")
	})
}

// TestEIP8037_GasPoolTracksRegularAndStateIndependently verifies that the
// EIP-8037 two-dimensional gas pool decrements regular and state budgets
// independently — neither is conflated with max(regular, state).
//
// Regression test for the bug where per-tx deduction used
// max(blockRegularGasUsed, blockStateGasUsed) against a single-dimension
// pool, giving Σ max(r_i, s_i) ≥ max(Σ r_i, Σ s_i) and rejecting valid
// blocks whose per-dimension sums fit (bal-devnet-3, block 193).
func TestEIP8037_GasPoolTracksRegularAndStateIndependently(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	ibs := state.New(state.NewNoopReader())
	gp := NewGasPool(blockGasLimit, 0)
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// TX 1: Contract creation — state gas dominates (intrinsic NEW_ACCOUNT
	// state >> regular). Initcode = STOP (0x00): creates account, deploys no code.
	evm1 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
	msg1 := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 300_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		[]byte{0x00}, nil,
		false, // checkNonce
		false, // checkTransaction
		true,  // checkGas
		false, // isFree
		nil,   // maxFeePerBlobGas
	)
	st1 := NewTxnExecutor(evm1, msg1, gp)
	result1, err := st1.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result1)

	r1 := result1.BlockRegularGasUsed
	s1 := result1.BlockStateGasUsed
	t.Logf("TX1 (create): regular=%d, state=%d", r1, s1)
	require.Greater(t, s1, r1, "contract creation must have state gas > regular gas")

	// After TX1: each dimension drops by its own per-tx usage.
	require.Equal(t, blockGasLimit-r1, gp.RegularGasAvailable(),
		"regular pool must drop by blockRegularGasUsed only")
	require.Equal(t, blockGasLimit-s1, gp.StateGasAvailable(),
		"state pool must drop by blockStateGasUsed only")

	// TX 2: 0-value transfer — regular gas only, no state gas.
	evm2 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
	msg2 := newSimpleTransferMsg(sender, recipient, 100_000, true)
	st2 := NewTxnExecutor(evm2, msg2, gp)
	result2, err := st2.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result2)

	r2 := result2.BlockRegularGasUsed
	s2 := result2.BlockStateGasUsed
	t.Logf("TX2 (transfer): regular=%d, state=%d", r2, s2)
	require.Zero(t, s2, "0-value transfer must not produce state gas")

	// After both TXs: each dimension's remaining budget equals
	// blockGasLimit − Σ (per-tx usage in that dimension), independently.
	totalRegular := r1 + r2
	totalState := s1 + s2
	require.Equal(t, blockGasLimit-totalRegular, gp.RegularGasAvailable(),
		"regular pool deduction must equal Σ blockRegularGasUsed")
	require.Equal(t, blockGasLimit-totalState, gp.StateGasAvailable(),
		"state pool deduction must equal Σ blockStateGasUsed")

	// Neither dimension may be charged max(r, s): if regular were charged
	// max(r1, s1) + max(r2, s2), it would overshoot totalRegular.
	require.NotEqual(t, blockGasLimit-(max(r1, s1)+max(r2, s2)), gp.RegularGasAvailable(),
		"regular pool must not be charged Σ max(r_i, s_i) — that is the pre-378d07cb bug")
}
