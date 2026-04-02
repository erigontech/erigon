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

// newAmsterdamEVM creates an EVM with Amsterdam (EIP-8037) rules enabled,
// including CostPerStateByte derived from the block gas limit.
func newAmsterdamEVM(ibs *state.IntraBlockState, blockGasLimit uint64) *vm.EVM {
	blockCtx := evmtypes.BlockContext{
		CanTransfer:      CanTransfer,
		Transfer:         misc.Transfer,
		GasLimit:         blockGasLimit,
		CostPerStateByte: misc.CostPerStateByte(blockGasLimit),
	}
	return vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
}

// TestEIP8037_GasPoolTracksOnlyRegularGas verifies that the gas pool per-tx
// deduction equals blockRegularGasUsed — not max(regular, state).
//
// Regression test for the bug where per-tx max(regular, state) gave
// Σ max(r_i, s_i) ≥ max(Σ r_i, Σ s_i), overestimating block gas and
// rejecting valid blocks from other clients (bal-devnet-3, block 193).
func TestEIP8037_GasPoolTracksOnlyRegularGas(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	ibs := state.New(state.NewNoopReader())
	gp := new(GasPool).AddGas(blockGasLimit)

	// TX 1: Contract creation — state gas dominates (112 × cpsb >> regular gas).
	// Initcode = STOP (0x00): creates account, deploys no code.
	evm1 := newAmsterdamEVM(ibs, blockGasLimit)
	msg1 := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 200_000,
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

	// After TX1: pool deduction must equal r1, not max(r1, s1).
	require.Equal(t, blockGasLimit-r1, gp.Gas(),
		"pool after TX1 should deduct only regular gas")

	// TX 2: 0-value transfer — regular gas only, no state gas.
	evm2 := newAmsterdamEVM(ibs, blockGasLimit)
	msg2 := newSimpleTransferMsg(sender, recipient, 100_000, true)
	st2 := NewTxnExecutor(evm2, msg2, gp)
	result2, err := st2.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result2)

	r2 := result2.BlockRegularGasUsed
	s2 := result2.BlockStateGasUsed
	t.Logf("TX2 (transfer): regular=%d, state=%d", r2, s2)
	require.Zero(t, s2, "0-value transfer must not produce state gas")

	// Pool deduction over both TXs must equal Σ regular, not Σ max(regular, state).
	totalRegular := r1 + r2
	totalState := s1 + s2
	poolDeducted := blockGasLimit - gp.Gas()

	require.Equal(t, totalRegular, poolDeducted,
		"total pool deduction must equal Σ regular_gas")

	// Verify the old code's sum-of-maxes strictly overestimates.
	sumOfMaxes := max(r1, s1) + max(r2, s2)
	maxOfSums := max(totalRegular, totalState)
	t.Logf("sum-of-maxes (old)=%d, max-of-sums (correct)=%d, pool-deducted=%d",
		sumOfMaxes, maxOfSums, poolDeducted)

	require.Greater(t, sumOfMaxes, maxOfSums,
		"Σ max(r_i, s_i) must strictly exceed max(Σ r_i, Σ s_i) — the old code's overestimation")
}
