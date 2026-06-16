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
	"github.com/erigontech/erigon/execution/tracing"
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

// TestPreCheckErrorOrdering_GasBeforeFeeCap asserts the geth-aligned
// validation ordering: a tx that fails both block-gas inclusion AND
// EIP-1559 fee-cap must produce ErrGasLimitReached, not ErrFeeCapTooLow.
//
// Regression test for the parallel-exec gap that prompted PR #21237: the
// parallel worker constructs a per-tx gas pool (trace_worker.go:121) so
// preCheck's gp-branch is a no-op (tx.gas vs tx.gas) under parallel and
// the fee-cap check fires first, mis-classifying the failure for
// EEST/Hive mappers (expected GAS_ALLOWANCE_EXCEEDED, got
// INSUFFICIENT_MAX_FEE_PER_GAS).
//
// The fix routes the same check through CheckBlockGasInclusion against
// the block-level pool, so the ordering is preserved on both paths.
func TestPreCheckErrorOrdering_GasBeforeFeeCap(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000
	cfg := chain.TestChainAuraConfig // London rules active

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	t.Run("tx-gas > block-gas-limit AND fee-cap < baseFee returns ErrGasLimitReached", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		blockCtx := evmtypes.BlockContext{
			CanTransfer: CanTransfer,
			Transfer:    misc.Transfer,
			GasLimit:    blockGasLimit,
			BaseFee:     *uint256.NewInt(100), // non-zero baseFee
		}
		evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, cfg, vm.Config{})

		// feeCap=1, baseFee=100 -> would fail ErrFeeCapTooLow
		// gas=blockGasLimit+1 -> must fail ErrGasLimitReached first
		msg := types.NewMessage(
			sender, recipient, 0, uint256.NewInt(0), blockGasLimit+1,
			uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(1),
			nil, nil,
			false, false, true, false, nil,
		)
		gp := new(GasPool).AddGas(blockGasLimit)

		st := NewTxnExecutor(evm, msg, gp)
		_, err := st.Execute(true, false)

		require.ErrorIs(t, err, ErrGasLimitReached,
			"gas-pool inclusion must reject before fee-cap")
		require.NotErrorIs(t, err, ErrFeeCapTooLow,
			"fee-cap error must not leak past the gas-pool reject")
	})

	t.Run("CheckBlockGasInclusion rejects regular contribution > regular pool", func(t *testing.T) {
		gp := new(GasPool).AddGas(blockGasLimit)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, blockGasLimit+1, 0), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion accepts contribution <= reservoirs", func(t *testing.T) {
		gp := new(GasPool).AddGas(blockGasLimit)
		require.NoError(t, CheckBlockGasInclusion(gp, blockGasLimit, 0))
		require.NoError(t, CheckBlockGasInclusion(gp, blockGasLimit-1, 0))
	})

	t.Run("CheckBlockGasInclusion is a no-op for nil gp", func(t *testing.T) {
		require.NoError(t, CheckBlockGasInclusion(nil, blockGasLimit*1000, blockGasLimit*1000))
	})

	t.Run("CheckBlockGasInclusion rejects state contribution > state pool", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, 50_000, 200_000), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion rejects regular contribution > regular pool (Amsterdam shape)", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, 200_000, 50_000), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion accepts when both contributions fit", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.NoError(t, CheckBlockGasInclusion(gp, 50_000, 80_000))
	})
}

// TestEIP8037_StateGasRefundPreexisting is testing what happens when we create a contract at an address
// that already exists.
// Here, basically what we are doing is, first we deploy a contract with no code (initcode is STOP) at a
// fresh new address. In this case, it must charge the full StateGasNewAccount fee of 183.6k.
// Next, we try the same deployment, but this time at an address that already exists in the state
// (we give it some balance first). Under EIP-8037, this pre-existing check should kick in and we must get
// the StateGasNewAccount fee refunded. So, the state gas used for the pre-existing case should be exactly
// StateGasNewAccount less than the brand-new creation (which in this case means it becomes 0).
func TestEIP8037_StateGasRefundPreexisting(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// 1. Creation at a brand-new address (must charge state gas)
	ibs1 := state.New(state.NewNoopReader())
	gp1 := NewGasPool(blockGasLimit, 0)
	evm1 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs1, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
	// Initcode = STOP (0x00)
	msg1 := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 300_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		[]byte{0x00}, nil,
		false, false, true, false, nil,
	)
	st1 := NewTxnExecutor(evm1, msg1, gp1)
	result1, err := st1.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result1)

	regularStateGas := result1.BlockStateGasUsed
	require.GreaterOrEqual(t, regularStateGas, uint64(params.StateGasNewAccount))

	// 2. Creation at a pre-existing address (must refund/exclude params.StateGasNewAccount)
	ibs2 := state.New(state.NewNoopReader())
	gp2 := NewGasPool(blockGasLimit, 0)
	evm2 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs2, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})

	// Compute the contract address for sender at nonce 0
	targetAddr := accounts.InternAddress(types.CreateAddress(sender.Value(), 0))
	// Make it pre-existing by giving it balance
	ibs2.AddBalance(targetAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified)

	st2 := NewTxnExecutor(evm2, msg1, gp2)
	result2, err := st2.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result2)

	preexistingStateGas := result2.BlockStateGasUsed
	t.Logf("State gas used: brand-new = %d, pre-existing = %d", regularStateGas, preexistingStateGas)

	// Verify that the state gas used for pre-existing account is exactly StateGasNewAccount less
	require.Equal(t, regularStateGas-uint64(params.StateGasNewAccount), preexistingStateGas,
		"State gas for pre-existing account must be StateGasNewAccount less than brand-new account")
}

// TestEIP8037_StateGasRefundPreexistingWithCodeDeposit is verifying how the refund works when code is
// actually deposited.
// Here, we deploy a contract that returns a 10-byte runtime code.
// For a brand-new address, it should charge the new account fee (183.6k) plus the code deposit cost
// (10 * 192 = 1,920 gas).
// But for a pre-existing target address, the new account fee must be refunded, but the code deposit
// fee of 1,920 gas must still be charged!
// So, we are checking that the pre-existing target's state gas usage ends up being exactly the code
// deposit cost, and the base creation fee is successfully refunded.
func TestEIP8037_StateGasRefundPreexistingWithCodeDeposit(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// Bytecode that returns a 10-byte code:
	// PUSH10 0x0102030405060708090a (0x69 0102030405060708090a)
	// PUSH1 0x00 (0x60 00)
	// MSTORE (0x52)
	// PUSH1 10 (0x60 0a)
	// PUSH1 22 (0x60 16) - MSTORE stores at offset 32 - 10 = 22 for right alignment
	// RETURN (0xf3)
	// Total initcode: 690102030405060708090a600052600a6016f3
	initCode := common.Hex2Bytes("690102030405060708090a600052600a6016f3")

	// 1. Creation at a brand-new address (must charge state gas for new account + 10 bytes code deposit)
	ibs1 := state.New(state.NewNoopReader())
	gp1 := NewGasPool(blockGasLimit, 0)
	evm1 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs1, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
	msg1 := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 300_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		initCode, nil,
		false, false, true, false, nil,
	)
	st1 := NewTxnExecutor(evm1, msg1, gp1)
	result1, err := st1.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result1)

	regularStateGas := result1.BlockStateGasUsed
	expectedNewAccountGas := uint64(params.StateGasNewAccount) + 10*uint64(params.CostPerStateByte)
	require.Equal(t, expectedNewAccountGas, regularStateGas)

	// 2. Creation at a pre-existing address (must refund params.StateGasNewAccount but keep 10 bytes code deposit)
	ibs2 := state.New(state.NewNoopReader())
	gp2 := NewGasPool(blockGasLimit, 0)
	evm2 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs2, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})

	// Compute the contract address for sender at nonce 0
	targetAddr := accounts.InternAddress(types.CreateAddress(sender.Value(), 0))
	// Make it pre-existing by giving it balance
	ibs2.AddBalance(targetAddr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified)

	st2 := NewTxnExecutor(evm2, msg1, gp2)
	result2, err := st2.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result2)

	preexistingStateGas := result2.BlockStateGasUsed
	t.Logf("State gas used with code: brand-new = %d, pre-existing = %d", regularStateGas, preexistingStateGas)

	// Verify that the state gas used for pre-existing account is exactly 10 * CostPerStateByte
	require.Equal(t, 10*uint64(params.CostPerStateByte), preexistingStateGas)
}

// TestEIP8037_StateGasSpilloverRevert checks what happens when a nested contract creation reverts.
// Here, basically what we are doing is, we are testing that if a sub-creation reverts,
// then whatever state gas was charged for it (which actually spilled over into the regular gas pool
// because the reservoir was empty) should be fully refunded back to the regular gas pool of the parent.
// So, at the end, only the parent's creation state gas of 183.6k should be charged, and the reverted
// child's state gas must be returned completely.
func TestEIP8037_StateGasSpilloverRevert(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// Parent initcode that runs CREATE on a child that reverts.
	// Bytecode: 6460006000fd6000526005601b6000f05000
	initCode := common.Hex2Bytes("6460006000fd6000526005601b6000f05000")

	ibs := state.New(state.NewNoopReader())
	gp := NewGasPool(blockGasLimit, 0)
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})

	// Use a small gas limit (but enough to run without OOG) so that the state reservoir split is 0,
	// forcing the 183.6k CREATE state gas charge to spill over into the regular gas pool.
	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 500_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		initCode, nil,
		false, false, true, false, nil,
	)
	st := NewTxnExecutor(evm, msg, gp)
	result, err := st.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	t.Logf("Spillover revert test - regular gas used: %d, state gas used: %d", result.BlockRegularGasUsed, result.BlockStateGasUsed)

	// Since the sub-creation reverted, the state gas charged for it should be fully refunded.
	// The parent creation itself succeeded (it returned empty code via STOP).
	// So the only net state gas charge should be the parent contract creation cost (params.StateGasNewAccount = 183600).
	require.Equal(t, uint64(params.StateGasNewAccount), result.BlockStateGasUsed,
		"The reverted sub-creation must not leave any net state gas charge")
}

// TestEIP8037_StateGasSpilloverHalt checks the exceptional halt scenario.
// In this test, we are checking that if the child contract creation execution fails with an INVALID
// instruction, the child frame halts. According to EIP-8037 rules, since it is a halt, all the regular
// gas given to it is gone/burned. But, the state changes are rolled back, so the state gas charged for
// the sub-creation must still be restored/refunded. Therefore, the net state gas charged at the
// transaction level should only be for the parent creation, and the child's charge must not be kept.
func TestEIP8037_StateGasSpilloverHalt(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// Parent initcode that runs CREATE on a child that executes INVALID (0xfe).
	// Bytecode: 626000fe6000526003601d6000f05000
	initCode := common.Hex2Bytes("626000fe6000526003601d6000f05000")

	ibs := state.New(state.NewNoopReader())
	gp := NewGasPool(blockGasLimit, 0)
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})

	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 500_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		initCode, nil,
		false, false, true, false, nil,
	)
	st := NewTxnExecutor(evm, msg, gp)
	result, err := st.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	t.Logf("Spillover halt test - regular gas used: %d, state gas used: %d", result.BlockRegularGasUsed, result.BlockStateGasUsed)

	// The child halted, so the child's state changes are reverted and the sub-creation cost is refunded.
	// Parent creation succeeded.
	require.Equal(t, uint64(params.StateGasNewAccount), result.BlockStateGasUsed,
		"The halted sub-creation must not leave any net state gas charge")
}

