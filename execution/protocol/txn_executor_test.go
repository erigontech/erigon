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
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
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

type nilBlobFeeCapMessage struct {
	*types.Message
}

func (nilBlobFeeCapMessage) MaxFeePerBlobGas() *uint256.Int { return nil }

func eip2780TestAuthorization() (types.Authorization, accounts.Address) {
	auth := types.Authorization{
		ChainID: *uint256.NewInt(7088110746),
		Address: common.Address{180, 125, 156, 99, 77, 80, 241, 96, 13, 77, 247, 103, 233, 71, 76, 37, 160, 48, 52, 40},
		Nonce:   1,
		YParity: 1,
		R:       uint256.Int{11238962557009670571, 14017651393191758745, 18358999445216475025, 5549385460848219779},
		S:       uint256.Int{6390522493159340108, 17630603794136184458, 14442462445950880280, 846710983706847255},
	}
	return auth, accounts.InternAddress(common.HexToAddress("0x8ED5ABe9DE62dB2F266b06b86203f71e4C1e357f"))
}

func eip2780TestConfig(t *testing.T) *chain.Config {
	t.Helper()
	cfg := new(chain.Config)
	require.NoError(t, copier.CopyWithOption(cfg, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
	cfg.ChainID = uint256.NewInt(7088110746)
	return cfg
}

type codeAccessRecordingReader struct {
	state.StateReader
	accesses []accounts.Address
}

func (r *codeAccessRecordingReader) OnCodeAccess(addr accounts.Address, _ []byte) {
	r.accesses = append(r.accesses, addr)
}

type accountErrorReader struct {
	state.StateReader
	err error
}

func (r *accountErrorReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return nil, r.err
}

// TestEIP7825_GasPoolPreservedOnReject verifies that rejecting a transaction
// above the gas-limit cap does not consume block gas needed by later
// transactions.
func TestEIP7825_GasPoolPreservedOnReject(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	cfg := chain.TestChainOsakaConfig

	t.Run("rejected tx preserves gas pool", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
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
		defer ibs.Close()
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
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		gp := new(GasPool).AddGas(blockGasLimit)

		evm1 := newTestEVM(ibs, cfg, blockGasLimit)
		msg1 := newSimpleTransferMsg(sender, recipient, params.MaxTxnGasLimit+1, true)
		st1 := NewTxnExecutor(evm1, msg1, gp)
		_, err := st1.Execute(true, false)
		require.ErrorIs(t, err, ErrGasLimitTooHigh)

		poolAfterReject := gp.Gas()
		require.Equal(t, uint64(blockGasLimit), poolAfterReject,
			"gas pool must be unchanged after rejected tx")

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

// TestIntrinsicGasReject_NoStateMutation verifies that EIP-7623 calldata-floor
// rejection reports the effective requirement without changing sender state.
func TestIntrinsicGasReject_NoStateMutation(t *testing.T) {
	t.Parallel()

	const (
		blockGasLimit = 30_000_000
		// 100 zero-byte calldata: execution intrinsic = 21000 + 100*4 = 21400;
		// EIP-7623 floor = 21000 + 100*10 = 22000. A gas limit between the two
		// clears the execution intrinsic check but fails the floor check.
		gasLimit = 21404
	)

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	cfg := chain.TestChainOsakaConfig // Prague active (EIP-7623 floor), Amsterdam inactive

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	initialBalance := uint256.NewInt(1_000_000_000_000_000_000)
	require.NoError(t, ibs.AddBalance(sender, *initialBalance, tracing.BalanceChangeUnspecified))

	evm := newTestEVM(ibs, cfg, blockGasLimit)
	gasPrice := uint256.NewInt(1_000_000_000)
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), gasLimit,
		gasPrice, gasPrice, gasPrice,
		make([]byte, 100), nil,
		false, // checkNonce
		false, // checkTransaction
		true,  // checkGas
		false, // isFree
		nil,   // maxFeePerBlobGas
	)
	gp := new(GasPool).AddGas(blockGasLimit)

	_, err := NewTxnExecutor(evm, msg, gp).Execute(true, false)
	require.ErrorIs(t, err, ErrIntrinsicGas, "tx below the EIP-7623 floor must be rejected")
	require.ErrorContains(t, err, "have 21404, want 22000")

	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.Zero(t, nonce, "sender nonce must not be incremented when the tx is rejected for intrinsic gas")

	balance, err := ibs.GetBalance(sender)
	require.NoError(t, err)
	require.Equal(t, *initialBalance, balance, "sender balance must not be debited when the tx is rejected for intrinsic gas")
}

// TestPreCheck_InsufficientFundsBeforeIntrinsicGas verifies geth-compatible
// error precedence when a transaction is both unaffordable and below intrinsic
// gas.
func TestPreCheck_InsufficientFundsBeforeIntrinsicGas(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	cfg := chain.TestChainOsakaConfig

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.AddBalance(sender, *uint256.NewInt(5120), tracing.BalanceChangeUnspecified))

	evm := newTestEVM(ibs, cfg, blockGasLimit)
	gasPrice := uint256.NewInt(20)
	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(366), 21000,
		gasPrice, gasPrice, gasPrice,
		nil, nil,
		false, // checkNonce
		false, // checkTransaction
		true,  // checkGas
		false, // isFree
		nil,   // maxFeePerBlobGas
	)
	gp := new(GasPool).AddGas(blockGasLimit)

	_, err := NewTxnExecutor(evm, msg, gp).Execute(true, false)
	require.ErrorIs(t, err, ErrInsufficientFunds, "insufficient funds must take precedence over intrinsic gas")
	require.NotErrorIs(t, err, ErrIntrinsicGas)
}

// TestEIP8037_GasPoolTracksExecutionAndStateIndependently verifies that each gas
// dimension accumulates independently. Summing max(execution, state) per
// transaction can exceed max(total execution, total state).
func TestEIP8037_GasPoolTracksExecutionAndStateIndependently(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 60_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	gp := NewGasPool(blockGasLimit, 0)
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}

	// TX 1: Contract creation — state gas dominates (intrinsic NEW_ACCOUNT
	// state >> execution). Initcode = STOP (0x00): creates account, deploys no code.
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

	r1 := result1.BlockExecutionGasUsed
	s1 := result1.BlockStateGasUsed
	t.Logf("TX1 (create): execution=%d, state=%d", r1, s1)
	require.Greater(t, s1, r1, "contract creation must have state gas > execution gas")

	// After TX1: each dimension drops by its own per-tx usage.
	require.Equal(t, blockGasLimit-r1, gp.ExecutionGasAvailable(),
		"execution pool must drop by blockExecutionGasUsed only")
	require.Equal(t, blockGasLimit-s1, gp.StateGasAvailable(),
		"state pool must drop by blockStateGasUsed only")

	// TX 2: 0-value transfer — execution gas only, no state gas.
	evm2 := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{NoBaseFee: true})
	msg2 := newSimpleTransferMsg(sender, recipient, 100_000, true)
	st2 := NewTxnExecutor(evm2, msg2, gp)
	result2, err := st2.Execute(true, false)
	require.NoError(t, err)
	require.NotNil(t, result2)

	r2 := result2.BlockExecutionGasUsed
	s2 := result2.BlockStateGasUsed
	t.Logf("TX2 (transfer): execution=%d, state=%d", r2, s2)
	require.Zero(t, s2, "0-value transfer must not produce state gas")

	// After both TXs: each dimension's remaining budget equals
	// blockGasLimit − Σ (per-tx usage in that dimension), independently.
	totalExecution := r1 + r2
	totalState := s1 + s2
	require.Equal(t, blockGasLimit-totalExecution, gp.ExecutionGasAvailable(),
		"execution pool deduction must equal Σ blockExecutionGasUsed")
	require.Equal(t, blockGasLimit-totalState, gp.StateGasAvailable(),
		"state pool deduction must equal Σ blockStateGasUsed")

	// Neither dimension may be charged max(r, s): if execution were charged
	// max(r1, s1) + max(r2, s2), it would overshoot totalExecution.
	require.NotEqual(t, blockGasLimit-(max(r1, s1)+max(r2, s2)), gp.ExecutionGasAvailable(),
		"execution pool must not be charged Σ max(r_i, s_i) — that is the pre-378d07cb bug")
}

func TestEIP2780AuthorizationChargesAtRuntime(t *testing.T) {
	const blockGasLimit = 1_000_000

	auth, authority := eip2780TestAuthorization()
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	ibs := state.New(state.NewNoopReader())
	require.NoError(t, ibs.SetNonce(authority, auth.Nonce, tracing.NonceChangeUnspecified))
	evm := newTestEVM(ibs, eip2780TestConfig(t), blockGasLimit)
	msg := newSimpleTransferMsg(sender, recipient, 100_000, true)
	msg.SetAuthorizations([]types.Authorization{auth})

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, params.TxBaseEIP2780+params.ColdAccountAccessEIP2780+params.ExecutionPerAuthBaseCostEIP8038+params.AccountWriteCostEIP8038, result.BlockExecutionGasUsed)
	require.Equal(t, uint64(params.StateGasAuthBase), result.BlockStateGasUsed)
	require.Equal(t, result.BlockExecutionGasUsed+result.BlockStateGasUsed, result.ReceiptGasUsed)

	nonce, err := ibs.GetNonce(authority)
	require.NoError(t, err)
	require.Equal(t, auth.Nonce+1, nonce)
	delegatedTo, ok, err := ibs.GetDelegatedDesignation(authority)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, accounts.InternAddress(auth.Address), delegatedTo)
}

func TestEIP2780AuthorizationOutOfGasRollsBack(t *testing.T) {
	const blockGasLimit = 1_000_000

	auth, authority := eip2780TestAuthorization()
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	gasLimit := params.TxBaseEIP2780 + params.ColdAccountAccessEIP2780 + params.ExecutionPerAuthBaseCostEIP8038 + params.AccountWriteCostEIP8038 + params.StateGasAuthBase - 1

	ibs := state.New(state.NewNoopReader())
	require.NoError(t, ibs.SetNonce(authority, auth.Nonce, tracing.NonceChangeUnspecified))
	evm := newTestEVM(ibs, eip2780TestConfig(t), blockGasLimit)
	msg := newSimpleTransferMsg(sender, recipient, gasLimit, true)
	msg.SetAuthorizations([]types.Authorization{auth})

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
	require.Equal(t, gasLimit, result.ReceiptGasUsed)
	require.Equal(t, gasLimit, result.BlockExecutionGasUsed)
	require.Zero(t, result.BlockStateGasUsed)

	nonce, err := ibs.GetNonce(authority)
	require.NoError(t, err)
	require.Equal(t, auth.Nonce, nonce)
	_, ok, err := ibs.GetDelegatedDesignation(authority)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestEIP2780AuthorizationOutOfGasProducesCallTrace(t *testing.T) {
	const blockGasLimit = 1_000_000

	auth, authority := eip2780TestAuthorization()
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	gasLimit := params.TxBaseEIP2780 + params.ColdAccountAccessEIP2780 + params.ExecutionPerAuthBaseCostEIP8038 + params.AccountWriteCostEIP8038 + params.StateGasAuthBase - 1

	for _, tracerName := range []string{"callTracer", "flatCallTracer"} {
		t.Run(tracerName, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetNonce(authority, auth.Nonce, tracing.NonceChangeUnspecified))

			tracer, err := tracers.New(tracerName, &tracers.Context{}, json.RawMessage("{}"))
			require.NoError(t, err)
			ibs.SetHooks(tracer.Hooks)
			blockCtx := evmtypes.BlockContext{
				CanTransfer: CanTransfer,
				Transfer:    misc.Transfer,
				GasLimit:    blockGasLimit,
			}
			evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, eip2780TestConfig(t), vm.Config{
				NoBaseFee: true,
				Tracer:    tracer.Hooks,
			})
			msg := newSimpleTransferMsg(sender, recipient, gasLimit, true)
			msg.SetAuthorizations([]types.Authorization{auth})
			tx := types.NewTransaction(0, recipient.Value(), uint256.NewInt(0), gasLimit, uint256.NewInt(0), nil)

			tracer.OnTxStart(evm.GetVMContext(), tx, sender)
			result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
			require.NoError(t, err)
			require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
			tracer.OnTxEnd(&types.Receipt{GasUsed: result.ReceiptGasUsed}, nil)

			trace, err := tracer.GetResult()
			require.NoError(t, err)
			require.Contains(t, string(trace), `"error":"runtime: out of gas"`)
		})
	}
}

func TestEIP2780TopLevelCallTraceStartsBeforeStateChanges(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	for _, tc := range []struct {
		name       string
		value      uint64
		tracerCfg  string
		wantTxLogs bool
	}{
		{name: "zero value", tracerCfg: "{}"},
		{name: "value transfer log", value: 1, tracerCfg: `{"withLog":true}`, wantTxLogs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetBalance(sender, *uint256.NewInt(tc.value), tracing.BalanceChangeUnspecified))

			tracer, err := tracers.New("callTracer", &tracers.Context{}, json.RawMessage(tc.tracerCfg))
			require.NoError(t, err)
			ibs.SetHooks(tracer.Hooks)
			blockCtx := evmtypes.BlockContext{
				CanTransfer: CanTransfer,
				Transfer:    misc.Transfer,
				GasLimit:    blockGasLimit,
			}
			evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, vm.Config{
				NoBaseFee: true,
				Tracer:    tracer.Hooks,
			})
			value := uint256.NewInt(tc.value)
			msg := types.NewMessage(
				sender, recipient, 0, value, blockGasLimit,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
				nil, nil, false, false, true, false, nil,
			)
			tx := types.NewTransaction(0, recipient.Value(), value, blockGasLimit, uint256.NewInt(0), nil)

			tracer.OnTxStart(evm.GetVMContext(), tx, sender)
			var result *evmtypes.ExecutionResult
			require.NotPanics(t, func() {
				result, err = NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
			})
			require.NoError(t, err)
			require.NoError(t, result.Err)
			tracer.OnTxEnd(&types.Receipt{GasUsed: result.ReceiptGasUsed}, nil)

			trace, err := tracer.GetResult()
			require.NoError(t, err)
			if tc.wantTxLogs {
				require.Contains(t, string(trace), `"logs"`)
			}
		})
	}
}

func TestEIP2780RecipientStartsWarm(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetCode(recipient, []byte{byte(vm.ADDRESS), byte(vm.BALANCE), byte(vm.STOP)}, tracing.CodeChangeUnspecified))

	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := newSimpleTransferMsg(sender, recipient, 100_000, true)
	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t,
		params.TxBaseEIP2780+params.ColdAccountAccessEIP2780+vm.GasQuickStep+params.WarmStorageReadCostEIP2929,
		result.BlockExecutionGasUsed,
	)
}

func TestEIP2780DelegationTargetAccessUsesWarmCost(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	delegatedTo := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	accessList := types.AccessList{{Address: delegatedTo.Value()}}

	ibs := state.New(state.NewNoopReader())
	require.NoError(t, ibs.SetCode(recipient, types.AddressToDelegation(delegatedTo), tracing.CodeChangeUnspecified))
	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 100_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		nil, accessList, false, false, true, false, nil,
	)

	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		AccessListLen: 1,
		IsEIP2:        true,
		IsEIP2028:     true,
		IsEIP7623:     true,
		IsEIP7976:     true,
		IsEIP7981:     true,
		IsEIP2780:     true,
	})
	require.False(t, overflow)

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, intrinsic.ExecutionGas+params.WarmStorageReadCostEIP2929, result.BlockExecutionGasUsed)
	require.Zero(t, result.BlockStateGasUsed)
}

func TestEIP2780DelegationTargetIsNotReadWhenAccessChargeRunsOutOfGas(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	delegatedTo := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	reader := &codeAccessRecordingReader{StateReader: state.NewNoopReader()}
	ibs := state.New(reader)
	require.NoError(t, ibs.SetCode(recipient, types.AddressToDelegation(delegatedTo), tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(delegatedTo, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))

	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		IsEIP2:    true,
		IsEIP2028: true,
		IsEIP7623: true,
		IsEIP7976: true,
		IsEIP7981: true,
		IsEIP2780: true,
	})
	require.False(t, overflow)
	gasLimit := intrinsic.ExecutionGas + params.ColdAccountAccessEIP2780 - 1
	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := newSimpleTransferMsg(sender, recipient, gasLimit, true)

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
	require.Contains(t, reader.accesses, recipient)
	require.NotContains(t, reader.accesses, delegatedTo)
}

func TestEIP2780RecipientRuntimeOutOfGasPrecedesTransfer(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	value := uint256.NewInt(1)
	intrinsic := params.TxBaseEIP2780 +
		params.ColdAccountAccessEIP2780 +
		params.TransferLogCostEIP2780 +
		params.TxValueCostEIP2780
	gasLimit := intrinsic + params.StateGasNewAccount - 1

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetBalance(sender, *value, tracing.BalanceChangeUnspecified))

	var logs int
	ibs.SetHooks(&tracing.Hooks{
		OnLog: func(*types.Log) {
			logs++
		},
	})
	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := types.NewMessage(
		sender, recipient, 0, value, gasLimit,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		nil, nil, false, false, true, false, nil,
	)

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
	require.Zero(t, logs)
}

func TestEIP2780CalldataFloorBindsBlockExecutionGas(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	data := make([]byte, 128)
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:      data,
		IsEIP2:    true,
		IsEIP2028: true,
		IsEIP7623: true,
		IsEIP7976: true,
		IsEIP7981: true,
		IsEIP2780: true,
	})
	require.False(t, overflow)
	require.Greater(t, intrinsic.FloorGasCost, intrinsic.ExecutionGas)

	ibs := state.New(state.NewNoopReader())
	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 100_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		data, nil, false, false, true, false, nil,
	)

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, intrinsic.FloorGasCost, result.ReceiptGasUsed)
	require.Equal(t, intrinsic.FloorGasCost, result.BlockExecutionGasUsed)
	require.Zero(t, result.BlockStateGasUsed)
}

func TestEIP2780ContractCreationRuntimeOutOfGasKeepsSenderNonce(t *testing.T) {
	const blockGasLimit = 1_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               []byte{0x00},
		IsContractCreation: true,
		IsEIP2:             true,
		IsEIP2028:          true,
		IsEIP3860:          true,
		IsEIP7623:          true,
		IsEIP7976:          true,
		IsEIP7981:          true,
		IsEIP2780:          true,
	})
	require.False(t, overflow)
	gasLimit := intrinsic.ExecutionGas + params.StateGasNewAccount - 1

	ibs := state.New(state.NewNoopReader())
	evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), gasLimit,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		[]byte{0x00}, nil, false, false, true, false, nil,
	)

	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrRuntimeOutOfGas)
	require.Equal(t, gasLimit, result.ReceiptGasUsed)
	require.Equal(t, gasLimit, result.BlockExecutionGasUsed)
	require.Zero(t, result.BlockStateGasUsed)

	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)
	created := accounts.InternAddress(types.CreateAddress(sender.Value(), 0))
	exists, err := ibs.Exist(created)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestEIP2780ContractCreationNonceReadErrorIsExecutionFailure(t *testing.T) {
	t.Parallel()
	const blockGasLimit = 1_000_000
	const gasLimit = 100_000
	backendErr := errors.New("nonce read failed")
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	versionMap := state.NewVersionMap(nil)
	versionMap.WriteBalance(sender, state.Version{TxIndex: 0}, *uint256.NewInt(1), true)
	statedb := state.NewWithVersionMap(
		&accountErrorReader{StateReader: state.NewNoopReader(), err: backendErr},
		versionMap,
	)
	defer statedb.Close()
	statedb.SetTxContext(1, 1)
	evm := newTestEVM(statedb, chain.AllProtocolChanges, blockGasLimit)
	msg := types.NewMessage(
		sender,
		accounts.NilAddress,
		0,
		uint256.NewInt(0),
		gasLimit,
		uint256.NewInt(0),
		uint256.NewInt(0),
		uint256.NewInt(0),
		[]byte{byte(vm.STOP)},
		nil,
		false,
		false,
		true,
		false,
		nil,
	)
	executor := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0))
	executor.noFeeBurnAndTip = true
	result, err := executor.Execute(true, false)
	require.Nil(t, result)
	require.ErrorIs(t, err, backendErr)
	require.ErrorIs(t, err, ErrTxnExecutionFailed)
}

func TestEIP2780ContractCreationFrameStartsAfterRuntimeCharge(t *testing.T) {
	t.Parallel()
	const blockGasLimit = 1_000_000
	const frameGas = 1_000
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	initCode := []byte{byte(vm.STOP)}
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               initCode,
		IsContractCreation: true,
		IsEIP2:             true,
		IsEIP2028:          true,
		IsEIP3860:          true,
		IsEIP7623:          true,
		IsEIP7976:          true,
		IsEIP7981:          true,
		IsEIP2780:          true,
	})
	require.False(t, overflow)
	gasLimit := intrinsic.ExecutionGas + params.StateGasNewAccount + frameGas
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	var enteredGas uint64
	hooks := &tracing.Hooks{
		OnEnter: func(depth int, typ byte, _ accounts.Address, _ accounts.Address, _ bool, _ []byte, gas uint64, _ uint256.Int, _ []byte) {
			if depth == 0 && vm.OpCode(typ) == vm.CREATE {
				enteredGas = gas
			}
		},
	}
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
	}
	evm := vm.NewEVM(
		blockCtx,
		evmtypes.TxContext{},
		ibs,
		chain.AllProtocolChanges,
		vm.Config{
			NoBaseFee: true,
			Tracer:    hooks,
		},
	)
	msg := types.NewMessage(
		sender,
		accounts.NilAddress,
		0,
		uint256.NewInt(0),
		gasLimit,
		uint256.NewInt(0),
		uint256.NewInt(0),
		uint256.NewInt(0),
		initCode,
		nil,
		false,
		false,
		true,
		false,
		nil,
	)
	result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, uint64(frameGas), enteredGas)
}

func TestEIP2780ContractCreationOntoStorageOnlyAccountChargesBeforeCollision(t *testing.T) {
	t.Parallel()
	const blockGasLimit = 30_000_000
	sender := accounts.InternAddress(common.HexToAddress("0xcafe"))
	target := accounts.InternAddress(types.CreateAddress(sender.Value(), 0))
	initCode := []byte{byte(vm.STOP)}
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               initCode,
		IsContractCreation: true,
		IsEIP2:             true,
		IsEIP2028:          true,
		IsEIP3860:          true,
		IsEIP7623:          true,
		IsEIP7976:          true,
		IsEIP7981:          true,
		IsEIP2780:          true,
	})
	require.False(t, overflow)
	for _, tt := range []struct {
		name           string
		gasLimit       uint64
		wantErr        error
		wantReceiptGas uint64
	}{
		{
			name:           "insufficient gas",
			gasLimit:       intrinsic.ExecutionGas + params.StateGasNewAccount - 1,
			wantErr:        vm.ErrRuntimeOutOfGas,
			wantReceiptGas: intrinsic.ExecutionGas + params.StateGasNewAccount - 1,
		},
		{
			name:           "collision refills charge",
			gasLimit:       params.MaxTxnGasLimit + params.StateGasNewAccount,
			wantErr:        vm.ErrContractAddressCollision,
			wantReceiptGas: params.MaxTxnGasLimit,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			ibs.CreateAccount(sender, false)
			ibs.CreateAccount(target, true)
			require.NoError(t, ibs.SetState(target, accounts.ZeroKey, *uint256.NewInt(1)))
			empty, err := ibs.Empty(target)
			require.NoError(t, err)
			require.True(t, empty)
			hasStorage, err := ibs.HasStorage(target)
			require.NoError(t, err)
			require.True(t, hasStorage)
			evm := newTestEVM(ibs, chain.AllProtocolChanges, blockGasLimit)
			msg := types.NewMessage(
				sender,
				accounts.NilAddress,
				0,
				uint256.NewInt(0),
				tt.gasLimit,
				uint256.NewInt(0),
				uint256.NewInt(0),
				uint256.NewInt(0),
				initCode,
				nil,
				false,
				false,
				true,
				false,
				nil,
			)
			result, err := NewTxnExecutor(evm, msg, NewGasPool(blockGasLimit, 0)).Execute(true, false)
			require.NoError(t, err)
			require.ErrorIs(t, result.Err, tt.wantErr)
			require.Equal(t, tt.wantReceiptGas, result.ReceiptGasUsed)
			require.Zero(t, result.BlockStateGasUsed)
			nonce, err := ibs.GetNonce(sender)
			require.NoError(t, err)
			require.Equal(t, uint64(1), nonce)
		})
	}
}

func TestContractCreationDoesNotWarmZeroAddressDelegationTarget(t *testing.T) {
	t.Parallel()

	const blockGasLimit = uint64(1_000_000)
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	delegatedTo := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetCode(accounts.ZeroAddress, types.AddressToDelegation(delegatedTo), tracing.CodeChangeUnspecified))

	evm := newTestEVM(ibs, chain.TestChainOsakaConfig, blockGasLimit)
	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 100_000,
		uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
		[]byte{byte(vm.STOP)}, nil, false, false, true, false, nil,
	)
	result, err := NewTxnExecutor(evm, msg, new(GasPool).AddGas(blockGasLimit)).Execute(true, false)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.False(t, ibs.AddressInAccessList(delegatedTo))
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
		defer ibs.Close()
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

	t.Run("CheckBlockGasInclusion rejects execution contribution > execution pool", func(t *testing.T) {
		gp := new(GasPool).AddGas(blockGasLimit)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, blockGasLimit+1, 0, 0), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion accepts contribution <= reservoirs", func(t *testing.T) {
		gp := new(GasPool).AddGas(blockGasLimit)
		require.NoError(t, CheckBlockGasInclusion(gp, blockGasLimit, 0, 0))
		require.NoError(t, CheckBlockGasInclusion(gp, blockGasLimit-1, 0, 0))
	})

	t.Run("CheckBlockGasInclusion is a no-op for nil gp", func(t *testing.T) {
		require.NoError(t, CheckBlockGasInclusion(nil, blockGasLimit*1000, blockGasLimit*1000, blockGasLimit*1000))
	})

	t.Run("CheckBlockGasInclusion rejects state contribution > state pool", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, 50_000, 200_000, 0), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion rejects execution contribution > execution pool (Amsterdam shape)", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.ErrorIs(t, CheckBlockGasInclusion(gp, 200_000, 50_000, 0), ErrGasLimitReached)
	})

	t.Run("CheckBlockGasInclusion accepts when both contributions fit", func(t *testing.T) {
		gp := NewGasPool(100_000, 0)
		require.NoError(t, CheckBlockGasInclusion(gp, 50_000, 80_000, 0))
	})

	t.Run("CheckBlockGasInclusion rejects blob gas > blob pool", func(t *testing.T) {
		gp := NewGasPool(100_000, params.GasPerBlob) // budget for one blob
		require.ErrorIs(t, CheckBlockGasInclusion(gp, 50_000, 50_000, 2*params.GasPerBlob), ErrBlobGasLimitReached)
		require.NoError(t, CheckBlockGasInclusion(gp, 50_000, 50_000, params.GasPerBlob))
	})
}

// TestBlobGasPreservedOnReject verifies that blob gas is reserved only after
// preCheck succeeds, while a valid blob transaction still consumes the pool.
func TestBlobGasPreservedOnReject(t *testing.T) {
	t.Parallel()

	const (
		blockGasLimit = 30_000_000
		blockBlobGas  = 6 * params.GasPerBlob
		txBlobGas     = 2 * params.GasPerBlob // two blob hashes
	)

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	cfg := chain.TestChainOsakaConfig // Cancun active -> blob path

	newBlobMsg := func(gas uint64) *types.Message {
		m := types.NewMessage(
			sender, recipient, 0, uint256.NewInt(0), gas,
			uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
			nil, nil,
			false, false, true, false,
			uint256.NewInt(1), // maxFeePerBlobGas
		)
		m.SetBlobVersionedHashes(make([]common.Hash, 2))
		return m
	}

	t.Run("rejected tx preserves blob pool", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		evm := newTestEVM(ibs, cfg, blockGasLimit)
		gp := new(GasPool).AddGas(blockGasLimit).AddBlobGas(blockBlobGas)

		_, err := NewTxnExecutor(evm, newBlobMsg(100_000), gp).Execute(true, false)

		require.ErrorIs(t, err, ErrInsufficientFunds)
		require.Equal(t, uint64(blockBlobGas), gp.BlobGas(),
			"blob-gas pool must be unchanged after a rejected tx")
	})

	t.Run("valid blob tx consumes blob pool", func(t *testing.T) {
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		require.NoError(t, ibs.AddBalance(sender, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified))
		evm := newTestEVM(ibs, cfg, blockGasLimit)
		gp := new(GasPool).AddGas(blockGasLimit).AddBlobGas(blockBlobGas)

		_, err := NewTxnExecutor(evm, newBlobMsg(100_000), gp).Execute(true, false)

		require.NoError(t, err)
		require.Equal(t, uint64(blockBlobGas-txBlobGas), gp.BlobGas(),
			"a valid blob tx must consume its blob gas from the pool")
	})
}

func TestPreCheck_NilMaxFeePerBlobGas(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    blockGasLimit,
		BaseFee:     *uint256.NewInt(1),
		BlobBaseFee: *uint256.NewInt(1),
	}
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chain.TestChainOsakaConfig, vm.Config{})
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 100_000,
		uint256.NewInt(1), uint256.NewInt(1), uint256.NewInt(1),
		nil, nil,
		false, false, true, false, nil,
	)
	msg.SetBlobVersionedHashes([]common.Hash{{1}})
	gp := new(GasPool).AddGas(blockGasLimit).AddBlobGas(params.GasPerBlob)

	_, err := NewTxnExecutor(evm, nilBlobFeeCapMessage{msg}, gp).Execute(true, false)
	require.ErrorIs(t, err, ErrMaxFeePerBlobGas)
}

// TestType4Prereq_NoStateMutationOnReject verifies that SetCode prerequisites
// take precedence over affordability and intrinsic gas, leaving sender state
// unchanged on rejection.
func TestType4Prereq_NoStateMutationOnReject(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	cfg := chain.TestChainBerlinConfig // pre-Prague: type-4 not allowed

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	initialBalance := uint256.NewInt(1)
	require.NoError(t, ibs.AddBalance(sender, *initialBalance, tracing.BalanceChangeUnspecified))

	evm := newTestEVM(ibs, cfg, 30_000_000)
	gasPrice := uint256.NewInt(1_000_000_000)
	msg := types.NewMessage(
		sender, recipient, 0, uint256.NewInt(0), 1_000,
		gasPrice, gasPrice, gasPrice,
		nil, nil,
		false, false, true, false, nil,
	)
	msg.SetAuthorizations([]types.Authorization{{}})

	gp := new(GasPool).AddGas(30_000_000)
	_, err := NewTxnExecutor(evm, msg, gp).Execute(true, false)
	require.EqualError(t, err, "SetCode transaction not allowed before Prague fork")

	nonce, nErr := ibs.GetNonce(sender)
	require.NoError(t, nErr)
	require.Zero(t, nonce, "nonce must be untouched on a type-4 prerequisite rejection")

	bal, bErr := ibs.GetBalance(sender)
	require.NoError(t, bErr)
	require.Equal(t, *initialBalance, bal, "balance must be untouched on a type-4 prerequisite rejection")
}

// TestMaxInitCodeSizeReject_NoStateMutation verifies that EIP-3860 initcode-size
// rejection occurs before fees are debited or the sender nonce is incremented.
func TestMaxInitCodeSizeReject_NoStateMutation(t *testing.T) {
	t.Parallel()

	const blockGasLimit = 30_000_000

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	cfg := chain.TestChainOsakaConfig // Shanghai active -> EIP-3860 initcode limit

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	initialBalance := uint256.NewInt(1_000_000_000_000_000_000)
	require.NoError(t, ibs.AddBalance(sender, *initialBalance, tracing.BalanceChangeUnspecified))

	evm := newTestEVM(ibs, cfg, blockGasLimit)
	gasPrice := uint256.NewInt(1)
	// One byte over the EIP-3860 limit, with ample gas to clear intrinsic gas.
	initcode := make([]byte, params.MaxInitCodeSize+1)
	msg := types.NewMessage(
		sender, accounts.NilAddress, 0, uint256.NewInt(0), 1_000_000,
		gasPrice, gasPrice, gasPrice,
		initcode, nil,
		false, false, true, false, nil,
	)
	gp := new(GasPool).AddGas(blockGasLimit)

	_, err := NewTxnExecutor(evm, msg, gp).Execute(true, false)
	require.ErrorIs(t, err, vm.ErrMaxInitCodeSizeExceeded)

	nonce, nErr := ibs.GetNonce(sender)
	require.NoError(t, nErr)
	require.Zero(t, nonce, "nonce must be untouched on an oversized-initcode rejection")

	bal, bErr := ibs.GetBalance(sender)
	require.NoError(t, bErr)
	require.Equal(t, *initialBalance, bal, "balance must be untouched on an oversized-initcode rejection")
}

func TestExecute_NilMaxFeePerBlobGas(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	for _, tc := range []struct {
		name     string
		withBlob bool
	}{
		{name: "without blob"},
		{name: "with blob", withBlob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := newSimpleTransferMsg(sender, recipient, 100_000, false)
			blobGas := uint64(0)
			if tc.withBlob {
				msg.SetBlobVersionedHashes([]common.Hash{{0x01}})
				blobGas = params.GasPerBlob
			}

			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			evm := newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000)
			gp := new(GasPool).AddGas(30_000_000).AddBlobGas(blobGas)

			var err error
			require.NotPanics(t, func() {
				_, err = NewTxnExecutor(evm, nilBlobFeeCapMessage{msg}, gp).Execute(true, false)
			})
			require.NoError(t, err)
		})
	}
}

// TestPreCheckNonceMismatchError pins the message text and the errors.Is
// identity that block assembly and eth_simulate match on. The sender address
// carries letters so the pinned text also covers EIP-55 casing.
func TestPreCheckNonceMismatchError(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	preCheckWithNonce := func(t *testing.T, stateNonce, msgNonce uint64) error {
		t.Helper()
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		require.NoError(t, ibs.SetNonce(sender, stateNonce, tracing.NonceChangeGenesis))

		evm := newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000)
		msg := types.NewMessage(
			sender, recipient, msgNonce, uint256.NewInt(0), 100_000,
			uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
			nil, nil,
			true,  // checkNonce
			false, // checkTransaction
			false, // checkGas
			false, // isFree
			nil,   // maxFeePerBlobGas
		)
		st := NewTxnExecutor(evm, msg, new(GasPool).AddGas(30_000_000))
		_, err := st.preCheck(false)
		return err
	}

	t.Run("nonce too high", func(t *testing.T) {
		err := preCheckWithNonce(t, 3, 7)
		require.ErrorIs(t, err, ErrNonceTooHigh)
		require.NotErrorIs(t, err, ErrNonceTooLow)
		require.Equal(t,
			"nonce too high: address 0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF, tx: 7 state: 3",
			err.Error())
	})

	t.Run("nonce too low", func(t *testing.T) {
		err := preCheckWithNonce(t, 7, 3)
		require.ErrorIs(t, err, ErrNonceTooLow)
		require.NotErrorIs(t, err, ErrNonceTooHigh)
		require.Equal(t,
			"nonce too low: address 0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF, tx: 3 state: 7",
			err.Error())
	})

	t.Run("matching nonce passes", func(t *testing.T) {
		require.NoError(t, preCheckWithNonce(t, 5, 5))
	})
}

// accessListCountingMsg counts AccessList reads, which both the intrinsic-gas
// calculation and the access-list clone perform.
type accessListCountingMsg struct {
	*types.Message
	reads *int
}

func (m accessListCountingMsg) AccessList() types.AccessList {
	*m.reads++
	return m.Message.AccessList()
}

// TestPreCheckDefersIntrinsicGasUntilNeeded pins that a transaction rejected by
// a state check never pays for the intrinsic-gas calculation. Under parallel
// execution a stale-nonce rejection is a routine re-execution signal, so work
// done ahead of it is repeated for every speculative attempt.
func TestPreCheckDefersIntrinsicGasUntilNeeded(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	run := func(t *testing.T, stateNonce, msgNonce uint64) (int, error) {
		t.Helper()
		ibs := state.New(state.NewNoopReader())
		defer ibs.Close()
		require.NoError(t, ibs.SetNonce(sender, stateNonce, tracing.NonceChangeGenesis))

		evm := newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000)
		inner := types.NewMessage(
			sender, recipient, msgNonce, uint256.NewInt(0), 100_000,
			uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
			nil, types.AccessList{{Address: recipient.Value()}},
			true,  // checkNonce
			false, // checkTransaction
			true,  // checkGas
			false, // isFree
			nil,   // maxFeePerBlobGas
		)
		reads := 0
		msg := accessListCountingMsg{Message: inner, reads: &reads}
		st := NewTxnExecutor(evm, msg, new(GasPool).AddGas(30_000_000))
		_, err := st.Execute(true, false)
		return reads, err
	}

	t.Run("stale nonce rejects before intrinsic gas is computed", func(t *testing.T) {
		reads, err := run(t, 3, 7)
		require.ErrorIs(t, err, ErrNonceTooHigh)
		require.Zero(t, reads, "rejected tx must not read the access list")
	})

	t.Run("accepted tx still computes intrinsic gas", func(t *testing.T) {
		reads, err := run(t, 5, 5)
		require.NoError(t, err)
		require.NotZero(t, reads, "accepted tx must compute intrinsic gas")
	})
}

// BenchmarkExecuteStaleNonceReject measures a transaction rejected on a stale
// nonce, the routine outcome of a speculative parallel attempt. Access-list
// size drives both the intrinsic-gas calculation and the tuple copy.
func BenchmarkExecuteStaleNonceReject(b *testing.B) {
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))

	for _, alLen := range []int{0, 8, 64} {
		b.Run(fmt.Sprintf("accesslist=%d", alLen), func(b *testing.B) {
			al := make(types.AccessList, alLen)
			for i := range al {
				al[i] = types.AccessTuple{
					Address:     common.BigToAddress(big.NewInt(int64(i + 1))),
					StorageKeys: []common.Hash{{byte(i)}, {byte(i), 0x01}},
				}
			}
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(b, ibs.SetNonce(sender, 3, tracing.NonceChangeGenesis))

			msg := types.NewMessage(
				sender, recipient, 7, uint256.NewInt(0), 100_000,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
				nil, al,
				true, false, true, false, nil,
			)

			b.ReportAllocs()
			for b.Loop() {
				evm := newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000)
				st := NewTxnExecutor(evm, msg, new(GasPool).AddGas(30_000_000))
				if _, err := st.Execute(true, false); !errors.Is(err, ErrNonceTooHigh) {
					b.Fatalf("want ErrNonceTooHigh, got %v", err)
				}
			}
		})
	}
}

// TestPreCheckIntrinsicGasMatchesMessage pins that the intrinsic gas preCheck
// leaves behind equals what the message's own fields imply, across the shapes
// that feed the calculation.
func TestPreCheckIntrinsicGasMatchesMessage(t *testing.T) {
	t.Parallel()

	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	auth, _ := eip2780TestAuthorization()

	al := types.AccessList{
		{Address: common.HexToAddress("0xaa"), StorageKeys: []common.Hash{{0x1}, {0x2}, {0x3}}},
		{Address: common.HexToAddress("0xbb"), StorageKeys: []common.Hash{{0x4}}},
	}

	cases := []struct {
		name   string
		to     accounts.Address
		data   []byte
		al     types.AccessList
		auths  []types.Authorization
		amount *uint256.Int
	}{
		{"plain transfer", recipient, nil, nil, nil, uint256.NewInt(0)},
		{"with value", recipient, nil, nil, nil, uint256.NewInt(7)},
		{"with data", recipient, []byte{0, 1, 0, 2, 3}, nil, nil, uint256.NewInt(0)},
		{"with access list", recipient, nil, al, nil, uint256.NewInt(0)},
		{"with authorizations", recipient, nil, al, []types.Authorization{auth}, uint256.NewInt(0)},
		{"contract creation", accounts.NilAddress, []byte{0x60, 0x01}, al, nil, uint256.NewInt(0)},
		{"self transfer", sender, nil, al, nil, uint256.NewInt(1)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			ibs.SetBalance(sender, *uint256.NewInt(1_000_000), tracing.BalanceChangeUnspecified)
			evm := newTestEVM(ibs, chain.TestChainOsakaConfig, 30_000_000)
			msg := types.NewMessage(
				sender, tc.to, 0, tc.amount, 10_000_000,
				uint256.NewInt(0), uint256.NewInt(0), uint256.NewInt(0),
				tc.data, tc.al,
				false, false, true, false, nil,
			)
			msg.SetAuthorizations(tc.auths)

			st := NewTxnExecutor(evm, msg, new(GasPool).AddGas(30_000_000))
			fees, err := st.preCheck(false)
			require.NoError(t, err)

			// Derive the arguments the way Execute used to, from a cloned access
			// list, and compare against what preCheck stored.
			accessTuples := slices.Clone[types.AccessList](msg.AccessList())
			contractCreation := msg.To().IsNil()
			rules := evm.ChainRules()
			vmConfig := evm.Config()
			want, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
				Data:               tc.data,
				AuthorizationsLen:  uint64(len(msg.Authorizations())),
				AccessListLen:      uint64(len(accessTuples)),
				StorageKeysLen:     uint64(accessTuples.StorageKeys()),
				IsContractCreation: contractCreation,
				IsSelfTransfer:     !contractCreation && msg.To() == msg.From(),
				HasValue:           !msg.Value().IsZero(),
				IsEIP2:             rules.IsHomestead,
				IsEIP2028:          rules.IsIstanbul,
				IsEIP3860:          vmConfig.HasEip3860(rules),
				IsEIP7623:          rules.IsPrague,
				IsEIP7976:          rules.IsAmsterdam,
				IsEIP7981:          rules.IsAmsterdam,
				IsEIP2780:          rules.IsAmsterdam,
			})
			require.False(t, overflow)
			require.Equal(t, want, fees.intrinsicGas)
			require.NotZero(t, fees.intrinsicGas.ExecutionGas)
		})
	}
}
