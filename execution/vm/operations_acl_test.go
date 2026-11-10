package vm

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestGasSStoreDoesNotRefill(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetState(accounts.ZeroAddress, accounts.ZeroKey, *uint256.NewInt(1)))
	ibs.AddSlotToAccessList(accounts.ZeroAddress, accounts.ZeroKey)
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	scope := evm.getCallContext(*NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{}), nil, mdgas.MdGas{Execution: 200_000})
	defer evm.putCallContext(scope)
	scope.cacheGen++
	require.True(t, scope.useMdGas(params.StateGasPerStorageSet, mdgas.StateGas, nil, tracing.GasChangeIgnored))
	scope.Stack.push(uint256.Int{})
	scope.Stack.push(uint256.Int{})
	old := scope.Gas()
	oldSpill := scope.stateGasSpill

	cost, err := gasSStoreEIP3529(evm, scope, old, 0)
	require.NoError(t, err)
	require.Equal(t, old, scope.Gas())
	require.Equal(t, oldSpill, scope.stateGasSpill)
	require.EqualValues(t, params.WarmStorageReadCostEIP2929, cost.Execution)
	require.EqualValues(t, -int64(params.StateGasPerStorageSet), cost.State)
}

func TestGasCallDoesNotCharge(t *testing.T) {
	for _, op := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL} {
		t.Run(op.String(), func(t *testing.T) {
			address := accounts.InternAddress([20]byte{18: 0x10})
			readErr := errors.New("account read failed")
			var reader state.StateReader = state.NewNoopReader()
			if op != CALL {
				reader = beneficiaryErrReader{StateReader: reader, fail: address, err: readErr}
			}
			ibs := state.New(reader)
			defer ibs.Close()
			evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
			initial := mdgas.MdGas{Execution: 500_000, State: params.StateGasNewAccount / 2}
			scope := evm.getCallContext(*NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{}), nil, initial)
			defer evm.putCallContext(scope)
			scope.cacheGen++
			for range 4 {
				scope.Stack.push(uint256.Int{})
			}
			if op == CALL || op == CALLCODE {
				scope.Stack.push(*uint256.NewInt(1))
			}
			scope.Stack.push(*uint256.NewInt(0x1000))
			scope.Stack.push(*uint256.NewInt(100_000))

			cost, err := evm.jt[op].dynamicGas(evm, scope, initial, 0)
			if op == CALL {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, readErr)
			}
			require.Equal(t, initial, scope.Gas())
			require.Zero(t, scope.stateGasSpill)
			if op == CALL {
				require.EqualValues(t, params.StateGasNewAccount, cost.State)
				require.EqualValues(t, 100_000, evm.CallGasTemp())
				require.EqualValues(t, 100_000+params.ColdAccountAccessCostEIP8038-params.WarmStorageReadCostEIP2929+params.CallValueTransferGasEIP8038, cost.Execution)
			}
		})
	}
}

func TestFrameCallDoesNotSpillStateGas(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	tx := &types.FrameTransaction{NonceKeys: []uint256.Int{{}}, Frames: []types.Frame{{Limits: mdgas.MdGas{Execution: 500_000, State: params.StateGasNewAccount - 1}}}}
	evm.FrameContext = ibs.NewFrameContext(tx, uint256.Int{})
	scope := evm.getCallContext(*NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{}), nil, tx.Frames[0].Limits)
	defer evm.putCallContext(scope)
	scope.cacheGen++
	for range 4 {
		scope.Stack.push(uint256.Int{})
	}
	scope.Stack.push(*uint256.NewInt(1))
	scope.Stack.push(*uint256.NewInt(0x1000))
	scope.Stack.push(*uint256.NewInt(500_000))
	_, err := evm.jt[CALL].dynamicGas(evm, scope, tx.Frames[0].Limits, 0)
	require.ErrorIs(t, err, ErrOutOfGas)
	require.Zero(t, evm.FrameContext.Receipts[0].GasUsed.State)
}

func callFamilyScope(t *testing.T, op OpCode) (*EVM, *state.IntraBlockState, *CallContext, accounts.Address, accounts.Address) {
	t.Helper()
	onStack := accounts.InternAddress([20]byte{18: 0x10})
	memoized := accounts.InternAddress([20]byte{18: 0x20})
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	code := []byte{byte(PUSH1), 1, byte(PUSH0), byte(MSTORE8), byte(PUSH1), 1, byte(PUSH0), byte(RETURN)}
	require.NoError(t, ibs.CreateAccount(memoized, true))
	require.NoError(t, ibs.SetCode(memoized, code, tracing.CodeChangeUnspecified))
	evm := NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	scope := evm.getCallContext(*NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{}), nil, mdgas.MdGas{Execution: 500_000})
	t.Cleanup(func() { evm.putCallContext(scope) })
	scope.cacheGen++
	for range 4 {
		scope.Stack.push(uint256.Int{})
	}
	if op == CALL || op == CALLCODE {
		scope.Stack.push(uint256.Int{})
	}
	word := onStack.Value()
	scope.Stack.push(*new(uint256.Int).SetBytes(word[:]))
	scope.Stack.push(*uint256.NewInt(100_000))
	return evm, ibs, scope, onStack, memoized
}

func TestCallFamilyGasAndExecuteShareTheInternedTarget(t *testing.T) {
	for _, op := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL} {
		t.Run(op.String(), func(t *testing.T) {
			evm, ibs, scope, onStack, memoized := callFamilyScope(t, op)
			scope.cachedAddr, scope.cachedAddrGen = memoized, scope.cacheGen

			_, err := evm.jt[op].dynamicGas(evm, scope, scope.Gas(), 0)
			require.NoError(t, err)
			require.True(t, ibs.AddressInAccessList(memoized))
			require.False(t, ibs.AddressInAccessList(onStack))

			_, _, err = evm.jt[op].execute(0, evm, scope)
			require.NoError(t, err)
			require.Equal(t, []byte{1}, evm.returnData)
		})
	}
}

func TestCallFamilyIgnoresTargetInternedByEarlierDispatch(t *testing.T) {
	for _, op := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL} {
		t.Run(op.String(), func(t *testing.T) {
			evm, ibs, scope, onStack, memoized := callFamilyScope(t, op)
			scope.cachedAddr, scope.cachedAddrGen = memoized, scope.cacheGen-1

			_, err := evm.jt[op].dynamicGas(evm, scope, scope.Gas(), 0)
			require.NoError(t, err)
			require.True(t, ibs.AddressInAccessList(onStack))
			require.False(t, ibs.AddressInAccessList(memoized))

			_, _, err = evm.jt[op].execute(0, evm, scope)
			require.NoError(t, err)
			require.Empty(t, evm.returnData)
		})
	}
}
