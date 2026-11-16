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
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var (
	ovCaller = accounts.InternAddress(common.HexToAddress("0x1000"))
	ovTarget = accounts.InternAddress(common.HexToAddress("0x2000"))
	ovMiddle = accounts.InternAddress(common.HexToAddress("0x3000"))
	ovInner  = accounts.InternAddress(common.HexToAddress("0x4000"))
)

func ovSlot(i byte) accounts.StorageKey { return accounts.InternKey(common.Hash{31: i}) }

// callTarget emits op(gas, 0x2000, [value], 0, 0, 0, 32) for CALL/CALLCODE (with value) or DELEGATECALL/STATICCALL.
func callTarget(op OpCode, target uint16, value byte) []byte {
	c := []byte{byte(PUSH1), 0x20, byte(PUSH0), byte(PUSH0), byte(PUSH0)}
	if op == CALL || op == CALLCODE {
		c = append(c, byte(PUSH1), value)
	}
	return append(c, byte(PUSH2), byte(target>>8), byte(target), byte(GAS), byte(op))
}

// targetCode writes slot 0 = 1 and logs, so any execution of the callee is visible.
var targetCode = []byte{byte(PUSH1), 1, byte(PUSH0), byte(SSTORE), byte(PUSH0), byte(PUSH0), byte(LOG0), byte(STOP)}

func ovState(t *testing.T, callerCode []byte) *state.IntraBlockState {
	t.Helper()
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	require.NoError(t, ibs.SetCode(ovCaller, callerCode, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(ovTarget, targetCode, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetBalance(ovCaller, *uint256.NewInt(10), tracing.BalanceChangeUnspecified))
	return ibs
}

func ovRun(t *testing.T, ibs *state.IntraBlockState, cfg Config, to accounts.Address) (mdgas.MdGas, error) {
	t.Helper()
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, cfg)
	_, left, _, err := evm.Call(accounts.ZeroAddress, to, nil, mdgas.MdGas{Execution: 1_000_000}, uint256.Int{}, false)
	return left, err
}

func ovGet(t *testing.T, ibs *state.IntraBlockState, a accounts.Address, i byte) uint64 {
	t.Helper()
	v, err := ibs.GetState(a, ovSlot(i))
	require.NoError(t, err)
	return v.Uint64()
}

func overrideTarget(ret []byte, err error) CallOverride {
	return func(_ *EVM, _ int, _, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if addr != ovTarget {
			return nil, false, nil
		}
		return ret, true, err
	}
}

func TestCallOverrideReplacesCall(t *testing.T) {
	t.Parallel()
	// CALL 0x2000 value 1; slot0 = success, slot1 = RETURNDATASIZE, slot2 = mem[0:32].
	code := append(callTarget(CALL, 0x2000, 1),
		byte(PUSH0), byte(SSTORE),
		byte(RETURNDATASIZE), byte(PUSH1), 1, byte(SSTORE),
		byte(PUSH0), byte(MLOAD), byte(PUSH1), 2, byte(SSTORE), byte(STOP))
	ibs := ovState(t, code)
	_, err := ovRun(t, ibs, Config{CallOverride: overrideTarget([]byte{0xbe, 0xef}, nil)}, ovCaller)
	require.NoError(t, err)

	require.EqualValues(t, 1, ovGet(t, ibs, ovCaller, 0), "CALL pushes 1")
	require.EqualValues(t, 2, ovGet(t, ibs, ovCaller, 1), "return data is the override's")
	v, err := ibs.GetState(ovCaller, ovSlot(2))
	require.NoError(t, err)
	require.Equal(t, uint64(0xbeef), new(uint256.Int).Rsh(&v, 240).Uint64(), "return data is copied to memory")
	require.EqualValues(t, 0, ovGet(t, ibs, ovTarget, 0), "callee code does not run")
	require.Empty(t, ibs.Logs(), "callee logs nothing")
	bal, err := ibs.GetBalance(ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 10, bal.Uint64(), "no value leaves the caller")
	bal, err = ibs.GetBalance(ovTarget)
	require.NoError(t, err)
	require.True(t, bal.IsZero(), "no value reaches the callee")
}

func TestCallOverrideReturnsAllGas(t *testing.T) {
	t.Parallel()
	code := append(callTarget(CALL, 0x2000, 1), byte(POP), byte(STOP))

	// Baseline: the same CALL to a non-empty account without code runs nothing
	// and returns its gas, stipend included.
	base := state.New(state.NewNoopReader())
	defer base.Close()
	require.NoError(t, base.SetCode(ovCaller, code, tracing.CodeChangeUnspecified))
	require.NoError(t, base.SetBalance(ovCaller, *uint256.NewInt(10), tracing.BalanceChangeUnspecified))
	require.NoError(t, base.SetBalance(ovTarget, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	want, err := ovRun(t, base, Config{}, ovCaller)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		err  error
	}{{"success", nil}, {"revert", ErrExecutionReverted}} {
		ibs := ovState(t, code)
		got, err := ovRun(t, ibs, Config{CallOverride: overrideTarget([]byte{1}, tc.err)}, ovCaller)
		require.NoError(t, err, tc.name)
		require.Equal(t, want, got, tc.name)
	}
}

func TestCallOverrideRevert(t *testing.T) {
	t.Parallel()
	code := append(callTarget(CALL, 0x2000, 0),
		byte(PUSH1), 1, byte(SSTORE), // slot1 = success
		byte(RETURNDATASIZE), byte(PUSH1), 2, byte(SSTORE), byte(STOP))
	ibs := ovState(t, code)
	_, err := ovRun(t, ibs, Config{CallOverride: overrideTarget([]byte{0xde, 0xad, 0x01}, ErrExecutionReverted)}, ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 0, ovGet(t, ibs, ovCaller, 1), "CALL pushes 0")
	require.EqualValues(t, 3, ovGet(t, ibs, ovCaller, 2), "revert data is the return data")
}

func TestCallOverrideOnlyMutableCall(t *testing.T) {
	t.Parallel()
	var code []byte
	for _, op := range []OpCode{DELEGATECALL, CALLCODE, STATICCALL} {
		code = append(code, callTarget(op, 0x2000, 0)...)
		code = append(code, byte(POP))
	}
	// STATICCALL 0x3000, which zero-value CALLs 0x2000 from a static frame.
	code = append(code, callTarget(STATICCALL, 0x3000, 0)...)
	code = append(code, byte(POP), byte(STOP))
	ibs := ovState(t, code)
	require.NoError(t, ibs.SetCode(ovMiddle, append(callTarget(CALL, 0x2000, 0), byte(POP), byte(STOP)), tracing.CodeChangeUnspecified))

	var hooked, entered int
	hook := func(_ *EVM, _ int, _, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if addr == ovTarget {
			hooked++
		}
		return nil, false, nil
	}
	tracer := &tracing.Hooks{OnEnter: func(_ int, _ byte, _, to accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
		if to == ovTarget {
			entered++
		}
	}}
	_, err := ovRun(t, ibs, Config{CallOverride: hook, Tracer: tracer}, ovCaller)
	require.NoError(t, err)
	require.Equal(t, 4, entered, "every frame to the target is entered")
	require.Zero(t, hooked, "only a CALL outside a static frame is offered")
}

func TestCallOverrideDepth(t *testing.T) {
	t.Parallel()
	ibs := ovState(t, append(callTarget(CALL, 0x2000, 0), byte(POP), byte(STOP)))
	var depths []int
	hook := func(_ *EVM, depth int, _, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if addr == ovTarget {
			depths = append(depths, depth)
		}
		return nil, false, nil
	}
	cfg := Config{CallOverride: hook}
	_, err := ovRun(t, ibs, cfg, ovTarget)
	require.NoError(t, err)
	_, err = ovRun(t, ibs, cfg, ovCaller)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, depths, "the tx frame is depth 0")
	require.EqualValues(t, 1, ovGet(t, ibs, ovTarget, 0), "ok=false runs the callee")

	// ok=false costs the same as no hook.
	a := ovState(t, append(callTarget(CALL, 0x2000, 0), byte(POP), byte(STOP)))
	b := ovState(t, append(callTarget(CALL, 0x2000, 0), byte(POP), byte(STOP)))
	ga, err := ovRun(t, a, Config{}, ovCaller)
	require.NoError(t, err)
	gb, err := ovRun(t, b, cfg, ovCaller)
	require.NoError(t, err)
	require.Equal(t, ga, gb)
}

func TestCallOverrideStateVisibleToCaller(t *testing.T) {
	t.Parallel()
	var calls int
	hook := func(evm *EVM, _ int, caller, addr accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		if addr != ovTarget {
			return nil, false, nil
		}
		calls++
		if calls%2 == 1 {
			return nil, true, nil
		}
		return nil, true, evm.IntraBlockState().SetState(caller, ovSlot(0), *uint256.NewInt(7))
	}
	// CALL (warms the target), SLOAD slot0 (caches it), CALL (hook writes), SLOAD slot0 → slot1.
	body := append(callTarget(CALL, 0x2000, 0), byte(POP), byte(PUSH0), byte(SLOAD), byte(POP))
	body = append(body, callTarget(CALL, 0x2000, 0)...)
	body = append(body, byte(POP), byte(PUSH0), byte(SLOAD), byte(PUSH1), 1, byte(SSTORE))

	ibs := ovState(t, append(body, byte(STOP)))
	_, err := ovRun(t, ibs, Config{CallOverride: hook}, ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 7, ovGet(t, ibs, ovCaller, 1), "caller reads the hook's write after the CALL")

	ibs = ovState(t, append(body, byte(PUSH0), byte(PUSH0), byte(REVERT)))
	_, err = ovRun(t, ibs, Config{CallOverride: hook}, ovCaller)
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.EqualValues(t, 0, ovGet(t, ibs, ovCaller, 0), "an enclosing revert unwinds the hook's write")
}

func TestCallOverrideTracerPaired(t *testing.T) {
	t.Parallel()
	ibs := ovState(t, append(callTarget(CALL, 0x2000, 0), byte(POP), byte(STOP)))
	type exit struct {
		depth    int
		out      []byte
		err      error
		reverted bool
	}
	var enters []int
	var exits []exit
	tracer := &tracing.Hooks{
		OnEnter: func(depth int, _ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
			enters = append(enters, depth)
		},
		OnExit: func(depth int, out []byte, _ uint64, err error, reverted bool) {
			exits = append(exits, exit{depth, out, err, reverted})
		},
	}
	_, err := ovRun(t, ibs, Config{Tracer: tracer, CallOverride: overrideTarget([]byte{9}, ErrExecutionReverted)}, ovCaller)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, enters)
	require.Len(t, exits, 2)
	require.Equal(t, 1, exits[0].depth)
	require.Equal(t, []byte{9}, exits[0].out)
	require.True(t, exits[0].reverted)
	require.ErrorIs(t, exits[0].err, ErrExecutionReverted)
}

func TestCallOverridePassThroughLeaksNothing(t *testing.T) {
	t.Parallel()
	// CALL a non-empty code-less account; slot1 = success, slot2 = RETURNDATASIZE.
	code := append(callTarget(CALL, 0x2000, 0),
		byte(PUSH1), 1, byte(SSTORE),
		byte(RETURNDATASIZE), byte(PUSH1), 2, byte(SSTORE), byte(STOP))
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetCode(ovCaller, code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetBalance(ovTarget, *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	hook := func(_ *EVM, _ int, _, _ accounts.Address, _ []byte, _ uint256.Int) ([]byte, bool, error) {
		return []byte{1, 2, 3}, false, errors.New("ignored")
	}
	_, err := ovRun(t, ibs, Config{CallOverride: hook}, ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 1, ovGet(t, ibs, ovCaller, 1), "ok=false ignores the hook's err")
	require.EqualValues(t, 0, ovGet(t, ibs, ovCaller, 2), "ok=false ignores the hook's ret")
}

func TestCallOverrideTopFrame(t *testing.T) {
	t.Parallel()
	ibs := ovState(t, nil)
	evm := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{CallOverride: overrideTarget([]byte{5}, nil)})
	in := mdgas.MdGas{Execution: 100_000}
	ret, left, used, err := evm.Call(ovCaller, ovTarget, nil, in, *uint256.NewInt(3), false)
	require.NoError(t, err)
	require.Equal(t, []byte{5}, ret)
	require.Equal(t, in, left, "the tx frame keeps all its gas")
	require.Zero(t, used.Execution)
	require.EqualValues(t, 0, ovGet(t, ibs, ovTarget, 0), "callee code does not run")
	bal, err := ibs.GetBalance(ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 10, bal.Uint64(), "tx value is not transferred")
}

func TestCallOverrideOtherErrorKeepsGas(t *testing.T) {
	t.Parallel()
	code := append(callTarget(CALL, 0x2000, 0), byte(PUSH1), 1, byte(SSTORE),
		byte(RETURNDATASIZE), byte(PUSH1), 2, byte(SSTORE), byte(STOP))
	ibs := ovState(t, code)
	ibs2 := ovState(t, code)
	fail := errors.New("dispatch failed")
	_, a, _, err := NewEVM(gasTraceBlockContext(), evmtypes.TxContext{}, ibs, chain.AllProtocolChanges,
		Config{CallOverride: overrideTarget([]byte{1}, fail)}).Call(accounts.ZeroAddress, ovTarget, nil, mdgas.MdGas{Execution: 50_000}, uint256.Int{}, false)
	require.ErrorIs(t, err, fail, "a non-revert error reaches the caller of Call unchanged")
	require.Equal(t, mdgas.MdGas{Execution: 50_000}, a, "and does not zero gas")
	_, err = ovRun(t, ibs2, Config{CallOverride: overrideTarget([]byte{1}, fail)}, ovCaller)
	require.NoError(t, err)
	require.EqualValues(t, 0, ovGet(t, ibs2, ovCaller, 1), "CALL pushes 0")
	require.EqualValues(t, 0, ovGet(t, ibs2, ovCaller, 2), "no return data on a non-revert error")
}
