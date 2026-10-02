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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/params"
)

// TestStackBoundsCheckEquivalence proves the interpreter's single unsigned
// range check fires exactly when the two-comparison form would, and that
// stackBoundsErr reproduces the same error type and fields, over the whole
// reachable domain (and beyond: negative lengths, in case of stack
// corruption).
func TestStackBoundsCheckEquivalence(t *testing.T) {
	t.Parallel()
	for numPop := 0; numPop <= 20; numPop++ {
		for numPush := 0; numPush <= 20; numPush++ {
			op := &operation{numPop: numPop, maxStack: maxStack(numPop, numPush)}
			for sLen := -2; sLen <= 1200; sLen++ {
				var want error
				if sLen < op.numPop {
					want = &ErrStackUnderflow{stackLen: sLen, required: op.numPop}
				} else if sLen > op.maxStack {
					want = &ErrStackOverflow{stackLen: sLen, limit: op.maxStack}
				}
				fired := uint(sLen-op.numPop) > uint(op.maxStack-op.numPop)
				require.Equal(t, want != nil, fired,
					"numPop=%d maxStack=%d sLen=%d", op.numPop, op.maxStack, sLen)
				if fired {
					require.Equal(t, want, stackBoundsErr(sLen, op))
				}
			}
		}
	}
}

// TestStackBoundsInvariant pins the table invariant the range check relies on:
// every entry of every fork table (including EnableEIP-patched copies, which
// go through validateAndFillMaxStack) satisfies 0 <= numPop <= maxStack.
func TestStackBoundsInvariant(t *testing.T) {
	t.Parallel()
	tables := map[string]*JumpTable{
		"frontier":         &frontierInstructionSet,
		"homestead":        &homesteadInstructionSet,
		"tangerineWhistle": &tangerineWhistleInstructionSet,
		"spuriousDragon":   &spuriousDragonInstructionSet,
		"byzantium":        &byzantiumInstructionSet,
		"constantinople":   &constantinopleInstructionSet,
		"istanbul":         &istanbulInstructionSet,
		"berlin":           &berlinInstructionSet,
		"london":           &londonInstructionSet,
		"shanghai":         &shanghaiInstructionSet,
		"cancun":           &cancunInstructionSet,
		"prague":           &pragueInstructionSet,
		"osaka":            &osakaInstructionSet,
		"amsterdam":        &amsterdamInstructionSet,
	}
	for name, jt := range tables {
		for i, op := range jt {
			require.NotNilf(t, op, "%s[0x%02X] nil entry", name, i)
			require.GreaterOrEqualf(t, op.numPop, 0, "%s[0x%02X]", name, i)
			require.LessOrEqualf(t, op.numPop, op.maxStack, "%s[0x%02X]", name, i)
		}
	}
}

// TestFastPathMatchesJumpTables pins the constants hard-coded in the
// interpreter's fast path to every jump table it can run with.
func TestFastPathMatchesJumpTables(t *testing.T) {
	t.Parallel()
	// DUPs are makeDup closures, whose code pointers differ by inlining site,
	// so they are checked by behaviour instead of by function identity.
	type want struct {
		execute         executionFunc
		gas             uint64
		numPop, numPush int
	}
	fast := map[OpCode]want{
		PUSH1:    {opPush1, GasFastestStep, 0, 1},
		PUSH2:    {opPush2, GasFastestStep, 0, 1},
		DUP1:     {nil, GasFastestStep, 1, 2},
		DUP2:     {nil, GasFastestStep, 2, 3},
		DUP3:     {nil, GasFastestStep, 3, 4},
		SWAP1:    {opSwap1, GasFastestStep, 2, 2},
		SWAP2:    {opSwap2, GasFastestStep, 3, 3},
		ADD:      {opAdd, GasFastestStep, 2, 1},
		POP:      {opPop, GasQuickStep, 1, 0},
		JUMPDEST: {opJumpdest, params.JumpdestGas, 0, 0},
		JUMP:     {opJump, GasMidStep, 1, 0},
		JUMPI:    {opJumpi, GasSlowStep, 2, 0},
	}
	tables := []*JumpTable{
		&frontierInstructionSet, &homesteadInstructionSet, &tangerineWhistleInstructionSet,
		&spuriousDragonInstructionSet, &byzantiumInstructionSet, &constantinopleInstructionSet,
		&istanbulInstructionSet, &berlinInstructionSet, &londonInstructionSet,
		&shanghaiInstructionSet, &cancunInstructionSet, &pragueInstructionSet,
		&osakaInstructionSet, &amsterdamInstructionSet,
	}
	for _, jt := range tables {
		for eip := range activators {
			cp := copyJumpTable(jt)
			require.NoError(t, EnableEIP(eip, cp))
			tables = append(tables, cp)
		}
	}
	for i, jt := range tables {
		push0 := &jt[PUSH0]
		if push0.numPush == 1 {
			require.Equal(t, reflect.ValueOf(opPush0).Pointer(), reflect.ValueOf(push0.execute).Pointer(), "table %d PUSH0 execute", i)
			require.Equal(t, GasQuickStep, push0.constantGas, "table %d PUSH0 gas", i)
			require.Zero(t, push0.numPop, "table %d PUSH0 numPop", i)
			require.Nil(t, push0.dynamicGas, "table %d PUSH0 dynamicGas", i)
		} else {
			require.Equal(t, reflect.ValueOf(opUndefined).Pointer(), reflect.ValueOf(push0.execute).Pointer(), "table %d PUSH0 execute", i)
		}
		for op, w := range fast {
			got := &jt[op]
			if w.execute != nil {
				require.Equal(t, reflect.ValueOf(w.execute).Pointer(), reflect.ValueOf(got.execute).Pointer(), "table %d %s execute", i, op)
			} else {
				scope := new(CallContext)
				for v := range uint64(4) {
					scope.Stack.pushRef().SetUint64(v)
				}
				_, _, err := got.execute(0, nil, scope)
				require.NoError(t, err)
				require.Equal(t, 5, scope.Stack.len(), "table %d %s stack", i, op)
				require.Equal(t, uint64(4-w.numPop), scope.Stack.peek().Uint64(), "table %d %s top", i, op)
			}
			require.Equal(t, w.gas, got.constantGas, "table %d %s gas", i, op)
			require.Equal(t, w.numPop, got.numPop, "table %d %s numPop", i, op)
			require.Equal(t, w.numPush, got.numPush, "table %d %s numPush", i, op)
			require.Nil(t, got.dynamicGas, "table %d %s dynamicGas", i, op)
			require.Nil(t, got.memorySize, "table %d %s memorySize", i, op)
		}
	}
}
