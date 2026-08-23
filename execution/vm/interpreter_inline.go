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
	"fmt"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol/params"
)

// runGoInline executes a burst of consecutive hot opcodes with constant-folded
// prologues. It advances pc and returns at the first opcode it does not handle
// (halt=false, err=nil, pc left at that opcode) for the caller to run via the
// generic jump-table path; on STOP or running off the end it returns halt=true;
// on a fault it returns the typed error.
func (evm *EVM) runGoInline(cc *CallContext, contract *Contract, pc uint64) (uint64, bool, error) {
	codeLen := uint64(len(contract.Code))
	stack := &cc.Stack
	for {
		op := contract.GetOp(pc)
		switch {
		case op == STOP:
			return pc, true, nil

		case op == JUMPDEST:
			if cc.gas < params.JumpdestGas {
				return pc, false, ErrOutOfGas
			}
			cc.gas -= params.JumpdestGas
			pc++

		case op == POP:
			sLen := stack.len()
			if sLen < 1 {
				return pc, false, &ErrStackUnderflow{stackLen: sLen, required: 1}
			}
			if cc.gas < GasQuickStep {
				return pc, false, ErrOutOfGas
			}
			cc.gas -= GasQuickStep
			stack.pop()
			pc++

		case op == PUSH1:
			sLen := stack.len()
			if sLen > 1023 {
				return pc, false, &ErrStackOverflow{stackLen: sLen, limit: 1023}
			}
			if cc.gas < GasFastestStep {
				return pc, false, ErrOutOfGas
			}
			cc.gas -= GasFastestStep
			if pc+1 < codeLen {
				stack.push(uint256.Int{uint64(contract.Code[pc+1])})
			} else {
				stack.push(uint256.Int{})
			}
			pc += 2

		case op >= DUP1 && op <= DUP16:
			n := int(op-DUP1) + 1
			sLen := stack.len()
			if sLen < n {
				return pc, false, &ErrStackUnderflow{stackLen: sLen, required: n}
			}
			if sLen > 1023 {
				return pc, false, &ErrStackOverflow{stackLen: sLen, limit: 1023}
			}
			if cc.gas < GasFastestStep {
				return pc, false, ErrOutOfGas
			}
			cc.gas -= GasFastestStep
			stack.dup(n - 1)
			pc++

		case op >= SWAP1 && op <= SWAP16:
			n := int(op-SWAP1) + 1
			sLen := stack.len()
			if sLen < n+1 {
				return pc, false, &ErrStackUnderflow{stackLen: sLen, required: n + 1}
			}
			if cc.gas < GasFastestStep {
				return pc, false, ErrOutOfGas
			}
			cc.gas -= GasFastestStep
			stack.swap(n)
			pc++

		default:
			return pc, false, nil // uncovered — caller runs it generically
		}
	}
}

var inlineConstsOnce sync.Once

// assertInlineConsts verifies the folded gas/stack constants in runGoInline
// match the jump table, so a fork that changed one of these costs trips a panic
// at construction rather than silently diverging consensus.
func assertInlineConsts(jt *JumpTable) {
	check := func(op OpCode, gas uint64, numPop, maxStack int) {
		e := jt[op]
		if e.constantGas != gas || e.numPop != numPop || e.maxStack != maxStack {
			panic(fmt.Sprintf("inline dispatch constants drifted for %s: folded {gas:%d pop:%d max:%d} vs table {gas:%d pop:%d max:%d}",
				op, gas, numPop, maxStack, e.constantGas, e.numPop, e.maxStack))
		}
	}
	check(POP, GasQuickStep, 1, int(params.StackLimit)+1)
	check(JUMPDEST, params.JumpdestGas, 0, int(params.StackLimit))
	check(PUSH1, GasFastestStep, 0, int(params.StackLimit)-1)
	for op := DUP1; op <= DUP16; op++ {
		check(op, GasFastestStep, int(op-DUP1)+1, int(params.StackLimit)-1)
	}
	for op := SWAP1; op <= SWAP16; op++ {
		check(op, GasFastestStep, int(op-SWAP1)+2, int(params.StackLimit))
	}
}
