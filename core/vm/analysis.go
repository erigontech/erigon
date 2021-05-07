// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/params")


// what we want are relative changes, what we have are absolute minStack and maxStack
func pops(oper operation) int {
	return op.minStack
}
func pushes(oper operation) int {
	return -(oper.maxStack - int(params.StackLimit) - pops(oper))
}
func changes(oper operation) int {
	return pushes(oper) - pops(oper)
}

// fill in segment of operation information array for a block
func analyzeBlock(ctx *callCtx, pc uint64) (*BlockInfo, error) {
	blockInfo := NewBlockInfo(ctx.contract, pc)
	code := ctx.contract.Code
	jumpTable := ctx.interpreter.jt

	height := 0
	minHeight := 0
	maxHeight := 0
	for ; pc < uint64(len(code)); pc++ {
		oper := jumpTable[op]
		if oper == nil {
			continue
		}
		op := OpCode(code[pc])

		// track low and high watermark relative to block entry
		height += changes(*oper)
		minHeight = min(minHeight, height)
		maxHeight = max(maxHeight, height)
		blockInfo.constantGas += oper.constantGas

		if PUSH1 <= op && op <= PUSH32 {
		    pushByteSize := int(op) - int(PUSH1) + 1
			codeLen := len(ctx.contract.Code)

			startMin := int(pc + 1)
			if startMin >= codeLen {
				startMin = codeLen
			}
			endMin := startMin + pushByteSize
			if startMin+pushByteSize >= codeLen {
				endMin = codeLen
			}

			integer := new(uint256.Int)
			integer.SetBytes(common.RightPadBytes(
				// So it doesn't matter what we push onto the stack.
				ctx.contract.Code[startMin:endMin], pushByteSize))

			// attach PushInfo with decoded push data to PUSHn
			ctx.contract.opsInfo[pc] = NewPushInfo(ctx.contract, pc, *integer)

			continue
		}
		if op == JUMP || op == JUMPI {
			prevOp := OpCode(code[pc-1])
			if prevOp >= PUSH1 && prevOp <= PUSH32 {

				// replace with JMP NOOP or JMPI NOOP and attach JumpInfo to JMP or JMPI
				if op == JUMP {
					code[pc-1] = byte(JMP)
				}
				if op == JUMPI {
					code[pc-1] = byte(JMPI)
				}
				code[pc] = byte(NOOP)
				ctx.contract.opsInfo[pc] = NewJumpInfo(ctx.contract, pc)

				// end block
				break
			}
		}
		if	op == JUMPDEST || op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {

			// end block
			break
		}
	}

	// min and max absolute stack length to avoid stack underflow or underflow
	blockInfo.minStack = -minHeight
	blockInfo.maxStack = int(params.StackLimit) - maxHeight

	ctx.contract.opsInfo[pc] = blockInfo
	return blockInfo, nil
}

// codeBitmap collects data locations in code.
func codeBitmap(code []byte) []uint64 {
	// The bitmap is 4 bytes longer than necessary, in case the code
	// ends with a PUSH32, the algorithm will push zeroes onto the
	// bitvector outside the bounds of the actual code.
	bits := make([]uint64, (len(code)+32+63)/64)

	for pc := 0; pc < len(code); {
		op := OpCode(code[pc])
		pc++
		if op >= PUSH1 && op <= PUSH32 {
			numbits := int(op - PUSH1 + 1)
			x := uint64(1) << (op - PUSH1)
			x = x | (x - 1) // Smear the bit to the right
			idx := pc / 64
			shift := pc & 63
			bits[idx] |= x << shift
			if shift+shift > 64 {
				bits[idx+1] |= x >> (64 - shift)
			}
			pc += numbits
		}
	}
	return bits
}
