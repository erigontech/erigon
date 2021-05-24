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
)

// fill in a segment of the operation information array for a block
// -- conditions for block boundaries are subtle
//
func analyzeBlock(ctx *callCtx, pc uint64) (*BlockInfo, error) {
	contract := ctx.contract
	blockInfo := NewBlockInfo(pc)
	if (int64(pc) == -1) {
		contract.preInfo = blockInfo
		pc++
	} else {
		contract.opsInfo[pc] = blockInfo
	}
	code := contract.Code
	codeLen := len(code)
	jumpTable := ctx.interpreter.jt
	var (
		height		int
		minHeight	int
		maxHeight	int
		op			OpCode
		prevOp      OpCode
		prevPC      uint64
	)
	for ; pc < uint64(codeLen); pc++ {
		op = OpCode(code[pc])
		oper := jumpTable[op]
		if oper == nil {
			continue
		}

		// track low and high watermark relative to block entry
		height -= oper.numPop
		minHeight = min(minHeight, height)	// will be <= 0
		height += oper.numPush
		maxHeight = max(maxHeight, height)	// will be >= 0
		blockInfo.constantGas += oper.constantGas
		pushByteSize := 0

		if PUSH1 <= op && op <= PUSH32 {
			pushByteSize = int(op) - int(PUSH1) + 1

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
				code[startMin:endMin], pushByteSize))

			// attach PushInfo with decoded push data to PUSHn
			pushInfo := NewPushInfo(pc, *integer)
			contract.opsInfo[pc] = pushInfo

			prevOp = op
			prevPC = pc
			pc += uint64(pushByteSize)
			continue

		} else if op == JUMP || op == JUMPI {

			// check jump destinations and optimize static jumps
			if PUSH1 <= prevOp && prevOp <= PUSH32 {
				pos := contract.opsInfo[prevPC].(*PushInfo).data
				if valid, _ := contract.validJumpdest(&pos); !valid {
					return nil, ErrInvalidJump
				}
				// replace PUSH* JUMP with JMP NOOP...
				if op == JUMP {
					code[prevPC] = byte(JMP)
					code[prevPC+1] = byte(NOOP)
				}
				// replace PUSH* JUMPI with JMPI JUMPDEST NOOP...
				if op == JUMPI {
					code[prevPC] = byte(JMPI)
					code[prevPC+1] = byte(JUMPDEST)
				}
				for i := prevPC+2; i <= pc; i++ {
					code[i] = byte(NOOP)
				}	
				// attach JumpInfo			
				contract.opsInfo[prevPC] = NewJumpInfo(prevPC, pos.Uint64())
				break // end block
			}
		}
		prevOp = OpCode(0)
		prevPC = 0
		if op == JUMPI {
			if (pc != blockInfo.pc) { // if op doesn't start block
				break // end block
			}
		} else if op == JUMPDEST {
			if (pc != blockInfo.pc) { // if op doesn't start block
				break // end block
			}
		} else if op == JUMP || op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			break // end block
		}
	}
	// min and max absolute stack length to avoid stack underflow or underflow
	blockInfo.minStack = -minHeight
	blockInfo.maxStack = maxHeight

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
