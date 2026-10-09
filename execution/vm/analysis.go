// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// codeBitmapGeneric collects valid jump destinations in code: JUMPDEST opcodes outside of push data.
func codeBitmapGeneric(code []byte) bitvec {
	bits := make(bitvec, (len(code)+63)/64)
	for pc := 0; pc < len(code); {
		// Collect the bits of a 64-byte chunk in a register: updating the bitmap in memory
		// for every opcode makes each iteration wait for the previous store.
		i := pc / 64
		end := min(i*64+64, len(code))
		var w uint64
		for pc < end {
			op := OpCode(code[pc])
			if int8(op) < int8(PUSH1) { // not PUSH1..PUSH32, as int8(op) > int8(PUSH32) is always false
				// Avoid a data-dependent branch: it mispredicts on code mixing JUMPDEST and other opcodes.
				var j uint64
				if op == JUMPDEST {
					j = 1
				}
				w |= j << (uint(pc) % 64)
				pc++
				continue
			}
			pc += 1 + int(op-PUSH1+1)
		}
		bits[i] = w
	}
	return bits
}

// markJumpdestsTail marks the JUMPDESTs of the code tail of less than 32 bytes starting at pc.
func markJumpdestsTail(code []byte, bits bitvec, pc int) bitvec {
	for ; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			bits[pc/64] |= 1 << (uint(pc) % 64)
		}
	}
	return bits
}

// bitvec is a bit vector which maps bytes in a program.
// A set bit means the byte is a valid jump destination.
type bitvec []uint64

// isJumpdest checks if the position is a valid jump destination.
func (bits bitvec) isJumpdest(pos uint64) bool {
	return ((bits[pos/64] >> (pos % 64)) & 1) != 0
}

// instr is one instruction of a decoded program: its kind, which is the opcode
// or one of the kinds below for a PUSH, its pc and the kind's operand. It is one
// word, so the loop loads it with one instruction.
type instr uint64

func newInstr(kind OpCode, pc int, arg uint32) instr {
	return instr(kind) | instr(pc)<<16 | instr(arg)<<32
}

func (in instr) kind() OpCode { return OpCode(in) }
func (in instr) pc() uint64   { return uint64(uint16(in >> 16)) }
func (in instr) arg() uint32  { return uint32(in >> 32) }

// The kinds of decoded PUSHes. They reuse PUSH opcodes, which decode never emits as kinds.
const (
	pushImm   = PUSH1 // pushes arg
	pushConst = PUSH2 // pushes consts[arg]
	jumpTo    = PUSH3 // PUSHn dest JUMP to a valid dest, whose instruction index is arg
	jumpiTo   = PUSH4 // PUSHn dest JUMPI, as jumpTo
)

// jumpdestBit marks the JUMPDESTs in program.idx.
const jumpdestBit = 1 << 15

// program is code decoded for runDecoded. Its 16-bit pcs and indices fit code of up to params.MaxCodeSize.
type program struct {
	ins []instr
	// idx maps each pc that starts an instruction, and len(code), to its instruction
	// index, with jumpdestBit set at JUMPDESTs.
	idx    []uint16
	consts []uint256.Int
	hash   accounts.CodeHash
}

func decode(code []byte) *program {
	n := 0
	for pc := 0; pc < len(code); pc++ {
		if op := OpCode(code[pc]); op.IsPushWithImmediateArgs() {
			pc += int(op - PUSH0)
		}
		n++
	}
	p := &program{ins: make([]instr, 0, n), idx: make([]uint16, len(code)+1)}
	for pc := 0; pc < len(code); {
		op := OpCode(code[pc])
		p.idx[pc] = uint16(len(p.ins))
		if op == JUMPDEST {
			p.idx[pc] |= jumpdestBit
		}
		kind, arg := op, uint32(0)
		next := pc + 1
		if op.IsPushWithImmediateArgs() {
			next += int(op - PUSH0)
			var v uint256.Int
			v.SetBytes(code[pc+1 : min(next, len(code))])
			if missing := next - len(code); missing > 0 {
				v.Lsh(&v, uint(8*missing))
			}
			if v.IsUint64() && v.Uint64() <= math.MaxUint32 {
				kind, arg = pushImm, uint32(v.Uint64())
			} else {
				kind, arg = pushConst, uint32(len(p.consts))
				p.consts = append(p.consts, v)
			}
		}
		p.ins = append(p.ins, newInstr(kind, pc, arg))
		pc = next
	}
	p.idx[len(code)] = uint16(len(p.ins))
	for i := 1; i < len(p.ins); i++ {
		push := p.ins[i-1]
		if push.kind() != pushImm || push.arg() >= uint32(len(code)) || p.idx[push.arg()]&jumpdestBit == 0 {
			continue
		}
		kind := jumpTo
		switch p.ins[i].kind() {
		case JUMP:
		case JUMPI:
			kind = jumpiTo
		default:
			continue
		}
		p.ins[i-1] = newInstr(kind, int(push.pc()), uint32(p.idx[push.arg()]&^jumpdestBit))
	}
	return p
}

// jumpdest returns idx at pos, which has jumpdestBit set only for a valid jump destination.
func (p *program) jumpdest(pos *uint256.Int) uint16 {
	if !pos.IsUint64() || pos.Uint64() >= uint64(len(p.idx)) {
		return 0
	}
	return p.idx[pos.Uint64()]
}

func (p *program) size() int {
	return len(p.ins)*8 + len(p.idx)*2 + len(p.consts)*32
}
