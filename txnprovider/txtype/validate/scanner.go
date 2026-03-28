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

// Package validate provides static bytecode analysis for transaction validation.
// It is used as a pool-admission pre-filter for AA (RIP-7560) and Frame (EIP-8141)
// transactions; it complements but does not replace the execution-time
// ValidationRulesTracer.
//
// The scanner is PUSH-aware: PUSH1–PUSH32 operand bytes are consumed silently so
// they are never mistaken for opcodes.  Rules receive the full instruction slice
// and a current index so they can look ahead or behind without extra state.
package validate

import (
	"github.com/erigontech/erigon/execution/vm"
)

// Insn is a single decoded instruction: its byte-offset within the bytecode
// and the opcode at that position.
type Insn struct {
	PC     uint64
	Opcode vm.OpCode
}

// Scan decodes raw EVM bytecode into a slice of instructions.
//
// The scanner is PUSH-aware: after a PUSH1–PUSH32 opcode it advances past the
// immediate operand bytes so they are never emitted as instructions.  All other
// bytes are emitted as-is.  An empty or nil input returns an empty slice.
func Scan(code []byte) []Insn {
	if len(code) == 0 {
		return nil
	}
	out := make([]Insn, 0, len(code)/2+1)
	for i := 0; i < len(code); {
		op := vm.OpCode(code[i])
		out = append(out, Insn{PC: uint64(i), Opcode: op})
		i++
		if op.IsPushWithImmediateArgs() {
			// PUSH1 has 1 operand byte, PUSH2 has 2, ..., PUSH32 has 32.
			operandBytes := int(op-vm.PUSH1) + 1
			i += operandBytes
		}
	}
	return out
}
