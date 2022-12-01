// Copyright 2021 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// validateCode checks that there're no undefined instructions and code ends with a terminating instruction
func validateCode(code []byte, codeSections int, jumpTable *JumpTable) error {
	var (
		i        = 0
		analysis []uint64
		opcode   OpCode
	)
	for i < len(code) {
		switch opcode = OpCode(code[i]); {
		case jumpTable[opcode].undefined:
			return fmt.Errorf("%v: %v", ErrEOF1UndefinedInstruction, opcode)
		case opcode >= PUSH1 && opcode <= PUSH32:
			i += int(opcode) - int(PUSH1) + 2
			continue // todo make sure this actually continues
		case opcode == RJUMP || opcode == RJUMPI:
			var arg int16
			// Read immediate argument.
			if err := binary.Read(bytes.NewReader(code[i+1:]), binary.BigEndian, &arg); err != nil {
				return ErrEOF1InvalidRelativeOffset
			}
			// Calculate relative target.
			pos := i + 3 + int(arg)

			// Check if offset points to out-of-bounds location.
			if pos < 0 || pos >= len(code) {
				return ErrEOF1InvalidRelativeOffset
			}
			// Check if offset points to non-code segment.
			// TODO(matt): include CALLF and RJUMPs in analysis.
			if analysis == nil {
				analysis = codeBitmap(code)
			}
			if analysis[pos/64]&(1<<(uint64(pos)&63)) != 0 {
				return ErrEOF1InvalidRelativeOffset
			}
			i += 3
		case opcode == CALLF:
			arg, err := parseArg(code[i+1:])
			if err != nil {
				return ErrEOF1InvalidCallfSection
			}
			if int(arg) >= codeSections {
				return fmt.Errorf("%v: want section %v, but only have %d sections", ErrEOF1InvalidCallfSection, arg, codeSections)
			}
			i += 3
		default:
			i++
		}
	}
	if !opcode.isTerminating() {
		return fmt.Errorf("%v: %v", ErrEOF1TerminatingInstructionMissing, opcode)
	}
	return nil
}
