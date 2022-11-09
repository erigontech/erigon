// Copyright 2021 The go-ethereum Authors
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
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestHasEOFMagic(t *testing.T) {
	for i, test := range []struct {
		code  string
		valid bool
	}{
		{"EF00010100010000", true},
		{"EF0001010002000000", true},
		// invalid EOF but still has magic
		{"EF00010100020F0004006000AABBCCDD", true},
		// empty
		{"", false},
		// invalid first byte
		{"FE", false},
		// valid except first byte of magic
		{"FE0001010002020004006000AABBCCDD", false},
		// incomplete magic
		{"EF", false},
		// not correct magic
		{"EF01", false},
		// valid except second byte of magic
		{"EF0101010002020004006000AABBCCDD", false},
	} {
		if hasEOFMagic(common.Hex2Bytes(test.code)) != test.valid {
			t.Errorf("test %d: code %v expected to be EOF", i, test.code)
		}
	}
}

func TestEOFContainer(t *testing.T) {
	for i, test := range []struct {
		code     string
		codeSize uint16
		dataSize uint16
		err      error
	}{
		{
			code:     "EF00010100010000",
			codeSize: 1,
			dataSize: 0,
		},
		{
			code:     "EF0001010002000000",
			codeSize: 2,
			dataSize: 0,
		},
		{
			code:     "EF0001010002020001000000AA",
			codeSize: 2,
			dataSize: 1,
		},
		{
			code:     "EF0001010002020004000000AABBCCDD",
			codeSize: 2,
			dataSize: 4,
		},
		{
			code:     "EF0001010005020002006000600100AABB",
			codeSize: 5,
			dataSize: 2,
		},
		{
			code:     "EF00010100070200040060006001600200AABBCCDD",
			codeSize: 7,
			dataSize: 4,
		},
		{ // INVALID is defined and can be terminating
			code:     "EF000101000100FE",
			codeSize: 1,
			dataSize: 0,
		},
		{ // terminating with RETURN
			code:     "EF00010100050060006000F3",
			codeSize: 5,
			dataSize: 0,
		},
		{ // terminating with REVERT
			code:     "EF00010100050060006000FD",
			codeSize: 5,
			dataSize: 0,
		},
		{ // terminating with SELFDESTRUCT
			code:     "EF0001010003006000FF",
			codeSize: 3,
			dataSize: 0,
		},
		{ // PUSH32
			code:     "EF0001010022007F000000000000000000000000000000000000000000000000000000000000000000",
			codeSize: 34,
			dataSize: 0,
		},
		{ // undefined instructions inside push data
			code:     "EF0001010022007F0C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F00",
			codeSize: 34,
			dataSize: 0,
		},
		{ // undefined instructions inside data section
			code:     "EF000101000102002000000C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F",
			codeSize: 1,
			dataSize: 32,
		},
		{ // no version
			code: "EF00",
			err:  ErrEOF1InvalidVersion,
		},
		{ // invalid version
			code: "EF0000",
			err:  ErrEOF1InvalidVersion,
		},
		{ // invalid version
			code: "EF0002",
			err:  ErrEOF1InvalidVersion,
		},
		{ // valid except version
			code: "EF0000010002020004006000AABBCCDD",
			err:  ErrEOF1InvalidVersion,
		},
		{ // no header
			code: "EF0001",
			err:  ErrEOF1CodeSectionMissing,
		},
		{ // no code section
			code: "EF000100",
			err:  ErrEOF1CodeSectionMissing,
		},
		{ // no code section size
			code: "EF000101",
			err:  ErrEOF1CodeSectionSizeMissing,
		},
		{ // code section size incomplete
			code: "EF00010100",
			err:  ErrEOF1CodeSectionSizeMissing,
		},
		{ // no section terminator
			code: "EF0001010002",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // no code section contents
			code: "EF000101000200",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // not complete code section contents
			code: "EF00010100020060",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // trailing bytes after code
			code: "EF0001010002006000DEADBEEF",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // 0 size code section
			code: "EF000101000000",
			err:  ErrEOF1EmptyCodeSection,
		},
		{ // 0 size code section, with non-0 data section
			code: "EF000101000002000200AABB",
			err:  ErrEOF1EmptyCodeSection,
		},
		{ // data section before code section
			code: "EF000102000401000200AABBCCDD6000",
			err:  ErrEOF1DataSectionBeforeCodeSection,
		},
		{ // data section without code section
			code: "EF0001020004AABBCCDD",
			err:  ErrEOF1DataSectionBeforeCodeSection,
		},
		{ // no data section size
			code: "EF000101000202",
			err:  ErrEOF1DataSectionSizeMissing,
		},
		{ // data section size incomplete
			code: "EF00010100020200",
			err:  ErrEOF1DataSectionSizeMissing,
		},
		{ // no section terminator
			code: "EF0001010002020004",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // no data section contents
			code: "EF0001010002020004006000",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // not complete data section contents
			code: "EF0001010002020004006000AABBCC",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // trailing bytes after data
			code: "EF0001010002020004006000AABBCCDDEE",
			err:  ErrEOF1InvalidTotalSize,
		},
		{ // 0 size data section
			code: "EF0001010002020000006000",
			err:  ErrEOF1EmptyDataSection,
		},
		{ // two data sections
			code: "EF0001010002020004020004006000AABBCCDDAABBCCDD",
			err:  ErrEOF1MultipleDataSections,
		},
		{ // section id = F
			code: "EF00010100020F0004006000AABBCCDD",
			err:  ErrEOF1UnknownSection,
		},
	} {
		header, err := readEOF1Header(common.Hex2Bytes(test.code))
		if err != nil {
			if err == test.err {
				// failed as expected
				continue
			} else {
				t.Errorf("test %d: code %v validation failure, error: %v", i, test.code, err)
			}
		}
		if header.codeSize[0] != test.codeSize {
			t.Errorf("test %d: code %v codeSize expected %v, got %v", i, test.code, test.codeSize, header.codeSize[0])
		}
		if header.dataSize != test.dataSize {
			t.Errorf("test %d: code %v dataSize expected %v, got %v", i, test.code, test.dataSize, header.dataSize)
		}
	}
}

func TestInvalidInstructions(t *testing.T) {
	for _, test := range []struct {
		code string
		err  error
	}{
		// 0C is undefined instruction
		{"EF0001010001000C", ErrEOF1UndefinedInstruction},
		// EF is undefined instruction
		{"EF000101000100EF", ErrEOF1UndefinedInstruction},
		// ADDRESS is not a terminating instruction
		{"EF00010100010030", ErrEOF1TerminatingInstructionMissing},
		// PUSH1 without data
		{"EF00010100010060", ErrEOF1TerminatingInstructionMissing},
		// PUSH32 with 31 bytes of data
		{"EF0001010020007F00000000000000000000000000000000000000000000000000000000000000", ErrEOF1TerminatingInstructionMissing},
		// PUSH32 with 32 bytes of data and no terminating instruction
		{"EF0001010021007F0000000000000000000000000000000000000000000000000000000000000000", ErrEOF1TerminatingInstructionMissing},
		// RJUMP to out-of-bounds (negative) offset.
		{"EF0001010004005CFFFA00", ErrEOF1InvalidRelativeOffset},
		// RJUMP to out-of-bounds (positive) offset.
		{"EF0001010004005C000600", ErrEOF1InvalidRelativeOffset},
		// RJUMP to push data.
		{"EF0001010006005C0001600100", ErrEOF1InvalidRelativeOffset},
	} {
		_, err := validateEOF(common.Hex2Bytes(test.code), &shanghaiInstructionSet)
		if err == nil {
			t.Errorf("code %v expected to be invalid", test.code)
		} else if err.Error() != test.err.Error() {
			t.Errorf("code %v expected error: \"%v\" got error: \"%v\"", test.code, test.err, err.Error())
		}
	}
}

func TestValidateUndefinedInstructions(t *testing.T) {
	jt := &shanghaiInstructionSet
	code := common.Hex2Bytes("EF0001010002000C00")
	instrByte := &code[7]
	for opcode := uint16(0); opcode <= 0xff; opcode++ {
		if OpCode(opcode) >= PUSH1 && OpCode(opcode) <= PUSH32 {
			continue
		}
		if OpCode(opcode) == RJUMP || OpCode(opcode) == RJUMPI || OpCode(opcode) == CALLF {
			continue
		}

		*instrByte = byte(opcode)
		_, err := validateEOF(code, jt)
		if jt[opcode].undefined {
			if err == nil {
				t.Errorf("opcode %v expected to be invalid", opcode)
			} else if err != ErrEOF1UndefinedInstruction {
				t.Errorf("opcode %v unxpected error: \"%v\"", opcode, err.Error())
			}
		} else {
			if err != nil {
				t.Errorf("code %v instruction validation failure, error: %v", common.Bytes2Hex(code), err)
			}
		}
	}
}

func TestValidateTerminatingInstructions(t *testing.T) {
	jt := &shanghaiInstructionSet
	code := common.Hex2Bytes("EF0001010001000C")
	instrByte := &code[7]
	for opcodeValue := uint16(0); opcodeValue <= 0xff; opcodeValue++ {
		opcode := OpCode(opcodeValue)
		if opcode >= PUSH1 && opcode <= PUSH32 || opcode == RJUMP || opcode == RJUMPI || opcode == CALLF || opcode == RETF {
			continue
		}
		if jt[opcode].undefined {
			continue
		}
		*instrByte = byte(opcode)
		_, err := validateEOF(code, jt)
		if opcode.isTerminating() {
			if err != nil {
				t.Errorf("opcode %v expected to be valid terminating instruction", opcode)
			}
		} else {
			if err == nil {
				t.Errorf("opcode %v expected to be invalid terminating instruction", opcode)
			} else if err != ErrEOF1TerminatingInstructionMissing {
				t.Errorf("opcode %v unexpected error: \"%v\"", opcode, err.Error())
			}
		}
	}
}

func TestValidateTruncatedPush(t *testing.T) {
	jt := &shanghaiInstructionSet
	zeroes := [33]byte{}
	code := common.Hex2Bytes("EF0001010001000C")
	for opcode := PUSH1; opcode <= PUSH32; opcode++ {
		requiredBytes := opcode - PUSH1 + 1

		// make code with truncated PUSH data
		codeTruncatedPush := append(code, zeroes[:requiredBytes-1]...)
		codeTruncatedPush[5] = byte(len(codeTruncatedPush) - 7)
		codeTruncatedPush[7] = byte(opcode)

		_, err := validateEOF(codeTruncatedPush, jt)
		if err == nil {
			t.Errorf("code %v has truncated PUSH, expected to be invalid", common.Bytes2Hex(codeTruncatedPush))
		} else if err != ErrEOF1TerminatingInstructionMissing {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeTruncatedPush), err)
		}

		// make code with full PUSH data but no terminating instruction in the end
		codeNotTerminated := append(code, zeroes[:requiredBytes]...)
		codeNotTerminated[5] = byte(len(codeNotTerminated) - 7)
		codeNotTerminated[7] = byte(opcode)

		_, err = validateEOF(codeNotTerminated, jt)
		if err == nil {
			t.Errorf("code %v does not have terminating instruction, expected to be invalid", common.Bytes2Hex(codeNotTerminated))
		} else if err != ErrEOF1TerminatingInstructionMissing {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeNotTerminated), err)
		}

		// make valid code
		codeValid := append(code, zeroes[:requiredBytes+1]...) // + 1 for terminating STOP
		codeValid[5] = byte(len(codeValid) - 7)
		codeValid[7] = byte(opcode)

		_, err = validateEOF(codeValid, jt)
		if err != nil {
			t.Errorf("code %v instruction validation failure, error: %v", common.Bytes2Hex(code), err)
		}
	}
}
