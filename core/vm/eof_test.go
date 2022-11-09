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
	"fmt"
	"strings"
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
		code        string
		typeSize    int
		codeSize    []int
		dataSize    int
		types       [][]int
		codeOffsets []int
		data        int
		err         error
	}{
		{
			code:        "EF00010100010000",
			codeSize:    []int{1},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{
			code:        "EF0001010002000000",
			codeSize:    []int{2},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{
			code:        "EF0001010002020001000000AA",
			codeSize:    []int{2},
			dataSize:    1,
			codeOffsets: []int{10},
			data:        12,
		},
		{
			code:        "EF0001010002020004000000AABBCCDD",
			codeSize:    []int{2},
			dataSize:    4,
			codeOffsets: []int{10},
			data:        12,
		},
		{
			code:        "EF0001010005020002006000600100AABB",
			codeSize:    []int{5},
			dataSize:    2,
			codeOffsets: []int{10},
			data:        15,
		},
		{
			code:        "EF00010100070200040060006001600200AABBCCDD",
			codeSize:    []int{7},
			dataSize:    4,
			codeOffsets: []int{10},
			data:        17,
		},
		{
			code:        "EF00010300040100080100020000000201600160025e0001000149",
			codeSize:    []int{8, 2},
			dataSize:    0,
			typeSize:    4,
			codeOffsets: []int{17, 25},
			types:       [][]int{{0, 0}, {2, 1}},
		},
		{ // INVALID is defined and can be terminating
			code:        "EF000101000100FE",
			codeSize:    []int{1},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // terminating with RETURN
			code:        "EF00010100050060006000F3",
			codeSize:    []int{5},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // terminating with REVERT
			code:        "EF00010100050060006000FD",
			codeSize:    []int{5},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // terminating with SELFDESTRUCT
			code:        "EF0001010003006000FF",
			codeSize:    []int{3},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // PUSH32
			code:        "EF0001010022007F000000000000000000000000000000000000000000000000000000000000000000",
			codeSize:    []int{34},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // undefined instructions inside push data
			code:        "EF0001010022007F0C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F00",
			codeSize:    []int{34},
			dataSize:    0,
			codeOffsets: []int{7},
		},
		{ // undefined instructions inside data section
			code:        "EF000101000102002000000C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F",
			codeSize:    []int{1},
			dataSize:    32,
			codeOffsets: []int{10},
			data:        11,
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
		var (
			jt   = &shanghaiInstructionSet
			code = common.Hex2Bytes(test.code)
		)

		// Need to validate both code paths. First is the "trusted" EOF
		// code that was validated before being stored. This can't
		// throw and error. The other path is "untrusted" and so the
		// code is validated as it is parsed. This may throw an error.
		// If the error is expected, we compare the errors and continue
		// on.
		con1, err := NewEOF1Container(code, jt, false)
		if err != nil {
			if err == test.err {
				// failed as expected
				continue
			} else {
				t.Errorf("test %d: code %v validation failure, error: %v", i, test.code, err)
			}
		}
		con2, _ := NewEOF1Container(code, jt, true)

		validate := func(c EOF1Container, name string) {
			if len(c.header.codeSize) != len(test.codeSize) {
				t.Errorf("%s unexpected number of code sections (want: %d, got %d)", name, len(test.codeSize), len(c.header.codeSize))
			}
			if len(c.code) != len(test.codeOffsets) {
				t.Errorf("%s unexpected number of code offsets (want: %d, got %d)", name, len(test.codeOffsets), len(c.code))
			}
			if len(c.code) != len(c.header.codeSize) {
				t.Errorf("%s code offsets does not match code sizes (want: %d, got %d)", name, len(c.header.codeSize), len(c.code))
			}
			for j := 0; j < len(c.header.codeSize); j++ {
				if test.codeSize != nil && c.header.codeSize[j] != uint16(test.codeSize[j]) {
					t.Errorf("%s codeSize expected %v, got %v", name, test.codeSize[j], c.header.codeSize[j])
				}
				if test.codeOffsets != nil && c.code[j] != uint64(test.codeOffsets[j]) {
					t.Errorf("%s code offset expected %v, got %v", name, test.codeOffsets[j], c.code[j])
				}
			}
			if c.header.dataSize != uint16(test.dataSize) {
				t.Errorf("%s dataSize expected %v, got %v", name, test.dataSize, c.header.dataSize)
			}
			if c.header.dataSize != 0 && c.data != uint64(test.data) {
				t.Errorf("%s data offset expected %v, got %v", name, test.data, c.data)
			}
			if c.header.typeSize != uint16(test.typeSize) {
				t.Errorf("%s typeSize expected %v, got %v", name, test.typeSize, c.header.typeSize)
			}
			if c.header.typeSize != 0 && len(c.types) != len(test.types) {
				t.Errorf("%s unexpected number of types %v, want %v", name, len(c.types), len(test.types))
			}
			for j, ty := range c.types {
				if int(ty.input) != test.types[j][0] || int(ty.output) != test.types[j][1] {
					t.Errorf("%s types annotation mismatch (want {%d, %d}, got {%d, %d}", name, test.types[j][0], test.types[j][1], ty.input, ty.output)
				}
			}
		}
		validate(con1, fmt.Sprintf("test %2d     (validated): code %v", i, test.code))
		validate(con2, fmt.Sprintf("test %2d (not validated): code %v", i, test.code))
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
		_, err := NewEOF1Header(common.Hex2Bytes(test.code), &shanghaiInstructionSet, false)
		if err == nil {
			t.Errorf("code %v expected to be invalid", test.code)
		} else if !strings.HasPrefix(err.Error(), test.err.Error()) {
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
		_, err := NewEOF1Header(code, jt, false)
		if jt[opcode].undefined {
			if err == nil {
				t.Errorf("opcode %v expected to be invalid", opcode)
			} else if !strings.HasPrefix(err.Error(), ErrEOF1UndefinedInstruction.Error()) {
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
		_, err := NewEOF1Header(code, jt, false)
		if opcode.isTerminating() {
			if err != nil {
				t.Errorf("opcode %v expected to be valid terminating instruction", opcode)
			}
		} else {
			if err == nil {
				t.Errorf("opcode %v expected to be invalid terminating instruction", opcode)
			} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
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

		_, err := NewEOF1Header(codeTruncatedPush, jt, false)
		if err == nil {
			t.Errorf("code %v has truncated PUSH, expected to be invalid", common.Bytes2Hex(codeTruncatedPush))
		} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeTruncatedPush), err)
		}

		// make code with full PUSH data but no terminating instruction in the end
		codeNotTerminated := append(code, zeroes[:requiredBytes]...)
		codeNotTerminated[5] = byte(len(codeNotTerminated) - 7)
		codeNotTerminated[7] = byte(opcode)

		_, err = NewEOF1Header(codeNotTerminated, jt, false)
		if err == nil {
			t.Errorf("code %v does not have terminating instruction, expected to be invalid", common.Bytes2Hex(codeNotTerminated))
		} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeNotTerminated), err)
		}

		// make valid code
		codeValid := append(code, zeroes[:requiredBytes+1]...) // + 1 for terminating STOP
		codeValid[5] = byte(len(codeValid) - 7)
		codeValid[7] = byte(opcode)

		_, err = NewEOF1Header(codeValid, jt, false)
		if err != nil {
			t.Errorf("code %v instruction validation failure, error: %v", common.Bytes2Hex(code), err)
		}
	}
}
