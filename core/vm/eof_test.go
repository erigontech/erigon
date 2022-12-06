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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon/common"
)

func TestHasEOFMagic(t *testing.T) {
	t.Parallel()
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
		if hasEOFMagic(common.FromHex(test.code)) != test.valid {
			t.Errorf("test %d: code %v expected to be EOF", i, test.code)
		}
	}
}

func TestEOFContainer(t *testing.T) {
	t.Parallel()
	for i, test := range []struct {
		code            string
		wantTypeSize    int
		wantCodeSize    []int
		wantDataSize    int
		wantTypes       [][]int
		wantCodeOffsets []int
		wantDataOffset  int
		wantErr         error
	}{
		{
			code:            "EF00010100010000",
			wantCodeSize:    []int{1},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{
			code:            "EF0001010002000000",
			wantCodeSize:    []int{2},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{
			code:            "EF0001010002020001000000AA",
			wantCodeSize:    []int{2},
			wantDataSize:    1,
			wantCodeOffsets: []int{10},
			wantDataOffset:  12,
		},
		{
			code:            "EF0001010002020004000000AABBCCDD",
			wantCodeSize:    []int{2},
			wantDataSize:    4,
			wantCodeOffsets: []int{10},
			wantDataOffset:  12,
		},
		{
			code:            "EF0001010005020002006000600100AABB",
			wantCodeSize:    []int{5},
			wantDataSize:    2,
			wantCodeOffsets: []int{10},
			wantDataOffset:  15,
		},
		{
			code:            "EF00010100070200040060006001600200AABBCCDD",
			wantCodeSize:    []int{7},
			wantDataSize:    4,
			wantCodeOffsets: []int{10},
			wantDataOffset:  17,
		},
		{
			code:            "EF0001030004010008010002000000020160016002b000010001b1",
			wantCodeSize:    []int{8, 2},
			wantDataSize:    0,
			wantTypeSize:    4,
			wantCodeOffsets: []int{17, 25},
			wantTypes:       [][]int{{0, 0}, {2, 1}},
		},
		{ // INVALID is defined and can be terminating
			code:            "EF000101000100FE",
			wantCodeSize:    []int{1},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // terminating with RETURN
			code:            "EF00010100050060006000F3",
			wantCodeSize:    []int{5},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // terminating with REVERT
			code:            "EF00010100050060006000FD",
			wantCodeSize:    []int{5},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // terminating with SELFDESTRUCT
			code:            "EF0001010003006000FF",
			wantCodeSize:    []int{3},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // PUSH32
			code:            "EF0001010022007F000000000000000000000000000000000000000000000000000000000000000000",
			wantCodeSize:    []int{34},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // undefined instructions inside push data
			code:            "EF0001010022007F0C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F00",
			wantCodeSize:    []int{34},
			wantDataSize:    0,
			wantCodeOffsets: []int{7},
		},
		{ // undefined instructions inside data section
			code:            "EF000101000102002000000C0D0E0F1E1F2122232425262728292A2B2C2D2E2F494A4B4C4D4E4F5C5D5E5F",
			wantCodeSize:    []int{1},
			wantDataSize:    32,
			wantCodeOffsets: []int{10},
			wantDataOffset:  11,
		},
		{ // no version
			code:    "EF00",
			wantErr: ErrEOF1InvalidVersion,
		},
		{ // invalid version
			code:    "EF0000",
			wantErr: ErrEOF1InvalidVersion,
		},
		{ // invalid version
			code:    "EF0002",
			wantErr: ErrEOF1InvalidVersion,
		},
		{ // valid except version
			code:    "EF0000010002020004006000AABBCCDD",
			wantErr: ErrEOF1InvalidVersion,
		},
		{ // no header
			code:    "EF0001",
			wantErr: ErrEOF1CodeSectionMissing,
		},
		{ // no code section
			code:    "EF000100",
			wantErr: ErrEOF1CodeSectionMissing,
		},
		{ // no code section size
			code:    "EF000101",
			wantErr: ErrEOF1CodeSectionSizeMissing,
		},
		{ // code section size incomplete
			code:    "EF00010100",
			wantErr: ErrEOF1CodeSectionSizeMissing,
		},
		{ // no section terminator
			code:    "EF0001010002",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // no code section contents
			code:    "EF000101000200",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // not complete code section contents
			code:    "EF00010100020060",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // trailing bytes after code
			code:    "EF0001010002006000DEADBEEF",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // 0 size code section
			code:    "EF000101000000",
			wantErr: ErrEOF1EmptyCodeSection,
		},
		{ // 0 size code section, with non-0 data section
			code:    "EF000101000002000200AABB",
			wantErr: ErrEOF1EmptyCodeSection,
		},
		{ // data section before code section
			code:    "EF000102000401000200AABBCCDD6000",
			wantErr: ErrEOF1DataSectionBeforeCodeSection,
		},
		{ // data section without code section
			code:    "EF0001020004AABBCCDD",
			wantErr: ErrEOF1DataSectionBeforeCodeSection,
		},
		{ // no data section size
			code:    "EF000101000202",
			wantErr: ErrEOF1DataSectionSizeMissing,
		},
		{ // data section size incomplete
			code:    "EF00010100020200",
			wantErr: ErrEOF1DataSectionSizeMissing,
		},
		{ // no section terminator
			code:    "EF0001010002020004",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // no data section contents
			code:    "EF0001010002020004006000",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // not complete data section contents
			code:    "EF0001010002020004006000AABBCC",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // trailing bytes after data
			code:    "EF0001010002020004006000AABBCCDDEE",
			wantErr: ErrEOF1InvalidTotalSize,
		},
		{ // 0 size data section
			code:    "EF0001010002020000006000",
			wantErr: ErrEOF1EmptyDataSection,
		},
		{ // two data sections
			code:    "EF0001010002020004020004006000AABBCCDDAABBCCDD",
			wantErr: ErrEOF1MultipleDataSections,
		},
		{ // section id = F
			code:    "EF00010100020F0004006000AABBCCDD",
			wantErr: ErrEOF1UnknownSection,
		},
		{
			code:    "0xEF000103000401000801000201000801000200303030303030303030303000300030303030303030003000",
			wantErr: ErrEOF1TypeSectionInvalidSize,
		},
	} {
		var (
			code = common.FromHex(test.code)
			name = fmt.Sprintf("test %2d: code %v", i, test.code)
		)

		c, err := ParseEOF1Container(code)
		if err != nil || test.wantErr != nil {
			if errors.Is(err, test.wantErr) {
				continue
			}
			t.Errorf("test %d: code %v validation failure, have: %v want %v", i, test.code, err, test.wantErr)
		}

		switch {
		case len(c.codeSize) != len(test.wantCodeSize):
			t.Errorf("%s unexpected number of code sections (want: %d, have %d)", name, len(test.wantCodeSize), len(c.codeSize))
		case len(c.codeOffsets) != len(test.wantCodeOffsets):
			t.Errorf("%s unexpected number of code offsets (want: %d, have %d)", name, len(test.wantCodeOffsets), len(c.codeOffsets))
		case len(c.codeOffsets) != len(c.codeSize):
			t.Errorf("%s code offsets does not match code sizes (want: %d, have %d)", name, len(c.codeSize), len(c.codeOffsets))
		case c.dataSize != uint16(test.wantDataSize):
			t.Errorf("%s dataSize want %v, have %v", name, test.wantDataSize, c.dataSize)
		case c.dataSize != 0 && c.dataOffset != uint64(test.wantDataOffset):
			t.Errorf("%s data offset want %v, have %v", name, test.wantDataOffset, c.data)
		case c.typeSize != uint16(test.wantTypeSize):
			t.Errorf("%s typeSize want %v, have %v", name, test.wantTypeSize, c.typeSize)
		case c.typeSize != 0 && len(c.types) != len(test.wantTypes):
			t.Errorf("%s unexpected number of types %v, want %v", name, len(c.types), len(test.wantTypes))
		}
		for j := 0; j < len(c.codeSize); j++ {
			if test.wantCodeSize != nil && c.codeSize[j] != uint16(test.wantCodeSize[j]) {
				t.Errorf("%s codeSize want %v, have %v", name, test.wantCodeSize[j], c.codeSize[j])
			}
			if test.wantCodeOffsets != nil && c.codeOffsets[j] != uint64(test.wantCodeOffsets[j]) {
				t.Errorf("%s code offset want %v, have %v", name, test.wantCodeOffsets[j], c.codeOffsets[j])
			}
		}
		for j, ty := range c.types {
			if int(ty.Input) != test.wantTypes[j][0] || int(ty.Output) != test.wantTypes[j][1] {
				t.Errorf("%s types annotation mismatch (want {%d, %d}, have {%d, %d}", name, test.wantTypes[j][0], test.wantTypes[j][1], ty.Input, ty.Output)
			}
		}
	}
}

func TestInvalidInstructions(t *testing.T) {
	t.Parallel()
	for i, test := range []struct {
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
		if _, err := parseEOF(common.FromHex(test.code), &shanghaiInstructionSet); err == nil {
			t.Errorf("test %d: expected invalid code: %v", i, test.code)
		} else if !strings.HasPrefix(err.Error(), test.err.Error()) {
			t.Errorf("test %d: want error: \"%v\" have error: \"%v\"", i, test.err, err.Error())
		}
	}
}

func TestValidateUndefinedInstructions(t *testing.T) {
	t.Parallel()
	jt := &shanghaiInstructionSet
	code := common.FromHex("EF000101000200" + "0C00")
	// Iterate over all possible opcodes, except the ones with immediates:
	// PUSH1-PUSH32, RJUMP, RJUMPI, CALLF
	// The actual code being validated is
	// 0000
	// 0100
	/// ...
	for opcode := uint16(0); opcode <= 0xff; opcode++ {
		if OpCode(opcode) >= PUSH1 && OpCode(opcode) <= PUSH32 {
			continue
		}
		if OpCode(opcode) == RJUMP || OpCode(opcode) == RJUMPI || OpCode(opcode) == CALLF {
			continue
		}
		code[7] = byte(opcode)
		_, err := parseEOF(code, jt)
		if !jt[opcode].undefined {
			if err != nil {
				t.Errorf("code %v instruction validation failure, error: %v", common.Bytes2Hex(code), err)
			}
			continue
		}
		if err == nil {
			t.Errorf("opcode %v expected to be invalid", opcode)
		} else if !strings.HasPrefix(err.Error(), ErrEOF1UndefinedInstruction.Error()) {
			t.Errorf("opcode %v unexpected error: \"%v\"", opcode, err.Error())
		}
	}
}

func TestValidateTerminatingInstructions(t *testing.T) {
	t.Parallel()
	jt := &shanghaiInstructionSet
	code := common.FromHex("EF000101000100" + "0C")
	for opcodeValue := uint16(0); opcodeValue <= 0xff; opcodeValue++ {
		opcode := OpCode(opcodeValue)
		// Iterate over all possible opcodes, except the ones with immediates:
		// PUSH1-PUSH32, RJUMP, RJUMPI, CALLF
		if opcode >= PUSH1 && opcode <= PUSH32 || opcode == RJUMP || opcode == RJUMPI || opcode == CALLF || opcode == RETF {
			continue
		}
		if jt[opcode].undefined {
			continue
		}
		code[7] = byte(opcode)
		_, err := parseEOF(code, jt)
		if opcode.isTerminating() {
			if err != nil {
				t.Errorf("opcode %v expected to be valid terminating instruction", opcode)
			}
			continue
		}
		if err == nil {
			t.Errorf("opcode %v expected to be invalid terminating instruction", opcode)
		} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
			t.Errorf("opcode %v unexpected error: \"%v\"", opcode, err.Error())
		}
	}
}

func TestValidateTruncatedPush(t *testing.T) {
	jt := &shanghaiInstructionSet
	zeroes := [33]byte{}
	code := common.FromHex("EF0001010001000C")
	for opcode := PUSH1; opcode <= PUSH32; opcode++ {
		requiredBytes := opcode - PUSH1 + 1

		// make code with truncated PUSH data
		codeTruncatedPush := append(code, zeroes[:requiredBytes-1]...)
		codeTruncatedPush[5] = byte(len(codeTruncatedPush) - 7)
		codeTruncatedPush[7] = byte(opcode)

		if _, err := parseEOF(codeTruncatedPush, jt); err == nil {
			t.Errorf("code %v has truncated PUSH, expected to be invalid", common.Bytes2Hex(codeTruncatedPush))
		} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeTruncatedPush), err)
		}

		// make code with full PUSH data but no terminating instruction in the end
		codeNotTerminated := append(code, zeroes[:requiredBytes]...)
		codeNotTerminated[5] = byte(len(codeNotTerminated) - 7)
		codeNotTerminated[7] = byte(opcode)

		if _, err := parseEOF(codeNotTerminated, jt); err == nil {
			t.Errorf("code %v does not have terminating instruction, expected to be invalid", common.Bytes2Hex(codeNotTerminated))
		} else if !strings.HasPrefix(err.Error(), ErrEOF1TerminatingInstructionMissing.Error()) {
			t.Errorf("code %v unexpected validation error: %v", common.Bytes2Hex(codeNotTerminated), err)
		}

		// make valid code
		codeValid := append(code, zeroes[:requiredBytes+1]...) // + 1 for terminating STOP
		codeValid[5] = byte(len(codeValid) - 7)
		codeValid[7] = byte(opcode)

		if _, err := parseEOF(codeValid, jt); err != nil {
			t.Errorf("code %v instruction validation failure, error: %v", common.Bytes2Hex(code), err)
		}
	}
}

func parseEOF(b []byte, jt *JumpTable) (*EOF1Container, error) {
	c, err := ParseEOF1Container(b)
	if err != nil {
		return nil, err
	}
	if err := c.ValidateCode(jt); err != nil {
		return nil, err
	}
	return c, nil
}
