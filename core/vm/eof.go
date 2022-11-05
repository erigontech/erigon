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

type eofVersion int
type headerSection byte

const (
	eofFormatByte  byte          = 0xEF
	eof1Version    eofVersion    = 1
	kindTerminator headerSection = 0
	kindCode       headerSection = 1
	kindData       headerSection = 2

	versionOffset      int = 2
	sectionHeaderStart int = 3
)

var eofMagic []byte = []byte{0xEF, 0x00}

type EOF1Header struct {
	codeSize uint16 // Size of code section. Cannot be 0 for EOF1 code. Equals 0 for legacy code.
	dataSize uint16 // Size of data section. Equals 0 if data section is absent in EOF1 code. Equals 0 for legacy code.
}

// hasEOFByte returns true if code starts with 0xEF byte
func hasEOFByte(code []byte) bool {
	return len(code) != 0 && code[0] == eofFormatByte
}

// hasEOFMagic returns true if code starts with magic defined by EIP-3540
func hasEOFMagic(code []byte) bool {
	return len(eofMagic) <= len(code) && bytes.Equal(eofMagic, code[0:len(eofMagic)])
}

// isEOFVersion1 returns true if the code's version byte equals eof1Version. It
// does not verify the EOF magic is valid.
func isEOFVersion1(code []byte) bool {
	return versionOffset < len(code) && code[versionOffset] == byte(eof1Version)
}

// readSectionSize returns the size of the section at the offset i.
func readSectionSize(code []byte, i int) (uint16, error) {
	if len(code) < i+2 {
		return 0, fmt.Errorf("section size missing")
	}
	return binary.BigEndian.Uint16(code[i : i+2]), nil
}

// readEOF1Header parses EOF1-formatted code header
func readEOF1Header(code []byte) (EOF1Header, error) {
	if !hasEOFMagic(code) || !isEOFVersion1(code) {
		return EOF1Header{}, ErrEOF1InvalidVersion
	}
	var (
		i        = sectionHeaderStart
		err      error
		codeRead int
		dataRead int
		header   EOF1Header
	)
outer:
	for i < len(code) {
		switch headerSection(code[i]) {
		case kindTerminator:
			i += 1
			break outer
		case kindCode:
			// Only 1 code section is allowed.
			if codeRead != 0 {
				return EOF1Header{}, ErrEOF1MultipleCodeSections
			}
			// Size must be present.
			if header.codeSize, err = readSectionSize(code, i+1); err != nil {
				return EOF1Header{}, ErrEOF1CodeSectionSizeMissing
			}
			// Size must not be 0.
			if header.codeSize == 0 {
				return EOF1Header{}, ErrEOF1EmptyCodeSection
			}
			i += 3
			codeRead += 1
		case kindData:
			// Data section is allowed only after code section.
			if codeRead == 0 {
				return EOF1Header{}, ErrEOF1DataSectionBeforeCodeSection
			}
			// Only 1 data section is allowed.
			if dataRead != 0 {
				return EOF1Header{}, ErrEOF1MultipleDataSections
			}
			// Size must be present.
			if header.dataSize, err = readSectionSize(code, i+1); err != nil {
				return EOF1Header{}, ErrEOF1DataSectionSizeMissing
			}
			// Data section size must not be 0.
			if header.dataSize == 0 {
				return EOF1Header{}, ErrEOF1EmptyDataSection
			}
			i += 3
			dataRead += 1
		default:
			return EOF1Header{}, ErrEOF1UnknownSection
		}
	}
	// 1 code section is required.
	if codeRead != 1 {
		return EOF1Header{}, ErrEOF1CodeSectionMissing
	}
	// Declared section sizes must correspond to real size (trailing bytes are not allowed.)
	if i+int(header.codeSize)+int(header.dataSize) != len(code) {
		return EOF1Header{}, ErrEOF1InvalidTotalSize
	}

	return header, nil
}

// validateInstructions checks that there're no undefined instructions and code ends with a terminating instruction
func validateInstructions(code []byte, header *EOF1Header, jumpTable *JumpTable) error {
	var (
		i        = header.CodeBeginOffset()
		end      = header.CodeEndOffset()
		analysis = codeBitmap(code[i : end-1])
		opcode   OpCode
	)
	for i < end {
		opcode = OpCode(code[i])
		if jumpTable[opcode].undefined {
			return ErrEOF1UndefinedInstruction
		}
		if opcode >= PUSH1 && opcode <= PUSH32 {
			i += uint64(opcode) - uint64(PUSH1) + 1
		}
		if opcode == RJUMP || opcode == RJUMPI {
			var arg int16
			// Read immediate argument.
			if err := binary.Read(bytes.NewReader(code[i+1:]), binary.BigEndian, &arg); err != nil {
				return ErrEOF1InvalidRelativeOffset
			}
			// Check if offfset points to out-of-bounds code
			// location.
			if (arg < 0 && i+3 < uint64(arg)) || (arg > 0 && end < i+3+uint64(arg)) {
				return ErrEOF1InvalidRelativeOffset
			}
			// Check if offset points to non-code segment.
			pos := uint64(int64(i+3) + int64(arg))
			if !analysis.codeSegment(pos - header.CodeBeginOffset()) {
				return ErrEOF1InvalidRelativeOffset
			}
			i += 2
		}
		i += 1
	}
	if !opcode.isTerminating() {
		return ErrEOF1TerminatingInstructionMissing
	}
	return nil
}

// validateEOF returns true if code has valid format and code section
func validateEOF(code []byte, jumpTable *JumpTable) (EOF1Header, error) {
	header, err := readEOF1Header(code)
	if err != nil {
		return EOF1Header{}, err
	}
	err = validateInstructions(code, &header, jumpTable)
	if err != nil {
		return EOF1Header{}, err
	}
	return header, nil
}

// readValidEOF1Header parses EOF1-formatted code header, assuming that it is already validated
func readValidEOF1Header(code []byte) EOF1Header {
	var header EOF1Header
	codeSizeOffset := sectionHeaderStart + 1
	header.codeSize = binary.BigEndian.Uint16(code[codeSizeOffset : codeSizeOffset+2])
	if code[codeSizeOffset+2] == 2 {
		dataSizeOffset := codeSizeOffset + 3
		header.dataSize = binary.BigEndian.Uint16(code[dataSizeOffset : dataSizeOffset+2])
	}
	return header
}

// CodeBeginOffset returns starting offset of the code section
func (header *EOF1Header) CodeBeginOffset() uint64 {
	if header.dataSize == 0 {
		// len(magic) + version + code_section_id + code_section_size + terminator
		return 7
	}
	// len(magic) + version + code_section_id + code_section_size + data_section_id + data_section_size + terminator
	return 10
}

// CodeEndOffset returns offset of the code section end
func (header *EOF1Header) CodeEndOffset() uint64 {
	return header.CodeBeginOffset() + uint64(header.codeSize)
}
