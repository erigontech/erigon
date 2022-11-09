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
	kindType       headerSection = 3

	versionOffset      int = 2
	sectionHeaderStart int = 3
)

var eofMagic []byte = []byte{0xEF, 0x00}

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

type EOF1Header struct {
	typeSize uint16   // Size of type section. Must be 2 * n bytes, where n is number of code sections.
	codeSize []uint16 // Size of code sections. Cannot be 0 for EOF1 code. Equals 0 for legacy code.
	dataSize uint16   // Size of data section. Equals 0 if data section is absent in EOF1 code. Equals 0 for legacy code.
}

func NewEOF1Header(code []byte, jt *JumpTable, validated bool) (EOF1Header, error) {
	if validated {
		return readEOF1Header(code), nil
	}

	header, err := readUncheckedEOF1Header(code)
	if err != nil {
		return EOF1Header{}, err
	}
	i := header.Size()
	if header.typeSize != 0 {
		// TODO(matt): validate type info
		i += int(header.typeSize)
	}
	// Validate each code section.
	for _, size := range header.codeSize {
		err = validateInstructions(code[i:i+int(size)], &header, jt)
		if err != nil {
			return EOF1Header{}, err
		}
		i += int(size)
	}
	return header, nil
}

// Size returns the total size of the EOF1 header.
func (h *EOF1Header) Size() int {
	var (
		typeHeaderSize = 0
		codeHeaderSize = len(h.codeSize) * 3
		dataHeaderSize = 0
	)
	if h.typeSize != 0 {
		typeHeaderSize = 3
	}
	if h.dataSize != 0 {
		dataHeaderSize = 3
	}
	// len(magic) + version + typeHeader + codeHeader + dataHeader + terminator
	return 2 + 1 + typeHeaderSize + codeHeaderSize + dataHeaderSize + 1
}

// readUncheckedEOF1Header attempts to parse an EOF1-formatted code header.
func readUncheckedEOF1Header(code []byte) (EOF1Header, error) {
	if !hasEOFMagic(code) || !isEOFVersion1(code) {
		return EOF1Header{}, ErrEOF1InvalidVersion
	}
	var (
		i        = sectionHeaderStart
		err      error
		codeRead int
		dataRead int
		typeRead int
		header   EOF1Header
		codeSize int
	)
outer:
	for i < len(code) {
		switch headerSection(code[i]) {
		case kindTerminator:
			i += 1
			break outer
		case kindType:
			// Type section header must be read first.
			if codeRead != 0 || dataRead != 0 {
				return EOF1Header{}, ErrEOF1TypeSectionHeaderAfterOthers
			}
			// Only 1 type section is allowed.
			if typeRead != 0 {
				return EOF1Header{}, ErrEOF1MultipleTypeSections
			}
			// Size must be present.
			if header.typeSize, err = readSectionSize(code, i+1); err != nil {
				return EOF1Header{}, ErrEOF1TypeSectionSizeMissing
			}
			// Type section size must not be 0.
			if header.typeSize == 0 {
				return EOF1Header{}, ErrEOF1EmptyDataSection
			}
			typeRead += 1
		case kindCode:
			// Size must be present.
			size, err := readSectionSize(code, i+1)
			if err != nil {
				return EOF1Header{}, ErrEOF1CodeSectionSizeMissing
			}
			// Size must not be 0.
			if size == 0 {
				return EOF1Header{}, ErrEOF1EmptyCodeSection
			}
			header.codeSize = append(header.codeSize, size)
			codeSize += int(size)
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
			dataRead += 1
		default:
			return EOF1Header{}, ErrEOF1UnknownSection
		}
		i += 3
	}
	// 1 code section is required.
	if codeRead < 1 {
		return EOF1Header{}, ErrEOF1CodeSectionMissing
	}
	// 1024 max code sections.
	if len(header.codeSize) > 1024 {
		return EOF1Header{}, ErrEOF1CodeSectionOverflow
	}
	// Must have type section if more than one code section.
	if len(header.codeSize) > 1 && header.typeSize == 0 {
		return EOF1Header{}, ErrEOF1TypeSectionMissing
	}
	// Declared section sizes must correspond to real size (trailing bytes are not allowed.)
	if i+int(header.typeSize)+codeSize+int(header.dataSize) != len(code) {
		return EOF1Header{}, ErrEOF1InvalidTotalSize
	}
	return header, nil
}

// readEOF1Header parses EOF1-formatted code header, assuming that it is already validated.
func readEOF1Header(code []byte) EOF1Header {
	var (
		header       EOF1Header
		i            = sectionHeaderStart
		codeSections = uint16(1)
	)
	// Try to read type section.
	if code[i] == byte(kindType) {
		size, _ := readSectionSize(code, i+1)
		header.typeSize = size
		codeSections = size / 2
		i += 3
	}
	i += 1
	// Read code sections.
	for j := 0; j < int(codeSections); j++ {
		size := binary.BigEndian.Uint16(code[i+3*j : i+3*j+2])
		header.codeSize = append(header.codeSize, size)
	}
	i += int(2 * codeSections)
	// Try to read data section.
	if code[i] == byte(kindData) {
		header.dataSize = binary.BigEndian.Uint16(code[i+1 : i+3])
	}
	return header
}

// validateInstructions checks that there're no undefined instructions and code ends with a terminating instruction
func validateInstructions(code []byte, header *EOF1Header, jumpTable *JumpTable) error {
	var (
		i        = 0
		analysis = codeBitmap(code)
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
			if analysis[uint64(pos)/64]&(uint64(1)<<(uint64(pos)&63)) != 0 {
				return ErrEOF1InvalidRelativeOffset
			}
			i += 3
		case opcode == CALLF:
			var arg int16
			// Read immediate argument.
			if err := binary.Read(bytes.NewReader(code[i+1:]), binary.BigEndian, &arg); err != nil {
				return fmt.Errorf("%v: %v", ErrEOF1InvalidCallfSection, err)
			}
			if int(arg) >= len(header.codeSize) {
				return fmt.Errorf("%v: want section %v, but only have %d sections", ErrEOF1InvalidCallfSection, arg, len(header.codeSize))
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

// readSectionSize returns the size of the section at the offset i.
func readSectionSize(code []byte, i int) (uint16, error) {
	if len(code) < i+2 {
		return 0, fmt.Errorf("section size missing")
	}
	return binary.BigEndian.Uint16(code[i : i+2]), nil
}

type Annotation struct {
	input  uint8
	output uint8
}

type EOF1Container struct {
	header EOF1Header
	types  []Annotation // type defs
	code   []uint64     // code start offsets
	data   uint64       // data start offset
}

func NewEOF1Container(code []byte, jt *JumpTable, validated bool) (EOF1Container, error) {
	var (
		container EOF1Container
		err       error
	)
	// If the code has been validated (e.g. during deployment), skip the
	// full validation.
	container.header, err = NewEOF1Header(code, jt, validated)
	if err != nil {
		return EOF1Container{}, err
	}

	// Set index to first byte after EOF header.
	idx := container.header.Size()

	// Read type section if it exists.
	if typeSize := container.header.typeSize; typeSize != 0 {
		container.types = readTypeSection(code[idx : idx+int(typeSize)])
		idx += int(typeSize)
	}
	// Calculate starting offset for each code section.
	for _, size := range container.header.codeSize {
		container.code = append(container.code, uint64(idx))
		idx += int(size)
	}
	// Set data offset.
	container.data = uint64(idx)
	return container, nil
}

// readTypeSection parses an EOF type section.
func readTypeSection(code []byte) (out []Annotation) {
	for i := 0; i < len(code); i += 2 {
		sig := Annotation{
			input:  code[i],
			output: code[i+1],
		}
		out = append(out, sig)
	}
	return
}
