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

// Annotation represents an EOF v1 function signature.
type Annotation struct {
	Input  uint8 // number of stack inputs
	Output uint8 // number of stack outputs
}

// EOF1Container is an EOF v1 container object.
type EOF1Container struct {
	// header items
	typeSize uint16
	codeSize []uint16
	dataSize uint16

	// sections
	types       []Annotation
	codeOffsets []uint64
	dataOffset  uint64

	// backing data
	data []byte
}

// ParseEOF1Container parses an EOF v1 container from the provided byte slice.
func ParseEOF1Container(b []byte) (*EOF1Container, error) {
	var c EOF1Container
	err := c.UnmarshalBinary(b)
	return &c, err
}

// ParseEOF1Container parses and validates an EOF v1 container from the
// provided byte slice.
func ParseAndValidateEOF1Container(b []byte, jt *JumpTable) (*EOF1Container, error) {
	c, err := ParseEOF1Container(b)
	if err != nil {
		return nil, err
	}
	if err := c.ValidateCode(jt); err != nil {
		return nil, err
	}
	return c, nil
}

// UnmarshalBinary decodes an EOF v1 container.
//
// This function ensures that the container is well-formed (e.g. size values
// are correct, sections are ordered correctly, etc), but it does not perform
// code validation on the container.
func (c *EOF1Container) UnmarshalBinary(b []byte) error {
	if err := c.parseHeader(b); err != nil {
		return err
	}
	idx := c.HeaderSize()
	// Read type section if it exists.
	if typeSize := c.typeSize; typeSize != 0 {
		c.types = parseTypeSection(b[idx : idx+int(typeSize)])
		idx += int(typeSize)
	}
	// Calculate starting offset for each code section.
	for _, size := range c.codeSize {
		c.codeOffsets = append(c.codeOffsets, uint64(idx))
		idx += int(size)
	}
	// Set data offset.
	c.dataOffset = uint64(idx)
	c.data = b
	return nil
}

// HeaderSize returns the total size of the EOF1 header.
func (c *EOF1Container) HeaderSize() int {
	var (
		typeHeaderSize = 0
		codeHeaderSize = len(c.codeSize) * 3
		dataHeaderSize = 0
	)
	if c.typeSize != 0 {
		typeHeaderSize = 3
	}
	if c.dataSize != 0 {
		dataHeaderSize = 3
	}
	// len(magic) + version + typeHeader + codeHeader + dataHeader + terminator
	return 2 + 1 + typeHeaderSize + codeHeaderSize + dataHeaderSize + 1
}

// ValidateCode performs the EOF v1 code validation on the container.
func (c *EOF1Container) ValidateCode(jt *JumpTable) error {
	idx := c.HeaderSize()
	for _, size := range c.codeSize {
		if err := validateInstructions(c.data[idx:idx+int(size)], len(c.codeSize), jt); err != nil {
			return err
		}
		idx += int(size)
	}
	return nil
}

// parseEOF1Header attempts to parse an EOF1-formatted code header.
func (c *EOF1Container) parseHeader(b []byte) error {
	if !hasEOFMagic(b) || !isEOFVersion1(b) {
		return ErrEOF1InvalidVersion
	}
	var (
		i             = sectionHeaderStart
		err           error
		typeRead      int
		codeRead      int
		dataRead      int
		totalCodeSize int
	)
outer:
	for i < len(b) {
		switch headerSection(b[i]) {
		case kindTerminator:
			i += 1
			break outer
		case kindType:
			// Type section header must be read first.
			if codeRead != 0 || dataRead != 0 {
				return ErrEOF1TypeSectionHeaderAfterOthers
			}
			// Only 1 type section is allowed.
			if typeRead != 0 {
				return ErrEOF1MultipleTypeSections
			}
			// Size must be present.
			if c.typeSize, err = parseSectionSize(b, i+1); err != nil {
				return ErrEOF1TypeSectionSizeMissing
			}
			// Type section size must not be 0.
			if c.typeSize == 0 {
				return ErrEOF1EmptyDataSection
			}
			typeRead += 1
		case kindCode:
			// Size must be present.
			var size uint16
			size, err = parseSectionSize(b, i+1)
			if err != nil {
				return ErrEOF1CodeSectionSizeMissing
			}
			// Size must not be 0.
			if size == 0 {
				return ErrEOF1EmptyCodeSection
			}
			c.codeSize = append(c.codeSize, size)
			totalCodeSize += int(size)
			codeRead += 1
		case kindData:
			// Data section is allowed only after code section.
			if codeRead == 0 {
				return ErrEOF1DataSectionBeforeCodeSection
			}
			// Only 1 data section is allowed.
			if dataRead != 0 {
				return ErrEOF1MultipleDataSections
			}
			// Size must be present.
			if c.dataSize, err = parseSectionSize(b, i+1); err != nil {
				return ErrEOF1DataSectionSizeMissing

			}
			// Data section size must not be 0.
			if c.dataSize == 0 {
				return ErrEOF1EmptyDataSection
			}
			dataRead += 1
		default:
			return ErrEOF1UnknownSection
		}
		i += 3
	}
	// 1 code section is required.
	if codeRead < 1 {
		return ErrEOF1CodeSectionMissing
	}
	// 1024 max code sections.
	if len(c.codeSize) > 1024 {
		return ErrEOF1CodeSectionOverflow
	}
	// Must have type section if more than one code section.
	if len(c.codeSize) > 1 && c.typeSize == 0 {
		return ErrEOF1TypeSectionMissing
	}
	// Declared section sizes must correspond to real size (trailing bytes are not allowed.)
	if c.HeaderSize()+int(c.typeSize)+totalCodeSize+int(c.dataSize) != len(b) {
		return ErrEOF1InvalidTotalSize
	}
	return nil
}

// parseSectionSize returns the size of the section at the offset i.
func parseSectionSize(code []byte, i int) (uint16, error) {
	if len(code) < i+2 {
		return 0, fmt.Errorf("section size missing")
	}
	return binary.BigEndian.Uint16(code[i : i+2]), nil
}

// validateInstructions checks that there're no undefined instructions and code ends with a terminating instruction
func validateInstructions(code []byte, codeSections int, jumpTable *JumpTable) error {
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

// uncheckedParseContainer parses an EOF v1 container without performing
// validation.
func uncheckedParseContainer(code []byte) *EOF1Container {
	var c EOF1Container
	// If the code has been validated (e.g. during deployment), skip the
	// full validation.
	c.typeSize, c.codeSize, c.dataSize = parseHeaderUnchecked(code)

	// Set index to first byte after EOF header.
	idx := c.HeaderSize()

	// Read type section if it exists.
	if typeSize := c.typeSize; typeSize != 0 {
		c.types = parseTypeSection(code[idx : idx+int(typeSize)])
		idx += int(typeSize)
	}
	// Calculate starting offset for each code section.
	for _, size := range c.codeSize {
		c.codeOffsets = append(c.codeOffsets, uint64(idx))
		idx += int(size)
	}
	// Set data offset.
	c.dataOffset = uint64(idx)
	return &c
}

// parseHeaderUnchecked parses an EOF v1 header without performing validation.
func parseHeaderUnchecked(code []byte) (typeSize uint16, codeSize []uint16, dataSize uint16) {
	var (
		i            = sectionHeaderStart
		codeSections = uint16(1)
	)
	// Try to read type section.
	if code[i] == byte(kindType) {
		size, _ := parseSectionSize(code, i+1)
		typeSize = size
		codeSections = size / 2
		i += 3
	}
	i += 1
	// Read code sections.
	for j := 0; j < int(codeSections); j++ {
		size := binary.BigEndian.Uint16(code[i+3*j : i+3*j+2])
		codeSize = append(codeSize, size)
	}
	i += int(2 * codeSections)
	// Try to read data section.
	if code[i] == byte(kindData) {
		dataSize = binary.BigEndian.Uint16(code[i+1 : i+3])
	}
	return
}

// parseTypeSection parses an EOF type section.
func parseTypeSection(code []byte) (out []Annotation) {
	for i := 0; i < len(code); i += 2 {
		sig := Annotation{
			Input:  code[i],
			Output: code[i+1],
		}
		out = append(out, sig)
	}
	return
}
