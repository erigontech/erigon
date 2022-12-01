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

// eofMagic is the prefix denoting a contract is an EOF contract.
var eofMagic []byte = []byte{0xEF, 0x00}

const (
	eofFormatByte byte = 0xEF
	eof1Version   int  = 1
)

const (
	kindTerminator int = iota
	kindCode
	kindData
	kindType
)

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

// Annotation represents an EOF v1 function signature.
type Annotation struct {
	Input  uint8 // number of stack inputs
	Output uint8 // number of stack outputs
}

// ParseEOF1Container parses an EOF v1 container from the provided byte slice.
//
// This function ensures that the container is well-formed (e.g. size values
// are correct, sections are ordered correctly, etc), but it does not perform
// code validation on the container.
func ParseEOF1Container(b []byte) (*EOF1Container, error) {
	var c EOF1Container
	idx, err := c.parseHeader(b)
	if err != nil {
		return nil, err
	}
	// Read type section if it exists.
	if ts := int(c.typeSize); ts != 0 {
		c.types = parseTypeSection(b[idx : idx+ts])
		if c.types[0].Input != 0 || c.types[0].Output != 0 {
			return nil, ErrEOF1TypeSectionFirstEntryNonZero
		}
		idx += ts
	}
	// Calculate starting offset for each code section.
	for _, size := range c.codeSize {
		c.codeOffsets = append(c.codeOffsets, uint64(idx))
		idx += int(size)
	}
	// Set data offset.
	c.dataOffset = uint64(idx)
	c.data = b
	return &c, nil
}

// ValidateCode performs the EOF v1 code validation on the container.
func (c *EOF1Container) ValidateCode(jt *JumpTable) error {
	for i, start := range c.codeOffsets {
		end := start + uint64(c.codeSize[i])
		if err := validateCode(c.data[start:end], len(c.codeSize), jt); err != nil {
			return err
		}
	}
	return nil
}

// CodeAt returns the code section at the specified index.
func (c *EOF1Container) CodeAt(section uint64) []byte {
	if len(c.codeOffsets) <= int(section) {
		return nil
	}
	idx := c.codeOffsets[section]
	size := uint64(c.codeSize[section])
	return c.data[idx : idx+size]
}

// parseEOF1Header attempts to parse an EOF1-formatted code header.
func (c *EOF1Container) parseHeader(b []byte) (int, error) {
	if !hasEOFMagic(b) || !isEOFVersion1(b) {
		return 0, ErrEOF1InvalidVersion
	}
	var (
		i             = 3
		err           error
		typeRead      int
		codeRead      int
		dataRead      int
		totalCodeSize int
	)
outer:
	for i < len(b) {
		switch int(b[i]) {
		case kindTerminator:
			i += 1
			break outer
		case kindType:
			// Type section header must be read first.
			if codeRead != 0 || dataRead != 0 {
				return i, ErrEOF1TypeSectionHeaderAfterOthers
			}
			// Only 1 type section is allowed.
			if typeRead != 0 {
				return i, ErrEOF1TypeSectionDuplicate
			}
			// Size must be present.
			if c.typeSize, err = parseSectionSize(b, i+1); err != nil {
				return i, ErrEOF1TypeSectionSizeMissing
			}
			// Type section size must not be 0.
			if c.typeSize == 0 {
				return i, ErrEOF1EmptyDataSection
			}
			typeRead += 1
		case kindCode:
			// Size must be present.
			var size uint16
			size, err = parseSectionSize(b, i+1)
			if err != nil {
				return i, ErrEOF1CodeSectionSizeMissing
			}
			// Size must not be 0.
			if size == 0 {
				return i, ErrEOF1EmptyCodeSection
			}
			c.codeSize = append(c.codeSize, size)
			totalCodeSize += int(size)
			codeRead += 1
		case kindData:
			// Data section is allowed only after code section.
			if codeRead == 0 {
				return i, ErrEOF1DataSectionBeforeCodeSection
			}
			// Only 1 data section is allowed.
			if dataRead != 0 {
				return i, ErrEOF1MultipleDataSections
			}
			// Size must be present.
			if c.dataSize, err = parseSectionSize(b, i+1); err != nil {
				return i, ErrEOF1DataSectionSizeMissing

			}
			// Data section size must not be 0.
			if c.dataSize == 0 {
				return i, ErrEOF1EmptyDataSection
			}
			dataRead += 1
		default:
			return i, ErrEOF1UnknownSection
		}
		i += 3
	}
	// 1 code section is required.
	if codeRead < 1 {
		return i, ErrEOF1CodeSectionMissing
	}
	// 1024 max code sections.
	if len(c.codeSize) > 1024 {
		return i, ErrEOF1CodeSectionOverflow
	}
	// Must have type section if more than one code section.
	if len(c.codeSize) > 1 && c.typeSize == 0 {
		return i, ErrEOF1TypeSectionMissing
	}
	// If type section, ensure type section size is 2n the number of code sections.
	if c.typeSize != 0 && len(c.codeSize) != int(c.typeSize/2) {
		return i, ErrEOF1TypeSectionInvalidSize
	}
	// Declared section sizes must correspond to real size (trailing bytes are not allowed.)
	if i+int(c.typeSize)+totalCodeSize+int(c.dataSize) != len(b) {
		return i, ErrEOF1InvalidTotalSize
	}
	return i, nil
}

// parseSectionSize returns the size of the section at the offset i.
func parseSectionSize(code []byte, i int) (uint16, error) {
	if len(code) < i+2 {
		return 0, fmt.Errorf("section size missing")
	}
	return binary.BigEndian.Uint16(code[i : i+2]), nil
}

// parseTypeSection parses an EOF type section.
func parseTypeSection(code []byte) (out []Annotation) {
	for i := 0; i < len(code)-1; i += 2 {
		sig := Annotation{
			Input:  code[i],
			Output: code[i+1],
		}
		out = append(out, sig)
	}
	return
}

// parseArg returns the int16 located at b[0:2].
func parseArg(b []byte) (int16, error) {
	if len(b) < 2 {
		return 0, fmt.Errorf("argument missing")
	}
	arg := int16(b[0]) << 8
	arg += int16(b[1])
	return arg, nil
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
	return 2 < len(code) && code[2] == byte(eof1Version)
}
