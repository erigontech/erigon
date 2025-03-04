// Copyright 2022 The go-ethereum Authors
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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	offsetVersion   = 2
	offsetTypesKind = 3
	offsetCodeKind  = 6

	kindTypes     = 1
	kindCode      = 2
	kindContainer = 3
	kindData      = 4

	eofFormatByte = 0xef
	eof1Version   = 1

	maxInputItems        = 127
	maxOutputItems       = 127
	maxStackHeight       = 1023
	maxContainerSections = 256

	nonReturningFunction = 0x80

	stackSizeLimit = 1024

	maxInitCodeSize = 49152
)

// TODO(racytech): remove unnecessary errors and add matched errors, some errors do not match the logic
// sort the errors by the logic
var (
	ErrIncompleteEOF            = errors.New("incomplete EOF code")
	ErrInvalidMagic             = errors.New("invalid magic")
	ErrInvalidVersion           = errors.New("invalid version")
	ErrMissingTypeHeader        = errors.New("missing type header")
	ErrInvalidTypeSize          = errors.New("invalid type section size")
	ErrMissingCodeHeader        = errors.New("missing code header")
	ErrInvalidCodeHeader        = errors.New("invalid code header")
	ErrInvalidCodeSize          = errors.New("invalid code size")
	ErrMissingDataHeader        = errors.New("missing data header")
	ErrMissingTerminator        = errors.New("missing header terminator")
	ErrMissingTypesInput        = errors.New("missing types input")
	ErrMissingTypesOutput       = errors.New("missing types output")
	ErrMissingMaxStackHeight    = errors.New("missing types max_stack_height")
	ErrTooManyInputs            = errors.New("invalid type content, too many inputs")
	ErrTooManyOutputs           = errors.New("invalid type content, too many outputs")
	ErrInvalidFirstSectionType  = errors.New("invalid section 0 type, input should be 0 and output should 128")
	ErrTooLargeMaxStackHeight   = errors.New("invalid type content, max stack height exceeds limit")
	ErrInvalidContainerSize     = errors.New("invalid container size")
	ErrInvalidMemoryAccess      = errors.New("invalid memory access")
	ErrTooLargeByteCode         = errors.New("bytecode exceeds allowed limit")
	ErrZeroSizeContainerSection = errors.New("number of container sections may not be 0")
	ErrTooManyContainerSections = errors.New("number of container sections must not exceed 256")
	ErrZeroContainerSize        = errors.New("container size may not be 0")
	ErrInvalidSectionCount      = errors.New("invalid section count")
	ErrReturnStackExceeded      = errors.New("return stack limit reached")
	ErrLegacyCode               = errors.New("invalid code: EOF contract must not deploy legacy code")
	ErrInvalidCode              = errors.New("invalid code: must not begin with 0xef")
	ErrInvalidEOF               = errors.New("invalid eof")
	ErrInvalidEOFInitcode       = errors.New("invalid eof initcode")
)

var (
	ErrUndefinedInstruction     = errors.New("undefined instrustion")
	ErrTruncatedImmediate       = errors.New("truncated immediate")
	ErrInvalidSectionArgument   = errors.New("invalid section argument")
	ErrInvalidContainerArgument = errors.New("invalid container argument")
	ErrInvalidJumpDest          = errors.New("invalid jump destination")
	ErrConflictingStack         = errors.New("conflicting stack height")
	ErrInvalidBranchCount       = errors.New("invalid number of branches in jump table")
	ErrInvalidOutputs           = errors.New("invalid number of outputs")
	ErrInvalidMaxStackHeight    = errors.New("invalid max stack height")
	ErrInvalidCodeTermination   = errors.New("invalid code termination")
	ErrUnreachableCode          = errors.New("unreachable code")
	ErrInvalidDataLoadN         = errors.New("invalid DATALOADN index")
	ErrEOFStackOverflow         = errors.New("stack overflow")
	ErrEOFStackUnderflow        = errors.New("stack underflow")
	ErrJUMPFOutputs             = errors.New("current secion outputs less then target section outputs")
	ErrStackHeightHigher        = errors.New("stack height higher then outputs required")
	ErrNoTerminalInstruction    = errors.New("expected terminal instruction")
	ErrStackHeightMismatch      = errors.New("stack height mismatch")
	ErrCALLFtoNonReturning      = errors.New("op CALLF to non returning function")
	ErrInvalidNonReturning      = errors.New("declared returning code section does not return")
	ErrInvalidRjumpDest         = errors.New("invalid relative jump")
)

var (
	ErrIncompatibleContainer           = errors.New("INCOMPATIBLE_CONTAINER_KIND")
	ErrAmbiguousContainer              = errors.New("AMBIGUOUS_CONTAINER_KIND")
	ErrInvalidSectionsSize             = errors.New("INVALID_SECTION_BODIES_SIZE")
	ErrOrphanSubContainer              = errors.New("UNREFERENCED_SUBCONTAINER")
	ErrTopLevelTruncated               = errors.New("TOPLEVEL_CONTAINER_TRUNCATED")
	ErrEOFCreateWithTruncatedContainer = errors.New("EOF_CREATE_WITH_TRUNCATED_CONTAINER")
	ErrUnreachableCodeSections         = errors.New("UNREACHABLE_CODE_SECTIONS")
)

var eofMagic = []byte{0xef, 0x00}

// HasEOFByte returns true if code starts with 0xEF byte
func HasEOFByte(code []byte) bool {
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

// Container is an EOF container object.
type Container struct {
	Types         []*FunctionMetadata
	Code          [][]byte
	SubContainers [][]byte
	Data          []byte
	DataSize      int // might be more than len(Data)
}

// FunctionMetadata is an EOF function signature.
type FunctionMetadata struct {
	Inputs         uint8
	Outputs        uint8
	MaxStackHeight uint16
}

// MarshalBinary encodes an EOF container into binary format.
func (c *Container) MarshalBinary() []byte {
	// Build EOF prefix.
	b := make([]byte, 2)
	copy(b, eofMagic)
	b = append(b, eof1Version)

	// Write section headers.
	b = append(b, kindTypes)
	b = binary.BigEndian.AppendUint16(b, uint16(len(c.Types)*4))
	b = append(b, kindCode)
	b = binary.BigEndian.AppendUint16(b, uint16(len(c.Code)))
	for _, code := range c.Code {
		b = binary.BigEndian.AppendUint16(b, uint16(len(code)))
	}
	var encodedContainer [][]byte
	if len(c.SubContainers) != 0 {
		b = append(b, kindContainer)
		b = binary.BigEndian.AppendUint16(b, uint16(len(c.SubContainers)))
		for _, section := range c.SubContainers {
			b = binary.BigEndian.AppendUint16(b, uint16(len(section)))
			encodedContainer = append(encodedContainer, section)
		}
	}
	b = append(b, kindData)
	b = binary.BigEndian.AppendUint16(b, uint16(len(c.Data)))
	b = append(b, 0) // terminator

	// Write section contents.
	for _, ty := range c.Types {
		b = append(b, []byte{ty.Inputs, ty.Outputs, byte(ty.MaxStackHeight >> 8), byte(ty.MaxStackHeight & 0x00ff)}...)
	}
	for _, code := range c.Code {
		b = append(b, code...)
	}
	for _, section := range encodedContainer {
		b = append(b, section...)
	}
	b = append(b, c.Data...)

	return b
}

// UnmarshalBinary decodes an EOF container.
func (c *Container) UnmarshalBinary(b []byte, isInitCode bool) error {
	// TODO(racytech): make sure this one is correct!
	fmt.Println("len(b) = ", len(b))
	if !hasEOFMagic(b) {
		return fmt.Errorf("%w: want %x", ErrInvalidMagic, eofMagic)
	}
	if len(b) < 14 {
		return ErrIncompleteEOF
	}
	if len(b) > maxInitCodeSize {
		return fmt.Errorf("%w: %v > %v", ErrTooLargeByteCode, len(b), maxInitCodeSize)
	}
	if !isEOFVersion1(b) {
		return fmt.Errorf("%w: have %d, want %d", ErrInvalidVersion, b[2], eof1Version)
	}

	var (
		kind, typesSize, dataSize int
		codeSizes                 []int
		containerSizes            []int
		err                       error
	)

	// Parse type section header.
	kind, typesSize, err = parseSection(b, offsetTypesKind)
	if err != nil {
		return err
	}
	if kind != kindTypes {
		return fmt.Errorf("%w: found section kind %x instead", ErrMissingTypeHeader, kind)
	}
	if typesSize < 4 || typesSize%4 != 0 {
		return fmt.Errorf("%w: type section size must be divisible by 4, have %d", ErrInvalidTypeSize, typesSize)
	}
	if typesSize/4 > 1024 {
		return fmt.Errorf("%w: type section must not exceed 4*1024, have %d", ErrInvalidTypeSize, typesSize)
	}

	// Parse code section header.
	kind, codeSizes, err = parseSectionList(b, offsetCodeKind)
	if err != nil {
		return err
	}
	if kind != kindCode {
		return fmt.Errorf("%w: found section kind %x instead", ErrMissingCodeHeader, kind)
	}
	if len(codeSizes) != typesSize/4 { // invalid code section count or type section count
		return fmt.Errorf("%w: mismatch of code sections count and type signatures, types %d, code %d", ErrInvalidSectionCount, typesSize/4, len(codeSizes))
	}

	// Parse optional container section header
	offsetContainerKind := offsetCodeKind + 2 + 2*len(codeSizes) + 1
	offsetDataKind := offsetContainerKind
	if offsetContainerKind < len(b) && b[offsetContainerKind] == kindContainer {
		kind, containerSizes, err = parseSectionList(b, offsetContainerKind)
		if err != nil {
			return err
		}
		if kind != kindContainer {
			panic("expected kind container, got something else")
		}
		if len(containerSizes) == 0 {
			return ErrZeroSizeContainerSection
		}
		if len(containerSizes) > 256 {
			return ErrTooManyContainerSections
		}
		offsetDataKind = offsetContainerKind + 1 + 2*len(containerSizes) + 2 // we have containers, add kind_byte + 2*len(container_sizes) + container_size (2-bytes)
	}

	// Parse data section header.
	kind, dataSize, err = parseSection(b, offsetDataKind)
	if err != nil {
		return err
	}
	if kind != kindData {
		return fmt.Errorf("%w: found section %x instead", ErrMissingDataHeader, kind)
	}
	c.DataSize = dataSize

	// Check for terminator.
	offsetTerminator := offsetDataKind + 3
	if len(b) < offsetTerminator {
		return ErrMissingTerminator
	}
	if b[offsetTerminator] != 0 {
		return fmt.Errorf("%w: have %x", ErrMissingTerminator, b[offsetTerminator])
	}

	// Verify overall container size.
	expectedSize := offsetTerminator + typesSize + sum(codeSizes) + sum(containerSizes) + dataSize + 1
	if len(b) < expectedSize-dataSize {
		return fmt.Errorf("%w: have %d, want %d", ErrInvalidContainerSize, len(b), expectedSize)
	}
	// Only check that the expected size is not exceed on non-initcode
	if !isInitCode && len(b) > expectedSize {
		return fmt.Errorf("%w: have %d, want %d", ErrInvalidContainerSize, len(b), expectedSize)
	}

	// Parse types section.
	idx := offsetTerminator + 1
	var types []*FunctionMetadata

	// first, parse the first section and check if it meets the boundries
	i := 0
	typ := &FunctionMetadata{
		Inputs:         b[idx+i*4],
		Outputs:        b[idx+i*4+1],
		MaxStackHeight: binary.BigEndian.Uint16(b[idx+i*4+2:]),
	}
	if typ.Inputs != 0 || typ.Outputs != nonReturningFunction {
		return fmt.Errorf("%w: have %d, %d", ErrInvalidFirstSectionType, typ.Inputs, typ.Outputs)
	}
	if typ.MaxStackHeight > maxStackHeight {
		return fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, i, typ.MaxStackHeight)
	}
	types = append(types, typ)

	i = 1 // go to the next section
	for ; i < typesSize/4; i++ {
		typ := &FunctionMetadata{
			Inputs:         b[idx+i*4],
			Outputs:        b[idx+i*4+1],
			MaxStackHeight: binary.BigEndian.Uint16(b[idx+i*4+2:]),
		}

		if typ.Inputs > maxInputItems {
			return fmt.Errorf("%w for section %d: have %d", ErrTooManyInputs, i, typ.Inputs)
		}
		if typ.Outputs > maxOutputItems && typ.Outputs != nonReturningFunction {
			return fmt.Errorf("%w for section %d: have %d", ErrTooManyOutputs, i, typ.Outputs)
		}
		if typ.MaxStackHeight > maxStackHeight {
			return fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, i, typ.MaxStackHeight)
		}

		types = append(types, typ)
	}
	c.Types = types

	// Parse code sections.
	idx += typesSize
	code := make([][]byte, len(codeSizes))
	for i, size := range codeSizes {
		if size == 0 {
			return fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidCodeSize, i)
		}
		code[i] = b[idx : idx+size]
		idx += size
	}
	c.Code = code

	// Parse containers if any
	if len(containerSizes) != 0 {
		if len(containerSizes) > maxContainerSections {
			return fmt.Errorf("containers exceed allowed limit: %v: have %v", maxContainerSections, len(containerSizes))
		}
		containers := make([][]byte, len(containerSizes))
		for i, size := range containerSizes {
			if size == 0 || idx+size > len(b) {
				return fmt.Errorf("%w, container#: %d", ErrZeroContainerSize, i)
			}
			end := min(idx+size, len(b))
			containers[i] = b[idx:end]
			idx += size
		}
		c.SubContainers = containers
	}

	// Parse data section.
	end := len(b)
	if !isInitCode {
		end = min(idx+dataSize, len(b))
	}
	c.Data = b[idx:end]

	return nil
}

// ValidateCode validates each code section of the container against the EOF v1
// rule set.
func (c *Container) ValidateCode(jt *JumpTable) error {
	// for i, code := range c.Code {
	// 	// if err := validateCode(code, i, c.Types, jt, len(c.Data), len(c.SubContainers)); err != nil {
	// 	// 	return err
	// 	// }
	// }
	return nil
}

// parseSection decodes a (kind, size) pair from an EOF header.
func parseSection(b []byte, idx int) (kind, size int, err error) {
	if idx+3 >= len(b) {
		return 0, 0, io.ErrUnexpectedEOF
	}
	kind = int(b[idx])
	size = int(binary.BigEndian.Uint16(b[idx+1:]))
	return kind, size, nil
}

// parseSectionList decodes a (kind, len, []codeSize) section list from an EOF
// header.
func parseSectionList(b []byte, idx int) (kind int, list []int, err error) {
	if idx >= len(b) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	kind = int(b[idx])
	list, err = parseList(b, idx+1)
	if err != nil {
		return 0, nil, err
	}
	return kind, list, nil
}

// parseList decodes a list of uint16..
func parseList(b []byte, idx int) ([]int, error) {
	if len(b) < idx+2 {
		return nil, io.ErrUnexpectedEOF
	}
	count := binary.BigEndian.Uint16(b[idx:])
	if len(b) <= idx+2+int(count)*2 {
		return nil, io.ErrUnexpectedEOF
	}
	list := make([]int, count) // list of sizes
	for i := 0; i < int(count); i++ {
		list[i] = int(binary.BigEndian.Uint16(b[idx+2+2*i:]))
	}
	return list, nil
}

// parseUint16 parses a 16 bit BigEndian unsigned integer.
func parseUint16(b []byte) (int, error) {
	if len(b) < 2 {
		// return 0, io.ErrUnexpectedEOF
		panic("parseUint16: len(b) < 2") // TODO(racytech): undo this when done with tests
	}
	return int(binary.BigEndian.Uint16(b)), nil
}

// parseInt16 parses a 16 bit signed integer.
func parseInt16(b []byte) int {
	return int(int16(b[1]) | int16(b[0])<<8)
}

// sum computes the sum of a slice.
func sum(list []int) (s int) {
	for _, n := range list {
		s += n
	}
	return
}
