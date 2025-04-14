// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
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
	ErrAuxDataTooLarge          = errors.New("auxdata too large")
	ErrAuxDataDecrease          = errors.New("auxdata decrease")
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
