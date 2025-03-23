package vm

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidEOFInitcode = errors.New("invalid eof initcode")
)

const (
	initcode byte = 0
	runtime  byte = 1

	kindTypes     byte = 1
	kindCode      byte = 2
	kindContainer byte = 3
	kindData      byte = 4

	maxInitCodeSize = 49152

	nonReturningFunction = 0x80
	maxStackHeight       = 1023

	maxInputItems  = 127
	maxOutputItems = 127

	stackSizeLimit = 1024
)

const ( // EOFv1 Opcodes
	RJUMP  OpCode = 0xe0
	RJUMPI OpCode = 0xe1
	RJUMPV OpCode = 0xe2

	CALLF OpCode = 0xe3
	RETF  OpCode = 0xe4
	JUMPF OpCode = 0xe5

	DUPN     OpCode = 0xe6
	SWAPN    OpCode = 0xe7
	EXCHANGE OpCode = 0xe8

	EOFCREATE  OpCode = 0xec
	RETURNCODE OpCode = 0xee

	DATALOAD  OpCode = 0xd0
	DATALOADN OpCode = 0xd1
	DATASIZE  OpCode = 0xd2
	DATACOPY  OpCode = 0xd3

	RETURNDATALOAD  OpCode = 0xf7
	EXTCALL         OpCode = 0xf8
	EXTDELEGATECALL OpCode = 0xf9
	EXTSTATICCALL   OpCode = 0xfb
)

func isEOFcode(code []byte) bool {
	return len(code) > 1 && code[0] == 0xEF && code[1] == 0x00
}

func isSupportedVersion(version byte) bool {
	return version == 1 // || version == 2 etc.
}

type eofHeader struct {
	version          byte
	typesOffset      uint16
	codeSizes        []uint16
	codeOffsets      []uint16
	containerSizes   []uint16
	containerOffsets []uint16
	dataSize         uint16
	dataSizePos      uint16
	dataOffset       uint16
}

func ParseEOFHeader(eofCode []byte, jt *JumpTable, containerKind byte, validate bool, depth int) (*eofHeader, error) {

	header := &eofHeader{}
	if len(eofCode) < 14 {
		return nil, fmt.Errorf("EOFException.CODE_TOO_SHORT")
	}
	if len(eofCode) > maxInitCodeSize {
		return nil, fmt.Errorf("EOFException.CONTAINER_SIZE_ABOVE_LIMIT")
	}
	if !isEOFcode(eofCode) { // additional check for recursive container validations
		return nil, fmt.Errorf("EOFException.INVALID_MAGIC")
	}

	var offset uint16 = 2
	header.version = eofCode[offset]
	if !isSupportedVersion(header.version) {
		return nil, fmt.Errorf("EOFException.INVALID_VERSION")
	}
	offset++ // version

	if eofCode[offset] != kindTypes {
		return nil, fmt.Errorf("EOFException.MISSING_TYPE_HEADER")
	}
	offset++ // kind types

	typesSizes := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	if typesSizes < 4 || typesSizes%4 != 0 {
		return nil, fmt.Errorf("EOFException.INVALID_TYPE_SECTION_SIZE")
	}
	if typesSizes/4 > 1024 {
		return nil, fmt.Errorf("EOFException.TOO_MANY_CODE_SECTIONS")
	}
	offset += 2 // type sizes

	if eofCode[offset] != kindCode {
		return nil, fmt.Errorf("EOFException.MISSING_CODE_HEADER|EOFException.UNEXPECTED_HEADER_KIND")
	}
	offset++ // kind code

	numCodeSections := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	offset += 2 // num code sections

	if numCodeSections == 0 {
		return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE|EOFException.INCOMPLETE_SECTION_NUMBER")
	}
	if numCodeSections != typesSizes/4 {
		return nil, fmt.Errorf("EOFException.INVALID_TYPE_SECTION_SIZE|EOFException.INVALID_SECTION_BODIES_SIZE")
	}
	// if numCodeSections > 1024 { // no need to check this
	// 	return nil, fmt.Errorf("EOF code sections == 0")
	// }

	for i := uint16(0); i < numCodeSections; i++ {
		if offset+1 >= uint16(len(eofCode)) {
			return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
		}
		size := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
		if size == 0 {
			return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
		}
		header.codeSizes = append(header.codeSizes, size)
		offset += 2
	} // code sizes
	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}

	if eofCode[offset] != kindContainer && eofCode[offset] != kindData {
		return nil, fmt.Errorf("EOFException.MISSING_DATA_SECTION|EOFException.UNEXPECTED_HEADER_KIND")
	}

	if eofCode[offset] == kindContainer {
		offset++
		numContainerSections := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
		offset += 2
		if numContainerSections > 256 {
			return nil, fmt.Errorf("EOFException.TOO_MANY_CONTAINERS")
		}
		for i := uint16(0); i < numContainerSections; i++ {
			if offset+1 >= uint16(len(eofCode)) {
				return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
			}
			size := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
			if size == 0 {
				return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
			}
			header.containerSizes = append(header.containerSizes, size)
			offset += 2
		} // container sizes

		if len(header.containerSizes) == 0 {
			return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
		}
	}
	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}
	// skip check for data section, already did above
	if eofCode[offset] != kindData {
		return nil, fmt.Errorf("EOFException.MISSING_DATA_SECTION|EOFException.UNEXPECTED_HEADER_KIND")
	}

	offset++
	if offset+1 >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}

	header.dataSizePos = offset
	header.dataSize = uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	offset += 2

	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}
	if eofCode[offset] != 0 {
		return nil, fmt.Errorf("EOFException.MISSING_TERMINATOR")
	}
	offset++ // terminator

	header.typesOffset = offset

	offset += typesSizes

	for i := uint16(0); i < numCodeSections; i++ {
		header.codeOffsets = append(header.codeOffsets, offset)
		offset += header.codeSizes[i]
	}

	if len(header.containerSizes) > 0 { // do we need this check?
		for i := range header.containerSizes {
			header.containerOffsets = append(header.containerOffsets, offset)
			offset += header.containerSizes[i]
		}
	}

	if validate {
		var err error
		referencedByEofCreate := make([]bool, len(header.containerSizes))
		referencedByReturnCode := make([]bool, len(header.containerSizes))

		// validate body
		// check if remaining bytes are enough for the body
		if err := validateBody(eofCode, header, containerKind, depth); err != nil {
			return nil, err
		}

		// validate types
		if offset, err = validateTypes(eofCode, header); err != nil {
			return nil, err
		}
		// validate code
		if err = validateCode(eofCode, header, offset, containerKind, jt, &referencedByEofCreate, &referencedByReturnCode); err != nil {
			return nil, err
		}

		for i := range header.codeSizes {
			offset += header.codeSizes[i]
		}

		// validate containers
		for i := range header.containerSizes {
			if _, err = validateContainer(eofCode, header, offset, i, referencedByEofCreate[i], referencedByReturnCode[i], jt, depth); err != nil {
				return nil, err
			}
			offset += header.containerSizes[i]
		}
	}

	header.dataOffset = offset

	return header, nil
}

func validateBody(code []byte, header *eofHeader, containerKind byte, depth int) error {

	offset := int(header.typesOffset) + 4*len(header.codeSizes)
	for i := range header.codeSizes {
		offset += int(header.codeSizes[i])
	}
	for i := range header.containerSizes {
		offset += int(header.containerSizes[i])
	}
	if offset > len(code) {
		return fmt.Errorf("EOFException.INVALID_SECTION_BODIES_SIZE")
	}
	// offset here is data offset
	if offset == len(code) &&
		header.dataSize > 0 && depth == 0 /* top level container */ {
		return fmt.Errorf("EOFException.TOPLEVEL_CONTAINER_TRUNCATED")
	}
	if offset+int(header.dataSize) > len(code) {
		if depth == 0 {
			return fmt.Errorf("EOFException.TOPLEVEL_CONTAINER_TRUNCATED")
		}
		if containerKind == initcode {
			return fmt.Errorf("EOFException.EOFCREATE_WITH_TRUNCATED_CONTAINER")
		}
	}

	if offset+int(header.dataSize) < len(code) {
		return fmt.Errorf("EOFException.INVALID_SECTION_BODIES_SIZE")
	}

	return nil
}

func validateTypes(code []byte, header *eofHeader) (uint16, error) {

	offset := header.typesOffset

	inputs := code[offset] // TODO: check if inputs okay?
	if inputs != 0 {
		return 0, fmt.Errorf("EOFException.INVALID_FIRST_SECTION_TYPE")
	}
	offset++

	outputs := code[offset]
	if outputs != nonReturningFunction {
		return 0, fmt.Errorf("EOFException.INVALID_FIRST_SECTION_TYPE")
	}
	offset++

	stackHeight := uint16(code[offset])<<8 | uint16(code[offset+1])
	if stackHeight > maxStackHeight {
		return 0, fmt.Errorf("EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT")
	}
	offset += 2

	for offset < header.codeOffsets[0] {
		inputs = code[offset]
		if inputs > maxInputItems {
			return 0, fmt.Errorf("EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT")
		}
		offset++

		outputs = code[offset]
		if outputs > maxOutputItems && outputs != nonReturningFunction {
			return 0, fmt.Errorf("EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT")
		}
		offset++

		stackHeight = uint16(code[offset])<<8 | uint16(code[offset+1])
		if stackHeight > maxStackHeight {
			return 0, fmt.Errorf("EOF type section stack height > maxStackHeight")
		}
		offset += 2
	}

	return offset, nil
}

func validateCode(eofCode []byte, header *eofHeader, offset uint16, containerKind byte, jt *JumpTable, referencedByEofCreate, referencedByReturnCode *[]bool) error {

	// at least one code section is guaranteed by spec

	visitedCodeSections := make([]bool, len(header.codeSizes))

	codeSectionsQueue := make([]uint16, 0)
	codeSectionsQueue = append(codeSectionsQueue, 0)

	var err error
	var subcontainerRefs [][2]uint16
	var accessCodeSections map[uint16]bool

	for len(codeSectionsQueue) > 0 {
		codeIdx := codeSectionsQueue[0]
		codeSectionsQueue = codeSectionsQueue[1:]

		if visitedCodeSections[codeIdx] {
			continue
		}
		visitedCodeSections[codeIdx] = true

		start := offset
		for i := uint16(0); i < codeIdx; i++ {
			start += header.codeSizes[i]
		}
		// codeSection := code[start : start+header.codeSizes[codeIdx]]
		// offset += header.codeSizes[codeIdx]
		if subcontainerRefs, accessCodeSections, err = validateInstructions(eofCode, codeIdx, header, start, containerKind, jt); err != nil {
			return err
		} else {
			for _, ref := range subcontainerRefs {
				if OpCode(ref[1]) == EOFCREATE {
					(*referencedByEofCreate)[ref[0]] = true
				} else if OpCode(ref[1]) == RETURNCODE {
					(*referencedByReturnCode)[ref[0]] = true
				}
			}
		}
		for idx := range accessCodeSections { // TODO: do we need a map here?
			codeSectionsQueue = append(codeSectionsQueue, idx)
		}
		if err := validateRjumpDestinations(eofCode, header, jt, codeIdx, start); err != nil {
			return err
		}
		if maxStackHeight, err := validateMaxStackHeight(eofCode, codeIdx, header, start, jt); err != nil {
			return err
		} else {
			typesOffset := getTypeSectionOffset(codeIdx, header)
			sectionMaxStack := int(eofCode[typesOffset+2])<<8 | int(eofCode[typesOffset+3])
			if maxStackHeight != sectionMaxStack {
				return fmt.Errorf("EOFException.INVALID_MAX_STACK_HEIGHT")
			}
		}
	}
	for _, visited := range visitedCodeSections {
		if !visited {
			return fmt.Errorf("EOFException.UNREACHABLE_CODE_SECTIONS")
		}
	}

	return nil
}

func validateContainer(eofCode []byte, header *eofHeader, offset uint16, containerIdx int, eofcreate, returnCode bool, jt *JumpTable, depth int) (*eofHeader, error) {

	container := eofCode[offset : offset+header.containerSizes[containerIdx]]

	if eofcreate && returnCode {
		return nil, fmt.Errorf("EOF container referenced by both eofcreate and returncode")
	}
	if !eofcreate && !returnCode {
		return nil, fmt.Errorf("EOFException.ORPHAN_SUBCONTAINER")
	}
	var err error
	var h *eofHeader
	if eofcreate {
		_, err = ParseEOFHeader(container, jt, initcode, true, depth+1)
	} else {
		_, err = ParseEOFHeader(container, jt, runtime, true, depth+1)
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}
