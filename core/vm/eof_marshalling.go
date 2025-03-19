package vm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	MaxContainerDepth = 16 // Follows EIP-3670 recommendation

	_kindTypes     byte = 1
	_kindCode      byte = 2
	_kindContainer byte = 3
	_kindData      byte = 4

	initcode byte = 0
	runtime  byte = 1
)

var (
	supportedVersions = []byte{0x01}
)

// EOFContainer represents the structure of the EOF container.
type EOFContainer struct {
	_header        *header
	_types         []*eofMetaData
	_code          [][]byte
	_subContainers []*EOFContainer
	_data          []byte
	rawData        []byte // think about not having this
}

type eofMetaData struct {
	inputs         byte
	outputs        byte
	maxStackHeight uint16
}

type header struct {
	magic             [2]byte
	version           uint8
	numTypeSections   uint16
	numCodeSections   uint16
	codeSectionSizes  []uint16
	numSubContainers  uint16
	subContainerSizes []uint16
	dataOffset        uint16
	dataLength        uint16
	dataSizePos       uint16
}

// var jumpTables = []JumpTable{NewEOFInstructionSet()}

// MarshalEOF serializes theEOFContainer into a byte array.
func MarshalEOF(container *EOFContainer, depth int) ([]byte, error) {
	// if depth >= MaxContainerDepth {
	// 	panic("depth >= MaxContainerDepth")
	// }
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, container._header.magic); err != nil { // magic
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, container._header.version); err != nil { // version
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, _kindTypes); err != nil { // types section
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(container._types)*4)); err != nil { // types section length
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, _kindCode); err != nil { // types code
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(container._code))); err != nil { // num code sections
		return nil, err
	}
	for _, code := range container._code {
		if err := binary.Write(buf, binary.BigEndian, uint16(len(code))); err != nil { // length of each code
			return nil, err
		}
	}
	var encodedContainers [][]byte
	if len(container._subContainers) > 0 {
		if err := binary.Write(buf, binary.BigEndian, _kindContainer); err != nil { // type container section
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint16(len(container._subContainers))); err != nil { // num container sections
			return nil, err
		}
		for _, subcontainer := range container._subContainers {
			if encoded, err := MarshalEOF(subcontainer, depth+1); err != nil {
				return nil, err
			} else {
				if err := binary.Write(buf, binary.BigEndian, uint16(len(encoded))); err != nil { // lenght of each subcontainer
					return nil, err
				}
				encodedContainers = append(encodedContainers, encoded)
			}
		}
	}
	if err := binary.Write(buf, binary.BigEndian, _kindData); err != nil { // types data
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(container._data))); err != nil { // types data length
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, byte(0)); err != nil { // terminator
		return nil, err
	}

	for _, t := range container._types {
		if _, err := buf.Write([]byte{t.inputs, t.outputs, byte(t.maxStackHeight >> 8), byte(t.maxStackHeight & 0x00ff)}); err != nil {
			return nil, err
		}
	}

	for _, code := range container._code { // write each code
		if _, err := buf.Write(code); err != nil {
			return nil, err
		}
	}

	for _, subcontainer := range encodedContainers { // write each subcontainer
		if _, err := buf.Write(subcontainer); err != nil {
			return nil, err
		}
	}

	if _, err := buf.Write(container._data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func readHeader(buf *bytes.Reader) (*header, int64, error) {
	header := &header{}

	if buf.Size() < 14 {
		return nil, 0, ErrIncompleteEOF
	}
	if buf.Size() > maxInitCodeSize {
		return nil, 0, fmt.Errorf("%w: %v > %v", ErrTooLargeByteCode, buf.Size(), maxInitCodeSize)
	}

	if err := binary.Read(buf, binary.BigEndian, &header.magic); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrInvalidMagic)
	}
	if header.magic[0] != 0xef || header.magic[1] != 0x0 {
		return nil, 0, fmt.Errorf("%w: %v", ErrInvalidMagic, header.magic)
	}
	if err := binary.Read(buf, binary.BigEndian, &header.version); err != nil {
		return nil, 0, err
	}
	isSupportedVersion := false
	for _, version := range supportedVersions {
		if version == header.version {
			isSupportedVersion = true
		}
	}
	if !isSupportedVersion {
		return nil, 0, fmt.Errorf("%w: %v", ErrInvalidVersion, header.version)
	}

	var kind byte
	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	}
	if kind != _kindTypes {
		return nil, 0, fmt.Errorf("%w: found section kind %x instead", ErrMissingTypeHeader, kind)
	}
	if err := binary.Read(buf, binary.BigEndian, &header.numTypeSections); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrMissingTypeHeader, err)
	}
	if header.numTypeSections < 4 || header.numTypeSections%4 != 0 {
		return nil, 0, fmt.Errorf("%w: type section size must be divisible by 4, have %d", ErrInvalidTypeSize, header.numTypeSections)
	}
	if header.numTypeSections/4 > 1024 {
		return nil, 0, fmt.Errorf("%w: type section must not exceed 4*1024: %d", ErrInvalidTypeSize, header.numTypeSections)
	}

	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingCodeHeader)
	}
	if kind != _kindCode {
		return nil, 0, fmt.Errorf("%w: found section kind %x instead", ErrMissingCodeHeader, kind)
	}
	if err := binary.Read(buf, binary.BigEndian, &header.numCodeSections); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingCodeHeader)
	}
	if header.numCodeSections == 0 {
		return nil, 0, fmt.Errorf("%w: numCodeSections must not be 0", ErrIncompleteEOF)
	}
	for i := uint16(0); i < header.numCodeSections; i++ {
		var codeLength uint16
		if err := binary.Read(buf, binary.BigEndian, &codeLength); err != nil {
			return nil, 0, fmt.Errorf("%w: %w", err, ErrInvalidCodeSize)
		}
		if codeLength == 0 {
			return nil, 0, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidCodeSize, i)
		}
		header.codeSectionSizes = append(header.codeSectionSizes, codeLength)
	}
	if len(header.codeSectionSizes) != int(header.numTypeSections)/4 {
		return nil, 0, fmt.Errorf("%w: mismatch of code sections count and type signatures, code %d, types %d", ErrInvalidSectionCount, len(header.codeSectionSizes), int(header.numTypeSections)/4)
	}

	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	}
	if kind == _kindContainer {
		if err := binary.Read(buf, binary.BigEndian, &header.numSubContainers); err != nil {
			return nil, 0, fmt.Errorf("%w: %w", err, ErrInvalidSectionCount)
		}
		// if len(containerSizes) > maxContainerSections {
		// 	return fmt.Errorf("containers exceed allowed limit: %v: have %v", maxContainerSections, len(containerSizes))
		// }
		if header.numSubContainers > 256 {
			return nil, 0, fmt.Errorf("%w: too many container sections: %d", ErrTooManyContainerSections, header.numSubContainers)
		}
		for i := uint16(0); i < header.numSubContainers; i++ {
			var subContainerLength uint16
			if err := binary.Read(buf, binary.BigEndian, &subContainerLength); err != nil {
				return nil, 0, fmt.Errorf("%w: %w", err, ErrInvalidContainerSize)
			}
			if subContainerLength == 0 {
				return nil, 0, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidContainerSize, i)
			}
			header.subContainerSizes = append(header.subContainerSizes, subContainerLength)
			if len(header.subContainerSizes) > 256 {
				return nil, 0, fmt.Errorf("%w: too many container sections: %d", ErrTooManyContainerSections, len(header.subContainerSizes))
			}
		}
		if len(header.subContainerSizes) == 0 {
			return nil, 0, ErrZeroSizeContainerSection
		}
		if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
			return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
		}
	}

	if kind != _kindData {
		return nil, 0, fmt.Errorf("%w: %v", ErrMissingDataHeader, "expected data section")
	}
	header.dataSizePos = uint16(buf.Size() - int64(buf.Len()))
	if err := binary.Read(buf, binary.BigEndian, &header.dataLength); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
	}

	// Read Terminator
	var terminator byte
	if err := binary.Read(buf, binary.BigEndian, &terminator); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingTerminator)
	}
	if terminator != 0 {
		return nil, 0, ErrMissingTerminator
	}

	return header, buf.Size() - int64(buf.Len()), nil
}

func validateBody(header *header, bytesRead, bytesRemaining int64, depth int, containerKind byte) error {
	// bytesRead is a headerSize in bytes

	expectedRemaining := int64(header.numTypeSections)
	for _, codeSize := range header.codeSectionSizes {
		expectedRemaining += int64(codeSize)
	}
	for _, subContainerSize := range header.subContainerSizes {
		expectedRemaining += int64(subContainerSize)
	}
	if expectedRemaining > bytesRemaining {
		return fmt.Errorf("%w: %d > %d", ErrInvalidSectionsSize, expectedRemaining, bytesRemaining)
	}
	if expectedRemaining < bytesRemaining-int64(header.dataLength) {
		return fmt.Errorf("%w: %d < %d", ErrInvalidSectionsSize, expectedRemaining, bytesRemaining-int64(header.dataLength))
	}
	total := bytesRead + bytesRemaining
	dataOffset := bytesRead + expectedRemaining
	if dataOffset == total && header.dataLength > 0 && depth == 0 /* top level container */ {
		return ErrTopLevelTruncated
	}
	if dataOffset+int64(header.dataLength) > total {
		if depth == 0 {
			return ErrTopLevelTruncated
		}
		if containerKind == initcode {
			return ErrEOFCreateWithTruncatedContainer
		}
	}
	if dataOffset+int64(header.dataLength) < total {
		return fmt.Errorf("%w: %d + %d > %d", ErrInvalidSectionsSize, dataOffset, header.dataLength, total)
	}

	return nil
}

func readMetaData(buf *bytes.Reader, numTypes int) ([]*eofMetaData, int64, error) {
	_types := make([]*eofMetaData, numTypes)
	metadata := &eofMetaData{}
	if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingTypesInput)
	}
	if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingTypesOutput)
	}
	if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", err, ErrMissingMaxStackHeight)
	}
	if metadata.inputs != 0 || metadata.outputs != nonReturningFunction {
		return nil, 0, fmt.Errorf("%w: have %d, %d", ErrInvalidFirstSectionType, metadata.inputs, metadata.outputs)
	}
	if metadata.maxStackHeight > maxStackHeight {
		return nil, 0, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, 0, metadata.maxStackHeight)
	}
	_types[0] = metadata
	for i := 1; i < numTypes; i++ {
		metadata := &eofMetaData{}
		if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
			return nil, 0, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesInput, i)
		}
		if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
			return nil, 0, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesOutput, i)
		}
		if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
			return nil, 0, fmt.Errorf("%w: %w at %d", err, ErrMissingMaxStackHeight, i)
		}
		if metadata.inputs > maxInputItems {
			return nil, 0, fmt.Errorf("%w for section %d: have %d", ErrTooManyInputs, i, metadata.inputs)
		}
		if metadata.outputs > maxOutputItems && metadata.outputs != nonReturningFunction {
			return nil, 0, fmt.Errorf("%w for section %d: have %d", ErrTooManyOutputs, i, metadata.outputs)
		}
		if metadata.maxStackHeight > maxStackHeight {
			return nil, 0, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, i, metadata.maxStackHeight)
		}
		_types[i] = metadata
	}
	return _types, buf.Size() - int64(buf.Len()), nil
}

func readCodeSection(buf *bytes.Reader, jt *JumpTable, header *header, _types []*eofMetaData, start int64, numCodeSections, numSubContainers uint16, containerKind byte, validateCode bool) ([][]byte, []bool, []bool, int64, error) {

	referencedByEofCreate := make([]bool, numSubContainers)
	referencedByReturnContract := make([]bool, numSubContainers)
	_code := make([][]byte, numCodeSections)

	visitedCodeSections := make([]bool, numCodeSections)
	codeSectionsQueue := make([]uint16, 0)
	codeSectionsQueue = append(codeSectionsQueue, 0)
	var err error
	var subcontainerRefs [][2]uint16
	var accessCodeSections map[uint16]bool
	end := start
	for len(codeSectionsQueue) > 0 {
		codeIdx := codeSectionsQueue[0]
		codeSectionsQueue = codeSectionsQueue[1:]
		if visitedCodeSections[codeIdx] {
			continue
		}
		visitedCodeSections[codeIdx] = true
		code := make([]byte, header.codeSectionSizes[codeIdx])
		end += int64(header.codeSectionSizes[codeIdx])
		offset := start
		for i := uint16(0); i < codeIdx; i++ { // TODO(racytech): extra loop! do we need this?
			offset += int64(header.codeSectionSizes[i])
		}
		if _, err := buf.ReadAt(code, offset); err != nil {
			return nil, nil, nil, -1, fmt.Errorf("%w: %w", err, ErrInvalidCode)
		}
		if validateCode {
			if subcontainerRefs, accessCodeSections, err = validateInstructions(code, _types, jt, int(codeIdx), int(header.dataLength), int(numSubContainers), containerKind); err != nil {
				return nil, nil, nil, -1, err
			} else {
				for _, arr := range subcontainerRefs { // arr[0] - container index, arr[1] - EOFCREATE || RETURNCONTRACT
					if OpCode(arr[1]) == EOFCREATE {
						referencedByEofCreate[arr[0]] = true
					} else if OpCode(arr[1]) == RETURNCODE {
						referencedByReturnContract[arr[0]] = true
					} else {
						panic("unexpected OpCode")
					}
				}
			}
			for idx, _ := range accessCodeSections {
				codeSectionsQueue = append(codeSectionsQueue, idx)
			}
			if err := validateRjumpDestinations(code, jt); err != nil {
				return nil, nil, nil, -1, err
			}
			if maxStackHeight, err := validateMaxStackHeight(code, _types, jt, int(codeIdx)); err != nil {
				return nil, nil, nil, -1, err
			} else {
				if maxStackHeight != int(_types[codeIdx].maxStackHeight) {
					return nil, nil, nil, -1, ErrInvalidMaxStackHeight
				}
			}
		}
		_code[codeIdx] = code
	}
	for _, visited := range visitedCodeSections {
		if !visited {
			return nil, nil, nil, -1, ErrUnreachableCodeSections
		}
	}
	return _code, referencedByEofCreate, referencedByReturnContract, end, nil

}

func readSubContainer(buf *bytes.Reader, header *header, jt *JumpTable, referencedByEofCreate, referencedByReturnContract []bool, numSubContainers, depth int) ([]*EOFContainer, int64, error) {
	_subContainer := make([]*EOFContainer, numSubContainers)
	for i := 0; i < numSubContainers; i++ {

		eofcreate := referencedByEofCreate[i]
		returnContract := referencedByReturnContract[i]
		if eofcreate && returnContract {
			return nil, 0, ErrAmbiguousContainer
		}
		if !eofcreate && !returnContract {
			return nil, 0, ErrOrphanSubContainer
		}
		subContainerData := make([]byte, header.subContainerSizes[i])
		if _, err := buf.Read(subContainerData); err != nil {
			return nil, 0, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
		}
		var subContainer *EOFContainer
		var err error
		if eofcreate {
			subContainer, err = UnmarshalEOF(subContainerData, depth+1, initcode, jt, true)
		} else {
			subContainer, err = UnmarshalEOF(subContainerData, depth+1, runtime, jt, true)
		}
		if err != nil {
			return nil, 0, err
		}
		_subContainer[i] = subContainer
	}
	return _subContainer, buf.Size() - int64(buf.Len()), nil
}

// UnmarshalEOF deserializes a byte array into an EOFContainer object.
func UnmarshalEOF(data []byte, depth int, containerKind byte, jt *JumpTable, validateCode bool) (*EOFContainer, error) {
	buf := bytes.NewReader(data)
	container := &EOFContainer{}
	container.rawData = data
	header, bytesRead, err := readHeader(buf)
	if err != nil {
		return nil, err
	}
	container._header = header
	if err := validateBody(header, bytesRead, int64(buf.Len()), depth, containerKind); err != nil {
		return nil, err
	}

	// jt := jumpTables[header.version-1]

	_types, codeBodyOffset, err := readMetaData(buf, int(header.numTypeSections)/4)
	if err != nil {
		return nil, err
	}
	container._types = _types

	_code, referencedByEofCreate, referencedByReturnContract, subContainerBodyOffset, err := readCodeSection(buf, jt, header, _types, codeBodyOffset, header.numCodeSections, header.numSubContainers, containerKind, validateCode)
	if err != nil {
		return nil, err
	}
	container._code = _code

	buf.Seek(subContainerBodyOffset, io.SeekStart)
	_subContainer, _, err := readSubContainer(buf, header, jt, referencedByEofCreate, referencedByReturnContract, int(header.numSubContainers), depth)
	if err != nil {
		return nil, err
	}
	container._subContainers = _subContainer

	header.dataOffset = uint16(buf.Size() - int64(buf.Len()))
	if header.dataLength > 0 {
		container._data = make([]byte, header.dataLength)
		if _, err := buf.Read(container._data); err != nil && err != io.EOF {
			return nil, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
		}
	}
	return container, nil
}

func (c *EOFContainer) updateData(auxData []byte) error {

	// dataOffset := len(c.rawData) - len(c._data)
	newDataSize := len(c.rawData) - int(c._header.dataOffset) + len(auxData)

	if newDataSize > 65535 {
		return ErrAuxDataTooLarge
	}
	if newDataSize < len(c._data) {
		return ErrAuxDataDecrease
	}

	// const auto data_size_pos = header.data_size_position();
	// dataSizePo
	c.rawData = append(c.rawData, auxData...)
	c.rawData[c._header.dataSizePos] = byte(newDataSize >> 8)
	c.rawData[c._header.dataSizePos+1] = byte(newDataSize)
	return nil
}
