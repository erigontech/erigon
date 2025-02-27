package vm

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	Magic         [2]byte
	Version       uint8
	_types        []*eofMetaData
	_code         [][]byte
	_subContainer []*EOFContainer
	_data         []byte
}

type eofMetaData struct {
	inputs         byte
	outputs        byte
	maxStackHeight uint16
}

var jumpTables = []JumpTable{NewEOFInstructionSet()}

// MarshalEOF serializes theEOFContainer into a byte array.
func MarshalEOF(container *EOFContainer, depth int) ([]byte, error) {
	// if depth >= MaxContainerDepth {
	// 	panic("depth >= MaxContainerDepth")
	// }
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, container.Magic); err != nil { // magic
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, container.Version); err != nil { // version
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
	if len(container._subContainer) > 0 {
		if err := binary.Write(buf, binary.BigEndian, _kindContainer); err != nil { // type container section
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, uint16(len(container._subContainer))); err != nil { // num container sections
			return nil, err
		}
		for _, subcontainer := range container._subContainer {
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

// UnmarshalEOF deserializes a byte array into an EOFContainer object.
func UnmarshalEOF(data []byte, depth int, containerKind byte) (*EOFContainer, error) {
	// if depth >= MaxContainerDepth {
	// 	return nil, errors.New("depth >= MaxContainerDepth")
	// }
	fmt.Println("initcodeSize: ", len(data))
	buf := bytes.NewReader(data)
	container := &EOFContainer{}

	if buf.Size() < 14 {
		return nil, ErrIncompleteEOF
	}
	if buf.Size() > maxInitCodeSize {
		return nil, fmt.Errorf("%w: %v > %v", ErrTooLargeByteCode, buf.Size(), maxInitCodeSize)
	}

	// Read Magic
	if err := binary.Read(buf, binary.BigEndian, &container.Magic); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrInvalidMagic)
	}
	if container.Magic[0] != 0xef || container.Magic[1] != 0x0 {
		return nil, fmt.Errorf("%w: want %x, got: %x", ErrInvalidMagic, eofMagic, container.Magic)
	}

	// Read Version
	if err := binary.Read(buf, binary.BigEndian, &container.Version); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrInvalidVersion)
	}
	isSupportedVersion := false
	for _, version := range supportedVersions {
		if version == container.Version {
			isSupportedVersion = true
		}
	}
	if !isSupportedVersion {
		return nil, fmt.Errorf("%w: have %d", ErrInvalidVersion, container.Version)
	}

	jt := jumpTables[container.Version-1]

	// Read Types Section
	var kind byte
	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	}

	if kind != _kindTypes {
		fmt.Println("HERE")
		return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingTypeHeader, kind)
	}

	var typesLength uint16
	if err := binary.Read(buf, binary.BigEndian, &typesLength); err != nil {
		return nil, err
	}
	if typesLength < 4 || typesLength%4 != 0 {
		return nil, fmt.Errorf("%w: type section size must be divisible by 4, have %d", ErrInvalidTypeSize, typesLength)
	}
	if typesLength/4 > 1024 {
		return nil, fmt.Errorf("%w: type section must not exceed 4*1024, have %d", ErrInvalidTypeSize, typesLength)
	}

	// Read Code Section
	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingCodeHeader)
	}
	if kind != _kindCode {
		return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingCodeHeader, kind)
	}
	var numCodeSections uint16
	if err := binary.Read(buf, binary.BigEndian, &numCodeSections); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeHeader)
	}
	var codeSizes []uint16
	for i := uint16(0); i < numCodeSections; i++ {
		var codeLength uint16
		if err := binary.Read(buf, binary.BigEndian, &codeLength); err != nil {
			return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeSize)
		}
		if codeLength == 0 {
			return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidCodeSize, i)
		}
		codeSizes = append(codeSizes, codeLength)
	}
	if len(codeSizes) != int(typesLength)/4 { // invalid code section count or type section count
		return nil, fmt.Errorf("%w: mismatch of code sections count and type signatures, types %d, code %d", ErrInvalidSectionCount, typesLength/4, len(codeSizes))
	}

	// Read SubContainer Section
	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	}

	var subContainerSizes []uint16
	var numSubContainers uint16
	if kind == _kindContainer {
		if err := binary.Read(buf, binary.BigEndian, &numSubContainers); err != nil {
			return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerArgument)
		}
		if numSubContainers > 256 {
			return nil, ErrTooManyContainerSections
		}
		container._subContainer = make([]*EOFContainer, numSubContainers)
		for i := uint16(0); i < numSubContainers; i++ {
			var subContainerLength uint16
			if err := binary.Read(buf, binary.BigEndian, &subContainerLength); err != nil {
				return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerSize)
			}
			if subContainerLength == 0 {
				return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidContainerSize, i)
			}
			subContainerSizes = append(subContainerSizes, subContainerLength)
			if len(subContainerSizes) > 256 {
				return nil, ErrTooManyContainerSections
			}
		}
		if len(subContainerSizes) == 0 {
			return nil, ErrZeroSizeContainerSection
		}
		// Read Data Section
		if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
			return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
		}
	}

	if kind != _kindData {
		return nil, fmt.Errorf("%w: found section %x instead", ErrMissingDataHeader, kind)
	}
	var dataLength uint16
	if err := binary.Read(buf, binary.BigEndian, &dataLength); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
	}

	// Read Terminator
	var terminator byte
	if err := binary.Read(buf, binary.BigEndian, &terminator); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingTerminator)
	}
	if terminator != 0 {
		return nil, ErrMissingTerminator
	}

	expectedRemaining := int(typesLength)
	for _, codeSize := range codeSizes {
		expectedRemaining += int(codeSize)
	}
	for _, subContainerSize := range subContainerSizes {
		expectedRemaining += int(subContainerSize)
	}
	if expectedRemaining < buf.Len()-int(dataLength) {
		return nil, fmt.Errorf("%w: %d < %d", ErrInvalidSectionsSize, expectedRemaining, buf.Len()-int(dataLength))
	}

	dataOffset := (int(buf.Size()) - buf.Len()) + expectedRemaining
	if dataOffset == int(buf.Size()) && dataLength > 0 && depth == 0 /* top level container */ {
		return nil, ErrTopLevelTruncated
	}

	// numTypes
	numTypes := typesLength / 4
	container._types = make([]*eofMetaData, numTypes)
	metadata := &eofMetaData{}
	if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypesInput)
	}
	if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypesOutput)
	}
	if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
		return nil, fmt.Errorf("%w: %w", err, ErrMissingMaxStackHeight)
	}
	if metadata.inputs != 0 || metadata.outputs != nonReturningFunction {
		return nil, fmt.Errorf("%w: have %d, %d", ErrInvalidFirstSectionType, metadata.inputs, metadata.outputs)
	}
	if metadata.maxStackHeight > maxStackHeight {
		return nil, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, 0, metadata.maxStackHeight)
	}
	container._types[0] = metadata
	for i := uint16(1); i < numTypes; i++ {
		metadata := &eofMetaData{}
		if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesInput, i)
		}
		if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesOutput, i)
		}
		if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingMaxStackHeight, i)
		}
		if metadata.inputs > maxInputItems {
			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooManyInputs, i, metadata.inputs)
		}
		if metadata.outputs > maxOutputItems && metadata.outputs != nonReturningFunction {
			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooManyOutputs, i, metadata.outputs)
		}
		if metadata.maxStackHeight > maxStackHeight {
			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, i, metadata.maxStackHeight)
		}
		container._types[i] = metadata
	}

	referencedByEofCreate := make([]bool, numSubContainers)
	referencedByReturnContract := make([]bool, numSubContainers)
	container._code = make([][]byte, numCodeSections)
	for i := 0; i < len(codeSizes); i++ {
		code := make([]byte, codeSizes[i])
		if _, err := buf.Read(code); err != nil {
			return nil, fmt.Errorf("%w: %w", err, ErrInvalidCode)
		}
		fmt.Println("len subcontainer: ", len(container._subContainer))
		fmt.Println(code)
		if subcontainerRefs, err := validateInstructions(code, container._types, &jt, i, int(dataLength), len(container._subContainer), containerKind); err != nil {
			return nil, err
		} else {
			for _, arr := range subcontainerRefs { // arr[0] - container index, arr[1] - EOFCREATE || RETURNCONTRACT
				if OpCode(arr[1]) == EOFCREATE {
					referencedByEofCreate[arr[0]] = true
				} else if OpCode(arr[1]) == RETURNCONTRACT {
					referencedByReturnContract[arr[0]] = true
				} else {
					panic("unexpected OpCode")
				}
			}
		}
		if err := validateRjumpDestinations(code, &jt); err != nil {
			return nil, fmt.Errorf("RJUMP destinations validation fail")
		}
		if _, err := validateMaxStackHeight(code, container._types, &jt, i); err != nil {
			return nil, fmt.Errorf("max_stack_height validation fail")
		}
		container._code[i] = code
	}

	if len(subContainerSizes) > 0 {
		container._subContainer = make([]*EOFContainer, numSubContainers)
		for i := uint16(0); i < numSubContainers; i++ {

			eofcreate := referencedByEofCreate[i]
			returnContract := referencedByReturnContract[i]
			fmt.Println("eofcreate, returnContract: ", eofcreate, returnContract)
			if eofcreate && returnContract {
				return nil, ErrAmbiguousContainer
			}
			if !eofcreate && !returnContract {
				return nil, ErrOrphanSubContainer
			}
			subContainerData := make([]byte, subContainerSizes[i])
			if _, err := buf.Read(subContainerData); err != nil {
				return nil, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
			}
			var subContainer *EOFContainer
			var err error
			if eofcreate {
				subContainer, err = UnmarshalEOF(subContainerData, depth+1, initcode)
			} else {
				subContainer, err = UnmarshalEOF(subContainerData, depth+1, runtime)
			}
			if err != nil {
				return nil, err
			}
			container._subContainer[i] = subContainer
		}
	}

	if dataLength > 0 {
		container._data = make([]byte, dataLength)
		if _, err := buf.Read(container._data); err != nil {
			return nil, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
		}
	}

	return container, nil
}

// // UnmarshalEOF deserializes a byte array into an EOFContainer object.
// func ParseEOF(data []byte, depth int, containerKind byte) (*EOFContainer, error) {
// 	// if depth >= MaxContainerDepth {
// 	// 	return nil, errors.New("depth >= MaxContainerDepth")
// 	// }
// 	fmt.Printf("%sinitcode: %x\n", strings.Repeat("\t", depth), data)
// 	buf := bytes.NewReader(data)
// 	container := &EOFContainer{}

// 	if buf.Size() < 14 {
// 		return nil, ErrIncompleteEOF
// 	}
// 	if buf.Size() > maxInitCodeSize {
// 		return nil, fmt.Errorf("%w: %v > %v", ErrTooLargeByteCode, buf.Size(), maxInitCodeSize)
// 	}

// 	// Read Magic
// 	if err := binary.Read(buf, binary.BigEndian, &container.Magic); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidMagic)
// 	}
// 	if container.Magic[0] != 0xef && container.Magic[1] != 0x00 {
// 		return nil, fmt.Errorf("%w: want %x", ErrInvalidMagic, eofMagic)
// 	}
// 	fmt.Printf("%smagic: %x\n", strings.Repeat("\t", depth), container.Magic)

// 	// Read Version
// 	if err := binary.Read(buf, binary.BigEndian, &container.Version); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidVersion)
// 	}
// 	isSupportedVersion := false
// 	for _, version := range supportedVersions {
// 		if version == container.Version {
// 			isSupportedVersion = true
// 		}
// 	}
// 	if !isSupportedVersion {
// 		return nil, fmt.Errorf("%w: have %d", ErrInvalidVersion, container.Version)
// 	}
// 	fmt.Printf("%sversion: %x\n", strings.Repeat("\t", depth), container.Version)

// 	jt := jumpTables[container.Version-1]

// 	// Read Types Section
// 	var kind byte
// 	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
// 	}

// 	if kind != _kindTypes {
// 		return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingTypeHeader, kind)
// 	}
// 	fmt.Printf("%ssection_types\n", strings.Repeat("\t", depth))
// 	var typesLength uint16
// 	if err := binary.Read(buf, binary.BigEndian, &typesLength); err != nil {
// 		return nil, err
// 	}
// 	if typesLength < 4 || typesLength%4 != 0 {
// 		return nil, fmt.Errorf("%w: type section size must be divisible by 4, have %d", ErrInvalidTypeSize, typesLength)
// 	}
// 	if typesLength/4 > 1024 {
// 		return nil, fmt.Errorf("%w: type section must not exceed 4*1024, have %d", ErrInvalidTypeSize, typesLength)
// 	}
// 	fmt.Printf("%stypes_length: %x\n", strings.Repeat("\t", depth), typesLength)

// 	// Read Code Section
// 	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingCodeHeader)
// 	}
// 	if kind != _kindCode {
// 		return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingCodeHeader, kind)
// 	}
// 	fmt.Printf("%ssection_code\n", strings.Repeat("\t", depth))
// 	var numCodeSections uint16
// 	if err := binary.Read(buf, binary.BigEndian, &numCodeSections); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeHeader)
// 	}
// 	var codeSizes []uint16
// 	for i := uint16(0); i < numCodeSections; i++ {
// 		var codeLength uint16
// 		if err := binary.Read(buf, binary.BigEndian, &codeLength); err != nil {
// 			return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeSize)
// 		}
// 		if codeLength == 0 {
// 			return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidCodeSize, i)
// 		}
// 		codeSizes = append(codeSizes, codeLength)
// 		fmt.Printf("%scode_length: %v\n", strings.Repeat("\t", depth), codeLength)
// 	}
// 	if len(codeSizes) != int(typesLength)/4 { // invalid code section count or type section count
// 		return nil, fmt.Errorf("%w: mismatch of code sections count and type signatures, types %d, code %d", ErrInvalidSectionCount, typesLength/4, len(codeSizes))
// 	}
// 	fmt.Printf("%snum_code_section: %v\n", strings.Repeat("\t", depth), len(codeSizes))
// 	// Read SubContainer Section
// 	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
// 	}

// 	var subContainerSizes []uint16
// 	var numSubContainers uint16
// 	if kind == _kindContainer {
// 		fmt.Printf("%ssection_subcontainers\n", strings.Repeat("\t", depth))
// 		if err := binary.Read(buf, binary.BigEndian, &numSubContainers); err != nil {
// 			return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerArgument)
// 		}
// 		container._subContainer = make([]*EOFContainer, numSubContainers)
// 		for i := uint16(0); i < numSubContainers; i++ {
// 			var subContainerLength uint16
// 			if err := binary.Read(buf, binary.BigEndian, &subContainerLength); err != nil {
// 				return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerSize)
// 			}
// 			if subContainerLength == 0 {
// 				return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidContainerSize, i)
// 			}
// 			subContainerSizes = append(subContainerSizes, subContainerLength)
// 			fmt.Printf("%ssub_container_length: %v\n", strings.Repeat("\t", depth), subContainerLength)
// 		}
// 		if len(subContainerSizes) == 0 {
// 			return nil, ErrZeroSizeContainerSection
// 		}
// 		if len(subContainerSizes) > 256 {
// 			return nil, ErrTooManyContainerSections
// 		}
// 		fmt.Printf("%snum_subcontainers: %v\n", strings.Repeat("\t", depth), len(subContainerSizes))
// 		// Read Data Section
// 		if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
// 			return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
// 		}
// 	}

// 	if kind != _kindData {
// 		return nil, fmt.Errorf("%w: found section %x instead", ErrMissingDataHeader, kind)
// 	}
// 	fmt.Printf("%ssection_data\n", strings.Repeat("\t", depth))
// 	var dataLength uint16
// 	if err := binary.Read(buf, binary.BigEndian, &dataLength); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
// 	}
// 	fmt.Printf("%sdata_length: %v\n", strings.Repeat("\t", depth), dataLength)
// 	// Read Terminator
// 	var terminator byte
// 	if err := binary.Read(buf, binary.BigEndian, &terminator); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingTerminator)
// 	}
// 	if terminator != 0 {
// 		return nil, ErrMissingTerminator
// 	}
// 	fmt.Printf("%sterminator: 00\n", strings.Repeat("\t", depth))

// 	expectedRemaining := int(typesLength)
// 	for _, codeSize := range codeSizes {
// 		expectedRemaining += int(codeSize)
// 	}
// 	for _, subContainerSize := range subContainerSizes {
// 		expectedRemaining += int(subContainerSize)
// 	}
// 	if expectedRemaining < buf.Len()-int(dataLength) {
// 		return nil, fmt.Errorf("%w: %d < %d", ErrInvalidSectionsSize, expectedRemaining, buf.Len()-int(dataLength))
// 	}

// 	// numTypes
// 	numTypes := typesLength / 4
// 	fmt.Printf("%snum_types: %v\n", strings.Repeat("\t", depth), numTypes)
// 	container._types = make([]*eofMetaData, numTypes)
// 	metadata := &eofMetaData{}
// 	if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypesInput)
// 	}
// 	if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingTypesOutput)
// 	}
// 	if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingMaxStackHeight)
// 	}
// 	if metadata.inputs != 0 || metadata.outputs != nonReturningFunction {
// 		return nil, fmt.Errorf("%w: have %d, %d", ErrInvalidFirstSectionType, metadata.inputs, metadata.outputs)
// 	}
// 	if metadata.maxStackHeight > maxStackHeight {
// 		return nil, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, 0, metadata.maxStackHeight)
// 	}
// 	fmt.Printf("%smetadata-0: %v\n", strings.Repeat("\t", depth), metadata)
// 	container._types[0] = metadata
// 	for i := uint16(1); i < numTypes; i++ {
// 		metadata := &eofMetaData{}
// 		if err := binary.Read(buf, binary.BigEndian, &metadata.inputs); err != nil {
// 			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesInput, i)
// 		}
// 		if err := binary.Read(buf, binary.BigEndian, &metadata.outputs); err != nil {
// 			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingTypesOutput, i)
// 		}
// 		if err := binary.Read(buf, binary.BigEndian, &metadata.maxStackHeight); err != nil {
// 			return nil, fmt.Errorf("%w: %w at %d", err, ErrMissingMaxStackHeight, i)
// 		}
// 		if metadata.inputs > maxInputItems {
// 			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooManyInputs, i, metadata.inputs)
// 		}
// 		if metadata.outputs > maxOutputItems && metadata.outputs != nonReturningFunction {
// 			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooManyOutputs, i, metadata.outputs)
// 		}
// 		if metadata.maxStackHeight > maxStackHeight {
// 			return nil, fmt.Errorf("%w for section %d: have %d", ErrTooLargeMaxStackHeight, i, metadata.maxStackHeight)
// 		}
// 		fmt.Printf("%smetadata-%d: %v\n", strings.Repeat("\t", depth), i, metadata)
// 		container._types[i] = metadata
// 	}

// 	referencedByEofCreate := make([]bool, numSubContainers)
// 	referencedByReturnContract := make([]bool, numSubContainers)
// 	container._code = make([][]byte, numCodeSections)
// 	for i := 0; i < len(codeSizes); i++ {
// 		code := make([]byte, codeSizes[i])
// 		if _, err := buf.Read(code); err != nil {
// 			return nil, fmt.Errorf("%w: %w", err, ErrInvalidCode)
// 		}
// 		fmt.Printf("containerKind: %d, codeIndex: %d, depth: %d\n", containerKind, i, depth)
// 		fmt.Println("len subcontainer: ", len(container._subContainer))
// 		fmt.Println(code)
// 		if subcontainerRefs, err := validateInstructions(code, container._types, &jt, i, int(dataLength), len(container._subContainer), containerKind); err != nil {
// 			return nil, err
// 		} else {
// 			for _, arr := range subcontainerRefs { // arr[0] - container index, arr[1] - EOFCREATE || RETURNCONTRACT
// 				if OpCode(arr[1]) == EOFCREATE {
// 					referencedByEofCreate[arr[0]] = true
// 				} else if OpCode(arr[1]) == RETURNCONTRACT {
// 					referencedByReturnContract[arr[0]] = true
// 				} else {
// 					panic("unexpected OpCode")
// 				}
// 			}
// 		}
// 		if err := validateRjumpDestinations(code, &jt); err != nil {
// 			return nil, fmt.Errorf("RJUMP destinations validation fail")
// 		}
// 		if _, err := validateMaxStackHeight(code, container._types, &jt, i); err != nil {
// 			return nil, fmt.Errorf("max_stack_height validation fail")
// 		}
// 		fmt.Printf("%scode-%d: %x\n", strings.Repeat("\t", depth), i, code)
// 		container._code[i] = code
// 	}

// 	if len(subContainerSizes) > 0 {
// 		container._subContainer = make([]*EOFContainer, numSubContainers)
// 		for i := uint16(0); i < numSubContainers; i++ {

// 			eofcreate := referencedByEofCreate[i]
// 			returnContract := referencedByReturnContract[i]

// 			if eofcreate && returnContract {
// 				return nil, fmt.Errorf("ambiguous_container_kind")
// 			}
// 			if !eofcreate && !returnContract {
// 				return nil, fmt.Errorf("unreferenced_subcontainer")
// 			}
// 			subContainerData := make([]byte, subContainerSizes[i])
// 			if _, err := buf.Read(subContainerData); err != nil {
// 				return nil, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
// 			}
// 			var subContainer *EOFContainer
// 			var err error
// 			if eofcreate {
// 				subContainer, err = ParseEOF(subContainerData, depth+1, initcode)
// 			} else {
// 				subContainer, err = ParseEOF(subContainerData, depth+1, runtime)
// 			}
// 			if err != nil {
// 				return nil, err
// 			}
// 			container._subContainer[i] = subContainer
// 		}
// 	}

// 	if dataLength > 0 {
// 		container._data = make([]byte, dataLength)
// 		if _, err := buf.Read(container._data); err != nil {
// 			return nil, fmt.Errorf("%w: %w", err, ErrInvalidEOF)
// 		}
// 		fmt.Printf("%sdata: %x\n", strings.Repeat("\t", depth), container._data)
// 	} else {
// 		fmt.Printf("%sdata: nil\n", strings.Repeat("\t", depth))
// 	}

// 	return container, nil
// }
