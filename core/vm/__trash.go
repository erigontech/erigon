	// referencedByEofCreate := make([]bool, numSubContainers)
	// referencedByReturnContract := make([]bool, numSubContainers)
	// container._code = make([][]byte, numCodeSections)

	// for i := 0; i < len(codeSizes); i++ {
	// 	code := make([]byte, codeSizes[i])
	// 	if _, err := buf.Read(code); err != nil {
	// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidCode)
	// 	}
	// 	fmt.Println("len subcontainer: ", len(container._subContainer))
	// 	if subcontainerRefs, _, err := validateInstructions(code, container._types, &jt, i, int(dataLength), len(container._subContainer), containerKind); err != nil {
	// 		return nil, err
	// 	} else {
	// 		for _, arr := range subcontainerRefs { // arr[0] - container index, arr[1] - EOFCREATE || RETURNCONTRACT
	// 			if OpCode(arr[1]) == EOFCREATE {
	// 				referencedByEofCreate[arr[0]] = true
	// 			} else if OpCode(arr[1]) == RETURNCONTRACT {
	// 				referencedByReturnContract[arr[0]] = true
	// 			} else {
	// 				panic("unexpected OpCode")
	// 			}
	// 		}
	// 	}
	// 	if err := validateRjumpDestinations(code, &jt); err != nil {
	// 		return nil, fmt.Errorf("RJUMP destinations validation fail")
	// 	}
	// 	if maxStackHeight, err := validateMaxStackHeight(code, container._types, &jt, i); err != nil {
	// 		return nil, err
	// 	} else {
	// 		if maxStackHeight != int(container._types[i].maxStackHeight) {
	// 			return nil, ErrInvalidMaxStackHeight
	// 		}
	// 	}
	// 	container._code[i] = code
	// }

	// // // Validate code sections
	// // std::vector<bool> visited_code_sections(header.code_sizes.size());
	// // std::queue<uint16_t> code_sections_queue({0});

	// visitedCodeSections := make([]bool, numCodeSections)
	// codeSectionsQueue := make([]uint16, 0)
	// codeSectionsQueue = append(codeSectionsQueue, 0)
	// start := buf.Size() - int64(buf.Len()) // code section start
	// var err error
	// var subcontainerRefs [][2]uint16
	// var accessCodeSections map[uint16]bool
	// for len(codeSectionsQueue) > 0 {
	// 	codeIdx := codeSectionsQueue[0]
	// 	codeSectionsQueue = codeSectionsQueue[1:]
	// 	if visitedCodeSections[codeIdx] {
	// 		continue
	// 	}
	// 	visitedCodeSections[codeIdx] = true
	// 	code := make([]byte, codeSizes[codeIdx])
	// 	offset := start
	// 	for i := uint16(0); i < codeIdx; i++ {
	// 		offset += int64(codeSizes[i])
	// 	}
	// 	if _, err := buf.ReadAt(code, offset); err != nil {
	// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidCode)
	// 	}

	// 	if subcontainerRefs, accessCodeSections, err = validateInstructions(code, container._types, &jt, int(codeIdx), int(dataLength), len(container._subContainer), containerKind); err != nil {
	// 		return nil, err
	// 	} else {
	// 		for _, arr := range subcontainerRefs { // arr[0] - container index, arr[1] - EOFCREATE || RETURNCONTRACT
	// 			if OpCode(arr[1]) == EOFCREATE {
	// 				referencedByEofCreate[arr[0]] = true
	// 			} else if OpCode(arr[1]) == RETURNCONTRACT {
	// 				referencedByReturnContract[arr[0]] = true
	// 			} else {
	// 				panic("unexpected OpCode")
	// 			}
	// 		}
	// 	}
	// 	for idx, _ := range accessCodeSections {
	// 		codeSectionsQueue = append(codeSectionsQueue, idx)
	// 	}
	// 	if err := validateRjumpDestinations(code, &jt); err != nil {
	// 		return nil, fmt.Errorf("RJUMP destinations validation fail")
	// 	}
	// 	if maxStackHeight, err := validateMaxStackHeight(code, container._types, &jt, int(codeIdx)); err != nil {
	// 		return nil, err
	// 	} else {
	// 		if maxStackHeight != int(container._types[codeIdx].maxStackHeight) {
	// 			return nil, ErrInvalidMaxStackHeight
	// 		}
	// 	}
	// 	container._code[codeIdx] = code
	// }
	// for _, visited := range visitedCodeSections {
	// 	if !visited {
	// 		return nil, ErrUnreachableCodeSections
	// 	}
	// }

	// while (!code_sections_queue.empty())
	// {
	// 	const auto code_idx = code_sections_queue.front();
	// 	code_sections_queue.pop();

	// 	if (visited_code_sections[code_idx])
	// 		continue;

	// 	visited_code_sections[code_idx] = true;

	// 	// Validate instructions
	// 	const auto instr_validation_result_or_error =
	// 		validate_instructions(rev, header, container_kind, code_idx, container);
	// 	if (const auto* error =
	// 			std::get_if<EOFValidationError>(&instr_validation_result_or_error))
	// 		return *error;

	// 	const auto& [subcontainer_references, accessed_code_sections] =
	// 		std::get<InstructionValidationResult>(instr_validation_result_or_error);

	// 	// Mark what instructions referenced which subcontainers.
	// 	for (const auto& [index, opcode] : subcontainer_references)
	// 	{
	// 		if (opcode == OP_EOFCREATE)
	// 			subcontainer_referenced_by_eofcreate[index] = true;
	// 		else if (opcode == OP_RETURNCONTRACT)
	// 			subcontainer_referenced_by_returncontract[index] = true;
	// 		else
	// 			intx::unreachable();
	// 	}

	// 	// TODO(C++23): can use push_range()
	// 	for (const auto section_id : accessed_code_sections)
	// 		code_sections_queue.push(section_id);

	// 	// Validate jump destinations
	// 	if (!validate_rjump_destinations(header.get_code(container, code_idx)))
	// 		return EOFValidationError::invalid_rjump_destination;

	// 	// Validate stack
	// 	auto msh_or_error = validate_max_stack_height(
	// 		header.get_code(container, code_idx), code_idx, header, container);
	// 	if (const auto* error = std::get_if<EOFValidationError>(&msh_or_error))
	// 		return *error;
	// 	// TODO(clang-tidy): Too restrictive, see
	// 	//   https://github.com/llvm/llvm-project/issues/120867.
	// 	// NOLINTNEXTLINE(modernize-use-integer-sign-comparison)
	// 	if (std::get<int32_t>(msh_or_error) !=
	// 		header.get_type(container, code_idx).max_stack_height)
	// 		return EOFValidationError::invalid_max_stack_height;
	// }

	// if (std::ranges::find(visited_code_sections, false) != visited_code_sections.end())
	// return EOFValidationError::unreachable_code_sections;

	// // read header
	// if buf.Size() < 14 {
	// 	return nil, ErrIncompleteEOF
	// }
	// if buf.Size() > maxInitCodeSize {
	// 	fmt.Println("HITTING THIS")
	// 	return nil, fmt.Errorf("%w: %v > %v", ErrTooLargeByteCode, buf.Size(), maxInitCodeSize)
	// }
	// // Read Magic
	// if err := binary.Read(buf, binary.BigEndian, &container.Magic); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrInvalidMagic)
	// }
	// if container.Magic[0] != 0xef || container.Magic[1] != 0x0 {
	// 	return nil, fmt.Errorf("%w: want %x, got: %x", ErrInvalidMagic, eofMagic, container.Magic)
	// }

	// // Read Version
	// if err := binary.Read(buf, binary.BigEndian, &container.Version); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrInvalidVersion)
	// }
	// isSupportedVersion := false
	// for _, version := range supportedVersions {
	// 	if version == container.Version {
	// 		isSupportedVersion = true
	// 	}
	// }
	// if !isSupportedVersion {
	// 	return nil, fmt.Errorf("%w: have %d", ErrInvalidVersion, container.Version)
	// }

	// jt := jumpTables[container.Version-1]

	// // Read Types Section
	// var kind byte
	// if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	// }

	// if kind != _kindTypes {
	// 	fmt.Println("HERE")
	// 	return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingTypeHeader, kind)
	// }

	// var typesLength uint16
	// if err := binary.Read(buf, binary.BigEndian, &typesLength); err != nil {
	// 	return nil, err
	// }
	// if typesLength < 4 || typesLength%4 != 0 {
	// 	return nil, fmt.Errorf("%w: type section size must be divisible by 4, have %d", ErrInvalidTypeSize, typesLength)
	// }
	// if typesLength/4 > 1024 {
	// 	return nil, fmt.Errorf("%w: type section must not exceed 4*1024, have %d", ErrInvalidTypeSize, typesLength)
	// }

	// // Read Code Section
	// if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrMissingCodeHeader)
	// }
	// if kind != _kindCode {
	// 	return nil, fmt.Errorf("%w: found section kind %x instead", ErrMissingCodeHeader, kind)
	// }
	// var numCodeSections uint16
	// if err := binary.Read(buf, binary.BigEndian, &numCodeSections); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeHeader)
	// }
	// var codeSizes []uint16
	// for i := uint16(0); i < numCodeSections; i++ {
	// 	var codeLength uint16
	// 	if err := binary.Read(buf, binary.BigEndian, &codeLength); err != nil {
	// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidCodeSize)
	// 	}
	// 	if codeLength == 0 {
	// 		return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidCodeSize, i)
	// 	}
	// 	codeSizes = append(codeSizes, codeLength)
	// }
	// if len(codeSizes) != int(typesLength)/4 { // invalid code section count or type section count
	// 	return nil, fmt.Errorf("%w: mismatch of code sections count and type signatures, types %d, code %d", ErrInvalidSectionCount, typesLength/4, len(codeSizes))
	// }

	// // Read SubContainer Section
	// if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrMissingTypeHeader)
	// }

	// var subContainerSizes []uint16
	// var numSubContainers uint16
	// if kind == _kindContainer {
	// 	if err := binary.Read(buf, binary.BigEndian, &numSubContainers); err != nil {
	// 		return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerArgument)
	// 	}
	// 	if numSubContainers > 256 {
	// 		return nil, ErrTooManyContainerSections
	// 	}
	// 	for i := uint16(0); i < numSubContainers; i++ {
	// 		var subContainerLength uint16
	// 		if err := binary.Read(buf, binary.BigEndian, &subContainerLength); err != nil {
	// 			return nil, fmt.Errorf("%w: %w", err, ErrInvalidContainerSize)
	// 		}
	// 		if subContainerLength == 0 {
	// 			return nil, fmt.Errorf("%w for section %d: size must not be 0", ErrInvalidContainerSize, i)
	// 		}
	// 		subContainerSizes = append(subContainerSizes, subContainerLength)
	// 		if len(subContainerSizes) > 256 {
	// 			return nil, ErrTooManyContainerSections
	// 		}
	// 	}
	// 	if len(subContainerSizes) == 0 {
	// 		return nil, ErrZeroSizeContainerSection
	// 	}
	// 	// Read Data Section
	// 	if err := binary.Read(buf, binary.BigEndian, &kind); err != nil {
	// 		return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
	// 	}
	// }

	// if kind != _kindData {
	// 	return nil, fmt.Errorf("%w: found section %x instead", ErrMissingDataHeader, kind)
	// }
	// var dataLength uint16
	// if err := binary.Read(buf, binary.BigEndian, &dataLength); err != nil {
	// 	fmt.Println("THIS ERRRRR")
	// 	return nil, fmt.Errorf("%w: %w", err, ErrMissingDataHeader)
	// }

	// // Read Terminator
	// var terminator byte
	// if err := binary.Read(buf, binary.BigEndian, &terminator); err != nil {
	// 	return nil, fmt.Errorf("%w: %w", err, ErrMissingTerminator)
	// }
	// if terminator != 0 {
	// 	return nil, ErrMissingTerminator
	// }

	// // Validate Body
	// expectedRemaining := int(header.numTypeSections)
	// for _, codeSize := range header.codeSectionSizes {
	// 	expectedRemaining += int(codeSize)
	// }
	// for _, subContainerSize := range header.subContainerSizes {
	// 	expectedRemaining += int(subContainerSize)
	// }
	// if expectedRemaining > buf.Len() {
	// 	return nil, fmt.Errorf("%w: %d > %d", ErrInvalidSectionsSize, expectedRemaining, buf.Len())
	// }
	// if expectedRemaining < buf.Len()-int(header.dataLength) {
	// 	return nil, fmt.Errorf("%w: %d < %d", ErrInvalidSectionsSize, expectedRemaining, buf.Len()-int(header.dataLength))
	// }
	// fmt.Println("expectedRemaining: ", expectedRemaining)
	// fmt.Println("buf.Len(): ", buf.Len())
	// dataOffset := (int(buf.Size()) - buf.Len()) + expectedRemaining
	// if dataOffset == int(buf.Size()) && header.dataLength > 0 && depth == 0 /* top level container */ {
	// 	return nil, ErrTopLevelTruncated
	// }
	// if dataOffset+int(header.dataLength) > int(buf.Size()) {
	// 	if depth == 0 {
	// 		return nil, ErrTopLevelTruncated
	// 	}
	// 	if containerKind == initcode {
	// 		return nil, ErrEOFCreateWithTruncatedContainer
	// 	}
	// }
	// if dataOffset+int(header.dataLength) < int(buf.Size()) {
	// 	return nil, fmt.Errorf("%w: %d + %d > %d", ErrInvalidSectionsSize, dataOffset, header.dataLength, buf.Size())
	// }
	// fmt.Println("dataOffset: ", dataOffset)
	// fmt.Println("dataLength: ", dataLength)
	// fmt.Println("buf.Size(): ", buf.Size())