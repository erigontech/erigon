package vm

import (
	"encoding/binary"
	"fmt"
)

// parseUint16 parses a 16 bit BigEndian unsigned integer.
func parseUint16(b []byte) (uint16, error) {
	if len(b) < 2 {
		// return 0, io.ErrUnexpectedEOF
		panic("parseUint16: len(b) < 2") // TODO(racytech): undo this when done with tests
	}
	return binary.BigEndian.Uint16(b), nil
}

// parseInt16 parses a 16 bit signed integer.
func parseInt16(b []byte) int {
	return int(int16(b[1]) | int16(b[0])<<8)
}

func getTypeSectionOffset(idx uint16, header *eofHeader) uint16 {
	return header.typesOffset + idx*4
}

func isTerminal(op OpCode) bool {
	return op == RETURNCODE || op == JUMPF || op == RETF ||
		op == REVERT || op == STOP || op == RETURN || op == INVALID
}

func validateInstructions(eofCode []byte, codeIdx uint16, header *eofHeader, offset uint16, containerKind byte, jt *JumpTable) ([][2]uint16, map[uint16]bool, error) {

	var (
		code               = eofCode[offset : offset+header.codeSizes[codeIdx]]
		codeSize           = len(code)
		typeSectionOffset  = getTypeSectionOffset(codeIdx, header)
		expectedReturning  = eofCode[typeSectionOffset+1] != nonReturningFunction
		isReturning        = false
		op                 OpCode
		accessCodeSections = map[uint16]bool{}
		subcontainerRefs   = make([][2]uint16, 0, len(header.codeSizes))
	)
	pos := 0
	for ; pos < codeSize; pos++ {
		op = OpCode(code[pos])
		if jt[op].undefined {
			return nil, nil, fmt.Errorf("EOFException.UNDEFINED_INSTRUCTION")
		}
		if pos+int(jt[op].immediateSize) >= codeSize {
			return nil, nil, fmt.Errorf("EOFException.TRUNCATED_INSTRUCTION")
		}

		if op == RJUMPV {
			count := int(code[pos+1]) + 1
			pos += (1 + count*2)
			if pos >= codeSize {
				return nil, nil, fmt.Errorf("EOFException.TRUNCATED_INSTRUCTION")
			}
		} else {
			if op == CALLF {
				fid, _ := parseUint16(code[pos+1:])
				if int(fid) >= len(header.codeSizes) {
					return nil, nil, fmt.Errorf("EOFException.INVALID_CODE_SECTION_INDEX: %d", fid)
				}
				typesOffset := getTypeSectionOffset(fid, header)
				if eofCode[typesOffset+1] == nonReturningFunction {
					return nil, nil, fmt.Errorf("EOFException.CALLF_TO_NON_RETURNING: %d", fid)
				}
				if codeIdx != fid {
					accessCodeSections[fid] = true
				}
			} else if op == RETF {
				isReturning = true
			} else if op == JUMPF {
				fid, _ := parseUint16(code[pos+1:])
				if int(fid) >= len(header.codeSizes) {
					return nil, nil, fmt.Errorf("EOFException.INVALID_CODE_SECTION_INDEX: %d", fid)
				}
				typesOffset := getTypeSectionOffset(fid, header)
				if eofCode[typesOffset+1] != nonReturningFunction {
					isReturning = true
				}
				if codeIdx != fid {
					accessCodeSections[fid] = true
				}
			} else if op == DATALOADN {
				index, _ := parseUint16(code[pos+1:])
				if header.dataSize < 32 || index > header.dataSize-32 {
					return nil, nil, fmt.Errorf("EOFException.INVALID_DATALOADN_INDEX: %d", index)
				}
			} else if op == EOFCREATE || op == RETURNCODE {
				containerIDX := uint16(code[pos+1])
				if int(containerIDX) >= len(header.containerSizes) {
					return nil, nil, fmt.Errorf("EOFException.INVALID_CONTAINER_SECTION_INDEX: %d", containerIDX)
				}
				if op == RETURNCODE && containerKind == runtime {
					return nil, nil, fmt.Errorf("EOFException.INCOMPATIBLE_CONTAINER_KIND")
				}
				subcontainerRefs = append(subcontainerRefs, [2]uint16{containerIDX, uint16(op)})
			} else if op == RETURN || op == STOP {
				if containerKind == initcode {
					return nil, nil, fmt.Errorf("EOFException.INCOMPATIBLE_CONTAINER_KIND")
				}
			}
			pos += int(jt[op].immediateSize)
		}
	}
	// Code sections may not "fall through" and require proper termination.
	// Therefore, the last instruction must be considered "terminal"
	if !isTerminal(op) && op != RJUMP {
		return nil, nil, fmt.Errorf("EOFException.MISSING_STOP_OPCODE")
	}

	if isReturning != expectedReturning {
		return nil, nil, fmt.Errorf("EOFException.INVALID_NON_RETURNING_FLAG")
	}

	return subcontainerRefs, accessCodeSections, nil
}

const REL_OFFSET_SIZE = 2 // size of uint16
func checkRjumpDest(codeSize, postPos, relOffset int, rjumpDests *[]int) bool {

	jumpDest := postPos + relOffset
	if jumpDest < 0 || jumpDest >= codeSize {
		return false
	}

	*rjumpDests = append(*rjumpDests, jumpDest)

	return true
}

func validateRjumpDestinations(eofCode []byte, header *eofHeader, jt *JumpTable, codeIdx, offset uint16) error {
	var (
		code         = eofCode[offset : offset+header.codeSizes[codeIdx]]
		codeSize     = len(code)
		rjumpDests   = make([]int, 0)
		immediateMap = make([]bool, codeSize)
		op           OpCode
	)

	for pos := 0; pos < codeSize; pos++ {
		op = OpCode(code[pos])
		immSize := int(jt[op].immediateSize)
		if op == RJUMP || op == RJUMPI { // both RJUMP and RJUMPI have 2 bytes immediates
			relOffset := parseInt16(code[pos+1:])
			postPos := pos + 2 + 1 // REL_OFFSET_SIZE = 2 // size of uint16
			if !checkRjumpDest(codeSize, postPos, relOffset, &rjumpDests) {
				return fmt.Errorf("EOFException.INVALID_RJUMP_DESTINATION: %d", relOffset)
			}
		} else if op == RJUMPV { // 1 byte immediate
			count := int(code[pos+1]) + 1
			immSize += count * REL_OFFSET_SIZE
			postPos := pos + 1 + immSize
			for j := 0; j < count*REL_OFFSET_SIZE; j += REL_OFFSET_SIZE {
				relOffset := parseInt16(code[pos+1+1+j:])
				if !checkRjumpDest(codeSize, postPos, relOffset, &rjumpDests) {
					return fmt.Errorf("EOFException.INVALID_RJUMP_DESTINATION: %d", relOffset)
				}
			}
		}
		for j := pos + 1; j <= pos+immSize; j++ {
			immediateMap[j] = true
		}
		pos += immSize
	}
	for _, dest := range rjumpDests {
		if immediateMap[dest] {
			return fmt.Errorf("EOFException.INVALID_RJUMP_DESTINATION")
		}
	}

	return nil
}

type stackHeightRange struct {
	min int
	max int
}

func (st *stackHeightRange) visited() bool { return st.min != -1 }

func visitSuccessor(currentOffset, nextOffset int, stackRequired stackHeightRange, stackHeights *[]stackHeightRange) bool {
	nextStackHeight := (*stackHeights)[nextOffset]
	if nextOffset <= currentOffset { // backwards jump
		if !nextStackHeight.visited() {
			panic("successor wasn't visited") // TODO(racytech): handle this better
		}
		return nextStackHeight.min == stackRequired.min && nextStackHeight.max == stackRequired.max
	} else if !nextStackHeight.visited() { // forwards jump, new target
		nextStackHeight = stackRequired
	} else { // forwards jump, target known
		nextStackHeight.min = min(stackRequired.min, nextStackHeight.min)
		nextStackHeight.max = max(stackRequired.max, nextStackHeight.max)
	}
	(*stackHeights)[nextOffset] = nextStackHeight
	return true
}

func validateMaxStackHeight(eofCode []byte, codeIdx uint16, header *eofHeader, offset uint16, jt *JumpTable) (int, error) {
	var (
		code = eofCode[offset : offset+header.codeSizes[codeIdx]]
		// typeSectionOffset = getTypeSectionOffset(codeIdx, header)
		stackHeights = make([]stackHeightRange, len(code))

		inputs, outputs, sectionMaxStack int
	)
	thisTypesOffset := getTypeSectionOffset(codeIdx, header)
	thisInputs := int(eofCode[thisTypesOffset])
	thisOutputs := int(eofCode[thisTypesOffset+1])

	for i := 1; i < len(code); i++ {
		stackHeights[i] = stackHeightRange{min: -1, max: -1}
	}
	stackHeights[0] = stackHeightRange{min: int(thisInputs), max: int(thisInputs)}
	for pos := 0; pos < len(code); {
		op := OpCode(code[pos])
		stackHeightRequired := jt[op].numPop // how many stack items required by the instruction
		stackHeightChange := 0
		if stackHeightRequired != jt[op].numPush {
			stackHeightChange = jt[op].numPush - stackHeightRequired // can be negative
		}
		stackHeight := stackHeights[pos]
		if !stackHeight.visited() {
			return 0, fmt.Errorf("EOFException.UNREACHABLE_INSTRUCTIONS")
		}

		if op == CALLF {
			fid, _ := parseUint16(code[pos+1:]) // function id
			typesOffset := getTypeSectionOffset(fid, header)
			inputs = int(eofCode[typesOffset])
			outputs = int(eofCode[typesOffset+1])
			sectionMaxStack = int(eofCode[typesOffset+2])<<8 | int(eofCode[typesOffset+3])
			stackHeightRequired = inputs
			if stackHeight.max+sectionMaxStack-stackHeightRequired > stackSizeLimit {
				return 0, fmt.Errorf("EOFException.STACK_OVERFLOW")
			}
			if outputs == nonReturningFunction {
				return 0, fmt.Errorf("EOFException.CALLF_TO_NON_RETURNING")
			}
			stackHeightChange = outputs - inputs
		} else if op == JUMPF {
			fid, _ := parseUint16(code[pos+1:]) // function id

			typesOffset := getTypeSectionOffset(fid, header)
			inputs = int(eofCode[typesOffset])
			outputs = int(eofCode[typesOffset+1])
			sectionMaxStack = int(eofCode[typesOffset+2])<<8 | int(eofCode[typesOffset+3])

			if stackHeight.max+sectionMaxStack-inputs > stackSizeLimit {
				return 0, fmt.Errorf("EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT")
			}

			if outputs == nonReturningFunction {
				stackHeightRequired = inputs
			} else { // returning function
				if thisOutputs < outputs {
					return 0, fmt.Errorf("EOFException.JUMPF_DESTINATION_INCOMPATIBLE_OUTPUTS")
				}
				stackHeightRequired = thisOutputs + inputs - outputs
				if stackHeight.max > stackHeightRequired {
					return 0, fmt.Errorf("EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT")
				}
			}
		} else if op == RETF {
			stackHeightRequired = thisOutputs
			if stackHeight.max > stackHeightRequired {
				return 0, fmt.Errorf("EOFException.STACK_HIGHER_THAN_OUTPUTS")
			}
		} else if op == DUPN {
			stackHeightRequired = int(code[pos+1]) + 1
		} else if op == SWAPN {
			stackHeightRequired = int(code[pos+1]) + 2
		} else if op == EXCHANGE {
			n := (int(code[pos+1]) >> 4) + 1
			m := (int(code[pos+1]) & 0x0F) + 1
			stackHeightRequired = n + m + 1
		}

		if stackHeight.min < stackHeightRequired {
			return 0, fmt.Errorf("EOFException.STACK_UNDERFLOW")
		}

		nextStackHeight := stackHeightRange{min: stackHeight.min + stackHeightChange, max: stackHeight.max + stackHeightChange}
		immSize := int(jt[op].immediateSize)
		if op == RJUMPV {
			immSize = 1 + (int(code[pos+1])+1)*REL_OFFSET_SIZE // (size of int16)
		}

		next := pos + immSize + 1 // offset to the next instruction (may be invalid)
		// check validity of next instuction, skip RJUMP and termination instructions
		if !isTerminal(op) && op != RJUMP {
			if next >= len(code) {
				return 0, fmt.Errorf("EOFException.INVALID_NEXT_INSTRUCTION")
			}
			if !visitSuccessor(pos, next, nextStackHeight, &stackHeights) {
				return 0, fmt.Errorf("EOFException.INVALID_NEXT_INSTRUCTION")
			}
		}

		if op == RJUMP || op == RJUMPI {
			targetRelOffset := parseInt16(code[pos+1:])
			target := pos + targetRelOffset + 3

			if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
				return 0, fmt.Errorf("EOFException.STACK_HEIGHT_MISMATCH")
			}
		} else if op == RJUMPV {
			maxIndex := int(code[pos+1])
			for i := 0; i <= maxIndex; i++ {
				targetRelOffset := parseInt16(code[pos+i*REL_OFFSET_SIZE+2:])
				target := next + targetRelOffset
				if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
					return 0, fmt.Errorf("EOFException.STACK_HEIGHT_MISMATCH")
				}
			}
		}
		pos = next
	}

	max := 0
	for _, height := range stackHeights {
		if height.max > max {
			max = height.max
		}
	}

	return max, nil
}
