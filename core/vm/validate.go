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
	"errors"
	"fmt"
)

const (
	REL_OFFSET_SIZE = 2 // size of uint16
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

// TODO(racytech): split validate code into validate_instructions and validate_rjump_destinations

func makeEOFerr(errOrigin, pos int, op OpCode, err error) error {
	errs := []string{"validateInstructions", "validateRjumpDestinations"}

	return fmt.Errorf("%s: %s - %w, pos %d", errs[errOrigin], op, err, pos)
}

// validateCode validates the code parameter against the EOF v1 validity requirements.
func validateCode(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable, dataSize, containerCount int) error {
	fmt.Println("-------------------------")
	if err := validateInstructions(code, section, metadata, jt, dataSize, containerCount); err != nil {
		return err
	}
	if err := validateRjumpDestinations(code, jt); err != nil {
		return err
	}
	if _, err := validateMaxStackHeight(code, section, metadata, jt); err != nil {
		return err
	}
	return nil
}

func validateInstructions(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable, dataSize, containerCount int) error {
	fmt.Println("--- validateInstructions")
	var (
		expectedReturning  = metadata[section].Outputs != nonReturningFunction
		isReturning        = false
		op                 OpCode
		codeSize           = len(code)
		accessCodeSections = map[int]bool{}
		subcontainerRefs   = make([][2]int, 0, len(metadata))
	)
	// fmt.Println("codeSize: ", codeSize)
	for pos := 0; pos < codeSize; pos++ {
		op = OpCode(code[pos])
		if jt[op].undefined {
			return makeEOFerr(0, pos, op, ErrUndefinedInstruction)
		}
		if pos+int(jt[op].immediateSize) >= codeSize {
			return makeEOFerr(0, pos, op, ErrTruncatedImmediate)
		}
		// fmt.Printf("%s ", op)
		if op == RJUMPV {
			count := int(code[pos+1]) + 1
			pos += (1 + count*2)
			if pos >= codeSize {
				return makeEOFerr(0, pos, op, ErrTruncatedImmediate)
			}
			// fmt.Printf("-> %s ", OpCode(code[pos]))
		} else {
			if op == CALLF {
				fid, _ := parseUint16(code[pos+1:]) // function id
				if fid >= len(metadata) {
					return makeEOFerr(0, pos, op, ErrInvalidSectionArgument)
				}
				if metadata[fid].Outputs == nonReturningFunction {
					return makeEOFerr(0, pos, op, ErrInvalidSectionArgument)
				}
				if section != fid {
					accessCodeSections[fid] = true
				}
			} else if op == RETF {
				isReturning = true
			} else if op == JUMPF {
				// const auto fid = read_uint16_be(&code[i + 1]);
				// if (fid >= header.types.size())
				// 	return EOFValidationError::invalid_code_section_index;
				// // JUMPF into returning function means current function is returning.
				// if (header.types[fid].outputs != NON_RETURNING_FUNCTION)
				// 	is_returning = true;
				// if (code_idx != fid)
				// 	accessed_code_sections.insert(fid);
				// i += 2;

				fid, _ := parseUint16(code[pos+1:])
				fmt.Println("Function ID: ", fid)
				if fid >= len(metadata) {
					fmt.Println("HITTING THIS ERR: JUMPF")
					return makeEOFerr(0, pos, op, ErrInvalidSectionArgument)
				}
				if metadata[fid].Outputs != nonReturningFunction {
					isReturning = true
				}
				if section != fid {
					accessCodeSections[fid] = true
				}
			} else if op == DATALOADN {
				index, _ := parseUint16(code[pos+1:])
				if dataSize < 32 || index > dataSize-32 {
					return makeEOFerr(0, pos, op, ErrInvalidDataLoadN)
				}
			} else if op == EOFCREATE || op == RETURNCONTRACT {
				containerIDX := int(code[pos+1])
				if containerIDX >= containerCount {
					return makeEOFerr(0, pos, op, ErrInvalidContainerArgument)
				}
				subcontainerRefs = append(subcontainerRefs, [2]int{containerIDX, int(op)})
			}
			// for j := pos + 1; j <= pos+int(jt[op].immediateSize); j++ {
			// 	fmt.Printf("(%x) ", code[j])
			// }
			pos += int(jt[op].immediateSize)
		}
	}

	// fmt.Println("")

	if isReturning != expectedReturning {
		return ErrInvalidNonReturning
	}
	return nil
}

func checkRjumpDest(codeSize, postPos, relOffset int, rjumpDests *[]int) bool {

	jumpDest := postPos + relOffset
	// fmt.Printf("relOffset: %v, postPos: %v, jumpDest: %v\n", relOffset, postPos, postPos+relOffset)
	if jumpDest < 0 || jumpDest >= codeSize {
		return false
	}

	*rjumpDests = append(*rjumpDests, jumpDest)

	return true
}

func validateRjumpDestinations(code []byte, jt *JumpTable) error {
	fmt.Println("--- validateRjumpDestinations")
	var (
		codeSize     = len(code)
		rjumpDests   = make([]int, 0)
		immediateMap = make([]bool, codeSize)
		op           OpCode
	)
	// fmt.Println("CODE SIZE: ", codeSize)
	for pos := 0; pos < codeSize; pos++ {
		op = OpCode(code[pos])
		immSize := int(jt[op].immediateSize)
		if op == RJUMP || op == RJUMPI { // both RJUMP and RJUMPI have 2 bytes immediates
			relOffset := parseInt16(code[pos+1:])
			postPos := pos + REL_OFFSET_SIZE + 1
			if !checkRjumpDest(codeSize, postPos, relOffset, &rjumpDests) {
				return makeEOFerr(1, pos, op, ErrInvalidRjumpDest)
			}
		} else if op == RJUMPV { // 1 byte immediate
			// fmt.Println("RJUMPV")
			count := int(code[pos+1]) + 1
			immSize += count * REL_OFFSET_SIZE
			postPos := pos + 1 + immSize
			for j := 0; j < count*REL_OFFSET_SIZE; j += REL_OFFSET_SIZE {
				relOffset := parseInt16(code[pos+1+1+j:])
				if !checkRjumpDest(codeSize, postPos, relOffset, &rjumpDests) {
					return makeEOFerr(1, pos, op, ErrInvalidRjumpDest)
				}
			}
		}
		for j := pos + 1; j <= pos+immSize; j++ {
			immediateMap[j] = true
		}
		pos += immSize
	}
	// fmt.Println(rjumpDests)
	// fmt.Println(immediateMap)
	for _, dest := range rjumpDests {
		if immediateMap[dest] {
			return fmt.Errorf("%w: immediateMap[dest] is true: dest-%v", ErrInvalidRjumpDest, dest)
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
			// fmt.Printf("current offset: %v, next offset %v\n", currentOffset, nextOffset)
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

func validateMaxStackHeight(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable) (int, error) {
	fmt.Println("--- validateMaxStackHeight")
	// fmt.Println("outputs: ", metadata[section].Outputs)
	stackHeights := make([]stackHeightRange, len(code))
	for i := 1; i < len(code); i++ {
		stackHeights[i] = stackHeightRange{min: -1, max: -1}
	}
	stackHeights[0] = stackHeightRange{min: int(metadata[section].Inputs), max: int(metadata[section].Inputs)}

	for pos := 0; pos < len(code); {
		op := OpCode(code[pos])
		// fmt.Printf("%v ", op)
		stackHeightRequired := jt[op].numPop // how many stack items required by the instruction
		stackHeightChange := 0
		if stackHeightRequired != jt[op].numPush {
			stackHeightChange = jt[op].numPush - stackHeightRequired // can be negative
		}
		stackHeight := stackHeights[pos]

		if !stackHeight.visited() {
			// fmt.Println("hittin this err")
			return 0, ErrUnreachableCode
		}

		if op == CALLF {
			fid, _ := parseUint16(code[pos+1:]) // function id
			stackHeightRequired = int(metadata[fid].Inputs)
			if stackHeight.max+int(metadata[fid].MaxStackHeight)-stackHeightRequired > stackSizeLimit {
				return 0, ErrEOFStackOverflow
			}
			if metadata[fid].Outputs == nonReturningFunction {
				panic("CALLF returning") // TODO(racytech): handle panics!!!
			}
			stackHeightChange = int(metadata[fid].Outputs) - stackHeightRequired
		} else if op == JUMPF {
			fid, _ := parseUint16(code[pos+1:]) // function id
			if stackHeight.max+int(metadata[fid].MaxStackHeight)-int(metadata[fid].Inputs) > stackSizeLimit {
				return 0, ErrEOFStackOverflow
			}

			if metadata[fid].Outputs == nonReturningFunction {
				stackHeightRequired = int(metadata[fid].Inputs)
			} else { // returning function
				// type[current_section_index].outputs MUST be greater or equal type[target_section_index].outputs,
				// or type[target_section_index].outputs MUST be 0x80, checked above
				if metadata[section].Outputs < metadata[fid].Outputs {
					return 0, ErrJUMPFOutputs
				}
				stackHeightRequired = int(metadata[section].Outputs) + int(metadata[fid].Inputs) - int(metadata[fid].Outputs)
				if stackHeight.max > stackHeightRequired {
					// fmt.Println("HITTING THIS 0")
					return 0, ErrStackHeightHigher
				}
			}
		} else if op == RETF {
			stackHeightRequired = int(metadata[section].Outputs)
			// fmt.Println("stack height max: ", stackHeight.max)
			if stackHeight.max > stackHeightRequired {
				// fmt.Println("HITTING THIS 2")
				return 0, ErrStackHeightHigher
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
			return 0, ErrEOFStackUnderflow
		}

		nextStackHeight := stackHeightRange{min: stackHeight.min + stackHeightChange, max: stackHeight.max + stackHeightChange}
		immSize := int(jt[op].immediateSize)
		if op == RJUMPV {
			immSize = 1 + (int(code[pos+1])+1)*REL_OFFSET_SIZE // (size of int16)
		}

		next := pos + immSize + 1 // offset to the next instruction (may be invalid)
		// check validity of next instuction, skip RJUMP and termination instructions
		if !jt[op].terminal && op != RJUMP {
			if next >= len(code) {
				fmt.Println("Hitting this err")
				return 0, ErrNoTerminalInstruction
			}
			if !visitSuccessor(pos, next, nextStackHeight, &stackHeights) {
				return 0, ErrStackHeightMismatch
			}
		}

		if op == RJUMP || op == RJUMPI {
			targetRelOffset := parseInt16(code[pos+1:])
			target := pos + targetRelOffset + 3

			if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
				return 0, ErrStackHeightMismatch
			}
		} else if op == RJUMPV {
			maxIndex := int(code[pos+1])
			for i := 0; i <= maxIndex; i++ {
				targetRelOffset := parseInt16(code[pos+i*REL_OFFSET_SIZE+2:])
				target := next + targetRelOffset
				if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
					return 0, ErrStackHeightMismatch
				}
			}
		}

		pos = next
	}
	// fmt.Println("")

	return len(stackHeights), nil
}
