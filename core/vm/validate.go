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
	"io"

	"github.com/ledgerwatch/erigon/params"
)

var (
	ErrUndefinedInstruction   = errors.New("undefined instrustion")
	ErrTruncatedImmediate     = errors.New("truncated immediate")
	ErrInvalidSectionArgument = errors.New("invalid section argument")
	ErrInvalidJumpDest        = errors.New("invalid jump destination")
	ErrConflictingStack       = errors.New("conflicting stack height")
	ErrInvalidBranchCount     = errors.New("invalid number of branches in jump table")
	ErrInvalidOutputs         = errors.New("invalid number of outputs")
	ErrInvalidMaxStackHeight  = errors.New("invalid max stack height")
	ErrInvalidCodeTermination = errors.New("invalid code termination")
	ErrUnreachableCode        = errors.New("unreachable code")
	ErrInvalidDataLoadN       = errors.New("invalid DATALOADN index")
	ErrEOFStackOverflow       = errors.New("stack overflow")
	ErrJUMPFOutputs           = errors.New("current secion outputs less then target section outputs")
	ErrStackHeightHigher      = errors.New("stack height higher then outputs required")
	ErrNoTerminalInstruction  = errors.New("expected terminal instruction")
	ErrStackHeightMismatch    = errors.New("stack height mismatch")
	ErrCALLFtoNonReturning    = errors.New("op CALLF to non returning function")
)

// validateCode validates the code parameter against the EOF v1 validity requirements.
func validateCode(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable, dataSize int) error {
	var (
		i = 0
		// Tracks the number of actual instructions in the code (e.g.
		// non-immediate values). This is used at the end to determine
		// if each instruction is reachable.
		count    = 0
		op       OpCode
		analysis bitvec
	)
	// This loop visits every single instruction and verifies:
	// * if the instruction is valid for the given jump table.
	// * if the instruction has an immediate value, it is not truncated.
	// * if performing a relative jump, all jump destinations are valid.
	// * if changing code sections, the new code section index is valid and
	//   will not cause a stack overflow.
	for i < len(code) {
		count++
		op = OpCode(code[i])
		if jt[op].undefined {
			return fmt.Errorf("%w: op %s, pos %d", ErrUndefinedInstruction, op, i)
		}
		switch {
		case op >= PUSH1 && op <= PUSH32:
			size := int(op - PUSH0)
			if len(code) <= i+size {
				return fmt.Errorf("%w: op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			i += size
		case op == RJUMP || op == RJUMPI:
			if len(code) <= i+2 {
				return fmt.Errorf("%w: op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			if err := checkDest(code, &analysis, i+1, i+3, len(code)); err != nil {
				return err
			}
			i += 2
		case op == RJUMPV:
			if len(code) <= i+1 {
				return fmt.Errorf("%w: jump table size missing, op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			count := int(code[i+1])
			if count == 0 {
				return fmt.Errorf("%w: must not be 0, pos %d", ErrInvalidBranchCount, i)
			}
			if len(code) <= i+count {
				return fmt.Errorf("%w: jump table truncated, op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			for j := 0; j < count; j++ {
				if err := checkDest(code, &analysis, i+2+j*2, i+2*count+2, len(code)); err != nil {
					return err
				}
			}
			i += 1 + 2*count
		case op == CALLF:

			// const auto fid = read_uint16_be(&code[i + 1]);
			// if (fid >= header.types.size())
			//     return EOFValidationError::invalid_code_section_index;
			// if (header.types[fid].outputs == NON_RETURNING_FUNCTION)
			//     return EOFValidationError::callf_to_non_returning_function;
			// if (code_idx != fid)
			//     accessed_code_sections.insert(fid);
			// i += 2;

			if i+2 >= len(code) {
				return fmt.Errorf("%w: op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			fid, _ := parseUint16(code[i+1:]) // function id
			if fid >= len(metadata) {
				return fmt.Errorf("%w: arg %d, last %d, pos %d", ErrInvalidSectionArgument, fid, len(metadata), i)
			}
			if metadata[fid].Output == nonReturningFunction {
				return fmt.Errorf("%w, op: %s, pos: %d", ErrCALLFtoNonReturning, op, i)
			}
			count += 2
			i += 2
		case op == JUMPF:
			if i+2 >= len(code) {
				return fmt.Errorf("%w: op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			fid, _ := parseUint16(code[i+1:]) // function id
			fmt.Println("FID: ", fid)
			if fid >= len(metadata) {
				return fmt.Errorf("%w: arg %d, last %d, pos %d", ErrInvalidSectionArgument, fid, len(metadata), i)
			}
			count += 2
			i += 2
		case op == DATALOADN:
			if i+2 >= len(code) {
				return fmt.Errorf("%w: op %s, pos %d", ErrTruncatedImmediate, op, i)
			}
			arg, _ := parseUint16(code[i+1:]) // read index
			if dataSize < 32 || arg > dataSize-32 {
				return fmt.Errorf("%w: op %s, pos %d", ErrInvalidDataLoadN, op, i)
			}
			fmt.Println("DATA SIZE, INDEX: ", dataSize, arg)
			i += 2
		}
		i += 1
	}
	// Code sections may not "fall through" and require proper termination.
	// Therefore, the last instruction must be considered terminal.
	if !jt[op].terminal {
		return fmt.Errorf("%w: end with %s, pos %d", ErrInvalidCodeTermination, op, i)
	}
	if _, err := validateControlFlow2(code, section, metadata, jt); err != nil {
		return err
	}
	//  else if paths != count {
	// 	// TODO(matt): return actual position of unreachable code
	// 	fmt.Println("paths, count: ", paths, count)
	// 	return ErrUnreachableCode
	// }
	return nil
}

// checkDest parses a relative offset at code[0:2] and checks if it is a valid jump destination.
func checkDest(code []byte, analysis *bitvec, imm, from, length int) error {
	if len(code) < imm+2 {
		return io.ErrUnexpectedEOF
	}
	if analysis != nil && *analysis == nil {
		*analysis = eofCodeBitmap(code)
	}
	offset := parseInt16(code[imm:])
	dest := from + offset
	if dest < 0 || dest >= length {
		return fmt.Errorf("%w: out-of-bounds offset: offset %d, dest %d, pos %d", ErrInvalidJumpDest, offset, dest, imm)
	}
	if !analysis.codeSegment(uint64(dest)) {
		return fmt.Errorf("%w: offset into immediate: offset %d, dest %d, pos %d", ErrInvalidJumpDest, offset, dest, imm)
	}
	return nil
}

type stackHeightRange struct {
	min int
	max int
}

func (st *stackHeightRange) visited() bool { return st.min != -1 }

func visitSuccessor(currentOffset, nextOffset int, stackRequired stackHeightRange, stackHeights *[]stackHeightRange) bool {
	fmt.Printf("Visitin successor: currentOffset: %v, nextOffset: %v, stckRequire.max: %v, stckRequired.min: %v\n", currentOffset, nextOffset, stackRequired.max, stackRequired.min)
	nextStackHeight := (*stackHeights)[nextOffset]
	fmt.Printf("next stack: nextStack.max: %v, nextStack.min: %v\n", nextStackHeight.max, nextStackHeight.min)
	if nextOffset <= currentOffset { // backwards jump
		if !nextStackHeight.visited() {
			panic("successor wasn't visited")
		}
		return nextStackHeight.min == stackRequired.min && nextStackHeight.max == stackRequired.max
	} else if !nextStackHeight.visited() { // forwards jump, new target
		fmt.Println("NOT VISITED")
		nextStackHeight = stackRequired
	} else { // forwards jump, target known
		nextStackHeight.min = min(stackRequired.min, nextStackHeight.min)
		nextStackHeight.max = max(stackRequired.max, nextStackHeight.max)
	}
	(*stackHeights)[nextOffset] = nextStackHeight
	fmt.Printf("Setting nextStackHeight.max: %v, nextStackHeigh.min: %v, offset: %v\n", nextStackHeight.max, nextStackHeight.min, nextOffset)
	return true
}

func validateControlFlow2(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable) (int, error) {
	fmt.Printf("section: %v, len(code): %v\n", section, len(code))
	stackHeights := make([]stackHeightRange, len(code))
	for i := 1; i < len(code); i++ {
		stackHeights[i] = stackHeightRange{min: -1, max: -1}
	}
	stackHeights[0] = stackHeightRange{min: int(metadata[section].Input), max: int(metadata[section].Input)}

	for pos := 0; pos < len(code); {
		op := OpCode(code[pos])
		fmt.Printf("At position: %v, op: %v, opcodeHex: %v\n", pos, opCodeToString[op], code[pos])
		stackHeightRequired := jt[op].numPop // how many stack items required by the instruction
		stackHeightChange := jt[op].numPush
		fmt.Println("STACK HEIGHT REQUIRED: ", stackHeightRequired)
		stackHeight := stackHeights[pos]

		if !stackHeight.visited() {
			fmt.Println("HITTING THIS")
			return 0, ErrUnreachableCode
		}

		if op == CALLF {
			fid := parseInt16(code[pos+1:]) // function id
			stackHeightRequired = int(metadata[fid].Input)
			if stackHeight.max+int(metadata[fid].MaxStackHeight)+stackHeightRequired > stackSizeLimit {
				return 0, ErrEOFStackOverflow
			}
			if metadata[fid].Output == nonReturningFunction {
				panic("CALLF returning")
			}
			stackHeightChange = int(metadata[fid].Output)
		} else if op == JUMPF {
			fid := parseInt16(code[pos+1:]) // function id
			if stackHeight.max+int(metadata[fid].MaxStackHeight)-int(metadata[fid].Input) > stackSizeLimit {
				return 0, ErrEOFStackOverflow
			}

			if metadata[fid].Output == nonReturningFunction {
				stackHeightRequired = int(metadata[fid].Input)
			} else { // returning function
				// type[current_section_index].outputs MUST be greater or equal type[target_section_index].outputs,
				// or type[target_section_index].outputs MUST be 0x80, checked above
				if metadata[section].Output < metadata[fid].Output {
					return 0, ErrJUMPFOutputs
				}

				stackHeightRequired = int(metadata[section].Output) + int(metadata[fid].Input) - int(metadata[fid].Output)

				if stackHeight.max > stackHeightRequired {
					return 0, ErrStackHeightHigher
				}
			}
		} else if op == RETF {
			stackHeightRequired = int(metadata[section].Output)
			if stackHeight.max > stackHeightRequired {
				fmt.Printf("RETF: stackHeight.max: %v, stackHeightRequired: %v\n", stackHeight.max, stackHeightRequired)
				return 0, ErrStackHeightHigher
			}
		} else if op == DUPN {
			stackHeightRequired = int(code[pos+1]) + 1
		} else if op == SWAPN {
			stackHeightRequired = int(code[pos+1]) + 1
		} else if op == EXCHANAGE {
			n := (int(code[pos+1]) >> 4) + 1
			m := (int(code[pos+1]) & 0x0F) + 1
			stackHeightRequired = n + m + 1
		}

		if stackHeight.min < stackHeightRequired {
			return 0, ErrEOFStackOverflow
		}
		fmt.Printf("before setting next: StackHeight.Max: %v, StackHeight: %v\n", stackHeight.max, stackHeight.min)
		nextStackHeight := stackHeightRange{min: stackHeight.min + stackHeightChange, max: stackHeight.max + stackHeightChange}

		var immSize int // immediate size
		if op == RJUMPV {
			immSize = 1 + int(code[pos+1]) + 1*2 // (size of int16)
		} else {
			immSize = int(jt[op].immediateSize)
		}
		fmt.Println("immSize: ", immSize)

		next := pos + immSize + 1 // offset of the next instruction (may be invalid)
		fmt.Println("next: ", next)
		// check validity of next instuction, skip RJUMP and termination instructions
		if !jt[op].terminal && op != RJUMP {
			fmt.Println("VISITIN NEXXT")
			if next >= len(code) {
				fmt.Println("next >= len(code)")
				return 0, ErrNoTerminalInstruction
			}
			if !visitSuccessor(pos, next, nextStackHeight, &stackHeights) {
				return 0, ErrStackHeightMismatch
			}
		}

		if op == RJUMP || op == RJUMPI {
			targetRelOffset := parseInt16(code[pos+1:])
			target := pos + int(targetRelOffset) + 3

			if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
				return 0, ErrStackHeightMismatch
			}
		} else if op == RJUMPV {
			maxIndex := code[pos+1]
			for i := 0; i <= int(maxIndex); i++ {
				targetRelOffset := parseInt16(code[pos+i*2+2:])
				target := pos + int(next) + int(targetRelOffset)

				if !visitSuccessor(pos, target, nextStackHeight, &stackHeights) {
					return 0, ErrStackHeightMismatch
				}
			}
		}

		pos = next
	}

	// const auto max_stack_height_it = std::max_element(stack_heights.begin(), stack_heights.end(),
	//     [](StackHeightRange lhs, StackHeightRange rhs) noexcept { return lhs.max < rhs.max; });
	// return max_stack_height_it->max;
	return len(stackHeights), nil
}

// validateControlFlow iterates through all possible branches the provided code
// value and determines if it is valid per EOF v1.
func validateControlFlow(code []byte, section int, metadata []*FunctionMetadata, jt *JumpTable) (int, error) {
	type item struct {
		pos    int
		height int
	}
	// TODO(racytech): compares this to EVMone's stack validation
	var (
		heights        = make(map[int]int)
		worklist       = []item{{0, int(metadata[section].Input)}}
		maxStackHeight = int(metadata[section].Input)
	)
	for 0 < len(worklist) {
		var (
			idx    = len(worklist) - 1
			pos    = worklist[idx].pos
			height = worklist[idx].height
		)
		worklist = worklist[:idx]
	outer:
		for pos < len(code) {
			op := OpCode(code[pos])

			// Check if pos has already be visited; if so, the stack heights should be the same.
			if want, ok := heights[pos]; ok {
				if height != want {
					return 0, fmt.Errorf("%w: have %d, want %d", ErrConflictingStack, height, want)
				}
				// Already visited this path and stack height
				// matches.
				break
			}
			heights[pos] = height

			// Validate height for current op and update as needed.
			if want, have := jt[op].numPop, height; want > have {
				return 0, fmt.Errorf("%w: at pos %d", ErrStackUnderflow{stackLen: have, required: want}, pos)
			}
			if want, have := jt[op].maxStack, height; want < have {
				return 0, fmt.Errorf("%w: at pos %d", ErrStackOverflow{stackLen: have, limit: want}, pos)
			}
			height += int(params.StackLimit) - jt[op].maxStack

			switch {
			case op == CALLF:
				arg, _ := parseUint16(code[pos+1:])
				if want, have := int(metadata[arg].Input), height; want > have {
					return 0, fmt.Errorf("%w: at pos %d", ErrStackUnderflow{stackLen: have, required: want}, pos)
				}
				if have, limit := int(metadata[arg].Output)+height, int(params.StackLimit); have > limit {
					return 0, fmt.Errorf("%w: at pos %d", ErrStackOverflow{stackLen: have, limit: limit}, pos)
				}
				height -= int(metadata[arg].Input)
				height += int(metadata[arg].Output)
				pos += 3
			case op == RETF:
				if have, want := int(metadata[section].Output), height; have != want {
					return 0, fmt.Errorf("%w: have %d, want %d, at pos %d", ErrInvalidOutputs, have, want, pos)
				}
				break outer
			case op == RJUMP:
				arg := parseInt16(code[pos+1:])
				pos += 3 + arg
			case op == RJUMPI:
				arg := parseInt16(code[pos+1:])
				worklist = append(worklist, item{pos: pos + 3 + arg, height: height})
				pos += 3
			case op == RJUMPV:
				count := int(code[pos+1])
				for i := 0; i < count; i++ {
					arg := parseInt16(code[pos+2+2*i:])
					worklist = append(worklist, item{pos: pos + 2 + 2*count + arg, height: height})
				}
				pos += 2 + 2*count
			default:
				if op >= PUSH1 && op <= PUSH32 {
					pos += 1 + int(op-PUSH0)
				} else if jt[op].terminal {
					break outer
				} else {
					// Simple op, no operand.
					pos += 1
				}
			}
			maxStackHeight = max(maxStackHeight, height)
		}
	}
	if maxStackHeight != int(metadata[section].MaxStackHeight) {
		return 0, fmt.Errorf("%w in code section %d: have %d, want %d", ErrInvalidMaxStackHeight, section, maxStackHeight, metadata[section].MaxStackHeight)
	}
	return len(heights), nil
}
