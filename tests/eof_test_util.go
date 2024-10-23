package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/core/vm"
)

// A BlockTest checks handling of entire blocks.
type EOFTest struct {
	json eofJSON
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (e *EOFTest) UnmarshalJSON(in []byte) error {
	return json.Unmarshal(in, &e.json)
}

type eofJSON struct {
	Vector _vector `json:"vectors"`
}

type _vector struct {
	Index _index `json:"0"`
}

type _index struct {
	Code   string `json:"code"`
	Result struct {
		Osaka struct {
			Exception string `json:"exception"`
			Result    bool   `json:"result"`
		} `json:"Osaka"`
	} `json:"results"`
}

func parseError(err error) error {
	var _errors = []error{ // add new errors here
		vm.ErrUndefinedInstruction,
		vm.ErrIncompleteEOF,
		vm.ErrInvalidMagic,
		vm.ErrInvalidVersion,
		vm.ErrMissingTypeHeader,
		vm.ErrInvalidTypeSize,
		vm.ErrMissingCodeHeader,
		vm.ErrInvalidCodeHeader,
		vm.ErrInvalidCodeSize,
		vm.ErrMissingDataHeader,
		vm.ErrMissingTerminator,
		vm.ErrTooManyInputs,
		vm.ErrTooManyOutputs,
		vm.ErrInvalidFirstSectionType,
		vm.ErrTooLargeMaxStackHeight,
		vm.ErrInvalidContainerSize,
		vm.ErrInvalidMemoryAccess,
		vm.ErrInvalidCodeTermination,
		vm.ErrInvalidSectionArgument,
		vm.ErrInvalidMaxStackHeight,
		vm.ErrInvalidOutputs,
		vm.ErrInvalidDataLoadN,
		vm.ErrUnreachableCode,
		vm.ErrNoTerminalInstruction,
		vm.ErrCALLFtoNonReturning,
		vm.ErrEOFStackOverflow,
		vm.ErrStackHeightHigher,
		vm.ErrInvalidJumpDest,
		vm.ErrInvalidBranchCount,
		vm.ErrTruncatedImmediate,
		vm.ErrJUMPFOutputs,
		vm.ErrInvalidRjumpDest,
		vm.ErrInvalidContainerArgument,
		vm.ErrInvalidNonReturning,
		vm.ErrEOFStackUnderflow,
		io.ErrUnexpectedEOF,
		vm.ErrStackHeightMismatch,
		vm.ErrTooLargeByteCode,
		vm.ErrZeroSizeContainerSection,
		vm.ErrZeroContainerSize,
		vm.ErrTooManyContainerSections,
		vm.ErrInvalidSectionCount,
	}

	for _, _err := range _errors {
		if errors.Is(err, _err) {
			return _err
		}
	}
	return nil
}

var errorsMap = map[string][]error{
	"EOFException.INVALID_FIRST_SECTION_TYPE":             []error{vm.ErrInvalidFirstSectionType, vm.ErrTooManyInputs, vm.ErrTooManyOutputs, vm.ErrTooLargeMaxStackHeight, vm.ErrInvalidContainerSize},
	"EOFException.INCOMPLETE_SECTION_NUMBER":              []error{vm.ErrIncompleteEOF},
	"EOFException.MISSING_HEADERS_TERMINATOR":             []error{vm.ErrIncompleteEOF, io.ErrUnexpectedEOF},
	"EOFException.INCOMPLETE_SECTION_SIZE":                []error{vm.ErrIncompleteEOF, io.ErrUnexpectedEOF},
	"EOFException.TOO_MANY_CODE_SECTIONS":                 []error{vm.ErrInvalidTypeSize, vm.ErrIncompleteEOF},
	"EOFException.MISSING_CODE_HEADER":                    []error{vm.ErrMissingCodeHeader, vm.ErrIncompleteEOF},
	"EOFException.ZERO_SECTION_SIZE":                      []error{vm.ErrIncompleteEOF, vm.ErrInvalidTypeSize, vm.ErrInvalidFirstSectionType, vm.ErrInvalidCodeSize, vm.ErrZeroSizeContainerSection, vm.ErrZeroContainerSize, vm.ErrInvalidContainerSize, vm.ErrInvalidSectionCount},
	"EOFException.INVALID_SECTION_BODIES_SIZE":            []error{vm.ErrInvalidContainerSize, vm.ErrInvalidTypeSize, vm.ErrInvalidSectionCount},
	"EOFException.INVALID_MAGIC":                          []error{vm.ErrInvalidMagic},
	"EOFException.INVALID_VERSION":                        []error{vm.ErrInvalidVersion, vm.ErrIncompleteEOF},
	"EOFException.MISSING_TERMINATOR":                     []error{vm.ErrMissingTerminator, vm.ErrInvalidCodeSize, vm.ErrInvalidSectionCount},
	"EOFException.MISSING_TYPE_HEADER":                    []error{vm.ErrIncompleteEOF, vm.ErrMissingTypeHeader},
	"EOFException.MISSING_STOP_OPCODE":                    []error{vm.ErrInvalidCodeTermination},
	"EOFException.UNDEFINED_EXCEPTION":                    []error{vm.ErrTooManyOutputs, vm.ErrInvalidSectionArgument, vm.ErrInvalidCodeTermination, vm.ErrInvalidMaxStackHeight, vm.ErrInvalidOutputs, vm.ErrNoTerminalInstruction, vm.ErrCALLFtoNonReturning, vm.ErrStackHeightHigher},
	"EOFException.INVALID_DATALOADN_INDEX":                []error{vm.ErrInvalidDataLoadN},
	"EOFException.STACK_UNDERFLOW":                        []error{vm.ErrEOFStackUnderflow},
	"EOFException.TOPLEVEL_CONTAINER_TRUNCATED":           []error{vm.ErrInvalidContainerSize},
	"EOFException.INVALID_TYPE_SECTION_SIZE":              []error{vm.ErrInvalidCodeSize, vm.ErrInvalidTypeSize, vm.ErrInvalidSectionCount},
	"EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT":         []error{vm.ErrTooManyOutputs, vm.ErrTooManyInputs},
	"EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT":           []error{vm.ErrTooLargeMaxStackHeight},
	"EOFException.MISSING_DATA_SECTION":                   []error{vm.ErrMissingDataHeader, vm.ErrInvalidCodeSize, vm.ErrInvalidSectionCount},
	"EOFException.UNREACHABLE_CODE_SECTIONS":              []error{vm.ErrInvalidContainerSize, vm.ErrUndefinedInstruction, vm.ErrInvalidNonReturning, vm.ErrInvalidSectionArgument},
	"EOFException.UNDEFINED_INSTRUCTION":                  []error{vm.ErrUndefinedInstruction},
	"EOFException.INVALID_RJUMP_DESTINATION":              []error{vm.ErrInvalidRjumpDest, vm.ErrInvalidContainerArgument, vm.ErrInvalidNonReturning},
	"EOFException.UNREACHABLE_INSTRUCTIONS":               []error{vm.ErrUnreachableCode},
	"EOFException.TRUNCATED_INSTRUCTION":                  []error{vm.ErrTruncatedImmediate},
	"EOFException.HIGHER_THAN_OUTPUTS":                    []error{vm.ErrStackHeightHigher},
	"EOFException.JUMPF_DESTINATION_INCOMPATIBLE_OUTPUTS": []error{vm.ErrJUMPFOutputs},
	"EOFException.INVALID_NON_RETURNING_FLAG":             []error{vm.ErrInvalidNonReturning},
	"EOFException.STACK_HIGHER_THAN_OUTPUTS":              []error{vm.ErrStackHeightHigher},
	"EOFException.CONTAINER_SIZE_ABOVE_LIMIT":             []error{vm.ErrInvalidMagic, vm.ErrTooLargeByteCode}, // TODO(racytech): change this when tests get updated
	"EOFException.INVALID_CONTAINER_SECTION_INDEX":        []error{vm.ErrInvalidContainerArgument},
	"EOFException.STACK_HEIGHT_MISMATCH":                  []error{vm.ErrStackHeightMismatch},
	"EOFException.INVALID_CODE_SECTION_INDEX":             []error{vm.ErrInvalidSectionArgument},
	"EOFException.INVALID_MAX_STACK_HEIGHT":               []error{vm.ErrEOFStackUnderflow},
	"EOFException.TOO_MANY_CONTAINERS":                    []error{vm.ErrTooManyContainerSections},
}

func mapError(exception string, err error) bool {

	cmp := parseError(err)

	if cmp == nil {
		fmt.Println("Add err to the error array", err)
		panic("add err to getError func")
	}

	errs := errorsMap[exception]
	if len(errs) == 0 {
		fmt.Printf("exception was not added to map: %s\n", exception)
		panic("no mapped errors found for exception")
	}

	for _, er := range errs {
		if er == cmp {
			return true
		}
	}
	return false
}

func (e *EOFTest) Run(t *testing.T) error {
	hexCode := e.json.Vector.Index.Code
	// fmt.Println("hexCode: ", hexCode)
	result := e.json.Vector.Index.Result.Osaka.Result // TODO(racytech): revisit this part, think about result=true -> what to expect from test?
	exception := e.json.Vector.Index.Result.Osaka.Exception
	code, err := hexutil.Decode(hexCode)
	if err != nil {
		return fmt.Errorf("error decoding hex string: %v", hexCode)
	}
	fmt.Println("result: ", result)
	fmt.Println("exception: ", exception)
	fmt.Println("eof code size: ", len(code))
	eofJt := vm.NewPragueEOFInstructionSet()
	var c vm.Container
	arr := strings.Split(exception, "|")
	fmt.Println(arr)
	var found bool
	if err := c.UnmarshalBinary(code, false); err != nil {
		fmt.Println("err unmarshal: ", err)
		for _, _exception := range arr {
			found = mapError(_exception, err)
			if !found {
				return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
			} else if found && len(arr) > 1 { // no need to go ther second exception
				return nil
			}
		}
		if found {
			return nil
		} else {
			panic("something was not right")
		}
	}
	found = false
	if err := c.ValidateCode(&eofJt); err != nil {
		fmt.Println("err validate: ", err)
		for _, _exception := range arr {
			found = mapError(_exception, err)
			if !found {
				return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
			} else if found && len(arr) > 1 { // no need to go ther second exception
				return nil
			}
		}
		if found {
			return nil
		} else {
			panic("something was not right")
		}
		// found := mapError(exception, err)
		// if !found {
		// 	return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
		// } else {
		// 	return nil
		// }
	}
	return nil
}

// given code 0xef00010100100200040008000a00040006040000000080000200000001008000000000000260006000e3000100600035e10001e4e50002e30003006001600055e4
// 0x
// ef00 - magic
// 01 - version
// 01 - kind type
// 0010 - type sizes (16/4=4 type sections)
// 02 - kind code
// 0004 - num code sections (4)
// 0008 - 1st code section size
// 000a - 2nd
// 0004 - 3d
// 0006 - 4th
// 04 - kind data
// 0000 - data size
// 00 - terminator
// 00 - inputs 1st type section
// 80 - outputs 1st type section (non returning function)
// 0002 - max stack height 1st section
// 00 - inputs 2nd
// 00 - outputs 2nd
// 0001 - max stack height 2nd
// 00 - inputs 3d
// 80 - outputs 3d (non returning function)
// 0000 - max stack height 3d
// 00 - inputs 4th
// 00 - outputs 4th
// 0002 - max stack heitgh 4th
// 60006000e3000100 - 1st code
// 600035e10001e4e50002 - 2nd code
// e3000300 - 3d code
// 6001600055e4 - 4th code

// 0x
// ef00
// 01
// 01
// 0008
// 02
// 0002
// 0003
// 0003
// 04
// 0004
// 00
// 00
// 80
// 0001
// 00
// 00
// 0000
// 30 50 00 0b ad 60 a7
