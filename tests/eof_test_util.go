// Copyright 2024 The Erigon Authors
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

package tests

import (
	"encoding/json"
	"errors"
	"fmt"
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
	Code          string `json:"code"`
	ContainerKind string `json:"containerKind"`
	Result        struct {
		Osaka struct {
			Exception string `json:"exception"`
			Result    bool   `json:"result"`
		} `json:"Osaka"`
	} `json:"results"`
}

func parseError(err error) error {
	var _errors = []error{ // add new errors here
		vm.ErrIncompatibleContainer,
		vm.ErrAmbiguousContainer,
		vm.ErrInvalidSectionsSize,
		vm.ErrInvalidCodeTermination,
		vm.ErrInvalidMagic,
		vm.ErrOrphanSubContainer,
		vm.ErrTopLevelTruncated,
		vm.ErrIncompleteEOF,
		vm.ErrInvalidVersion,
		vm.ErrTooManyContainerSections,
		vm.ErrTooLargeByteCode,
		vm.ErrInvalidMaxStackHeight,
		vm.ErrTooLargeMaxStackHeight,
		vm.ErrEOFStackUnderflow,
		vm.ErrUndefinedInstruction,
		vm.ErrUnreachableCode,
		vm.ErrTruncatedImmediate,
		vm.ErrMissingTypeHeader,
		vm.ErrInvalidTypeSize,
		vm.ErrInvalidSectionCount,
		vm.ErrInvalidFirstSectionType,
		vm.ErrTooManyInputs,
		vm.ErrTooManyOutputs,
		vm.ErrMissingCodeHeader,
		vm.ErrMissingDataHeader,
		vm.ErrInvalidCodeSize,
		vm.ErrInvalidContainerSize,
		vm.ErrMissingTerminator,
		vm.ErrZeroSizeContainerSection,
		vm.ErrUnreachableCodeSections,
		vm.ErrInvalidRjumpDest,
		vm.ErrStackHeightMismatch,
		vm.ErrEOFStackOverflow,
		vm.ErrStackHeightHigher,
		vm.ErrInvalidSectionArgument,
		vm.ErrInvalidNonReturning,
		vm.ErrJUMPFOutputs,
		vm.ErrInvalidDataLoadN,
		vm.ErrInvalidContainerArgument,
		vm.ErrEOFCreateWithTruncatedContainer,
	}

	for _, _err := range _errors {
		if errors.Is(err, _err) {
			return _err
		}
	}
	return nil
}

var errorsMap = map[string][]error{
	"EOFException.INCOMPATIBLE_CONTAINER_KIND": []error{vm.ErrIncompatibleContainer, vm.ErrAmbiguousContainer},
	"EOFException.INVALID_SECTION_BODIES_SIZE": []error{
		vm.ErrInvalidSectionsSize, vm.ErrInvalidSectionCount, vm.ErrInvalidTypeSize},
	"EOFException.MISSING_STOP_OPCODE":            []error{vm.ErrInvalidCodeTermination},
	"EOFException.INVALID_MAGIC":                  []error{vm.ErrInvalidMagic, vm.ErrIncompleteEOF},
	"EOFException.ORPHAN_SUBCONTAINER":            []error{vm.ErrOrphanSubContainer},
	"EOFException.TOPLEVEL_CONTAINER_TRUNCATED":   []error{vm.ErrTopLevelTruncated},
	"EOFException.INVALID_VERSION":                []error{vm.ErrInvalidVersion, vm.ErrIncompleteEOF},
	"EOFException.TOO_MANY_CONTAINERS":            []error{vm.ErrTooManyContainerSections},
	"EOFException.CONTAINER_SIZE_ABOVE_LIMIT":     []error{vm.ErrTooLargeByteCode},
	"EOFException.INVALID_MAX_STACK_HEIGHT":       []error{vm.ErrInvalidMaxStackHeight},
	"EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT":   []error{vm.ErrTooLargeMaxStackHeight},
	"EOFException.STACK_UNDERFLOW":                []error{vm.ErrEOFStackUnderflow},
	"EOFException.UNDEFINED_INSTRUCTION":          []error{vm.ErrUndefinedInstruction},
	"EOFException.UNREACHABLE_INSTRUCTIONS":       []error{vm.ErrUnreachableCode},
	"EOFException.TRUNCATED_INSTRUCTION":          []error{vm.ErrTruncatedImmediate},
	"EOFException.MISSING_TYPE_HEADER":            []error{vm.ErrMissingTypeHeader, vm.ErrIncompleteEOF},
	"EOFException.INVALID_TYPE_SECTION_SIZE":      []error{vm.ErrInvalidTypeSize, vm.ErrInvalidSectionCount},
	"EOFException.INVALID_FIRST_SECTION_TYPE":     []error{vm.ErrInvalidFirstSectionType, vm.ErrTopLevelTruncated},
	"EOFException.TOO_MANY_CODE_SECTIONS":         []error{vm.ErrIncompleteEOF, vm.ErrInvalidTypeSize},
	"EOFException.INCOMPLETE_SECTION_NUMBER":      []error{vm.ErrIncompleteEOF},
	"EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT": []error{vm.ErrTooManyInputs, vm.ErrTooManyOutputs},
	"EOFException.MISSING_HEADERS_TERMINATOR": []error{
		vm.ErrIncompleteEOF, vm.ErrInvalidContainerSize, vm.ErrMissingTerminator, vm.ErrMissingDataHeader},
	"EOFException.INCOMPLETE_SECTION_SIZE": []error{
		vm.ErrIncompleteEOF, vm.ErrMissingDataHeader, vm.ErrInvalidContainerSize},
	"EOFException.ZERO_SECTION_SIZE": []error{
		vm.ErrIncompleteEOF, vm.ErrInvalidTypeSize, vm.ErrInvalidCodeSize, vm.ErrZeroSizeContainerSection, vm.ErrInvalidSectionCount, vm.ErrInvalidContainerSize},
	"EOFException.MISSING_CODE_HEADER":                    []error{vm.ErrMissingCodeHeader, vm.ErrIncompleteEOF},
	"EOFException.MISSING_DATA_SECTION":                   []error{vm.ErrMissingDataHeader, vm.ErrInvalidSectionCount},
	"EOFException.MISSING_TERMINATOR":                     []error{vm.ErrMissingTerminator, vm.ErrInvalidSectionCount},
	"EOFException.UNEXPECTED_HEADER_KIND":                 []error{vm.ErrMissingTypeHeader},
	"EOFException.UNREACHABLE_CODE_SECTIONS":              []error{vm.ErrUnreachableCodeSections, vm.ErrInvalidNonReturning, vm.ErrTopLevelTruncated},
	"EOFException.INVALID_RJUMP_DESTINATION":              []error{vm.ErrInvalidRjumpDest},
	"EOFException.STACK_HEIGHT_MISMATCH":                  []error{vm.ErrStackHeightMismatch},
	"EOFException.STACK_OVERFLOW":                         []error{vm.ErrEOFStackOverflow},
	"EOFException.STACK_HIGHER_THAN_OUTPUTS":              []error{vm.ErrStackHeightHigher},
	"EOFException.INVALID_CODE_SECTION_INDEX":             []error{vm.ErrInvalidSectionArgument},
	"EOFException.INVALID_NON_RETURNING_FLAG":             []error{vm.ErrInvalidNonReturning},
	"EOFException.JUMPF_DESTINATION_INCOMPATIBLE_OUTPUTS": []error{vm.ErrJUMPFOutputs},
	"EOFException.CALLF_TO_NON_RETURNING":                 []error{vm.ErrInvalidSectionArgument},
	"EOFException.INVALID_DATALOADN_INDEX":                []error{vm.ErrInvalidDataLoadN},
	"EOFException.INVALID_CONTAINER_SECTION_INDEX":        []error{vm.ErrInvalidContainerArgument},
	"EOFException.EOFCREATE_WITH_TRUNCATED_CONTAINER":     []error{vm.ErrEOFCreateWithTruncatedContainer},
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
	containerKind := e.json.Vector.Index.ContainerKind
	code, err := hexutil.Decode(hexCode)
	if err != nil {
		return fmt.Errorf("error decoding hex string: %v", hexCode)
	}
	fmt.Println("result: ", result)
	fmt.Println("exception: ", exception)
	fmt.Println("eof code size: ", len(code))
	fmt.Println("containerKind: ", containerKind)

	var _cotainerKind byte
	if containerKind == "RUNTIME" {
		_cotainerKind = 1
	}
	eofJt := vm.NewEOFInstructionSet()
	// var c vm.Container
	arr := strings.Split(exception, "|")

	if _, err := vm.UnmarshalEOF(code, 0, byte(_cotainerKind), &eofJt, true); err != nil {
		fmt.Println("------------------ got err, ", err)
		if !result { // if we do expect fail and got an error
			var found bool
			for _, exception := range arr {
				found = mapError(exception, err)
				if found {
					break
				}
			}
			if !found {
				t.Errorf("test did not pass: expected err: %v, got err: %v", exception, err.Error())
			}
		} else { // if we do not expect fail and got an error
			fmt.Println(err)
			t.Errorf("test did not pass: expected err: nil, got err: %v", err.Error())
		}
	} else {
		fmt.Println("------------------ no err")
		if !result { // if do expect fail, but did not got an error
			t.Errorf("test did not pass: expected err: %v, got err: nil", arr[0])
		} else { // if we do not expect fail and did not got and error
			// skip
		}
		fmt.Println("unmarshal no error")
		fmt.Println(err)
		// vm.MarshalEOF(cont, 0)
	}
	return nil
}
