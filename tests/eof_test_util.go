package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
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
		Prague struct {
			Exception string `json:"exception"`
			Result    bool   `json:"result"`
		} `json:"Prague"`
	} `json:"results"`
}

func getError(err error) error {
	var _errors = []error{ // add new errors here
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
	}

	for _, _err := range _errors {
		if errors.Is(err, _err) {
			return _err
		}
	}
	return nil
}

var errorsMap = map[string][]error{
	"EOFException.INVALID_FIRST_SECTION_TYPE":  []error{vm.ErrInvalidFirstSectionType, vm.ErrTooManyInputs, vm.ErrTooManyOutputs, vm.ErrTooLargeMaxStackHeight},
	"EOFException.INCOMPLETE_SECTION_NUMBER":   []error{vm.ErrIncompleteEOF},
	"EOFException.MISSING_HEADERS_TERMINATOR":  []error{vm.ErrIncompleteEOF},
	"EOFException.INCOMPLETE_SECTION_SIZE":     []error{vm.ErrIncompleteEOF},
	"EOFException.TOO_MANY_CODE_SECTIONS":      []error{vm.ErrInvalidTypeSize},
	"EOFException.MISSING_CODE_HEADER":         []error{vm.ErrMissingCodeHeader},
	"EOFException.INVALID_TYPE_SIZE":           []error{vm.ErrInvalidCodeSize, vm.ErrInvalidTypeSize},
	"EOFException.ZERO_SECTION_SIZE":           []error{vm.ErrIncompleteEOF, vm.ErrInvalidTypeSize, vm.ErrInvalidFirstSectionType, vm.ErrInvalidCodeSize},
	"EOFException.INVALID_SECTION_BODIES_SIZE": []error{vm.ErrInvalidContainerSize, vm.ErrInvalidTypeSize},
	"EOFException.INVALID_MAGIC":               []error{vm.ErrInvalidMagic},
	"EOFException.INVALID_VERSION":             []error{vm.ErrInvalidVersion, vm.ErrIncompleteEOF},
	"EOFException.MISSING_TERMINATOR":          []error{vm.ErrMissingTerminator},
	"EOFException.MISSING_TYPE_HEADER":         []error{vm.ErrIncompleteEOF, vm.ErrMissingTypeHeader},
	"EOFException.MISSING_STOP_OPCODE":         []error{vm.ErrInvalidCodeTermination},
}

func mapError(exception string, cmp error) bool {
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

func compareExceptionToErr(exc string, err error) error {
	_err := getError(err)
	if _err == nil {
		fmt.Println("Add err to the error array", err)
		panic("add err to getError func")
	}
	if exc != "" {
		if mapError(exc, _err) {
			return nil
		}
		return nil
	}
	fmt.Println("------------------ Error not found: ", err)
	return err
}

func (e *EOFTest) Run(t *testing.T) error {
	hexCode := e.json.Vector.Index.Code
	// result := e.json.Vector.Index.Result.Prague.Result // TODO(racytech): revisit this part, think about result=true -> what to expect from test?
	exception := e.json.Vector.Index.Result.Prague.Exception
	code, err := hexutil.Decode(hexCode)
	if err != nil {
		return fmt.Errorf("error decoding hex string: %v", hexCode)
	}
	eofJt := vm.NewPragueEOFInstructionSet()
	var c vm.Container
	if err := c.UnmarshalBinary(code); err != nil {
		if err = compareExceptionToErr(exception, err); err != nil {
			return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
		} else {
			return nil
		}
	}
	if err := c.ValidateCode(&eofJt); err != nil {
		if err = compareExceptionToErr(exception, err); err != nil {
			return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
		} else {
			return nil
		}
	}
	return nil
}
