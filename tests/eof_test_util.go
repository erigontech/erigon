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
	}

	for _, _err := range _errors {
		if errors.Is(err, _err) {
			return _err
		}
	}
	return nil
}

var errorsMap = map[string][]error{

	// new
	"EOFException.INCOMPATIBLE_CONTAINER_KIND": []error{vm.ErrIncompatibleContainer, vm.ErrAmbiguousContainer},
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
	// eofJt := vm.NewEOFInstructionSet()
	// var c vm.Container
	arr := strings.Split(exception, ".")
	fmt.Println(arr)
	if _, err := vm.UnmarshalEOF(code, 0, byte(_cotainerKind)); err != nil {
		fmt.Println("------------------ got err")
		if !result { // if we do expect fail and got an error
			if !mapError(exception, err) {
				t.Errorf("test did not pas: expected err: %v, got err: %v", exception, err.Error())
			}
		} else { // if we do not expect fail and got an error
			fmt.Println(err)
			t.Errorf("test did not pas: expected err: nil, got err: %v", err.Error())
		}
	} else {
		fmt.Println("------------------ no err")
		if !result { // if do expect fail, but did not got an error
			t.Errorf("test did not pas: expected err: %v, got err: nil", arr[1])
		} else { // if we do not expect fail and did not got and error
			// skip
		}
		fmt.Println("unmarshal no error")
		fmt.Println(err)
		// vm.MarshalEOF(cont, 0)
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
