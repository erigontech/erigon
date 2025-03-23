package tests

import (
	"encoding/json"
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
	if _, err := vm.ParseEOFHeader(code, &eofJt, _cotainerKind, true, 0); err != nil {
		fmt.Println("got err: ", err)
		if !result {

			if err.Error() == exception {
				return nil
			}
			if len(code) < 14 && err.Error() == "EOFException.CODE_TOO_SHORT" {
				return nil
			}
			if err.Error() == "EOFException.CODE_TOO_SHORT" { // subcontainer case
				if exception == "EOFException.INVALID_MAGIC" {
					return nil
				}
			}
			if err.Error() == "EOFException.INCOMPLETE_SECTION_SIZE" {
				if exception == "EOFException.MISSING_HEADERS_TERMINATOR" {
					return nil
				}
			}

			if err.Error() == "EOFException.INVALID_TYPE_SECTION_SIZE" {
				if exception == "EOFException.ZERO_SECTION_SIZE|EOFException.UNEXPECTED_HEADER_KIND" {
					return nil
				}
				if exception == "EOFException.ZERO_SECTION_SIZE|EOFException.INVALID_SECTION_BODIES_SIZE" {
					return nil
				}
			}

			if err.Error() == "EOFException.INVALID_TYPE_SECTION_SIZE|EOFException.INVALID_SECTION_BODIES_SIZE" {
				if exception == "EOFException.MISSING_DATA_SECTION|EOFException.UNEXPECTED_HEADER_KIND" {
					return nil
				}
				if exception == "EOFException.MISSING_TERMINATOR|EOFException.UNEXPECTED_HEADER_KIND" {
					return nil
				}
			}

			if err.Error() == "EOFException.TOPLEVEL_CONTAINER_TRUNCATED" {
				if exception == "EOFException.INVALID_FIRST_SECTION_TYPE" {
					return nil
				}
			}

			if err.Error() == "EOFException.TOO_MANY_CODE_SECTIONS" {
				if exception == "EOFException.INVALID_SECTION_BODIES_SIZE" {
					return nil
				}
			}

			if err.Error() == "EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT" {
				if exception == "EOFException.STACK_HIGHER_THAN_OUTPUTS" {
					return nil
				}
			}

			if strings.Contains(exception, err.Error()) {
				return nil
			}

			if !strings.Contains(err.Error(), exception) {
				t.Errorf("test did not pass: expected err: %v, got err: %v", exception, err.Error())
			}

		} else {
			t.Errorf("test did not pass: expected err: nil, got err: %v", err.Error())
		}
	} else {
		if !result {
			t.Errorf("test did not pass: expected err: %v, got err: nil", exception)
		}
	}

	return nil
}
