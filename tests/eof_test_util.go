package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	}

	for _, _err := range _errors {
		if errors.Is(err, _err) {
			return _err
		}
	}
	return nil
}

var errorsMap = map[string][]error{
	"EOFException.INVALID_FIRST_SECTION_TYPE":             []error{vm.ErrInvalidFirstSectionType, vm.ErrTooManyInputs, vm.ErrTooManyOutputs, vm.ErrTooLargeMaxStackHeight},
	"EOFException.INCOMPLETE_SECTION_NUMBER":              []error{vm.ErrIncompleteEOF},
	"EOFException.MISSING_HEADERS_TERMINATOR":             []error{vm.ErrIncompleteEOF, io.ErrUnexpectedEOF},
	"EOFException.INCOMPLETE_SECTION_SIZE":                []error{vm.ErrIncompleteEOF},
	"EOFException.TOO_MANY_CODE_SECTIONS":                 []error{vm.ErrInvalidTypeSize},
	"EOFException.MISSING_CODE_HEADER":                    []error{vm.ErrMissingCodeHeader},
	"EOFException.ZERO_SECTION_SIZE":                      []error{vm.ErrIncompleteEOF, vm.ErrInvalidTypeSize, vm.ErrInvalidFirstSectionType, vm.ErrInvalidCodeSize},
	"EOFException.INVALID_SECTION_BODIES_SIZE":            []error{vm.ErrInvalidContainerSize, vm.ErrInvalidTypeSize},
	"EOFException.INVALID_MAGIC":                          []error{vm.ErrInvalidMagic},
	"EOFException.INVALID_VERSION":                        []error{vm.ErrInvalidVersion, vm.ErrIncompleteEOF},
	"EOFException.MISSING_TERMINATOR":                     []error{vm.ErrMissingTerminator},
	"EOFException.MISSING_TYPE_HEADER":                    []error{vm.ErrIncompleteEOF, vm.ErrMissingTypeHeader},
	"EOFException.MISSING_STOP_OPCODE":                    []error{vm.ErrInvalidCodeTermination},
	"EOFException.UNDEFINED_EXCEPTION":                    []error{vm.ErrTooManyOutputs, vm.ErrInvalidSectionArgument, vm.ErrInvalidCodeTermination, vm.ErrInvalidMaxStackHeight, vm.ErrInvalidOutputs, vm.ErrNoTerminalInstruction, vm.ErrCALLFtoNonReturning, vm.ErrStackHeightHigher},
	"EOFException.INVALID_DATALOADN_INDEX":                []error{vm.ErrInvalidDataLoadN},
	"EOFException.STACK_UNDERFLOW":                        []error{vm.ErrEOFStackUnderflow},
	"EOFException.TOPLEVEL_CONTAINER_TRUNCATED":           []error{vm.ErrInvalidContainerSize},
	"EOFException.INVALID_TYPE_SECTION_SIZE":              []error{vm.ErrInvalidCodeSize, vm.ErrInvalidCodeSize},
	"EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT":         []error{vm.ErrTooManyOutputs, vm.ErrTooManyInputs},
	"EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT":           []error{vm.ErrTooLargeMaxStackHeight},
	"EOFException.MISSING_DATA_SECTION":                   []error{vm.ErrMissingDataHeader},
	"EOFException.UNREACHABLE_CODE_SECTIONS":              []error{vm.ErrInvalidContainerSize},
	"EOFException.UNDEFINED_INSTRUCTION":                  []error{vm.ErrUndefinedInstruction},
	"EOFException.INVALID_RJUMP_DESTINATION":              []error{vm.ErrInvalidRjumpDest, vm.ErrInvalidContainerArgument, vm.ErrInvalidNonReturning},
	"EOFException.UNREACHABLE_INSTRUCTIONS":               []error{vm.ErrUnreachableCode},
	"EOFException.TRUNCATED_INSTRUCTION":                  []error{vm.ErrTruncatedImmediate},
	"EOFException.HIGHER_THAN_OUTPUTS":                    []error{vm.ErrStackHeightHigher},
	"EOFException.JUMPF_DESTINATION_INCOMPATIBLE_OUTPUTS": []error{vm.ErrJUMPFOutputs},
	"EOFException.INVALID_NON_RETURNING_FLAG":             []error{vm.ErrEOFStackOverflow}, // TODO(racytech): comment this out and test on jumpf, there supposed to be another error from our side, compare it to EVMone
	"EOFException.STACK_HIGHER_THAN_OUTPUTS":              []error{vm.ErrStackHeightHigher},
	"EOFException.CONTAINER_SIZE_ABOVE_LIMIT":             []error{vm.ErrInvalidMagic}, // TODO(racytech): change this when tests get updated
	"EOFException.INVALID_CONTAINER_SECTION_INDEX":        []error{vm.ErrInvalidContainerArgument},
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
	// fmt.Println("hexCode: ", hexCode)
	result := e.json.Vector.Index.Result.Prague.Result // TODO(racytech): revisit this part, think about result=true -> what to expect from test?
	exception := e.json.Vector.Index.Result.Prague.Exception
	code, err := hexutil.Decode(hexCode)
	if err != nil {
		return fmt.Errorf("error decoding hex string: %v", hexCode)
	}
	fmt.Println("result: ", result)
	fmt.Println("exception: ", exception)
	fmt.Println("eof code size: ", len(code))
	eofJt := vm.NewPragueEOFInstructionSet()
	var c vm.Container
	if err := c.UnmarshalBinary(code); err != nil {
		fmt.Println("err unmarshal: ", err)
		if err = compareExceptionToErr(exception, err); err != nil {
			return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
		} else {
			return nil
		}
	}
	if err := c.ValidateCode(&eofJt); err != nil {
		fmt.Println("err validate: ", err)
		if err = compareExceptionToErr(exception, err); err != nil {
			return fmt.Errorf("%w: %v", vm.ErrInvalidEOFInitcode, err)
		} else {
			return nil
		}
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
// 0004
// 02
// 0001
// 090b
// 04
// 0000
// 00
//00
// 80
// 0002
// 5f - PUSH0
// 35 - CALLDATALOAD
// e2 -
// ff
// 00
// 07
// 00
// 0e
// 0015001c0023002a00310038003f0046004d0054005b0062006900700077007e0085008c0093009a00a100a800af00b600bd00c400cb00d200d900e000e700ee00f500fc0103010a01110118011f0126012d0134013b0142014901500157015e0165016c0173017a01810188018f0196019d01a401ab01b201b901c001c701ce01d501dc01e301ea01f101f801ff0206020d0214021b0222022902300237023e0245024c0253025a02610268026f0276027d0284028b0292029902a002a702ae02b502bc02c302ca02d102d802df02e602ed02f402fb0302030903100317031e0325032c0333033a03410348034f0356035d0364036b0372037903800387038e0395039c03a303aa03b103b803bf03c603cd03d403db03e203e903f003f703fe0405040c0413041a04210428042f0436043d0444044b0452045904600467046e0475047c0483048a04910498049f04a604ad04b404bb04c204c904d004d704de04e504ec04f304fa05010508050f0516051d0524052b0532053905400547054e0555055c0563056a05710578057f0586058d0594059b05a205a905b005b705be05c505cc05d305da05e105e805ef05f605fd0604060b0612061906200627062e0635063c0643064a06510658065f0666066d0674067b0682068906900697069e06a506ac06b306ba06c106c806cf06d606dd06e406eb06f206f9070061ffff600255006110006002550061100160025500611002600255006110036002550061100460025500611005600255006110066002550061100760025500611008600255006110096002550061100a6002550061100b6002550061100c6002550061100d6002550061100e6002550061100f600255006110106002550061101160025500611012600255006110136002550061101460025500611015600255006110166002550061101760025500611018600255006110196002550061101a6002550061101b6002550061101c6002550061101d6002550061101e6002550061101f600255006110206002550061102160025500611022600255006110236002550061102460025500611025600255006110266002550061102760025500611028600255006110296002550061102a6002550061102b6002550061102c6002550061102d6002550061102e6002550061102f600255006110306002550061103160025500611032600255006110336002550061103460025500611035600255006110366002550061103760025500611038600255006110396002550061103a6002550061103b6002550061103c6002550061103d6002550061103e6002550061103f600255006110406002550061104160025500611042600255006110436002550061104460025500611045600255006110466002550061104760025500611048600255006110496002550061104a6002550061104b6002550061104c6002550061104d6002550061104e6002550061104f600255006110506002550061105160025500611052600255006110536002550061105460025500611055600255006110566002550061105760025500611058600255006110596002550061105a6002550061105b6002550061105c6002550061105d6002550061105e6002550061105f600255006110606002550061106160025500611062600255006110636002550061106460025500611065600255006110666002550061106760025500611068600255006110696002550061106a6002550061106b6002550061106c6002550061106d6002550061106e6002550061106f600255006110706002550061107160025500611072600255006110736002550061107460025500611075600255006110766002550061107760025500611078600255006110796002550061107a6002550061107b6002550061107c6002550061107d6002550061107e6002550061107f600255006110806002550061108160025500611082600255006110836002550061108460025500611085600255006110866002550061108760025500611088600255006110896002550061108a6002550061108b6002550061108c6002550061108d6002550061108e6002550061108f600255006110906002550061109160025500611092600255006110936002550061109460025500611095600255006110966002550061109760025500611098600255006110996002550061109a6002550061109b6002550061109c6002550061109d6002550061109e6002550061109f600255006110a0600255006110a1600255006110a2600255006110a3600255006110a4600255006110a5600255006110a6600255006110a7600255006110a8600255006110a9600255006110aa600255006110ab600255006110ac600255006110ad600255006110ae600255006110af600255006110b0600255006110b1600255006110b2600255006110b3600255006110b4600255006110b5600255006110b6600255006110b7600255006110b8600255006110b9600255006110ba600255006110bb600255006110bc600255006110bd600255006110be600255006110bf600255006110c0600255006110c1600255006110c2600255006110c3600255006110c4600255006110c5600255006110c6600255006110c7600255006110c8600255006110c9600255006110ca600255006110cb600255006110cc600255006110cd600255006110ce600255006110cf600255006110d0600255006110d1600255006110d2600255006110d3600255006110d4600255006110d5600255006110d6600255006110d7600255006110d8600255006110d9600255006110da600255006110db600255006110dc600255006110dd600255006110de600255006110df600255006110e0600255006110e1600255006110e2600255006110e3600255006110e4600255006110e5600255006110e6600255006110e7600255006110e8600255006110e9600255006110ea600255006110eb600255006110ec600255006110ed600255006110ee600255006110ef600255006110f0600255006110f1600255006110f2600255006110f3600255006110f4600255006110f5600255006110f6600255006110f7600255006110f8600255006110f9600255006110fa600255006110fb600255006110fc600255006110fd600255006110fe600255006110ff60025500
