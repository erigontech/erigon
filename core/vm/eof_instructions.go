package vm

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/holiman/uint256"
)

// EOFv1 instructions

func readInt16Be(data []byte) int16 {
	return int16(data[0])<<8 | int16(data[1])
}

func opRjump(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset := readInt16Be(scope.Contract.Code[*pc+1:])
	*pc = uint64(int64(*pc+3) + int64(offset) - 1) // we do pc++ in interperter, so -1
	return nil, nil
}

func opRjumpi(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	condition := scope.Stack.Pop()
	if condition.BitLen() == 0 {
		// Not branching, just skip over immediate argument.
		*pc += 2
		return nil, nil
	}
	return opRjump(pc, interpreter, scope)
}

// opRjumpv implements the RJUMPV opcode
func opRjumpv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		maxIdx = uint64(scope.Contract.Code[*pc+1])
		_case  = scope.Stack.Pop()
	)
	pcPost := *pc + 1 + 1 + (maxIdx+1)*REL_OFFSET_SIZE - 1 // we do pc++ in interperter, so -1

	if case64, overflow := _case.Uint64WithOverflow(); overflow || case64 > maxIdx {
		// Index out-of-bounds, don't branch, just skip over immediate
		// argument.
		*pc = pcPost
		return nil, nil
	}
	relOffset := readInt16Be(scope.Contract.Code[*pc+2+2*_case.Uint64():])
	// *pc = pcPost + uint64(relOffset) // may be uint64(pcPost + relOffset)?
	*pc = uint64(int64(pcPost) + int64(relOffset))
	return nil, nil
}

func opCallf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	idx := binary.BigEndian.Uint16(scope.Contract.Code[*pc+1:])

	typSectionOffset := getTypeSectionOffset(idx, scope.EofHeader)
	inputs := int(scope.Contract.Code[typSectionOffset])
	sectionMaxStack := int(scope.Contract.Code[typSectionOffset+2])<<8 |
		int(scope.Contract.Code[typSectionOffset+3])
	if scope.Stack.Len()+sectionMaxStack-inputs > 1024 {
		return nil, fmt.Errorf("CALLF stack overflow: StackLen: %v, sectionMaxStack: %v, sectionInputs: %v", scope.Stack.Len(), sectionMaxStack, inputs)
	}

	if len(scope.ReturnStack) >= 1024 {
		return nil, fmt.Errorf("CALLF return_stack limit reached")
	}
	scope.ReturnStack = append(
		scope.ReturnStack,
		[2]uint64{*pc + 2, scope.SectionIdx},
	)
	*pc = uint64(scope.EofHeader.codeOffsets[idx]) - 1 // we do pc++ in the interpreter loop
	scope.SectionIdx = uint64(idx)
	return nil, nil
}

func opRetf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	*pc = scope.ReturnStack[len(scope.ReturnStack)-1][0]
	scope.SectionIdx = scope.ReturnStack[len(scope.ReturnStack)-1][1]
	scope.ReturnStack = scope.ReturnStack[:len(scope.ReturnStack)-1]
	return nil, nil
}

func opJumpf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	idx := binary.BigEndian.Uint16(scope.Contract.Code[*pc+1:])
	*pc = uint64(scope.EofHeader.codeOffsets[idx]) - 1 // we do pc++ in the interpreter loop

	typSectionOffset := getTypeSectionOffset(idx, scope.EofHeader)
	inputs := int(scope.Contract.Code[typSectionOffset])
	sectionMaxStack := int(scope.Contract.Code[typSectionOffset+2])<<8 |
		int(scope.Contract.Code[typSectionOffset+3])

	if scope.Stack.Len()+sectionMaxStack-inputs > 1024 {
		return nil, fmt.Errorf("JUMPF stack overflow: StackLen: %v, sectionMaxStack: %v, inputs: %v", scope.Stack.Len(), sectionMaxStack, inputs)
	}
	scope.SectionIdx = uint64(idx)
	return nil, nil
}

func opDupN(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	idx := int(scope.Contract.Code[*pc+1])
	scope.Stack.DupN(idx)
	*pc += 1 // we do one more pc++ in the interpeter loop
	return nil, nil
}

func opSwapN(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	idx := int(scope.Contract.Code[*pc+1]) + 1
	scope.Stack.SwapTop(idx)
	*pc += 1
	return nil, nil
}

func opExchange(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	n := (int(scope.Contract.Code[*pc+1]) >> 4) + 1
	m := (int(scope.Contract.Code[*pc+1]) & 0x0f) + 1
	stackTop := scope.Stack.Len() - 1
	scope.Stack.Exchange(stackTop-n, stackTop-n-m)
	*pc += 1
	return nil, nil
}

func opDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	index := scope.Stack.Peek()
	data := scope.Contract.Code[scope.EofHeader.dataOffset:]
	dataLen := uint256.NewInt(uint64(len(data)))
	b := [32]byte{}
	if index.Gt(dataLen) {
		index.SetBytes32(b[:])
	} else {
		offset := int(index.Uint64())
		end := min(offset+32, len(data))
		for i := 0; i < end-offset; i++ {
			b[i] = data[offset+i]
		}
		index.SetBytes32(b[:])
	}
	return nil, nil
}

func opDataLoadN(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	data := scope.Contract.Code[scope.EofHeader.dataOffset:]
	offset := int(binary.BigEndian.Uint16(scope.Contract.Code[*pc+1:]))
	val := new(uint256.Int).SetBytes(data[offset : offset+32])
	scope.Stack.Push(val)

	*pc += 2 // one more +1 we do in the interpreter loop
	return nil, nil
}

func opDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	val := new(uint256.Int).SetUint64(uint64(scope.EofHeader.dataSize))
	scope.Stack.Push(val)
	return nil, nil
}

func opDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memIndex256  = scope.Stack.Pop()
		dataIndex256 = scope.Stack.Pop()
		size256      = scope.Stack.Pop()

		data        = scope.Contract.Code[scope.EofHeader.dataOffset:]
		dataSize256 = uint256.NewInt(uint64(len(data)))
		// dataLen            = uint64(len(data))
		// dataIndex          = dataIndex256.Uint64()
		dst, src, copySize uint64
	)
	dst = memIndex256.Uint64()
	if dataSize256.Lt(&dataIndex256) {
		src = uint64(len(data))
	} else {
		src = dataIndex256.Uint64()
	}
	s := size256.Uint64()
	copySize = min(s, uint64(len(data))-src)

	if copySize > 0 {
		scope.Memory.CopyFromData(dst, data, src, copySize)
	}

	if s-copySize > 0 {
		scope.Memory.SetZero(dst+copySize, s-copySize)
	}
	return nil, nil
}

func opEOFCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	if interpreter.readOnly { // STATICCALL
		return nil, fmt.Errorf("calling EOFCreate in static mode")
	}

	var (
		initContainerIdx = scope.Contract.Code[*pc+1]

		endowment = scope.Stack.Pop()
		salt      = scope.Stack.Pop()
		offset    = scope.Stack.Pop()
		size      = scope.Stack.Pop()
		input     []byte
		gas       = scope.Contract.Gas
	)
	*pc += 1

	_offset := scope.EofHeader.containerOffsets[initContainerIdx]
	_size := scope.EofHeader.containerSizes[initContainerIdx]
	initContainer := scope.Contract.Code[_offset : _offset+_size]

	gas -= gas / 64
	if ok := scope.Contract.UseGas(gas, tracing.GasChangeCallContractEOFCreation); !ok {
		return nil, ErrOutOfGas
	}
	if size.Uint64() > 0 {
		input = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	}
	stackValue := size
	res, addr, returnGas, suberr := interpreter.evm.EOFCreate(scope.Contract, input, initContainer, gas, &endowment, &salt, false)
	if suberr != nil {
		stackValue.Clear()
	} else {
		stackValue.SetBytes(addr.Bytes())
	}

	scope.Stack.Push(&stackValue)
	scope.Contract.RefundGas(returnGas, tracing.GasChangeCallLeftOverRefunded)
	if suberr == ErrExecutionReverted {
		interpreter.returnData = res // set REVERT data to return data buffer
		return res, nil
	}
	interpreter.returnData = nil // clear dirty return data buffer
	return nil, nil
}

func opReturnCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	offset256 := scope.Stack.Pop()
	size256 := scope.Stack.Pop()
	deployContainerIdx := int(scope.Contract.Code[*pc+1])
	if deployContainerIdx >= len(scope.EofHeader.containerSizes) {
		return nil, fmt.Errorf("invalid subcontainer index: deployContainerIdx=%v, len(scope.EofHeader.containerSizes)=%v", deployContainerIdx, len(scope.EofHeader.containerSizes))
	}

	_offset := scope.EofHeader.containerOffsets[deployContainerIdx] //  container offset
	_size := scope.EofHeader.containerSizes[deployContainerIdx]     // container size
	container := make([]byte, _size)
	copy(container, scope.Contract.Code[_offset:_offset+_size])

	offset := int64(offset256.Uint64())
	size := int64(size256.Uint64())
	auxData := scope.Memory.GetCopy(offset, size)

	header, err := ParseEOFHeader(container, interpreter.jtEOF, runtime, false, 0)
	if err != nil {
		return nil, fmt.Errorf("ParseEOFHeader: %w", err)
	}

	newDataSize := len(container) - int(header.dataOffset) + len(auxData)

	if newDataSize > 65535 {
		return nil, fmt.Errorf("newDataSize > 65535")
	}
	if newDataSize < int(header.dataSize) {
		return nil, fmt.Errorf("newDataSize < scope.EofHeader.dataSize")
	}
	container = append(container, auxData...)

	container[header.dataSizePos] = byte(newDataSize >> 8)
	container[header.dataSizePos+1] = byte(newDataSize)

	*pc += 1
	return container, errStopToken
}

func opReturnDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	var index256 = scope.Stack.Peek()
	// _, overflow := index256.Uint64WithOverflow()
	// if overflow {
	// 	// TODO: can this be the case?
	// }
	start := index256.Uint64()
	b := [32]byte{}
	if uint64(len(interpreter.returnData)) < start {
		index256.SetBytes32(b[:]) // set zero
	} else {
		end := min(start+32, uint64(len(interpreter.returnData)))
		for i := uint64(0); i < end-start; i++ {
			b[i] = interpreter.returnData[start+i]
		}
		index256.SetBytes32(b[:])
	}
	return nil, nil
}

func opExtCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	var (
		dst256    = scope.Stack.Pop()
		offset256 = scope.Stack.Pop()
		size256   = scope.Stack.Pop()
		value     = scope.Stack.Pop()

		toAddr = dst256.Bytes20()
		offset = int64(offset256.Uint64())
		size   = int64(size256.Uint64())

		gas = interpreter.evm.CallGasTemp()
	)

	if interpreter.readOnly && !value.IsZero() {
		return nil, ErrWriteProtection
	}

	if dst256.ByteLen() > 20 {
		return nil, fmt.Errorf("argument out of range")
	}

	args := scope.Memory.GetPtr(offset, size)
	var (
		ret       []byte
		returnGas uint64
		err       error
	)
	if gas == 0 {
		// zero temp call gas indicates a min retained gas error
		ret, returnGas, err = nil, 0, ErrExecutionReverted
	} else {
		ret, returnGas, err = interpreter.evm.ExtCall(scope.Contract, toAddr, args, gas, &value)
	}

	if err == ErrExecutionReverted || err == ErrDepth || err == ErrInsufficientBalance {
		dst256.SetOne()
	} else if err != nil {
		dst256.SetUint64(2)
	} else {
		dst256.Clear()
	}
	scope.Stack.Push(&dst256)

	scope.Contract.RefundGas(returnGas, tracing.GasChangeCallLeftOverRefunded)
	interpreter.returnData = ret

	return ret, nil
}
func opExtDelegateCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		addr256   = scope.Stack.Pop()
		offset256 = scope.Stack.Pop()
		size256   = scope.Stack.Pop()

		toAddr = common.Address(addr256.Bytes20())
		offset = int64(offset256.Uint64())
		size   = int64(size256.Uint64())

		gas = interpreter.evm.CallGasTemp()
	)

	if addr256.ByteLen() > 20 {
		return nil, fmt.Errorf("argument out of range")
	}

	if gas == 0 {
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}

	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(offset, size)

	// The code targeted by EXTDELEGATECALL must also be an EOF.
	// This restriction has been added to EIP-3540 in
	// https://github.com/ethereum/EIPs/pull/7131
	code, err := interpreter.evm.intraBlockState.GetCode(toAddr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	if !isEOFcode(code) {
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}
	ret, returnGas, err := interpreter.evm.ExtDelegateCall(scope.Contract, toAddr, args, gas)
	if err == ErrExecutionReverted || err == ErrDepth {
		addr256.SetOne()
	} else if err != nil {
		addr256.SetUint64(2)
	} else {
		addr256.Clear()
	}
	scope.Stack.Push(&addr256)

	scope.Contract.RefundGas(returnGas, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}
func opExtStaticCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		addr256   = scope.Stack.Pop()
		offset256 = scope.Stack.Pop()
		size256   = scope.Stack.Pop()

		toAddr = addr256.Bytes20()
		offset = int64(offset256.Uint64())
		size   = int64(size256.Uint64())

		gas = interpreter.evm.CallGasTemp()
	)

	if addr256.ByteLen() > 20 {
		return nil, fmt.Errorf("argument out of range")
	}

	if gas == 0 {
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}

	// Get the arguments from the memory.
	args := scope.Memory.GetPtr(offset, size)
	ret, returnGas, err := interpreter.evm.ExtStaticCall(scope.Contract, toAddr, args, gas)

	if err == ErrExecutionReverted || err == ErrDepth {
		addr256.SetOne()
	} else if err != nil {
		addr256.SetUint64(2)
	} else {
		addr256.Clear()
	}
	scope.Stack.Push(&addr256)

	scope.Contract.RefundGas(returnGas, tracing.GasChangeCallLeftOverRefunded)

	interpreter.returnData = ret
	return ret, nil
}
