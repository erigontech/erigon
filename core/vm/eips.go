// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/params"
)

var activators = map[int]func(*JumpTable){
	7702: enable7702,
	7516: enable7516,
	6780: enable6780,
	5656: enable5656,
	4844: enable4844,
	3860: enable3860,
	3855: enable3855,
	3529: enable3529,
	3198: enable3198,
	2929: enable2929,
	2200: enable2200,
	1884: enable1884,
	1344: enable1344,
	1153: enable1153,
}

// EnableEIP enables the given EIP on the config.
// This operation writes in-place, and callers need to ensure that the globally
// defined jump tables are not polluted.
func EnableEIP(eipNum int, jt *JumpTable) error {
	enablerFn, ok := activators[eipNum]
	if !ok {
		return fmt.Errorf("undefined eip %d", eipNum)
	}
	enablerFn(jt)
	validateAndFillMaxStack(jt)
	return nil
}

func ValidEip(eipNum int) bool {
	_, ok := activators[eipNum]
	return ok
}
func ActivateableEips() []string {
	var nums []string //nolint:prealloc
	for k := range activators {
		nums = append(nums, strconv.Itoa(k))
	}
	sort.Strings(nums)
	return nums
}

// enable1884 applies EIP-1884 to the given jump table:
// - Increase cost of BALANCE to 700
// - Increase cost of EXTCODEHASH to 700
// - Increase cost of SLOAD to 800
// - Define SELFBALANCE, with cost GasFastStep (5)
func enable1884(jt *JumpTable) {
	// Gas cost changes
	jt[SLOAD].constantGas = params.SloadGasEIP1884
	jt[BALANCE].constantGas = params.BalanceGasEIP1884
	jt[EXTCODEHASH].constantGas = params.ExtcodeHashGasEIP1884

	// New opcode
	jt[SELFBALANCE] = &operation{
		execute:     opSelfBalance,
		constantGas: GasFastStep,
		numPop:      0,
		numPush:     1,
	}
}

func opSelfBalance(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error) {
	balance, err := interpreter.evm.IntraBlockState().GetBalance(callContext.Contract.Address())
	if err != nil {
		return nil, err
	}
	callContext.Stack.Push(balance)
	return nil, nil
}

// enable1344 applies EIP-1344 (ChainID Opcode)
// - Adds an opcode that returns the current chain’s EIP-155 unique identifier
func enable1344(jt *JumpTable) {
	// New opcode
	jt[CHAINID] = &operation{
		execute:     opChainID,
		constantGas: GasQuickStep,
		numPop:      0,
		numPush:     1,
	}
}

// opChainID implements CHAINID opcode
func opChainID(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error) {
	chainId, _ := uint256.FromBig(interpreter.evm.ChainRules().ChainID)
	callContext.Stack.Push(chainId)
	return nil, nil
}

// enable2200 applies EIP-2200 (Rebalance net-metered SSTORE)
func enable2200(jt *JumpTable) {
	jt[SLOAD].constantGas = params.SloadGasEIP2200
	jt[SSTORE].dynamicGas = gasSStoreEIP2200
}

// enable2929 enables "EIP-2929: Gas cost increases for state access opcodes"
// https://eips.ethereum.org/EIPS/eip-2929
func enable2929(jt *JumpTable) {
	jt[SSTORE].dynamicGas = gasSStoreEIP2929

	jt[SLOAD].constantGas = 0
	jt[SLOAD].dynamicGas = gasSLoadEIP2929

	jt[EXTCODECOPY].constantGas = params.WarmStorageReadCostEIP2929
	jt[EXTCODECOPY].dynamicGas = gasExtCodeCopyEIP2929

	jt[EXTCODESIZE].constantGas = params.WarmStorageReadCostEIP2929
	jt[EXTCODESIZE].dynamicGas = gasEip2929AccountCheck

	jt[EXTCODEHASH].constantGas = params.WarmStorageReadCostEIP2929
	jt[EXTCODEHASH].dynamicGas = gasEip2929AccountCheck

	jt[BALANCE].constantGas = params.WarmStorageReadCostEIP2929
	jt[BALANCE].dynamicGas = gasEip2929AccountCheck

	jt[CALL].constantGas = params.WarmStorageReadCostEIP2929
	jt[CALL].dynamicGas = gasCallEIP2929

	jt[CALLCODE].constantGas = params.WarmStorageReadCostEIP2929
	jt[CALLCODE].dynamicGas = gasCallCodeEIP2929

	jt[STATICCALL].constantGas = params.WarmStorageReadCostEIP2929
	jt[STATICCALL].dynamicGas = gasStaticCallEIP2929

	jt[DELEGATECALL].constantGas = params.WarmStorageReadCostEIP2929
	jt[DELEGATECALL].dynamicGas = gasDelegateCallEIP2929

	// This was previously part of the dynamic cost, but we're using it as a constantGas
	// factor here
	jt[SELFDESTRUCT].constantGas = params.SelfdestructGasEIP150
	jt[SELFDESTRUCT].dynamicGas = gasSelfdestructEIP2929
}

func enable3529(jt *JumpTable) {
	jt[SSTORE].dynamicGas = gasSStoreEIP3529
	jt[SELFDESTRUCT].dynamicGas = gasSelfdestructEIP3529
}

// enable3198 applies EIP-3198 (BASEFEE Opcode)
// - Adds an opcode that returns the current block's base fee.
func enable3198(jt *JumpTable) {
	// New opcode
	jt[BASEFEE] = &operation{
		execute:     opBaseFee,
		constantGas: GasQuickStep,
		numPop:      0,
		numPush:     1,
	}
}

// enable1153 applies EIP-1153 "Transient Storage"
// - Adds TLOAD that reads from transient storage
// - Adds TSTORE that writes to transient storage
func enable1153(jt *JumpTable) {
	jt[TLOAD] = &operation{
		execute:     opTload,
		constantGas: params.WarmStorageReadCostEIP2929,
		numPop:      1,
		numPush:     1,
	}

	jt[TSTORE] = &operation{
		execute:     opTstore,
		constantGas: params.WarmStorageReadCostEIP2929,
		numPop:      2,
		numPush:     0,
	}
}

// opTload implements TLOAD opcode
func opTload(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	loc := scope.Stack.Peek()
	hash := libcommon.Hash(loc.Bytes32())
	val := interpreter.evm.IntraBlockState().GetTransientState(scope.Contract.Address(), hash)
	loc.SetBytes(val.Bytes())
	return nil, nil
}

// opTstore implements TSTORE opcode
func opTstore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	if interpreter.readOnly {
		return nil, ErrWriteProtection
	}
	loc := scope.Stack.Pop()
	val := scope.Stack.Pop()
	interpreter.evm.IntraBlockState().SetTransientState(scope.Contract.Address(), loc.Bytes32(), val)
	return nil, nil
}

// opBaseFee implements BASEFEE opcode
func opBaseFee(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error) {
	baseFee := interpreter.evm.Context.BaseFee
	callContext.Stack.Push(baseFee)
	return nil, nil
}

// enable3855 applies EIP-3855 (PUSH0 opcode)
func enable3855(jt *JumpTable) {
	// New opcode
	jt[PUSH0] = &operation{
		execute:     opPush0,
		constantGas: GasQuickStep,
		numPop:      0,
		numPush:     1,
	}
}

// opPush0 implements the PUSH0 opcode
func opPush0(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	scope.Stack.Push(new(uint256.Int))
	return nil, nil
}

// EIP-3860: Limit and meter initcode
// https://eips.ethereum.org/EIPS/eip-3860
func enable3860(jt *JumpTable) {
	jt[CREATE].dynamicGas = gasCreateEip3860
	jt[CREATE2].dynamicGas = gasCreate2Eip3860
}

// enable4844 applies mini-danksharding (BLOBHASH opcode)
// - Adds an opcode that returns the versioned blob hash of the txn at a index.
func enable4844(jt *JumpTable) {
	jt[BLOBHASH] = &operation{
		execute:     opBlobHash,
		constantGas: GasFastestStep,
		numPop:      1,
		numPush:     1,
	}
}

// opBlobHash implements the BLOBHASH opcode
func opBlobHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	idx := scope.Stack.Peek()
	if idx.LtUint64(uint64(len(interpreter.evm.BlobHashes))) {
		hash := interpreter.evm.BlobHashes[idx.Uint64()]
		idx.SetBytes(hash.Bytes())
	} else {
		idx.Clear()
	}
	return nil, nil
}

// enable5656 enables EIP-5656 (MCOPY opcode)
// https://eips.ethereum.org/EIPS/eip-5656
func enable5656(jt *JumpTable) {
	jt[MCOPY] = &operation{
		execute:     opMcopy,
		constantGas: GasFastestStep,
		dynamicGas:  gasMcopy,
		numPop:      3,
		numPush:     0,
		memorySize:  memoryMcopy,
	}
}

// opMcopy implements the MCOPY opcode (https://eips.ethereum.org/EIPS/eip-5656)
func opMcopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		dst    = scope.Stack.Pop()
		src    = scope.Stack.Pop()
		length = scope.Stack.Pop()
	)
	// These values are checked for overflow during memory expansion calculation
	// (the memorySize function on the opcode).
	scope.Memory.Copy(dst.Uint64(), src.Uint64(), length.Uint64())
	return nil, nil
}

// enable6780 applies EIP-6780 (deactivate SELFDESTRUCT)
func enable6780(jt *JumpTable) {
	jt[SELFDESTRUCT].execute = opSelfdestruct6780
}

// opBlobBaseFee implements the BLOBBASEFEE opcode
func opBlobBaseFee(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error) {
	blobBaseFee := interpreter.evm.Context.BlobBaseFee
	callContext.Stack.Push(blobBaseFee)
	return nil, nil
}

// enable7516 applies EIP-7516 (BLOBBASEFEE opcode)
// - Adds an opcode that returns the current block's blob base fee.
func enable7516(jt *JumpTable) {
	jt[BLOBBASEFEE] = &operation{
		execute:     opBlobBaseFee,
		constantGas: GasQuickStep,
		numPop:      0,
		numPush:     1,
	}
}

func enable7702(jt *JumpTable) {
	jt[CALL].dynamicGas = gasCallEIP7702
	jt[CALLCODE].dynamicGas = gasCallCodeEIP7702
	jt[STATICCALL].dynamicGas = gasStaticCallEIP7702
	jt[DELEGATECALL].dynamicGas = gasDelegateCallEIP7702
}

// enableEOF applies the EOF changes.
func enableEOF(jt *JumpTable) {
	// TODO(racytech): Make sure everything is correct, add all EOF opcodes and remove deprecated ones
	// add them to `opCodeToString` as well
	undefined := &operation{
		execute:     opUndefined,
		constantGas: 0,
		numPop:      0,
		numPush:     0,
		undefined:   true,
	}

	jt[JUMP] = undefined
	jt[JUMPI] = undefined
	jt[PC] = undefined

	// 0x38, 0x39, 0x3b, 0x3c, 0x3f, 0x5a, 0xf0, 0xf1, 0xf2, 0xf4, 0xf5, 0xfa, 0xff - rejected opcodes
	jt[CODESIZE] = undefined     // 0x38
	jt[CODECOPY] = undefined     // 0x39
	jt[EXTCODESIZE] = undefined  // 0x3b
	jt[EXTCODECOPY] = undefined  // 0x3c
	jt[EXTCODEHASH] = undefined  // 0x3f
	jt[GAS] = undefined          // 0x5a
	jt[CREATE] = undefined       // 0xf0
	jt[CALL] = undefined         // 0xf1
	jt[CALLCODE] = undefined     // 0xf2
	jt[DELEGATECALL] = undefined // 0xf4
	jt[CREATE2] = undefined      // 0xf5
	jt[STATICCALL] = undefined   // 0xfa
	jt[SELFDESTRUCT] = undefined // 0xff

	jt[RJUMP] = &operation{
		execute:       opRjump,
		constantGas:   GasQuickStep,
		immediateSize: 2,
	}
	jt[RJUMPI] = &operation{
		execute:       opRjumpi,
		constantGas:   GasSwiftStep,
		numPop:        1,
		immediateSize: 2,
	}
	jt[RJUMPV] = &operation{
		execute:       opRjumpv,
		constantGas:   GasSwiftStep,
		numPop:        1,
		immediateSize: 1,
	}
	jt[CALLF] = &operation{
		execute:       opCallf,
		constantGas:   GasFastStep,
		immediateSize: 2,
	}
	jt[RETF] = &operation{
		execute:     opRetf,
		constantGas: GasFastestStep,
		terminal:    true,
	}
	jt[JUMPF] = &operation{
		execute:       opJumpf,
		constantGas:   GasFastStep,
		terminal:      true,
		immediateSize: 2,
	}
	jt[DUPN] = &operation{
		execute:       opDupN,
		constantGas:   GasFastestStep,
		numPop:        0,
		numPush:       1,
		immediateSize: 1,
	}
	jt[SWAPN] = &operation{
		execute:       opSwapN,
		constantGas:   GasFastestStep,
		immediateSize: 1,
	}
	jt[EXCHANGE] = &operation{
		execute:       opExchange,
		constantGas:   GasFastestStep,
		immediateSize: 1,
	}
	jt[DATALOAD] = &operation{
		execute:     opDataLoad,
		constantGas: GasSwiftStep,
		numPop:      1,
		numPush:     1,
	}
	jt[DATALOADN] = &operation{
		execute:       opDataLoadN,
		constantGas:   GasFastestStep,
		numPush:       1,
		immediateSize: 2,
	}
	jt[DATASIZE] = &operation{
		execute:     opDataSize,
		constantGas: GasQuickStep,
		numPush:     1,
	}
	jt[DATACOPY] = &operation{
		execute:     opDataCopy,
		constantGas: GasFastestStep,
		dynamicGas:  gasDataCopy,
		numPop:      3,
		memorySize:  memoryDataCopy,
	}
	jt[EOFCREATE] = &operation{
		execute:       opEOFCreate,
		constantGas:   params.CreateGas,
		dynamicGas:    gasEOFCreate,
		numPop:        4,
		numPush:       1,
		immediateSize: 1,
		memorySize:    memoryEOFCreate,
	}
	jt[RETURNCODE] = &operation{
		execute:       opReturnCode,
		dynamicGas:    pureMemoryGascost,
		numPop:        2,
		immediateSize: 1,
		memorySize:    memoryReturnContract,
		terminal:      true,
	}
	jt[RETURNDATALOAD] = &operation{
		execute:     opReturnDataLoad,
		constantGas: GasFastestStep,
		numPop:      1,
		numPush:     1,
	}
	jt[EXTCALL] = &operation{
		execute:     opExtCall,
		constantGas: 100,
		dynamicGas:  gasExtCall,
		numPop:      4,
		numPush:     1,
		memorySize:  memoryExtCall,
	}
	jt[EXTDELEGATECALL] = &operation{
		execute:     opExtDelegateCall,
		constantGas: 100,
		dynamicGas:  gasExtDelegateCall,
		numPop:      3,
		numPush:     1,
		memorySize:  memoryExtCall,
	}
	jt[EXTSTATICCALL] = &operation{
		execute:     opExtStaticCall,
		constantGas: 100,
		dynamicGas:  gasExtStaticCall,
		numPop:      3,
		numPush:     1,
		memorySize:  memoryExtCall,
	}

	immSize := uint8(1)
	for op := 0x60; op < 0x60+32; op++ {
		jt[op].immediateSize = immSize
		immSize++
	}
}

// opRjump implements the rjump opcode.
func opRjump(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		code   = scope.Contract.CodeAt(scope.CodeSection)
		offset = parseInt16(code[*pc+1:])
	)
	// move pc past op and operand (+3), add relative offset, subtract 1 to
	// account for interpreter loop.
	*pc = uint64(int64(*pc+3) + int64(offset) - 1)
	return nil, nil
}

// opRjumpi implements the RJUMPI opcode
func opRjumpi(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	condition := scope.Stack.Pop()
	fmt.Println("condition: ", condition)
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
		code   = scope.Contract.CodeAt(scope.CodeSection)
		maxIdx = uint64(code[*pc+1])
		_case  = scope.Stack.Pop()
	)
	pcPost := *pc + 1 + 1 + (maxIdx+1)*2 - 1 // we do pc++ in interperter Run

	if case64, overflow := _case.Uint64WithOverflow(); overflow || case64 > maxIdx {
		// Index out-of-bounds, don't branch, just skip over immediate
		// argument.
		*pc = pcPost
		return nil, nil
	}
	relOffset := parseInt16(code[*pc+2+2*_case.Uint64():])
	*pc = pcPost + uint64(relOffset)
	return nil, nil
}

// inline code_iterator callf(StackTop stack, ExecutionState& state, code_iterator pos) noexcept
// {
//     const auto index = read_uint16_be(&pos[1]);
//     const auto& header = state.analysis.baseline->eof_header;
//     const auto stack_size = &stack.top() - state.stack_space.bottom();

//     const auto callee_required_stack_size =
//         header.types[index].max_stack_height - header.types[index].inputs;
//     if (stack_size + callee_required_stack_size > StackSpace::limit)
//     {
//         state.status = EVMC_STACK_OVERFLOW;
//         return nullptr;
//     }

//     if (state.call_stack.size() >= StackSpace::limit)
//     {
//         // TODO: Add different error code.
//         state.status = EVMC_STACK_OVERFLOW;
//         return nullptr;
//     }
//     state.call_stack.push_back(pos + 3);

//     const auto offset = header.code_offsets[index] - header.code_offsets[0];
//     auto code = state.analysis.baseline->executable_code;
//     return code.data() + offset;
// }

// opCallf implements the CALLF opcode
func opCallf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	fmt.Println(scope.CodeSection)
	code := scope.Contract.CodeAt(scope.CodeSection)
	fmt.Printf("code: 0x%x\n", code)
	fmt.Println("PC: ", *pc)
	idx := binary.BigEndian.Uint16(code[*pc+1:])
	fmt.Println("IDX: ", idx)
	fmt.Println(code[*pc+1:])
	var a uint64 = 0xFFFFFFFF_FFFFFFFF
	fmt.Println(a + 1)
	typ := scope.Contract.Container._types[idx]
	// fmt.Println("CODE SECTION", scope.CodeSection)
	// fmt.Println(scope.Contract.CodeAt(scope.CodeSection))
	// fmt.Println(scope.Contract.CodeAt(scope.CodeSection)[*pc+1:])
	// var (
	// 	code = scope.Contract.CodeAt(scope.CodeSection)
	// 	idx  = binary.BigEndian.Uint16(code[*pc+1:])
	// 	typ  = scope.Contract.Container.Types[idx]
	// )

	// fmt.Printf("StackLen: %v, typ.MaxStackHeight: %v, typ.Inputs: %v\n", scope.Stack.Len(), typ.MaxStackHeight, typ.Inputs)
	if scope.Stack.Len()+int(typ.maxStackHeight)-int(typ.inputs) > 1024 {
		return nil, fmt.Errorf("CALLF stack overflow: StackLen: %v, typ.MaxStackHeight: %v, typ.Inputs: %v", scope.Stack.Len(), typ.maxStackHeight, typ.inputs)
	}
	if len(scope.ReturnStack) > 1024 {
		return nil, fmt.Errorf("CALLF return_stack limit reached")
	}

	retCtx := &ReturnContext{
		Section:     scope.CodeSection,
		Pc:          *pc + 3,
		StackHeight: scope.Stack.Len() - int(typ.inputs),
	}
	scope.ReturnStack = append(scope.ReturnStack, retCtx)
	scope.CodeSection = uint64(idx)
	*pc = 0xFFFFFFFF_FFFFFFFF // set all bits, so when we increment pc it will become pc = 0
	return nil, nil
}

// opRetf implements the RETF opcode
func opRetf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		last   = len(scope.ReturnStack) - 1
		retCtx = scope.ReturnStack[last]
	)
	scope.ReturnStack = scope.ReturnStack[:last]
	scope.CodeSection = retCtx.Section
	*pc = retCtx.Pc - 1

	// If returning from top frame, exit cleanly.
	if len(scope.ReturnStack) == 0 {
		return nil, errStopToken
	}
	return nil, nil
}

func opJumpf(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		code = scope.Contract.CodeAt(scope.CodeSection)
		idx  = binary.BigEndian.Uint16(code[*pc+1:])
		typ  = scope.Contract.Container._types[idx]
	)
	fmt.Println("JUMPF index: ", idx)
	if scope.Stack.Len()+int(typ.maxStackHeight)-int(typ.inputs) > 1024 {
		return nil, fmt.Errorf("JUMPF stack overflow: StackLen: %v, typ.MaxStackHeight: %v, typ.Inputs: %v", scope.Stack.Len(), typ.maxStackHeight, typ.inputs)
	}
	scope.CodeSection = uint64(idx)
	*pc = 0xFFFFFFFF_FFFFFFFF
	return nil, nil
}

func opDupN(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		code = scope.Contract.CodeAt(scope.CodeSection)
		idx  = int(code[*pc+1])
	)
	scope.Stack.DupN(idx)
	*pc += 1 // we do one more pc++ in the interpeter loop
	return nil, nil
}

func opSwapN(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		code = scope.Contract.CodeAt(scope.CodeSection)
		idx  = int(code[*pc+1]) + 1
	)
	scope.Stack.SwapWith(0, idx)
	*pc += 1
	return nil, nil
}

func opExchange(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		code = scope.Contract.CodeAt(scope.CodeSection)
		n    = (int(code[*pc+1]) >> 4) + 1
		m    = (int(code[*pc+1]) & 0x0f) + 1
	)
	scope.Stack.SwapWith(n, n+m)
	*pc += 1
	return nil, nil
}

func opDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		index = scope.Stack.Peek()
		data  = scope.Contract.Data()
		// with overflow maybe?
	)

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
	var (
		code   = scope.Contract.CodeAt(scope.CodeSection)
		data   = scope.Contract.Data()
		offset = int(binary.BigEndian.Uint16(code[*pc+1:]))
	)
	// if len(data) < 32 || len(data)-32 < offset {
	// 	return nil, ErrInvalidMemoryAccess
	// }
	fmt.Printf("DataSize: %v, OFFSET: %v\n", len(data), offset)
	val := new(uint256.Int).SetBytes(data[offset : offset+32])
	scope.Stack.Push(val)

	*pc += 2 // one more +1 we do in the interpreter loop
	return nil, nil
}

func opDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	dataSize := len(scope.Contract.Data())
	val := new(uint256.Int).SetUint64(uint64(dataSize))
	scope.Stack.Push(val)
	return nil, nil
}

func opDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	var (
		memIndex256  = scope.Stack.Pop()
		dataIndex256 = scope.Stack.Pop()
		size256      = scope.Stack.Pop()

		data        = scope.Contract.Data()
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
	fmt.Printf("src: %d, s: %d, copySize: %d\n", src, s, copySize)

	if copySize > 0 {
		fmt.Println("CopyFromData")
		scope.Memory.CopyFromData(dst, data, src, copySize)
	}

	if s-copySize > 0 {
		fmt.Println("Setting Zero")
		scope.Memory.SetZero(dst+copySize, s-copySize)
	}
	fmt.Println("Exiting")
	return nil, nil
}

func opEOFCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	// if (state.in_static_mode())
	// return {EVMC_STATIC_MODE_VIOLATION, gas_left};
	if interpreter.readOnly { // STATICCALL
		return nil, ErrStaticModeViolation
	}

	var (
		code             = scope.Contract.CodeAt(scope.CodeSection)
		initContainerIdx = code[*pc+1]

		endowment = scope.Stack.Pop()
		salt      = scope.Stack.Pop()
		offset    = scope.Stack.Pop()
		size      = scope.Stack.Pop()
		input     []byte
		gas       = scope.Contract.Gas
	)
	*pc += 1

	initContainer := scope.Contract.SubcontainerAt(int(initContainerIdx))
	// TODO(racytech): this should be done in `dynamicGas` func, leave it here for now
	hashingCharge := uint64(6 * ((len(initContainer.rawData) + 31) / 32))
	if ok := scope.Contract.UseGas(hashingCharge, tracing.GasChangeCallContractEOFCreation); !ok {
		return nil, ErrOutOfGas
	}
	igas := int64(gas) - int64(hashingCharge) // TODO(racytech): make it better (doesn't look good)
	if igas <= 0 {
		return nil, ErrOutOfGas
	}
	gas = uint64(igas)

	gas -= gas / 64
	if ok := scope.Contract.UseGas(gas, tracing.GasChangeCallContractEOFCreation); !ok {
		return nil, ErrOutOfGas
	}
	if size.Uint64() > 0 {
		input = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	}
	stackValue := size
	res, addr, returnGas, suberr := interpreter.evm.EOFCreate(scope.Contract, input, initContainer.rawData, gas, &endowment, &salt, false)
	// fmt.Printf("EOFCREATE out_data: 0x%x, addr: 0x%x, returnGas: %v, suberr: %v\n", res, addr, returnGas, suberr)
	// Push item on the stack based on the returned error.
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

func opTxnCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	var (
		// code = scope.Contract.CodeAt(scope.CodeSection)

		initcodeHash = scope.Stack.Pop()
		value        = scope.Stack.Pop()
		salt         = scope.Stack.Pop()
		offset       = scope.Stack.Pop()
		size         = scope.Stack.Pop()
		input        = scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
		gas          = scope.Contract.Gas
	)
	*pc += 1

	initContainer := interpreter.evm.TxContext.Initcodes[initcodeHash.Bytes32()]

	// initcontainer = state.get_tx_initcode_by_hash(initcode_hash);
	// // In case initcode was not found, empty bytes_view was returned.
	// // Transaction initcodes are not allowed to be empty.
	// if (initcontainer.empty())
	// 	return {EVMC_SUCCESS, gas_left};  // "Light" failure

	// TODO(racytech): do the gas calculations
	// // Charge for initcode validation.
	// constexpr auto initcode_word_cost_validation = 2;
	// const auto initcode_cost_validation =
	// 	num_words(initcontainer.size()) * initcode_word_cost_validation;
	// if ((gas_left -= initcode_cost_validation) < 0)
	// 	return {EVMC_OUT_OF_GAS, gas_left};

	// TODO(racytech): we need to check data field in the message, since it contains initcodes, as well as adding initcodes into the execution env
	// 1. get the initcontainer -> get the coresponding initcode using hash (initcode_hash poped from stack)
	// 2. we need to run validation and unmarshalling on initcontainer again?

	stackValue := size
	res, addr, returnGas, suberr := interpreter.evm.TxnCreate(scope.Contract, input, initContainer, gas, &value, &salt, false)
	// Push item on the stack based on the returned error.
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
	var (
		code               = scope.Contract.CodeAt(scope.CodeSection)
		deployContainerIdx = int(code[*pc+1])
		offset256          = scope.Stack.Pop()
		size256            = scope.Stack.Pop()
	)
	// deployContainerIdx = 255
	fmt.Println("len(scope.Contract.Container._subContainers): ", len(scope.Contract.Container._subContainers))
	if deployContainerIdx >= len(scope.Contract.Container._subContainers) {
		return nil, fmt.Errorf("invalid subcontainer index: deployContainerIdx=%v, len(scope.Contract.Container.SubContainers)=%v", deployContainerIdx, len(scope.Contract.Container._subContainers))
	}
	fmt.Println("deploy container idx:", deployContainerIdx)
	*pc += 1
	offset := int64(offset256.Uint64())
	size := int64(size256.Uint64())
	container := scope.Contract.SubcontainerAt(deployContainerIdx)
	fmt.Printf("deployContainer: 0x%x, len: %v\n", container._data, len(container._data))
	auxData := scope.Memory.GetCopy(offset, size)
	fmt.Printf("auxData: 0x%x, len: %v\n", auxData, len(auxData))
	if err := container.updateData(auxData); err != nil {
		return nil, err
	}
	fmt.Printf("deployContainer: 0x%x, len: %v\n", container._data, len(container._data))

	if len(container.rawData) == 0 {
		return nil, fmt.Errorf("len(container._data) == 0")
	}

	// d, err := MarshalEOF(container, 0)
	// if err != nil {
	// 	return nil, err
	// }
	// fmt.Println("len(d): ", len(d))
	return container.rawData, errStopToken
}

func opReturnDataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {

	var index256 = scope.Stack.Peek()
	start := int(index256.Uint64())
	b := [32]byte{}
	if len(interpreter.returnData) < start {
		index256.SetBytes32(b[:]) // set zero
	} else {
		end := min(start+32, len(interpreter.returnData))
		for i := 0; i < end-start; i++ {
			b[i] = interpreter.returnData[start+i]
		}
		index256.SetBytes32(b[:])
		fmt.Println("RETURNDATALOAD: ", b)
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
	fmt.Println("BEFORE: ")
	var (
		ret       []byte
		returnGas uint64
		err       error
	)
	fmt.Println("GAS: ", gas)
	if gas == 0 {
		// zero temp call gas indicates a min retained gas error
		ret, returnGas, err = nil, 0, ErrExecutionReverted
	} else {
		ret, returnGas, err = interpreter.evm.ExtCall(scope.Contract, toAddr, args, gas, &value)
	}

	fmt.Println("opExtCall ERR: ", err)
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
	if gas == 0 {
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}
	if addr256.ByteLen() > 20 {
		return nil, fmt.Errorf("argument out of range")
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
	if !hasEOFMagic(code) { // TODO(racytech): see if this part is correct and can be done better
		fmt.Println("DOES NOT HAVE EOF MAGIC")
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}
	fmt.Printf("ADDR BEFORE: 0x%x\n", addr256.Bytes32())
	fmt.Printf("scope.Contract.Value(): 0x%x\n", scope.Contract.Value().Bytes32())
	ret, returnGas, err := interpreter.evm.ExtDelegateCall(scope.Contract, toAddr, args, gas)
	if err == ErrExecutionReverted || err == ErrDepth {
		addr256.SetOne()
	} else if err != nil {
		addr256.SetUint64(2)
	} else {
		addr256.Clear()
	}
	fmt.Printf("ADDR: 0x%x\n", addr256.Bytes32())
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

	if gas == 0 {
		addr256.SetOne()
		scope.Stack.Push(&addr256)
		scope.Contract.RefundGas(gas, tracing.GasChangeCallLeftOverRefunded)
		interpreter.returnData = nil
		return nil, nil
	}

	if addr256.ByteLen() > 20 {
		return nil, fmt.Errorf("argument out of range")
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
