// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
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
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

type (
	executionFunc func(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error)
	gasFunc       func(*EVM, *Contract, *stack.Stack, *Memory, uint64) (uint64, error) // last parameter is the requested memory size as a uint64
	// memorySizeFunc returns the required size, and whether the operation overflowed a uint64
	memorySizeFunc func(*stack.Stack) (size uint64, overflow bool)
)

type operation struct {
	// execute is the operation function
	execute     executionFunc
	constantGas uint64
	dynamicGas  gasFunc
	// minStack tells how many stack items are required
	minStack int
	// maxStack specifies the max length the stack can have for this operation
	// to not overflow the stack.
	maxStack int

	numPop  int
	numPush int
	isPush  bool
	isSwap  bool
	isDup   bool
	opNum   int // only for push, swap, dup
	// memorySize returns the memory size required for the operation
	memorySize memorySizeFunc

	halts   bool // indicates whether the operation should halt further execution
	jumps   bool // indicates whether the program counter should not increment
	writes  bool // determines whether this a state modifying operation
	reverts bool // determines whether the operation reverts state (implicitly halts)
	returns bool // determines whether the operations sets the return data content
}

var (
	frontierInstructionSet         = newFrontierInstructionSet()
	homesteadInstructionSet        = newHomesteadInstructionSet()
	tangerineWhistleInstructionSet = newTangerineWhistleInstructionSet()
	spuriousDragonInstructionSet   = newSpuriousDragonInstructionSet()
	byzantiumInstructionSet        = newByzantiumInstructionSet()
	constantinopleInstructionSet   = newConstantinopleInstructionSet()
	istanbulInstructionSet         = newIstanbulInstructionSet()
	berlinInstructionSet           = newBerlinInstructionSet()
	londonInstructionSet           = newLondonInstructionSet()
	shanghaiInstructionSet         = newShanghaiInstructionSet()
	cancunInstructionSet           = newCancunInstructionSet()
)

// JumpTable contains the EVM opcodes supported at a given fork.
type JumpTable [256]*operation

// newCancunInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, london, paris, shanghai,
// and cancun instructions.
func newCancunInstructionSet() JumpTable {
	instructionSet := newShanghaiInstructionSet()
	return instructionSet
}

// newShanghaiInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, london, paris, and shanghai instructions.
func newShanghaiInstructionSet() JumpTable {
	instructionSet := newLondonInstructionSet()
	enable3855(&instructionSet) // PUSH0 instruction https://eips.ethereum.org/EIPS/eip-3855
	enable3860(&instructionSet) // Limit and meter initcode https://eips.ethereum.org/EIPS/eip-3860
	return instructionSet
}

// newLondonInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, and london instructions.
func newLondonInstructionSet() JumpTable {
	instructionSet := newBerlinInstructionSet()
	enable3529(&instructionSet) // Reduction in refunds https://eips.ethereum.org/EIPS/eip-3529
	enable3198(&instructionSet) // Base fee opcode https://eips.ethereum.org/EIPS/eip-3198
	return instructionSet
}

// newBerlinInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg and berlin instructions.
func newBerlinInstructionSet() JumpTable {
	instructionSet := newIstanbulInstructionSet()
	enable2929(&instructionSet) // Access lists for trie accesses https://eips.ethereum.org/EIPS/eip-2929
	return instructionSet
}

// newIstanbulInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul and petersburg instructions.
func newIstanbulInstructionSet() JumpTable {
	instructionSet := newConstantinopleInstructionSet()

	enable1344(&instructionSet) // ChainID opcode - https://eips.ethereum.org/EIPS/eip-1344
	enable1884(&instructionSet) // Reprice reader opcodes - https://eips.ethereum.org/EIPS/eip-1884
	enable2200(&instructionSet) // Net metered SSTORE - https://eips.ethereum.org/EIPS/eip-2200

	return instructionSet
}

// newConstantinopleInstructionSet returns the frontier, homestead,
// byzantium and constantinople instructions.
func newConstantinopleInstructionSet() JumpTable {
	instructionSet := newByzantiumInstructionSet()
	instructionSet[SHL] = &operation{
		execute:     opSHL,
		constantGas: GasFastestStep,
		minStack:    minStack(2, 1),
		maxStack:    maxStack(2, 1),
		numPop:      2,
		numPush:     1,
	}
	instructionSet[SHR] = &operation{
		execute:     opSHR,
		constantGas: GasFastestStep,
		minStack:    minStack(2, 1),
		maxStack:    maxStack(2, 1),
		numPop:      2,
		numPush:     1,
	}
	instructionSet[SAR] = &operation{
		execute:     opSAR,
		constantGas: GasFastestStep,
		minStack:    minStack(2, 1),
		maxStack:    maxStack(2, 1),
		numPop:      2,
		numPush:     1,
	}
	instructionSet[EXTCODEHASH] = &operation{
		execute:     opExtCodeHash,
		constantGas: params.ExtcodeHashGasConstantinople,
		minStack:    minStack(1, 1),
		maxStack:    maxStack(1, 1),
		numPop:      1,
		numPush:     1,
	}
	instructionSet[CREATE2] = &operation{
		execute:     opCreate2,
		constantGas: params.Create2Gas,
		dynamicGas:  gasCreate2,
		minStack:    minStack(4, 1),
		maxStack:    maxStack(4, 1),
		numPop:      4,
		numPush:     1,
		memorySize:  memoryCreate2,
		writes:      true,
		returns:     true,
	}
	return instructionSet
}

// newByzantiumInstructionSet returns the frontier, homestead and
// byzantium instructions.
func newByzantiumInstructionSet() JumpTable {
	instructionSet := newSpuriousDragonInstructionSet()
	instructionSet[STATICCALL] = &operation{
		execute:     opStaticCall,
		constantGas: params.CallGasEIP150,
		dynamicGas:  gasStaticCall,
		minStack:    minStack(6, 1),
		maxStack:    maxStack(6, 1),
		numPop:      6,
		numPush:     1,
		memorySize:  memoryStaticCall,
		returns:     true,
	}
	instructionSet[RETURNDATASIZE] = &operation{
		execute:     opReturnDataSize,
		constantGas: GasQuickStep,
		minStack:    minStack(0, 1),
		maxStack:    maxStack(0, 1),
		numPop:      0,
		numPush:     1,
	}
	instructionSet[RETURNDATACOPY] = &operation{
		execute:     opReturnDataCopy,
		constantGas: GasFastestStep,
		dynamicGas:  gasReturnDataCopy,
		minStack:    minStack(3, 0),
		maxStack:    maxStack(3, 0),
		numPop:      3,
		numPush:     0,
		memorySize:  memoryReturnDataCopy,
	}
	instructionSet[REVERT] = &operation{
		execute:    opRevert,
		dynamicGas: gasRevert,
		minStack:   minStack(2, 0),
		maxStack:   maxStack(2, 0),
		numPop:     2,
		numPush:    0,
		memorySize: memoryRevert,
		reverts:    true,
		returns:    true,
	}
	return instructionSet
}

// EIP 158 a.k.a Spurious Dragon
func newSpuriousDragonInstructionSet() JumpTable {
	instructionSet := newTangerineWhistleInstructionSet()
	instructionSet[EXP].dynamicGas = gasExpEIP160
	return instructionSet

}

// EIP 150 a.k.a Tangerine Whistle
func newTangerineWhistleInstructionSet() JumpTable {
	instructionSet := newHomesteadInstructionSet()
	instructionSet[BALANCE].constantGas = params.BalanceGasEIP150
	instructionSet[EXTCODESIZE].constantGas = params.ExtcodeSizeGasEIP150
	instructionSet[SLOAD].constantGas = params.SloadGasEIP150
	instructionSet[EXTCODECOPY].constantGas = params.ExtcodeCopyBaseEIP150
	instructionSet[CALL].constantGas = params.CallGasEIP150
	instructionSet[CALLCODE].constantGas = params.CallGasEIP150
	instructionSet[DELEGATECALL].constantGas = params.CallGasEIP150
	return instructionSet
}

// newHomesteadInstructionSet returns the frontier and homestead
// instructions that can be executed during the homestead phase.
func newHomesteadInstructionSet() JumpTable {
	instructionSet := newFrontierInstructionSet()
	instructionSet[DELEGATECALL] = &operation{
		execute:     opDelegateCall,
		dynamicGas:  gasDelegateCall,
		constantGas: params.CallGasFrontier,
		minStack:    minStack(6, 1),
		maxStack:    maxStack(6, 1),
		numPop:      6,
		numPush:     1,
		memorySize:  memoryDelegateCall,
		returns:     true,
	}
	return instructionSet
}

// newFrontierInstructionSet returns the frontier instructions
// that can be executed during the frontier phase.
func newFrontierInstructionSet() JumpTable {
	return JumpTable{
		STOP: {
			execute:     opStop,
			constantGas: 0,
			minStack:    minStack(0, 0),
			maxStack:    maxStack(0, 0),
			numPop:      0,
			numPush:     0,
			halts:       true,
		},
		ADD: {
			execute:     opAdd,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		MUL: {
			execute:     opMul,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		SUB: {
			execute:     opSub,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		DIV: {
			execute:     opDiv,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		SDIV: {
			execute:     opSdiv,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		MOD: {
			execute:     opMod,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		SMOD: {
			execute:     opSmod,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		ADDMOD: {
			execute:     opAddmod,
			constantGas: GasMidStep,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			numPop:      3,
			numPush:     1,
		},
		MULMOD: {
			execute:     opMulmod,
			constantGas: GasMidStep,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			numPop:      3,
			numPush:     1,
		},
		EXP: {
			execute:    opExp,
			dynamicGas: gasExpFrontier,
			minStack:   minStack(2, 1),
			maxStack:   maxStack(2, 1),
			numPop:     2,
			numPush:    1,
		},
		SIGNEXTEND: {
			execute:     opSignExtend,
			constantGas: GasFastStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		LT: {
			execute:     opLt,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		GT: {
			execute:     opGt,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		SLT: {
			execute:     opSlt,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		SGT: {
			execute:     opSgt,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		EQ: {
			execute:     opEq,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		ISZERO: {
			execute:     opIszero,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		AND: {
			execute:     opAnd,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		XOR: {
			execute:     opXor,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		OR: {
			execute:     opOr,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		NOT: {
			execute:     opNot,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		BYTE: {
			execute:     opByte,
			constantGas: GasFastestStep,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
		},
		KECCAK256: {
			execute:     opKeccak256,
			constantGas: params.Keccak256Gas,
			dynamicGas:  gasKeccak256,
			minStack:    minStack(2, 1),
			maxStack:    maxStack(2, 1),
			numPop:      2,
			numPush:     1,
			memorySize:  memoryKeccak256,
		},
		ADDRESS: {
			execute:     opAddress,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		BALANCE: {
			execute:     opBalance,
			constantGas: params.BalanceGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		ORIGIN: {
			execute:     opOrigin,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		CALLER: {
			execute:     opCaller,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		CALLVALUE: {
			execute:     opCallValue,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		CALLDATALOAD: {
			execute:     opCallDataLoad,
			constantGas: GasFastestStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		CALLDATASIZE: {
			execute:     opCallDataSize,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		CALLDATACOPY: {
			execute:     opCallDataCopy,
			constantGas: GasFastestStep,
			dynamicGas:  gasCallDataCopy,
			minStack:    minStack(3, 0),
			maxStack:    maxStack(3, 0),
			numPop:      3,
			numPush:     0,
			memorySize:  memoryCallDataCopy,
		},
		CODESIZE: {
			execute:     opCodeSize,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		CODECOPY: {
			execute:     opCodeCopy,
			constantGas: GasFastestStep,
			dynamicGas:  gasCodeCopy,
			minStack:    minStack(3, 0),
			maxStack:    maxStack(3, 0),
			numPop:      3,
			numPush:     0,
			memorySize:  memoryCodeCopy,
		},
		GASPRICE: {
			execute:     opGasprice,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		EXTCODESIZE: {
			execute:     opExtCodeSize,
			constantGas: params.ExtcodeSizeGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		EXTCODECOPY: {
			execute:     opExtCodeCopy,
			constantGas: params.ExtcodeCopyBaseFrontier,
			dynamicGas:  gasExtCodeCopy,
			minStack:    minStack(4, 0),
			maxStack:    maxStack(4, 0),
			numPop:      4,
			numPush:     0,
			memorySize:  memoryExtCodeCopy,
		},
		BLOCKHASH: {
			execute:     opBlockhash,
			constantGas: GasExtStep,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		COINBASE: {
			execute:     opCoinbase,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		TIMESTAMP: {
			execute:     opTimestamp,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		NUMBER: {
			execute:     opNumber,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		DIFFICULTY: {
			execute:     opDifficulty,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		GASLIMIT: {
			execute:     opGasLimit,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		POP: {
			execute:     opPop,
			constantGas: GasQuickStep,
			minStack:    minStack(1, 0),
			maxStack:    maxStack(1, 0),
			numPop:      1,
			numPush:     0,
		},
		MLOAD: {
			execute:     opMload,
			constantGas: GasFastestStep,
			dynamicGas:  gasMLoad,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
			memorySize:  memoryMLoad,
		},
		MSTORE: {
			execute:     opMstore,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),
			numPop:      2,
			numPush:     0,
			memorySize:  memoryMStore,
		},
		MSTORE8: {
			execute:     opMstore8,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore8,
			memorySize:  memoryMStore8,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),

			numPop:  2,
			numPush: 0,
		},
		SLOAD: {
			execute:     opSload,
			constantGas: params.SloadGasFrontier,
			minStack:    minStack(1, 1),
			maxStack:    maxStack(1, 1),
			numPop:      1,
			numPush:     1,
		},
		SSTORE: {
			execute:    opSstore,
			dynamicGas: gasSStore,
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			numPop:     2,
			numPush:    0,
			writes:     true,
		},
		JUMP: {
			execute:     opJump,
			constantGas: GasMidStep,
			minStack:    minStack(1, 0),
			maxStack:    maxStack(1, 0),
			numPop:      1,
			numPush:     0,
			jumps:       true,
		},
		JUMPI: {
			execute:     opJumpi,
			constantGas: GasSlowStep,
			minStack:    minStack(2, 0),
			maxStack:    maxStack(2, 0),
			numPop:      2,
			numPush:     0,
			jumps:       true,
		},
		PC: {
			execute:     opPc,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		MSIZE: {
			execute:     opMsize,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		GAS: {
			execute:     opGas,
			constantGas: GasQuickStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
		},
		JUMPDEST: {
			execute:     opJumpdest,
			constantGas: params.JumpdestGas,
			minStack:    minStack(0, 0),
			maxStack:    maxStack(0, 0),
			numPop:      0,
			numPush:     0,
		},
		PUSH1: {
			execute:     opPush1,
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       1,
		},
		PUSH2: {
			execute:     makePush(2, 2),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       2,
		},
		PUSH3: {
			execute:     makePush(3, 3),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       3,
		},
		PUSH4: {
			execute:     makePush(4, 4),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       4,
		},
		PUSH5: {
			execute:     makePush(5, 5),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       5,
		},
		PUSH6: {
			execute:     makePush(6, 6),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       6,
		},
		PUSH7: {
			execute:     makePush(7, 7),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       7,
		},
		PUSH8: {
			execute:     makePush(8, 8),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       8,
		},
		PUSH9: {
			execute:     makePush(9, 9),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       9,
		},
		PUSH10: {
			execute:     makePush(10, 10),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       10,
		},
		PUSH11: {
			execute:     makePush(11, 11),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       11,
		},
		PUSH12: {
			execute:     makePush(12, 12),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       12,
		},
		PUSH13: {
			execute:     makePush(13, 13),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       13,
		},
		PUSH14: {
			execute:     makePush(14, 14),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       14,
		},
		PUSH15: {
			execute:     makePush(15, 15),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       15,
		},
		PUSH16: {
			execute:     makePush(16, 16),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       16,
		},
		PUSH17: {
			execute:     makePush(17, 17),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       17,
		},
		PUSH18: {
			execute:     makePush(18, 18),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       18,
		},
		PUSH19: {
			execute:     makePush(19, 19),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       19,
		},
		PUSH20: {
			execute:     makePush(20, 20),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       20,
		},
		PUSH21: {
			execute:     makePush(21, 21),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       21,
		},
		PUSH22: {
			execute:     makePush(22, 22),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       22,
		},
		PUSH23: {
			execute:     makePush(23, 23),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       23,
		},
		PUSH24: {
			execute:     makePush(24, 24),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       24,
		},
		PUSH25: {
			execute:     makePush(25, 25),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       25,
		},
		PUSH26: {
			execute:     makePush(26, 26),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       26,
		},
		PUSH27: {
			execute:     makePush(27, 27),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       27,
		},
		PUSH28: {
			execute:     makePush(28, 28),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       28,
		},
		PUSH29: {
			execute:     makePush(29, 29),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       29,
		},
		PUSH30: {
			execute:     makePush(30, 30),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       30,
		},
		PUSH31: {
			execute:     makePush(31, 31),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       31,
		},
		PUSH32: {
			execute:     makePush(32, 32),
			constantGas: GasFastestStep,
			minStack:    minStack(0, 1),
			maxStack:    maxStack(0, 1),
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       32,
		},
		DUP1: {
			execute:     makeDup(1),
			constantGas: GasFastestStep,
			minStack:    minDupStack(1),
			maxStack:    maxDupStack(1),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       1,
		},
		DUP2: {
			execute:     makeDup(2),
			constantGas: GasFastestStep,
			minStack:    minDupStack(2),
			maxStack:    maxDupStack(2),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       2,
		},
		DUP3: {
			execute:     makeDup(3),
			constantGas: GasFastestStep,
			minStack:    minDupStack(3),
			maxStack:    maxDupStack(3),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       3,
		},
		DUP4: {
			execute:     makeDup(4),
			constantGas: GasFastestStep,
			minStack:    minDupStack(4),
			maxStack:    maxDupStack(4),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       4,
		},
		DUP5: {
			execute:     makeDup(5),
			constantGas: GasFastestStep,
			minStack:    minDupStack(5),
			maxStack:    maxDupStack(5),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       5,
		},
		DUP6: {
			execute:     makeDup(6),
			constantGas: GasFastestStep,
			minStack:    minDupStack(6),
			maxStack:    maxDupStack(6),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       6,
		},
		DUP7: {
			execute:     makeDup(7),
			constantGas: GasFastestStep,
			minStack:    minDupStack(7),
			maxStack:    maxDupStack(7),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       7,
		},
		DUP8: {
			execute:     makeDup(8),
			constantGas: GasFastestStep,
			minStack:    minDupStack(8),
			maxStack:    maxDupStack(8),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       8,
		},
		DUP9: {
			execute:     makeDup(9),
			constantGas: GasFastestStep,
			minStack:    minDupStack(9),
			maxStack:    maxDupStack(9),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       9,
		},
		DUP10: {
			execute:     makeDup(10),
			constantGas: GasFastestStep,
			minStack:    minDupStack(10),
			maxStack:    maxDupStack(10),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       10,
		},
		DUP11: {
			execute:     makeDup(11),
			constantGas: GasFastestStep,
			minStack:    minDupStack(11),
			maxStack:    maxDupStack(11),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       11,
		},
		DUP12: {
			execute:     makeDup(12),
			constantGas: GasFastestStep,
			minStack:    minDupStack(12),
			maxStack:    maxDupStack(12),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       12,
		},
		DUP13: {
			execute:     makeDup(13),
			constantGas: GasFastestStep,
			minStack:    minDupStack(13),
			maxStack:    maxDupStack(13),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       13,
		},
		DUP14: {
			execute:     makeDup(14),
			constantGas: GasFastestStep,
			minStack:    minDupStack(14),
			maxStack:    maxDupStack(14),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       14,
		},
		DUP15: {
			execute:     makeDup(15),
			constantGas: GasFastestStep,
			minStack:    minDupStack(15),
			maxStack:    maxDupStack(15),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       15,
		},
		DUP16: {
			execute:     makeDup(16),
			constantGas: GasFastestStep,
			minStack:    minDupStack(16),
			maxStack:    maxDupStack(16),
			numPop:      0,
			numPush:     1,
			isDup:       true,
			opNum:       16,
		},
		SWAP1: {
			execute:     makeSwap(1),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(2),
			maxStack:    maxSwapStack(2),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       1,
		},
		SWAP2: {
			execute:     makeSwap(2),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(3),
			maxStack:    maxSwapStack(3),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       2,
		},
		SWAP3: {
			execute:     makeSwap(3),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(4),
			maxStack:    maxSwapStack(4),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       3,
		},
		SWAP4: {
			execute:     makeSwap(4),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(5),
			maxStack:    maxSwapStack(5),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       4,
		},
		SWAP5: {
			execute:     makeSwap(5),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(6),
			maxStack:    maxSwapStack(6),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       5,
		},
		SWAP6: {
			execute:     makeSwap(6),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(7),
			maxStack:    maxSwapStack(7),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       6,
		},
		SWAP7: {
			execute:     makeSwap(7),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(8),
			maxStack:    maxSwapStack(8),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       7,
		},
		SWAP8: {
			execute:     makeSwap(8),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(9),
			maxStack:    maxSwapStack(9),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       8,
		},
		SWAP9: {
			execute:     makeSwap(9),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(10),
			maxStack:    maxSwapStack(10),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       9,
		},
		SWAP10: {
			execute:     makeSwap(10),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(11),
			maxStack:    maxSwapStack(11),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       10,
		},
		SWAP11: {
			execute:     makeSwap(11),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(12),
			maxStack:    maxSwapStack(12),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       11,
		},
		SWAP12: {
			execute:     makeSwap(12),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(13),
			maxStack:    maxSwapStack(13),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       12,
		},
		SWAP13: {
			execute:     makeSwap(13),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(14),
			maxStack:    maxSwapStack(14),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       13,
		},
		SWAP14: {
			execute:     makeSwap(14),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(15),
			maxStack:    maxSwapStack(15),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       14,
		},
		SWAP15: {
			execute:     makeSwap(15),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(16),
			maxStack:    maxSwapStack(16),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       15,
		},
		SWAP16: {
			execute:     makeSwap(16),
			constantGas: GasFastestStep,
			minStack:    minSwapStack(17),
			maxStack:    maxSwapStack(17),
			numPop:      1,
			numPush:     1,
			isSwap:      true,
			opNum:       16,
		},
		LOG0: {
			execute:    makeLog(0),
			dynamicGas: makeGasLog(0),
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			numPop:     2,
			numPush:    0,
			memorySize: memoryLog,
			writes:     true,
		},
		LOG1: {
			execute:    makeLog(1),
			dynamicGas: makeGasLog(1),
			minStack:   minStack(3, 0),
			maxStack:   maxStack(3, 0),
			numPop:     3,
			numPush:    0,
			memorySize: memoryLog,
			writes:     true,
		},
		LOG2: {
			execute:    makeLog(2),
			dynamicGas: makeGasLog(2),
			minStack:   minStack(4, 0),
			maxStack:   maxStack(4, 0),
			numPop:     4,
			numPush:    0,
			memorySize: memoryLog,
			writes:     true,
		},
		LOG3: {
			execute:    makeLog(3),
			dynamicGas: makeGasLog(3),
			minStack:   minStack(5, 0),
			maxStack:   maxStack(5, 0),
			numPop:     5,
			numPush:    0,
			memorySize: memoryLog,
			writes:     true,
		},
		LOG4: {
			execute:    makeLog(4),
			dynamicGas: makeGasLog(4),
			minStack:   minStack(6, 0),
			maxStack:   maxStack(6, 0),
			numPop:     6,
			numPush:    0,
			memorySize: memoryLog,
			writes:     true,
		},
		CREATE: {
			execute:     opCreate,
			constantGas: params.CreateGas,
			dynamicGas:  gasCreate,
			minStack:    minStack(3, 1),
			maxStack:    maxStack(3, 1),
			numPop:      3,
			numPush:     1,
			memorySize:  memoryCreate,
			writes:      true,
			returns:     true,
		},
		CALL: {
			execute:     opCall,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCall,
			minStack:    minStack(7, 1),
			maxStack:    maxStack(7, 1),
			numPop:      7,
			numPush:     1,
			memorySize:  memoryCall,
			returns:     true,
		},
		CALLCODE: {
			execute:     opCallCode,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCallCode,
			minStack:    minStack(7, 1),
			maxStack:    maxStack(7, 1),
			numPop:      7,
			numPush:     1,
			memorySize:  memoryCall,
			returns:     true,
		},
		RETURN: {
			execute:    opReturn,
			dynamicGas: gasReturn,
			minStack:   minStack(2, 0),
			maxStack:   maxStack(2, 0),
			numPop:     2,
			numPush:    0,
			memorySize: memoryReturn,
			halts:      true,
		},
		SELFDESTRUCT: {
			execute:    opSuicide,
			dynamicGas: gasSelfdestruct,
			minStack:   minStack(1, 0),
			maxStack:   maxStack(1, 0),
			numPop:     1,
			numPush:    0,
			halts:      true,
			writes:     true,
		},
	}
}
