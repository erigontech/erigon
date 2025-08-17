// Copyright 2015 The go-ethereum Authors
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
	"fmt"

	"github.com/erigontech/erigon/execution/chain/params"
)

type (
	executionFunc func(pc *uint64, interpreter *EVMInterpreter, callContext *ScopeContext) ([]byte, error)
	gasFunc       func(*EVM, *Contract, *Stack, *Memory, uint64) (uint64, error) // last parameter is the requested memory size as a uint64
	// memorySizeFunc returns the required size, and whether the operation overflowed a uint64
	memorySizeFunc func(*Stack) (size uint64, overflow bool)
	stringer       func(pc uint64, callContext *ScopeContext) string
)

type operation struct {
	// execute is the operation function
	execute     executionFunc
	constantGas uint64
	dynamicGas  gasFunc
	// maxStack specifies the max length the stack can have for this operation
	// to not overflow the stack.
	maxStack int

	// numPop tells how many stack items are required
	numPop  int // δ in the Yellow Paper
	numPush int // α in the Yellow Paper
	isPush  bool
	isSwap  bool
	isDup   bool
	opNum   int // only for push, swap, dup
	// memorySize returns the memory size required for the operation
	memorySize memorySizeFunc
	string     stringer
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
	napoliInstructionSet           = newNapoliInstructionSet()
	bhilaiInstructionSet           = newBhilaiInstructionSet()
	cancunInstructionSet           = newCancunInstructionSet()
	pragueInstructionSet           = newPragueInstructionSet()
	osakaInstructionSet            = newOsakaInstructionSet()
)

// JumpTable contains the EVM opcodes supported at a given fork.
type JumpTable [256]*operation

func validateAndFillMaxStack(jt *JumpTable) {
	for i, op := range jt {
		if op == nil {
			panic(fmt.Sprintf("op 0x%x is not set", i))
		}
		// The interpreter has an assumption that if the memorySize function is
		// set, then the dynamicGas function is also set. This is a somewhat
		// arbitrary assumption, and can be removed if we need to -- but it
		// allows us to avoid a condition check. As long as we have that assumption
		// in there, this little sanity check prevents us from merging in a
		// change which violates it.
		if op.memorySize != nil && op.dynamicGas == nil {
			panic(fmt.Sprintf("op %v has dynamic memory but not dynamic gas", OpCode(i).String()))
		}
		op.maxStack = maxStack(op.numPop, op.numPush)
	}
}

// newPragueInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, london, paris, shanghai,
// cancun, and prague instructions.
func newPragueInstructionSet() JumpTable {
	instructionSet := newCancunInstructionSet()
	enable7702(&instructionSet) // EIP-7702: set code tx
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newCancunInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, london, paris, shanghai,
// and cancun instructions.
func newCancunInstructionSet() JumpTable {
	instructionSet := newNapoliInstructionSet()
	enable4844(&instructionSet) // BLOBHASH opcode
	enable7516(&instructionSet) // BLOBBASEFEE opcode
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

func newBhilaiInstructionSet() JumpTable {
	instructionSet := newNapoliInstructionSet()
	enable7702(&instructionSet) // EIP-7702: set code tx
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

func newNapoliInstructionSet() JumpTable {
	instructionSet := newShanghaiInstructionSet()
	enable1153(&instructionSet) // Transient storage opcodes
	enable5656(&instructionSet) // MCOPY opcode
	enable6780(&instructionSet) // SELFDESTRUCT only in same transaction
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newShanghaiInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, london, paris, and shanghai instructions.
func newShanghaiInstructionSet() JumpTable {
	instructionSet := newLondonInstructionSet()
	enable3855(&instructionSet) // PUSH0 instruction https://eips.ethereum.org/EIPS/eip-3855
	enable3860(&instructionSet) // Limit and meter initcode https://eips.ethereum.org/EIPS/eip-3860
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newLondonInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg, berlin, and london instructions.
func newLondonInstructionSet() JumpTable {
	instructionSet := newBerlinInstructionSet()
	enable3529(&instructionSet) // Reduction in refunds https://eips.ethereum.org/EIPS/eip-3529
	enable3198(&instructionSet) // Base fee opcode https://eips.ethereum.org/EIPS/eip-3198
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newBerlinInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul, petersburg and berlin instructions.
func newBerlinInstructionSet() JumpTable {
	instructionSet := newIstanbulInstructionSet()
	enable2929(&instructionSet) // Access lists for trie accesses https://eips.ethereum.org/EIPS/eip-2929
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newIstanbulInstructionSet returns the frontier, homestead, byzantium,
// constantinople, istanbul and petersburg instructions.
func newIstanbulInstructionSet() JumpTable {
	instructionSet := newConstantinopleInstructionSet()

	enable1344(&instructionSet) // ChainID opcode - https://eips.ethereum.org/EIPS/eip-1344
	enable1884(&instructionSet) // Reprice reader opcodes - https://eips.ethereum.org/EIPS/eip-1884
	enable2200(&instructionSet) // Net metered SSTORE - https://eips.ethereum.org/EIPS/eip-2200

	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newConstantinopleInstructionSet returns the frontier, homestead,
// byzantium and constantinople instructions.
func newConstantinopleInstructionSet() JumpTable {
	instructionSet := newByzantiumInstructionSet()
	instructionSet[SHL] = &operation{
		execute:     opSHL,
		constantGas: GasFastestStep,
		numPop:      2,
		numPush:     1,
	}
	instructionSet[SHR] = &operation{
		execute:     opSHR,
		constantGas: GasFastestStep,
		numPop:      2,
		numPush:     1,
	}
	instructionSet[SAR] = &operation{
		execute:     opSAR,
		constantGas: GasFastestStep,
		numPop:      2,
		numPush:     1,
	}
	instructionSet[EXTCODEHASH] = &operation{
		execute:     opExtCodeHash,
		constantGas: params.ExtcodeHashGasConstantinople,
		numPop:      1,
		numPush:     1,
	}
	instructionSet[CREATE2] = &operation{
		execute:     opCreate2,
		constantGas: params.Create2Gas,
		dynamicGas:  gasCreate2,
		numPop:      4,
		numPush:     1,
		memorySize:  memoryCreate2,
	}
	validateAndFillMaxStack(&instructionSet)
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
		numPop:      6,
		numPush:     1,
		memorySize:  memoryStaticCall,
		string:      stStaticCall,
	}
	instructionSet[RETURNDATASIZE] = &operation{
		execute:     opReturnDataSize,
		constantGas: GasQuickStep,
		numPop:      0,
		numPush:     1,
	}
	instructionSet[RETURNDATACOPY] = &operation{
		execute:     opReturnDataCopy,
		constantGas: GasFastestStep,
		dynamicGas:  gasReturnDataCopy,
		numPop:      3,
		numPush:     0,
		memorySize:  memoryReturnDataCopy,
		string:      stReturnDataCopy,
	}
	instructionSet[REVERT] = &operation{
		execute:    opRevert,
		dynamicGas: gasRevert,
		numPop:     2,
		numPush:    0,
		memorySize: memoryRevert,
	}
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// EIP 158 a.k.a Spurious Dragon
func newSpuriousDragonInstructionSet() JumpTable {
	instructionSet := newTangerineWhistleInstructionSet()
	instructionSet[EXP].dynamicGas = gasExpEIP160
	validateAndFillMaxStack(&instructionSet)
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
	validateAndFillMaxStack(&instructionSet)
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
		numPop:      6,
		numPush:     1,
		memorySize:  memoryDelegateCall,
		string:      stDelegateCall,
	}
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

func newOsakaInstructionSet() JumpTable {
	instructionSet := newPragueInstructionSet()
	enable7939(&instructionSet) // EIP-7939 (CLZ opcode)
	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newFrontierInstructionSet returns the frontier instructions
// that can be executed during the frontier phase.
func newFrontierInstructionSet() JumpTable {
	tbl := JumpTable{
		STOP: {
			execute:     opStop,
			constantGas: 0,
			numPop:      0,
			numPush:     0,
		},
		ADD: {
			execute:     opAdd,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stAdd,
		},
		MUL: {
			execute:     opMul,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
			string:      stMul,
		},
		SUB: {
			execute:     opSub,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stSub,
		},
		DIV: {
			execute:     opDiv,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
			string:      stDiv,
		},
		SDIV: {
			execute:     opSdiv,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
			string:      stSdiv,
		},
		MOD: {
			execute:     opMod,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
			string:      stMod,
		},
		SMOD: {
			execute:     opSmod,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
			string:      stSmod,
		},
		ADDMOD: {
			execute:     opAddmod,
			constantGas: GasMidStep,
			numPop:      3,
			numPush:     1,
			string:      stAddmod,
		},
		MULMOD: {
			execute:     opMulmod,
			constantGas: GasMidStep,
			numPop:      3,
			numPush:     1,
			string:      stMulmod,
		},
		EXP: {
			execute:    opExp,
			dynamicGas: gasExpFrontier,
			numPop:     2,
			numPush:    1,
		},
		SIGNEXTEND: {
			execute:     opSignExtend,
			constantGas: GasFastStep,
			numPop:      2,
			numPush:     1,
		},
		LT: {
			execute:     opLt,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stLt,
		},
		GT: {
			execute:     opGt,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stGt,
		},
		SLT: {
			execute:     opSlt,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stSlt,
		},
		SGT: {
			execute:     opSgt,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stSgt,
		},
		EQ: {
			execute:     opEq,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stEq,
		},
		ISZERO: {
			execute:     opIszero,
			constantGas: GasFastestStep,
			numPop:      1,
			numPush:     1,
			string:      stIsZero,
		},
		AND: {
			execute:     opAnd,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stAnd,
		},
		XOR: {
			execute:     opXor,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stXor,
		},
		OR: {
			execute:     opOr,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
			string:      stOr,
		},
		NOT: {
			execute:     opNot,
			constantGas: GasFastestStep,
			numPop:      1,
			numPush:     1,
			string:      stNot,
		},
		BYTE: {
			execute:     opByte,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     1,
		},
		KECCAK256: {
			execute:     opKeccak256,
			constantGas: params.Keccak256Gas,
			dynamicGas:  gasKeccak256,
			numPop:      2,
			numPush:     1,
			memorySize:  memoryKeccak256,
		},
		ADDRESS: {
			execute:     opAddress,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		BALANCE: {
			execute:     opBalance,
			constantGas: params.BalanceGasFrontier,
			numPop:      1,
			numPush:     1,
		},
		ORIGIN: {
			execute:     opOrigin,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		CALLER: {
			execute:     opCaller,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		CALLVALUE: {
			execute:     opCallValue,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		CALLDATALOAD: {
			execute:     opCallDataLoad,
			constantGas: GasFastestStep,
			numPop:      1,
			numPush:     1,
			string:      stCallDataLoad,
		},
		CALLDATASIZE: {
			execute:     opCallDataSize,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
			string:      stCallDataSize,
		},
		CALLDATACOPY: {
			execute:     opCallDataCopy,
			constantGas: GasFastestStep,
			dynamicGas:  gasCallDataCopy,
			numPop:      3,
			numPush:     0,
			memorySize:  memoryCallDataCopy,
			string:      stCallDataCopy,
		},
		CODESIZE: {
			execute:     opCodeSize,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		CODECOPY: {
			execute:     opCodeCopy,
			constantGas: GasFastestStep,
			dynamicGas:  gasCodeCopy,
			numPop:      3,
			numPush:     0,
			memorySize:  memoryCodeCopy,
		},
		GASPRICE: {
			execute:     opGasprice,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		EXTCODESIZE: {
			execute:     opExtCodeSize,
			constantGas: params.ExtcodeSizeGasFrontier,
			numPop:      1,
			numPush:     1,
		},
		EXTCODECOPY: {
			execute:     opExtCodeCopy,
			constantGas: params.ExtcodeCopyBaseFrontier,
			dynamicGas:  gasExtCodeCopy,
			numPop:      4,
			numPush:     0,
			memorySize:  memoryExtCodeCopy,
		},
		BLOCKHASH: {
			execute:     opBlockhash,
			constantGas: GasExtStep,
			numPop:      1,
			numPush:     1,
			string:      stBlockhash,
		},
		COINBASE: {
			execute:     opCoinbase,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		TIMESTAMP: {
			execute:     opTimestamp,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		NUMBER: {
			execute:     opNumber,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		DIFFICULTY: {
			execute:     opDifficulty,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		GASLIMIT: {
			execute:     opGasLimit,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		POP: {
			execute:     opPop,
			constantGas: GasQuickStep,
			numPop:      1,
			numPush:     0,
		},
		MLOAD: {
			execute:     opMload,
			constantGas: GasFastestStep,
			dynamicGas:  gasMLoad,
			numPop:      1,
			numPush:     1,
			memorySize:  memoryMLoad,
			string:      stMload,
		},
		MSTORE: {
			execute:     opMstore,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore,
			numPop:      2,
			numPush:     0,
			memorySize:  memoryMStore,
			string:      stMstore,
		},
		MSTORE8: {
			execute:     opMstore8,
			constantGas: GasFastestStep,
			dynamicGas:  gasMStore8,
			memorySize:  memoryMStore8,
			numPop:      2,
			numPush:     0,
		},
		SLOAD: {
			execute:     opSload,
			constantGas: params.SloadGasFrontier,
			numPop:      1,
			numPush:     1,
			string:      stSload,
		},
		SSTORE: {
			execute:    opSstore,
			dynamicGas: gasSStore,
			numPop:     2,
			numPush:    0,
			string:     stSstore,
		},
		JUMP: {
			execute:     opJump,
			constantGas: GasMidStep,
			numPop:      1,
			numPush:     0,
			string:      stJump,
		},
		JUMPI: {
			execute:     opJumpi,
			constantGas: GasSlowStep,
			numPop:      2,
			numPush:     0,
			string:      stJumpi,
		},
		PC: {
			execute:     opPc,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
			string:      stPc,
		},
		MSIZE: {
			execute:     opMsize,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		GAS: {
			execute:     opGas,
			constantGas: GasQuickStep,
			numPop:      0,
			numPush:     1,
		},
		JUMPDEST: {
			execute:     opJumpdest,
			constantGas: params.JumpdestGas,
			numPop:      0,
			numPush:     0,
		},
		PUSH1: {
			execute:     opPush1,
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       1,
			string:      stPush1,
		},
		PUSH2: {
			execute:     opPush2,
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       2,
			string:      makePushStringer(2, 2),
		},
		PUSH3: {
			execute:     makePush(3, 3),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       3,
			string:      makePushStringer(3, 3),
		},
		PUSH4: {
			execute:     makePush(4, 4),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       4,
			string:      makePushStringer(4, 4),
		},
		PUSH5: {
			execute:     makePush(5, 5),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       5,
			string:      makePushStringer(5, 5),
		},
		PUSH6: {
			execute:     makePush(6, 6),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       6,
			string:      makePushStringer(6, 6),
		},
		PUSH7: {
			execute:     makePush(7, 7),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       7,
			string:      makePushStringer(7, 7),
		},
		PUSH8: {
			execute:     makePush(8, 8),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       8,
			string:      makePushStringer(8, 8),
		},
		PUSH9: {
			execute:     makePush(9, 9),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       9,
			string:      makePushStringer(9, 9),
		},
		PUSH10: {
			execute:     makePush(10, 10),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       10,
			string:      makePushStringer(10, 10),
		},
		PUSH11: {
			execute:     makePush(11, 11),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       11,
			string:      makePushStringer(11, 11),
		},
		PUSH12: {
			execute:     makePush(12, 12),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       12,
			string:      makePushStringer(12, 12),
		},
		PUSH13: {
			execute:     makePush(13, 13),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       13,
			string:      makePushStringer(13, 13),
		},
		PUSH14: {
			execute:     makePush(14, 14),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       14,
			string:      makePushStringer(14, 14),
		},
		PUSH15: {
			execute:     makePush(15, 15),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       15,
			string:      makePushStringer(15, 15),
		},
		PUSH16: {
			execute:     makePush(16, 16),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       16,
			string:      makePushStringer(16, 16),
		},
		PUSH17: {
			execute:     makePush(17, 17),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       17,
			string:      makePushStringer(17, 17),
		},
		PUSH18: {
			execute:     makePush(18, 18),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       18,
			string:      makePushStringer(18, 18),
		},
		PUSH19: {
			execute:     makePush(19, 19),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       19,
			string:      makePushStringer(19, 19),
		},
		PUSH20: {
			execute:     makePush(20, 20),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       20,
			string:      makePushStringer(20, 20),
		},
		PUSH21: {
			execute:     makePush(21, 21),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       21,
			string:      makePushStringer(21, 21),
		},
		PUSH22: {
			execute:     makePush(22, 22),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       22,
			string:      makePushStringer(22, 22),
		},
		PUSH23: {
			execute:     makePush(23, 23),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       23,
			string:      makePushStringer(23, 23),
		},
		PUSH24: {
			execute:     makePush(24, 24),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       24,
			string:      makePushStringer(24, 24),
		},
		PUSH25: {
			execute:     makePush(25, 25),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       25,
			string:      makePushStringer(25, 25),
		},
		PUSH26: {
			execute:     makePush(26, 26),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       26,
			string:      makePushStringer(26, 26),
		},
		PUSH27: {
			execute:     makePush(27, 27),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       27,
			string:      makePushStringer(27, 27),
		},
		PUSH28: {
			execute:     makePush(28, 28),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       28,
			string:      makePushStringer(28, 28),
		},
		PUSH29: {
			execute:     makePush(29, 29),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       29,
			string:      makePushStringer(29, 29),
		},
		PUSH30: {
			execute:     makePush(30, 30),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       30,
			string:      makePushStringer(30, 30),
		},
		PUSH31: {
			execute:     makePush(31, 31),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       31,
			string:      makePushStringer(31, 31),
		},
		PUSH32: {
			execute:     makePush(32, 32),
			constantGas: GasFastestStep,
			numPop:      0,
			numPush:     1,
			isPush:      true,
			opNum:       32,
			string:      makePushStringer(32, 32),
		},
		DUP1: {
			execute:     makeDup(1),
			constantGas: GasFastestStep,
			numPop:      1,
			numPush:     2,
			isDup:       true,
			opNum:       1,
			string:      makeDupStringer(1),
		},
		DUP2: {
			execute:     makeDup(2),
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     3,
			isDup:       true,
			opNum:       2,
			string:      makeDupStringer(2),
		},
		DUP3: {
			execute:     makeDup(3),
			constantGas: GasFastestStep,
			numPop:      3,
			numPush:     4,
			isDup:       true,
			opNum:       3,
			string:      makeDupStringer(3),
		},
		DUP4: {
			execute:     makeDup(4),
			constantGas: GasFastestStep,
			numPop:      4,
			numPush:     5,
			isDup:       true,
			opNum:       4,
			string:      makeDupStringer(4),
		},
		DUP5: {
			execute:     makeDup(5),
			constantGas: GasFastestStep,
			numPop:      5,
			numPush:     6,
			isDup:       true,
			opNum:       5,
			string:      makeDupStringer(5),
		},
		DUP6: {
			execute:     makeDup(6),
			constantGas: GasFastestStep,
			numPop:      6,
			numPush:     7,
			isDup:       true,
			opNum:       6,
			string:      makeDupStringer(6),
		},
		DUP7: {
			execute:     makeDup(7),
			constantGas: GasFastestStep,
			numPop:      7,
			numPush:     8,
			isDup:       true,
			opNum:       7,
			string:      makeDupStringer(7),
		},
		DUP8: {
			execute:     makeDup(8),
			constantGas: GasFastestStep,
			numPop:      8,
			numPush:     9,
			isDup:       true,
			opNum:       8,
			string:      makeDupStringer(8),
		},
		DUP9: {
			execute:     makeDup(9),
			constantGas: GasFastestStep,
			numPop:      9,
			numPush:     10,
			isDup:       true,
			opNum:       9,
			string:      makeDupStringer(9),
		},
		DUP10: {
			execute:     makeDup(10),
			constantGas: GasFastestStep,
			numPop:      10,
			numPush:     11,
			isDup:       true,
			opNum:       10,
			string:      makeDupStringer(10),
		},
		DUP11: {
			execute:     makeDup(11),
			constantGas: GasFastestStep,
			numPop:      11,
			numPush:     12,
			isDup:       true,
			opNum:       11,
			string:      makeDupStringer(11),
		},
		DUP12: {
			execute:     makeDup(12),
			constantGas: GasFastestStep,
			numPop:      12,
			numPush:     13,
			isDup:       true,
			opNum:       12,
			string:      makeDupStringer(12),
		},
		DUP13: {
			execute:     makeDup(13),
			constantGas: GasFastestStep,
			numPop:      13,
			numPush:     14,
			isDup:       true,
			opNum:       13,
			string:      makeDupStringer(13),
		},
		DUP14: {
			execute:     makeDup(14),
			constantGas: GasFastestStep,
			numPop:      14,
			numPush:     15,
			isDup:       true,
			opNum:       14,
			string:      makeDupStringer(14),
		},
		DUP15: {
			execute:     makeDup(15),
			constantGas: GasFastestStep,
			numPop:      15,
			numPush:     16,
			isDup:       true,
			opNum:       15,
			string:      makeDupStringer(15),
		},
		DUP16: {
			execute:     makeDup(16),
			constantGas: GasFastestStep,
			numPop:      16,
			numPush:     17,
			isDup:       true,
			opNum:       16,
			string:      makeDupStringer(16),
		},
		SWAP1: {
			execute:     opSwap1,
			constantGas: GasFastestStep,
			numPop:      2,
			numPush:     2,
			isSwap:      true,
			opNum:       1,
			string:      makeSwapStringer(1),
		},
		SWAP2: {
			execute:     opSwap2,
			constantGas: GasFastestStep,
			numPop:      3,
			numPush:     3,
			isSwap:      true,
			opNum:       2,
			string:      makeSwapStringer(2),
		},
		SWAP3: {
			execute:     opSwap3,
			constantGas: GasFastestStep,
			numPop:      4,
			numPush:     4,
			isSwap:      true,
			opNum:       3,
			string:      makeSwapStringer(3),
		},
		SWAP4: {
			execute:     opSwap4,
			constantGas: GasFastestStep,
			numPop:      5,
			numPush:     5,
			isSwap:      true,
			opNum:       4,
			string:      makeSwapStringer(4),
		},
		SWAP5: {
			execute:     opSwap5,
			constantGas: GasFastestStep,
			numPop:      6,
			numPush:     6,
			isSwap:      true,
			opNum:       5,
			string:      makeSwapStringer(5),
		},
		SWAP6: {
			execute:     opSwap6,
			constantGas: GasFastestStep,
			numPop:      7,
			numPush:     7,
			isSwap:      true,
			opNum:       6,
			string:      makeSwapStringer(6),
		},
		SWAP7: {
			execute:     opSwap7,
			constantGas: GasFastestStep,
			numPop:      8,
			numPush:     8,
			isSwap:      true,
			opNum:       7,
			string:      makeSwapStringer(7),
		},
		SWAP8: {
			execute:     opSwap8,
			constantGas: GasFastestStep,
			numPop:      9,
			numPush:     9,
			isSwap:      true,
			opNum:       8,
			string:      makeSwapStringer(8),
		},
		SWAP9: {
			execute:     opSwap9,
			constantGas: GasFastestStep,
			numPop:      10,
			numPush:     10,
			isSwap:      true,
			opNum:       9,
			string:      makeSwapStringer(9),
		},
		SWAP10: {
			execute:     opSwap10,
			constantGas: GasFastestStep,
			numPop:      11,
			numPush:     11,
			isSwap:      true,
			opNum:       10,
			string:      makeSwapStringer(10),
		},
		SWAP11: {
			execute:     opSwap11,
			constantGas: GasFastestStep,
			numPop:      12,
			numPush:     12,
			isSwap:      true,
			opNum:       11,
			string:      makeSwapStringer(11),
		},
		SWAP12: {
			execute:     opSwap12,
			constantGas: GasFastestStep,
			numPop:      13,
			numPush:     13,
			isSwap:      true,
			opNum:       12,
			string:      makeSwapStringer(12),
		},
		SWAP13: {
			execute:     opSwap13,
			constantGas: GasFastestStep,
			numPop:      14,
			numPush:     14,
			isSwap:      true,
			opNum:       13,
			string:      makeSwapStringer(13),
		},
		SWAP14: {
			execute:     opSwap14,
			constantGas: GasFastestStep,
			numPop:      15,
			numPush:     15,
			isSwap:      true,
			opNum:       14,
			string:      makeSwapStringer(14),
		},
		SWAP15: {
			execute:     opSwap15,
			constantGas: GasFastestStep,
			numPop:      16,
			numPush:     16,
			isSwap:      true,
			opNum:       15,
			string:      makeSwapStringer(15),
		},
		SWAP16: {
			execute:     opSwap16,
			constantGas: GasFastestStep,
			numPop:      17,
			numPush:     17,
			isSwap:      true,
			opNum:       16,
			string:      makeSwapStringer(16),
		},
		LOG0: {
			execute:    makeLog(0),
			dynamicGas: makeGasLog(0),
			numPop:     2,
			numPush:    0,
			memorySize: memoryLog,
		},
		LOG1: {
			execute:    makeLog(1),
			dynamicGas: makeGasLog(1),
			numPop:     3,
			numPush:    0,
			memorySize: memoryLog,
		},
		LOG2: {
			execute:    makeLog(2),
			dynamicGas: makeGasLog(2),
			numPop:     4,
			numPush:    0,
			memorySize: memoryLog,
		},
		LOG3: {
			execute:    makeLog(3),
			dynamicGas: makeGasLog(3),
			numPop:     5,
			numPush:    0,
			memorySize: memoryLog,
		},
		LOG4: {
			execute:    makeLog(4),
			dynamicGas: makeGasLog(4),
			numPop:     6,
			numPush:    0,
			memorySize: memoryLog,
		},
		CREATE: {
			execute:     opCreate,
			constantGas: params.CreateGas,
			dynamicGas:  gasCreate,
			numPop:      3,
			numPush:     1,
			memorySize:  memoryCreate,
		},
		CALL: {
			execute:     opCall,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCall,
			numPop:      7,
			numPush:     1,
			memorySize:  memoryCall,
			string:      stCall,
		},
		CALLCODE: {
			execute:     opCallCode,
			constantGas: params.CallGasFrontier,
			dynamicGas:  gasCallCode,
			numPop:      7,
			numPush:     1,
			memorySize:  memoryCall,
		},
		RETURN: {
			execute:    opReturn,
			dynamicGas: gasReturn,
			numPop:     2,
			numPush:    0,
			memorySize: memoryReturn,
		},
		SELFDESTRUCT: {
			execute:    opSelfdestruct,
			dynamicGas: gasSelfdestruct,
			numPop:     1,
			numPush:    0,
		},
	}

	// Fill all unassigned slots with opUndefined.
	for i, entry := range tbl {
		if entry == nil {
			tbl[i] = &operation{execute: opUndefined}
		}
	}

	validateAndFillMaxStack(&tbl)
	return tbl
}
