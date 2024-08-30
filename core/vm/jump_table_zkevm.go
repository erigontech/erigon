package vm

var (
	forkID4InstructionSet            = newForkID4InstructionSet()
	forkID5DragonfruitInstructionSet = newForkID5DragonfruitInstructionSet()
	forkID8ElderberryInstructionSet  = newForkID8InstructionSet()
)

// newForkID4InstructionSet returns the instruction set for the forkID4
func newForkID4InstructionSet() JumpTable {
	instructionSet := newBerlinInstructionSet()

	enable2929_zkevm(&instructionSet) // Access lists for trie accesses https://eips.ethereum.org/EIPS/eip-2929

	// zkevm logs have data length multiple of 32
	// zeroes are added to the end in order to fill the 32 bytes if needed
	instructionSet[LOG1].execute = makeLog_zkevm_logIndexFromZero(1)
	instructionSet[LOG2].execute = makeLog_zkevm_logIndexFromZero(2)
	instructionSet[LOG3].execute = makeLog_zkevm_logIndexFromZero(3)
	instructionSet[LOG4].execute = makeLog_zkevm_logIndexFromZero(4)

	instructionSet[DELEGATECALL].execute = opDelegateCall_zkevm
	instructionSet[CALLCODE].execute = opCallCode_zkevm
	instructionSet[CALL].execute = opCall_zkevm
	instructionSet[CREATE].execute = opCreate_zkevm
	instructionSet[CREATE2].execute = opCreate2_zkevm

	instructionSet[CALLDATALOAD].execute = opCallDataLoad_zkevmIncompatible
	instructionSet[CALLDATACOPY].execute = opCallDataCopy_zkevmIncompatible

	instructionSet[STATICCALL].execute = opStaticCall_zkevm

	instructionSet[NUMBER].execute = opNumber_zkevm

	instructionSet[DIFFICULTY].execute = opDifficulty_zkevm

	instructionSet[BLOCKHASH].execute = opBlockhash_zkevm

	instructionSet[EXTCODEHASH].execute = opExtCodeHash_zkevm

	// SELFDESTRUCT is replaces by SENDALL
	instructionSet[SELFDESTRUCT] = &operation{
		execute:    opSendAll_zkevm,
		dynamicGas: gasSelfdestruct_zkevm,
		numPop:     1,
		numPush:    0,
	}

	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newForkID5DragonfruitInstructionSet returns the instruction set for the forkID5
func newForkID5DragonfruitInstructionSet() JumpTable {
	instructionSet := newForkID4InstructionSet()

	instructionSet[CALLDATACOPY].execute = opCallDataCopy
	instructionSet[CALLDATALOAD].execute = opCallDataLoad
	instructionSet[STATICCALL].execute = opStaticCall

	enable3855(&instructionSet) // EIP-3855: Enable PUSH0 opcode

	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}

// newForkID4InstructionSet returns the instruction set for the forkID4
func newForkID8InstructionSet() JumpTable {
	instructionSet := newForkID5DragonfruitInstructionSet()

	// zkevm logs have data length multiple of 32
	// zeroes are added to the end in order to fill the 32 bytes if needed
	instructionSet[LOG1].execute = makeLog_zkevm_regularLogIndexes(1)
	instructionSet[LOG2].execute = makeLog_zkevm_regularLogIndexes(2)
	instructionSet[LOG3].execute = makeLog_zkevm_regularLogIndexes(3)
	instructionSet[LOG4].execute = makeLog_zkevm_regularLogIndexes(4)

	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}
