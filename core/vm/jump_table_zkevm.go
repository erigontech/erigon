package vm

var (
	forkID4InstructionSet            = newForkID4InstructionSet()
	forkID5DragonfruitInstructionSet = newForkID5DragonfruitInstructionSet()
	forkID7EtrogInstructionSet       = newForkID7EtrogInstructionSet()
)

// newForkID4InstructionSet returns the instruction set for the forkID4
func newForkID4InstructionSet() JumpTable {
	instructionSet := newBerlinInstructionSet()

	enable2929_zkevm(&instructionSet) // Access lists for trie accesses https://eips.ethereum.org/EIPS/eip-2929

	instructionSet[LOG1].execute = makeLog_zkevm(1)
	instructionSet[LOG2].execute = makeLog_zkevm(2)
	instructionSet[LOG3].execute = makeLog_zkevm(3)
	instructionSet[LOG4].execute = makeLog_zkevm(4)

	instructionSet[CALLDATALOAD].execute = opCallDataLoad_zkevmIncompatible
	instructionSet[CALLDATACOPY].execute = opCallDataCopy_zkevmIncompatible

	instructionSet[STATICCALL].execute = opStaticCall_zkevm

	instructionSet[NUMBER].execute = opNumber_zkevm

	instructionSet[DIFFICULTY].execute = opDifficulty_zkevm

	instructionSet[BLOCKHASH].execute = opBlockhash_zkevm

	instructionSet[EXTCODEHASH].execute = opExtCodeHash_zkevm

	instructionSet[SENDALL] = &operation{
		execute:    opSendAll_zkevm,
		dynamicGas: gasSelfdestruct_zkevm,
		numPop:     1,
		numPush:    0,
	}

	// SELFDESTRUCT is replaces by SENDALL
	instructionSet[SELFDESTRUCT] = instructionSet[SENDALL]

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

// newForkID7EtrogInstructionSet returns the instruction set for the forkID7
func newForkID7EtrogInstructionSet() JumpTable {
	instructionSet := newForkID5DragonfruitInstructionSet()
	// TODO add the new instructions for forkID7

	return instructionSet
}
