package vm

var (
	zkevmForkID4InstructionSet = newZkEVM_forkID4InstructionSet()
	zkevmForkID5InstructionSet = newZkEVM_forkID5InstructionSet()
)

// newZkEVM_forkID4InstructionSet returns the instruction set for the forkID4
func newZkEVM_forkID4InstructionSet() JumpTable {
	instructionSet := newBerlinInstructionSet()

	enable2929_zkevm(&instructionSet) // Access lists for trie accesses https://eips.ethereum.org/EIPS/eip-2929

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

// newZkEVM_forkID5InstructionSet returns the instruction set for the forkID5
func newZkEVM_forkID5InstructionSet() JumpTable {
	instructionSet := newZkEVM_forkID4InstructionSet()

	instructionSet[CALLDATACOPY].execute = opCallDataCopy
	instructionSet[CALLDATALOAD].execute = opCallDataLoad
	instructionSet[STATICCALL].execute = opStaticCall

	enable3855(&instructionSet) // EIP-3855: Enable PUSH0 opcode

	validateAndFillMaxStack(&instructionSet)
	return instructionSet
}
