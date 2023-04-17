package fakevm

import "github.com/ledgerwatch/erigon/core/vm"

// Config are the configuration options for the Interpreter
type Config struct {
	Debug                   bool      // Enables debugging
	Tracer                  EVMLogger // Opcode logger
	NoBaseFee               bool      // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	EnablePreimageRecording bool      // Enables recording of SHA3/keccak preimages

	JumpTable *vm.JumpTable // EVM instruction table, automatically populated if unset

	ExtraEips []int // Additional EIPS that are to be enabled
}
