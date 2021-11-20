package minievm

// 0x0 range - arithmetic ops.
const (
	STOP byte = iota
	ADD
	MUL
	SUB
	DIV
	SDIV
	MOD
	SMOD
	ADDMOD
	MULMOD
	EXP
	SIGNEXTEND
)

// 0x10 range - comparison ops.
const (
	LT byte = iota + 0x10
	GT
	SLT
	SGT
	EQ
	ISZERO
	AND
	OR
	XOR
	NOT
	BYTE
	SHL
	SHR
	SAR

	SHA3 byte = 0x20
)

// 0x30 range - closure state.
const (
	ADDRESS byte = 0x30 + iota
	BALANCE
	ORIGIN
	CALLER
	CALLVALUE
	CALLDATALOAD
	CALLDATASIZE
	CALLDATACOPY
	CODESIZE
	CODECOPY
	GASPRICE
	EXTCODESIZE
	EXTCODECOPY
	RETURNDATASIZE
	RETURNDATACOPY
	EXTCODEHASH
)

// 0x40 range - block operations.
const (
	BLOCKHASH byte = 0x40 + iota
	COINBASE
	TIMESTAMP
	NUMBER
	DIFFICULTY
	GASLIMIT
	CHAINID     byte = 0x46
	SELFBALANCE byte = 0x47
	BASEFEE     byte = 0x48
)

// 0x50 range - 'storage' and execution.
const (
	POP      byte = 0x50
	MLOAD    byte = 0x51
	MSTORE   byte = 0x52
	MSTORE8  byte = 0x53
	SLOAD    byte = 0x54
	SSTORE   byte = 0x55
	JUMP     byte = 0x56
	JUMPI    byte = 0x57
	PC       byte = 0x58
	MSIZE    byte = 0x59
	GAS      byte = 0x5a
	JUMPDEST byte = 0x5b
)

// 0x60 range.
const (
	PUSH1 byte = 0x60 + iota
	PUSH2
	PUSH3
	PUSH4
	PUSH5
	PUSH6
	PUSH7
	PUSH8
	PUSH9
	PUSH10
	PUSH11
	PUSH12
	PUSH13
	PUSH14
	PUSH15
	PUSH16
	PUSH17
	PUSH18
	PUSH19
	PUSH20
	PUSH21
	PUSH22
	PUSH23
	PUSH24
	PUSH25
	PUSH26
	PUSH27
	PUSH28
	PUSH29
	PUSH30
	PUSH31
	PUSH32
	DUP1
	DUP2
	DUP3
	DUP4
	DUP5
	DUP6
	DUP7
	DUP8
	DUP9
	DUP10
	DUP11
	DUP12
	DUP13
	DUP14
	DUP15
	DUP16
	SWAP1
	SWAP2
	SWAP3
	SWAP4
	SWAP5
	SWAP6
	SWAP7
	SWAP8
	SWAP9
	SWAP10
	SWAP11
	SWAP12
	SWAP13
	SWAP14
	SWAP15
	SWAP16
)

// 0xa0 range - logging ops.
const (
	LOG0 byte = 0xa0 + iota
	LOG1
	LOG2
	LOG3
	LOG4
)

// unofficial bytes used for parsing.
const (
	PUSH byte = 0xb0 + iota
	DUP
	SWAP
)

// 0xf0 range - closures.
const (
	CREATE byte = 0xf0 + iota
	CALL
	CALLCODE
	RETURN
	DELEGATECALL
	CREATE2
	STATICCALL   byte = 0xfa
	REVERT       byte = 0xfd
	INVALID      byte = 0xfe
	SELFDESTRUCT byte = 0xff
)
