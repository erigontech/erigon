package fakevm

// Ethereum Virtual Machine OpCode s
// https://ethervm.io/#opcodes

// OpCode is the EVM opcode
type OpCode byte

const (

	// STOP halts execution of the contract
	STOP = 0x00

	// ADD performs (u)int256 addition modulo 2**256
	ADD = 0x01

	// MUL performs (u)int256 multiplication modulo 2**256
	MUL = 0x02

	// SUB performs (u)int256 subtraction modulo 2**256
	SUB = 0x03

	// DIV performs uint256 division
	DIV = 0x04

	// SDIV performs int256 division
	SDIV = 0x05

	// MOD performs uint256 modulus
	MOD = 0x06

	// SMOD performs int256 modulus
	SMOD = 0x07

	// ADDMOD performs (u)int256 addition modulo N
	ADDMOD = 0x08

	// MULMOD performs (u)int256 multiplication modulo N
	MULMOD = 0x09

	// EXP performs uint256 exponentiation modulo 2**256
	EXP = 0x0A

	// SIGNEXTEND performs sign extends x from (b + 1) * 8 bits to 256 bits.
	SIGNEXTEND = 0x0B

	// LT performs int256 comparison
	LT = 0x10

	// GT performs int256 comparison
	GT = 0x11

	// SLT performs int256 comparison
	SLT = 0x12

	// SGT performs int256 comparison
	SGT = 0x13

	// EQ performs (u)int256 equality
	EQ = 0x14

	// ISZERO checks if (u)int256 is zero
	ISZERO = 0x15

	// AND performs 256-bit bitwise and
	AND = 0x16

	// OR performs 256-bit bitwise or
	OR = 0x17

	// XOR performs 256-bit bitwise xor
	XOR = 0x18

	// NOT performs 256-bit bitwise not
	NOT = 0x19

	// BYTE returns the ith byte of (u)int256 x counting from most significant byte
	BYTE = 0x1A

	// SHL performs a shift left
	SHL = 0x1B

	// SHR performs a logical shift right
	SHR = 0x1C

	// SAR performs an arithmetic shift right
	SAR = 0x1D

	// SHA3 performs the keccak256 hash function
	SHA3 = 0x20

	// ADDRESS returns the address of the executing contract
	ADDRESS = 0x30

	// BALANCE returns the address balance in wei
	BALANCE = 0x31

	// ORIGIN returns the transaction origin address
	ORIGIN = 0x32

	// CALLER returns the message caller address
	CALLER = 0x33

	// CALLVALUE returns the message funds in wei
	CALLVALUE = 0x34

	// CALLDATALOAD reads a (u)int256 from message data
	CALLDATALOAD = 0x35

	// CALLDATASIZE returns the message data length in bytes
	CALLDATASIZE = 0x36

	// CALLDATACOPY copies the message data
	CALLDATACOPY = 0x37

	// CODESIZE returns the length of the executing contract's code in bytes
	CODESIZE = 0x38

	// CODECOPY copies the executing contract bytecode
	CODECOPY = 0x39

	// GASPRICE returns the gas price of the executing transaction, in wei per unit of gas
	GASPRICE = 0x3A

	// EXTCODESIZE returns the length of the contract bytecode at addr
	EXTCODESIZE = 0x3B

	// EXTCODECOPY copies the contract bytecode
	EXTCODECOPY = 0x3C

	// RETURNDATASIZE returns the size of the returned data from the last external call in bytes
	RETURNDATASIZE = 0x3D

	// RETURNDATACOPY copies the returned data
	RETURNDATACOPY = 0x3E

	// EXTCODEHASH returns the hash of the specified contract bytecode
	EXTCODEHASH = 0x3F

	// BLOCKHASH returns the hash of the specific block. Only valid for the last 256 most recent blocks
	BLOCKHASH = 0x40

	// COINBASE returns the address of the current block's miner
	COINBASE = 0x41

	// TIMESTAMP returns the current block's Unix timestamp in seconds
	TIMESTAMP = 0x42

	// NUMBER returns the current block's number
	NUMBER = 0x43

	// DIFFICULTY returns the current block's difficulty
	DIFFICULTY = 0x44

	// GASLIMIT returns the current block's gas limit
	GASLIMIT = 0x45

	// CHAINID returns the id of the chain
	CHAINID = 0x46

	// SELFBALANCE returns the balance of the current account
	SELFBALANCE = 0x47

	// POP pops a (u)int256 off the stack and discards it
	POP = 0x50

	// MLOAD reads a (u)int256 from memory
	MLOAD = 0x51

	// MSTORE writes a (u)int256 to memory
	MSTORE = 0x52

	// MSTORE8 writes a uint8 to memory
	MSTORE8 = 0x53

	// SLOAD reads a (u)int256 from storage
	SLOAD = 0x54

	// SSTORE writes a (u)int256 to storage
	SSTORE = 0x55

	// JUMP performs an unconditional jump
	JUMP = 0x56

	// JUMPI performs a conditional jump if condition is truthy
	JUMPI = 0x57

	// PC returns the program counter
	PC = 0x58

	// MSIZE returns the size of memory for this contract execution, in bytes
	MSIZE = 0x59

	// GAS returns the remaining gas
	GAS = 0x5A

	// JUMPDEST corresponds to a possible jump destination
	JUMPDEST = 0x5B

	// PUSH1 pushes a 1-byte value onto the stack
	PUSH1 = 0x60

	// PUSH2 pushes a 2-bytes value onto the stack
	PUSH2 = 0x61

	// PUSH3 pushes a 3-bytes value onto the stack
	PUSH3 = 0x62

	// PUSH4 pushes a 4-bytes value onto the stack
	PUSH4 = 0x63

	// PUSH5 pushes a 5-bytes value onto the stack
	PUSH5 = 0x64

	// PUSH6 pushes a 6-bytes value onto the stack
	PUSH6 = 0x65

	// PUSH7 pushes a 7-bytes value onto the stack
	PUSH7 = 0x66

	// PUSH8 pushes a 8-bytes value onto the stack
	PUSH8 = 0x67

	// PUSH9 pushes a 9-bytes value onto the stack
	PUSH9 = 0x68

	// PUSH10 pushes a 10-bytes value onto the stack
	PUSH10 = 0x69

	// PUSH11 pushes a 11-bytes value onto the stack
	PUSH11 = 0x6A

	// PUSH12 pushes a 12-bytes value onto the stack
	PUSH12 = 0x6B

	// PUSH13 pushes a 13-bytes value onto the stack
	PUSH13 = 0x6C

	// PUSH14 pushes a 14-bytes value onto the stack
	PUSH14 = 0x6D

	// PUSH15 pushes a 15-bytes value onto the stack
	PUSH15 = 0x6E

	// PUSH16 pushes a 16-bytes value onto the stack
	PUSH16 = 0x6F

	// PUSH17 pushes a 17-bytes value onto the stack
	PUSH17 = 0x70

	// PUSH18 pushes a 18-bytes value onto the stack
	PUSH18 = 0x71

	// PUSH19 pushes a 19-bytes value onto the stack
	PUSH19 = 0x72

	// PUSH20 pushes a 20-bytes value onto the stack
	PUSH20 = 0x73

	// PUSH21 pushes a 21-bytes value onto the stack
	PUSH21 = 0x74

	// PUSH22 pushes a 22-bytes value onto the stack
	PUSH22 = 0x75

	// PUSH23 pushes a 23-bytes value onto the stack
	PUSH23 = 0x76

	// PUSH24 pushes a 24-bytes value onto the stack
	PUSH24 = 0x77

	// PUSH25 pushes a 25-bytes value onto the stack
	PUSH25 = 0x78

	// PUSH26 pushes a 26-bytes value onto the stack
	PUSH26 = 0x79

	// PUSH27 pushes a 27-bytes value onto the stack
	PUSH27 = 0x7A

	// PUSH28 pushes a 28-bytes value onto the stack
	PUSH28 = 0x7B

	// PUSH29 pushes a 29-bytes value onto the stack
	PUSH29 = 0x7C

	// PUSH30 pushes a 30-bytes value onto the stack
	PUSH30 = 0x7D

	// PUSH31 pushes a 31-bytes value onto the stack
	PUSH31 = 0x7E

	// PUSH32 pushes a 32-byte value onto the stack
	PUSH32 = 0x7F

	// DUP1 clones the last value on the stack
	DUP1 = 0x80

	// DUP2 clones the 2nd last value on the stack
	DUP2 = 0x81

	// DUP3 clones the 3rd last value on the stack
	DUP3 = 0x82

	// DUP4 clones the 4th last value on the stack
	DUP4 = 0x83

	// DUP5 clones the 5th last value on the stack
	DUP5 = 0x84

	// DUP6 clones the 6th last value on the stack
	DUP6 = 0x85

	// DUP7 clones the 7th last value on the stack
	DUP7 = 0x86

	// DUP8 clones the 8th last value on the stack
	DUP8 = 0x87

	// DUP9 clones the 9th last value on the stack
	DUP9 = 0x88

	// DUP10 clones the 10th last value on the stack
	DUP10 = 0x89

	// DUP11 clones the 11th last value on the stack
	DUP11 = 0x8A

	// DUP12 clones the 12th last value on the stack
	DUP12 = 0x8B

	// DUP13 clones the 13th last value on the stack
	DUP13 = 0x8C

	// DUP14 clones the 14th last value on the stack
	DUP14 = 0x8D

	// DUP15 clones the 15th last value on the stack
	DUP15 = 0x8E

	// DUP16 clones the 16th last value on the stack
	DUP16 = 0x8F

	// SWAP1 swaps the last two values on the stack
	SWAP1 = 0x90

	// SWAP2 swaps the top of the stack with the 3rd last element
	SWAP2 = 0x91

	// SWAP3 swaps the top of the stack with the 4th last element
	SWAP3 = 0x92

	// SWAP4 swaps the top of the stack with the 5th last element
	SWAP4 = 0x93

	// SWAP5 swaps the top of the stack with the 6th last element
	SWAP5 = 0x94

	// SWAP6 swaps the top of the stack with the 7th last element
	SWAP6 = 0x95

	// SWAP7 swaps the top of the stack with the 8th last element
	SWAP7 = 0x96

	// SWAP8 swaps the top of the stack with the 9th last element
	SWAP8 = 0x97

	// SWAP9 swaps the top of the stack with the 10th last element
	SWAP9 = 0x98

	// SWAP10 swaps the top of the stack with the 11th last element
	SWAP10 = 0x99

	// SWAP11 swaps the top of the stack with the 12th last element
	SWAP11 = 0x9A

	// SWAP12 swaps the top of the stack with the 13th last element
	SWAP12 = 0x9B

	// SWAP13 swaps the top of the stack with the 14th last element
	SWAP13 = 0x9C

	// SWAP14 swaps the top of the stack with the 15th last element
	SWAP14 = 0x9D

	// SWAP15 swaps the top of the stack with the 16th last element
	SWAP15 = 0x9E

	// SWAP16 swaps the top of the stack with the 17th last element
	SWAP16 = 0x9F

	// LOG0 fires an event without topics
	LOG0 = 0xA0

	// LOG1 fires an event with one topic
	LOG1 = 0xA1

	// LOG2 fires an event with two topics
	LOG2 = 0xA2

	// LOG3 fires an event with three topics
	LOG3 = 0xA3

	// LOG4 fires an event with four topics
	LOG4 = 0xA4

	// CREATE creates a child contract
	CREATE = 0xF0

	// CALL calls a method in another contract
	CALL = 0xF1

	// CALLCODE calls a method in another contract
	CALLCODE = 0xF2

	// RETURN returns from this contract call
	RETURN = 0xF3

	// DELEGATECALL calls a method in another contract using the storage of the current contract
	DELEGATECALL = 0xF4

	// CREATE2 creates a child contract with a salt
	CREATE2 = 0xF5

	// STATICCALL calls a method in another contract
	STATICCALL = 0xFA

	// REVERT reverts with return data
	REVERT = 0xFD

	// SELFDESTRUCT destroys the contract and sends all funds to addr
	SELFDESTRUCT = 0xFF
)

var opCodeToString = map[OpCode]string{
	STOP:           "STOP",
	ADD:            "ADD",
	MUL:            "MUL",
	SUB:            "SUB",
	DIV:            "DIV",
	SDIV:           "SDIV",
	MOD:            "MOD",
	SMOD:           "SMOD",
	EXP:            "EXP",
	NOT:            "NOT",
	LT:             "LT",
	GT:             "GT",
	SLT:            "SLT",
	SGT:            "SGT",
	EQ:             "EQ",
	ISZERO:         "ISZERO",
	SIGNEXTEND:     "SIGNEXTEND",
	AND:            "AND",
	OR:             "OR",
	XOR:            "XOR",
	BYTE:           "BYTE",
	SHL:            "SHL",
	SHR:            "SHR",
	SAR:            "SAR",
	ADDMOD:         "ADDMOD",
	MULMOD:         "MULMOD",
	SHA3:           "SHA3",
	ADDRESS:        "ADDRESS",
	BALANCE:        "BALANCE",
	ORIGIN:         "ORIGIN",
	CALLER:         "CALLER",
	CALLVALUE:      "CALLVALUE",
	CALLDATALOAD:   "CALLDATALOAD",
	CALLDATASIZE:   "CALLDATASIZE",
	CALLDATACOPY:   "CALLDATACOPY",
	CODESIZE:       "CODESIZE",
	CODECOPY:       "CODECOPY",
	GASPRICE:       "GASPRICE",
	EXTCODESIZE:    "EXTCODESIZE",
	EXTCODECOPY:    "EXTCODECOPY",
	RETURNDATASIZE: "RETURNDATASIZE",
	RETURNDATACOPY: "RETURNDATACOPY",
	EXTCODEHASH:    "EXTCODEHASH",
	BLOCKHASH:      "BLOCKHASH",
	COINBASE:       "COINBASE",
	TIMESTAMP:      "TIMESTAMP",
	NUMBER:         "NUMBER",
	DIFFICULTY:     "DIFFICULTY",
	GASLIMIT:       "GASLIMIT",
	POP:            "POP",
	MLOAD:          "MLOAD",
	MSTORE:         "MSTORE",
	MSTORE8:        "MSTORE8",
	SLOAD:          "SLOAD",
	SSTORE:         "SSTORE",
	JUMP:           "JUMP",
	JUMPI:          "JUMPI",
	PC:             "PC",
	MSIZE:          "MSIZE",
	GAS:            "GAS",
	JUMPDEST:       "JUMPDEST",
	PUSH1:          "PUSH1",
	PUSH2:          "PUSH2",
	PUSH3:          "PUSH3",
	PUSH4:          "PUSH4",
	PUSH5:          "PUSH5",
	PUSH6:          "PUSH6",
	PUSH7:          "PUSH7",
	PUSH8:          "PUSH8",
	PUSH9:          "PUSH9",
	PUSH10:         "PUSH10",
	PUSH11:         "PUSH11",
	PUSH12:         "PUSH12",
	PUSH13:         "PUSH13",
	PUSH14:         "PUSH14",
	PUSH15:         "PUSH15",
	PUSH16:         "PUSH16",
	PUSH17:         "PUSH17",
	PUSH18:         "PUSH18",
	PUSH19:         "PUSH19",
	PUSH20:         "PUSH20",
	PUSH21:         "PUSH21",
	PUSH22:         "PUSH22",
	PUSH23:         "PUSH23",
	PUSH24:         "PUSH24",
	PUSH25:         "PUSH25",
	PUSH26:         "PUSH26",
	PUSH27:         "PUSH27",
	PUSH28:         "PUSH28",
	PUSH29:         "PUSH29",
	PUSH30:         "PUSH30",
	PUSH31:         "PUSH31",
	PUSH32:         "PUSH32",
	DUP1:           "DUP1",
	DUP2:           "DUP2",
	DUP3:           "DUP3",
	DUP4:           "DUP4",
	DUP5:           "DUP5",
	DUP6:           "DUP6",
	DUP7:           "DUP7",
	DUP8:           "DUP8",
	DUP9:           "DUP9",
	DUP10:          "DUP10",
	DUP11:          "DUP11",
	DUP12:          "DUP12",
	DUP13:          "DUP13",
	DUP14:          "DUP14",
	DUP15:          "DUP15",
	DUP16:          "DUP16",
	SWAP1:          "SWAP1",
	SWAP2:          "SWAP2",
	SWAP3:          "SWAP3",
	SWAP4:          "SWAP4",
	SWAP5:          "SWAP5",
	SWAP6:          "SWAP6",
	SWAP7:          "SWAP7",
	SWAP8:          "SWAP8",
	SWAP9:          "SWAP9",
	SWAP10:         "SWAP10",
	SWAP11:         "SWAP11",
	SWAP12:         "SWAP12",
	SWAP13:         "SWAP13",
	SWAP14:         "SWAP14",
	SWAP15:         "SWAP15",
	SWAP16:         "SWAP16",
	LOG0:           "LOG0",
	LOG1:           "LOG1",
	LOG2:           "LOG2",
	LOG3:           "LOG3",
	LOG4:           "LOG4",
	CREATE:         "CREATE",
	CALL:           "CALL",
	RETURN:         "RETURN",
	CALLCODE:       "CALLCODE",
	DELEGATECALL:   "DELEGATECALL",
	CREATE2:        "CREATE2",
	STATICCALL:     "STATICCALL",
	REVERT:         "REVERT",
	SELFDESTRUCT:   "SELFDESTRUCT",
	CHAINID:        "CHAINID",
	SELFBALANCE:    "SELFBALANCE",
}

func (op OpCode) String() string {
	return opCodeToString[op]
}
