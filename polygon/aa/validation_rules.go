package aa

// Implements tracing required for ERC-7562 validation. The rules are as follows:

// Banned Opcodes
// ORIGIN (0x32)
// GASPRICE (0x3A)
// BLOCKHASH (0x40)
// COINBASE (0x41)
// TIMESTAMP (0x42)
// NUMBER (0x43)
// PREVRANDAO/DIFFICULTY (0x44)
// GASLIMIT (0x45)
// BASEFEE (0x48)
// BLOBHASH (0x49)
// BLOBBASEFEE (0x4A)
// INVALID (0xFE)
// SELFDESTRUCT (0xFF)
// BALANCE (0x31)
// SELFBALANCE (0x47)
// GAS (0x5A), unless followed immediately by a CALL

// Banned behaviour
// Revert on “out of gas” is banned
// Access to an address without a deployed code is forbidden for EXTCODE and CALL, except for sender address
// CALL with value is forbidden

// Storage rules
// Access to the “account” storage is always allowed
// Access to associated storage of the account in an external contract is allowed if the account already exists.

type ValidationRulesTracer struct{}
