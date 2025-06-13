package aa

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
)

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
// GAS (0x5A), unless followed immediately by a *CALL

// Banned behaviour
// Revert on "out of gas" is banned
// Access to an address without a deployed code is forbidden for EXTCODE and CALL, except for sender address
// CALL with value is forbidden

// Storage rules
// Access to the "account" storage is always allowed
// Access to associated storage of the account in an external contract is allowed if the account already exists.

// Authorization rules
// An account with EIP-7702 delegation can only be used as the sender of the transaction (not allowed as paymaster or deployer).
// An account with EIP-7702 delegation can only be accessed (using *CALL or EXTCODE* opcodes) if it is the sender of the transaction.

type ValidationRulesTracer struct {
	err error

	bannedOpcodes    map[vm.OpCode]bool
	prevWasGas       bool
	senderAddress    libcommon.Address
	accessedAccounts map[libcommon.Address]bool
	currentContract  libcommon.Address
	checkedAccounts  map[libcommon.Address]bool
	senderHasCode    bool
}

func NewValidationRulesTracer(sender libcommon.Address, senderHasCode bool) *ValidationRulesTracer {
	t := &ValidationRulesTracer{
		bannedOpcodes:    make(map[vm.OpCode]bool),
		senderAddress:    sender,
		senderHasCode:    senderHasCode,
		accessedAccounts: make(map[libcommon.Address]bool),
		checkedAccounts:  make(map[libcommon.Address]bool),
	}

	bannedOpcodes := []vm.OpCode{
		vm.ORIGIN,
		vm.GASPRICE,
		vm.BLOCKHASH,
		vm.COINBASE,
		vm.TIMESTAMP,
		vm.NUMBER,
		vm.DIFFICULTY,
		vm.GASLIMIT,
		vm.BASEFEE,
		vm.BLOBHASH,
		vm.BLOBBASEFEE,
		vm.INVALID,
		vm.SELFDESTRUCT,
		vm.BALANCE,
		vm.SELFBALANCE,
	}

	for _, op := range bannedOpcodes {
		t.bannedOpcodes[op] = true
	}

	return t
}

func (t *ValidationRulesTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnOpcode:        t.OnOpcode,
		OnEnter:         t.OnEnter,
		OnExit:          t.OnExit,
		OnFault:         t.OnFault,
		OnStorageChange: t.OnStorageChange,
	}
}

func (t *ValidationRulesTracer) isDelegatedAccount(code []byte) bool {
	// Check if code starts with "0xef0100"
	return len(code) >= 3 && bytes.Equal(code[:3], []byte{0xef, 0x01, 0x00})
}

func (t *ValidationRulesTracer) OnOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	if t.err != nil {
		return
	}

	opCode := vm.OpCode(op)

	if opCode == vm.GAS {
		t.prevWasGas = true
		return
	}

	if t.prevWasGas {
		// GAS must be followed immediately by a *CALL instruction
		if opCode != vm.CALL && opCode != vm.CALLCODE && opCode != vm.DELEGATECALL && opCode != vm.STATICCALL {
			t.err = errors.New("GAS opcode not followed by CALL instruction")
			return
		}
		t.prevWasGas = false
	}

	if opCode == vm.EXTCODESIZE || opCode == vm.EXTCODECOPY || opCode == vm.EXTCODEHASH {
		if len(scope.StackData()) > 0 {
			addr := libcommon.BytesToAddress(scope.StackData()[0].Bytes())
			if t.isDelegatedAccount(scope.Code()) && addr != t.senderAddress {
				t.err = fmt.Errorf("access to delegated account %s not allowed", addr.Hex())
				return
			}
			t.accessedAccounts[addr] = true
		}
	}

	if opCode == vm.CALL || opCode == vm.CALLCODE || opCode == vm.DELEGATECALL || opCode == vm.STATICCALL {
		if len(scope.StackData()) > 0 {
			addr := libcommon.BytesToAddress(scope.StackData()[0].Bytes())
			if t.isDelegatedAccount(scope.Code()) && addr != t.senderAddress {
				t.err = fmt.Errorf("access to delegated account %s not allowed", addr.Hex())
				return
			}
		}
	}

	if t.bannedOpcodes[opCode] {
		t.err = fmt.Errorf("banned opcode %s used", opCode.String())
	}
}

func (t *ValidationRulesTracer) OnEnter(depth int, typ byte, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.err != nil {
		return
	}

	if value != nil && !value.IsZero() {
		t.err = errors.New("CALL with value is forbidden")
		return
	}

	if t.isDelegatedAccount(code) && from != t.senderAddress {
		t.err = fmt.Errorf("delegated account %s can only be used as sender", from.Hex())
		return
	}

	t.currentContract = to
}

func (t *ValidationRulesTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if t.err != nil {
		return
	}

	t.prevWasGas = false
}

func (t *ValidationRulesTracer) OnFault(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, depth int, err error) {
	if t.err != nil {
		return
	}

	if err != nil && err.Error() == "out of gas" {
		t.err = errors.New("revert on 'out of gas' is banned")
	}
}

func (t *ValidationRulesTracer) isAssociatedStorage(slot libcommon.Hash, addr libcommon.Address) bool {
	// Case 1: The slot value is the address
	if bytes.Equal(slot.Bytes(), addr.Bytes()) {
		return true
	}

	// Case 2: The slot value was calculated as keccak(A||x)+n, we test the first 50 slots and 128 offsets
	buf := make([]byte, 52)
	copy(buf, addr.Bytes())

	hash := sha3.NewLegacyKeccak256()
	result := make([]byte, 32)

	for x := 0; x < 50; x++ {
		buf[51] = byte(x)

		hash.Reset()
		hash.Write(buf)
		result = hash.Sum(result[:0])

		for n := 0; n <= 128; n++ {
			result[31] += byte(n)
			if bytes.Equal(result, slot.Bytes()) {
				return true
			}
			result[31] -= byte(n)
		}
	}

	return false
}

func (t *ValidationRulesTracer) OnStorageChange(addr libcommon.Address, slot libcommon.Hash, prev, new uint256.Int) {
	if t.err != nil {
		return
	}

	if addr == t.senderAddress {
		return
	}

	if !t.senderHasCode {
		t.err = fmt.Errorf("access to storage of external contract %s not allowed - sender has no code", addr.Hex())
		return
	}

	if !t.isAssociatedStorage(slot, t.senderAddress) {
		t.err = fmt.Errorf("access to non-associated storage slot %s in account %s", slot.Hex(), addr.Hex())
		return
	}
}

func (t *ValidationRulesTracer) Err() error {
	return t.err
}
