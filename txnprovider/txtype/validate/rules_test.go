// Copyright 2026 The Erigon Authors
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

package validate_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/txnprovider/txtype/validate"
)

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// ops builds a bytecode slice from a list of opcode values.
// Use this only for opcodes that have no immediate operands.
func ops(opcodes ...vm.OpCode) []byte {
	b := make([]byte, len(opcodes))
	for i, op := range opcodes {
		b[i] = byte(op)
	}
	return b
}

// --------------------------------------------------------------------------
// BaseRules — environment opcode ban
// --------------------------------------------------------------------------

// TestBaseRulesCleanCode verifies that code with no banned opcodes passes.
func TestBaseRulesCleanCode(t *testing.T) {
	// A simple function that loads calldata and returns it.
	// CALLDATASIZE CALLDATALOAD PUSH1 0x00 MSTORE PUSH1 0x20 PUSH1 0x00 RETURN
	code := []byte{
		byte(vm.CALLDATASIZE),
		byte(vm.CALLDATALOAD),
		byte(vm.PUSH1), 0x00,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x20,
		byte(vm.PUSH1), 0x00,
		byte(vm.RETURN),
	}
	require.Nil(t, validate.BaseRules.Validate(code))
}

// TestBaseRulesEmptyCode verifies that empty bytecode passes (no instructions
// to check).
func TestBaseRulesEmptyCode(t *testing.T) {
	require.Nil(t, validate.BaseRules.Validate(nil))
	require.Nil(t, validate.BaseRules.Validate([]byte{}))
}

// TestBaseRulesBannedOpcodes tests each environment-dependent opcode that is
// banned under both ERC-7562 and EIP-8141.
//
// Ref: ERC-7562 §3 [OP-011]–[OP-014]; EIP-8141 §4.1 (draft)
func TestBaseRulesBannedOpcodes(t *testing.T) {
	cases := []struct {
		name string
		op   vm.OpCode
		ref  string
	}{
		// Block environment opcodes
		{"BLOCKHASH", vm.BLOCKHASH, "ERC-7562 [OP-012]"},
		{"COINBASE", vm.COINBASE, "ERC-7562 [OP-011]"},
		{"TIMESTAMP", vm.TIMESTAMP, "ERC-7562 [OP-011]"},
		{"NUMBER", vm.NUMBER, "ERC-7562 [OP-011]"},
		{"DIFFICULTY/PREVRANDAO", vm.DIFFICULTY, "ERC-7562 [OP-013]"},
		{"GASLIMIT", vm.GASLIMIT, "ERC-7562 [OP-011]"},
		{"BASEFEE", vm.BASEFEE, "ERC-7562 [OP-014]"},
		{"BLOBHASH", vm.BLOBHASH, "ERC-7562 [OP-014]"},
		{"BLOBBASEFEE", vm.BLOBBASEFEE, "ERC-7562 [OP-014]"},
		// Transaction environment opcodes
		{"ORIGIN", vm.ORIGIN, "ERC-7562 [OP-011]"},
		{"GASPRICE", vm.GASPRICE, "ERC-7562 [OP-011]"},
		// Dangerous state-modifying opcodes
		{"SELFDESTRUCT", vm.SELFDESTRUCT, "ERC-7562 [OP-061]"},
		{"INVALID", vm.INVALID, "ERC-7562 [OP-061]"},
		// Balance probing
		{"SELFBALANCE", vm.SELFBALANCE, "ERC-7562 [OP-011]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code := ops(tc.op)
			v := validate.BaseRules.Validate(code)
			require.NotNil(t, v, "%s (%s): expected violation", tc.name, tc.ref)
			require.Equal(t, validate.ViolBannedOpcode, v.Kind)
			require.Equal(t, tc.op, v.Op)
			require.Equal(t, uint64(0), v.PC)
		})
	}
}

// TestBaseRulesBannedOpcodePC verifies that the reported PC is correct when
// the banned opcode is not at position 0.
func TestBaseRulesBannedOpcodePC(t *testing.T) {
	// STOP(0) STOP(1) COINBASE(2)
	code := ops(vm.STOP, vm.STOP, vm.COINBASE)
	v := validate.BaseRules.Validate(code)
	require.NotNil(t, v)
	require.Equal(t, uint64(2), v.PC, "PC must point to the offending opcode")
	require.Equal(t, vm.COINBASE, v.Op)
}

// TestBaseRulesFirstViolationReturned verifies that only the first violation is
// returned when multiple banned opcodes are present (fail-fast semantics).
func TestBaseRulesFirstViolationReturned(t *testing.T) {
	// ORIGIN(0) COINBASE(1) — both banned; only ORIGIN should be reported
	code := ops(vm.ORIGIN, vm.COINBASE)
	v := validate.BaseRules.Validate(code)
	require.NotNil(t, v)
	require.Equal(t, vm.ORIGIN, v.Op, "first violation must be reported")
}

// --------------------------------------------------------------------------
// ERC7562Rules — BALANCE and GAS-before-CALL
// --------------------------------------------------------------------------

// TestERC7562BalanceBanned verifies that BALANCE is rejected.
//
// Ref: ERC-7562 §3 [OP-011] "may not use BALANCE opcode"
func TestERC7562BalanceBanned(t *testing.T) {
	code := ops(vm.BALANCE)
	v := validate.ERC7562Rules.Validate(code)
	require.NotNil(t, v, "ERC-7562 [OP-011]: BALANCE must be rejected")
	require.Equal(t, validate.ViolBannedOpcode, v.Kind)
	require.Equal(t, vm.BALANCE, v.Op)
}

// TestERC7562GasFollowedByCall verifies that GAS immediately followed by each
// *CALL variant is permitted.
//
// Ref: ERC-7562 §3 [OP-031] "GAS opcode is allowed only if followed by a *CALL"
func TestERC7562GasFollowedByCall(t *testing.T) {
	callOps := []struct {
		name string
		op   vm.OpCode
	}{
		{"CALL", vm.CALL},
		{"CALLCODE", vm.CALLCODE},
		{"DELEGATECALL", vm.DELEGATECALL},
		{"STATICCALL", vm.STATICCALL},
	}
	for _, tc := range callOps {
		t.Run(tc.name, func(t *testing.T) {
			// GAS followed immediately by a *CALL — must be allowed.
			// Ref: ERC-7562 [OP-031]
			code := ops(vm.GAS, tc.op)
			v := validate.ERC7562Rules.Validate(code)
			require.Nil(t, v, "ERC-7562 [OP-031]: GAS+%s must be permitted", tc.name)
		})
	}
}

// TestERC7562GasNotFollowedByCall verifies that GAS not followed by a *CALL
// is rejected.
//
// Ref: ERC-7562 §3 [OP-031]
func TestERC7562GasNotFollowedByCall(t *testing.T) {
	notCallOps := []struct {
		name string
		op   vm.OpCode
	}{
		{"STOP", vm.STOP},
		{"ADD", vm.ADD},
		{"MSTORE", vm.MSTORE},
		{"RETURN", vm.RETURN},
		{"SSTORE", vm.SSTORE},
	}
	for _, tc := range notCallOps {
		t.Run("GAS+"+tc.name, func(t *testing.T) {
			code := ops(vm.GAS, tc.op)
			v := validate.ERC7562Rules.Validate(code)
			require.NotNil(t, v, "ERC-7562 [OP-031]: GAS+%s must be rejected", tc.name)
			require.Equal(t, validate.ViolGasNotFollowedByCall, v.Kind)
			require.Equal(t, vm.GAS, v.Op)
		})
	}
}

// TestERC7562GasAtEndOfBytecode verifies that GAS with no following instruction
// is rejected.
//
// Ref: ERC-7562 §3 [OP-031]
func TestERC7562GasAtEndOfBytecode(t *testing.T) {
	code := ops(vm.GAS)
	v := validate.ERC7562Rules.Validate(code)
	require.NotNil(t, v, "ERC-7562 [OP-031]: GAS at end of bytecode must be rejected")
	require.Equal(t, validate.ViolGasNotFollowedByCall, v.Kind)
}

// TestERC7562GasEmbeddedInPushOperand verifies that a GAS byte (0x5a) inside a
// PUSH operand does not trigger the GAS rule.
//
// This is the static-analysis analogue of the scanner regression test: the rule
// must only fire on decoded instructions, not on raw bytes.
func TestERC7562GasEmbeddedInPushOperand(t *testing.T) {
	// PUSH1 <GAS byte> STOP
	code := []byte{byte(vm.PUSH1), byte(vm.GAS), byte(vm.STOP)}
	v := validate.ERC7562Rules.Validate(code)
	require.Nil(t, v, "ERC-7562: GAS byte inside PUSH1 operand must not trigger the GAS rule")
}

// TestERC7562InheritsBaseRules verifies that ERC7562Rules still rejects
// base-banned opcodes (i.e. Extend did not lose the base rules).
func TestERC7562InheritsBaseRules(t *testing.T) {
	// ORIGIN is banned by BaseRules, which ERC7562Rules extends.
	code := ops(vm.ORIGIN)
	v := validate.ERC7562Rules.Validate(code)
	require.NotNil(t, v, "ERC7562Rules must inherit BaseRules bans")
	require.Equal(t, vm.ORIGIN, v.Op)
}

// TestERC7562SstoreAllowed verifies that SSTORE is NOT banned by ERC7562Rules
// (it is only banned in EIP-8141 VERIFY frames).
func TestERC7562SstoreAllowed(t *testing.T) {
	code := ops(vm.SSTORE)
	v := validate.ERC7562Rules.Validate(code)
	require.Nil(t, v, "ERC7562Rules must not ban SSTORE (only EIP8141VerifyRules does)")
}

// --------------------------------------------------------------------------
// EIP8141VerifyRules — SSTORE ban
// --------------------------------------------------------------------------

// TestEIP8141SstoreBanned verifies that SSTORE is rejected in a VERIFY frame.
//
// Ref: EIP-8141 §4.1 (draft) "VERIFY frames must be read-only"
func TestEIP8141SstoreBanned(t *testing.T) {
	code := ops(vm.SSTORE)
	v := validate.EIP8141VerifyRules.Validate(code)
	require.NotNil(t, v, "EIP-8141 §4.1: SSTORE must be rejected in VERIFY frame")
	require.Equal(t, validate.ViolSstoreInVerify, v.Kind)
	require.Equal(t, vm.SSTORE, v.Op)
}

// TestEIP8141SstorePC verifies the correct PC is reported for SSTORE.
func TestEIP8141SstorePC(t *testing.T) {
	// MLOAD(0) MLOAD(1) SSTORE(2)
	code := ops(vm.MLOAD, vm.MLOAD, vm.SSTORE)
	v := validate.EIP8141VerifyRules.Validate(code)
	require.NotNil(t, v)
	require.Equal(t, uint64(2), v.PC)
}

// TestEIP8141InheritsBaseRules verifies that EIP8141VerifyRules still rejects
// base-banned opcodes.
//
// Ref: EIP-8141 §4.1 (draft)
func TestEIP8141InheritsBaseRules(t *testing.T) {
	code := ops(vm.TIMESTAMP)
	v := validate.EIP8141VerifyRules.Validate(code)
	require.NotNil(t, v, "EIP8141VerifyRules must inherit BaseRules bans")
	require.Equal(t, vm.TIMESTAMP, v.Op)
}

// TestEIP8141GasAllowed verifies that GAS without a following *CALL is NOT
// banned by EIP8141VerifyRules — the GAS rule is ERC-7562-specific.
//
// EIP-8141 does not carry over the GAS-before-CALL restriction from ERC-7562.
func TestEIP8141GasAllowed(t *testing.T) {
	code := ops(vm.GAS, vm.STOP)
	v := validate.EIP8141VerifyRules.Validate(code)
	require.Nil(t, v, "EIP8141VerifyRules must not apply the ERC-7562 GAS-before-CALL rule")
}

// TestEIP8141SstoreInPushOperand verifies that an SSTORE byte inside a PUSH
// operand does not trigger the SSTORE rule.
func TestEIP8141SstoreInPushOperand(t *testing.T) {
	// PUSH1 <SSTORE byte> STOP
	code := []byte{byte(vm.PUSH1), byte(vm.SSTORE), byte(vm.STOP)}
	v := validate.EIP8141VerifyRules.Validate(code)
	require.Nil(t, v, "EIP-8141: SSTORE byte inside PUSH1 operand must not trigger the SSTORE rule")
}

// --------------------------------------------------------------------------
// RuleSet composition
// --------------------------------------------------------------------------

// TestRuleSetExtendDoesNotMutateBase verifies that Extend creates a new
// RuleSet and does not modify the original.
func TestRuleSetExtendDoesNotMutateBase(t *testing.T) {
	originalLen := len(validate.BaseRules)
	_ = validate.BaseRules.Extend(func(insns []validate.Insn, idx int) (validate.Violation, bool) {
		return validate.Violation{}, false
	})
	require.Equal(t, originalLen, len(validate.BaseRules), "Extend must not mutate the original RuleSet")
}

// TestRuleSetValidateCleanCode verifies that genuinely clean code passes all
// rule sets.
func TestRuleSetValidateCleanCode(t *testing.T) {
	// Pure arithmetic: PUSH1 5 PUSH1 3 ADD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		byte(vm.PUSH1), 0x05,
		byte(vm.PUSH1), 0x03,
		byte(vm.ADD),
		byte(vm.PUSH1), 0x00,
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x20,
		byte(vm.PUSH1), 0x00,
		byte(vm.RETURN),
	}
	require.Nil(t, validate.BaseRules.Validate(code), "BaseRules: clean code must pass")
	require.Nil(t, validate.ERC7562Rules.Validate(code), "ERC7562Rules: clean code must pass")
	require.Nil(t, validate.EIP8141VerifyRules.Validate(code), "EIP8141VerifyRules: clean code must pass")
}
