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

// TestScanEmpty verifies that nil and zero-length input yield no instructions.
func TestScanEmpty(t *testing.T) {
	require.Nil(t, validate.Scan(nil))
	require.Nil(t, validate.Scan([]byte{}))
}

// TestScanNoPush verifies that a sequence of non-PUSH opcodes is decoded
// one-for-one with correct PC values.
func TestScanNoPush(t *testing.T) {
	// STOP (0x00), ADD (0x01), RETURN (0xf3)
	code := []byte{byte(vm.STOP), byte(vm.ADD), byte(vm.RETURN)}
	insns := validate.Scan(code)
	require.Len(t, insns, 3)
	require.Equal(t, uint64(0), insns[0].PC)
	require.Equal(t, vm.STOP, insns[0].Opcode)
	require.Equal(t, uint64(1), insns[1].PC)
	require.Equal(t, vm.ADD, insns[1].Opcode)
	require.Equal(t, uint64(2), insns[2].PC)
	require.Equal(t, vm.RETURN, insns[2].Opcode)
}

// TestScanPush1SkipsOperand verifies that the single byte following PUSH1 is
// consumed as an operand and not emitted as an instruction.
//
// EVM spec: PUSH1 (0x60) pushes a 1-byte immediate value onto the stack.
// The operand must not be decoded as a separate opcode.
func TestScanPush1SkipsOperand(t *testing.T) {
	// PUSH1 0x60 (operand = 0x60, which is also the PUSH1 opcode byte)
	// STOP
	code := []byte{byte(vm.PUSH1), 0x60, byte(vm.STOP)}
	insns := validate.Scan(code)

	require.Len(t, insns, 2, "operand byte must not appear as an instruction")
	require.Equal(t, vm.PUSH1, insns[0].Opcode)
	require.Equal(t, uint64(0), insns[0].PC)
	require.Equal(t, vm.STOP, insns[1].Opcode)
	require.Equal(t, uint64(2), insns[1].PC, "STOP must appear at pc=2, after PUSH1+operand")
}

// TestScanPush32SkipsAllOperands verifies that PUSH32 consumes exactly 32
// operand bytes before the next instruction.
//
// EVM spec: PUSH32 (0x7f) pushes a 32-byte immediate value.
func TestScanPush32SkipsAllOperands(t *testing.T) {
	code := make([]byte, 0, 34)
	code = append(code, byte(vm.PUSH32))
	// 32 operand bytes — use 0xff which would be SELFDESTRUCT if decoded as opcode
	for i := 0; i < 32; i++ {
		code = append(code, 0xff)
	}
	code = append(code, byte(vm.STOP))

	insns := validate.Scan(code)
	require.Len(t, insns, 2, "32 operand bytes must not appear as instructions")
	require.Equal(t, vm.PUSH32, insns[0].Opcode)
	require.Equal(t, uint64(0), insns[0].PC)
	require.Equal(t, vm.STOP, insns[1].Opcode)
	require.Equal(t, uint64(33), insns[1].PC, "STOP must appear at pc=33")
}

// TestScanPushAllSizes verifies that every PUSH variant (PUSH1–PUSH32) skips
// exactly the right number of operand bytes.
//
// EVM spec: PUSHn opcode byte = 0x60 + (n-1), operand = n bytes.
func TestScanPushAllSizes(t *testing.T) {
	for n := 1; n <= 32; n++ {
		opcode := vm.OpCode(byte(vm.PUSH1) + byte(n-1))
		// Build: PUSHn | n operand bytes (0x00) | STOP
		code := make([]byte, 1+n+1)
		code[0] = byte(opcode)
		// operand bytes are 0x00 (STOP), which would be misidentified if not skipped
		code[1+n] = byte(vm.STOP) // trailing STOP at correct position

		insns := validate.Scan(code)
		require.Len(t, insns, 2, "PUSH%d: operand bytes must not be decoded as instructions", n)
		require.Equal(t, opcode, insns[0].Opcode, "PUSH%d: first instruction must be the PUSH opcode", n)
		require.Equal(t, uint64(0), insns[0].PC, "PUSH%d: PUSH must be at pc=0", n)
		require.Equal(t, vm.STOP, insns[1].Opcode, "PUSH%d: second instruction must be STOP", n)
		require.Equal(t, uint64(1+n), insns[1].PC, "PUSH%d: STOP must be at pc=%d", n, 1+n)
	}
}

// TestScanOperandMistakenForBannedOpcode is a regression test ensuring that
// banned-opcode bytes embedded as PUSH operands are never flagged as violations.
//
// E.g. PUSH1 0x32 encodes the byte value 50 (ORIGIN) as data — it must not
// be treated as an ORIGIN instruction.
func TestScanOperandMistakenForBannedOpcode(t *testing.T) {
	// PUSH1 <ORIGIN byte> STOP
	// The 0x32 byte is an operand, not an opcode.
	code := []byte{byte(vm.PUSH1), byte(vm.ORIGIN), byte(vm.STOP)}
	insns := validate.Scan(code)

	require.Len(t, insns, 2)
	require.Equal(t, vm.PUSH1, insns[0].Opcode)
	require.Equal(t, vm.STOP, insns[1].Opcode)

	// Confirm that ERC7562Rules finds no violation in this bytecode.
	v := validate.ERC7562Rules.Validate(code)
	require.Nil(t, v, "ORIGIN byte in PUSH1 operand must not be treated as a banned opcode")
}

// TestScanMixedCode verifies correct PC assignment across a realistic snippet
// that mixes PUSH, non-PUSH, and varying PUSH sizes.
func TestScanMixedCode(t *testing.T) {
	// PUSH2 0x01 0x00   — pushes 256 (pc=0, operand at 1..2)
	// DUP1              — (pc=3)
	// PUSH1 0x40        — (pc=4, operand at 5)
	// MSTORE            — (pc=6)
	// STOP              — (pc=7)
	code := []byte{
		byte(vm.PUSH2), 0x01, 0x00,
		byte(vm.DUP1),
		byte(vm.PUSH1), 0x40,
		byte(vm.MSTORE),
		byte(vm.STOP),
	}
	insns := validate.Scan(code)
	require.Len(t, insns, 5)

	expected := []struct {
		pc uint64
		op vm.OpCode
	}{
		{0, vm.PUSH2},
		{3, vm.DUP1},
		{4, vm.PUSH1},
		{6, vm.MSTORE},
		{7, vm.STOP},
	}
	for i, e := range expected {
		require.Equal(t, e.pc, insns[i].PC, "insns[%d].PC", i)
		require.Equal(t, e.op, insns[i].Opcode, "insns[%d].Opcode", i)
	}
}
