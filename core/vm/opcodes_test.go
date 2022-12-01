package vm

import (
	"testing"
)

// TestOpcodeStringer is a sanity check to test the internal consistency of our opcode:string mappings.
func TestOpcodeStringer(t *testing.T) {
	for op, str := range opCodeToString {
		// StringToOp will panic if the string is not valid.
		if gotOp := StringToOp(str); gotOp != op {
			t.Errorf("StringToOp[%q] = %v, want %v", str, gotOp, op)
		}
	}
}
