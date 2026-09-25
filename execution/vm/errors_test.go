package vm

import (
	"fmt"
	"testing"
)

// The interpreter wraps a dynamic-gas error that is not ErrOutOfGas as
// fmt.Errorf("%w: %w", ErrOutOfGas, err). For static-context CREATE / CREATE2 /
// SSTORE the underlying error is ErrWriteProtection, so the classification must
// report write protection rather than the out-of-gas wrapper.
func TestVMErrorCodeWriteProtectionWrappedAsOutOfGas(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("%w: %w", ErrOutOfGas, ErrWriteProtection)
	if got := vmErrorCodeFromErr(wrapped); got != VMErrorCodeWriteProtection {
		t.Fatalf("vmErrorCodeFromErr(write protection wrapped as out-of-gas) = %d, want VMErrorCodeWriteProtection (%d)", got, VMErrorCodeWriteProtection)
	}
}

func TestRevertClassificationIgnoresSwitchOrder(t *testing.T) {
	t.Parallel()
	for _, exceptional := range exceptionalErrs {
		wrapped := fmt.Errorf("%w: %w", exceptional, ErrExecutionReverted)
		if isRevert(wrapped) {
			t.Errorf("isRevert(%v wrapped with a revert) = true, want false", exceptional)
		}
	}
	if !isRevert(ErrExecutionReverted) {
		t.Error("isRevert(bare sentinel) = false, want true")
	}
	if !isRevert(fmt.Errorf("precompile failed: %w", ErrExecutionReverted)) {
		t.Error("isRevert(single-wrapped revert) = false, want true")
	}
	if isRevert(ErrOutOfGas) {
		t.Error("isRevert(non-revert) = true, want false")
	}
}
