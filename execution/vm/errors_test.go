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
