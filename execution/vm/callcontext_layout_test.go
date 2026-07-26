package vm

import (
	"testing"
	"unsafe"
)

// pinnedStackOffset is the offset Stack has on the interned branches. Stack.data's
// cache-line alignment follows it, and moving it shifts unrelated benchmarks by
// several percent, so a by-value key/address must keep it by adjusting the pad.
const pinnedStackOffset = 264

func TestCallContextStackOffset(t *testing.T) {
	if got := unsafe.Offsetof(CallContext{}.Stack); got != pinnedStackOffset {
		t.Fatalf("Stack moved to offset %d (want %d): adjust the pad in CallContext", got, pinnedStackOffset)
	}
}
