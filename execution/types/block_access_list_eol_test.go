package types

import (
	"testing"

	"github.com/erigontech/erigon/common"
)

// A block access list whose account entry is an empty list (no Address) must be
// rejected, not silently decoded as an empty BAL. Exercises the production
// decoder DecodeBlockAccessListBytes -> checkErrListEnd: AccountChanges.DecodeRLP
// fails reading the address with a wrapped rlp.EOL, which checkErrListEnd must
// treat as a real error rather than a clean end-of-list. (The rlp reflection
// path already handles this; DecodeBlockAccessListBytes is the only place that
// silently truncated 0xc1c0 to an empty BAL before the fix.)
func TestBlockAccessListRejectsAddresslessAccount(t *testing.T) {
	t.Parallel()
	// c1 = list(len 1) containing c0 = empty list (an account with no address).
	input := common.FromHex("0xc1c0")
	if _, err := DecodeBlockAccessListBytes(input); err == nil {
		t.Fatal("expected error decoding addressless account, got nil")
	}
}
