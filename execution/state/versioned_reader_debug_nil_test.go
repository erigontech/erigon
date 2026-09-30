package state

import (
	"testing"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// A tracer's OnBalanceChange reads the prev account via ReadAccountDataForDebug.
// For a first transfer to a fresh account the base reader returns (nil, nil);
// the versioned reader must not dereference that nil.
func TestVersionedStateReaderReadAccountDataForDebugNilAccount(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	vr := NewVersionedStateReader(0, ReadSet{}, vm, &minimalStateReader{}, false)

	acc, err := vr.ReadAccountDataForDebug(accounts.InternAddress(accounts.Address{}.Value()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if acc != nil {
		t.Fatalf("expected nil account for an absent address, got %+v", acc)
	}
}
