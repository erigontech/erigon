package notifications

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
)

// THE SEALED BODY WINS OVER WHAT PRE-EXECUTION RAN.
//
// ⚠ THIS COST A SOAK ITS ENTIRE POOL, UNRECOVERABLY.
//
// Pre-execution starts the change for a height with the transactions it executed. What executed is
// NOT what the block sealed with: a stopped round commits only the prefix it applied, and a cut
// transaction goes into the next round. If the FCU flush only fills a change that is EMPTY, the
// pre-executed set survives, goes out as a FORWARD state change, and the txpool takes every one of
// them as mined — and removeMined then deletes the sender's whole run at or below the highest nonce
// it was told about, because on a canonical chain that would be sound.
//
// Measured on live433: the pool deleted up to nonce 1732 while the chain was at 1512, the chain
// reached 1639 and stopped for ever, and 6,434 transactions became permanently unpromotable.
func TestFCUBodyOverwritesWhatPreExecutionRan(t *testing.T) {
	txn := func(n byte) []byte { return []byte{n} }
	a := NewAccumulator()

	h := &types.Header{Number: *uint256.NewInt(1)}
	// Pre-execution ran three; the block sealed with the first two.
	a.StartChange(h, [][]byte{txn(1), txn(2), txn(3)}, false)
	a.SetCanonicalChangeTxs(1, [][]byte{txn(1), txn(2)})

	chs := a.Changes()
	if len(chs) != 1 {
		t.Fatalf("expected one change, got %d", len(chs))
	}
	if got := len(chs[0].Txs); got != 2 {
		t.Fatalf("the change carries %d txs, want the 2 the block SEALED with — a third would be "+
			"announced as mined and deleted from the pool with everything below it", got)
	}
	for i, want := range [][]byte{txn(1), txn(2)} {
		if string(chs[0].Txs[i]) != string(want) {
			t.Fatalf("tx %d is %x, want %x", i, chs[0].Txs[i], want)
		}
	}
}

// An empty canonical body must not erase what is there: a flush with nothing to say says nothing.
func TestAnEmptyCanonicalBodyLeavesTheChangeAlone(t *testing.T) {
	a := NewAccumulator()
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{{7}}, false)
	a.SetCanonicalChangeTxs(1, nil)
	if got := len(a.Changes()[0].Txs); got != 1 {
		t.Fatalf("an empty body wiped the change: %d txs left", got)
	}
}
