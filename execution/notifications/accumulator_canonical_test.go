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

// AN EMPTY CANONICAL BODY IS AN ANSWER, AND IT MUST WIN.
//
// I first wrote this test the other way round - "a flush with nothing to say says nothing" - and
// that instinct is what let the original defect survive its own fix. If the body cannot be read, or
// is genuinely empty, leaving the pre-executed set in place publishes a GUESS as canonical and the
// pool deletes every transaction in it. Measured on soak live436: 1200/2400/4800 drained in 60s
// each, then a fresh 87-wide hole at 9600 - chainNext=12447, poolLowest=12534, 26,052 transactions
// stranded behind it.
//
// The two errors are not symmetric. Removing too little self-corrects: the next block naming that
// sender removes its whole run at or below it. Removing too much is permanent - the nonce is gone,
// nothing re-sends it, and everything after it is stuck for ever. So announce nothing mined rather
// than guess.
func TestAnEmptyCanonicalBodyClearsThePreExecutedGuess(t *testing.T) {
	a := NewAccumulator()
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{{7}}, false)
	a.SetCanonicalChangeTxs(1, nil)
	if got := len(a.Changes()[0].Txs); got != 0 {
		t.Fatalf("the pre-executed guess survived an empty canonical body: %d txs left. Every one of "+
			"them would be announced as MINED and deleted from the pool with the sender's whole run "+
			"below it", got)
	}
}

// A CHANGE ABOVE THE CANONICAL HEAD MAY NOT CLAIM ANYTHING MINED.
//
// SendAndReset publishes EVERY accumulated change, and pre-execution starts one for every height it
// runs — ahead of what has been forkchoice-updated. Reconciling only the FCU'd height left every
// height above it carrying the pre-executed set, which the txpool took as mined and deleted along
// with each sender's whole run below it.
//
// The hole shrank across three fixes without closing — frontGap 93 -> 87 -> 34 — because each
// closed one path into the same defect. This is the one that remained.
func TestChangesAboveTheFCUHeightAnnounceNothingMined(t *testing.T) {
	txn := func(n byte) []byte { return []byte{n} }
	a := NewAccumulator()

	// Canonical head is 1; pre-execution has already run 2 and 3.
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{txn(1)}, false)
	a.StartChange(&types.Header{Number: *uint256.NewInt(2)}, [][]byte{txn(2)}, false)
	a.StartChange(&types.Header{Number: *uint256.NewInt(3)}, [][]byte{txn(3)}, false)

	a.ClearChangeTxsAbove(1)

	chs := a.Changes()
	if len(chs[0].Txs) != 1 {
		t.Fatalf("the FCU'd height lost its body: %d txs", len(chs[0].Txs))
	}
	for _, ch := range chs[1:] {
		if len(ch.Txs) != 0 {
			t.Fatalf("height %d is ABOVE the canonical head and still claims %d mined tx(s) — the pool "+
				"will delete them and every nonce below them, permanently", ch.BlockHeight, len(ch.Txs))
		}
	}
}
