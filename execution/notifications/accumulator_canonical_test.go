package notifications

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
)

// ONLY WHAT THE CANONICAL BLOCK SEALED WITH MAY BE ANNOUNCED AS MINED.
//
// The txpool's removeMined deletes a sender's whole run at or below the highest nonce it is told
// about — correct for a block that is on the chain, destructive for anything else, because the
// nonce is then gone and nothing re-sends it.
//
// ⚠ AND THE CHANGE'S OWN HASH CANNOT BE USED TO DECIDE THAT. Pre-execution starts the change from
// the header it is building, so BlockHash is a provisional hash and no block by it ever exists.
// A rule of "announce only if the change's hash is canonical at this height" is false for EVERY
// block: measured on live445 height 1, changeHash=0x86da26db (unknown to the chain) against
// canonicalHash=0x9766df8e, dropping the 21 transactions that block really did seal with. The
// txpool was then told nothing had ever been mined — removals with reason Mined fell from 4,627
// to 0 — and it kept every transaction the chain had already taken.
//
// So the canonical body is read and INSTALLED, never compared against.
func TestTheCanonicalBodyReplacesWhateverTheChangeWasCarrying(t *testing.T) {
	txn := func(n byte) []byte { return []byte{n} }

	a := NewAccumulator()
	// Pre-execution's guess: a prefix of what the block went on to seal with.
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{txn(1)}, false)
	a.StartChange(&types.Header{Number: *uint256.NewInt(2)}, [][]byte{txn(9)}, false)

	replaced, cleared := a.SetCanonicalTxs(func(height uint64) ([][]byte, bool) {
		switch height {
		case 1:
			return [][]byte{txn(1), txn(2), txn(3)}, true // the block sealed with three
		default:
			return nil, false // body not readable here
		}
	})

	if replaced != 1 || cleared != 1 {
		t.Fatalf("replaced=%d cleared=%d, want 1/1", replaced, cleared)
	}
	chs := a.Changes()
	if got := len(chs[0].Txs); got != 3 {
		t.Errorf("height 1 announced %d transactions, want the canonical 3", got)
	}
	if chs[0].Txs[2][0] != 3 {
		t.Errorf("height 1 did not take the canonical set: %v", chs[0].Txs)
	}
	if len(chs[1].Txs) != 0 {
		t.Errorf("a height whose body could not be read must announce nothing, got %v", chs[1].Txs)
	}
}

// A height the chain genuinely sealed empty announces nothing, and is not counted as a loss.
func TestAnEmptyCanonicalBlockAnnouncesNothing(t *testing.T) {
	a := NewAccumulator()
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{{7}}, false)

	replaced, cleared := a.SetCanonicalTxs(func(uint64) ([][]byte, bool) { return nil, true })
	if replaced != 0 || cleared != 1 {
		t.Fatalf("replaced=%d cleared=%d, want 0/1", replaced, cleared)
	}
	if len(a.Changes()[0].Txs) != 0 {
		t.Error("an empty canonical block left the pre-executed guess standing")
	}
}

// The counts are the contract: clearing silently is what let a total failure of mined-removal run
// unnoticed across four soaks.
func TestSetCanonicalTxsReportsWhatItDid(t *testing.T) {
	a := NewAccumulator()
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{{1}}, false)
	a.StartChange(&types.Header{Number: *uint256.NewInt(2)}, nil, false) // nothing to lose
	a.StartChange(&types.Header{Number: *uint256.NewInt(3)}, [][]byte{{3}}, false)

	replaced, cleared := a.SetCanonicalTxs(func(height uint64) ([][]byte, bool) {
		if height == 1 {
			return [][]byte{{1}}, true
		}
		return nil, false
	})
	// Height 2 carried nothing, so clearing it is not a loss and is not counted.
	if replaced != 1 || cleared != 1 {
		t.Fatalf("replaced=%d cleared=%d, want 1/1", replaced, cleared)
	}
}
