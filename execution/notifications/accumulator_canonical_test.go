package notifications

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// ONLY THE BLOCK THAT WAS CHOSEN MAY SAY ANYTHING MINED.
//
// ⚠ THIS IS THE DEFECT THAT COST A SOAK ITS WHOLE POOL, FIVE TIMES OVER.
//
// A block's transaction set is settled when it SEALS - it is a property of that block and never
// changes. Whether that block is the canonical one at its height is a property of the CHAIN, and is
// settled later. The code conflated the two, and every variant of the same bug followed:
//
//   - pre-execution's set was left as the default, and a stopped round commits only the prefix it
//     applied, so the set routinely held transactions the block never sealed with;
//   - the content was re-derived at FCU by reading the body back out of the database, which can
//     come back EMPTY while the body is still flushing - and then the guess stood;
//   - only ONE height was reconciled, which is what you write having never seen an FCU advance more
//     than one block. Under load it advances several.
//
// The txpool then took all of it as mined, and removeMined deletes a sender's whole run at or below
// the highest nonce it is told about - correct on a canonical chain, fatal on a guess. The nonce is
// gone, nothing re-sends it, and every later transaction from that sender is stranded for ever.
// Measured: chainNext 12447, poolLowest 12534, 26,052 transactions behind an 87-wide hole, every
// round offering 128 and keeping 0.
//
// So the rule is one line: a change announces its transactions only if its block is the canonical
// one at its height. A height that cannot be resolved counts as not chosen - removing too little
// self-corrects on the next block, removing too much is permanent.
func TestOnlyTheCanonicalBlockAnnouncesItsTransactions(t *testing.T) {
	txn := func(n byte) []byte { return []byte{n} }
	chosen := common.HexToHash("0xc0")
	orphan := common.HexToHash("0x0f")

	a := NewAccumulator()
	a.StartChange(&types.Header{Number: *uint256.NewInt(1)}, [][]byte{txn(1)}, false)
	// Same height, a block that lost - and a height above the head, which has no canonical hash.
	a.StartChange(&types.Header{Number: *uint256.NewInt(2)}, [][]byte{txn(2)}, false)
	a.StartChange(&types.Header{Number: *uint256.NewInt(3)}, [][]byte{txn(3)}, false)

	chs := a.Changes()
	chs[0].BlockHash = chosen
	chs[1].BlockHash = orphan
	chs[2].BlockHash = orphan

	a.KeepOnlyCanonicalTxs(func(height uint64) (common.Hash, bool) {
		switch height {
		case 1:
			return chosen, true
		case 2:
			return common.HexToHash("0xbb"), true // a DIFFERENT block won this height
		default:
			return common.Hash{}, false // above the head: nothing is canonical yet
		}
	})

	out := a.Changes()
	if len(out[0].Txs) != 1 {
		t.Fatalf("the CHOSEN block lost its transactions: %d", len(out[0].Txs))
	}
	if len(out[1].Txs) != 0 {
		t.Fatal("an ORPHANED block still claims its transactions mined - the pool will delete them " +
			"and every nonce below them, permanently")
	}
	if len(out[2].Txs) != 0 {
		t.Fatal("a height ABOVE the canonical head still claims transactions mined - pre-execution " +
			"runs ahead, and a prediction may not say something mined")
	}
}
