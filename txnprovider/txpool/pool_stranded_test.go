package txpool

import (
	"testing"

	"github.com/erigontech/erigon/common"
)

// A removal is only sound if the chain actually reaches the nonce that was removed.
//
// ⚠ AND THAT CANNOT BE ASKED WHEN THE REMOVAL HAPPENS. removeMined runs before the block's state
// change is applied, so the view there is the PRE-block state and every healthy removal reads as
// above the chain. The first version of this check did exactly that and fired on the very first
// removal of the run — nonce 0 against chainNext 0 — which is the shape of a probe that cries wolf.
//
// So the pool records what it removed and judges it later, once the chain has had its chance.
func TestAStrandedSenderIsOnlyJudgedOnceTheChainHasHadItsChance(t *testing.T) {
	const sender = uint64(7)
	addr := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	newPool := func() *TxPool {
		p := &TxPool{senders: newSendersBatch(nil)}
		p.senders.senderID2Addr[sender] = addr
		p.removedAsMined = map[uint64]minedWatch{sender: {nonce: 100, atBlock: 1000}}
		return p
	}

	t.Run("within the grace, nothing is claimed either way", func(t *testing.T) {
		p := newPool()
		p.checkStrandedSenders(nil, 1000+strandedGrace-1)
		if p.aboveChainRemovals != 0 {
			t.Error("reported a sender stranded before the chain had its grace")
		}
		if _, still := p.removedAsMined[sender]; !still {
			t.Error("stopped watching a removal that has not been judged yet")
		}
	})

	t.Run("an unmapped sender is dropped, never reported", func(t *testing.T) {
		p := newPool()
		delete(p.senders.senderID2Addr, sender)
		p.checkStrandedSenders(nil, 1000+strandedGrace+1)
		if p.aboveChainRemovals != 0 {
			t.Error("an unmapped id reads a chain nonce of zero and must not be reported")
		}
		if _, still := p.removedAsMined[sender]; still {
			t.Error("kept watching a sender it can never resolve")
		}
	})
}
