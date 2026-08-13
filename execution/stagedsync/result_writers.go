package stagedsync

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// coinbaseFoldCheck reconstructs the coinbase balance in the apply loop, absolute
// and base-anchored: at each block it reads the block-start coinbase from the
// shared domain (base = the value apply committed for block N-1), then for each tx
// accumulates the traveling per-tx tips and asserts base + Σtips exactly equals the
// coinbase balance write calcFees materialized. This is the value apply must
// SYNTHESIZE once the fold moves off exec (step 5b), so proving it here de-risks the
// apply half without changing behaviour. baseReader reads sd.mem live; onTx runs
// before the tx's own writes are applied, so the first tx of a block reads N-1's
// coinbase. A direct EVM coinbase write is a checkpoint reset (base + Σtips no longer
// holds), so checking stops for the rest of that block — mirrors coinbaseVecCrossCheck.
type coinbaseFoldCheck struct {
	baseReader state.StateReader
	block      uint64
	base       uint256.Int
	started    bool
	stopped    bool
	sumTips    uint256.Int
}

func (c *coinbaseFoldCheck) onTx(r *txResult) {
	if r.coinbase.IsNil() || c.baseReader == nil {
		return
	}
	if !c.started || r.blockNum != c.block {
		c.block = r.blockNum
		c.started = true
		c.stopped = false
		c.sumTips = uint256.Int{}
		c.base = uint256.Int{}
		if acc, err := c.baseReader.ReadAccountData(r.coinbase); err == nil && acc != nil {
			c.base = acc.Balance
		}
	}
	if c.stopped {
		return
	}
	if r.coinbaseDirect {
		c.stopped = true
		return
	}
	c.sumTips.Add(&c.sumTips, &r.feeTipped)
	w, ok := coinbaseBalanceWrite(r.writes, r.coinbase)
	if !ok {
		return
	}
	var want uint256.Int
	want.Add(&c.base, &c.sumTips)
	if want != w {
		panic(fmt.Sprintf("coinbaseApplyCheck mismatch block=%d txNum=%d want=%s got=%s base=%s sumTips=%s",
			r.blockNum, r.txNum, want.String(), w.String(), c.base.String(), c.sumTips.String()))
	}
}

// coinbaseBalanceWrite returns the coinbase's balance write in this tx's write set,
// if present. WriteSetView exposes only iterators, so scan the balance writes.
func coinbaseBalanceWrite(writes state.WriteSetView, coinbase accounts.Address) (uint256.Int, bool) {
	if writes == nil {
		return uint256.Int{}, false
	}
	for addr, bw := range writes.Balances() {
		if addr == coinbase {
			return bw.Val, true
		}
	}
	return uint256.Int{}, false
}
