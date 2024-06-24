package shards

import (
	"github.com/ledgerwatch/erigon-lib/common"
)

func (a *Accumulator) ChangeTransactions(txs [][]byte) {
	if txs == nil {
		return
	}

	a.latestChange.Txs = make([][]byte, len(txs))
	for i := range txs {
		a.latestChange.Txs[i] = common.Copy(txs[i])
	}
}
