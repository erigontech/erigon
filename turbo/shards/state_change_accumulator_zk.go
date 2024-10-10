package shards

import (
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
)

func (a *Accumulator) ChangeTransactions(txs [][]byte) {
	if txs == nil {
		return
	}

	a.latestChange.Txs = make([][]byte, len(txs))
	for i := range txs {
		a.latestChange.Txs[i] = libcommon.Copy(txs[i])
	}
}
