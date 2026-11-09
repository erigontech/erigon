package exec

import (
	"os"
	"slices"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	sendersNoWait           = os.Getenv("SENDERS_NOWAIT") == "1"
	detachedSendersRecovery atomic.Pointer[SendersRecovery]
)

// DetachSendersRecovery lets exec read r per tx on the cached body, so the Senders stage need not wait for r.
func DetachSendersRecovery(r *SendersRecovery, cachedTxns types.Transactions) bool {
	if r.ready == nil || len(r.ready) != len(cachedTxns) {
		return false
	}
	r.cachedTxns = cachedTxns
	if prev := detachedSendersRecovery.Swap(r); prev != nil {
		log.Info("[dbg] SENDERS_NOWAIT", "txs", len(prev.cachedTxns), "lazy", prev.lazy.Load())
	}
	return true
}

func sendersDetached(txns types.Transactions) bool {
	r := detachedSendersRecovery.Load()
	return r != nil && slices.Equal(r.cachedTxns, txns)
}

func detachedSender(txn types.Transaction, i int) (accounts.Address, bool) {
	r := detachedSendersRecovery.Load()
	if r == nil || i >= len(r.cachedTxns) || r.cachedTxns[i] != txn {
		return accounts.NilAddress, false
	}
	if !r.ready[i].Load() {
		r.lazy.Add(1)
		return accounts.NilAddress, false
	}
	txn.SetSender(r.senders[i])
	return r.senders[i], true
}
