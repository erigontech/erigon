package containers

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

type TxnRef struct {
	Lock  sync.RWMutex
	Score atomic.Int64
	// SenderAddr stores a [common.Address] value atomically.
	// Use Store(common.Address) and Load().(common.Address) for type safety.
	SenderAddr  atomic.Value
	transaction *types.Transaction
	slot        txpool.TxnSlot
}

// GetSlot returns the slot.
func (t *TxnRef) GetSlot() txpool.TxnSlot {
	return t.slot
}

// SetSlot sets the slot.
func (t *TxnRef) SetSlot(slot txpool.TxnSlot) {
	t.slot = slot
}

// GetScore returns the current score.
func (t *TxnRef) GetScore() int64 {
	return t.Score.Load()
}

// SetScore sets the score to the given value.
func (t *TxnRef) SetScore(val int64) {
	t.Score.Store(val)
}

// GetSenderAddr returns the stored sender address.
func (t *TxnRef) GetSenderAddr() interface{} {
	return t.SenderAddr.Load()
}

// SetSenderAddr stores the sender address.
func (t *TxnRef) SetSenderAddr(addr interface{}) {
	t.SenderAddr.Store(addr)
}

// GetTransaction returns the transaction.
func (t *TxnRef) GetTransaction() *types.Transaction {
	t.Lock.RLock()
	defer t.Lock.RUnlock()
	return t.transaction
}

// SetTransaction sets the transaction.
func (t *TxnRef) SetTransaction(tx *types.Transaction) {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.transaction = tx
}
