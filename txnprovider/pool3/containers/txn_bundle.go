package containers

import (
	"sort"
	"sync"
)

type TxnBundle struct {
	lock sync.RWMutex
	txns []TxnRef
	head uint64
	tail uint64
	maxCapacity uint64
	totalGas uint64
}

func (b *TxnBundle) HasCapacity() bool {
	return b.txns != nil && b.tail < b.maxCapacity && len(b.txns) < int(b.maxCapacity)
}

// FindAndInsert finds the position of the given TxnRef as per its socre
// in the txns slice and inserts it there, it returns false if t is worse than
// the worst txn it has currently and is already at full capacity
func (b *TxnBundle) FindAndInsert(t TxnRef) (success bool, evicted *TxnRef){
    b.lock.Lock()
    defer b.lock.Unlock()

    s := t.Score

    // Fast-fail if full and the newcomer is not better than the current worst.
    if uint64(len(b.txns)) == b.maxCapacity &&
        s <= b.txns[len(b.txns)-1].Score {
        return
    }
    // Binary search for first element whose score is < s (higher-score-first order)
    idx := sort.Search(len(b.txns), func(i int) bool {
        return b.txns[i].Score < s
    })

    if uint64(len(b.txns)) < b.maxCapacity {
        // room: simple grow + shift
        b.txns = append(b.txns, TxnRef{})        // space
    } else {
        // bundle full => drop worst (last entry) before shifting
        b.totalGas -= b.txns[len(b.txns)-1].Slot.Gas
    }
    copy(b.txns[idx+1:], b.txns[idx:])
    b.txns[idx] = t
    b.totalGas += t.Slot.Gas
    return true, nil
}

func (b *TxnBundle) PollTop(k int) []TxnRef {
    b.lock.Lock()
    defer b.lock.Unlock()
    if k > len(b.txns) { k = len(b.txns) }
    out := b.txns[:k:k]          // full slice copy cap k
    b.txns = b.txns[k:]
    return out
}