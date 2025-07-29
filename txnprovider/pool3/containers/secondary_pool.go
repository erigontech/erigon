package containers

import (
	"math/big"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/google/btree"
)

func (a *TxnRef) Less(b *TxnRef) bool {
    as := a.Score
    bs := b.Score
    if as == bs {
        // Break ties with the lexical order of the tx hash so ordering is stable
		return big.NewInt(0).SetBytes(a.Slot.IDHash[:]).Cmp(big.NewInt(0).SetBytes(b.Slot.IDHash[:])) < 0
    }
    // Higher score == "smaller" element for BTree.Min()
    return as > bs
}

const btreeDegree = 128 // suitable for ~100K elements, balancing memory and performance

// SecondaryPool is a heavier alternative to TxnBundle backed by a cache‑efficient
// B‑tree.  It is intended for the L2 lane where capacity can be large and
// score re‑ordering is in bursts.
type SecondaryPool struct {
    lock        sync.RWMutex
    tree        *btree.BTreeG[*TxnRef]
    maxCapacity uint64
}

// NewSecondaryPool allocates an empty pool with the given hard capacity.
func NewSecondaryPool(max uint64) *SecondaryPool {
    return &SecondaryPool{
        tree:        btree.NewG(btreeDegree, func(a, b *TxnRef) bool { return a.Less(b) }),
        maxCapacity: max,
    }
}

// HasCapacity reports whether the pool can accept at least one more tx.
func (p *SecondaryPool) HasCapacity() bool {
    p.lock.RLock()
    defer p.lock.RUnlock()
    return uint64(p.tree.Len()) < p.maxCapacity
}

// Insert adds a transaction.  If the pool is at capacity and the newcomer’s
// score is better than the current worst, that worst txn is evicted and
// returned.  The boolean result is true when the caller should keep the new
// transaction (i.e. it made it into the pool).
func (p *SecondaryPool) Insert(ref *TxnRef) (evicted *TxnRef, inserted bool) {
    p.lock.Lock()
    defer p.lock.Unlock()

    // If there is capacity
    if uint64(p.tree.Len()) < p.maxCapacity {
        p.tree.ReplaceOrInsert(ref)
        return nil, true
    }

    // Pool full: compare with the current worst (Max) score.
    worst, isEmpty := p.tree.Max()
    if worst == nil || isEmpty{
        // should not happen, but be safe
        p.tree.ReplaceOrInsert(ref)
        return nil, true
    }
    if ref.Score <= worst.Score {
        return nil, false // not good enough — reject
    }
    // Evict the worst and insert the new one.
    p.tree.Delete(worst)
    p.tree.ReplaceOrInsert(ref)
    return worst, true
}

// Evict removes every transaction whose hash is in the provided slice and
// returns the refs actually removed (handy for bookkeeping in PoolManager).
func (p *SecondaryPool) Evict(txHashes []common.Hash) []*TxnRef {
    p.lock.Lock()
    defer p.lock.Unlock()

    removed := make([]*TxnRef, 0, len(txHashes))
    hashSet := make(map[common.Hash]struct{}, len(txHashes))
    for _, h := range txHashes {
        hashSet[h] = struct{}{}
    }

    // Walk the tree once; Delete‑while‑iterating is safe in BTreeG.
    p.tree.Ascend(func(it *TxnRef) bool {
        if _, ok := hashSet[it.Slot.IDHash]; ok {
            p.tree.Delete(it)
            removed = append(removed, it)
        }
        return true
    })
    return removed
}

// Shuffle recomputes the score of every transaction (using the supplied
// scorer) and rebuilds the B‑tree so ordering remains correct.  The scorer
// callback must be thread‑safe; it will be invoked with the pool lock held.
func (p *SecondaryPool) Shuffle(scoreFn func(*TxnRef)) {
    p.lock.Lock()
    defer p.lock.Unlock()

    // Collect all refs, rescore them, then rebuild a fresh tree.
    all := make([]*TxnRef, 0, p.tree.Len())
    p.tree.Ascend(func(it *TxnRef) bool {
        scoreFn(it)
        all = append(all, it)
        return true
    })

    p.tree.Clear(false) // keep underlying pages to reuse memory
    for _, it := range all {
        p.tree.ReplaceOrInsert(it)
    }
}






// package containers

// import (
// 	"sync/atomic"

// 	"github.com/tidwall/btree"
// )

// type SecondaryPool struct {
// 	tree      atomic.Pointer[btree.BTreeG[TxnRef]]
// 	maxCapacity uint64
// }

// func New(capacity uint64) *SecondaryPool{
// 	s := &SecondaryPool{
// 		maxCapacity: capacity,
// 	}
// 	s.tree.Store(btree.NewBTreeG[TxnRef](IsTxnAGtTxnB))
// 	return s
// }

// func (s *SecondaryPool) InsertBounded(t TxnRef) *TxnRef {
// 	tr := s.tree.Load()
// 	tr.Set(t)
	
// 	// Fast path: nothing to evict.
// 	if s.maxCapacity == 0 || tr.Len() <= int(s.maxCapacity) {
// 		return nil
// 	}
// 	ret, _ := tr.PopMin()
// 	return &ret
// }


// // PeekBest returns a slice of n-best items in the SecondaryPool instance
// func (s *SecondaryPool) PeekBest(n int) []TxnRef {
// 	t := s.tree.Load()
// 	ret := make([]TxnRef, 0, 10)
// 	i := n
// 	t.Scan(func(item TxnRef) bool {
// 		ret = append(ret, item)
// 		i--
// 		if i == 0{
// 			return false
// 		}
// 		return true
// 	})
// 	return ret
// }


// // PeekBest returns a slice of n-best items in the SecondaryPool instance
// func (s *SecondaryPool) Worst(n int) []TxnRef {
// 	t := s.tree.Load()
// 	ret := make([]TxnRef, 0, 10)
// 	i := n
// 	t.Reverse(func(item TxnRef) bool {
// 		ret = append(ret, item)
// 		i--
// 		if i == 0{
// 			return false
// 		}
// 		return true
// 	})
// 	return ret
// }


// func (s *SecondaryPool) Size() int {
// 	return s.tree.Load().Len()
// }