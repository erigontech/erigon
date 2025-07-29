package pool3

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	// "github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	// "github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/txnprovider/pool3/containers"
	"github.com/holiman/uint256"
	// "golang.org/x/exp/maps"
)

// TODO Add code to smartly allocate and manage the Pool3 memory

// Pool manager should be an interface

type CachedAcc struct {
	Nonce   atomic.Uint64
	Balance atomic.Pointer[uint256.Int]
}

type PoolManager struct {
	currentBaseFee uint64
	lock sync.RWMutex
	accountCache   map[common.Address]*CachedAcc
	chainDB        kv.TemporalRoDB

	// MemPool
	l0 containers.TxnBundle
	l1 containers.TxnBundle
	l2 containers.SecondaryPool
}

func (p *PoolManager) NewPoolmanager(dbRef kv.TemporalRoDB) {
	p.chainDB = dbRef
	p.accountCache = map[common.Address]*CachedAcc{}
}

func (p *PoolManager) Score(txnRef *containers.TxnRef) {
	txn := *txnRef.Slot
	var score int

	feeCap := txn.FeeCap.Uint64()
	if feeCap < p.currentBaseFee {
		diffPc := 100 * (p.currentBaseFee - feeCap) / p.currentBaseFee
		maxDiffSteps := diffPc*10/125 + 2
		score += int(100 / maxDiffSteps) // 100 / Number of blocks to reach that baseFee
	} else {
		diffPc := 100 * (feeCap - p.currentBaseFee) / p.currentBaseFee
		maxDiffSteps := diffPc*10/125 + 2
		score += int(80 + (60*(maxDiffSteps-1))/maxDiffSteps) // min 110, max 140
	}

	priorityFeeNormalized := math.Log2(txn.Tip.Div(&txn.Tip, uint256.NewInt(1_000_000_000)).Float64())
	score += int(priorityFeeNormalized * 50)

	sender := txnRef.SenderAddr
	if sender == nil {
		score -= 1000 // Sender should be calculated before scoring
		return
	}

	p.lock.RLock()
	defer p.lock.RUnlock()
	acc, ok := p.accountCache[*sender]
	p.lock.RUnlock()

	if !ok {
		roTx, err := p.chainDB.BeginTemporalRo(context.Background())
		if err != nil {
			return
		}
		if roTx != nil {
			stateReader := state.NewReaderV3(roTx.(kv.TemporalGetter))
			accPtr, err := stateReader.ReadAccountData(*sender)
			if err != nil {
				return
			}
			p.lock.Lock()
			defer p.lock.Unlock()
			acc = &CachedAcc{}
			acc.Balance.Store(&accPtr.Balance)
			acc.Nonce.Store(accPtr.Nonce)
			p.accountCache[*sender] = acc
		}
	}

	if txnRef.Slot.Nonce < acc.Nonce.Load() {
		score -= 1000
		return
	}
	nonceDist := txnRef.Slot.Nonce - acc.Nonce.Load()
	score += 100 - 75*int(nonceDist)
	txnRef.Score = score
}

func (p *PoolManager) AddTxn(txn TxnRef) {
    // if b, e := p.l0.FindAndInsert(txn); !b {                       // fast path
    //     p.idx[tx.hash()] = location{0, 0}
    //     return
    // }
    // if p.L1.FindAndInsert(tx) {
    //     p.idx[tx.hash()] = location{1, 0}
    //     return
    // }
    // p.L2.ReplaceOrInsert(tx)                          // O(log n)
    // p.idx[tx.hash()] = location{2, 0}
}


func (pm *PoolManager) Remove(hash common.Hash) (removed bool) {
    // pm.mu.Lock()
    // defer pm.mu.Unlock()

    // loc, ok := pm.idx[hash]
    // if !ok { return false }
    // switch loc.level {
    // case 0:
    //     removed = pm.L0.removeByHash(hash)  // linear scan ≤200
    // case 1:
    //     removed = pm.L1.removeByHash(hash)  // linear scan ≤2 000
    // case 2:
    //     removed = pm.L2.Delete(&TxnRef{Hash: hash}) != nil
    // }
    // delete(pm.idx, hash)
    // return
}

func (pm *PoolManager) Yield(n int) []TxnRef {
    // pm.mu.Lock()
    // defer pm.mu.Unlock()

    // out := pm.L0.PollTop(n)
    // n -= len(out)
    // if n > 0 {
    //     out = append(out, pm.L1.PollTop(n)...)
    //     n -= len(out)
    // }
    // if n > 0 {
    //     pm.L2.DescendGreaterOrEqual(maxScoreItem, func(i *TxnRef) bool {
    //         out = append(out, *i)
    //         pm.L2.Delete(i)
    //         return len(out) < n
    //     })
    // }
    // // update idx map in bulk if needed (omitted here for brevity)
    // return out
}