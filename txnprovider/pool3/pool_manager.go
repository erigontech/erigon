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
	l1 
}

func (p *PoolManager) NewPoolmanager(dbRef kv.TemporalRoDB) {
	p.chainDB = dbRef
	p.accountCache = map[common.Address]*CachedAcc{}
}

func (p *PoolManager) Score(txnRef *containers.TxnRef) {
	txn := *txnRef.GetTransaction()
	slot := txnRef.GetSlot()
	var score int64

	feeCap := txn.GetFeeCap().Uint64()
	if feeCap < p.currentBaseFee {
		diffPc := 100 * (p.currentBaseFee - feeCap) / p.currentBaseFee
		maxDiffSteps := diffPc*10/125 + 2
		score += int64(100 / maxDiffSteps) // 100 / Number of blocks to reach that baseFee
	} else {
		diffPc := 100 * (feeCap - p.currentBaseFee) / p.currentBaseFee
		maxDiffSteps := diffPc*10/125 + 2
		score += int64(80 + (60*(maxDiffSteps-1))/maxDiffSteps) // min 110, max 140
	}

	priorityFeeNormalized := math.Log2(txn.GetTipCap().Div(txn.GetTipCap(), uint256.NewInt(1_000_000_000)).Float64())
	score += int64(priorityFeeNormalized * 50)

	sender := slot.SenderAddress
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

	if txnRef.GetSlot().Nonce < acc.Nonce.Load() {
		score -= 1000
		return
	}
	nonceDist := txnRef.GetSlot().Nonce - acc.Nonce.Load()
	score += 100 - 75*int64(nonceDist)
	txnRef.Score.Store(score)
}

func (p *PoolManager) AddTxn(txnRef *containers.TxnRef) {

}