package sequencer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/zk/txpool"
	"github.com/erigontech/erigon/zk/utils"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/net/context"
)

type PoolTransactionYielder struct {
	ctx context.Context
	cfg ethconfig.Zk

	readyMtx sync.Mutex

	// readyTransactions and readyTransactionBytes are used to avoid us eagerly
	// decoding a transaction that we may never work with. instead we defer this
	// until we yield that single transaction.
	readyTransactions     []common.Hash
	readyTransactionBytes map[common.Hash][]byte

	pool      txpool.Pool
	yieldSize uint16

	// the pool db which sits separate from the usual erigon db
	db kv.RwDB

	decodedTxCache *expirable.LRU[common.Hash, *types.Transaction]

	// executionAt and forkId are used during the yielding process and will be
	// updated by the sequencer every time a new block is being processed.
	executionAt uint64
	forkId      uint64

	refreshing atomic.Bool
}

func NewPoolTransactionYielder(
	ctx context.Context,
	cfg ethconfig.Zk,
	pool txpool.Pool,
	yieldSize uint16,
	db kv.RwDB,
	decodedTxCache *expirable.LRU[common.Hash, *types.Transaction],
) *PoolTransactionYielder {
	readyTransactions := make([]common.Hash, 0)

	return &PoolTransactionYielder{
		readyTransactions:     readyTransactions,
		readyTransactionBytes: make(map[common.Hash][]byte),
		readyMtx:              sync.Mutex{},
		ctx:                   ctx,
		cfg:                   cfg,
		pool:                  pool,
		yieldSize:             yieldSize,
		db:                    db,
		decodedTxCache:        decodedTxCache,
	}
}

func (y *PoolTransactionYielder) YieldNextTransaction() (types.Transaction, uint8, bool) {
	var tx types.Transaction
	var effectiveGas uint8
	var err error
	var yieldedSomething bool

	y.readyMtx.Lock()
	defer y.readyMtx.Unlock()

	index := 0
	for idx, hash := range y.readyTransactions {
		index = idx
		if txBytes, found := y.readyTransactionBytes[hash]; found {
			txPtr, inCache := y.decodedTxCache.Get(hash)
			if inCache {
				tx = *txPtr
			} else {
				tx, err = types.DecodeTransaction(txBytes)
				if err != nil {
					log.Warn("[extractTransaction] Failed to decode transaction from ready queue, skipping and removing from queue",
						"error", err,
						"id", hash.String())
					y.pool.MarkForDiscardFromPendingBest(hash)
					y.cleanupTx(hash)
					continue
				}
				y.decodedTxCache.Add(hash, &tx)
			}
			effectiveGas = DeriveEffectiveGasPrice(y.cfg, tx)
			yieldedSomething = true
			if idx > 0 {
				log.Warn("Transaction not at front of ready queue", "id", hash.String())
			}
			break
		}
	}

	if index < len(y.readyTransactions) {
		y.readyTransactions = y.readyTransactions[index+1:]
	}

	// Lets maintain the size of the readyTransactions slice greater than the yield size
	if len(y.readyTransactions) < int(y.yieldSize) {
		y.performNextRefresh()
	}

	return tx, effectiveGas, yieldedSomething
}

func (y *PoolTransactionYielder) Requeue(txs []types.Transaction) {
	y.readyMtx.Lock()
	defer y.readyMtx.Unlock()

	toAdd := make([]common.Hash, 0, len(txs))
	for _, tx := range txs {
		hash := tx.Hash()
		// Add it to the front of the readyTransactions slice
		if _, found := y.readyTransactionBytes[hash]; !found {
			log.Warn("Transaction does not exist in readyTransactionBytes", "id", hash.String())
			continue
		}
		toAdd = append(toAdd, hash)
	}

	// Add them to the front of the readyTransactions slice
	y.readyTransactions = append(toAdd, y.readyTransactions...)
}

func (y *PoolTransactionYielder) AddMined(hash common.Hash) {
	y.readyMtx.Lock()
	defer y.readyMtx.Unlock()

	y.cleanupTx(hash)
}

func (y *PoolTransactionYielder) Discard(hash common.Hash) {
	y.readyMtx.Lock()
	defer y.readyMtx.Unlock()

	y.cleanupTx(hash)
}

func (y *PoolTransactionYielder) cleanupTx(hash common.Hash) {
	delete(y.readyTransactionBytes, hash)
	y.decodedTxCache.Remove(hash)
}

func (y *PoolTransactionYielder) SetExecutionDetails(executionAt, forkId uint64) {
	y.executionAt = executionAt
	y.forkId = forkId
}

func (y *PoolTransactionYielder) performNextRefresh() {
	if !y.refreshing.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer y.refreshing.Store(false)

		txHashes, txBytes, err := y.refreshPoolTransactions(y.executionAt, y.forkId)
		if err != nil {
			log.Error("Error while yielding next transactions", "error", err)
			time.Sleep(500 * time.Millisecond) // could be a transient error, wait before retrying
			// return early as there will be nothing to process now - we'll
			// wait until the next iteration happens to attempt again
			return
		}

		y.readyMtx.Lock()
		defer y.readyMtx.Unlock()

		for idx, hash := range txHashes {
			y.readyTransactions = append(y.readyTransactions, hash)
			y.readyTransactionBytes[hash] = txBytes[idx]
		}

		// Verify the order of nonces
		// y.debugVerifyNoncesOrder("refresh")
	}()
}

func (y *PoolTransactionYielder) debugVerifyNoncesOrder(prefix string) {
	noncesBySender := make(map[common.Address][]uint64)
	for _, hash := range y.readyTransactions {
		var tx types.Transaction
		var err error
		if txBytes, found := y.readyTransactionBytes[hash]; found {
			txPtr, inCache := y.decodedTxCache.Get(hash)
			if inCache {
				tx = *txPtr
			} else {
				tx, err = types.DecodeTransaction(txBytes)
				if err != nil {
					log.Error("Failed to decode transaction", "hash", hash.String(), "error", err)
					continue
				}
			}
		}
		if tx == nil {
			log.Error("Failed to get transaction", "hash", hash.String())
			panic("Failed to get transaction")
		}
		// Integrity check: decoded hash must match key
		if h := tx.Hash(); h != hash {
			log.Error("Hash/bytes mismatch", "expected", hash.String(), "decoded", h.String(), "prefix", prefix)
			panic("hash/bytes mismatch")
		}
		sender, _ := tx.Sender(*types.LatestSignerForChainID(tx.GetChainID().ToBig()))
		noncesBySender[sender] = append(noncesBySender[sender], tx.GetNonce())
	}

	for sender, nonces := range noncesBySender {
		// Verify the order of nonces
		if !sort.SliceIsSorted(nonces, func(i, j int) bool {
			notSorted := nonces[i] < nonces[j]
			if notSorted {
				log.Error(fmt.Sprintf("Nonces are not sorted for sender %v", sender.String()), "i", i, "nonce[i]", nonces[i], "j", j, "nonce[j]", nonces[j])
				for k := i - 10; k <= i+10; k++ {
					if k >= 0 && k < len(nonces) {
						log.Error("Nonce debug", "i", k, "nonce", nonces[k])
					}
				}
			}
			return notSorted
		}) {
			panic("nonces are not sorted")
		}
	}
}

func (y *PoolTransactionYielder) refreshPoolTransactions(executionAt, forkId uint64) ([]common.Hash, [][]byte, error) {
	gasLimit := utils.GetBlockGasLimitForFork(forkId)

	ti := utils.StartTimer("txpool", "get-transactions")
	defer ti.LogTimer()

	y.pool.PreYield()
	defer func() {
		y.pool.PostYield()
	}()

	var ids []common.Hash
	var txBytes [][]byte

	err := y.db.View(y.ctx, func(poolTx kv.Tx) error {
		slots := types2.TxsRlp{}
		_, _, err := y.pool.YieldBest(y.yieldSize, &slots, poolTx, executionAt, gasLimit, 0)
		if err != nil {
			return err
		}
		ids, txBytes, err = y.extractTransactionsFromSlot(&slots)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return ids, txBytes, nil
}

func (y *PoolTransactionYielder) extractTransactionsFromSlot(slot *types2.TxsRlp) ([]common.Hash, [][]byte, error) {
	ids := make([]common.Hash, 0, len(slot.TxIds))
	txBytes := make([][]byte, 0, len(slot.Txs))

	for idx, bytes := range slot.Txs {
		// get the id of the transaction and
		ids = append(ids, slot.TxIds[idx])
		bytesCopy := make([]byte, len(bytes))
		copy(bytesCopy, bytes)
		txBytes = append(txBytes, bytesCopy)
	}

	return ids, txBytes, nil
}

type LimboTransactionYielder struct {
	transactions []types.Transaction
	cfg          ethconfig.Zk
}

func NewLimboTransactionYielder(transactions []types.Transaction, cfg ethconfig.Zk) *LimboTransactionYielder {
	return &LimboTransactionYielder{
		transactions: transactions,
		cfg:          cfg,
	}
}

func (l *LimboTransactionYielder) YieldNextTransaction() (types.Transaction, uint8, bool) {
	if len(l.transactions) == 0 {
		return nil, 0, false
	}

	tx := l.transactions[0]
	effectiveGas := DeriveEffectiveGasPrice(l.cfg, tx)
	l.transactions = l.transactions[1:] // Remove the transaction after yielding it

	return tx, effectiveGas, true
}

func (l *LimboTransactionYielder) AddMined(_ common.Hash) {
	// no need to maintain this
}

func (l *LimboTransactionYielder) Discard(hash common.Hash) {
	// do nothing as we remove transactions immediately after yielding them
}

func (l *LimboTransactionYielder) SetExecutionDetails(_, _ uint64) {
	// LimboTransactionYielder does not use executionAt and forkId, so this method can be empty
}

func (l *LimboTransactionYielder) Requeue(txs []types.Transaction) {
	// do nothing
}

type RecoveryTransactionYielder struct {
	transactions         []types.Transaction
	effectivePercentages []uint8
}

func NewRecoveryTransactionYielder(transactions []types.Transaction, effectivePercentages []uint8) (*RecoveryTransactionYielder, error) {
	if len(transactions) != len(effectivePercentages) {
		return nil, errors.New("transactions and effectivePercentages must have the same length")
	}

	return &RecoveryTransactionYielder{
		transactions:         transactions,
		effectivePercentages: effectivePercentages,
	}, nil
}

func (d *RecoveryTransactionYielder) YieldNextTransaction() (types.Transaction, uint8, bool) {
	if len(d.transactions) == 0 {
		return nil, 0, false
	}

	tx := d.transactions[0]
	var effectiveGas uint8

	d.transactions = d.transactions[1:] // Remove the transaction after yielding it
	if len(d.effectivePercentages) > 0 {
		effectiveGas = d.effectivePercentages[0]
		d.effectivePercentages = d.effectivePercentages[1:]
	}

	return tx, effectiveGas, true
}

func (d *RecoveryTransactionYielder) AddMined(_ common.Hash) {
	// no need to maintain this
}

func (d *RecoveryTransactionYielder) Discard(hash common.Hash) {
	// do nothing as we remove transactions immediately after yielding them
}

func (d *RecoveryTransactionYielder) SetExecutionDetails(_, _ uint64) {
	// RecoveryTransactionYielder does not use executionAt and forkId, so this method can be empty
}

func (d *RecoveryTransactionYielder) Requeue(txs []types.Transaction) {
	// do nothing
}
