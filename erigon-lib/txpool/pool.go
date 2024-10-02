/*
   Copyright 2022 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	libkzg "github.com/ledgerwatch/erigon-lib/crypto/kzg"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	txpoolproto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
	types2 "github.com/ledgerwatch/erigon-lib/types"
)

const DefaultBlockGasLimit = uint64(30000000)

var (
	processBatchTxsTimer    = metrics.NewSummary(`pool_process_remote_txs`)
	addRemoteTxsTimer       = metrics.NewSummary(`pool_add_remote_txs`)
	newBlockTimer           = metrics.NewSummary(`pool_new_block`)
	writeToDBTimer          = metrics.NewSummary(`pool_write_to_db`)
	propagateToNewPeerTimer = metrics.NewSummary(`pool_propagate_to_new_peer`)
	propagateNewTxsTimer    = metrics.NewSummary(`pool_propagate_new_txs`)
	writeToDBBytesCounter   = metrics.GetOrCreateGauge(`pool_write_to_db_bytes`)
	pendingSubCounter       = metrics.GetOrCreateGauge(`txpool_pending`)
	queuedSubCounter        = metrics.GetOrCreateGauge(`txpool_queued`)
	basefeeSubCounter       = metrics.GetOrCreateGauge(`txpool_basefee`)
)

var TraceAll = false

// Pool is interface for the transaction pool
// This interface exists for the convenience of testing, and not yet because
// there are multiple implementations
//
//go:generate mockgen -typed=true -destination=./pool_mock.go -package=txpool . Pool
type Pool interface {
	ValidateSerializedTxn(serializedTxn []byte) error

	// Handle 3 main events - new remote txs from p2p, new local txs from RPC, new blocks from execution layer
	AddRemoteTxs(ctx context.Context, newTxs types.TxSlots)
	AddLocalTxs(ctx context.Context, newTxs types.TxSlots, tx kv.Tx) ([]txpoolcfg.DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, unwindBlobTxs, minedTxs types.TxSlots, tx kv.Tx) error
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	FilterKnownIdHashes(tx kv.Tx, hashes types.Hashes) (unknownHashes types.Hashes, err error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)

	AddNewGoodPeer(peerID types.PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check

// SubPoolMarker is an ordered bitset of five bits that's used to sort transactions into sub-pools. Bits meaning:
// 1. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 2. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 3. Not too much gas: Set to 1 if the transaction doesn't use too much gas
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

const (
	NoNonceGaps       = 0b010000
	EnoughBalance     = 0b001000
	NotTooMuchGas     = 0b000100
	EnoughFeeCapBlock = 0b000010
	IsLocal           = 0b000001

	BaseFeePoolBits = NoNonceGaps + EnoughBalance + NotTooMuchGas
)

// metaTx holds transaction and some metadata
type metaTx struct {
	Tx                        *types.TxSlot
	minFeeCap                 uint256.Int
	nonceDistance             uint64 // how far their nonces are from the state's nonce for the sender
	cumulativeBalanceDistance uint64 // how far their cumulativeRequiredBalance are from the state's balance for the sender
	minTip                    uint64
	bestIndex                 int
	worstIndex                int
	timestamp                 uint64 // when it was added to pool
	subPool                   SubPoolMarker
	currentSubPool            SubPoolType
	minedBlockNum             uint64
}

func newMetaTx(slot *types.TxSlot, isLocal bool, timestamp uint64) *metaTx {
	mt := &metaTx{Tx: slot, worstIndex: -1, bestIndex: -1, timestamp: timestamp}
	if isLocal {
		mt.subPool = IsLocal
	}
	return mt
}

type SubPoolType uint8

const PendingSubPool SubPoolType = 1
const BaseFeeSubPool SubPoolType = 2
const QueuedSubPool SubPoolType = 3

func (sp SubPoolType) String() string {
	switch sp {
	case PendingSubPool:
		return "Pending"
	case BaseFeeSubPool:
		return "BaseFee"
	case QueuedSubPool:
		return "Queued"
	}
	return fmt.Sprintf("Unknown:%d", sp)
}

// sender - immutable structure which stores only nonce and balance of account
type sender struct {
	balance uint256.Int
	nonce   uint64
}

func newSender(nonce uint64, balance uint256.Int) *sender {
	return &sender{nonce: nonce, balance: balance}
}

var emptySender = newSender(0, *uint256.NewInt(0))

func SortByNonceLess(a, b *metaTx) bool {
	if a.Tx.SenderID != b.Tx.SenderID {
		return a.Tx.SenderID < b.Tx.SenderID
	}
	return a.Tx.Nonce < b.Tx.Nonce
}

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// txpool doesn't start any goroutines - "leave concurrency to user" design
// txpool has no DB-TX fields - "leave db transactions management to user" design
// txpool has _chainDB field - but it must maximize local state cache hit-rate - and perform minimum _chainDB transactions
//
// It preserve TxSlot objects immutable
type TxPool struct {
	_chainDB               kv.RoDB // remote db - use it wisely
	_stateCache            kvcache.Cache
	lock                   *sync.Mutex
	recentlyConnectedPeers *recentlyConnectedPeers // all txs will be propagated to this peers eventually, and clear list
	senders                *sendersBatch
	// batch processing of remote transactions
	// handling is fast enough without batching, but batching allows:
	//   - fewer _chainDB transactions
	//   - batch notifications about new txs (reduced P2P spam to other nodes about txs propagation)
	//   - and as a result reducing lock contention
	unprocessedRemoteTxs    *types.TxSlots
	unprocessedRemoteByHash map[string]int                                  // to reject duplicates
	byHash                  map[string]*metaTx                              // tx_hash => tx : only those records not committed to db yet
	discardReasonsLRU       *simplelru.LRU[string, txpoolcfg.DiscardReason] // tx_hash => discard_reason : non-persisted
	pending                 *PendingPool
	baseFee                 *SubPool
	queued                  *SubPool
	minedBlobTxsByBlock     map[uint64][]*metaTx             // (blockNum => slice): cache of recently mined blobs
	minedBlobTxsByHash      map[string]*metaTx               // (hash => mt): map of recently mined blobs
	isLocalLRU              *simplelru.LRU[string, struct{}] // tx_hash => is_local : to restore isLocal flag of unwinded transactions
	newPendingTxs           chan types.Announcements         // notifications about new txs in Pending sub-pool
	all                     *BySenderAndNonce                // senderID => (sorted map of tx nonce => *metaTx)
	deletedTxs              []*metaTx                        // list of discarded txs since last db commit
	promoted                types.Announcements
	cfg                     txpoolcfg.Config
	chainID                 uint256.Int
	lastSeenBlock           atomic.Uint64
	lastSeenCond            *sync.Cond
	lastFinalizedBlock      atomic.Uint64
	started                 atomic.Bool
	pendingBaseFee          atomic.Uint64
	pendingBlobFee          atomic.Uint64 // For gas accounting for blobs, which has its own dimension
	blockGasLimit           atomic.Uint64
	totalBlobsInPool        atomic.Uint64
	shanghaiTime            *uint64
	isPostShanghai          atomic.Bool
	agraBlock               *uint64
	isPostAgra              atomic.Bool
	cancunTime              *uint64
	isPostCancun            atomic.Bool
	maxBlobsPerBlock        uint64
	feeCalculator           FeeCalculator
	logger                  log.Logger
}

type FeeCalculator interface {
	CurrentFees(chainConfig *chain.Config, db kv.Getter) (baseFee uint64, blobFee uint64, minBlobGasPrice, blockGasLimit uint64, err error)
}

func New(newTxs chan types.Announcements, coreDB kv.RoDB, cfg txpoolcfg.Config, cache kvcache.Cache,
	chainID uint256.Int, shanghaiTime, agraBlock, cancunTime *big.Int, maxBlobsPerBlock uint64,
	feeCalculator FeeCalculator, logger log.Logger,
) (*TxPool, error) {
	localsHistory, err := simplelru.NewLRU[string, struct{}](10_000, nil)
	if err != nil {
		return nil, err
	}
	discardHistory, err := simplelru.NewLRU[string, txpoolcfg.DiscardReason](10_000, nil)
	if err != nil {
		return nil, err
	}

	byNonce := &BySenderAndNonce{
		tree:              btree.NewG[*metaTx](32, SortByNonceLess),
		search:            &metaTx{Tx: &types.TxSlot{}},
		senderIDTxnCount:  map[uint64]int{},
		senderIDBlobCount: map[uint64]uint64{},
	}
	tracedSenders := make(map[common.Address]struct{})
	for _, sender := range cfg.TracedSenders {
		tracedSenders[common.BytesToAddress([]byte(sender))] = struct{}{}
	}

	lock := &sync.Mutex{}

	res := &TxPool{
		lock:                    lock,
		lastSeenCond:            sync.NewCond(lock),
		byHash:                  map[string]*metaTx{},
		isLocalLRU:              localsHistory,
		discardReasonsLRU:       discardHistory,
		all:                     byNonce,
		recentlyConnectedPeers:  &recentlyConnectedPeers{},
		pending:                 NewPendingSubPool(PendingSubPool, cfg.PendingSubPoolLimit),
		baseFee:                 NewSubPool(BaseFeeSubPool, cfg.BaseFeeSubPoolLimit),
		queued:                  NewSubPool(QueuedSubPool, cfg.QueuedSubPoolLimit),
		newPendingTxs:           newTxs,
		_stateCache:             cache,
		senders:                 newSendersCache(tracedSenders),
		_chainDB:                coreDB,
		cfg:                     cfg,
		chainID:                 chainID,
		unprocessedRemoteTxs:    &types.TxSlots{},
		unprocessedRemoteByHash: map[string]int{},
		minedBlobTxsByBlock:     map[uint64][]*metaTx{},
		minedBlobTxsByHash:      map[string]*metaTx{},
		maxBlobsPerBlock:        maxBlobsPerBlock,
		feeCalculator:           feeCalculator,
		logger:                  logger,
	}

	if shanghaiTime != nil {
		if !shanghaiTime.IsUint64() {
			return nil, errors.New("shanghaiTime overflow")
		}
		shanghaiTimeU64 := shanghaiTime.Uint64()
		res.shanghaiTime = &shanghaiTimeU64
	}
	if agraBlock != nil {
		if !agraBlock.IsUint64() {
			return nil, errors.New("agraBlock overflow")
		}
		agraBlockU64 := agraBlock.Uint64()
		res.agraBlock = &agraBlockU64
	}
	if cancunTime != nil {
		if !cancunTime.IsUint64() {
			return nil, errors.New("cancunTime overflow")
		}
		cancunTimeU64 := cancunTime.Uint64()
		res.cancunTime = &cancunTimeU64
	}

	return res, nil
}

func (p *TxPool) Start(ctx context.Context, db kv.RwDB) error {
	if p.started.Load() {
		return nil
	}

	return db.View(ctx, func(tx kv.Tx) error {
		coreDb, _ := p.coreDBWithCache()
		coreTx, err := coreDb.BeginRo(ctx)

		if err != nil {
			return err
		}

		defer coreTx.Rollback()

		if err := p.fromDB(ctx, tx, coreTx); err != nil {
			return fmt.Errorf("loading pool from DB: %w", err)
		}

		if p.started.CompareAndSwap(false, true) {
			p.logger.Info("[txpool] Started")
		}

		return nil
	})
}

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, unwindBlobTxs, minedTxs types.TxSlots, tx kv.Tx) error {
	defer newBlockTimer.ObserveDuration(time.Now())
	//t := time.Now()

	coreDB, cache := p.coreDBWithCache()
	cache.OnNewBlock(stateChanges)
	coreTx, err := coreDB.BeginRo(ctx)

	if err != nil {
		return err
	}

	defer coreTx.Rollback()

	block := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHeight
	baseFee := stateChanges.PendingBlockBaseFee
	available := len(p.pending.best.ms)

	defer func() {
		p.logger.Debug("[txpool] New block", "block", block, "unwound", len(unwindTxs.Txs), "mined", len(minedTxs.Txs), "baseFee", baseFee, "pending-pre", available, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len(), "err", err)
	}()

	if err = minedTxs.Valid(); err != nil {
		return err
	}

	cacheView, err := cache.View(ctx, coreTx)

	if err != nil {
		return err
	}

	p.lock.Lock()
	defer func() {
		if err == nil {
			p.lastSeenBlock.Store(block)
			p.lastSeenCond.Broadcast()
		}

		p.lock.Unlock()
	}()

	if assert.Enable {
		if _, err := kvcache.AssertCheckValues(ctx, coreTx, cache); err != nil {
			p.logger.Error("AssertCheckValues", "err", err, "stack", stack.Trace().String())
		}
	}

	pendingBaseFee, baseFeeChanged := p.setBaseFee(baseFee)
	// Update pendingBase for all pool queues and slices
	if baseFeeChanged {
		p.pending.best.pendingBaseFee = pendingBaseFee
		p.pending.worst.pendingBaseFee = pendingBaseFee
		p.baseFee.best.pendingBastFee = pendingBaseFee
		p.baseFee.worst.pendingBaseFee = pendingBaseFee
		p.queued.best.pendingBastFee = pendingBaseFee
		p.queued.worst.pendingBaseFee = pendingBaseFee
	}

	pendingBlobFee := stateChanges.PendingBlobFeePerGas
	p.setBlobFee(pendingBlobFee)

	oldGasLimit := p.blockGasLimit.Swap(stateChanges.BlockGasLimit)
	if oldGasLimit != stateChanges.BlockGasLimit {
		p.all.ascendAll(func(mt *metaTx) bool {
			var updated bool
			if mt.Tx.Gas < stateChanges.BlockGasLimit {
				updated = (mt.subPool & NotTooMuchGas) > 0
				mt.subPool |= NotTooMuchGas
			} else {
				updated = (mt.subPool & NotTooMuchGas) == 0
				mt.subPool &^= NotTooMuchGas
			}

			if mt.Tx.Traced {
				p.logger.Info("TX TRACING: on block gas limit update", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "senderId", mt.Tx.SenderID, "nonce", mt.Tx.Nonce, "subPool", mt.currentSubPool, "updated", updated)
			}

			if !updated {
				return true
			}

			switch mt.currentSubPool {
			case PendingSubPool:
				p.pending.Updated(mt)
			case BaseFeeSubPool:
				p.baseFee.Updated(mt)
			case QueuedSubPool:
				p.queued.Updated(mt)
			}
			return true
		})
	}

	for i, txn := range unwindBlobTxs.Txs {
		if txn.Type == types.BlobTxType {
			knownBlobTxn, err := p.getCachedBlobTxnLocked(coreTx, txn.IDHash[:])
			if err != nil {
				return err
			}
			if knownBlobTxn != nil {
				unwindTxs.Append(knownBlobTxn.Tx, unwindBlobTxs.Senders.At(i), false)
			}
		}
	}
	if err = p.senders.onNewBlock(stateChanges, unwindTxs, minedTxs, p.logger); err != nil {
		return err
	}

	_, unwindTxs, err = p.validateTxs(&unwindTxs, cacheView)

	if err != nil {
		return err
	}

	if assert.Enable {
		for _, txn := range unwindTxs.Txs {
			if txn.SenderID == 0 {
				panic(fmt.Errorf("onNewBlock.unwindTxs: senderID can't be zero"))
			}
		}
		for _, txn := range minedTxs.Txs {
			if txn.SenderID == 0 {
				panic(fmt.Errorf("onNewBlock.minedTxs: senderID can't be zero"))
			}
		}
	}

	if err = p.processMinedFinalizedBlobs(coreTx, minedTxs.Txs, stateChanges.FinalizedBlock); err != nil {
		return err
	}

	if err = p.removeMined(p.all, minedTxs.Txs); err != nil {
		return err
	}

	var announcements types.Announcements

	announcements, err = p.addTxsOnNewBlock(block, cacheView, stateChanges, p.senders, unwindTxs, /* newTxs */
		pendingBaseFee, stateChanges.BlockGasLimit, p.logger)

	if err != nil {
		return err
	}

	p.pending.EnforceWorstInvariants()
	p.baseFee.EnforceInvariants()
	p.queued.EnforceInvariants()
	p.promote(pendingBaseFee, pendingBlobFee, &announcements, p.logger)
	p.pending.EnforceBestInvariants()
	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	if p.promoted.Len() > 0 {
		select {
		case p.newPendingTxs <- p.promoted.Copy():
		default:
		}
	}

	return nil
}

func (p *TxPool) processRemoteTxs(ctx context.Context) error {
	if !p.Started() {
		return fmt.Errorf("txpool not started yet")
	}

	defer processBatchTxsTimer.ObserveDuration(time.Now())
	coreDB, cache := p.coreDBWithCache()
	coreTx, err := coreDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()
	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	//t := time.Now()
	p.lock.Lock()
	defer p.lock.Unlock()

	l := len(p.unprocessedRemoteTxs.Txs)
	if l == 0 {
		return nil
	}

	err = p.senders.registerNewSenders(p.unprocessedRemoteTxs, p.logger)
	if err != nil {
		return err
	}

	_, newTxs, err := p.validateTxs(p.unprocessedRemoteTxs, cacheView)
	if err != nil {
		return err
	}

	announcements, _, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs,
		p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), p.blockGasLimit.Load(), true, p.logger)
	if err != nil {
		return err
	}
	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	if p.promoted.Len() > 0 {
		select {
		case <-ctx.Done():
			return nil
		case p.newPendingTxs <- p.promoted.Copy():
		default:
		}
	}

	p.unprocessedRemoteTxs.Resize(0)
	p.unprocessedRemoteByHash = map[string]int{}

	//p.logger.Info("[txpool] on new txs", "amount", len(newPendingTxs.txs), "in", time.Since(t))
	return nil
}
func (p *TxPool) getRlpLocked(tx kv.Tx, hash []byte) (rlpTxn []byte, sender common.Address, isLocal bool, err error) {
	txn, ok := p.byHash[string(hash)]
	if ok && txn.Tx.Rlp != nil {
		return txn.Tx.Rlp, p.senders.senderID2Addr[txn.Tx.SenderID], txn.subPool&IsLocal > 0, nil
	}
	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, common.Address{}, false, err
	}
	if v == nil {
		return nil, common.Address{}, false, nil
	}
	return v[20:], *(*[20]byte)(v[:20]), txn != nil && txn.subPool&IsLocal > 0, nil
}
func (p *TxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	rlpTx, _, _, err := p.getRlpLocked(tx, hash)
	return common.Copy(rlpTx), err
}
func (p *TxPool) AppendLocalAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal == 0 {
			continue
		}
		types = append(types, txn.Tx.Type)
		sizes = append(sizes, txn.Tx.Size)
		hashes = append(hashes, hash...)
	}
	return types, sizes, hashes
}
func (p *TxPool) AppendRemoteAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal != 0 {
			continue
		}
		types = append(types, txn.Tx.Type)
		sizes = append(sizes, txn.Tx.Size)
		hashes = append(hashes, hash...)
	}
	for hash, txIdx := range p.unprocessedRemoteByHash {
		txSlot := p.unprocessedRemoteTxs.Txs[txIdx]
		types = append(types, txSlot.Type)
		sizes = append(sizes, txSlot.Size)
		hashes = append(hashes, hash...)
	}
	return types, sizes, hashes
}
func (p *TxPool) AppendAllAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	types, sizes, hashes = p.AppendLocalAnnouncements(types, sizes, hashes)
	types, sizes, hashes = p.AppendRemoteAnnouncements(types, sizes, hashes)
	return types, sizes, hashes
}
func (p *TxPool) idHashKnown(tx kv.Tx, hash []byte, hashS string) (bool, error) {
	if _, ok := p.unprocessedRemoteByHash[hashS]; ok {
		return true, nil
	}
	if _, ok := p.discardReasonsLRU.Get(hashS); ok {
		return true, nil
	}
	if _, ok := p.byHash[hashS]; ok {
		return true, nil
	}
	if _, ok := p.minedBlobTxsByHash[hashS]; ok {
		return true, nil
	}
	return tx.Has(kv.PoolTransaction, hash)
}
func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	hashS := string(hash)
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.idHashKnown(tx, hash, hashS)
}
func (p *TxPool) FilterKnownIdHashes(tx kv.Tx, hashes types.Hashes) (unknownHashes types.Hashes, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := 0; i < len(hashes); i += 32 {
		known, err := p.idHashKnown(tx, hashes[i:i+32], string(hashes[i:i+32]))
		if err != nil {
			return unknownHashes, err
		}
		if !known {
			unknownHashes = append(unknownHashes, hashes[i:i+32]...)
		}
	}
	return unknownHashes, err
}

func (p *TxPool) getUnprocessedTxn(hashS string) (*types.TxSlot, bool) {
	if i, ok := p.unprocessedRemoteByHash[hashS]; ok {
		return p.unprocessedRemoteTxs.Txs[i], true
	}
	return nil, false
}

func (p *TxPool) getCachedBlobTxnLocked(tx kv.Tx, hash []byte) (*metaTx, error) {
	hashS := string(hash)
	if mt, ok := p.minedBlobTxsByHash[hashS]; ok {
		return mt, nil
	}
	if txn, ok := p.getUnprocessedTxn(hashS); ok {
		return newMetaTx(txn, false, 0), nil
	}
	if mt, ok := p.byHash[hashS]; ok {
		return mt, nil
	}
	has, err := tx.Has(kv.PoolTransaction, hash)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}

	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, err
	}
	txRlp := common.Copy(v[20:])
	parseCtx := types.NewTxParseContext(p.chainID)
	parseCtx.WithSender(false)
	txSlot := &types.TxSlot{}
	parseCtx.ParseTransaction(txRlp, 0, txSlot, nil, false, true, nil)
	return newMetaTx(txSlot, false, 0), nil
}

func (p *TxPool) IsLocal(idHash []byte) bool {
	hashS := string(idHash)
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.isLocalLRU.Contains(hashS)
}
func (p *TxPool) AddNewGoodPeer(peerID types.PeerID) { p.recentlyConnectedPeers.AddPeer(peerID) }
func (p *TxPool) Started() bool                      { return p.started.Load() }

func (p *TxPool) best(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64, yielded mapset.Set[[32]byte]) (bool, int, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for last := p.lastSeenBlock.Load(); last < onTopOf; last = p.lastSeenBlock.Load() {
		p.logger.Debug("[txpool] Waiting for block", "expecting", onTopOf, "lastSeen", last, "txRequested", n, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
		p.lastSeenCond.Wait()
	}

	best := p.pending.best

	isShanghai := p.isShanghai() || p.isAgra()

	txs.Resize(uint(cmp.Min(int(n), len(best.ms))))
	var toRemove []*metaTx
	count := 0
	i := 0

	defer func() {
		p.logger.Debug("[txpool] Processing best request", "last", onTopOf, "txRequested", n, "txAvailable", len(best.ms), "txProcessed", i, "txReturned", count)
	}()

	for ; count < int(n) && i < len(best.ms); i++ {
		// if we wouldn't have enough gas for a standard transaction then quit out early
		if availableGas < fixedgas.TxGas {
			break
		}

		mt := best.ms[i]

		if yielded.Contains(mt.Tx.IDHash) {
			continue
		}

		if mt.Tx.Gas >= p.blockGasLimit.Load() {
			// Skip transactions with very large gas limit
			continue
		}

		rlpTx, sender, isLocal, err := p.getRlpLocked(tx, mt.Tx.IDHash[:])
		if err != nil {
			return false, count, err
		}
		if len(rlpTx) == 0 {
			toRemove = append(toRemove, mt)
			continue
		}

		// Skip transactions that require more blob gas than is available
		blobCount := uint64(len(mt.Tx.BlobHashes))
		if blobCount*fixedgas.BlobGasPerBlob > availableBlobGas {
			continue
		}
		availableBlobGas -= blobCount * fixedgas.BlobGasPerBlob

		// make sure we have enough gas in the caller to add this transaction.
		// not an exact science using intrinsic gas but as close as we could hope for at
		// this stage
		intrinsicGas, _ := txpoolcfg.CalcIntrinsicGas(uint64(mt.Tx.DataLen), uint64(mt.Tx.DataNonZeroLen), 0, nil, mt.Tx.Creation, true, true, isShanghai)
		if intrinsicGas > availableGas {
			// we might find another TX with a low enough intrinsic gas to include so carry on
			continue
		}
		availableGas -= intrinsicGas

		txs.Txs[count] = rlpTx
		copy(txs.Senders.At(count), sender.Bytes())
		txs.IsLocal[count] = isLocal
		yielded.Add(mt.Tx.IDHash)
		count++
	}

	txs.Resize(uint(count))
	if len(toRemove) > 0 {
		for _, mt := range toRemove {
			p.pending.Remove(mt, "best", p.logger)
		}
	}
	return true, count, nil
}

func (p *TxPool) YieldBest(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64, toSkip mapset.Set[[32]byte]) (bool, int, error) {
	return p.best(n, txs, tx, onTopOf, availableGas, availableBlobGas, toSkip)
}

func (p *TxPool) PeekBest(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64) (bool, error) {
	set := mapset.NewThreadUnsafeSet[[32]byte]()
	onTime, _, err := p.YieldBest(n, txs, tx, onTopOf, availableGas, availableBlobGas, set)
	return onTime, err
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pending.Len(), p.baseFee.Len(), p.queued.Len()
}
func (p *TxPool) AddRemoteTxs(_ context.Context, newTxs types.TxSlots) {
	if p.cfg.NoGossip {
		// if no gossip, then
		// disable adding remote transactions
		// consume remote tx from fetch
		return
	}

	defer addRemoteTxsTimer.ObserveDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	for i, txn := range newTxs.Txs {
		hashS := string(txn.IDHash[:])
		_, ok := p.unprocessedRemoteByHash[hashS]
		if ok {
			continue
		}
		p.unprocessedRemoteByHash[hashS] = len(p.unprocessedRemoteTxs.Txs)
		p.unprocessedRemoteTxs.Append(txn, newTxs.Senders.At(i), false)
	}
}

func toBlobs(_blobs [][]byte) []gokzg4844.Blob {
	blobs := make([]gokzg4844.Blob, len(_blobs))
	for i, _blob := range _blobs {
		var b gokzg4844.Blob
		copy(b[:], _blob)
		blobs[i] = b
	}
	return blobs
}

func (p *TxPool) validateTx(txn *types.TxSlot, isLocal bool, stateCache kvcache.CacheView) txpoolcfg.DiscardReason {
	isShanghai := p.isShanghai() || p.isAgra()
	if isShanghai && txn.Creation && txn.DataLen > fixedgas.MaxInitCodeSize {
		return txpoolcfg.InitCodeTooLarge // EIP-3860
	}
	if txn.Type == types.BlobTxType {
		if !p.isCancun() {
			return txpoolcfg.TypeNotActivated
		}
		if txn.Creation {
			return txpoolcfg.CreateBlobTxn
		}
		blobCount := uint64(len(txn.BlobHashes))
		if blobCount == 0 {
			return txpoolcfg.NoBlobs
		}
		if blobCount > p.maxBlobsPerBlock {
			return txpoolcfg.TooManyBlobs
		}
		equalNumber := len(txn.BlobHashes) == len(txn.Blobs) &&
			len(txn.Blobs) == len(txn.Commitments) &&
			len(txn.Commitments) == len(txn.Proofs)

		if !equalNumber {
			return txpoolcfg.UnequalBlobTxExt
		}

		for i := 0; i < len(txn.Commitments); i++ {
			if libkzg.KZGToVersionedHash(txn.Commitments[i]) != libkzg.VersionedHash(txn.BlobHashes[i]) {
				return txpoolcfg.BlobHashCheckFail
			}
		}

		// https://github.com/ethereum/consensus-specs/blob/017a8495f7671f5fff2075a9bfc9238c1a0982f8/specs/deneb/polynomial-commitments.md#verify_blob_kzg_proof_batch
		kzgCtx := libkzg.Ctx()
		err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(txn.Blobs), txn.Commitments, txn.Proofs)
		if err != nil {
			return txpoolcfg.UnmatchedBlobTxExt
		}

		if !isLocal && (p.all.blobCount(txn.SenderID)+uint64(len(txn.BlobHashes))) > p.cfg.BlobSlots {
			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: validateTx marked as spamming (too many blobs) idHash=%x slots=%d, limit=%d", txn.IDHash, p.all.count(txn.SenderID), p.cfg.AccountSlots))
			}
			return txpoolcfg.Spammer
		}
		if p.totalBlobsInPool.Load() >= p.cfg.TotalBlobPoolLimit {
			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: validateTx total blobs limit reached in pool limit=%x current blobs=%d", p.cfg.TotalBlobPoolLimit, p.totalBlobsInPool.Load()))
			}
			return txpoolcfg.BlobPoolOverflow
		}
	}

	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && uint256.NewInt(p.cfg.MinFeeCap).Cmp(&txn.FeeCap) == 1 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx underpriced idHash=%x local=%t, feeCap=%d, cfg.MinFeeCap=%d", txn.IDHash, isLocal, txn.FeeCap, p.cfg.MinFeeCap))
		}
		return txpoolcfg.UnderPriced
	}
	gas, reason := txpoolcfg.CalcIntrinsicGas(uint64(txn.DataLen), uint64(txn.DataNonZeroLen), 0, nil, txn.Creation, true, true, isShanghai)
	if txn.Traced {
		p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas idHash=%x gas=%d", txn.IDHash, gas))
	}
	if reason != txpoolcfg.Success {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas calculated failed idHash=%x reason=%s", txn.IDHash, reason))
		}
		return reason
	}
	if gas > txn.Gas {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas > txn.gas idHash=%x gas=%d, txn.gas=%d", txn.IDHash, gas, txn.Gas))
		}
		return txpoolcfg.IntrinsicGas
	}
	if !isLocal && uint64(p.all.count(txn.SenderID)) > p.cfg.AccountSlots {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx marked as spamming idHash=%x slots=%d, limit=%d", txn.IDHash, p.all.count(txn.SenderID), p.cfg.AccountSlots))
		}
		return txpoolcfg.Spammer
	}

	// Check nonce and balance
	senderNonce, senderBalance, _ := p.senders.info(stateCache, txn.SenderID)
	if senderNonce > txn.Nonce {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx nonce too low idHash=%x nonce in state=%d, txn.nonce=%d", txn.IDHash, senderNonce, txn.Nonce))
		}
		return txpoolcfg.NonceTooLow
	}
	// Transactor should have enough funds to cover the costs
	total := requiredBalance(txn)
	if senderBalance.Cmp(total) < 0 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx insufficient funds idHash=%x balance in state=%d, txn.gas*txn.tip=%d", txn.IDHash, senderBalance, total))
		}
		return txpoolcfg.InsufficientFunds
	}
	return txpoolcfg.Success
}

var maxUint256 = new(uint256.Int).SetAllOne()

// Sender should have enough balance for: gasLimit x feeCap + blobGas x blobFeeCap + transferred_value
// See YP, Eq (61) in Section 6.2 "Execution"
func requiredBalance(txn *types.TxSlot) *uint256.Int {
	// See https://github.com/ethereum/EIPs/pull/3594
	total := uint256.NewInt(txn.Gas)
	_, overflow := total.MulOverflow(total, &txn.FeeCap)
	if overflow {
		return maxUint256
	}
	// and https://eips.ethereum.org/EIPS/eip-4844#gas-accounting
	blobCount := uint64(len(txn.BlobHashes))
	if blobCount != 0 {
		maxBlobGasCost := uint256.NewInt(fixedgas.BlobGasPerBlob)
		maxBlobGasCost.Mul(maxBlobGasCost, uint256.NewInt(blobCount))
		_, overflow = maxBlobGasCost.MulOverflow(maxBlobGasCost, &txn.BlobFeeCap)
		if overflow {
			return maxUint256
		}
		_, overflow = total.AddOverflow(total, maxBlobGasCost)
		if overflow {
			return maxUint256
		}
	}

	_, overflow = total.AddOverflow(total, &txn.Value)
	if overflow {
		return maxUint256
	}
	return total
}

func (p *TxPool) isShanghai() bool {
	// once this flag has been set for the first time we no longer need to check the timestamp
	set := p.isPostShanghai.Load()
	if set {
		return true
	}
	if p.shanghaiTime == nil {
		return false
	}
	shanghaiTime := *p.shanghaiTime

	// a zero here means Shanghai is always active
	if shanghaiTime == 0 {
		p.isPostShanghai.Swap(true)
		return true
	}

	now := time.Now().Unix()
	activated := uint64(now) >= shanghaiTime
	if activated {
		p.isPostShanghai.Swap(true)
	}
	return activated
}

func (p *TxPool) isAgra() bool {
	// once this flag has been set for the first time we no longer need to check the block
	set := p.isPostAgra.Load()
	if set {
		return true
	}
	if p.agraBlock == nil {
		return false
	}
	agraBlock := *p.agraBlock

	// a zero here means Agra is always active
	if agraBlock == 0 {
		p.isPostAgra.Swap(true)
		return true
	}

	tx, err := p._chainDB.BeginRo(context.Background())
	if err != nil {
		return false
	}
	defer tx.Rollback()

	head_block, err := chain.CurrentBlockNumber(tx)
	if head_block == nil || err != nil {
		return false
	}
	// A new block is built on top of the head block, so when the head is agraBlock-1,
	// the new block should use the Agra rules.
	activated := (*head_block + 1) >= agraBlock
	if activated {
		p.isPostAgra.Swap(true)
	}
	return activated
}

func (p *TxPool) isCancun() bool {
	// once this flag has been set for the first time we no longer need to check the timestamp
	set := p.isPostCancun.Load()
	if set {
		return true
	}
	if p.cancunTime == nil {
		return false
	}
	cancunTime := *p.cancunTime

	// a zero here means Cancun is always active
	if cancunTime == 0 {
		p.isPostCancun.Swap(true)
		return true
	}

	now := time.Now().Unix()
	activated := uint64(now) >= cancunTime
	if activated {
		p.isPostCancun.Swap(true)
	}
	return activated
}

// Check that the serialized txn should not exceed a certain max size
func (p *TxPool) ValidateSerializedTxn(serializedTxn []byte) error {
	const (
		// txSlotSize is used to calculate how many data slots a single transaction
		// takes up based on its size. The slots are used as DoS protection, ensuring
		// that validating a new transaction remains a constant operation (in reality
		// O(maxslots), where max slots are 4 currently).
		txSlotSize = 32 * 1024

		// txMaxSize is the maximum size a single transaction can have. This field has
		// non-trivial consequences: larger transactions are significantly harder and
		// more expensive to propagate; larger transactions also take more resources
		// to validate whether they fit into the pool or not.
		txMaxSize = 4 * txSlotSize // 128KB

		// Should be enough for a transaction with 6 blobs
		blobTxMaxSize = 800_000
	)
	txType, err := types.PeekTransactionType(serializedTxn)
	if err != nil {
		return err
	}
	maxSize := txMaxSize
	if txType == types.BlobTxType {
		maxSize = blobTxMaxSize
	}
	if len(serializedTxn) > maxSize {
		return types.ErrRlpTooBig
	}
	return nil
}

func (p *TxPool) validateTxs(txs *types.TxSlots, stateCache kvcache.CacheView) (reasons []txpoolcfg.DiscardReason, goodTxs types.TxSlots, err error) {
	// reasons is pre-sized for direct indexing, with the default zero
	// value DiscardReason of NotSet
	reasons = make([]txpoolcfg.DiscardReason, len(txs.Txs))

	if err := txs.Valid(); err != nil {
		return reasons, goodTxs, err
	}

	goodCount := 0
	for i, txn := range txs.Txs {
		reason := p.validateTx(txn, txs.IsLocal[i], stateCache)
		if reason == txpoolcfg.Success {
			goodCount++
			// Success here means no DiscardReason yet, so leave it NotSet
			continue
		}
		if reason == txpoolcfg.Spammer {
			p.punishSpammer(txn.SenderID)
		}
		reasons[i] = reason
	}

	goodTxs.Resize(uint(goodCount))

	j := 0
	for i, txn := range txs.Txs {
		if reasons[i] == txpoolcfg.NotSet {
			goodTxs.Txs[j] = txn
			goodTxs.IsLocal[j] = txs.IsLocal[i]
			copy(goodTxs.Senders.At(j), txs.Senders.At(i))
			j++
		}
	}
	return reasons, goodTxs, nil
}

// punishSpammer by drop half of it's transactions with high nonce
func (p *TxPool) punishSpammer(spammer uint64) {
	count := p.all.count(spammer) / 2
	if count > 0 {
		txsToDelete := make([]*metaTx, 0, count)
		p.all.descend(spammer, func(mt *metaTx) bool {
			txsToDelete = append(txsToDelete, mt)
			count--
			return count > 0
		})

		for _, mt := range txsToDelete {
			switch mt.currentSubPool {
			case PendingSubPool:
				p.pending.Remove(mt, "punishSpammer", p.logger)
			case BaseFeeSubPool:
				p.baseFee.Remove(mt, "punishSpammer", p.logger)
			case QueuedSubPool:
				p.queued.Remove(mt, "punishSpammer", p.logger)
			default:
				//already removed
			}

			p.discardLocked(mt, txpoolcfg.Spammer) // can't call it while iterating by all
		}
	}
}

func fillDiscardReasons(reasons []txpoolcfg.DiscardReason, newTxs types.TxSlots, discardReasonsLRU *simplelru.LRU[string, txpoolcfg.DiscardReason]) []txpoolcfg.DiscardReason {
	for i := range reasons {
		if reasons[i] != txpoolcfg.NotSet {
			continue
		}
		reason, ok := discardReasonsLRU.Get(string(newTxs.Txs[i].IDHash[:]))
		if ok {
			reasons[i] = reason
		} else {
			reasons[i] = txpoolcfg.Success
		}
	}
	return reasons
}

func (p *TxPool) AddLocalTxs(ctx context.Context, newTransactions types.TxSlots, tx kv.Tx) ([]txpoolcfg.DiscardReason, error) {
	coreDb, cache := p.coreDBWithCache()
	coreTx, err := coreDb.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer coreTx.Rollback()

	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if err = p.senders.registerNewSenders(&newTransactions, p.logger); err != nil {
		return nil, err
	}

	reasons, newTxs, err := p.validateTxs(&newTransactions, cacheView)
	if err != nil {
		return nil, err
	}

	announcements, addReasons, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs,
		p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), p.blockGasLimit.Load(), true, p.logger)
	if err == nil {
		for i, reason := range addReasons {
			if reason != txpoolcfg.NotSet {
				reasons[i] = reason
			}
		}
	} else {
		return nil, err
	}
	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	reasons = fillDiscardReasons(reasons, newTxs, p.discardReasonsLRU)
	for i, reason := range reasons {
		if reason == txpoolcfg.Success {
			txn := newTxs.Txs[i]
			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: AddLocalTxs promotes idHash=%x, senderId=%d", txn.IDHash, txn.SenderID))
			}
			p.promoted.Append(txn.Type, txn.Size, txn.IDHash[:])
		}
	}
	if p.promoted.Len() > 0 {
		select {
		case p.newPendingTxs <- p.promoted.Copy():
		default:
		}
	}
	return reasons, nil
}
func (p *TxPool) coreDBWithCache() (kv.RoDB, kvcache.Cache) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._chainDB, p._stateCache
}

func (p *TxPool) addTxs(blockNum uint64, cacheView kvcache.CacheView, senders *sendersBatch,
	newTxs types.TxSlots, pendingBaseFee, pendingBlobFee, blockGasLimit uint64, collect bool, logger log.Logger) (types.Announcements, []txpoolcfg.DiscardReason, error) {
	if assert.Enable {
		for _, txn := range newTxs.Txs {
			if txn.SenderID == 0 {
				panic(fmt.Errorf("senderID can't be zero"))
			}
		}
	}

	// This can be thought of a reverse operation from the one described before.
	// When a block that was deemed "the best" of its height, is no longer deemed "the best", the
	// transactions contained in it, are now viable for inclusion in other blocks, and therefore should
	// be returned into the transaction pool.
	// An interesting note here is that if the block contained any transactions local to the node,
	// by being first removed from the pool (from the "local" part of it), and then re-injected,
	// they effective lose their priority over the "remote" transactions. In order to prevent that,
	// somehow the fact that certain transactions were local, needs to be remembered for some
	// time (up to some "immutability threshold").
	sendersWithChangedState := map[uint64]struct{}{}
	discardReasons := make([]txpoolcfg.DiscardReason, len(newTxs.Txs))
	announcements := types.Announcements{}
	for i, txn := range newTxs.Txs {
		if found, ok := p.byHash[string(txn.IDHash[:])]; ok {
			discardReasons[i] = txpoolcfg.DuplicateHash
			// In case if the transition is stuck, "poke" it to rebroadcast
			if collect && newTxs.IsLocal[i] && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.Tx.Type, found.Tx.Size, found.Tx.IDHash[:])
			}
			continue
		}
		mt := newMetaTx(txn, newTxs.IsLocal[i], blockNum)
		if reason := p.addLocked(mt, &announcements); reason != txpoolcfg.NotSet {
			discardReasons[i] = reason
			continue
		}
		discardReasons[i] = txpoolcfg.NotSet // unnecessary
		if txn.Traced {
			logger.Info(fmt.Sprintf("TX TRACING: schedule sendersWithChangedState idHash=%x senderId=%d", txn.IDHash, mt.Tx.SenderID))
		}
		sendersWithChangedState[mt.Tx.SenderID] = struct{}{}
	}

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return announcements, discardReasons, err
		}
		p.onSenderStateChange(senderID, nonce, balance, blockGasLimit, logger)
	}

	p.promote(pendingBaseFee, pendingBlobFee, &announcements, logger)
	p.pending.EnforceBestInvariants()

	return announcements, discardReasons, nil
}

// TODO: Looks like a copy of the above
func (p *TxPool) addTxsOnNewBlock(blockNum uint64, cacheView kvcache.CacheView, stateChanges *remote.StateChangeBatch,
	senders *sendersBatch, newTxs types.TxSlots, pendingBaseFee uint64, blockGasLimit uint64, logger log.Logger) (types.Announcements, error) {
	if assert.Enable {
		for _, txn := range newTxs.Txs {
			if txn.SenderID == 0 {
				panic(fmt.Errorf("senderID can't be zero"))
			}
		}
	}
	// This can be thought of a reverse operation from the one described before.
	// When a block that was deemed "the best" of its height, is no longer deemed "the best", the
	// transactions contained in it, are now viable for inclusion in other blocks, and therefore should
	// be returned into the transaction pool.
	// An interesting note here is that if the block contained any transactions local to the node,
	// by being first removed from the pool (from the "local" part of it), and then re-injected,
	// they effective lose their priority over the "remote" transactions. In order to prevent that,
	// somehow the fact that certain transactions were local, needs to be remembered for some
	// time (up to some "immutability threshold").
	sendersWithChangedState := map[uint64]struct{}{}
	announcements := types.Announcements{}
	for i, txn := range newTxs.Txs {
		if _, ok := p.byHash[string(txn.IDHash[:])]; ok {
			continue
		}
		mt := newMetaTx(txn, newTxs.IsLocal[i], blockNum)
		if reason := p.addLocked(mt, &announcements); reason != txpoolcfg.NotSet {
			p.discardLocked(mt, reason)
			continue
		}
		sendersWithChangedState[mt.Tx.SenderID] = struct{}{}
	}
	// add senders changed in state to `sendersWithChangedState` list
	for _, changesList := range stateChanges.ChangeBatch {
		for _, change := range changesList.Changes {
			switch change.Action {
			case remote.Action_UPSERT, remote.Action_UPSERT_CODE:
				if change.Incarnation > 0 {
					continue
				}
				addr := gointerfaces.ConvertH160toAddress(change.Address)
				id, ok := senders.getID(addr)
				if !ok {
					continue
				}
				sendersWithChangedState[id] = struct{}{}
			}
		}
	}

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return announcements, err
		}
		p.onSenderStateChange(senderID, nonce, balance, blockGasLimit, logger)
	}

	return announcements, nil
}

func (p *TxPool) setBaseFee(baseFee uint64) (uint64, bool) {
	changed := false
	if baseFee > 0 {
		changed = baseFee != p.pendingBaseFee.Load()
		p.pendingBaseFee.Store(baseFee)
	}
	return p.pendingBaseFee.Load(), changed
}

func (p *TxPool) setBlobFee(blobFee uint64) {
	if blobFee > 0 {
		p.pendingBlobFee.Store(blobFee)
	}
}

func (p *TxPool) addLocked(mt *metaTx, announcements *types.Announcements) txpoolcfg.DiscardReason {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.all.get(mt.Tx.SenderID, mt.Tx.Nonce)
	if found != nil {
		if found.Tx.Type == types.BlobTxType && mt.Tx.Type != types.BlobTxType {
			return txpoolcfg.BlobTxReplace
		}
		priceBump := p.cfg.PriceBump

		//Blob txn threshold checks for replace txn
		if mt.Tx.Type == types.BlobTxType {
			priceBump = p.cfg.BlobPriceBump
			blobFeeThreshold, overflow := (&uint256.Int{}).MulDivOverflow(
				&found.Tx.BlobFeeCap,
				uint256.NewInt(100+priceBump),
				uint256.NewInt(100),
			)
			if mt.Tx.BlobFeeCap.Lt(blobFeeThreshold) && !overflow {
				if bytes.Equal(found.Tx.IDHash[:], mt.Tx.IDHash[:]) {
					return txpoolcfg.NotSet
				}
				return txpoolcfg.ReplaceUnderpriced // TODO: This is the same as NotReplaced
			}
		}

		//Regular txn threshold checks
		tipThreshold := uint256.NewInt(0)
		tipThreshold = tipThreshold.Mul(&found.Tx.Tip, uint256.NewInt(100+priceBump))
		tipThreshold.Div(tipThreshold, u256.N100)
		feecapThreshold := uint256.NewInt(0)
		feecapThreshold.Mul(&found.Tx.FeeCap, uint256.NewInt(100+priceBump))
		feecapThreshold.Div(feecapThreshold, u256.N100)
		if mt.Tx.Tip.Cmp(tipThreshold) < 0 || mt.Tx.FeeCap.Cmp(feecapThreshold) < 0 {
			// Both tip and feecap need to be larger than previously to replace the transaction
			// In case if the transition is stuck, "poke" it to rebroadcast
			if mt.subPool&IsLocal != 0 && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.Tx.Type, found.Tx.Size, found.Tx.IDHash[:])
			}
			if bytes.Equal(found.Tx.IDHash[:], mt.Tx.IDHash[:]) {
				return txpoolcfg.NotSet
			}
			return txpoolcfg.NotReplaced
		}

		switch found.currentSubPool {
		case PendingSubPool:
			p.pending.Remove(found, "add", p.logger)
		case BaseFeeSubPool:
			p.baseFee.Remove(found, "add", p.logger)
		case QueuedSubPool:
			p.queued.Remove(found, "add", p.logger)
		default:
			//already removed
		}

		p.discardLocked(found, txpoolcfg.ReplacedByHigherTip)
	}

	// Don't add blob tx to queued if it's less than current pending blob base fee
	if mt.Tx.Type == types.BlobTxType && mt.Tx.BlobFeeCap.LtUint64(p.pendingBlobFee.Load()) {
		return txpoolcfg.FeeTooLow
	}

	hashStr := string(mt.Tx.IDHash[:])
	p.byHash[hashStr] = mt

	if replaced := p.all.replaceOrInsert(mt, p.logger); replaced != nil {
		if assert.Enable {
			panic("must never happen")
		}
	}

	if mt.subPool&IsLocal != 0 {
		p.isLocalLRU.Add(hashStr, struct{}{})
	}
	// All transactions are first added to the queued pool and then immediately promoted from there if required
	p.queued.Add(mt, "addLocked", p.logger)
	if mt.Tx.Type == types.BlobTxType {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t + (uint64(len(mt.Tx.BlobHashes))))
	}

	// Remove from mined cache as we are now "resurrecting" it to a sub-pool
	p.deleteMinedBlobTxn(hashStr)
	return txpoolcfg.NotSet
}

// dropping transaction from all sub-structures and from db
// Important: don't call it while iterating by all
func (p *TxPool) discardLocked(mt *metaTx, reason txpoolcfg.DiscardReason) {
	hashStr := string(mt.Tx.IDHash[:])
	delete(p.byHash, hashStr)
	p.deletedTxs = append(p.deletedTxs, mt)
	p.all.delete(mt, reason, p.logger)
	p.discardReasonsLRU.Add(hashStr, reason)
	if mt.Tx.Type == types.BlobTxType {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t - uint64(len(mt.Tx.BlobHashes)))
	}
}

// Cache recently mined blobs in anticipation of reorg, delete finalized ones
func (p *TxPool) processMinedFinalizedBlobs(coreTx kv.Tx, minedTxs []*types.TxSlot, finalizedBlock uint64) error {
	p.lastFinalizedBlock.Store(finalizedBlock)
	// Remove blobs in the finalized block and older, loop through all entries
	for l := len(p.minedBlobTxsByBlock); l > 0 && finalizedBlock > 0; l-- {
		// delete individual hashes
		for _, mt := range p.minedBlobTxsByBlock[finalizedBlock] {
			delete(p.minedBlobTxsByHash, string(mt.Tx.IDHash[:]))
		}
		// delete the map entry for this block num
		delete(p.minedBlobTxsByBlock, finalizedBlock)
		// move on to older blocks, if present
		finalizedBlock--
	}

	// Add mined blobs
	minedBlock := p.lastSeenBlock.Load()
	p.minedBlobTxsByBlock[minedBlock] = make([]*metaTx, 0)
	for _, txn := range minedTxs {
		if txn.Type == types.BlobTxType {
			mt := &metaTx{Tx: txn, minedBlockNum: minedBlock}
			p.minedBlobTxsByBlock[minedBlock] = append(p.minedBlobTxsByBlock[minedBlock], mt)
			mt.bestIndex = len(p.minedBlobTxsByBlock[minedBlock]) - 1
			p.minedBlobTxsByHash[string(txn.IDHash[:])] = mt
		}
	}
	return nil
}

// Delete individual hash entries from minedBlobTxs cache
func (p *TxPool) deleteMinedBlobTxn(hash string) {
	mt, exists := p.minedBlobTxsByHash[hash]
	if !exists {
		return
	}
	l := len(p.minedBlobTxsByBlock[mt.minedBlockNum])
	if l > 1 {
		p.minedBlobTxsByBlock[mt.minedBlockNum][mt.bestIndex] = p.minedBlobTxsByBlock[mt.minedBlockNum][l-1]
	}
	p.minedBlobTxsByBlock[mt.minedBlockNum] = p.minedBlobTxsByBlock[mt.minedBlockNum][:l-1]
	delete(p.minedBlobTxsByHash, hash)
}

func (p *TxPool) NonceFromAddress(addr [20]byte) (nonce uint64, inPool bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	senderID, found := p.senders.getID(addr)
	if !found {
		return 0, false
	}
	return p.all.nonce(senderID)
}

// removeMined - apply new highest block (or batch of blocks)
//
// 1. New best block arrives, which potentially changes the balance and the nonce of some senders.
// We use senderIds data structure to find relevant senderId values, and then use senders data structure to
// modify state_balance and state_nonce, potentially remove some elements (if transaction with some nonce is
// included into a block), and finally, walk over the transaction records and update SubPool fields depending on
// the actual presence of nonce gaps and what the balance is.
func (p *TxPool) removeMined(byNonce *BySenderAndNonce, minedTxs []*types.TxSlot) error {
	noncesToRemove := map[uint64]uint64{}
	for _, txn := range minedTxs {
		nonce, ok := noncesToRemove[txn.SenderID]
		if !ok || txn.Nonce > nonce {
			noncesToRemove[txn.SenderID] = txn.Nonce
		}
	}

	var toDel []*metaTx // can't delete items while iterate them

	discarded := 0
	pendingRemoved := 0
	baseFeeRemoved := 0
	queuedRemoved := 0

	for senderID, nonce := range noncesToRemove {
		byNonce.ascend(senderID, func(mt *metaTx) bool {
			if mt.Tx.Nonce > nonce {
				if mt.Tx.Traced {
					p.logger.Debug("[txpool] removing mined, cmp nonces", "tx.nonce", mt.Tx.Nonce, "sender.nonce", nonce)
				}

				return false
			}

			if mt.Tx.Traced {
				p.logger.Info("TX TRACING: removeMined", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "senderId", mt.Tx.SenderID, "nonce", mt.Tx.Nonce, "currentSubPool", mt.currentSubPool)
			}

			toDel = append(toDel, mt)
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				pendingRemoved++
				p.pending.Remove(mt, "remove-mined", p.logger)
			case BaseFeeSubPool:
				baseFeeRemoved++
				p.baseFee.Remove(mt, "remove-mined", p.logger)
			case QueuedSubPool:
				queuedRemoved++
				p.queued.Remove(mt, "remove-mined", p.logger)
			default:
				//already removed
			}
			return true
		})

		discarded += len(toDel)

		for _, mt := range toDel {
			p.discardLocked(mt, txpoolcfg.Mined)
		}
		toDel = toDel[:0]
	}

	if discarded > 0 {
		p.logger.Debug("Discarded transactions", "count", discarded, "pending", pendingRemoved, "baseFee", baseFeeRemoved, "queued", queuedRemoved)
	}

	return nil
}

// onSenderStateChange is the function that recalculates ephemeral fields of transactions and determines
// which sub pool they will need to go to. Since this depends on other transactions from the same sender by with lower
// nonces, and also affect other transactions from the same sender with higher nonce, it loops through all transactions
// for a given senderID
func (p *TxPool) onSenderStateChange(senderID uint64, senderNonce uint64, senderBalance uint256.Int, blockGasLimit uint64, logger log.Logger) {
	noGapsNonce := senderNonce
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint256.NewInt(0).SetAllOne()
	minTip := uint64(math.MaxUint64)
	var toDel []*metaTx // can't delete items while iterate them

	p.all.ascend(senderID, func(mt *metaTx) bool {
		deleteAndContinueReasonLog := ""
		if senderNonce > mt.Tx.Nonce {
			deleteAndContinueReasonLog = "low nonce"
		} else if mt.Tx.Nonce != noGapsNonce && mt.Tx.Type == types.BlobTxType { // Discard nonce-gapped blob txns
			deleteAndContinueReasonLog = "nonce-gapped blob txn"
		}
		if deleteAndContinueReasonLog != "" {
			if mt.Tx.Traced {
				logger.Info("TX TRACING: onSenderStateChange loop iteration remove", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "senderID", senderID, "senderNonce", senderNonce, "txn.nonce", mt.Tx.Nonce, "currentSubPool", mt.currentSubPool, "reason", deleteAndContinueReasonLog)
			}
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				p.pending.Remove(mt, deleteAndContinueReasonLog, p.logger)
			case BaseFeeSubPool:
				p.baseFee.Remove(mt, deleteAndContinueReasonLog, p.logger)
			case QueuedSubPool:
				p.queued.Remove(mt, deleteAndContinueReasonLog, p.logger)
			default:
				//already removed
			}
			toDel = append(toDel, mt)
			return true
		}

		if minFeeCap.Gt(&mt.Tx.FeeCap) {
			*minFeeCap = mt.Tx.FeeCap
		}
		mt.minFeeCap = *minFeeCap
		if mt.Tx.Tip.IsUint64() {
			minTip = cmp.Min(minTip, mt.Tx.Tip.Uint64())
		}
		mt.minTip = minTip

		mt.nonceDistance = 0
		if mt.Tx.Nonce > senderNonce { // no uint underflow
			mt.nonceDistance = mt.Tx.Nonce - senderNonce
		}

		needBalance := requiredBalance(mt.Tx)

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.Tx.Nonce {
			mt.subPool |= NoNonceGaps
			noGapsNonce++
		}

		// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the
		// state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the
		// sum of feeCap x gasLimit + transferred_value of all transactions from this sender with
		// nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is
		// set if there is currently a guarantee that the transaction and all its required prior
		// transactions will be able to pay for gas.
		mt.subPool &^= EnoughBalance
		mt.cumulativeBalanceDistance = math.MaxUint64
		if mt.Tx.Nonce >= senderNonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce <= sender.nonce
			if senderBalance.Gt(cumulativeRequiredBalance) || senderBalance.Eq(cumulativeRequiredBalance) {
				mt.subPool |= EnoughBalance
			} else {
				if cumulativeRequiredBalance.IsUint64() && senderBalance.IsUint64() {
					mt.cumulativeBalanceDistance = cumulativeRequiredBalance.Uint64() - senderBalance.Uint64()
				}
			}
		}

		mt.subPool &^= NotTooMuchGas
		if mt.Tx.Gas < blockGasLimit {
			mt.subPool |= NotTooMuchGas
		}

		if mt.Tx.Traced {
			logger.Info("TX TRACING: onSenderStateChange loop iteration update", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "senderId", mt.Tx.SenderID, "nonce", mt.Tx.Nonce, "subPool", mt.currentSubPool)
		}

		// Some fields of mt might have changed, need to fix the invariants in the subpool best and worst queues
		switch mt.currentSubPool {
		case PendingSubPool:
			p.pending.Updated(mt)
		case BaseFeeSubPool:
			p.baseFee.Updated(mt)
		case QueuedSubPool:
			p.queued.Updated(mt)
		}
		return true
	})

	for _, mt := range toDel {
		p.discardLocked(mt, txpoolcfg.NonceTooLow)
	}

	logger.Trace("[txpool] onSenderStateChange", "sender", senderID, "count", p.all.count(senderID), "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
}

// promote reasserts invariants of the subpool and returns the list of transactions that ended up
// being promoted to the pending or basefee pool, for re-broadcasting
func (p *TxPool) promote(pendingBaseFee uint64, pendingBlobFee uint64, announcements *types.Announcements, logger log.Logger) {
	// Demote worst transactions that do not qualify for pending sub pool anymore, to other sub pools, or discard
	for worst := p.pending.Worst(); p.pending.Len() > 0 && (worst.subPool < BaseFeePoolBits || worst.minFeeCap.LtUint64(pendingBaseFee) || (worst.Tx.Type == types.BlobTxType && worst.Tx.BlobFeeCap.LtUint64(pendingBlobFee))); worst = p.pending.Worst() {
		if worst.subPool >= BaseFeePoolBits {
			tx := p.pending.PopWorst()
			announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
			p.baseFee.Add(tx, "demote-pending", logger)
		} else {
			p.queued.Add(p.pending.PopWorst(), "demote-pending", logger)
		}
	}

	// Promote best transactions from base fee pool to pending pool while they qualify
	for best := p.baseFee.Best(); p.baseFee.Len() > 0 && best.subPool >= BaseFeePoolBits && best.minFeeCap.CmpUint64(pendingBaseFee) >= 0 && (best.Tx.Type != types.BlobTxType || best.Tx.BlobFeeCap.CmpUint64(pendingBlobFee) >= 0); best = p.baseFee.Best() {
		tx := p.baseFee.PopBest()
		announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
		p.pending.Add(tx, logger)
	}

	// Demote worst transactions that do not qualify for base fee pool anymore, to queued sub pool, or discard
	for worst := p.baseFee.Worst(); p.baseFee.Len() > 0 && worst.subPool < BaseFeePoolBits; worst = p.baseFee.Worst() {
		p.queued.Add(p.baseFee.PopWorst(), "demote-base", logger)
	}

	// Promote best transactions from the queued pool to either pending or base fee pool, while they qualify
	for best := p.queued.Best(); p.queued.Len() > 0 && best.subPool >= BaseFeePoolBits; best = p.queued.Best() {
		if best.minFeeCap.Cmp(uint256.NewInt(pendingBaseFee)) >= 0 {
			tx := p.queued.PopBest()
			announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
			p.pending.Add(tx, logger)
		} else {
			p.baseFee.Add(p.queued.PopBest(), "promote-queued", logger)
		}
	}

	// Discard worst transactions from the queued sub pool if they do not qualify
	// <FUNCTIONALITY REMOVED>

	// Discard worst transactions from pending pool until it is within capacity limit
	for p.pending.Len() > p.pending.limit {
		p.discardLocked(p.pending.PopWorst(), txpoolcfg.PendingPoolOverflow)
	}

	// Discard worst transactions from pending sub pool until it is within capacity limits
	for p.baseFee.Len() > p.baseFee.limit {
		p.discardLocked(p.baseFee.PopWorst(), txpoolcfg.BaseFeePoolOverflow)
	}

	// Discard worst transactions from the queued sub pool until it is within its capacity limits
	for _ = p.queued.Worst(); p.queued.Len() > p.queued.limit; _ = p.queued.Worst() {
		p.discardLocked(p.queued.PopWorst(), txpoolcfg.QueuedPoolOverflow)
	}
}

// txMaxBroadcastSize is the max size of a transaction that will be broadcasted.
// All transactions with a higher size will be announced and need to be fetched
// by the peer.
const txMaxBroadcastSize = 4 * 1024

// MainLoop - does:
// send pending byHash to p2p:
//   - new byHash
//   - all pooled byHash to recently connected peers
//   - all local pooled byHash to random peers periodically
//
// promote/demote transactions
// reorgs
func MainLoop(ctx context.Context, db kv.RwDB, p *TxPool, newTxs chan types.Announcements, send *Send, newSlotsStreams *NewSlotsStreams, notifyMiningAboutNewSlots func()) {
	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxsEvery := time.NewTicker(p.cfg.ProcessRemoteTxsEvery)
	defer processRemoteTxsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	err := p.Start(ctx, db)

	if err != nil {
		p.logger.Error("[txpool] Failed to start", "err", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			_, _ = p.flush(ctx, db)
			return
		case <-logEvery.C:
			p.logStats()
		case <-processRemoteTxsEvery.C:
			if !p.Started() {
				continue
			}

			if err := p.processRemoteTxs(ctx); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue
				}

				p.logger.Error("[txpool] process batch remote txs", "err", err)
			}
		case <-commitEvery.C:
			if db != nil && p.Started() {
				t := time.Now()
				written, err := p.flush(ctx, db)
				if err != nil {
					p.logger.Error("[txpool] flush is local history", "err", err)
					continue
				}
				writeToDBBytesCounter.SetUint64(written)
				p.logger.Debug("[txpool] Commit", "written_kb", written/1024, "in", time.Since(t))
			}
		case announcements := <-newTxs:
			go func() {
				for i := 0; i < 16; i++ { // drain more events from channel, then merge and dedup them
					select {
					case a := <-newTxs:
						announcements.AppendOther(a)
						continue
					default:
					}
					break
				}
				if announcements.Len() == 0 {
					return
				}
				defer propagateNewTxsTimer.ObserveDuration(time.Now())

				announcements = announcements.DedupCopy()

				notifyMiningAboutNewSlots()

				if p.cfg.NoGossip {
					// drain newTxs for emptying newTx channel
					// newTx channel will be filled only with local transactions
					// early return to avoid outbound transaction propagation
					log.Debug("[txpool] tx gossip disabled", "state", "drain new transactions")
					return
				}

				var localTxTypes []byte
				var localTxSizes []uint32
				var localTxHashes types.Hashes
				var localTxRlps [][]byte
				var remoteTxTypes []byte
				var remoteTxSizes []uint32
				var remoteTxHashes types.Hashes
				var remoteTxRlps [][]byte
				var broadcastHashes types.Hashes
				slotsRlp := make([][]byte, 0, announcements.Len())

				if err := db.View(ctx, func(tx kv.Tx) error {
					for i := 0; i < announcements.Len(); i++ {
						t, size, hash := announcements.At(i)
						slotRlp, err := p.GetRlp(tx, hash)
						if err != nil {
							return err
						}
						if len(slotRlp) == 0 {
							continue
						}
						// Strip away blob wrapper, if applicable
						slotRlp, err2 := types2.UnwrapTxPlayloadRlp(slotRlp)
						if err2 != nil {
							continue
						}

						// Empty rlp can happen if a transaction we want to broadcast has just been mined, for example
						slotsRlp = append(slotsRlp, slotRlp)
						if p.IsLocal(hash) {
							localTxTypes = append(localTxTypes, t)
							localTxSizes = append(localTxSizes, size)
							localTxHashes = append(localTxHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if t != types.BlobTxType {
								localTxRlps = append(localTxRlps, slotRlp)
								broadcastHashes = append(broadcastHashes, hash...)
							}
						} else {
							remoteTxTypes = append(remoteTxTypes, t)
							remoteTxSizes = append(remoteTxSizes, size)
							remoteTxHashes = append(remoteTxHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if t != types.BlobTxType && len(slotRlp) < txMaxBroadcastSize {
								remoteTxRlps = append(remoteTxRlps, slotRlp)
							}
						}
					}
					return nil
				}); err != nil {
					p.logger.Error("[txpool] collect info to propagate", "err", err)
					return
				}
				if newSlotsStreams != nil {
					newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: slotsRlp}, p.logger)
				}

				// broadcast local transactions
				const localTxsBroadcastMaxPeers uint64 = 10
				txSentTo := send.BroadcastPooledTxs(localTxRlps, localTxsBroadcastMaxPeers)
				for i, peer := range txSentTo {
					p.logger.Trace("Local tx broadcast", "txHash", hex.EncodeToString(broadcastHashes.At(i)), "to peer", peer)
				}
				hashSentTo := send.AnnouncePooledTxs(localTxTypes, localTxSizes, localTxHashes, localTxsBroadcastMaxPeers*2)
				for i := 0; i < localTxHashes.Len(); i++ {
					hash := localTxHashes.At(i)
					p.logger.Trace("Local tx announced", "txHash", hex.EncodeToString(hash), "to peer", hashSentTo[i], "baseFee", p.pendingBaseFee.Load())
				}

				// broadcast remote transactions
				const remoteTxsBroadcastMaxPeers uint64 = 3
				send.BroadcastPooledTxs(remoteTxRlps, remoteTxsBroadcastMaxPeers)
				send.AnnouncePooledTxs(remoteTxTypes, remoteTxSizes, remoteTxHashes, remoteTxsBroadcastMaxPeers*2)
			}()
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			if p.cfg.NoGossip {
				// avoid transaction gossiping for new peers
				log.Debug("[txpool] tx gossip disabled", "state", "sync new peers")
				continue
			}
			t := time.Now()
			var hashes types.Hashes
			var types []byte
			var sizes []uint32
			types, sizes, hashes = p.AppendAllAnnouncements(types, sizes, hashes[:0])
			go send.PropagatePooledTxsToPeersList(newPeers, types, sizes, hashes)
			propagateToNewPeerTimer.ObserveDuration(t)
		}
	}
}

func (p *TxPool) flushNoFsync(ctx context.Context, db kv.RwDB) (written uint64, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db tx is done inside lock, to make last writes visible for all read operations
	if err := db.UpdateNosync(ctx, func(tx kv.RwTx) error {
		err = p.flushLocked(tx)
		if err != nil {
			return err
		}
		written, _, err = tx.(*mdbx.MdbxTx).SpaceDirty()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return written, nil
}

func (p *TxPool) flush(ctx context.Context, db kv.RwDB) (written uint64, err error) {
	defer writeToDBTimer.ObserveDuration(time.Now())
	// 1. get global lock on txpool and flush it to db, without fsync (to release lock asap)
	// 2. then fsync db without txpool lock
	written, err = p.flushNoFsync(ctx, db)
	if err != nil {
		return 0, err
	}

	// fsync. increase state version - just to make RwTx non-empty (mdbx skips empty RwTx)
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		v, err := tx.GetOne(kv.PoolInfo, PoolStateVersion)
		if err != nil {
			return err
		}
		var version uint64
		if len(v) == 8 {
			version = binary.BigEndian.Uint64(v)
		}
		version++
		return tx.Put(kv.PoolInfo, PoolStateVersion, hexutility.EncodeTs(version))
	}); err != nil {
		return 0, err
	}
	return written, nil
}

func (p *TxPool) flushLocked(tx kv.RwTx) (err error) {
	for i, mt := range p.deletedTxs {
		id := mt.Tx.SenderID
		idHash := mt.Tx.IDHash[:]
		if !p.all.hasTxs(id) {
			addr, ok := p.senders.senderID2Addr[id]
			if ok {
				delete(p.senders.senderID2Addr, id)
				delete(p.senders.senderIDs, addr)
			}
		}
		//fmt.Printf("del:%d,%d,%d\n", mt.Tx.senderID, mt.Tx.nonce, mt.Tx.tip)
		has, err := tx.Has(kv.PoolTransaction, idHash)
		if err != nil {
			return err
		}
		if has {
			if err := tx.Delete(kv.PoolTransaction, idHash); err != nil {
				return err
			}
		}
		p.deletedTxs[i] = nil // for gc
	}

	txHashes := p.isLocalLRU.Keys()
	encID := make([]byte, 8)
	if err := tx.ClearBucket(kv.RecentLocalTransaction); err != nil {
		return err
	}
	for i, txHash := range txHashes {
		binary.BigEndian.PutUint64(encID, uint64(i))
		if err := tx.Append(kv.RecentLocalTransaction, encID, []byte(txHash)); err != nil {
			return err
		}
	}

	v := make([]byte, 0, 1024)
	for txHash, metaTx := range p.byHash {
		if metaTx.Tx.Rlp == nil {
			continue
		}
		v = common.EnsureEnoughSize(v, 20+len(metaTx.Tx.Rlp))

		addr, ok := p.senders.senderID2Addr[metaTx.Tx.SenderID]
		if !ok {
			p.logger.Warn("[txpool] flush: sender address not found by ID", "senderID", metaTx.Tx.SenderID)
			continue
		}

		copy(v[:20], addr.Bytes())
		copy(v[20:], metaTx.Tx.Rlp)

		has, err := tx.Has(kv.PoolTransaction, []byte(txHash))
		if err != nil {
			return err
		}
		if !has {
			if err := tx.Put(kv.PoolTransaction, []byte(txHash), v); err != nil {
				return err
			}
		}
		metaTx.Tx.Rlp = nil
	}

	binary.BigEndian.PutUint64(encID, p.pendingBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolPendingBaseFeeKey, encID); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(encID, p.pendingBlobFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolPendingBlobFeeKey, encID); err != nil {
		return err
	}
	if err := PutLastSeenBlock(tx, p.lastSeenBlock.Load(), encID); err != nil {
		return err
	}

	// clean - in-memory data structure as later as possible - because if during this Tx will happen error,
	// DB will stay consistent but some in-memory structures may be already cleaned, and retry will not work
	// failed write transaction must not create side-effects
	p.deletedTxs = p.deletedTxs[:0]
	return nil
}

func (p *TxPool) fromDB(ctx context.Context, tx kv.Tx, coreTx kv.Tx) error {
	if p.lastSeenBlock.Load() == 0 {
		lastSeenBlock, err := LastSeenBlock(tx)
		if err != nil {
			return err
		}

		p.lastSeenBlock.Store(lastSeenBlock)
	}

	// this is necessary as otherwise best - which waits for sync events
	// may wait for ever if blocks have been process before the txpool
	// starts with an empty db
	lastSeenProgress, err := getExecutionProgress(coreTx)

	if err != nil {
		return err
	}

	if p.lastSeenBlock.Load() < lastSeenProgress {
		// TODO we need to process the blocks since the
		// last seen to make sure that the tx pool is in
		// sync with the processed blocks

		p.lastSeenBlock.Store(lastSeenProgress)
	}

	cacheView, err := p._stateCache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	it, err := tx.Range(kv.RecentLocalTransaction, nil, nil)
	if err != nil {
		return err
	}
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return err
		}
		p.isLocalLRU.Add(string(v), struct{}{})
	}

	txs := types.TxSlots{}
	parseCtx := types.NewTxParseContext(p.chainID)
	parseCtx.WithSender(false)

	i := 0
	it, err = tx.Range(kv.PoolTransaction, nil, nil)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		addr, txRlp := *(*[20]byte)(v[:20]), v[20:]
		txn := &types.TxSlot{}

		// TODO(eip-4844) ensure wrappedWithBlobs when transactions are saved to the DB
		_, err = parseCtx.ParseTransaction(txRlp, 0, txn, nil, false /* hasEnvelope */, true /*wrappedWithBlobs*/, nil)
		if err != nil {
			err = fmt.Errorf("err: %w, rlp: %x", err, txRlp)
			p.logger.Warn("[txpool] fromDB: parseTransaction", "err", err)
			continue
		}
		txn.Rlp = nil // means that we don't need store it in db anymore

		txn.SenderID, txn.Traced = p.senders.getOrCreateID(addr, p.logger)
		binary.BigEndian.Uint64(v) // TODO - unnecessary line, remove

		isLocalTx := p.isLocalLRU.Contains(string(k))

		if reason := p.validateTx(txn, isLocalTx, cacheView); reason != txpoolcfg.NotSet && reason != txpoolcfg.Success {
			return nil // TODO: Clarify - if one of the txs has the wrong reason, no pooled txs!
		}
		txs.Resize(uint(i + 1))
		txs.Txs[i] = txn
		txs.IsLocal[i] = isLocalTx
		copy(txs.Senders.At(i), addr[:])
		i++
	}

	var pendingBaseFee, pendingBlobFee, minBlobGasPrice, blockGasLimit uint64

	if p.feeCalculator != nil {
		if chainConfig, _ := ChainConfig(tx); chainConfig != nil {
			pendingBaseFee, pendingBlobFee, minBlobGasPrice, blockGasLimit, err = p.feeCalculator.CurrentFees(chainConfig, coreTx)
			if err != nil {
				return err
			}
		}
	}

	if pendingBaseFee == 0 {
		v, err := tx.GetOne(kv.PoolInfo, PoolPendingBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			pendingBaseFee = binary.BigEndian.Uint64(v)
		}
	}

	if pendingBlobFee == 0 {
		v, err := tx.GetOne(kv.PoolInfo, PoolPendingBlobFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			pendingBlobFee = binary.BigEndian.Uint64(v)
		}
	}

	if pendingBlobFee == 0 {
		pendingBlobFee = minBlobGasPrice
	}

	if blockGasLimit == 0 {
		blockGasLimit = DefaultBlockGasLimit
	}

	err = p.senders.registerNewSenders(&txs, p.logger)
	if err != nil {
		return err
	}
	if _, _, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, txs,
		pendingBaseFee, pendingBlobFee, blockGasLimit, false, p.logger); err != nil {
		return err
	}
	p.pendingBaseFee.Store(pendingBaseFee)
	p.pendingBlobFee.Store(pendingBlobFee)
	p.blockGasLimit.Store(blockGasLimit)
	return nil
}

func getExecutionProgress(db kv.Getter) (uint64, error) {
	data, err := db.GetOne(kv.SyncStageProgress, []byte("Execution"))
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return 0, nil
	}

	if len(data) < 8 {
		return 0, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	}

	return binary.BigEndian.Uint64(data[:8]), nil
}

func LastSeenBlock(tx kv.Getter) (uint64, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolLastSeenBlockKey)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}
func PutLastSeenBlock(tx kv.Putter, n uint64, buf []byte) error {
	buf = common.EnsureEnoughSize(buf, 8)
	binary.BigEndian.PutUint64(buf, n)
	err := tx.Put(kv.PoolInfo, PoolLastSeenBlockKey, buf)
	if err != nil {
		return err
	}
	return nil
}
func ChainConfig(tx kv.Getter) (*chain.Config, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolChainConfigKey)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	var config chain.Config
	if err := json.Unmarshal(v, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	return &config, nil
}
func PutChainConfig(tx kv.Putter, cc *chain.Config, buf []byte) error {
	wr := bytes.NewBuffer(buf)
	if err := json.NewEncoder(wr).Encode(cc); err != nil {
		return fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	if err := tx.Put(kv.PoolInfo, PoolChainConfigKey, wr.Bytes()); err != nil {
		return err
	}
	return nil
}

// nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%d\n", j.Tx.SenderID, j.Tx.Nonce, j.Tx.Tip)
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for _, mt := range p.pending.best.ms {
		mt.Tx.PrintDebug(fmt.Sprintf("%s.pending: %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.SenderID, mt.Tx.Nonce, mt.Tx.Tip))
	}
	for _, mt := range p.baseFee.best.ms {
		mt.Tx.PrintDebug(fmt.Sprintf("%s.baseFee : %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.SenderID, mt.Tx.Nonce, mt.Tx.Tip))
	}
	for _, mt := range p.queued.best.ms {
		mt.Tx.PrintDebug(fmt.Sprintf("%s.queued : %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.SenderID, mt.Tx.Nonce, mt.Tx.Tip))
	}
}
func (p *TxPool) logStats() {
	if !p.Started() {
		//p.logger.Info("[txpool] Not started yet, waiting for new blocks...")
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	ctx := []interface{}{
		//"block", p.lastSeenBlock.Load(),
		"pending", p.pending.Len(),
		"baseFee", p.baseFee.Len(),
		"queued", p.queued.Len(),
	}
	cacheKeys := p._stateCache.Len()
	if cacheKeys > 0 {
		ctx = append(ctx, "cache_keys", cacheKeys)
	}
	ctx = append(ctx, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	p.logger.Info("[txpool] stat", ctx...)
	pendingSubCounter.SetInt(p.pending.Len())
	basefeeSubCounter.SetInt(p.baseFee.Len())
	queuedSubCounter.SetInt(p.queued.Len())
}

// Deprecated need switch to streaming-like
func (p *TxPool) deprecatedForEach(_ context.Context, f func(rlp []byte, sender common.Address, t SubPoolType), tx kv.Tx) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.all.ascendAll(func(mt *metaTx) bool {
		slot := mt.Tx
		slotRlp := slot.Rlp
		if slot.Rlp == nil {
			v, err := tx.GetOne(kv.PoolTransaction, slot.IDHash[:])
			if err != nil {
				p.logger.Warn("[txpool] foreach: get tx from db", "err", err)
				return true
			}
			if v == nil {
				p.logger.Warn("[txpool] foreach: tx not found in db")
				return true
			}
			slotRlp = v[20:]
		}
		if sender, found := p.senders.senderID2Addr[slot.SenderID]; found {
			f(slotRlp, sender, mt.currentSubPool)
		}
		return true
	})
}

var PoolChainConfigKey = []byte("chain_config")
var PoolLastSeenBlockKey = []byte("last_seen_block")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")
var PoolPendingBlobFeeKey = []byte("pending_blob_fee")
var PoolStateVersion = []byte("state_version")

// recentlyConnectedPeers does buffer IDs of recently connected good peers
// then sync of pooled Transaction can happen to all of then at once
// DoS protection and performance saving
// it doesn't track if peer disconnected, it's fine
type recentlyConnectedPeers struct {
	peers []types.PeerID
	lock  sync.Mutex
}

func (l *recentlyConnectedPeers) AddPeer(p types.PeerID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []types.PeerID {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}

// nolint
func (sc *sendersBatch) printDebug(prefix string) {
	fmt.Printf("%s.sendersBatch.sender\n", prefix)
	//for i, j := range sc.senderInfo {
	//	fmt.Printf("\tid=%d,nonce=%d,balance=%d\n", i, j.nonce, j.balance.Uint64())
	//}
}

// sendersBatch stores in-memory senders-related objects - which are different from DB (updated/dirty)
// flushing to db periodically. it doesn't play as read-cache (because db is small and memory-mapped - doesn't need cache)
// non thread-safe
type sendersBatch struct {
	senderIDs     map[common.Address]uint64
	senderID2Addr map[uint64]common.Address
	tracedSenders map[common.Address]struct{}
	senderID      uint64
}

func newSendersCache(tracedSenders map[common.Address]struct{}) *sendersBatch {
	return &sendersBatch{senderIDs: map[common.Address]uint64{}, senderID2Addr: map[uint64]common.Address{}, tracedSenders: tracedSenders}
}

func (sc *sendersBatch) getID(addr common.Address) (uint64, bool) {
	id, ok := sc.senderIDs[addr]
	return id, ok
}
func (sc *sendersBatch) getOrCreateID(addr common.Address, logger log.Logger) (uint64, bool) {
	_, traced := sc.tracedSenders[addr]

	if !traced {
		traced = TraceAll
	}

	id, ok := sc.senderIDs[addr]
	if !ok {
		sc.senderID++
		id = sc.senderID
		sc.senderIDs[addr] = id
		sc.senderID2Addr[id] = addr
		if traced {
			logger.Info(fmt.Sprintf("TX TRACING: allocated senderID %d to sender %x", id, addr))
		}
	}
	return id, traced
}
func (sc *sendersBatch) info(cacheView kvcache.CacheView, id uint64) (nonce uint64, balance uint256.Int, err error) {
	addr, ok := sc.senderID2Addr[id]
	if !ok {
		panic("must not happen")
	}
	encoded, err := cacheView.Get(addr.Bytes())
	if err != nil {
		return 0, emptySender.balance, err
	}
	if len(encoded) == 0 {
		return emptySender.nonce, emptySender.balance, nil
	}
	nonce, balance, err = types.DecodeSender(encoded)
	if err != nil {
		return 0, emptySender.balance, err
	}
	return nonce, balance, nil
}

func (sc *sendersBatch) registerNewSenders(newTxs *types.TxSlots, logger log.Logger) (err error) {
	for i, txn := range newTxs.Txs {
		txn.SenderID, txn.Traced = sc.getOrCreateID(newTxs.Senders.AddressAt(i), logger)
	}
	return nil
}
func (sc *sendersBatch) onNewBlock(stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs types.TxSlots, logger log.Logger) error {
	for _, diff := range stateChanges.ChangeBatch {
		for _, change := range diff.Changes { // merge state changes
			addrB := gointerfaces.ConvertH160toAddress(change.Address)
			sc.getOrCreateID(addrB, logger)
		}

		for i, txn := range unwindTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(unwindTxs.Senders.AddressAt(i), logger)
		}

		for i, txn := range minedTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(minedTxs.Senders.AddressAt(i), logger)
		}
	}
	return nil
}

// BySenderAndNonce - designed to perform most expensive operation in TxPool:
// "recalculate all ephemeral fields of all transactions" by algo
//   - for all senders - iterate over all transactions in nonce growing order
//
// Performances decisions:
//   - All senders stored inside 1 large BTree - because iterate over 1 BTree is faster than over map[senderId]BTree
//   - sortByNonce used as non-pointer wrapper - because iterate over BTree of pointers is 2x slower
type BySenderAndNonce struct {
	tree              *btree.BTreeG[*metaTx]
	search            *metaTx
	senderIDTxnCount  map[uint64]int    // count of sender's txns in the pool - may differ from nonce
	senderIDBlobCount map[uint64]uint64 // count of sender's total number of blobs in the pool
}

func (b *BySenderAndNonce) nonce(senderID uint64) (nonce uint64, ok bool) {
	s := b.search
	s.Tx.SenderID = senderID
	s.Tx.Nonce = math.MaxUint64

	b.tree.DescendLessOrEqual(s, func(mt *metaTx) bool {
		if mt.Tx.SenderID == senderID {
			nonce = mt.Tx.Nonce
			ok = true
		}
		return false
	})
	return nonce, ok
}

func (b *BySenderAndNonce) ascendAll(f func(*metaTx) bool) {
	b.tree.Ascend(func(mt *metaTx) bool {
		return f(mt)
	})
}

func (b *BySenderAndNonce) ascend(senderID uint64, f func(*metaTx) bool) {
	s := b.search
	s.Tx.SenderID = senderID
	s.Tx.Nonce = 0
	b.tree.AscendGreaterOrEqual(s, func(mt *metaTx) bool {
		if mt.Tx.SenderID != senderID {
			return false
		}
		return f(mt)
	})
}

func (b *BySenderAndNonce) descend(senderID uint64, f func(*metaTx) bool) {
	s := b.search
	s.Tx.SenderID = senderID
	s.Tx.Nonce = math.MaxUint64
	b.tree.DescendLessOrEqual(s, func(mt *metaTx) bool {
		if mt.Tx.SenderID != senderID {
			return false
		}
		return f(mt)
	})
}

func (b *BySenderAndNonce) count(senderID uint64) int {
	return b.senderIDTxnCount[senderID]
}

func (b *BySenderAndNonce) blobCount(senderID uint64) uint64 {
	return b.senderIDBlobCount[senderID]
}

func (b *BySenderAndNonce) hasTxs(senderID uint64) bool {
	has := false
	b.ascend(senderID, func(*metaTx) bool {
		has = true
		return false
	})
	return has
}

func (b *BySenderAndNonce) get(senderID, txNonce uint64) *metaTx {
	s := b.search
	s.Tx.SenderID = senderID
	s.Tx.Nonce = txNonce
	if found, ok := b.tree.Get(s); ok {
		return found
	}
	return nil
}

// nolint
func (b *BySenderAndNonce) has(mt *metaTx) bool {
	return b.tree.Has(mt)
}

func (b *BySenderAndNonce) delete(mt *metaTx, reason txpoolcfg.DiscardReason, logger log.Logger) {
	if _, ok := b.tree.Delete(mt); ok {
		if mt.Tx.Traced {
			logger.Info("TX TRACING: Deleted tx by nonce", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "sender", mt.Tx.SenderID, "nonce", mt.Tx.Nonce, "reason", reason)
		}

		senderID := mt.Tx.SenderID
		count := b.senderIDTxnCount[senderID]
		if count > 1 {
			b.senderIDTxnCount[senderID] = count - 1
		} else {
			delete(b.senderIDTxnCount, senderID)
		}

		if mt.Tx.Type == types.BlobTxType && mt.Tx.Blobs != nil {
			accBlobCount := b.senderIDBlobCount[senderID]
			txnBlobCount := len(mt.Tx.Blobs)
			if txnBlobCount > 1 {
				b.senderIDBlobCount[senderID] = accBlobCount - uint64(txnBlobCount)
			} else {
				delete(b.senderIDBlobCount, senderID)
			}
		}
	}
}

func (b *BySenderAndNonce) replaceOrInsert(mt *metaTx, logger log.Logger) *metaTx {
	it, ok := b.tree.ReplaceOrInsert(mt)

	if ok {
		if mt.Tx.Traced {
			logger.Info("TX TRACING: Replaced tx by nonce", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "sender", mt.Tx.SenderID, "nonce", mt.Tx.Nonce)
		}
		return it
	}

	if mt.Tx.Traced {
		logger.Info("TX TRACING: Inserted tx by nonce", "idHash", fmt.Sprintf("%x", mt.Tx.IDHash), "sender", mt.Tx.SenderID, "nonce", mt.Tx.Nonce)
	}

	b.senderIDTxnCount[mt.Tx.SenderID]++
	if mt.Tx.Type == types.BlobTxType && mt.Tx.Blobs != nil {
		b.senderIDBlobCount[mt.Tx.SenderID] += uint64(len(mt.Tx.Blobs))
	}
	return nil
}

// PendingPool - is different from other pools - it's best is Slice instead of Heap
// It's more expensive to maintain "slice sort" invariant, but it allow do cheap copy of
// pending.best slice for mining (because we consider txs and metaTx are immutable)
type PendingPool struct {
	best  *bestSlice
	worst *WorstQueue
	limit int
	t     SubPoolType
}

func NewPendingSubPool(t SubPoolType, limit int) *PendingPool {
	return &PendingPool{limit: limit, t: t, best: &bestSlice{ms: []*metaTx{}}, worst: &WorstQueue{ms: []*metaTx{}}}
}

// bestSlice - is similar to best queue, but uses a linear structure with O(n log n) sort complexity and
// it maintains element.bestIndex field
type bestSlice struct {
	ms             []*metaTx
	pendingBaseFee uint64
}

func (s *bestSlice) Len() int { return len(s.ms) }
func (s *bestSlice) Swap(i, j int) {
	s.ms[i], s.ms[j] = s.ms[j], s.ms[i]
	s.ms[i].bestIndex, s.ms[j].bestIndex = i, j
}
func (s *bestSlice) Less(i, j int) bool {
	return s.ms[i].better(s.ms[j], *uint256.NewInt(s.pendingBaseFee))
}
func (s *bestSlice) UnsafeRemove(i *metaTx) {
	s.Swap(i.bestIndex, len(s.ms)-1)
	s.ms[len(s.ms)-1].bestIndex = -1
	s.ms[len(s.ms)-1] = nil
	s.ms = s.ms[:len(s.ms)-1]
}
func (s *bestSlice) UnsafeAdd(i *metaTx) {
	i.bestIndex = len(s.ms)
	s.ms = append(s.ms, i)
}

func (p *PendingPool) EnforceWorstInvariants() {
	heap.Init(p.worst)
}
func (p *PendingPool) EnforceBestInvariants() {
	sort.Sort(p.best)
}

func (p *PendingPool) Best() *metaTx { //nolint
	if len(p.best.ms) == 0 {
		return nil
	}
	return p.best.ms[0]
}
func (p *PendingPool) Worst() *metaTx { //nolint
	if len(p.worst.ms) == 0 {
		return nil
	}
	return (p.worst.ms)[0]
}
func (p *PendingPool) PopWorst() *metaTx { //nolint
	i := heap.Pop(p.worst).(*metaTx)
	if i.bestIndex >= 0 {
		p.best.UnsafeRemove(i)
	}
	return i
}
func (p *PendingPool) Updated(mt *metaTx) {
	heap.Fix(p.worst, mt.worstIndex)
}
func (p *PendingPool) Len() int { return len(p.best.ms) }

func (p *PendingPool) Remove(i *metaTx, reason string, logger log.Logger) {
	if i.Tx.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: removed from subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.Tx.IDHash), "sender", i.Tx.SenderID, "nonce", i.Tx.Nonce, "reason", reason)
	}
	if i.worstIndex >= 0 {
		heap.Remove(p.worst, i.worstIndex)
	}
	if i.bestIndex >= 0 {
		p.best.UnsafeRemove(i)
	}
	i.currentSubPool = 0
}

func (p *PendingPool) Add(i *metaTx, logger log.Logger) {
	if i.Tx.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: added to subpool %s, IdHash=%x, sender=%d, nonce=%d", p.t, i.Tx.IDHash, i.Tx.SenderID, i.Tx.Nonce))
	}
	i.currentSubPool = p.t
	heap.Push(p.worst, i)
	p.best.UnsafeAdd(i)
}
func (p *PendingPool) DebugPrint(prefix string) {
	for i, it := range p.best.ms {
		fmt.Printf("%s.best: %d, %d, %d,%d\n", prefix, i, it.subPool, it.bestIndex, it.Tx.Nonce)
	}
	for i, it := range p.worst.ms {
		fmt.Printf("%s.worst: %d, %d, %d,%d\n", prefix, i, it.subPool, it.worstIndex, it.Tx.Nonce)
	}
}

type SubPool struct {
	best  *BestQueue
	worst *WorstQueue
	limit int
	t     SubPoolType
}

func NewSubPool(t SubPoolType, limit int) *SubPool {
	return &SubPool{limit: limit, t: t, best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}
func (p *SubPool) Best() *metaTx { //nolint
	if len(p.best.ms) == 0 {
		return nil
	}
	return p.best.ms[0]
}
func (p *SubPool) Worst() *metaTx { //nolint
	if len(p.worst.ms) == 0 {
		return nil
	}
	return p.worst.ms[0]
}
func (p *SubPool) PopBest() *metaTx { //nolint
	i := heap.Pop(p.best).(*metaTx)
	heap.Remove(p.worst, i.worstIndex)
	return i
}
func (p *SubPool) PopWorst() *metaTx { //nolint
	i := heap.Pop(p.worst).(*metaTx)
	heap.Remove(p.best, i.bestIndex)
	return i
}
func (p *SubPool) Len() int { return p.best.Len() }
func (p *SubPool) Add(i *metaTx, reason string, logger log.Logger) {
	if i.Tx.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: added to subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.Tx.IDHash), "sender", i.Tx.SenderID, "nonce", i.Tx.Nonce, "reason", reason)
	}
	i.currentSubPool = p.t
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}

func (p *SubPool) Remove(i *metaTx, reason string, logger log.Logger) {
	if i.Tx.Traced {
		logger.Info(fmt.Sprintf("TX TRACING: removed from subpool %s", p.t), "idHash", fmt.Sprintf("%x", i.Tx.IDHash), "sender", i.Tx.SenderID, "nonce", i.Tx.Nonce, "reason", reason)
	}
	heap.Remove(p.best, i.bestIndex)
	heap.Remove(p.worst, i.worstIndex)
	i.currentSubPool = 0
}

func (p *SubPool) Updated(i *metaTx) {
	heap.Fix(p.best, i.bestIndex)
	heap.Fix(p.worst, i.worstIndex)
}

func (p *SubPool) DebugPrint(prefix string) {
	for i, it := range p.best.ms {
		fmt.Printf("%s.best: %d, %d, %d\n", prefix, i, it.subPool, it.bestIndex)
	}
	for i, it := range p.worst.ms {
		fmt.Printf("%s.worst: %d, %d, %d\n", prefix, i, it.subPool, it.worstIndex)
	}
}

type BestQueue struct {
	ms             []*metaTx
	pendingBastFee uint64
}

// Returns true if the txn "mt" is better than the parameter txn "than"
// it first compares the subpool markers of the two meta txns, then,
// (since they have the same subpool marker, and thus same pool)
// depending on the pool - pending (P), basefee (B), queued (Q) -
// it compares the effective tip (for P), nonceDistance (for both P,Q)
// minFeeCap (for B), and cumulative balance distance (for P, Q)
func (mt *metaTx) better(than *metaTx, pendingBaseFee uint256.Int) bool {
	subPool := mt.subPool
	thanSubPool := than.subPool
	if mt.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		subPool |= EnoughFeeCapBlock
	}
	if than.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		thanSubPool |= EnoughFeeCapBlock
	}
	if subPool != thanSubPool {
		return subPool > thanSubPool
	}

	switch mt.currentSubPool {
	case PendingSubPool:
		var effectiveTip, thanEffectiveTip uint256.Int
		if mt.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
			difference := uint256.NewInt(0)
			difference.Sub(&mt.minFeeCap, &pendingBaseFee)
			if difference.Cmp(uint256.NewInt(mt.minTip)) <= 0 {
				effectiveTip = *difference
			} else {
				effectiveTip = *uint256.NewInt(mt.minTip)
			}
		}
		if than.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
			difference := uint256.NewInt(0)
			difference.Sub(&than.minFeeCap, &pendingBaseFee)
			if difference.Cmp(uint256.NewInt(than.minTip)) <= 0 {
				thanEffectiveTip = *difference
			} else {
				thanEffectiveTip = *uint256.NewInt(than.minTip)
			}
		}
		if effectiveTip.Cmp(&thanEffectiveTip) != 0 {
			return effectiveTip.Cmp(&thanEffectiveTip) > 0
		}
		// Compare nonce and cumulative balance. Just as a side note, it doesn't
		// matter if they're from same sender or not because we're comparing
		// nonce distance of the sender from state's nonce and not the actual
		// value of nonce.
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance < than.cumulativeBalanceDistance
		}
	case BaseFeeSubPool:
		if mt.minFeeCap.Cmp(&than.minFeeCap) != 0 {
			return mt.minFeeCap.Cmp(&than.minFeeCap) > 0
		}
	case QueuedSubPool:
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance < than.cumulativeBalanceDistance
		}
	}
	return mt.timestamp < than.timestamp
}

func (mt *metaTx) worse(than *metaTx, pendingBaseFee uint256.Int) bool {
	subPool := mt.subPool
	thanSubPool := than.subPool
	if mt.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		subPool |= EnoughFeeCapBlock
	}
	if than.minFeeCap.Cmp(&pendingBaseFee) >= 0 {
		thanSubPool |= EnoughFeeCapBlock
	}
	if subPool != thanSubPool {
		return subPool < thanSubPool
	}

	switch mt.currentSubPool {
	case PendingSubPool:
		if mt.minFeeCap != than.minFeeCap {
			return mt.minFeeCap.Cmp(&than.minFeeCap) < 0
		}
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance > than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance > than.cumulativeBalanceDistance
		}
	case BaseFeeSubPool, QueuedSubPool:
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance > than.nonceDistance
		}
		if mt.cumulativeBalanceDistance != than.cumulativeBalanceDistance {
			return mt.cumulativeBalanceDistance > than.cumulativeBalanceDistance
		}
	}
	return mt.timestamp > than.timestamp
}

func (p BestQueue) Len() int { return len(p.ms) }
func (p BestQueue) Less(i, j int) bool {
	return p.ms[i].better(p.ms[j], *uint256.NewInt(p.pendingBastFee))
}
func (p BestQueue) Swap(i, j int) {
	p.ms[i], p.ms[j] = p.ms[j], p.ms[i]
	p.ms[i].bestIndex = i
	p.ms[j].bestIndex = j
}
func (p *BestQueue) Push(x interface{}) {
	n := len(p.ms)
	item := x.(*metaTx)
	item.bestIndex = n
	p.ms = append(p.ms, item)
}

func (p *BestQueue) Pop() interface{} {
	old := p.ms
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.bestIndex = -1     // for safety
	item.currentSubPool = 0 // for safety
	p.ms = old[0 : n-1]
	return item
}

type WorstQueue struct {
	ms             []*metaTx
	pendingBaseFee uint64
}

func (p WorstQueue) Len() int { return len(p.ms) }
func (p WorstQueue) Less(i, j int) bool {
	return p.ms[i].worse(p.ms[j], *uint256.NewInt(p.pendingBaseFee))
}
func (p WorstQueue) Swap(i, j int) {
	p.ms[i], p.ms[j] = p.ms[j], p.ms[i]
	p.ms[i].worstIndex = i
	p.ms[j].worstIndex = j
}
func (p *WorstQueue) Push(x interface{}) {
	n := len(p.ms)
	item := x.(*metaTx)
	item.worstIndex = n
	p.ms = append(p.ms, x.(*metaTx))
}
func (p *WorstQueue) Pop() interface{} {
	old := p.ms
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.worstIndex = -1    // for safety
	item.currentSubPool = 0 // for safety
	p.ms = old[0 : n-1]
	return item
}
