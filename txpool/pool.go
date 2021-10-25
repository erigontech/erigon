/*
   Copyright 2021 Erigon contributors

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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc/status"
)

var (
	processBatchTxsTimer    = metrics.NewSummary(`pool_process_remote_txs`)
	addRemoteTxsTimer       = metrics.NewSummary(`pool_add_remote_txs`)
	newBlockTimer           = metrics.NewSummary(`pool_new_block`)
	writeToDbTimer          = metrics.NewSummary(`pool_write_to_db`)
	propagateToNewPeerTimer = metrics.NewSummary(`pool_propagate_to_new_peer`)
	propagateNewTxsTimer    = metrics.NewSummary(`pool_propagate_new_txs`)
	writeToDbBytesCounter   = metrics.GetOrCreateCounter(`pool_write_to_db_bytes`)
)

const ASSERT = false

type Config struct {
	DBDir                 string
	SyncToNewPeersEvery   time.Duration
	ProcessRemoteTxsEvery time.Duration
	CommitEvery           time.Duration
	LogEvery              time.Duration

	PendingSubPoolLimit int
	BaseFeeSubPoolLimit int
	QueuedSubPoolLimit  int

	MinFeeCap    uint64
	AccountSlots uint64 // Number of executable transaction slots guaranteed per account
}

var DefaultConfig = Config{
	SyncToNewPeersEvery:   2 * time.Minute,
	ProcessRemoteTxsEvery: 100 * time.Millisecond,
	CommitEvery:           15 * time.Second,
	LogEvery:              30 * time.Second,

	PendingSubPoolLimit: 10_000,
	BaseFeeSubPoolLimit: 10_000,
	QueuedSubPoolLimit:  10_000,

	MinFeeCap:    1,
	AccountSlots: 16, //TODO: to choose right value (16 to be compat with Geth)
}

// Pool is interface for the transaction pool
// This interface exists for the convinience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	// Handle 3 main events - new remote txs from p2p, new local txs from RPC, new blocks from execution layer
	AddRemoteTxs(ctx context.Context, newTxs TxSlots)
	AddLocalTxs(ctx context.Context, newTxs TxSlots) ([]DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots, tx kv.Tx) error

	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)

	AddNewGoodPeer(peerID PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check

// SubPoolMarker ordered bitset responsible to sort transactions by sub-pools. Bits meaning:
// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means this transaction will never be included into this particular chain.
// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for the sender is M, and there are transactions for all nonces between M and N from the same sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
// 3. Sufficient balance for gas. Set to 1 if the balance of sender's account in the state is B, nonce of the sender in the state is M, nonce of the transaction is N, and the sum of feeCap x gasLimit + transferred_value of all transactions from this sender with nonces N+1 ... M is no more than B. Set to 0 otherwise. In other words, this bit is set if there is currently a guarantee that the transaction and all its required prior transactions will be able to pay for gas.
// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than baseFee of the currently pending block. Set to 0 otherwise.
// 5. Local transaction. Set to 1 if transaction is local.
type SubPoolMarker uint8

const (
	EnoughFeeCapProtocol = 0b10000
	NoNonceGaps          = 0b01000
	EnoughBalance        = 0b00100
	EnoughFeeCapBlock    = 0b00010
	IsLocal              = 0b00001
)

type DiscardReason uint8

const (
	NotSet              DiscardReason = 0 // analog of "nil-value", means it will be set in future
	Success             DiscardReason = 1
	AlreadyKnown        DiscardReason = 2
	Mined               DiscardReason = 3
	ReplacedByHigherTip DiscardReason = 4
	UnderPriced         DiscardReason = 5
	ReplaceUnderpriced  DiscardReason = 6 // if a transaction is attempted to be replaced with a different one without the required price bump.
	FeeTooLow           DiscardReason = 7
	OversizedData       DiscardReason = 8
	InvalidSender       DiscardReason = 9
	NegativeValue       DiscardReason = 10 // ensure no one is able to specify a transaction with a negative value.
	Spammer             DiscardReason = 11
	PendingPoolOverflow DiscardReason = 12
	BaseFeePoolOverflow DiscardReason = 13
	QueuedPoolOverflow  DiscardReason = 14
	GasUintOverflow     DiscardReason = 15
	IntrinsicGas        DiscardReason = 16
)

func (r DiscardReason) String() string {
	switch r {
	case NotSet:
		return "not set"
	case Success:
		return "success"
	case AlreadyKnown:
		return "already known"
	case Mined:
		return "mined"
	case ReplacedByHigherTip:
		return "replaced by transaction with higher tip"
	case UnderPriced:
		return "underpriced"
	case ReplaceUnderpriced:
		return "replacement transaction underpriced"
	case FeeTooLow:
		return "fee too low"
	case OversizedData:
		return "oversized data"
	case InvalidSender:
		return "invalid sender"
	case NegativeValue:
		return "negative value"
	case PendingPoolOverflow:
		return "pending sub-pool is full"
	case BaseFeePoolOverflow:
		return "baseFee sub-pool is full"
	case QueuedPoolOverflow:
		return "queued sub-pool is full"
	default:
		panic(fmt.Sprintf("discard reason: %d", r))
	}
}

// metaTx holds transaction and some metadata
type metaTx struct {
	Tx                        *TxSlot
	subPool                   SubPoolMarker
	nonceDistance             uint64 // how far their nonces are from the state's nonce for the sender
	cumulativeBalanceDistance uint64 // how far their cumulativeRequiredBalance are from the state's balance for the sender
	minFeeCap                 uint64
	effectiveTip              uint64 // max(minTip, minFeeCap - baseFee)
	bestIndex                 int
	worstIndex                int
	currentSubPool            SubPoolType
	timestamp                 uint64 // when it was added to pool
}

func newMetaTx(slot *TxSlot, isLocal bool, timestmap uint64) *metaTx {
	mt := &metaTx{Tx: slot, worstIndex: -1, bestIndex: -1, timestamp: timestmap}
	if isLocal {
		mt.subPool = IsLocal
	}
	return mt
}

type SubPoolType uint8

const PendingSubPool SubPoolType = 1
const BaseFeeSubPool SubPoolType = 2
const QueuedSubPool SubPoolType = 3

// sender - immutable structure which stores only nonce and balance of account
type sender struct {
	balance uint256.Int
	nonce   uint64
}

func newSender(nonce uint64, balance uint256.Int) *sender {
	return &sender{nonce: nonce, balance: balance}
}

var emptySender = newSender(0, *uint256.NewInt(0))

type sortByNonce struct{ *metaTx }

func (i sortByNonce) Less(than btree.Item) bool {
	if i.metaTx.Tx.senderID != than.(sortByNonce).metaTx.Tx.senderID {
		return i.metaTx.Tx.senderID < than.(sortByNonce).metaTx.Tx.senderID
	}
	return i.metaTx.Tx.nonce < than.(sortByNonce).metaTx.Tx.nonce
}

func calcProtocolBaseFee(baseFee uint64) uint64 {
	return 7
}

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// txpool doesn't start any goroutines - "leave concurrency to user" design
// txpool has no DB or TX fields - "leave db transactions management to user" design
// txpool has _chainDB field - but it must maximize local state cache hit-rate - and perform minimum _chainDB transactions
//
// It preserve TxSlot objects immutable
type TxPool struct {
	lock *sync.RWMutex

	started        atomic.Bool
	lastSeenBlock  atomic.Uint64
	pendingBaseFee atomic.Uint64

	// batch processing of remote transactions
	// handling works fast without batching, but batching allow:
	//   - reduce amount of _chainDB transactions
	//   - batch notifications about new txs (reduce P2P spam to other nodes about txs propagation)
	//   - and as a result reducing pool.RWLock contention
	unprocessedRemoteTxs    *TxSlots
	unprocessedRemoteByHash map[string]int // to reject duplicates

	byHash            map[string]*metaTx // tx_hash => tx : only not committed to db yet records
	discardReasonsLRU *simplelru.LRU     // tx_hash => discard_reason : non-persisted
	pending           *PendingPool
	baseFee, queued   *SubPool
	isLocalLRU        *simplelru.LRU    // tx_hash => is_local : to restore isLocal flag of unwinded transactions
	newPendingTxs     chan Hashes       // notifications about new txs in Pending sub-pool
	deletedTxs        []*metaTx         // list of discarded txs since last db commit
	all               *BySenderAndNonce // senderID => (sorted map of tx nonce => *metaTx)
	promoted          Hashes            // pre-allocated temporary buffer to write promoted to pending pool txn hashes
	_chainDB          kv.RoDB           // remote db - use it wisely
	_stateCache       kvcache.Cache
	cfg               Config

	recentlyConnectedPeers *recentlyConnectedPeers // all txs will be propagated to this peers eventually, and clear list
	senders                *sendersBatch

	chainID uint256.Int
}

func New(newTxs chan Hashes, coreDB kv.RoDB, cfg Config, cache kvcache.Cache, chainID uint256.Int) (*TxPool, error) {
	localsHistory, err := simplelru.NewLRU(10_000, nil)
	if err != nil {
		return nil, err
	}
	discardHistory, err := simplelru.NewLRU(10_000, nil)
	if err != nil {
		return nil, err
	}

	return &TxPool{
		lock:                    &sync.RWMutex{},
		byHash:                  map[string]*metaTx{},
		isLocalLRU:              localsHistory,
		discardReasonsLRU:       discardHistory,
		all:                     &BySenderAndNonce{tree: btree.New(32), search: sortByNonce{&metaTx{Tx: &TxSlot{}}}},
		recentlyConnectedPeers:  &recentlyConnectedPeers{},
		pending:                 NewPendingSubPool(PendingSubPool, cfg.PendingSubPoolLimit),
		baseFee:                 NewSubPool(BaseFeeSubPool, cfg.BaseFeeSubPoolLimit),
		queued:                  NewSubPool(QueuedSubPool, cfg.QueuedSubPoolLimit),
		newPendingTxs:           newTxs,
		_stateCache:             cache,
		senders:                 newSendersCache(),
		_chainDB:                coreDB,
		cfg:                     cfg,
		chainID:                 chainID,
		unprocessedRemoteTxs:    &TxSlots{},
		unprocessedRemoteByHash: map[string]int{},
		promoted:                make(Hashes, 0, 32*1024),
	}, nil
}

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots, tx kv.Tx) error {
	defer newBlockTimer.UpdateDuration(time.Now())
	//t := time.Now()

	cache := p.cache()
	cache.OnNewBlock(stateChanges)
	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()

	p.lock.Lock()
	defer p.lock.Unlock()

	p.lastSeenBlock.Store(stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHeight)
	if !p.started.Load() {
		if err := p.fromDB(ctx, tx, coreTx); err != nil {
			return err
		}
	}

	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	if ASSERT {
		if _, err := kvcache.AssertCheckValues(ctx, coreTx, cache); err != nil {
			log.Error("AssertCheckValues", "err", err, "stack", stack.Trace().String())
		}
	}

	_, unwindTxs, err = p.validateTxs(unwindTxs)
	if err != nil {
		return err
	}
	if err := minedTxs.Valid(); err != nil {
		return err
	}
	baseFee := stateChanges.PendingBlockBaseFee

	pendingBaseFee, baseFeeChanged := p.setBaseFee(baseFee)
	if err := p.senders.onNewBlock(stateChanges, unwindTxs, minedTxs); err != nil {
		return err
	}

	if ASSERT {
		for i := range unwindTxs.txs {
			if unwindTxs.txs[i].senderID == 0 {
				panic(fmt.Errorf("onNewBlock.unwindTxs: senderID can't be zero"))
			}
		}
		for i := range minedTxs.txs {
			if minedTxs.txs[i].senderID == 0 {
				panic(fmt.Errorf("onNewBlock.minedTxs: senderID can't be zero"))
			}
		}
	}

	if err := removeMined(p.all, minedTxs.txs, p.pending, p.baseFee, p.queued, p.discardLocked); err != nil {
		return err
	}

	//log.Debug("[txpool] new block", "unwinded", len(unwindTxs.txs), "mined", len(minedTxs.txs), "baseFee", baseFee, "blockHeight", blockHeight)

	p.pending.captureAddedHashes(&p.promoted)
	if err := addTxsOnNewBlock(p.lastSeenBlock.Load(), cacheView, stateChanges, p.senders, unwindTxs, pendingBaseFee, baseFeeChanged, p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}
	p.pending.added = nil

	if p.started.CAS(false, true) {
		log.Info("[txpool] Started")
	}

	if p.promoted.Len() > 0 {
		select {
		case p.newPendingTxs <- common.Copy(p.promoted):
		default:
		}
	}

	//log.Info("[txpool] new block", "number", p.lastSeenBlock.Load(), "in", time.Since(t))
	return nil
}

func (p *TxPool) processRemoteTxs(ctx context.Context) error {
	if !p.started.Load() {
		return fmt.Errorf("txpool not started yet")
	}

	cache := p.cache()
	defer processBatchTxsTimer.UpdateDuration(time.Now())
	coreTx, err := p.coreDB().BeginRo(ctx)
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

	l := len(p.unprocessedRemoteTxs.txs)
	if l == 0 {
		return nil
	}
	_, newTxs, err := p.validateTxs(*p.unprocessedRemoteTxs)
	if err != nil {
		return err
	}

	err = p.senders.onNewTxs(newTxs)
	if err != nil {
		return err
	}

	p.pending.captureAddedHashes(&p.promoted)
	if err := addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs, p.pendingBaseFee.Load(), p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}
	p.pending.added = nil

	if p.promoted.Len() > 0 {
		select {
		case <-ctx.Done():
			return nil
		case p.newPendingTxs <- common.Copy(p.promoted):
		default:
		}
	}

	p.unprocessedRemoteTxs.Resize(0)
	p.unprocessedRemoteByHash = map[string]int{}

	//log.Info("[txpool] on new txs", "amount", len(newPendingTxs.txs), "in", time.Since(t))
	return nil
}
func (p *TxPool) getRlpLocked(tx kv.Tx, hash []byte) (rlpTxn []byte, sender []byte, isLocal bool, err error) {
	txn, ok := p.byHash[string(hash)]
	if ok && txn.Tx.rlp != nil {
		return txn.Tx.rlp, []byte(p.senders.senderID2Addr[txn.Tx.senderID]), txn.subPool&IsLocal > 0, nil
	}
	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, nil, false, err
	}
	if v == nil {
		return nil, nil, false, nil
	}
	return v[20:], v[:20], txn != nil && txn.subPool&IsLocal > 0, nil
}
func (p *TxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	rlpTx, _, _, err := p.getRlpLocked(tx, hash)
	return rlpTx, err
}
func (p *TxPool) AppendLocalHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal == 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendRemoteHashes(buf []byte) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()

	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal != 0 {
			continue
		}
		buf = append(buf, hash...)
	}
	for hash := range p.unprocessedRemoteByHash {
		buf = append(buf, hash...)
	}
	return buf
}
func (p *TxPool) AppendAllHashes(buf []byte) []byte {
	buf = p.AppendLocalHashes(buf)
	buf = p.AppendRemoteHashes(buf)
	return buf
}
func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if _, ok := p.discardReasonsLRU.Get(string(hash)); ok {
		return true, nil
	}
	if _, ok := p.unprocessedRemoteByHash[string(hash)]; ok {
		return true, nil
	}
	if _, ok := p.byHash[string(hash)]; ok {
		return true, nil
	}
	return tx.Has(kv.PoolTransaction, hash)
}
func (p *TxPool) IsLocal(idHash []byte) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.isLocalLRU.Contains(string(idHash))
}
func (p *TxPool) AddNewGoodPeer(peerID PeerID) { p.recentlyConnectedPeers.AddPeer(peerID) }
func (p *TxPool) Started() bool                { return p.started.Load() }

// Best - returns top `n` elements of pending queue
// id doesn't perform full copy of txs, hovewer underlying elements are immutable
func (p *TxPool) Best(n uint16, txs *TxsRlp, tx kv.Tx) error {
	p.lock.RLock()
	defer p.lock.RUnlock()

	txs.Resize(uint(min(uint64(n), uint64(len(p.pending.best)))))

	best := p.pending.best
	for i := 0; i < int(n) && i < len(best); i++ {
		rlpTx, sender, isLocal, err := p.getRlpLocked(tx, best[i].Tx.idHash[:])
		if err != nil {
			return err
		}
		if len(rlpTx) == 0 {
			continue
		}
		txs.Txs[i] = rlpTx
		copy(txs.Senders.At(i), sender)
		txs.IsLocal[i] = isLocal
	}
	return nil
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pending.Len(), p.baseFee.Len(), p.queued.Len()
}
func (p *TxPool) AddRemoteTxs(_ context.Context, newTxs TxSlots) {
	defer addRemoteTxsTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := range newTxs.txs {
		_, ok := p.unprocessedRemoteByHash[string(newTxs.txs[i].idHash[:])]
		if ok {
			continue
		}
		p.unprocessedRemoteTxs.Append(newTxs.txs[i], newTxs.senders.At(i), false)
	}
}

func (p *TxPool) validateTx(txn *TxSlot, isLocal bool) DiscardReason {
	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && txn.feeCap < p.cfg.MinFeeCap {
		return UnderPriced
	}
	gas, reason := CalcIntrinsicGas(uint64(txn.dataLen), uint64(txn.dataNonZeroLen), nil, txn.creation, true, true)
	if reason != Success {
		return reason
	}
	if gas > txn.gas {
		return IntrinsicGas
	}
	if uint64(p.all.count(txn.senderID)) > p.cfg.AccountSlots {
		return Spammer
	}
	return Success
}

func (p *TxPool) validateTxs(txs TxSlots) (reasons []DiscardReason, goodTxs TxSlots, err error) {
	reasons = make([]DiscardReason, len(txs.txs))

	if err := txs.Valid(); err != nil {
		return reasons, goodTxs, err
	}

	j := 0
	for i := range txs.txs {
		reasons[i] = p.validateTx(txs.txs[i], txs.isLocal[i])
		if reasons[i] != Success {
			if reasons[i] == Spammer {
				p.punishSpammer(txs.txs[i].senderID)
			}
			continue
		}
		reasons[i] = NotSet
		goodTxs.Resize(uint(j + 1))
		goodTxs.txs[j] = txs.txs[i]
		goodTxs.isLocal[j] = txs.isLocal[i]
		copy(goodTxs.senders.At(j), txs.senders.At(i))
		j++
	}

	return reasons, goodTxs, nil
}

// punishSpammer by drop half of it's transactions with high nonce
func (p *TxPool) punishSpammer(spammer uint64) {
	var txsToDelete []*metaTx
	count := p.all.count(spammer)
	i := 0
	p.all.ascend(spammer, func(tx *metaTx) bool {
		i++
		if i < count/2 {
			return true
		}
		txsToDelete = append(txsToDelete, tx)
		return true
	})
	for j := range txsToDelete {
		p.discardLocked(txsToDelete[j], Spammer) // can't call it while iterating by all
	}
}

func fillDiscardReasons(reasons []DiscardReason, newTxs TxSlots, discardReasonsLRU *simplelru.LRU) []DiscardReason {
	for i := range reasons {
		if reasons[i] != NotSet {
			continue
		}
		reason, ok := discardReasonsLRU.Get(string(newTxs.txs[i].idHash[:]))
		if ok {
			reasons[i] = reason.(DiscardReason)
		} else {
			reasons[i] = Success
		}
	}
	return reasons
}

func (p *TxPool) AddLocalTxs(ctx context.Context, newTransactions TxSlots) ([]DiscardReason, error) {
	reasons, newTxs, err := p.validateTxs(newTransactions)
	if err != nil {
		return nil, err
	}

	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer coreTx.Rollback()

	cacheView, err := p.cache().View(ctx, coreTx)
	if err != nil {
		return nil, err
	}

	if !p.Started() {
		return nil, fmt.Errorf("pool not started yet")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if err = p.senders.onNewTxs(newTxs); err != nil {
		return nil, err
	}

	p.pending.captureAddedHashes(&p.promoted)
	if err := addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs, p.pendingBaseFee.Load(), p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return nil, err
	}
	p.pending.added = nil

	reasons = fillDiscardReasons(reasons, newTxs, p.discardReasonsLRU)
	for i := range reasons {
		if reasons[i] == Success {
			p.promoted = append(p.promoted, newTxs.txs[i].idHash[:]...)
		}
	}
	p.newPendingTxs <- common.Copy(p.promoted)
	return reasons, nil
}

func (p *TxPool) coreDB() kv.RoDB {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p._chainDB
}

func (p *TxPool) cache() kvcache.Cache {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p._stateCache
}
func addTxs(blockNum uint64, cacheView kvcache.CacheView, senders *sendersBatch,
	newTxs TxSlots, pendingBaseFee uint64,
	pending *PendingPool, baseFee, queued *SubPool,
	byNonce *BySenderAndNonce, byHash map[string]*metaTx, add func(*metaTx) bool, discard func(*metaTx, DiscardReason)) error {
	protocolBaseFee := calcProtocolBaseFee(pendingBaseFee)
	if ASSERT {
		for i := range newTxs.txs {
			if newTxs.txs[i].senderID == 0 {
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
	for i, txn := range newTxs.txs {
		if _, ok := byHash[string(txn.idHash[:])]; ok {
			continue
		}
		mt := newMetaTx(txn, newTxs.isLocal[i], blockNum)
		if !add(mt) {
			continue
		}
		sendersWithChangedState[mt.Tx.senderID] = struct{}{}
	}

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return err
		}
		onSenderStateChange(senderID, nonce, balance, byNonce, protocolBaseFee, pendingBaseFee, pending, baseFee, queued, false)
	}

	//pending.EnforceWorstInvariants()
	//baseFee.EnforceInvariants()
	//queued.EnforceInvariants()
	promote(pending, baseFee, queued, discard)
	//pending.EnforceWorstInvariants()
	pending.EnforceBestInvariants()

	return nil
}
func addTxsOnNewBlock(blockNum uint64, cacheView kvcache.CacheView, stateChanges *remote.StateChangeBatch,
	senders *sendersBatch, newTxs TxSlots, pendingBaseFee uint64, baseFeeChanged bool,
	pending *PendingPool, baseFee, queued *SubPool,
	byNonce *BySenderAndNonce, byHash map[string]*metaTx, add func(*metaTx) bool, discard func(*metaTx, DiscardReason)) error {
	protocolBaseFee := calcProtocolBaseFee(pendingBaseFee)
	if ASSERT {
		for i := range newTxs.txs {
			if newTxs.txs[i].senderID == 0 {
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
	for i, txn := range newTxs.txs {
		if _, ok := byHash[string(txn.idHash[:])]; ok {
			continue
		}
		mt := newMetaTx(txn, newTxs.isLocal[i], blockNum)
		if !add(mt) {
			continue
		}
		sendersWithChangedState[mt.Tx.senderID] = struct{}{}
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
				id, ok := senders.id(string(addr[:]))
				if !ok {
					continue
				}
				sendersWithChangedState[id] = struct{}{}
			}
		}
	}
	if baseFeeChanged {
		// TODO: add here protocolBaseFee also
		onBaseFeeChange(byNonce, pendingBaseFee) // re-calc all fields depending on pendingBaseFee
	}

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return err
		}
		onSenderStateChange(senderID, nonce, balance, byNonce, protocolBaseFee, pendingBaseFee, pending, baseFee, queued, true)
	}
	pending.EnforceWorstInvariants()
	baseFee.EnforceInvariants()
	queued.EnforceInvariants()
	promote(pending, baseFee, queued, discard)
	pending.EnforceWorstInvariants()
	pending.EnforceBestInvariants()

	return nil
}

func (p *TxPool) setBaseFee(baseFee uint64) (uint64, bool) {
	changed := false
	if baseFee > 0 {
		changed = baseFee != p.pendingBaseFee.Load()
		p.pendingBaseFee.Store(baseFee)
	}
	return p.pendingBaseFee.Load(), changed
}

func (p *TxPool) addLocked(mt *metaTx) bool {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.all.get(mt.Tx.senderID, mt.Tx.nonce)
	if found != nil {
		if mt.Tx.tip <= found.Tx.tip {
			return false
		}

		switch found.currentSubPool {
		case PendingSubPool:
			p.pending.UnsafeRemove(found)
		case BaseFeeSubPool:
			p.baseFee.UnsafeRemove(found)
		case QueuedSubPool:
			p.queued.UnsafeRemove(found)
		default:
			//already removed
		}

		p.discardLocked(found, ReplacedByHigherTip)
	}

	p.byHash[string(mt.Tx.idHash[:])] = mt

	if replaced := p.all.replaceOrInsert(mt); replaced != nil {
		if ASSERT {
			panic("must neve happen")
		}
	}

	if mt.subPool&IsLocal != 0 {
		p.isLocalLRU.Add(string(mt.Tx.idHash[:]), struct{}{})
	}
	p.queued.Add(mt)
	return true
}

// dropping transaction from all sub-structures and from db
// Important: don't call it while iterating by all
func (p *TxPool) discardLocked(mt *metaTx, reason DiscardReason) {
	delete(p.byHash, string(mt.Tx.idHash[:]))
	p.deletedTxs = append(p.deletedTxs, mt)
	p.all.delete(mt)
	p.discardReasonsLRU.Add(string(mt.Tx.idHash[:]), reason)
}

// removeMined - apply new highest block (or batch of blocks)
//
// 1. New best block arrives, which potentially changes the balance and the nonce of some senders.
// We use senderIds data structure to find relevant senderId values, and then use senders data structure to
// modify state_balance and state_nonce, potentially remove some elements (if transaction with some nonce is
// included into a block), and finally, walk over the transaction records and update SubPool fields depending on
// the actual presence of nonce gaps and what the balance is.
func removeMined(byNonce *BySenderAndNonce, minedTxs []*TxSlot, pending *PendingPool, baseFee, queued *SubPool, discard func(*metaTx, DiscardReason)) error {
	noncesToRemove := map[uint64]uint64{}
	for _, txn := range minedTxs {
		nonce, ok := noncesToRemove[txn.senderID]
		if !ok || txn.nonce > nonce {
			noncesToRemove[txn.senderID] = txn.nonce
		}
	}

	var toDel []*metaTx // can't delete items while iterate them
	for senderID, nonce := range noncesToRemove {
		//if sender.all.Len() > 0 {
		//log.Debug("[txpool] removing mined", "senderID", tx.senderID, "sender.all.len()", sender.all.Len())
		//}
		// delete mined transactions from everywhere
		byNonce.ascend(senderID, func(mt *metaTx) bool {
			//log.Debug("[txpool] removing mined, cmp nonces", "tx.nonce", it.metaTx.Tx.nonce, "sender.nonce", sender.nonce)
			if mt.Tx.nonce > nonce {
				return false
			}
			toDel = append(toDel, mt)
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				pending.UnsafeRemove(mt)
			case BaseFeeSubPool:
				baseFee.UnsafeRemove(mt)
			case QueuedSubPool:
				queued.UnsafeRemove(mt)
			default:
				//already removed
			}
			return true
		})

		for i := range toDel {
			discard(toDel[i], Mined)
		}
		toDel = toDel[:0]
	}
	return nil
}

func onBaseFeeChange(byNonce *BySenderAndNonce, pendingBaseFee uint64) {
	var prevSenderID uint64
	var minFeeCap, minTip uint64
	byNonce.tree.Ascend(func(i btree.Item) bool {
		mt := i.(sortByNonce).metaTx
		if mt.Tx.senderID != prevSenderID {
			minFeeCap, minTip = uint64(math.MaxUint64), uint64(math.MaxUint64) // min of given sender
			prevSenderID = mt.Tx.senderID
		}
		minFeeCap = min(minFeeCap, mt.Tx.feeCap)
		mt.minFeeCap = minFeeCap
		minTip = min(minTip, mt.Tx.tip)
		if pendingBaseFee <= minFeeCap {
			mt.effectiveTip = min(minFeeCap-pendingBaseFee, minTip)
		} else {
			mt.effectiveTip = 0
		}

		// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		// baseFee of the currently pending block. Set to 0 otherwise.
		mt.subPool &^= EnoughFeeCapBlock
		if mt.Tx.feeCap >= pendingBaseFee {
			mt.subPool |= EnoughFeeCapBlock
		}
		return true
	})
}

func onSenderStateChange(senderID uint64, senderNonce uint64, senderBalance uint256.Int, byNonce *BySenderAndNonce, protocolBaseFee, pendingBaseFee uint64, pending *PendingPool, baseFee, queued *SubPool, unsafe bool) {
	noGapsNonce := senderNonce
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint64(math.MaxUint64)
	minTip := uint64(math.MaxUint64)
	byNonce.ascend(senderID, func(mt *metaTx) bool {
		minFeeCap = min(minFeeCap, mt.Tx.feeCap)
		mt.minFeeCap = minFeeCap
		minTip = min(minTip, mt.Tx.tip)
		if pendingBaseFee <= minFeeCap {
			mt.effectiveTip = min(minFeeCap-pendingBaseFee, minTip)
		} else {
			mt.effectiveTip = minTip
		}

		mt.nonceDistance = 0
		if mt.Tx.nonce > senderNonce { // no uint underflow
			mt.nonceDistance = mt.Tx.nonce - senderNonce
		}

		// Sender has enough balance for: gasLimit x feeCap + transferred_value
		needBalance := uint256.NewInt(mt.Tx.gas)
		needBalance.Mul(needBalance, uint256.NewInt(mt.Tx.feeCap))
		needBalance.Add(needBalance, &mt.Tx.value)
		// 1. Minimum fee requirement. Set to 1 if feeCap of the transaction is no less than in-protocol
		// parameter of minimal base fee. Set to 0 if feeCap is less than minimum base fee, which means
		// this transaction will never be included into this particular chain.
		mt.subPool &^= EnoughFeeCapProtocol
		if mt.Tx.feeCap >= protocolBaseFee {
			mt.subPool |= EnoughFeeCapProtocol
		} else {
			mt.subPool = 0 // TODO: we immediately drop all transactions if they have no first bit - then maybe we don't need this bit at all? And don't add such transactions to queue?
			return true
		}

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.Tx.nonce {
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
		if mt.Tx.nonce >= senderNonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce <= sender.nonce
			if senderBalance.Gt(cumulativeRequiredBalance) || senderBalance.Eq(cumulativeRequiredBalance) {
				mt.subPool |= EnoughBalance
			} else {
				if cumulativeRequiredBalance.IsUint64() && senderBalance.IsUint64() {
					mt.cumulativeBalanceDistance = cumulativeRequiredBalance.Uint64() - senderBalance.Uint64()
				}
			}
		}

		// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		// baseFee of the currently pending block. Set to 0 otherwise.
		mt.subPool &^= EnoughFeeCapBlock
		if mt.minFeeCap >= pendingBaseFee {
			mt.subPool |= EnoughFeeCapBlock
		}

		// 5. Local transaction. Set to 1 if transaction is local.
		// can't change

		if !unsafe {
			switch mt.currentSubPool {
			case PendingSubPool:
				pending.Updated(mt)
			case BaseFeeSubPool:
				baseFee.Updated(mt)
			case QueuedSubPool:
				queued.Updated(mt)
			}
		}
		return true
	})
}

func promote(pending *PendingPool, baseFee, queued *SubPool, discard func(*metaTx, DiscardReason)) {
	//1. If top element in the worst green queue has subPool != 0b1111 (binary), it needs to be removed from the green pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard.
	//   If subPool == 0b1110, demote to the yellow pool, otherwise demote to the red pool.
	for worst := pending.Worst(); pending.Len() > 0; worst = pending.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		if worst.subPool >= 0b11100 {
			baseFee.Add(pending.PopWorst())
			continue
		}
		if worst.subPool >= 0b10000 {
			queued.Add(pending.PopWorst())
			continue
		}
		discard(pending.PopWorst(), FeeTooLow)
	}

	//2. If top element in the worst green queue has subPool == 0b1111, but there is not enough room in the pool, discard.
	for worst := pending.Worst(); pending.Len() > pending.limit; worst = pending.Worst() {
		if worst.subPool >= 0b11111 { // TODO: here must 'subPool == 0b1111' or 'subPool <= 0b1111' ?
			break
		}
		discard(pending.PopWorst(), PendingPoolOverflow)
	}

	//3. If the top element in the best yellow queue has subPool == 0b1111, promote to the green pool.
	for best := baseFee.Best(); baseFee.Len() > 0; best = baseFee.Best() {
		if best.subPool < 0b11110 {
			break
		}
		pending.Add(baseFee.PopBest())
	}

	//4. If the top element in the worst yellow queue has subPool != 0x1110, it needs to be removed from the yellow pool.
	//   If subPool < 0b1000 (not satisfying minimum fee), discard. Otherwise, demote to the red pool.
	for worst := baseFee.Worst(); baseFee.Len() > 0; worst = baseFee.Worst() {
		if worst.subPool >= 0b11100 {
			break
		}
		if worst.subPool >= 0b10000 {
			queued.Add(baseFee.PopWorst())
			continue
		}
		discard(baseFee.PopWorst(), FeeTooLow)
	}

	//5. If the top element in the worst yellow queue has subPool == 0x1110, but there is not enough room in the pool, discard.
	for worst := baseFee.Worst(); baseFee.Len() > baseFee.limit; worst = baseFee.Worst() {
		if worst.subPool >= 0b11110 {
			break
		}
		discard(baseFee.PopWorst(), BaseFeePoolOverflow)
	}

	//6. If the top element in the best red queue has subPool == 0x1110, promote to the yellow pool. If subPool == 0x1111, promote to the green pool.
	for best := queued.Best(); queued.Len() > 0; best = queued.Best() {
		if best.subPool < 0b11100 {
			break
		}
		if best.subPool < 0b11110 {
			baseFee.Add(queued.PopBest())
			continue
		}

		pending.Add(queued.PopBest())
	}

	//7. If the top element in the worst red queue has subPool < 0b1000 (not satisfying minimum fee), discard.
	for worst := queued.Worst(); queued.Len() > 0; worst = queued.Worst() {
		if worst.subPool >= 0b10000 {
			break
		}
		discard(queued.PopWorst(), FeeTooLow)
	}

	//8. If the top element in the worst red queue has subPool >= 0b100, but there is not enough room in the pool, discard.
	for _ = queued.Worst(); queued.Len() > queued.limit; _ = queued.Worst() {
		discard(queued.PopWorst(), QueuedPoolOverflow)
	}
}

// MainLoop - does:
// send pending byHash to p2p:
//      - new byHash
//      - all pooled byHash to recently connected peers
//      - all local pooled byHash to random peers periodically
// promote/demote transactions
// reorgs
func MainLoop(ctx context.Context, db kv.RwDB, coreDB kv.RoDB, p *TxPool, newTxs chan Hashes, send *Send, newSlotsStreams *NewSlotsStreams, notifyMiningAboutNewSlots func()) {
	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxsEvery := time.NewTicker(p.cfg.ProcessRemoteTxsEvery)
	defer processRemoteTxsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	localTxHashes := make([]byte, 0, 128)
	remoteTxHashes := make([]byte, 0, 128)

	for {
		select {
		case <-ctx.Done():
			_, _ = p.flush(db)
			return
		case <-logEvery.C:
			p.logStats()
		case <-processRemoteTxsEvery.C:
			if !p.Started() {
				continue
			}

			if err := p.processRemoteTxs(ctx); err != nil {
				if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
					continue
				}
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Error("[txpool] process batch remote txs", "err", err)
			}
		case <-commitEvery.C:
			if db != nil && p.Started() {
				t := time.Now()
				written, err := p.flush(db)
				if err != nil {
					log.Error("[txpool] flush is local history", "err", err)
					continue
				}
				writeToDbBytesCounter.Set(written)
				log.Debug("[txpool] Commit", "written_kb", written/1024, "in", time.Since(t))
			}
		case h := <-newTxs:
			t := time.Now()
			notifyMiningAboutNewSlots()
			if err := db.View(ctx, func(tx kv.Tx) error {
				slotsRlp := make([][]byte, 0, h.Len())
				for i := 0; i < h.Len(); i++ {
					slotRlp, err := p.GetRlp(tx, h.At(i))
					if err != nil {
						return err
					}
					slotsRlp = append(slotsRlp, slotRlp)
				}
				newSlotsStreams.Broadcast(&proto_txpool.OnAddReply{RplTxs: slotsRlp})
				return nil
			}); err != nil {
				log.Error("[txpool] send new slots by grpc", "err", err)
			}

			// first broadcast all local txs to all peers, then non-local to random sqrt(peersAmount) peers
			localTxHashes = localTxHashes[:0]
			remoteTxHashes = remoteTxHashes[:0]

			for i := 0; i < h.Len(); i++ {
				if p.IsLocal(h.At(i)) {
					localTxHashes = append(localTxHashes, h.At(i)...)
				} else {
					remoteTxHashes = append(localTxHashes, h.At(i)...)
				}
			}

			sentTo := send.BroadcastLocalPooledTxs(localTxHashes)
			if len(localTxHashes)/32 > 0 {
				if len(localTxHashes)/32 == 1 {
					log.Info("local tx propagated", "to_peers_amount", sentTo, "tx_hash", fmt.Sprintf("%x", localTxHashes), "baseFee", p.pendingBaseFee.Load())
				} else {
					log.Info("local txs propagated", "to_peers_amount", sentTo, "txs_amount", len(localTxHashes)/32, "baseFee", p.pendingBaseFee.Load())
				}
			}
			send.BroadcastRemotePooledTxs(remoteTxHashes)
			propagateNewTxsTimer.UpdateDuration(t)
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			t := time.Now()
			remoteTxHashes = p.AppendAllHashes(remoteTxHashes[:0])
			send.PropagatePooledTxsToPeersList(newPeers, remoteTxHashes)
			propagateToNewPeerTimer.UpdateDuration(t)
		}
	}
}

func (p *TxPool) flush(db kv.RwDB) (written uint64, err error) {
	defer writeToDbTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db tx is done inside lock, to make last writes visible for all read operations
	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
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
func (p *TxPool) flushLocked(tx kv.RwTx) (err error) {
	for i := 0; i < len(p.deletedTxs); i++ {
		if !p.all.hasTxs(p.deletedTxs[i].Tx.senderID) {
			addr, ok := p.senders.senderID2Addr[p.deletedTxs[i].Tx.senderID]
			if ok {
				delete(p.senders.senderID2Addr, p.deletedTxs[i].Tx.senderID)
				delete(p.senders.senderIDs, string(addr))
			}
		}
		//fmt.Printf("del:%d,%d,%d\n", p.deletedTxs[i].Tx.senderID, p.deletedTxs[i].Tx.nonce, p.deletedTxs[i].Tx.tip)
		has, err := tx.Has(kv.PoolTransaction, p.deletedTxs[i].Tx.idHash[:])
		if err != nil {
			return err
		}
		if has {
			if err := tx.Delete(kv.PoolTransaction, p.deletedTxs[i].Tx.idHash[:], nil); err != nil {
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
	for i := range txHashes {
		binary.BigEndian.PutUint64(encID, uint64(i))
		if err := tx.Append(kv.RecentLocalTransaction, encID, []byte(txHashes[i].(string))); err != nil {
			return err
		}
	}

	v := make([]byte, 0, 1024)
	for txHash, metaTx := range p.byHash {
		if metaTx.Tx.rlp == nil {
			continue
		}
		v = common.EnsureEnoughSize(v, 20+len(metaTx.Tx.rlp))
		for addr, id := range p.senders.senderIDs { // no inverted index - tradeoff flush speed for memory usage
			if id == metaTx.Tx.senderID {
				copy(v[:20], addr)
				break
			}
		}
		copy(v[20:], metaTx.Tx.rlp)

		has, err := tx.Has(kv.PoolTransaction, []byte(txHash))
		if err != nil {
			return err
		}
		if !has {
			if err := tx.Put(kv.PoolTransaction, []byte(txHash), v); err != nil {
				return err
			}
		}
		metaTx.Tx.rlp = nil
	}

	binary.BigEndian.PutUint64(encID, p.pendingBaseFee.Load())
	if err := tx.Put(kv.PoolInfo, PoolPendingBaseFeeKey, encID); err != nil {
		return err
	}
	if err := PutLastSeenBlock(tx, p.lastSeenBlock.Load(), encID); err != nil {
		return err
	}

	// clean - in-memory data structure as later as possible - because if during this Tx will happen error,
	// DB will stay consitant but some in-memory structures may be alread cleaned, and retry will not work
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

	cacheView, err := p._stateCache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	if err := tx.ForEach(kv.RecentLocalTransaction, nil, func(k, v []byte) error {
		//fmt.Printf("is local restored from db: %x\n", k)
		p.isLocalLRU.Add(string(v), struct{}{})
		return nil
	}); err != nil {
		return err
	}

	txs := TxSlots{}
	parseCtx := NewTxParseContext(p.chainID)
	parseCtx.WithSender(false)

	i := 0
	if err := tx.ForEach(kv.PoolTransaction, nil, func(k, v []byte) error {
		addr, txRlp := v[:20], v[20:]
		txn := &TxSlot{}

		_, err := parseCtx.ParseTransaction(txRlp, 0, txn, nil)
		if err != nil {
			return fmt.Errorf("err: %w, rlp: %x", err, txRlp)
		}
		txn.rlp = nil // means that we don't need store it in db anymore

		id, ok := p.senders.senderIDs[string(addr)]
		if !ok {
			p.senders.senderID++
			id = p.senders.senderID
			p.senders.senderIDs[string(addr)] = id
			p.senders.senderID2Addr[id] = string(addr)
		}
		txn.senderID = id
		binary.BigEndian.Uint64(v)

		isLocalTx := p.isLocalLRU.Contains(string(k))

		if reason := p.validateTx(txn, isLocalTx); reason != NotSet && reason != Success {
			return nil
		}
		txs.Resize(uint(i + 1))
		txs.txs[i] = txn
		txs.isLocal[i] = isLocalTx
		copy(txs.senders.At(i), addr)
		i++
		return nil
	}); err != nil {
		return err
	}

	var pendingBaseFee uint64
	{
		v, err := tx.GetOne(kv.PoolInfo, PoolPendingBaseFeeKey)
		if err != nil {
			return err
		}
		if len(v) > 0 {
			pendingBaseFee = binary.BigEndian.Uint64(v)
		}
	}
	err = p.senders.onNewTxs(txs)
	if err != nil {
		return err
	}
	if err := addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, txs, pendingBaseFee, p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked); err != nil {
		return err
	}
	p.pendingBaseFee.Store(pendingBaseFee)

	return nil
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

//nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%d\n", j.Tx.senderID, j.Tx.nonce, j.Tx.tip)
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for _, mt := range p.pending.best {
		mt.Tx.printDebug(fmt.Sprintf("%s.pending: %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.senderID, mt.Tx.nonce, mt.Tx.tip))
	}
	for _, mt := range *p.baseFee.best {
		mt.Tx.printDebug(fmt.Sprintf("%s.baseFee : %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.senderID, mt.Tx.nonce, mt.Tx.tip))
	}
	for _, mt := range *p.queued.best {
		mt.Tx.printDebug(fmt.Sprintf("%s.queued : %b,%d,%d,%d", prefix, mt.subPool, mt.Tx.senderID, mt.Tx.nonce, mt.Tx.tip))
	}
}
func (p *TxPool) logStats() {
	if !p.started.Load() {
		//log.Info("[txpool] Not started yet, waiting for new blocks...")
		return
	}
	//protocolBaseFee, pendingBaseFee := p.protocolBaseFee.Load(), p.pendingBaseFee.Load()

	p.lock.RLock()
	defer p.lock.RUnlock()

	//idsInMem := p.senders.idsCount()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ctx := []interface{}{
		//"baseFee", fmt.Sprintf("%d, %dm", protocolBaseFee, pendingBaseFee/1_000_000),
		"block", p.lastSeenBlock.Load(),
		"pending", p.pending.Len(),
		"baseFee", p.baseFee.Len(),
		"queued", p.queued.Len(),
	}
	cacheKeys := p._stateCache.Len()
	if cacheKeys > 0 {
		ctx = append(ctx, "cache_keys", cacheKeys)
	}
	ctx = append(ctx, "alloc_mb", m.Alloc/1024/1024, "sys_mb", m.Sys/1024/1024)
	log.Info("[txpool] stat", ctx...)
	//if ASSERT {
	//stats := kvcache.DebugStats(p.senders.cache)
	//log.Info(fmt.Sprintf("[txpool] cache %T, roots amount %d", p.senders.cache, len(stats)))
	//for i := range stats {
	//	log.Info("[txpool] cache", "root", stats[i].BlockNum, "len", stats[i].Lenght)
	//}
	//stats := kvcache.DebugStats(p.senders.cache)
	//log.Info(fmt.Sprintf("[txpool] cache %T, roots amount %d", p.senders.cache, len(stats)))
	//for i := range stats {
	//	log.Info("[txpool] cache", "root", stats[i].BlockNum, "len", stats[i].Lenght)
	//}
	//ages := kvcache.DebugAges(p.senders.cache)
	//for i := range ages {
	//	log.Info("[txpool] age", "age", ages[i].BlockNum, "amount", ages[i].Lenght)
	//}
	//}
}

//Deprecated need switch to streaming-like
func (p *TxPool) deprecatedForEach(_ context.Context, f func(rlp, sender []byte, t SubPoolType), tx kv.Tx) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	p.all.tree.Ascend(func(i btree.Item) bool {
		mt := i.(sortByNonce).metaTx
		slot := mt.Tx
		slotRlp := slot.rlp
		if slot.rlp == nil {
			v, err := tx.GetOne(kv.PoolTransaction, slot.idHash[:])
			if err != nil {
				log.Error("[txpool] get tx from db", "err", err)
				return false
			}
			if v == nil {
				log.Error("[txpool] tx not found in db")
				return false
			}
			slotRlp = v[20:]
		}

		var sender []byte
		found := false
		for addr, senderID := range p.senders.senderIDs { // TODO: do we need inverted index here?
			if slot.senderID == senderID {
				sender = []byte(addr)
				found = true
				break
			}
		}
		if !found {
			return true
		}
		f(slotRlp, sender, mt.currentSubPool)
		return true
	})
	return nil
}

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func CalcIntrinsicGas(dataLen, dataNonZeroLen uint64, accessList AccessList, isContractCreation bool, isHomestead, isEIP2028 bool) (uint64, DiscardReason) {
	// Set the starting gas for the raw transaction
	var gas uint64
	if isContractCreation && isHomestead {
		gas = fixedgas.TxGasContractCreation
	} else {
		gas = fixedgas.TxGas
	}
	// Bump the required gas by the amount of transactional data
	if dataLen > 0 {
		// Zero and non-zero bytes are priced differently
		nz := dataNonZeroLen
		// Make sure we don't exceed uint64 for all data combinations
		nonZeroGas := fixedgas.TxDataNonZeroGasFrontier
		if isEIP2028 {
			nonZeroGas = fixedgas.TxDataNonZeroGasEIP2028
		}
		if (math.MaxUint64-gas)/nonZeroGas < nz {
			return 0, GasUintOverflow
		}
		gas += nz * nonZeroGas

		z := dataLen - nz
		if (math.MaxUint64-gas)/fixedgas.TxDataZeroGas < z {
			return 0, GasUintOverflow
		}
		gas += z * fixedgas.TxDataZeroGas
	}
	if accessList != nil {
		gas += uint64(len(accessList)) * fixedgas.TxAccessListAddressGas
		gas += uint64(accessList.StorageKeys()) * fixedgas.TxAccessListStorageKeyGas
	}
	return gas, Success
}

var PoolChainConfigKey = []byte("chain_config")
var PoolLastSeenBlockKey = []byte("last_seen_block")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")

// recentlyConnectedPeers does buffer IDs of recently connected good peers
// then sync of pooled Transaction can happen to all of then at once
// DoS protection and performance saving
// it doesn't track if peer disconnected, it's fine
type recentlyConnectedPeers struct {
	lock  sync.RWMutex
	peers []PeerID
}

func (l *recentlyConnectedPeers) AddPeer(p PeerID) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.peers = append(l.peers, p)
}

func (l *recentlyConnectedPeers) GetAndClean() []PeerID {
	l.lock.Lock()
	defer l.lock.Unlock()
	peers := l.peers
	l.peers = nil
	return peers
}

//nolint
func (sc *sendersBatch) printDebug(prefix string) {
	fmt.Printf("%s.sendersBatch.sender\n", prefix)
	//for i, j := range sc.senderInfo {
	//	fmt.Printf("\tid=%d,nonce=%d,balance=%d\n", i, j.nonce, j.balance.Uint64())
	//}
}

// sendersBatch stores in-memory senders-related objects - which are different from DB (updated/dirty)
// flushing to db periodicaly. it doesn't play as read-cache (because db is small and memory-mapped - doesn't need cache)
// non thread-safe
type sendersBatch struct {
	senderID      uint64
	senderIDs     map[string]uint64
	senderID2Addr map[uint64]string
}

func newSendersCache() *sendersBatch {
	return &sendersBatch{senderIDs: map[string]uint64{}, senderID2Addr: map[uint64]string{}}
}

func (sc *sendersBatch) id(addr string) (uint64, bool) {
	id, ok := sc.senderIDs[addr]
	return id, ok
}
func (sc *sendersBatch) info(cacheView kvcache.CacheView, id uint64) (nonce uint64, balance uint256.Int, err error) {
	addr, ok := sc.senderID2Addr[id]
	if !ok {
		panic("must not happen")
	}
	encoded, err := cacheView.Get([]byte(addr))
	if err != nil {
		return 0, emptySender.balance, err
	}
	if len(encoded) == 0 {
		return emptySender.nonce, emptySender.balance, nil
	}
	nonce, balance, err = DecodeSender(encoded)
	if err != nil {
		return 0, emptySender.balance, err
	}
	return nonce, balance, nil
}

func (sc *sendersBatch) onNewTxs(newTxs TxSlots) (err error) {
	for i := 0; i < len(newTxs.txs); i++ {
		addr := newTxs.senders.At(i)
		addrS := string(addr)
		id, ok := sc.id(addrS)
		if ok {
			newTxs.txs[i].senderID = id
			continue
		}
		sc.senderID++
		sc.senderIDs[string(newTxs.senders.At(i))] = sc.senderID
		sc.senderID2Addr[sc.senderID] = addrS

		newTxs.txs[i].senderID = sc.senderID
	}
	return nil
}
func (sc *sendersBatch) onNewBlock(stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxSlots) error {
	for _, diff := range stateChanges.ChangeBatch {
		for _, change := range diff.Changes { // merge state changes
			addrB := gointerfaces.ConvertH160toAddress(change.Address)
			addr := string(addrB[:])
			_, ok := sc.id(addr)
			if !ok {
				sc.senderID++
				sc.senderIDs[addr] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addr
			}
		}

		for i := 0; i < unwindTxs.senders.Len(); i++ {
			addr := unwindTxs.senders.At(i)
			addrS := string(addr)
			id, ok := sc.id(addrS)
			if !ok {
				sc.senderID++
				id = sc.senderID
				sc.senderIDs[addrS] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addrS
			}
			unwindTxs.txs[i].senderID = id
		}

		for i := 0; i < len(minedTxs.txs); i++ {
			addr := minedTxs.senders.At(i)
			addrS := string(addr)
			id, ok := sc.id(addrS)
			if !ok {
				sc.senderID++
				id = sc.senderID
				sc.senderIDs[addrS] = sc.senderID
				sc.senderID2Addr[sc.senderID] = addrS
			}
			minedTxs.txs[i].senderID = id
		}
	}
	return nil
}

// BySenderAndNonce - designed to perform most expensive operation in TxPool:
// "recalculate all ephemeral fields of all transactions" by algo
//      - for all senders - iterate over all transactions in nonce growing order
//
// Performane decisions:
//  - All senders stored inside 1 large BTree - because iterate over 1 BTree is faster than over map[senderId]BTree
//  - sortByNonce used as non-pointer wrapper - because iterate over BTree of pointers is 2x slower
type BySenderAndNonce struct {
	tree   *btree.BTree
	search sortByNonce
}

//nolint
func (b *BySenderAndNonce) nonce(senderID uint64) (nonce uint64) {
	s := b.search
	s.metaTx.Tx.senderID = senderID
	s.metaTx.Tx.nonce = 0

	b.tree.DescendLessOrEqual(s, func(i btree.Item) bool {
		mt := i.(sortByNonce).metaTx
		if mt.Tx.senderID != senderID {
			return false
		}
		nonce = mt.Tx.nonce
		return true
	})
	return nonce
}
func (b *BySenderAndNonce) ascend(senderID uint64, f func(*metaTx) bool) {
	s := b.search
	s.metaTx.Tx.senderID = senderID
	s.metaTx.Tx.nonce = 0
	b.tree.AscendGreaterOrEqual(s, func(i btree.Item) bool {
		mt := i.(sortByNonce).metaTx
		if mt.Tx.senderID != senderID {
			return false
		}
		return f(mt)
	})
}
func (b *BySenderAndNonce) count(senderID uint64) int {
	s := b.search
	s.metaTx.Tx.senderID = senderID
	s.metaTx.Tx.nonce = 0
	count := 0
	b.tree.AscendGreaterOrEqual(s, func(i btree.Item) bool {
		mt := i.(sortByNonce).metaTx
		if mt.Tx.senderID != senderID {
			return false
		}
		count++
		return true
	})
	return count
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
	s.metaTx.Tx.senderID = senderID
	s.metaTx.Tx.nonce = txNonce
	if found := b.tree.Get(s); found != nil {
		return found.(sortByNonce).metaTx
	}
	return nil
}

//nolint
func (b *BySenderAndNonce) has(mt *metaTx) bool {
	found := b.tree.Get(sortByNonce{mt})
	return found != nil
}
func (b *BySenderAndNonce) delete(mt *metaTx) { b.tree.Delete(sortByNonce{mt}) }
func (b *BySenderAndNonce) replaceOrInsert(mt *metaTx) *metaTx {
	it := b.tree.ReplaceOrInsert(sortByNonce{mt})
	if it != nil {
		return it.(sortByNonce).metaTx
	}
	return nil
}

// PendingPool - is different from other pools - it's best is Slice instead of Heap
// It's more expensive to maintain "slice sort" invariant, but it allow do cheap copy of
// pending.best slice for mining (because we consider txs and metaTx are immutable)
type PendingPool struct {
	limit int
	t     SubPoolType
	best  bestSlice
	worst *WorstQueue
	added *Hashes
}

func NewPendingSubPool(t SubPoolType, limit int) *PendingPool {
	return &PendingPool{limit: limit, t: t, best: []*metaTx{}, worst: &WorstQueue{}}
}

func (p *PendingPool) captureAddedHashes(to *Hashes) {
	p.added = to
	*p.added = (*p.added)[:0]
}

// bestSlice - is similar to best queue, but with O(n log n) complexity and
// it maintains element.bestIndex field
type bestSlice []*metaTx

func (s bestSlice) Len() int { return len(s) }
func (s bestSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
	s[i].bestIndex, s[j].bestIndex = i, j
}
func (s bestSlice) Less(i, j int) bool { return !s[i].Less(s[j]) }
func (s bestSlice) UnsafeRemove(i *metaTx) bestSlice {
	s.Swap(i.bestIndex, len(s)-1)
	s[len(s)-1].bestIndex = -1
	s[len(s)-1] = nil
	return s[:len(s)-1]
}
func (s bestSlice) UnsafeAdd(i *metaTx) bestSlice {
	a := append(s, i)
	i.bestIndex = len(s)
	return a
}

func (p *PendingPool) EnforceWorstInvariants() {
	heap.Init(p.worst)
}
func (p *PendingPool) EnforceBestInvariants() {
	sort.Sort(p.best)
}

func (p *PendingPool) Best() *metaTx {
	if len(p.best) == 0 {
		return nil
	}
	return p.best[0]
}
func (p *PendingPool) Worst() *metaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *PendingPool) PopWorst() *metaTx {
	i := heap.Pop(p.worst).(*metaTx)
	p.best = p.best.UnsafeRemove(i)
	return i
}
func (p *PendingPool) Updated(mt *metaTx) {
	heap.Fix(p.worst, mt.worstIndex)
}
func (p *PendingPool) Len() int { return len(p.best) }

// UnsafeRemove - does break Heap invariants, but it has O(1) instead of O(log(n)) complexity.
// Must manually call heap.Init after such changes.
// Make sense to batch unsafe changes
func (p *PendingPool) UnsafeRemove(i *metaTx) {
	if p.Len() == 0 {
		return
	}
	if p.Len() == 1 && i.bestIndex == 0 {
		p.worst.Pop()
		p.best = p.best.UnsafeRemove(i)
		return
	}
	// manually call funcs instead of heap.Pop
	p.worst.Swap(i.worstIndex, p.worst.Len()-1)
	p.worst.Pop()
	p.best.Swap(i.bestIndex, p.best.Len()-1)
	p.best = p.best.UnsafeRemove(i)
}
func (p *PendingPool) UnsafeAdd(i *metaTx) {
	if p.added != nil {
		*p.added = append(*p.added, i.Tx.idHash[:]...)
	}
	i.currentSubPool = p.t
	p.worst.Push(i)
	p.best = p.best.UnsafeAdd(i)
}
func (p *PendingPool) Add(i *metaTx) {
	if p.added != nil {
		*p.added = append(*p.added, i.Tx.idHash[:]...)
	}
	i.currentSubPool = p.t
	heap.Push(p.worst, i)
	p.best = p.best.UnsafeAdd(i)
}
func (p *PendingPool) DebugPrint(prefix string) {
	for i, it := range p.best {
		fmt.Printf("%s.best: %d, %d, %d,%d\n", prefix, i, it.subPool, it.bestIndex, it.Tx.nonce)
	}
	for i, it := range *p.worst {
		fmt.Printf("%s.worst: %d, %d, %d,%d\n", prefix, i, it.subPool, it.worstIndex, it.Tx.nonce)
	}
}

type SubPool struct {
	limit int
	t     SubPoolType
	best  *BestQueue
	worst *WorstQueue
}

func NewSubPool(t SubPoolType, limit int) *SubPool {
	return &SubPool{limit: limit, t: t, best: &BestQueue{}, worst: &WorstQueue{}}
}

func (p *SubPool) EnforceInvariants() {
	heap.Init(p.worst)
	heap.Init(p.best)
}
func (p *SubPool) Best() *metaTx {
	if len(*p.best) == 0 {
		return nil
	}
	return (*p.best)[0]
}
func (p *SubPool) Worst() *metaTx {
	if len(*p.worst) == 0 {
		return nil
	}
	return (*p.worst)[0]
}
func (p *SubPool) PopBest() *metaTx {
	i := heap.Pop(p.best).(*metaTx)
	heap.Remove(p.worst, i.worstIndex)
	return i
}
func (p *SubPool) PopWorst() *metaTx {
	i := heap.Pop(p.worst).(*metaTx)
	heap.Remove(p.best, i.bestIndex)
	return i
}
func (p *SubPool) Len() int { return p.best.Len() }
func (p *SubPool) Add(i *metaTx) {
	i.currentSubPool = p.t
	heap.Push(p.best, i)
	heap.Push(p.worst, i)
}

func (p *SubPool) Remove(i *metaTx) {
	heap.Remove(p.best, i.bestIndex)
	heap.Remove(p.worst, i.worstIndex)
	i.currentSubPool = 0
}
func (p *SubPool) Updated(i *metaTx) {
	heap.Fix(p.best, i.bestIndex)
	heap.Fix(p.worst, i.worstIndex)
}

// UnsafeRemove - does break Heap invariants, but it has O(1) instead of O(log(n)) complexity.
// Must manually call heap.Init after such changes.
// Make sense to batch unsafe changes
func (p *SubPool) UnsafeRemove(i *metaTx) {
	if p.Len() == 0 {
		return
	}
	if p.Len() == 1 && i.bestIndex == 0 {
		p.worst.Pop()
		p.best.Pop()
		return
	}
	// manually call funcs instead of heap.Pop
	p.worst.Swap(i.worstIndex, p.worst.Len()-1)
	p.worst.Pop()
	p.best.Swap(i.bestIndex, p.best.Len()-1)
	p.best.Pop()
}
func (p *SubPool) UnsafeAdd(i *metaTx) {
	i.currentSubPool = p.t
	p.worst.Push(i)
	p.best.Push(i)
}
func (p *SubPool) DebugPrint(prefix string) {
	for i, it := range *p.best {
		fmt.Printf("%s.best: %d, %d, %d\n", prefix, i, it.subPool, it.bestIndex)
	}
	for i, it := range *p.worst {
		fmt.Printf("%s.worst: %d, %d, %d\n", prefix, i, it.subPool, it.worstIndex)
	}
}

type BestQueue []*metaTx

func (mt *metaTx) Less(than *metaTx) bool {
	if mt.subPool != than.subPool {
		return mt.subPool < than.subPool
	}

	switch mt.currentSubPool {
	case PendingSubPool:
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
		}
		if mt.effectiveTip != than.effectiveTip {
			return mt.effectiveTip < than.effectiveTip
		}
	case BaseFeeSubPool:
		if mt.minFeeCap != than.minFeeCap {
			return mt.minFeeCap > than.minFeeCap // yes, here is greaterOrEqual to sort by revert order of minFeeCap
		}
		if mt.nonceDistance != than.nonceDistance {
			return mt.nonceDistance < than.nonceDistance
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

func (p BestQueue) Len() int           { return len(p) }
func (p BestQueue) Less(i, j int) bool { return !p[i].Less(p[j]) } // We want Pop to give us the highest, not lowest, priority so we use !less here.
func (p BestQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].bestIndex = i
	p[j].bestIndex = j
}
func (p *BestQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.bestIndex = n
	*p = append(*p, item)
}

func (p *BestQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.bestIndex = -1     // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

type WorstQueue []*metaTx

func (p WorstQueue) Len() int           { return len(p) }
func (p WorstQueue) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p WorstQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].worstIndex = i
	p[j].worstIndex = j
}
func (p *WorstQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*metaTx)
	item.worstIndex = n
	*p = append(*p, x.(*metaTx))
}
func (p *WorstQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	old[n-1] = nil          // avoid memory leak
	item.worstIndex = -1    // for safety
	item.currentSubPool = 0 // for safety
	*p = old[0 : n-1]
	return item
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
