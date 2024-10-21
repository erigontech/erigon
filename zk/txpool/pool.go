/*
   Copyright 2022 Erigon contributors

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
	"fmt"
	"math"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
	"github.com/status-im/keycard-go/hexutils"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	emath "github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/types"
)

var (
	processBatchTxsTimer    = metrics.NewSummary(`pool_process_remote_txs`)
	addRemoteTxsTimer       = metrics.NewSummary(`pool_add_remote_txs`)
	newBlockTimer           = metrics.NewSummary(`pool_new_block`)
	writeToDBTimer          = metrics.NewSummary(`pool_write_to_db`)
	propagateToNewPeerTimer = metrics.NewSummary(`pool_propagate_to_new_peer`)
	propagateNewTxsTimer    = metrics.NewSummary(`pool_propagate_new_txs`)
	writeToDBBytesCounter   = metrics.GetOrCreateCounter(`pool_write_to_db_bytes`)
	pendingSubCounter       = metrics.GetOrCreateCounter(`txpool_pending`)
	queuedSubCounter        = metrics.GetOrCreateCounter(`txpool_queued`)
	basefeeSubCounter       = metrics.GetOrCreateCounter(`txpool_basefee`)
)

// Pool is interface for the transaction pool
// This interface exists for the convenience of testing, and not yet because
// there are multiple implementations
type Pool interface {
	ValidateSerializedTxn(serializedTxn []byte) error

	// Handle 3 main events - new remote txs from p2p, new local txs from RPC, new blocks from execution layer
	AddRemoteTxs(ctx context.Context, newTxs types.TxSlots)
	AddLocalTxs(ctx context.Context, newTxs types.TxSlots, tx kv.Tx) ([]DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs types.TxSlots, tx kv.Tx) error

	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)

	AddNewGoodPeer(peerID types.PeerID)
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
	EnoughFeeCapProtocol = 0b100000
	NoNonceGaps          = 0b010000
	EnoughBalance        = 0b001000
	NotTooMuchGas        = 0b000100
	EnoughFeeCapBlock    = 0b000010
	IsLocal              = 0b000001

	BaseFeePoolBits = EnoughFeeCapProtocol + NoNonceGaps + EnoughBalance + NotTooMuchGas
	QueuedPoolBits  = EnoughFeeCapProtocol
)

type DiscardReason uint8

const (
	NotSet                          DiscardReason = 0 // analog of "nil-value", means it will be set in future
	Success                         DiscardReason = 1
	AlreadyKnown                    DiscardReason = 2
	Mined                           DiscardReason = 3
	ReplacedByHigherTip             DiscardReason = 4
	UnderPriced                     DiscardReason = 5
	ReplaceUnderpriced              DiscardReason = 6 // if a transaction is attempted to be replaced with a different one without the required price bump.
	FeeTooLow                       DiscardReason = 7
	OversizedData                   DiscardReason = 8
	InvalidSender                   DiscardReason = 9
	NegativeValue                   DiscardReason = 10 // ensure no one is able to specify a transaction with a negative value.
	Spammer                         DiscardReason = 11
	PendingPoolOverflow             DiscardReason = 12
	BaseFeePoolOverflow             DiscardReason = 13
	QueuedPoolOverflow              DiscardReason = 14
	GasUintOverflow                 DiscardReason = 15
	IntrinsicGas                    DiscardReason = 16
	RLPTooLong                      DiscardReason = 17
	NonceTooLow                     DiscardReason = 18
	InsufficientFunds               DiscardReason = 19
	NotReplaced                     DiscardReason = 20 // There was an existing transaction with the same sender and nonce, not enough price bump to replace
	DuplicateHash                   DiscardReason = 21 // There was an existing transaction with the same hash
	InitCodeTooLarge                DiscardReason = 22 // EIP-3860 - transaction init code is too large
	UnsupportedTx                   DiscardReason = 23 // unsupported transaction type
	OverflowZkCounters              DiscardReason = 24 // unsupported transaction type
	SenderDisallowedSendTx          DiscardReason = 25 // sender is not allowed to send transactions by ACL policy
	SenderDisallowedDeploy          DiscardReason = 26 // sender is not allowed to deploy contracts by ACL policy
	DiscardByLimbo                  DiscardReason = 27
	SmartContractDeploymentDisabled DiscardReason = 28 // to == null not allowed, config set to block smart contract deployment
	GasLimitTooHigh                 DiscardReason = 29 // gas limit is too high
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
	case Spammer:
		return "spammer"
	case PendingPoolOverflow:
		return "pending sub-pool is full"
	case BaseFeePoolOverflow:
		return "baseFee sub-pool is full"
	case QueuedPoolOverflow:
		return "queued sub-pool is full"
	case GasUintOverflow:
		return "GasUintOverflow"
	case IntrinsicGas:
		return "IntrinsicGas"
	case RLPTooLong:
		return "RLPTooLong"
	case NonceTooLow:
		return "nonce too low"
	case InsufficientFunds:
		return "insufficient funds"
	case NotReplaced:
		return "could not replace existing tx"
	case DuplicateHash:
		return "existing tx with same hash"
	case InitCodeTooLarge:
		return "initcode too large"
	case UnsupportedTx:
		return "unsupported transaction type"
	case OverflowZkCounters:
		return "overflow zk-counters"
	case SenderDisallowedSendTx:
		return "sender disallowed to send tx by ACL policy"
	case SenderDisallowedDeploy:
		return "sender disallowed to deploy contract by ACL policy"
	case DiscardByLimbo:
		return "limbo error"
	case SmartContractDeploymentDisabled:
		return "smart contract deployment disabled"
	case GasLimitTooHigh:
		return fmt.Sprintf("gas limit too high. Max: %d", transactionGasLimit)
	default:
		panic(fmt.Sprintf("discard reason: %d", r))
	}
}

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
	alreadyYielded            bool
}

func newMetaTx(slot *types.TxSlot, isLocal bool, timestmap uint64) *metaTx {
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
const LimboSubPool SubPoolType = 4

const LimboSubPoolSize = 100_000 // overkill but better too large than too small

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
// txpool has no DB or TX fields - "leave db transactions management to user" design
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
	// handling works fast without batching, but batching allow:
	//   - reduce amount of _chainDB transactions
	//   - batch notifications about new txs (reduce P2P spam to other nodes about txs propagation)
	//   - and as a result reducing lock contention
	unprocessedRemoteTxs    *types.TxSlots
	unprocessedRemoteByHash map[string]int                        // to reject duplicates
	byHash                  map[string]*metaTx                    // tx_hash => tx : only not committed to db yet records
	discardReasonsLRU       *simplelru.LRU[string, DiscardReason] // tx_hash => discard_reason : non-persisted
	pending                 *PendingPool
	baseFee                 *SubPool
	queued                  *SubPool
	isLocalLRU              *simplelru.LRU[string, struct{}] // tx_hash => is_local : to restore isLocal flag of unwinded transactions
	newPendingTxs           chan types.Announcements         // notifications about new txs in Pending sub-pool
	all                     *BySenderAndNonce                // senderID => (sorted map of tx nonce => *metaTx)
	deletedTxs              []*metaTx                        // list of discarded txs since last db commit
	overflowZkCounters      []*metaTx
	promoted                types.Announcements
	cfg                     txpoolcfg.Config
	chainID                 uint256.Int
	lastSeenBlock           atomic.Uint64
	started                 atomic.Bool
	pendingBaseFee          atomic.Uint64
	blockGasLimit           atomic.Uint64
	londonBlock             *big.Int
	isPostLondon            atomic.Bool
	shanghaiTime            *big.Int
	isPostShanghai          atomic.Bool
	ethCfg                  *ethconfig.Config
	aclDB                   kv.RwDB

	// we cannot be in a flushing state whilst getting transactions from the pool, so we have this mutex which is
	// exposed publicly so anything wanting to get "best" transactions can ensure a flush isn't happening and
	// vice versa
	flushMtx *sync.Mutex

	// limbo specific fields where bad batch transactions identified by the executor go
	limbo *Limbo
}

func CreateTxPoolBuckets(tx kv.RwTx) error {
	if err := tx.CreateBucket(TablePoolLimbo); err != nil {
		return err
	}
	return nil
}

func New(newTxs chan types.Announcements, coreDB kv.RoDB, cfg txpoolcfg.Config, ethCfg *ethconfig.Config, cache kvcache.Cache, chainID uint256.Int, shanghaiTime *big.Int, londonBlock *big.Int, aclDB kv.RwDB) (*TxPool, error) {
	var err error
	localsHistory, err := simplelru.NewLRU[string, struct{}](10_000, nil)
	if err != nil {
		return nil, err
	}
	discardHistory, err := simplelru.NewLRU[string, DiscardReason](10_000, nil)
	if err != nil {
		return nil, err
	}

	byNonce := &BySenderAndNonce{
		tree:             btree.NewG[*metaTx](32, SortByNonceLess),
		search:           &metaTx{Tx: &types.TxSlot{}},
		senderIDTxnCount: map[uint64]int{},
	}
	tracedSenders := make(map[common.Address]struct{})
	for _, sender := range cfg.TracedSenders {
		tracedSenders[common.BytesToAddress([]byte(sender))] = struct{}{}
	}

	return &TxPool{
		lock:                    &sync.Mutex{},
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
		londonBlock:             londonBlock,
		shanghaiTime:            shanghaiTime,
		ethCfg:                  ethCfg,
		flushMtx:                &sync.Mutex{},
		aclDB:                   aclDB,
		limbo:                   newLimbo(),
	}, nil
}

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs types.TxSlots, tx kv.Tx) error {
	defer newBlockTimer.UpdateDuration(time.Now())

	isAfterLimbo := len(unwindTxs.Txs) > 0 && p.isDeniedYieldingTransactions()

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
			return fmt.Errorf("loading txs from DB: %w", err)
		}
	}

	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	if assert.Enable {
		if _, err := kvcache.AssertCheckValues(ctx, coreTx, cache); err != nil {
			log.Error("AssertCheckValues", "err", err, "stack", stack.Trace().String())
		}
	}

	if err := minedTxs.Valid(); err != nil {
		return err
	}

	pendingBaseFee, baseFeeChanged := p.setBaseFee(stateChanges.PendingBlockBaseFee, p.ethCfg.AllowFreeTransactions)
	// Update pendingBase for all pool queues and slices
	if baseFeeChanged {
		p.pending.best.pendingBaseFee = pendingBaseFee
		p.pending.worst.pendingBaseFee = pendingBaseFee
		p.baseFee.best.pendingBastFee = pendingBaseFee
		p.baseFee.worst.pendingBaseFee = pendingBaseFee
		p.queued.best.pendingBastFee = pendingBaseFee
		p.queued.worst.pendingBaseFee = pendingBaseFee
	}

	p.addLimboToUnwindTxs(&unwindTxs)

	p.blockGasLimit.Store(stateChanges.BlockGasLimit)
	if err := p.senders.onNewBlock(stateChanges, unwindTxs, minedTxs); err != nil {
		return err
	}

	// No point to validate transactions that have already been executed.
	// It is clear that some of them will not pass the validation, because a unwound tx may depend on another unwound transaction. In this case the depended tx will be discarded
	// Let's all of them pass so they can stay in the queue subpool.
	// _, unwindTxs, err = p.validateTxs(&unwindTxs, cacheView)
	// if err != nil {
	// 	return err
	// }

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

	if err := removeMined(p.all, minedTxs.Txs, p.pending, p.baseFee, p.queued, p.discardLocked); err != nil {
		return err
	}

	blockNum := p.lastSeenBlock.Load()

	sendersWithChangedStateBeforeLimboTrim := prepareSendersWithChangedState(&unwindTxs)
	unwindTxs, limboTxs, forDiscard := p.trimLimboSlots(&unwindTxs)

	//log.Debug("[txpool] new block", "unwinded", len(unwindTxs.txs), "mined", len(minedTxs.txs), "baseFee", baseFee, "blockHeight", blockHeight)

	announcements, err := p.addTxsOnNewBlock(
		blockNum,
		cacheView,
		stateChanges,
		p.senders,
		unwindTxs,
		pendingBaseFee,
		stateChanges.BlockGasLimit,
		p.pending,
		p.baseFee,
		p.queued,
		p.all,
		p.byHash,
		sendersWithChangedStateBeforeLimboTrim,
		p.addLocked,
		p.discardLocked,
	)
	if err != nil {
		return err
	}

	p.pending.EnforceWorstInvariants()
	p.baseFee.EnforceInvariants()
	p.queued.EnforceInvariants()
	p.pending.EnforceBestInvariants()
	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	if p.started.CompareAndSwap(false, true) {
		log.Info("[txpool] Started")
	}

	if p.promoted.Len() > 0 {
		select {
		case p.newPendingTxs <- p.promoted.Copy():
		default:
		}
	}

	for idx, slot := range forDiscard.Txs {
		mt := newMetaTx(slot, forDiscard.IsLocal[idx], blockNum)
		p.discardLocked(mt, DiscardByLimbo)
		log.Info("[txpool] Discarding", "tx-hash", hexutils.BytesToHex(slot.IDHash[:]))
	}
	p.finalizeLimboOnNewBlock(limboTxs)
	if isAfterLimbo {
		p.allowYieldingTransactions()
	}

	//log.Info("[txpool] new block", "number", p.lastSeenBlock.Load(), "pendngBaseFee", pendingBaseFee, "in", time.Since(t))
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

	l := len(p.unprocessedRemoteTxs.Txs)
	if l == 0 {
		return nil
	}

	err = p.senders.registerNewSenders(p.unprocessedRemoteTxs)
	if err != nil {
		return err
	}

	_, newTxs, err := p.validateTxs(p.unprocessedRemoteTxs, cacheView)
	if err != nil {
		return err
	}

	announcements, _, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs,
		p.pendingBaseFee.Load(), p.blockGasLimit.Load(), p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked, true)
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

	//log.Info("[txpool] on new txs", "amount", len(newPendingTxs.txs), "in", time.Since(t))
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
func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
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
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.isLocalLRU.Contains(string(idHash))
}
func (p *TxPool) AddNewGoodPeer(peerID types.PeerID) { p.recentlyConnectedPeers.AddPeer(peerID) }
func (p *TxPool) Started() bool                      { return p.started.Load() }

func (p *TxPool) ResetYieldedStatus() {
	p.lock.Lock()
	defer p.lock.Unlock()
	best := p.pending.best
	for i := 0; i < len(best.ms); i++ {
		best.ms[i].alreadyYielded = false
	}
}

func (p *TxPool) YieldBest(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64, toSkip mapset.Set[[32]byte]) (bool, int, error) {
	return p.best(n, txs, tx, onTopOf, availableGas, availableBlobGas, toSkip)
}

func (p *TxPool) PeekBest(n uint16, txs *types.TxsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64) (bool, error) {
	set := mapset.NewThreadUnsafeSet[[32]byte]()
	onTime, _, err := p.best(n, txs, tx, onTopOf, availableGas, availableBlobGas, set)
	return onTime, err
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pending.Len(), p.baseFee.Len(), p.queued.Len()
}
func (p *TxPool) AddRemoteTxs(_ context.Context, newTxs types.TxSlots) {
	defer addRemoteTxsTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	for i, txn := range newTxs.Txs {
		_, ok := p.unprocessedRemoteByHash[string(txn.IDHash[:])]
		if ok {
			continue
		}
		p.unprocessedRemoteByHash[string(txn.IDHash[:])] = len(p.unprocessedRemoteTxs.Txs)
		p.unprocessedRemoteTxs.Append(txn, newTxs.Senders.At(i), false)
	}
}

func (p *TxPool) validateTx(txn *types.TxSlot, isLocal bool, stateCache kvcache.CacheView, from common.Address) DiscardReason {
	isShanghai := p.isShanghai()
	if isShanghai {
		if txn.DataLen > fixedgas.MaxInitCodeSize {
			return InitCodeTooLarge
		}
	}

	if p.ethCfg.Zk.TxPoolRejectSmartContractDeployments {
		if txn.To == (common.Address{}) {
			return SmartContractDeploymentDisabled
		}
	}

	isLondon := p.isLondon()
	if !isLondon && txn.Type == 0x2 {
		return UnsupportedTx
	}

	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && uint256.NewInt(p.cfg.MinFeeCap).Cmp(&txn.FeeCap) == 1 {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx underpriced idHash=%x local=%t, feeCap=%d, cfg.MinFeeCap=%d", txn.IDHash, isLocal, txn.FeeCap, p.cfg.MinFeeCap))
		}
		return UnderPriced
	}
	gas, reason := CalcIntrinsicGas(uint64(txn.DataLen), uint64(txn.DataNonZeroLen), nil, txn.Creation, true, true, isShanghai)
	if txn.Traced {
		log.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas idHash=%x gas=%d", txn.IDHash, gas))
	}
	if reason != Success {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas calculated failed idHash=%x reason=%s", txn.IDHash, reason))
		}
		return reason
	}
	if gas > txn.Gas {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas > txn.gas idHash=%x gas=%d, txn.gas=%d", txn.IDHash, gas, txn.Gas))
		}
		return IntrinsicGas
	}
	if txn.Gas > transactionGasLimit {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx gas limit too high idHash=%x gas=%d, limit=%d", txn.IDHash, txn.Gas, transactionGasLimit))
		}
		return GasLimitTooHigh
	}

	if !isLocal && uint64(p.all.count(txn.SenderID)) > p.cfg.AccountSlots {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx marked as spamming idHash=%x slots=%d, limit=%d", txn.IDHash, p.all.count(txn.SenderID), p.cfg.AccountSlots))
		}
		return Spammer
	}

	// check nonce and balance
	senderNonce, senderBalance, _ := p.senders.info(stateCache, txn.SenderID)
	if senderNonce > txn.Nonce {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx nonce too low idHash=%x nonce in state=%d, txn.nonce=%d", txn.IDHash, senderNonce, txn.Nonce))
		}
		return NonceTooLow
	}
	// Transactor should have enough funds to cover the costs
	total := uint256.NewInt(txn.Gas)
	total.Mul(total, &txn.FeeCap)
	total.Add(total, &txn.Value)
	if senderBalance.Cmp(total) < 0 {
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: validateTx insufficient funds idHash=%x balance in state=%d, txn.gas*txn.tip=%d", txn.IDHash, senderBalance, total))
		}
		return InsufficientFunds
	}

	switch resolvePolicy(txn) {
	case SendTx:
		var allow bool
		allow, err := p.isActionAllowed(context.TODO(), from, SendTx)
		if err != nil {
			panic(err)
		}
		if !allow {
			return SenderDisallowedSendTx
		}
	case Deploy:
		var allow bool
		// check that sender may deploy contracts
		allow, err := p.isActionAllowed(context.TODO(), from, Deploy)
		if err != nil {
			panic(err)
		}
		if !allow {
			return SenderDisallowedDeploy
		}
	}

	return Success
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
	shanghaiTime := p.shanghaiTime.Uint64()

	// a zero here means shanghai is always active
	if shanghaiTime == 0 {
		p.isPostShanghai.Swap(true)
		return true
	}

	now := big.NewInt(time.Now().Unix())
	is := now.Uint64() >= shanghaiTime
	if is {
		p.isPostShanghai.Swap(true)
	}
	return is
}

func (p *TxPool) isLondon() bool {
	set := p.isPostLondon.Load()
	if set {
		return true
	}
	lbsBig := big.NewInt(0).SetUint64(p.lastSeenBlock.Load())
	if p.londonBlock != nil && p.londonBlock.Cmp(lbsBig) <= 0 {
		p.isPostLondon.Swap(true)
		return true
	}
	return false
}

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
	)
	if len(serializedTxn) > txMaxSize {
		return types.ErrRlpTooBig
	}
	return nil
}
func (p *TxPool) validateTxs(txs *types.TxSlots, stateCache kvcache.CacheView) (reasons []DiscardReason, goodTxs types.TxSlots, err error) {
	// reasons is pre-sized for direct indexing, with the default zero
	// value DiscardReason of NotSet
	reasons = make([]DiscardReason, len(txs.Txs))

	if err := txs.Valid(); err != nil {
		return reasons, goodTxs, err
	}

	goodCount := 0
	for i, txn := range txs.Txs {
		reason := p.validateTx(txn, txs.IsLocal[i], stateCache, txs.Senders.AddressAt(i))
		if reason == Success {
			goodCount++
			// Success here means no DiscardReason yet, so leave it NotSet
			continue
		}
		if reason == Spammer {
			p.punishSpammer(txn.SenderID)
		}
		reasons[i] = reason
	}

	goodTxs.Resize(uint(goodCount))

	j := 0
	for i, txn := range txs.Txs {
		if reasons[i] == NotSet {
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
			p.discardLocked(mt, Spammer) // can't call it while iterating by all
		}
	}
}

func fillDiscardReasons(reasons []DiscardReason, newTxs types.TxSlots, discardReasonsLRU *simplelru.LRU[string, DiscardReason]) []DiscardReason {
	for i := range reasons {
		if reasons[i] != NotSet {
			continue
		}
		reason, ok := discardReasonsLRU.Get(string(newTxs.Txs[i].IDHash[:]))
		if ok {
			reasons[i] = reason
		} else {
			reasons[i] = Success
		}
	}
	return reasons
}

func (p *TxPool) AddLocalTxs(ctx context.Context, newTransactions types.TxSlots, tx kv.Tx) ([]DiscardReason, error) {
	coreTx, err := p.coreDB().BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer coreTx.Rollback()

	cacheView, err := p.cache().View(ctx, coreTx)
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.Started() {
		if err := p.fromDB(ctx, tx, coreTx); err != nil {
			return nil, fmt.Errorf("loading txs from DB: %w", err)
		}
		if p.started.CompareAndSwap(false, true) {
			log.Info("[txpool] Started")
		}
	}

	if err = p.senders.registerNewSenders(&newTransactions); err != nil {
		return nil, err
	}

	reasons, newTxs, err := p.validateTxs(&newTransactions, cacheView)
	if err != nil {
		return nil, err
	}

	announcements, addReasons, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, newTxs,
		p.pendingBaseFee.Load(), p.blockGasLimit.Load(), p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked, true)
	if err == nil {
		for i, reason := range addReasons {
			if reason != NotSet {
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
		if reason == Success {
			txn := newTxs.Txs[i]
			if txn.Traced {
				log.Info(fmt.Sprintf("TX TRACING: AddLocalTxs promotes idHash=%x, senderId=%d", txn.IDHash, txn.SenderID))
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

func (p *TxPool) coreDB() kv.RoDB {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._chainDB
}

func (p *TxPool) cache() kvcache.Cache {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._stateCache
}

func (p *TxPool) addTxs(blockNum uint64, cacheView kvcache.CacheView, senders *sendersBatch,
	newTxs types.TxSlots, pendingBaseFee, blockGasLimit uint64,
	pending *PendingPool, baseFee, queued *SubPool,
	byNonce *BySenderAndNonce, byHash map[string]*metaTx, add func(*metaTx, *types.Announcements) DiscardReason, discard func(*metaTx, DiscardReason), collect bool) (types.Announcements, []DiscardReason, error) {
	protocolBaseFee := calcProtocolBaseFee(pendingBaseFee)
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
	discardReasons := make([]DiscardReason, len(newTxs.Txs))
	announcements := types.Announcements{}
	for i, txn := range newTxs.Txs {
		if found, ok := byHash[string(txn.IDHash[:])]; ok {
			discardReasons[i] = DuplicateHash
			// In case if the transation is stuck, "poke" it to rebroadcast
			if collect && newTxs.IsLocal[i] && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.Tx.Type, found.Tx.Size, found.Tx.IDHash[:])
			}
			continue
		}
		mt := newMetaTx(txn, newTxs.IsLocal[i], blockNum)
		if reason := add(mt, &announcements); reason != NotSet {
			discardReasons[i] = reason
			continue
		}
		discardReasons[i] = NotSet
		if txn.Traced {
			log.Info(fmt.Sprintf("TX TRACING: schedule sendersWithChangedState idHash=%x senderId=%d", txn.IDHash, mt.Tx.SenderID))
		}
		sendersWithChangedState[mt.Tx.SenderID] = struct{}{}
	}

	p.discardOverflowZkCountersFromPending(pending, discard, sendersWithChangedState)

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return announcements, discardReasons, err
		}
		p.onSenderStateChange(senderID, nonce, balance, byNonce,
			protocolBaseFee, blockGasLimit, pending, baseFee, queued, discard)
	}

	promote(pending, baseFee, queued, pendingBaseFee, discard, &announcements)

	return announcements, discardReasons, nil
}

func (p *TxPool) addTxsOnNewBlock(
	blockNum uint64,
	cacheView kvcache.CacheView,
	stateChanges *remote.StateChangeBatch,
	senders *sendersBatch,
	newTxs types.TxSlots,
	pendingBaseFee uint64,
	blockGasLimit uint64,
	pending *PendingPool,
	baseFee,
	queued *SubPool,
	byNonce *BySenderAndNonce,
	byHash map[string]*metaTx,
	sendersWithChangedStateBeforeLimboTrim *LimboSendersWithChangedState,
	add func(*metaTx, *types.Announcements) DiscardReason,
	discard func(*metaTx, DiscardReason),
) (types.Announcements, error) {
	protocolBaseFee := calcProtocolBaseFee(pendingBaseFee)
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
		if _, ok := byHash[string(txn.IDHash[:])]; ok {
			sendersWithChangedStateBeforeLimboTrim.decrement(txn.SenderID)
			continue
		}
		mt := newMetaTx(txn, newTxs.IsLocal[i], blockNum)
		if reason := add(mt, &announcements); reason != NotSet {
			discard(mt, reason)
			sendersWithChangedStateBeforeLimboTrim.decrement(txn.SenderID)
			continue
		}
		sendersWithChangedState[mt.Tx.SenderID] = struct{}{}
	}
	// add senders changed in state to `sendersWithChangedState` list
	for _, changesList := range stateChanges.ChangeBatch {
		for _, change := range changesList.Changes {
			switch change.Action {
			case remote.Action_UPSERT, remote.Action_UPSERT_CODE, remote.Action_REMOVE:
				if change.Incarnation > 0 {
					continue
				}
				addr := gointerfaces.ConvertH160toAddress(change.Address)
				id, ok := senders.getID(addr)
				if !ok {
					sendersWithChangedStateBeforeLimboTrim.decrement(id)
					continue
				}
				sendersWithChangedState[id] = struct{}{}
			}
		}
	}

	for senderId, counter := range sendersWithChangedStateBeforeLimboTrim.Storage {
		if counter > 0 {
			sendersWithChangedState[senderId] = struct{}{}
		}
	}

	p.discardOverflowZkCountersFromPending(pending, discard, sendersWithChangedState)

	for senderID := range sendersWithChangedState {
		nonce, balance, err := senders.info(cacheView, senderID)
		if err != nil {
			return announcements, err
		}
		p.onSenderStateChange(senderID, nonce, balance, byNonce,
			protocolBaseFee, blockGasLimit, pending, baseFee, queued, discard)
	}

	promote(pending, baseFee, queued, pendingBaseFee, discard, &announcements)

	return announcements, nil
}

func (p *TxPool) setBaseFee(baseFee uint64, allowFreeTransactions bool) (uint64, bool) {
	changed := false
	changed = baseFee != p.pendingBaseFee.Load()
	p.pendingBaseFee.Store(baseFee)
	if allowFreeTransactions {
		changed = uint64(0) != p.pendingBaseFee.Load()
		p.pendingBaseFee.Store(0)
		return 0, changed
	}

	changed = baseFee != p.pendingBaseFee.Load()
	p.pendingBaseFee.Store(baseFee)

	return p.pendingBaseFee.Load(), changed
}

func (p *TxPool) addLocked(mt *metaTx, announcements *types.Announcements) DiscardReason {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.all.get(mt.Tx.SenderID, mt.Tx.Nonce)
	if found != nil {
		tipThreshold := uint256.NewInt(0)
		tipThreshold = tipThreshold.Mul(&found.Tx.Tip, uint256.NewInt(100+p.cfg.PriceBump))
		tipThreshold.Div(tipThreshold, u256.N100)
		feecapThreshold := uint256.NewInt(0)
		feecapThreshold.Mul(&found.Tx.FeeCap, uint256.NewInt(100+p.cfg.PriceBump))
		feecapThreshold.Div(feecapThreshold, u256.N100)
		if mt.Tx.Tip.Cmp(tipThreshold) < 0 || mt.Tx.FeeCap.Cmp(feecapThreshold) < 0 {
			// Both tip and feecap need to be larger than previously to replace the transaction
			// In case if the transation is stuck, "poke" it to rebroadcast
			if mt.subPool&IsLocal != 0 && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.Tx.Type, found.Tx.Size, found.Tx.IDHash[:])
			}
			if bytes.Equal(found.Tx.IDHash[:], mt.Tx.IDHash[:]) {
				return NotSet
			}
			return NotReplaced
		}

		switch found.currentSubPool {
		case PendingSubPool:
			p.pending.Remove(found)
		case BaseFeeSubPool:
			p.baseFee.Remove(found)
		case QueuedSubPool:
			p.queued.Remove(found)
		default:
			//already removed
		}

		p.discardLocked(found, ReplacedByHigherTip)
	} else if p.pending.IsFull() {
		// new transaction will be denied if pending pool is full unless it will replace an old transaction
		return PendingPoolOverflow
	}

	p.byHash[string(mt.Tx.IDHash[:])] = mt

	if replaced := p.all.replaceOrInsert(mt); replaced != nil {
		if assert.Enable {
			panic("must never happen")
		}
	}

	if mt.subPool&IsLocal != 0 {
		p.isLocalLRU.Add(string(mt.Tx.IDHash[:]), struct{}{})
	}
	// All transactions are first added to the queued pool and then immediately promoted from there if required
	p.queued.Add(mt)
	return NotSet
}

// dropping transaction from all sub-structures and from db
// Important: don't call it while iterating by all
func (p *TxPool) discardLocked(mt *metaTx, reason DiscardReason) {
	delete(p.byHash, string(mt.Tx.IDHash[:]))
	p.deletedTxs = append(p.deletedTxs, mt)
	p.all.delete(mt)
	p.discardReasonsLRU.Add(string(mt.Tx.IDHash[:]), reason)
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

func (p *TxPool) LockFlusher() {
	p.flushMtx.Lock()
}

func (p *TxPool) UnlockFlusher() {
	p.flushMtx.Unlock()
}

// removeMined - apply new highest block (or batch of blocks)
//
// 1. New best block arrives, which potentially changes the balance and the nonce of some senders.
// We use senderIds data structure to find relevant senderId values, and then use senders data structure to
// modify state_balance and state_nonce, potentially remove some elements (if transaction with some nonce is
// included into a block), and finally, walk over the transaction records and update SubPool fields depending on
// the actual presence of nonce gaps and what the balance is.
func removeMined(byNonce *BySenderAndNonce, minedTxs []*types.TxSlot, pending *PendingPool, baseFee, queued *SubPool, discard func(*metaTx, DiscardReason)) error {
	noncesToRemove := map[uint64]uint64{}
	for _, txn := range minedTxs {
		nonce, ok := noncesToRemove[txn.SenderID]
		if !ok || txn.Nonce > nonce {
			noncesToRemove[txn.SenderID] = txn.Nonce
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
			if mt.Tx.Nonce > nonce {
				return false
			}
			if mt.Tx.Traced {
				log.Info(fmt.Sprintf("TX TRACING: removeMined idHash=%x senderId=%d, currentSubPool=%s", mt.Tx.IDHash, mt.Tx.SenderID, mt.currentSubPool))
			}
			toDel = append(toDel, mt)
			// del from sub-pool
			switch mt.currentSubPool {
			case PendingSubPool:
				pending.Remove(mt)
			case BaseFeeSubPool:
				baseFee.Remove(mt)
			case QueuedSubPool:
				queued.Remove(mt)
			default:
				//already removed
			}
			return true
		})

		for _, mt := range toDel {
			discard(mt, Mined)
		}
		toDel = toDel[:0]
	}
	return nil
}

// promote reasserts invariants of the subpool and returns the list of transactions that ended up
// being promoted to the pending or basefee pool, for re-broadcasting
func promote(pending *PendingPool, baseFee, queued *SubPool, pendingBaseFee uint64, discard func(*metaTx, DiscardReason), announcements *types.Announcements) {
	// Demote worst transactions that do not qualify for pending sub pool anymore, to other sub pools, or discard
	for worst := pending.Worst(); pending.Len() > 0 && (worst.subPool < BaseFeePoolBits || worst.minFeeCap.Cmp(uint256.NewInt(pendingBaseFee)) < 0); worst = pending.Worst() {
		if worst.subPool >= BaseFeePoolBits {
			tx := pending.PopWorst()
			announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
			baseFee.Add(tx)
		} else if worst.subPool >= QueuedPoolBits {
			queued.Add(pending.PopWorst())
		} else {
			discard(pending.PopWorst(), FeeTooLow)
		}
	}

	// Promote best transactions from base fee pool to pending pool while they qualify
	for best := baseFee.Best(); baseFee.Len() > 0 && best.subPool >= BaseFeePoolBits && best.minFeeCap.Cmp(uint256.NewInt(pendingBaseFee)) >= 0; best = baseFee.Best() {
		tx := baseFee.PopBest()
		announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
		pending.Add(tx)
	}

	// Demote worst transactions that do not qualify for base fee pool anymore, to queued sub pool, or discard
	for worst := baseFee.Worst(); baseFee.Len() > 0 && worst.subPool < BaseFeePoolBits; worst = baseFee.Worst() {
		if worst.subPool >= QueuedPoolBits {
			queued.Add(baseFee.PopWorst())
		} else {
			discard(baseFee.PopWorst(), FeeTooLow)
		}
	}

	// Promote best transactions from the queued pool to either pending or base fee pool, while they qualify
	for best := queued.Best(); queued.Len() > 0 && best.subPool >= BaseFeePoolBits; best = queued.Best() {
		if best.minFeeCap.Cmp(uint256.NewInt(pendingBaseFee)) >= 0 {
			tx := queued.PopBest()
			announcements.Append(tx.Tx.Type, tx.Tx.Size, tx.Tx.IDHash[:])
			pending.Add(tx)
		} else {
			baseFee.Add(queued.PopBest())
		}
	}

	// Discard worst transactions from the queued sub pool if they do not qualify
	for worst := queued.Worst(); queued.Len() > 0 && worst.subPool < QueuedPoolBits; worst = queued.Worst() {
		discard(queued.PopWorst(), FeeTooLow)
	}

	// Discard worst transactions from pending pool until it is within capacity limit
	for pending.Len() > pending.limit {
		discard(pending.PopWorst(), PendingPoolOverflow)
	}

	// Discard worst transactions from pending sub pool until it is within capacity limits
	for baseFee.Len() > baseFee.limit {
		discard(baseFee.PopWorst(), BaseFeePoolOverflow)
	}

	// Discard worst transactions from the queued sub pool until it is within its capacity limits
	for _ = queued.Worst(); queued.Len() > queued.limit; _ = queued.Worst() {
		discard(queued.PopWorst(), QueuedPoolOverflow)
	}
}

// MainLoop - does:
// send pending byHash to p2p:
//   - new byHash
//   - all pooled byHash to recently connected peers
//   - all local pooled byHash to random peers periodically
//
// promote/demote transactions
// reorgs
func MainLoop(ctx context.Context, db kv.RwDB, coreDB kv.RoDB, p *TxPool, newTxs chan types.Announcements, send *Send, newSlotsStreams *NewSlotsStreams, notifyMiningAboutNewSlots func()) {
	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxsEvery := time.NewTicker(p.cfg.ProcessRemoteTxsEvery)
	defer processRemoteTxsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			p.LockFlusher()
			innerContext, innerContextcancel := context.WithCancel(context.Background())
			written, err := p.flush(innerContext, db)
			if err != nil {
				log.Error("[txpool] flush is local history", "err", err)
			} else {
				writeToDBBytesCounter.Set(written)
			}
			innerContextcancel()
			p.UnlockFlusher()
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

				log.Error("[txpool] process batch remote txs", "err", err)
			}
		case <-commitEvery.C:
			if db != nil && p.Started() {
				t := time.Now()
				p.LockFlusher()
				written, err := p.flush(ctx, db)
				p.UnlockFlusher()
				if err != nil {
					log.Error("[txpool] flush is local history", "err", err)
					continue
				}
				writeToDBBytesCounter.Set(written)
				log.Debug("[txpool] Commit", "written_kb", written/1024, "in", time.Since(t))
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
				defer propagateNewTxsTimer.UpdateDuration(time.Now())

				announcements = announcements.DedupCopy()

				notifyMiningAboutNewSlots()

				var localTxTypes []byte
				var localTxSizes []uint32
				var localTxHashes types.Hashes
				var localTxRlps [][]byte
				var remoteTxTypes []byte
				var remoteTxSizes []uint32
				var remoteTxHashes types.Hashes
				var remoteTxRlps [][]byte
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

						// Empty rlp can happen if a transaction we want to broadcase has just been mined, for example
						slotsRlp = append(slotsRlp, slotRlp)
						if p.IsLocal(hash) {
							localTxTypes = append(localTxTypes, t)
							localTxSizes = append(localTxSizes, size)
							localTxHashes = append(localTxHashes, hash...)
							localTxRlps = append(localTxRlps, slotRlp)
						} else {
							remoteTxTypes = append(remoteTxTypes, t)
							remoteTxSizes = append(remoteTxSizes, size)
							remoteTxHashes = append(remoteTxHashes, hash...)
							remoteTxRlps = append(remoteTxRlps, slotRlp)
						}
					}
					return nil
				}); err != nil {
					log.Error("[txpool] collect info to propagate", "err", err)
					return
				}
				if newSlotsStreams != nil {
					newSlotsStreams.Broadcast(&proto_txpool.OnAddReply{RplTxs: slotsRlp})
				}

				// first broadcast all local txs to all peers, then non-local to random sqrt(peersAmount) peers
				txSentTo := send.BroadcastPooledTxs(localTxRlps)
				hashSentTo := send.AnnouncePooledTxs(localTxTypes, localTxSizes, localTxHashes)
				for i := 0; i < localTxHashes.Len(); i++ {
					hash := localTxHashes.At(i)
					log.Debug("local tx propagated", "tx_hash", hex.EncodeToString(hash), "announced to peers", hashSentTo[i], "broadcast to peers", txSentTo[i], "baseFee", p.pendingBaseFee.Load())
				}
				send.BroadcastPooledTxs(remoteTxRlps)
				send.AnnouncePooledTxs(remoteTxTypes, remoteTxSizes, remoteTxHashes)
			}()
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			t := time.Now()
			var hashes types.Hashes
			var types []byte
			var sizes []uint32
			types, sizes, hashes = p.AppendAllAnnouncements(types, sizes, hashes[:0])
			go send.PropagatePooledTxsToPeersList(newPeers, types, sizes, hashes)
			propagateToNewPeerTimer.UpdateDuration(t)
		}
	}
}

func (p *TxPool) flush(ctx context.Context, db kv.RwDB) (written uint64, err error) {
	defer writeToDBTimer.UpdateDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db tx is done inside lock, to make last writes visible for all read operations
	if err := db.Update(ctx, func(tx kv.RwTx) error {
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
			log.Warn("[txpool] flush: sender address not found by ID", "senderID", metaTx.Tx.SenderID)
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
	if err := PutLastSeenBlock(tx, p.lastSeenBlock.Load(), encID); err != nil {
		return err
	}
	if err := p.flushLockedLimbo(tx); err != nil {
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

	cacheView, err := p._stateCache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	if err = p.fromDBLimbo(ctx, tx, cacheView); err != nil {
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

		_, err = parseCtx.ParseTransaction(txRlp, 0, txn, nil, false /* hasEnvelope */, false, nil)
		if err != nil {
			err = fmt.Errorf("err: %w, rlp: %x", err, txRlp)
			log.Warn("[txpool] fromDB: parseTransaction", "err", err)
			continue
		}
		txn.Rlp = nil // means that we don't need store it in db anymore

		txn.SenderID, txn.Traced = p.senders.getOrCreateID(addr)
		binary.BigEndian.Uint64(v)

		isLocalTx := p.isLocalLRU.Contains(string(k))

		if reason := p.validateTx(txn, isLocalTx, cacheView, addr); reason != NotSet && reason != Success {
			continue
		}
		txs.Resize(uint(i + 1))
		txs.Txs[i] = txn
		txs.IsLocal[i] = isLocalTx
		copy(txs.Senders.At(i), addr[:])
		i++
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
	err = p.senders.registerNewSenders(&txs)
	if err != nil {
		return err
	}
	if _, _, err := p.addTxs(p.lastSeenBlock.Load(), cacheView, p.senders, txs,
		pendingBaseFee, math.MaxUint64 /* blockGasLimit */, p.pending, p.baseFee, p.queued, p.all, p.byHash, p.addLocked, p.discardLocked, false); err != nil {
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
	if !p.started.Load() {
		//log.Info("[txpool] Not started yet, waiting for new blocks...")
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
	log.Info("[txpool] stat", ctx...)
	pendingSubCounter.Set(uint64(p.pending.Len()))
	basefeeSubCounter.Set(uint64(p.baseFee.Len()))
	queuedSubCounter.Set(uint64(p.queued.Len()))
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
				log.Warn("[txpool] foreach: get tx from db", "err", err)
				return true
			}
			if v == nil {
				log.Warn("[txpool] foreach: tx not found in db")
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

// CalcIntrinsicGas computes the 'intrinsic gas' for a message with the given data.
func CalcIntrinsicGas(dataLen, dataNonZeroLen uint64, accessList types.AccessList, isContractCreation, isHomestead, isEIP2028, isShanghai bool) (uint64, DiscardReason) {
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

		product, overflow := emath.SafeMul(nz, nonZeroGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		z := dataLen - nz

		product, overflow = emath.SafeMul(z, fixedgas.TxDataZeroGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		if isContractCreation && isShanghai {
			numWords := toWordSize(dataLen)
			product, overflow = emath.SafeMul(numWords, fixedgas.InitCodeWordGas)
			if overflow {
				return 0, GasUintOverflow
			}
			gas, overflow = emath.SafeAdd(gas, product)
			if overflow {
				return 0, GasUintOverflow
			}
		}
	}
	if accessList != nil {
		product, overflow := emath.SafeMul(uint64(len(accessList)), fixedgas.TxAccessListAddressGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}

		product, overflow = emath.SafeMul(uint64(accessList.StorageKeys()), fixedgas.TxAccessListStorageKeyGas)
		if overflow {
			return 0, GasUintOverflow
		}
		gas, overflow = emath.SafeAdd(gas, product)
		if overflow {
			return 0, GasUintOverflow
		}
	}
	return gas, Success
}

// toWordSize returns the ceiled word size required for memory expansion.
func toWordSize(size uint64) uint64 {
	if size > math.MaxUint64-31 {
		return math.MaxUint64/32 + 1
	}
	return (size + 31) / 32
}

var PoolChainConfigKey = []byte("chain_config")
var PoolLastSeenBlockKey = []byte("last_seen_block")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")

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
// flushing to db periodicaly. it doesn't play as read-cache (because db is small and memory-mapped - doesn't need cache)
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
func (sc *sendersBatch) getOrCreateID(addr common.Address) (uint64, bool) {
	_, traced := sc.tracedSenders[addr]
	id, ok := sc.senderIDs[addr]
	if !ok {
		sc.senderID++
		id = sc.senderID
		sc.senderIDs[addr] = id
		sc.senderID2Addr[id] = addr
		if traced {
			log.Info(fmt.Sprintf("TX TRACING: allocated senderID %d to sender %x", id, addr))
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

func (sc *sendersBatch) registerNewSenders(newTxs *types.TxSlots) (err error) {
	for i, txn := range newTxs.Txs {
		txn.SenderID, txn.Traced = sc.getOrCreateID(newTxs.Senders.AddressAt(i))
	}
	return nil
}
func (sc *sendersBatch) onNewBlock(stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs types.TxSlots) error {
	for _, diff := range stateChanges.ChangeBatch {
		for _, change := range diff.Changes { // merge state changes
			addrB := gointerfaces.ConvertH160toAddress(change.Address)
			sc.getOrCreateID(addrB)
		}

		for i, txn := range unwindTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(unwindTxs.Senders.AddressAt(i))
		}

		for i, txn := range minedTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(minedTxs.Senders.AddressAt(i))
		}
	}
	return nil
}

// BySenderAndNonce - designed to perform most expensive operation in TxPool:
// "recalculate all ephemeral fields of all transactions" by algo
//   - for all senders - iterate over all transactions in nonce growing order
//
// Performane decisions:
//   - All senders stored inside 1 large BTree - because iterate over 1 BTree is faster than over map[senderId]BTree
//   - sortByNonce used as non-pointer wrapper - because iterate over BTree of pointers is 2x slower
type BySenderAndNonce struct {
	tree             *btree.BTreeG[*metaTx]
	search           *metaTx
	senderIDTxnCount map[uint64]int // count of sender's txns in the pool - may differ from nonce
}

func (b *BySenderAndNonce) nonce(senderID uint64) (nonce uint64, ok bool) {
	s := b.search
	s.Tx.SenderID = senderID
	s.Tx.Nonce = math.MaxUint64

	b.tree.DescendLessOrEqual(s, func(mt *metaTx) bool {
		if mt.currentSubPool != PendingSubPool {
			// we only want to include transactions that are in the pending pool.  TXs in the queued pool
			// artificially increase the "pending" call which can cause transactions to just stack up
			// when libraries use eth_getTransactionCount "pending" for the next tx nonce - a common thing
			return true
		}
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
func (b *BySenderAndNonce) delete(mt *metaTx) {
	if _, ok := b.tree.Delete(mt); ok {
		senderID := mt.Tx.SenderID
		count := b.senderIDTxnCount[senderID]
		if count > 1 {
			b.senderIDTxnCount[senderID] = count - 1
		} else {
			delete(b.senderIDTxnCount, senderID)
		}
	}
}
func (b *BySenderAndNonce) replaceOrInsert(mt *metaTx) *metaTx {
	it, ok := b.tree.ReplaceOrInsert(mt)
	if ok {
		return it
	}
	b.senderIDTxnCount[mt.Tx.SenderID]++
	return nil
}

// PendingPool - is different from other pools - it's best is Slice instead of Heap
// It's more expensive to maintain "slice sort" invariant, but it allow do cheap copy of
// pending.best slice for mining (because we consider txs and metaTx are immutable)
type PendingPool struct {
	sorted bool // means `PendingPool.best` is sorted or not
	best   *bestSlice
	worst  *WorstQueue
	limit  int
	t      SubPoolType
}

func NewPendingSubPool(t SubPoolType, limit int) *PendingPool {
	return &PendingPool{limit: limit, t: t, best: &bestSlice{ms: []*metaTx{}}, worst: &WorstQueue{ms: []*metaTx{}}}
}

// bestSlice - is similar to best queue, but with O(n log n) complexity and
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
	if !p.sorted {
		sort.Sort(p.best)
		p.sorted = true
	}
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
func (p *PendingPool) Len() int     { return len(p.best.ms) }
func (p *PendingPool) IsFull() bool { return p.Len() >= p.limit }
func (p *PendingPool) Remove(i *metaTx) {
	if i.worstIndex >= 0 {
		heap.Remove(p.worst, i.worstIndex)
	}
	if i.bestIndex >= 0 {
		p.best.UnsafeRemove(i)
	}
	if i.bestIndex != p.best.Len()-1 {
		p.sorted = false
	}
	i.currentSubPool = 0
}

func (p *PendingPool) Add(i *metaTx) {
	if i.Tx.Traced {
		log.Info(fmt.Sprintf("TX TRACING: moved to subpool %s, IdHash=%x, sender=%d", p.t, i.Tx.IDHash, i.Tx.SenderID))
	}
	i.currentSubPool = p.t
	heap.Push(p.worst, i)
	p.best.UnsafeAdd(i)
	p.sorted = false
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
func (p *SubPool) Add(i *metaTx) {
	if i.Tx.Traced {
		log.Info(fmt.Sprintf("TX TRACING: moved to subpool %s, IdHash=%x, sender=%d", p.t, i.Tx.IDHash, i.Tx.SenderID))
	}
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
