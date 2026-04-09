// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// txMaxBroadcastSize is the max size of a transaction that will be broadcast.
// All transactions with a higher size will be announced and need to be fetched
// by the peer.
const txMaxBroadcastSize = 4 * 1024

// Pool is interface for the transaction pool
// This interface exists for the convenience of testing, and not yet because
// there are multiple implementations
//
//go:generate mockgen -typed=true -destination=./pool_mock.go -package=txpool . Pool
type Pool interface {
	ValidateSerializedTxn(serializedTxn []byte) error

	// Handle 3 main events - new remote txns from p2p, new local txns from RPC, new blocks from execution layer
	AddRemoteTxns(ctx context.Context, newTxns TxnSlots)
	AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remoteproto.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) error
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	FilterKnownIdHashes(tx kv.Tx, hashes Hashes) (unknownHashes Hashes, err error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)
	GetBlobs(blobhashes []common.Hash) []PoolBlobBundle
	AddNewGoodPeer(peerID PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check
var _ txnprovider.TxnProvider = (*TxPool)(nil)

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// txpool doesn't start any goroutines - "leave concurrency to user" design
// txpool has no DB-TX fields - "leave db transactions management to user" design
// txpool has _chainDB field - but it must maximize local state cache hit-rate - and perform minimum _chainDB transactions
//
// It preserve TxnSlot objects immutable
type TxPool struct {
	_chainDB               kv.TemporalRoDB // remote db - use it wisely
	_stateCache            kvcache.Cache
	poolDB                 kv.RwDB
	lock                   *sync.Mutex
	recentlyConnectedPeers *recentlyConnectedPeers // all txns will be propagated to this peers eventually, and clear list
	senders                *sendersBatch
	// batch processing of remote transactions
	// handling is fast enough without batching, but batching allows:
	//   - fewer _chainDB transactions
	//   - batch notifications about new txns (reduced P2P spam to other nodes about txns propagation)
	//   - and as a result reducing lock contention
	unprocessedRemoteTxns   *TxnSlots
	unprocessedRemoteByHash map[string]int                                  // to reject duplicates
	byHash                  map[string]*metaTxn                             // txn_hash => txn : only those records not committed to db yet
	discardReasonsLRU       *simplelru.LRU[string, txpoolcfg.DiscardReason] // txn_hash => discard_reason : non-persisted
	pending                 *PendingPool
	baseFee                 *SubPool
	queued                  *SubPool
	minedBlobTxnsByBlock    map[uint64][]*metaTxn            // (blockNum => slice): cache of recently mined blobs
	minedBlobTxnsByHash     map[string]*metaTxn              // (hash => mt): map of recently mined blobs
	isLocalLRU              *simplelru.LRU[string, struct{}] // txn_hash => is_local : to restore isLocal flag of unwinded transactions
	newPendingTxns          chan Announcements               // notifications about new txns in Pending sub-pool
	all                     *BySenderAndNonce                // senderID => (sorted map of txn nonce => *metaTxn)
	deletedTxns             []*metaTxn                       // list of discarded txns since last db commit
	promoted                Announcements
	cfg                     txpoolcfg.Config
	chainID                 uint256.Int
	chainConfig             *chain.Config
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
	bhilaiBlock             *uint64
	isPostBhilai            atomic.Bool
	cancunTime              *uint64
	isPostCancun            atomic.Bool
	pragueTime              *uint64
	isPostPrague            atomic.Bool
	osakaTime               *uint64
	isPostOsaka             atomic.Bool
	amsterdamTime           *uint64
	isPostAmsterdam         atomic.Bool
	feeCalculator           FeeCalculator
	p2pFetcher              *Fetch
	p2pSender               *Send
	newSlotsStreams         *NewSlotsStreams
	ethBackend              remoteproto.ETHBACKENDClient
	builderNotifyNewTxns    func()
	logger                  log.Logger
	auths                   map[AuthAndNonce]*metaTxn // All authority accounts with a pooled authorization
	blobHashToTxn           map[common.Hash]struct {
		index   int
		txnHash common.Hash
	}

	// Dormancy eviction: tracks the block number of the last on-chain state change (nonce or
	// balance) for each sender that has transactions in the queued sub-pool. Senders absent
	// from on-chain activity for longer than cfg.QueuedDormancyDuration are evicted. The timer
	// resets only on real on-chain state changes — not on new transaction submissions — so
	// senders cannot game it by re-submitting transactions.
	senderLastActivity   map[uint64]uint64 // senderID → block of last on-chain state change
	avgBlockTimeMs       atomic.Int64      // EWMA of block-to-block wall-clock interval (ms); default 12 000
	lastBlockTimestampMs atomic.Int64      // unix-ms timestamp of the last processed block
}

type ValidateAA interface {
	ValidateAA() (bool, error)
}

type FeeCalculator interface {
	CurrentFees(chainConfig *chain.Config, db kv.Getter) (baseFee uint64, blobFee uint64, minBlobGasPrice, blockGasLimit uint64, err error)
}

func New(
	ctx context.Context,
	newTxns chan Announcements,
	poolDB kv.RwDB,
	chainDB kv.TemporalRoDB,
	cfg txpoolcfg.Config,
	cache kvcache.Cache,
	chainConfig *chain.Config,
	sentryClients []sentryproto.SentryClient,
	stateChangesClient StateChangesClient,
	builderNotifyNewTxns func(),
	newSlotsStreams *NewSlotsStreams,
	ethBackend remoteproto.ETHBACKENDClient,
	logger log.Logger,
	opts ...Option,
) (*TxPool, error) {
	options := applyOpts(opts...)
	localsHistory, err := simplelru.NewLRU[string, struct{}](10_000, nil)
	if err != nil {
		return nil, err
	}
	discardHistory, err := simplelru.NewLRU[string, txpoolcfg.DiscardReason](10_000, nil)
	if err != nil {
		return nil, err
	}

	byNonce := &BySenderAndNonce{
		tree:              btree.NewG[*metaTxn](32, SortByNonceLess),
		search:            &metaTxn{TxnSlot: &TxnSlot{}},
		senderIDTxnCount:  map[uint64]int{},
		senderIDBlobCount: map[uint64]uint64{},
	}
	tracedSenders := make(map[common.Address]struct{})
	for _, sender := range cfg.TracedSenders {
		tracedSenders[common.BytesToAddress([]byte(sender))] = struct{}{}
	}

	configChainID, overflow := uint256.FromBig(chainConfig.ChainID)
	if overflow {
		return nil, errors.New("chainID overflow")
	}

	lock := &sync.Mutex{}

	res := &TxPool{
		lock:                    lock,
		lastSeenCond:            sync.NewCond(lock),
		byHash:                  map[string]*metaTxn{},
		isLocalLRU:              localsHistory,
		discardReasonsLRU:       discardHistory,
		all:                     byNonce,
		recentlyConnectedPeers:  &recentlyConnectedPeers{},
		pending:                 NewPendingSubPool(PendingSubPool, cfg.PendingSubPoolLimit),
		baseFee:                 NewSubPool(BaseFeeSubPool, cfg.BaseFeeSubPoolLimit),
		queued:                  NewSubPool(QueuedSubPool, cfg.QueuedSubPoolLimit),
		newPendingTxns:          newTxns,
		_stateCache:             cache,
		senders:                 newSendersBatch(tracedSenders),
		poolDB:                  poolDB,
		_chainDB:                chainDB,
		cfg:                     cfg,
		chainID:                 *configChainID,
		chainConfig:             chainConfig,
		unprocessedRemoteTxns:   &TxnSlots{},
		unprocessedRemoteByHash: map[string]int{},
		minedBlobTxnsByBlock:    map[uint64][]*metaTxn{},
		minedBlobTxnsByHash:     map[string]*metaTxn{},
		feeCalculator:           options.feeCalculator,
		ethBackend:              ethBackend,
		builderNotifyNewTxns:    builderNotifyNewTxns,
		newSlotsStreams:         newSlotsStreams,
		logger:                  logger,
		auths:                   make(map[AuthAndNonce]*metaTxn),
		blobHashToTxn: make(map[common.Hash]struct {
			index   int
			txnHash common.Hash
		}),
		senderLastActivity: make(map[uint64]uint64),
	}
	// Seed the EWMA block time with 12 s (Ethereum mainnet slot time). The tracker adjusts
	// automatically after a few blocks, so the seed only affects the very first sweep interval.
	res.avgBlockTimeMs.Store(12_000)

	res.shanghaiTime = chainConfig.ShanghaiTime
	if chainConfig.Bor != nil {
		res.agraBlock = chainConfig.Bor.GetAgraBlock()
		res.bhilaiBlock = chainConfig.Bor.GetBhilaiBlock()
	}
	res.cancunTime = chainConfig.CancunTime
	res.pragueTime = chainConfig.PragueTime
	res.osakaTime = chainConfig.OsakaTime
	res.amsterdamTime = chainConfig.AmsterdamTime

	res.p2pFetcher = NewFetch(ctx, sentryClients, res, stateChangesClient, poolDB, res.chainID, logger, opts...)
	res.p2pSender = NewSend(ctx, sentryClients, logger, opts...)

	return res, nil
}

func (p *TxPool) start(ctx context.Context) error {
	if p.started.Load() {
		return nil
	}

	return p.poolDB.View(ctx, func(tx kv.Tx) error {
		coreDb, _ := p.chainDB()
		coreTx, err := coreDb.BeginTemporalRo(ctx)
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

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remoteproto.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) error {
	defer newBlockTimer.ObserveDuration(time.Now())

	coreDB, cache := p.chainDB()
	cache.OnNewBlock(stateChanges)
	coreTx, err := coreDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()

	block := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHeight
	baseFee := stateChanges.PendingBlockBaseFee

	// Update EWMA block time (α=0.1) using wall-clock measurements between consecutive blocks.
	// Gaps >10 min are ignored: they indicate the node was paused, restarted, or fast-syncing
	// (during fast-sync the txpool receives block batches with large wall-clock gaps between
	// them, so individual inter-block deltas are not meaningful). As a result the EWMA only
	// tracks real-time block arrivals at chain tip. Seeded at 12 s (Ethereum mainnet slot time);
	// converges to actual observed block time (~13 s on mainnet due to missed slots) after ~30
	// chain-tip blocks. Done outside the lock — atomics only.
	nowMs := time.Now().UnixMilli()
	if prev := p.lastBlockTimestampMs.Load(); prev > 0 {
		if delta := nowMs - prev; delta > 0 && delta < 600_000 {
			p.avgBlockTimeMs.Store((9*p.avgBlockTimeMs.Load() + delta) / 10)
		}
	}
	p.lastBlockTimestampMs.Store(nowMs)

	if err = minedTxns.Valid(); err != nil {
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

	pendingPre := p.pending.Len()
	defer func() {

		p.logger.Debug("[txpool] New block", "block", block,
			"unwound", len(unwindTxns.Txns), "mined", len(minedTxns.Txns), "blockBaseFee", baseFee,
			"pending-pre", pendingPre, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len(),
			"err", err)
	}()

	if assert.Enable {
		if _, err := kvcache.AssertCheckValues(ctx, coreTx, cache); err != nil {
			p.logger.Error("AssertCheckValues", "err", err, "stack", stack.Trace().String())
		}
	}

	pendingBaseFee, baseFeeChanged := p.setBaseFee(baseFee)
	if baseFeeChanged {
		var pendingBaseFee256 uint256.Int
		pendingBaseFee256.SetUint64(pendingBaseFee)
		p.setPendingBaseFeeOnQueues(pendingBaseFee256)
	}

	pendingBlobFee := stateChanges.PendingBlobFeePerGas
	p.setBlobFee(pendingBlobFee)

	oldGasLimit := p.blockGasLimit.Swap(stateChanges.BlockGasLimit)
	if oldGasLimit != stateChanges.BlockGasLimit {
		p.all.ascendAll(func(mt *metaTxn) bool {
			if mt.TxnSlot.GetGas() < stateChanges.BlockGasLimit {
				mt.subPool |= NotTooMuchGas
			} else {
				mt.subPool &^= NotTooMuchGas
			}
			p.updateInSubPool(mt)
			return true
		})
	}

	for i, txn := range unwindBlobTxns.Txns {
		if txn.TxType() == BlobTxnType {
			knownBlobTxn, err := p.getCachedBlobTxnLocked(coreTx, txn.IDHash[:])
			if err != nil {
				return err
			}
			if knownBlobTxn != nil {
				unwindTxns.Append(knownBlobTxn.TxnSlot, unwindBlobTxns.Senders.At(i), false)
			}
		}
	}
	if err = p.senders.onNewBlock(stateChanges, unwindTxns, minedTxns, p.logger); err != nil {
		return err
	}

	_, unwindTxns, err = p.validateTxns(&unwindTxns, cacheView)
	if err != nil {
		return err
	}

	if assert.Enable {
		for _, txn := range unwindTxns.Txns {
			if txn.SenderID == 0 {
				panic("onNewBlock.unwindTxns: senderID can't be zero")
			}
		}
		for _, txn := range minedTxns.Txns {
			if txn.SenderID == 0 {
				panic("onNewBlock.minedTxns: senderID can't be zero")
			}
		}
	}

	if err = p.processMinedFinalizedBlobs(minedTxns.Txns, stateChanges.FinalizedBlock); err != nil {
		return err
	}

	if err = p.removeMined(p.all, minedTxns.Txns); err != nil {
		return err
	}

	var announcements Announcements
	announcements, err = p.addTxnsOnNewBlock(block, cacheView, stateChanges, p.senders, unwindTxns, /* newTxns */
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
		case p.newPendingTxns <- p.promoted.Copy():
		default:
		}
	}

	return nil
}

func (p *TxPool) processRemoteTxns(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, stack.Trace().String())
		}
	}()

	if !p.Started() {
		return errors.New("txpool not started yet")
	}

	defer processBatchTxnsTimer.ObserveDuration(time.Now())
	coreDB, cache := p.chainDB()
	coreTx, err := coreDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()
	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	l := len(p.unprocessedRemoteTxns.Txns)
	if l == 0 {
		return nil
	}

	err = p.senders.registerNewSenders(p.unprocessedRemoteTxns, p.logger)
	if err != nil {
		return err
	}

	_, newTxns, err := p.validateTxns(p.unprocessedRemoteTxns, cacheView)
	if err != nil {
		return err
	}

	announcements, reasons, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, newTxns,
		p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), p.blockGasLimit.Load(), true, p.logger)
	if err != nil {
		return err
	}

	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	reasons = fillDiscardReasons(reasons, newTxns, p.discardReasonsLRU)
	for i, reason := range reasons {
		txn := newTxns.Txns[i]

		if reason == txpoolcfg.Success {

			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: processRemoteTxns promotes idHash=%x, senderId=%d", txn.IDHash, txn.SenderID))
			}
			p.promoted.Append(txn.TxType(), txn.Size, txn.IDHash[:])
		}
	}

	if p.promoted.Len() > 0 {
		copied := p.promoted.Copy()
		select {
		case <-ctx.Done():
			return nil
		case p.newPendingTxns <- copied:
		default:
		}
	}

	p.unprocessedRemoteTxns.Resize(0)
	p.unprocessedRemoteByHash = map[string]int{}

	return nil
}

func (p *TxPool) getRlpLocked(tx kv.Tx, hash []byte) (rlpTxn []byte, sender common.Address, isLocal bool, err error) {
	txn, ok := p.byHash[string(hash)]
	if ok && txn.TxnSlot.Rlp != nil {
		return txn.TxnSlot.Rlp, p.senders.senderID2Addr[txn.TxnSlot.SenderID], txn.subPool&IsLocal > 0, nil
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

func (p *TxPool) AppendAllAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for hash, txn := range p.byHash {
		types = append(types, txn.TxnSlot.TxType())
		sizes = append(sizes, txn.TxnSlot.Size)
		hashes = append(hashes, hash...)
	}
	for hash, txIdx := range p.unprocessedRemoteByHash {
		txnSlot := p.unprocessedRemoteTxns.Txns[txIdx]
		types = append(types, txnSlot.TxType())
		sizes = append(sizes, txnSlot.Size)
		hashes = append(hashes, hash...)
	}
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
	if _, ok := p.minedBlobTxnsByHash[hashS]; ok {
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

func (p *TxPool) FilterKnownIdHashes(tx kv.Tx, hashes Hashes) (unknownHashes Hashes, err error) {
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

func (p *TxPool) getUnprocessedTxn(hashS string) (*TxnSlot, bool) {
	if i, ok := p.unprocessedRemoteByHash[hashS]; ok {
		return p.unprocessedRemoteTxns.Txns[i], true
	}
	return nil, false
}

func (p *TxPool) getCachedBlobTxnLocked(tx kv.Tx, hash []byte) (*metaTxn, error) {
	hashS := string(hash)
	if mt, ok := p.minedBlobTxnsByHash[hashS]; ok {
		return mt, nil
	}
	if txn, ok := p.getUnprocessedTxn(hashS); ok {
		return newMetaTxn(txn, false, 0), nil
	}
	if mt, ok := p.byHash[hashS]; ok {
		return mt, nil
	}
	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, fmt.Errorf("TxPool.getCachedBlobTxnLocked: Get: %d, %w", len(hash), err)
	}
	if len(v) == 0 {
		return nil, nil
	}
	txnRlp := common.Copy(v[20:])
	parseCtx := NewTxnParseContext(p.chainID)
	parseCtx.WithSender(false)
	txnSlot := &TxnSlot{}
	parseCtx.ParseTransaction(txnRlp, 0, txnSlot, nil, false, true, nil)
	return newMetaTxn(txnSlot, false, 0), nil
}

func (p *TxPool) IsLocal(idHash []byte) bool {
	hashS := string(idHash)
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.isLocalLRU.Contains(hashS)
}

func (p *TxPool) AddNewGoodPeer(peerID PeerID) {
	p.recentlyConnectedPeers.AddPeer(peerID)
}

func (p *TxPool) Started() bool {
	return p.started.Load()
}

// best returns the highest-priority pending transactions that fit within the given gas and RLP space budgets.
// EIP-8037: availableGas.Regular tracks regular gas; availableGas.State tracks intrinsic
// state gas. Execution-time state gas (SSTOREs) cannot be predicted here and is
// enforced by applyTransaction in the block assembler.
func (p *TxPool) best(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64,
	availableGas mdgas.FullMdGas,
	yielded mapset.Set[[32]byte], availableRlpSpace int) (bool, int, error) {

	p.lock.Lock()
	for last := p.lastSeenBlock.Load(); last < onTopOf; last = p.lastSeenBlock.Load() {
		select {
		case <-ctx.Done():
			return false, 0, ctx.Err()
		default:
			// continue
		}
		p.logger.Debug("[txpool] Waiting for block", "expecting", onTopOf, "lastSeen", last, "txRequested", n, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
		p.lastSeenCond.Wait()
	}
	// Important: poolDB.BeginRo has a RoTxsLimiter which is implemented using a weighted semaphore object. This means
	// that we are dealing with 2 locks at a time. All other usages in the pool that use both p.lock and poolDB.BeginRo
	// first acquire the RoTx and then acquire p.lock. However, here we do the opposite - we first acquire p.lock and
	// then try to acquire a RoTx. This creates an opportunity for a deadlock if we've acquired p.lock but at the same
	// time there has been a burst of goroutines (N=roTxsLimit) that have all acquired a RoTx and are now trying to
	// acquire p.lock which we've acquired here (the goroutines processing announcements := <-p.newPendingTxns in p.Run
	// are one example). One solution is to first acquire a RoTx here and then p.lock as everywhere else. However, this
	// won't work well if we wait in the "Waiting for block" loop for a while since our RoTx will then see stale data.
	// Instead, we can release p.lock once we're past "Waiting for block" -> try to acquire RoTx -> try to acquire
	// p.lock again as everywhere else.
	p.lock.Unlock()
	tx, err := p.poolDB.BeginRo(ctx)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	p.lock.Lock()
	defer p.lock.Unlock()

	best := p.pending.best

	isEIP3860 := p.isShanghai() || p.isAgra()
	isEIP7623 := p.isPrague() || p.isBhilai()

	txns.Resize(uint(min(n, len(best.ms))))
	var toRemove []*metaTxn
	var toDiscard []*metaTxn
	count := 0
	i := 0

	defer func() {
		p.logger.Debug("[txpool] Processing best request", "last", onTopOf, "txRequested", n, "txAvailable", len(best.ms), "txProcessed", i, "txReturned", count)
	}()

	for ; count < n && i < len(best.ms); i++ {
		// if we wouldn't have enough gas for a standard transaction then quit out early
		if availableGas.Regular < params.TxGas {
			break
		}
		if availableRlpSpace <= 0 {
			break
		}

		mt := best.ms[i]

		if yielded != nil && yielded.Contains(mt.TxnSlot.IDHash) {
			continue
		}

		if mt.TxnSlot.GetGas() >= p.blockGasLimit.Load() {
			// Skip transactions with very large gas limit
			continue
		}

		if int64(mt.TxnSlot.Size) > int64(availableRlpSpace) {
			p.logger.Debug("[txpool] skipping txn bigger than available rlp space", "size", int64(mt.TxnSlot.Size), "available", int64(availableRlpSpace))
			continue
		}

		rlpTxn, sender, isLocal, err := p.getRlpLocked(tx, mt.TxnSlot.IDHash[:])
		if err != nil {
			return false, count, err
		}
		if len(rlpTxn) == 0 {
			toRemove = append(toRemove, mt)
			continue
		}

		// Skip transactions that require more blob gas than is available
		blobCount := uint64(len(mt.TxnSlot.GetBlobHashes()))
		if blobCount > 0 {
			if p.isOsaka() {
				proofs := mt.TxnSlot.Proofs()
				if len(proofs) != int(blobCount)*int(params.CellsPerExtBlob) { // cell_proofs contains exactly CELLS_PER_EXT_BLOB * len(blobs) cell proofs
					toDiscard = append(toDiscard, mt)
					continue
				}
			}
			if blobCount*params.GasPerBlob > availableGas.Blob {
				continue
			}
		}

		// make sure we have enough gas in the caller to add this transaction.
		// not an exact science using intrinsic gas but as close as we could hope for at
		// this stage
		isAATxn := mt.TxnSlot.TxType() == types.AccountAbstractionTxType
		authorizationLen := uint64(len(mt.TxnSlot.AuthAndNonces))
		intrinsicGasResult, _ := mdgas.CalcIntrinsicGas(mdgas.IntrinsicGasCalcArgs{
			Data:               make([]byte, mt.TxnSlot.GetDataLen()),
			DataNonZeroLen:     uint64(mt.TxnSlot.GetDataNonZeroLen()),
			AuthorizationsLen:  authorizationLen,
			AccessListLen:      uint64(mt.TxnSlot.GetAccessListAddrCount()),
			StorageKeysLen:     uint64(mt.TxnSlot.GetAccessListStorCount()),
			CostPerStateByte:   misc.CostPerStateByte(p.blockGasLimit.Load()),
			IsContractCreation: mt.TxnSlot.IsCreation(),
			IsEIP2:             true,
			IsEIP2028:          true,
			IsEIP3860:          isEIP3860,
			IsEIP7623:          isEIP7623,
			IsEIP8037:          p.isAmsterdam(),
			IsAATxn:            isAATxn,
		})
		intrinsicRegularGas := intrinsicGasResult.RegularGas
		if isEIP7623 && intrinsicGasResult.FloorGasCost > intrinsicRegularGas {
			intrinsicRegularGas = intrinsicGasResult.FloorGasCost
		}
		if intrinsicRegularGas > availableGas.Regular {
			// we might find another txn with a low enough intrinsic gas to include so carry on
			continue
		}
		// EIP-8037: filter by intrinsic state gas. Execution-time state gas
		// (SSTOREs) is unpredictable and enforced in applyTransaction instead.
		if intrinsicGasResult.StateGas > availableGas.State {
			continue
		}
		availableGas.Regular -= intrinsicRegularGas
		availableGas.State -= intrinsicGasResult.StateGas
		availableGas.Blob -= blobCount * params.GasPerBlob
		availableRlpSpace -= len(rlpTxn)
		txns.Txns[count] = rlpTxn
		// For blob transactions, slot.Txn is the inner BlobTx without the
		// sidecar (blobs/commitments/proofs). Leave ParsedTxn nil so that
		// ProvideTxns falls back to DecodeWrappedTransaction on the full
		// wrapper RLP, which produces a BlobTxWrapper with sidecar intact.
		if mt.TxnSlot.TxType() != types.BlobTxType {
			txns.ParsedTxn[count] = mt.TxnSlot.Txn
		}
		copy(txns.Senders.At(count), sender.Bytes())
		txns.IsLocal[count] = isLocal
		if yielded != nil {
			yielded.Add(mt.TxnSlot.IDHash)
		}
		count++
	}

	txns.Resize(uint(count))
	if len(toRemove) > 0 {
		for _, mt := range toRemove {
			p.pending.Remove(mt, "best", p.logger)
		}
	}
	if len(toDiscard) > 0 {
		for _, mt := range toDiscard {
			p.pending.Remove(mt, "best", p.logger)
			p.discardLocked(mt, txpoolcfg.UnmatchedBlobTxExt)
		}
	}

	return true, count, nil
}

func (p *TxPool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	provideOptions := txnprovider.ApplyProvideOptions(opts...)
	var txnsRlp TxnsRlp
	_, _, err := p.best(
		ctx,
		provideOptions.Amount,
		&txnsRlp,
		provideOptions.ParentBlockNum,
		provideOptions.GasTarget,
		provideOptions.TxnIdsFilter,
		provideOptions.AvailableRlpSpace,
	)
	if err != nil {
		return nil, err
	}

	txns := make([]types.Transaction, 0, len(txnsRlp.ParsedTxn))
	for i, txn := range txnsRlp.ParsedTxn {
		if txn == nil {
			// Fallback: decode from RLP if parsed txn not available
			var err error
			txn, err = types.DecodeWrappedTransaction(txnsRlp.Txns[i])
			if err != nil {
				return nil, err
			}
		}

		var sender common.Address
		copy(sender[:], txnsRlp.Senders.At(i))
		txn.SetSender(accounts.InternAddress(sender))
		txns = append(txns, txn)
	}

	return txns, nil
}

func (p *TxPool) PeekBest(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64) (bool, error) {
	set := mapset.NewThreadUnsafeSet[[32]byte]()
	onTime, _, err := p.best(ctx, n, txns, onTopOf,
		mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, math.MaxUint64), set, math.MaxInt)
	return onTime, err
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pending.Len(), p.baseFee.Len(), p.queued.Len()
}

func (p *TxPool) AddRemoteTxns(_ context.Context, newTxns TxnSlots) {
	if p.cfg.NoGossip {
		// if no gossip, then
		// disable adding remote transactions
		// consume remote txn from fetch
		return
	}

	defer addRemoteTxnsTimer.ObserveDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	for i, txn := range newTxns.Txns {
		hashS := string(txn.IDHash[:])
		_, ok := p.unprocessedRemoteByHash[hashS]
		if ok {
			continue
		}
		p.unprocessedRemoteByHash[hashS] = len(p.unprocessedRemoteTxns.Txns)
		p.unprocessedRemoteTxns.Append(txn, newTxns.Senders.At(i), false)
	}
}

func toBlobs(_blobs [][]byte) []*goethkzg.Blob {
	blobs := make([]*goethkzg.Blob, len(_blobs))
	for i, _blob := range _blobs {
		blobs[i] = (*goethkzg.Blob)(_blob)
	}
	return blobs
}

func (p *TxPool) validateTx(txn *TxnSlot, isLocal bool, stateCache kvcache.CacheView) txpoolcfg.DiscardReason {
	isEIP3860 := p.isShanghai() || p.isAgra()
	isPrague := p.isPrague() || p.isBhilai()
	isEIP7954 := p.isAmsterdam()
	if txn.IsCreation() {
		if err := vm.CheckMaxInitCodeSize(uint64(txn.GetDataLen()), isEIP3860, isEIP7954); err != nil {
			return txpoolcfg.InitCodeTooLarge
		}
	}

	if txn.TxType() == types.AccountAbstractionTxType {
		if !p.cfg.AllowAA {
			return txpoolcfg.TypeNotActivated
		}

		res, err := p.ethBackend.AAValidation(context.Background(), &remoteproto.AAValidationRequest{Tx: txn.ToProtoAccountAbstractionTxn()}) // enforces ERC-7562 rules
		if err != nil {
			return txpoolcfg.InvalidAA
		}
		if !res.Valid {
			return txpoolcfg.InvalidAA
		}
	}

	authorizationLen := len(txn.AuthAndNonces)
	if txn.TxType() == SetCodeTxnType {
		if !isPrague {
			return txpoolcfg.TypeNotActivated
		}
		if txn.IsCreation() {
			return txpoolcfg.InvalidCreateTxn
		}
		if authorizationLen == 0 {
			return txpoolcfg.NoAuthorizations
		}
	}

	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && uint256.NewInt(p.cfg.MinFeeCap).Cmp(txn.GetFeeCap()) == 1 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx underpriced idHash=%x local=%t, feeCap=%d, cfg.MinFeeCap=%d", txn.IDHash, isLocal, txn.GetFeeCap(), p.cfg.MinFeeCap))
		}
		return txpoolcfg.UnderPriced
	}

	isAATxn := txn.TxType() == types.AccountAbstractionTxType
	intrinsicGasResult, overflow := mdgas.CalcIntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               make([]byte, txn.GetDataLen()),
		DataNonZeroLen:     uint64(txn.GetDataNonZeroLen()),
		AuthorizationsLen:  uint64(authorizationLen),
		AccessListLen:      uint64(txn.GetAccessListAddrCount()),
		StorageKeysLen:     uint64(txn.GetAccessListStorCount()),
		CostPerStateByte:   misc.CostPerStateByte(p.blockGasLimit.Load()),
		IsContractCreation: txn.IsCreation(),
		IsEIP2:             true,
		IsEIP2028:          true,
		IsEIP3860:          isEIP3860,
		IsEIP7623:          isPrague,
		IsEIP8037:          p.isAmsterdam(),
		IsAATxn:            isAATxn,
	})
	gas := mdgas.MdGas{
		Regular: intrinsicGasResult.RegularGas,
		State:   intrinsicGasResult.StateGas,
	}
	if isPrague && intrinsicGasResult.FloorGasCost > gas.Regular {
		gas.Regular = intrinsicGasResult.FloorGasCost
	}

	if txn.Traced {
		p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas idHash=%x gas=%d", txn.IDHash, gas))
	}
	if overflow {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas calculated failed due to overflow idHash=%x", txn.IDHash))
		}
		return txpoolcfg.GasUintOverflow
	}
	if gas.Total() > txn.GetGas() {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas > txn.gas idHash=%x gas=%d, txn.gas=%d", txn.IDHash, gas, txn.GetGas()))
		}
		return txpoolcfg.IntrinsicGas
	}
	if txn.GetGas() > p.blockGasLimit.Load() {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx txn.gas > block gas limit idHash=%x gas=%d, block gas limit=%d", txn.IDHash, txn.GetGas(), p.blockGasLimit.Load()))
		}
		return txpoolcfg.GasLimitTooHigh
	}
	// EIP-7825: Transaction Gas Limit Cap.
	// EIP-8037 (Amsterdam): TX_MAX_GAS_LIMIT applies to the regular gas dimension only.
	// Pre-Amsterdam: cap = full tx gas limit.
	var gasToCap uint64
	if p.isAmsterdam() {
		gasToCap = max(intrinsicGasResult.RegularGas, intrinsicGasResult.FloorGasCost)
	} else {
		gasToCap = txn.GetGas()
	}
	if gasToCap > params.MaxTxnGasLimit {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx gas cap exceeded idHash=%x gasToCap=%d, max=%d", txn.IDHash, gasToCap, params.MaxTxnGasLimit))
		}
		return txpoolcfg.GasLimitTooHigh
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
	if txn.TxType() == BlobTxnType {
		return p.validateBlobTxn(txn, isLocal)
	}
	return txpoolcfg.Success
}

func (p *TxPool) validateBlobTxn(txn *TxnSlot, isLocal bool) txpoolcfg.DiscardReason {
	if !p.isCancun() {
		return txpoolcfg.TypeNotActivated
	}
	if txn.IsCreation() {
		return txpoolcfg.InvalidCreateTxn
	}
	blobHashes := txn.GetBlobHashes()
	blobCount := len(blobHashes)
	if blobCount == 0 {
		return txpoolcfg.NoBlobs
	}
	if blobCount > min(params.MaxBlobsPerTxn, int(p.GetMaxBlobsPerBlock())) {
		return txpoolcfg.TooManyBlobs
	}

	if blobCount != len(txn.BlobBundles) {
		p.logger.Debug(fmt.Sprintf("TX POOL: blobCount %d != len(blobs) %d", blobCount, len(txn.BlobBundles)))
		return txpoolcfg.UnequalBlobTxExt
	}
	blobs := txn.Blobs()
	commitments := txn.Commitments()
	proofs := txn.Proofs()

	if len(blobs) != len(commitments) {
		p.logger.Debug(fmt.Sprintf("TX POOL: len(blobs) %d != len(commitments) %d", len(blobs), len(commitments)))
		return txpoolcfg.UnequalBlobTxExt
	}
	if p.isOsaka() {
		if len(proofs) != len(blobs)*int(params.CellsPerExtBlob) { // cell_proofs contains exactly CELLS_PER_EXT_BLOB * len(blobs) cell proofs
			return txpoolcfg.UnmatchedBlobTxExt
		}
	} else {
		if len(commitments) != len(proofs) {
			p.logger.Debug(fmt.Sprintf("TX POOL: NOT OSAKA len(commitments) %d != len(proofs) %d", len(commitments), len(proofs)))
			return txpoolcfg.UnequalBlobTxExt
		}
	}

	for i := 0; i < len(commitments); i++ {
		if libkzg.KZGToVersionedHash(commitments[i]) != libkzg.VersionedHash(blobHashes[i]) {
			return txpoolcfg.BlobHashCheckFail
		}
	}

	if p.isOsaka() {
		err := libkzg.VerifyCellProofBatch(blobs, commitments, proofs)
		if err != nil {
			return txpoolcfg.UnmatchedBlobTxExt
		}
	} else {
		// https://github.com/ethereum/consensus-specs/blob/017a8495f7671f5fff2075a9bfc9238c1a0982f8/specs/deneb/polynomial-commitments.md#verify_blob_kzg_proof_batch
		kzgCtx := libkzg.Ctx()
		err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(blobs), commitments, proofs)
		if err != nil {
			return txpoolcfg.UnmatchedBlobTxExt
		}
	}

	if !isLocal && (p.all.blobCount(txn.SenderID)+uint64(blobCount)) > p.cfg.BlobSlots {
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
	return txpoolcfg.Success
}

var maxUint256 = new(uint256.Int).SetAllOne()

// Sender should have enough balance for: gasLimit x feeCap + blobGas x blobFeeCap + transferred_value
// See YP, Eq (61) in Section 6.2 "Execution"
func requiredBalance(txn *TxnSlot) *uint256.Int {
	// See https://github.com/ethereum/EIPs/pull/3594
	total := uint256.NewInt(txn.GetGas())
	_, overflow := total.MulOverflow(total, txn.GetFeeCap())
	if overflow {
		return maxUint256
	}
	// and https://eips.ethereum.org/EIPS/eip-4844#gas-accounting
	blobCount := uint64(len(txn.GetBlobHashes()))
	if blobCount != 0 {
		maxBlobGasCost := uint256.NewInt(params.GasPerBlob)
		maxBlobGasCost.Mul(maxBlobGasCost, uint256.NewInt(blobCount))
		_, overflow = maxBlobGasCost.MulOverflow(maxBlobGasCost, txn.GetBlobFeeCap())
		if overflow {
			return maxUint256
		}
		_, overflow = total.AddOverflow(total, maxBlobGasCost)
		if overflow {
			return maxUint256
		}
	}

	_, overflow = total.AddOverflow(total, txn.GetValue())
	if overflow {
		return maxUint256
	}
	return total
}

func isTimeBasedForkActivated(isPostFlag *atomic.Bool, forkTime *uint64) bool {
	// once this flag has been set for the first time we no longer need to check the timestamp
	set := isPostFlag.Load()
	if set {
		return true
	}
	if forkTime == nil { // the fork is not enabled
		return false
	}

	// a zero here means the fork is always active
	if *forkTime == 0 {
		isPostFlag.Swap(true)
		return true
	}

	now := time.Now().Unix()
	activated := uint64(now) >= *forkTime
	if activated {
		isPostFlag.Swap(true)
	}
	return activated
}

func (p *TxPool) isShanghai() bool {
	return isTimeBasedForkActivated(&p.isPostShanghai, p.shanghaiTime)
}

func (p *TxPool) isBlockNumBasedForkActivated(isPostFlag *atomic.Bool, forkBlockNum *uint64) bool {
	// once this flag has been set for the first time we no longer need to check the block
	set := isPostFlag.Load()
	if set {
		return true
	}
	if forkBlockNum == nil {
		return false
	}
	forkBlock := *forkBlockNum

	// a zero here means the fork is always active
	if forkBlock == 0 {
		isPostFlag.Swap(true)
		return true
	}

	tx, err := p._chainDB.BeginRo(context.Background())
	if err != nil {
		return false
	}
	defer tx.Rollback()

	headBlock, err := chain.CurrentBlockNumber(tx)
	if headBlock == nil || err != nil {
		return false
	}
	// A new block is built on top of the head block, so when the head is forkBlock-1,
	// the new block should use the new fork rules.
	activated := (*headBlock + 1) >= forkBlock
	if activated {
		isPostFlag.Swap(true)
	}
	return activated
}

func (p *TxPool) isAgra() bool {
	return p.isBlockNumBasedForkActivated(&p.isPostAgra, p.agraBlock)
}

func (p *TxPool) isBhilai() bool {
	return p.isBlockNumBasedForkActivated(&p.isPostBhilai, p.bhilaiBlock)
}

func (p *TxPool) isCancun() bool {
	return isTimeBasedForkActivated(&p.isPostCancun, p.cancunTime)
}

func (p *TxPool) isPrague() bool {
	return isTimeBasedForkActivated(&p.isPostPrague, p.pragueTime)
}

func (p *TxPool) isOsaka() bool {
	return isTimeBasedForkActivated(&p.isPostOsaka, p.osakaTime)
}

func (p *TxPool) isAmsterdam() bool {
	return isTimeBasedForkActivated(&p.isPostAmsterdam, p.amsterdamTime)
}

func (p *TxPool) GetMaxBlobsPerBlock() uint64 {
	now := time.Now().Unix()
	return p.chainConfig.GetMaxBlobsPerBlock(uint64(now))
}

// Check that the serialized txn should not exceed a certain max size
func (p *TxPool) ValidateSerializedTxn(serializedTxn []byte) error {
	const (
		// txnSlotSize is used to calculate how many data slots a single transaction
		// takes up based on its size. The slots are used as DoS protection, ensuring
		// that validating a new transaction remains a constant operation (in reality
		// O(maxslots), where max slots are 4 currently).
		txnSlotSize = 32 * 1024

		// txnMaxSize is the maximum size a single transaction can have. This field has
		// non-trivial consequences: larger transactions are significantly harder and
		// more expensive to propagate; larger transactions also take more resources
		// to validate whether they fit into the pool or not.
		txnMaxSize = 4 * txnSlotSize // 128KB

		// Should be enough for a transaction with 6 blobs
		blobTxnMaxSize = 1024 * 1024
	)
	txnType, err := PeekTransactionType(serializedTxn)
	if err != nil {
		return err
	}
	maxSize := txnMaxSize
	if txnType == BlobTxnType {
		maxSize = blobTxnMaxSize
	}
	if len(serializedTxn) > maxSize {
		return fmt.Errorf("%w: type=%d", ErrRlpTooBig, txnType)
	}
	return nil
}

func (p *TxPool) validateTxns(txns *TxnSlots, stateCache kvcache.CacheView) (reasons []txpoolcfg.DiscardReason, goodTxns TxnSlots, err error) {
	// reasons is pre-sized for direct indexing, with the default zero
	// value DiscardReason of NotSet
	reasons = make([]txpoolcfg.DiscardReason, len(txns.Txns))

	if err := txns.Valid(); err != nil {
		return reasons, goodTxns, err
	}

	goodCount := 0
	for i, txn := range txns.Txns {
		reason := p.validateTx(txn, txns.IsLocal[i], stateCache)
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

	goodTxns.Resize(uint(goodCount))

	j := 0
	for i, txn := range txns.Txns {
		if reasons[i] == txpoolcfg.NotSet {
			goodTxns.Txns[j] = txn
			goodTxns.IsLocal[j] = txns.IsLocal[i]
			copy(goodTxns.Senders.At(j), txns.Senders.At(i))
			j++
		}
	}
	return reasons, goodTxns, nil
}

// punishSpammer by drop half of it's transactions with high nonce
func (p *TxPool) punishSpammer(spammer uint64) {
	count := p.all.count(spammer) / 2
	if count > 0 {
		txnsToDelete := make([]*metaTxn, 0, count)
		p.all.descend(spammer, func(mt *metaTxn) bool {
			txnsToDelete = append(txnsToDelete, mt)
			count--
			return count > 0
		})

		for _, mt := range txnsToDelete {
			p.removeFromSubPool(mt, "punishSpammer")
			p.discardLocked(mt, txpoolcfg.Spammer) // can't call it while iterating by all
		}
	}
}

func fillDiscardReasons(reasons []txpoolcfg.DiscardReason, newTxns TxnSlots, discardReasonsLRU *simplelru.LRU[string, txpoolcfg.DiscardReason]) []txpoolcfg.DiscardReason {
	for i := range reasons {
		if reasons[i] != txpoolcfg.NotSet {
			continue
		}
		reason, ok := discardReasonsLRU.Get(string(newTxns.Txns[i].IDHash[:]))
		if ok {
			reasons[i] = reason
		} else {
			reasons[i] = txpoolcfg.Success
		}
	}
	return reasons
}

func (p *TxPool) AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	coreDb, cache := p.chainDB()
	coreTx, err := coreDb.BeginTemporalRo(ctx)
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

	if err = p.senders.registerNewSenders(&newTxns, p.logger); err != nil {
		return nil, err
	}

	reasons, newTxns, err := p.validateTxns(&newTxns, cacheView)
	if err != nil {
		return nil, err
	}

	announcements, addReasons, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, newTxns,
		p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), p.blockGasLimit.Load(), true, p.logger)
	if err != nil {
		return nil, err
	}
	for i, reason := range addReasons {
		if reason != txpoolcfg.NotSet {
			reasons[i] = reason
		}
	}
	p.promoted.Reset()
	p.promoted.AppendOther(announcements)

	reasons = fillDiscardReasons(reasons, newTxns, p.discardReasonsLRU)
	for i, reason := range reasons {
		if reason == txpoolcfg.Success {
			txn := newTxns.Txns[i]
			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: AddLocalTxns promotes idHash=%x, senderId=%d", txn.IDHash, txn.SenderID))
			}
			p.promoted.Append(txn.TxType(), txn.Size, txn.IDHash[:])
		}
	}
	if p.promoted.Len() > 0 {
		select {
		case p.newPendingTxns <- p.promoted.Copy():
		default:
		}
	}
	return reasons, nil
}

func (p *TxPool) chainDB() (kv.TemporalRoDB, kvcache.Cache) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._chainDB, p._stateCache
}

func (p *TxPool) addTxns(blockNum uint64, cacheView kvcache.CacheView, senders *sendersBatch,
	newTxns TxnSlots, pendingBaseFee, pendingBlobFee, blockGasLimit uint64, collect bool, logger log.Logger) (Announcements, []txpoolcfg.DiscardReason, error) {
	if assert.Enable {
		for _, txn := range newTxns.Txns {
			if txn.SenderID == 0 {
				panic("senderID can't be zero")
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
	discardReasons := make([]txpoolcfg.DiscardReason, len(newTxns.Txns))
	announcements := Announcements{}
	for i, txn := range newTxns.Txns {
		if found, ok := p.byHash[string(txn.IDHash[:])]; ok {
			discardReasons[i] = txpoolcfg.DuplicateHash
			// In case if the transition is stuck, "poke" it to rebroadcast
			if collect && newTxns.IsLocal[i] && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.TxnSlot.TxType(), found.TxnSlot.Size, found.TxnSlot.IDHash[:])
			}
			continue
		}
		mt := newMetaTxn(txn, newTxns.IsLocal[i], blockNum)

		if reason := p.addLocked(mt, &announcements); reason != txpoolcfg.NotSet {
			discardReasons[i] = reason
			continue
		}
		discardReasons[i] = txpoolcfg.NotSet // unnecessary
		if txn.Traced {
			logger.Info(fmt.Sprintf("TX TRACING: schedule sendersWithChangedState idHash=%x senderId=%d", txn.IDHash, mt.TxnSlot.SenderID))
		}
		sendersWithChangedState[mt.TxnSlot.SenderID] = struct{}{}
	}

	for senderID := range sendersWithChangedState {
		// For freshly submitted transactions, initialize the dormancy timer if this sender
		// has no existing entry. We use blockNum (the current chain tip) as the baseline so
		// the sender gets the full dormancy grace period from the moment of first submission.
		if _, ok := p.senderLastActivity[senderID]; !ok {
			p.senderLastActivity[senderID] = blockNum
		}
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
func (p *TxPool) addTxnsOnNewBlock(blockNum uint64, cacheView kvcache.CacheView, stateChanges *remoteproto.StateChangeBatch,
	senders *sendersBatch, newTxns TxnSlots, pendingBaseFee uint64, blockGasLimit uint64, logger log.Logger) (Announcements, error) {
	if assert.Enable {
		for _, txn := range newTxns.Txns {
			if txn.SenderID == 0 {
				panic("senderID can't be zero")
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
	announcements := Announcements{}
	for i, txn := range newTxns.Txns {
		if _, ok := p.byHash[string(txn.IDHash[:])]; ok {
			continue
		}
		mt := newMetaTxn(txn, newTxns.IsLocal[i], blockNum)
		if reason := p.addLocked(mt, &announcements); reason != txpoolcfg.NotSet {
			p.discardLocked(mt, reason)
			continue
		}
		sendersWithChangedState[mt.TxnSlot.SenderID] = struct{}{}
	}
	// add senders changed in state to `sendersWithChangedState` list
	for _, changesList := range stateChanges.ChangeBatch {
		for _, change := range changesList.Changes {
			switch change.Action {
			case remoteproto.Action_UPSERT, remoteproto.Action_UPSERT_CODE:
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
		// Reset the dormancy timer: this sender had a real on-chain state change.
		p.senderLastActivity[senderID] = blockNum
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

// setPendingBaseFeeOnQueues propagates the pending base fee to all sub-pool comparators.
func (p *TxPool) setPendingBaseFeeOnQueues(fee uint256.Int) {
	p.pending.best.pendingBaseFee = fee
	p.pending.worst.pendingBaseFee = fee
	p.baseFee.best.pendingBaseFee = fee
	p.baseFee.worst.pendingBaseFee = fee
	p.queued.best.pendingBaseFee = fee
	p.queued.worst.pendingBaseFee = fee
}

// removeFromSubPool removes a metaTxn from whichever sub-pool it currently belongs to.
func (p *TxPool) removeFromSubPool(mt *metaTxn, reason string) {
	switch mt.currentSubPool {
	case PendingSubPool:
		p.pending.Remove(mt, reason, p.logger)
	case BaseFeeSubPool:
		p.baseFee.Remove(mt, reason, p.logger)
	case QueuedSubPool:
		p.queued.Remove(mt, reason, p.logger)
	}
}

// updateInSubPool notifies the sub-pool that a metaTxn's ephemeral fields have changed.
func (p *TxPool) updateInSubPool(mt *metaTxn) {
	switch mt.currentSubPool {
	case PendingSubPool:
		p.pending.Updated(mt)
	case BaseFeeSubPool:
		p.baseFee.Updated(mt)
	case QueuedSubPool:
		p.queued.Updated(mt)
	}
}

func (p *TxPool) addLocked(mt *metaTxn, announcements *Announcements) txpoolcfg.DiscardReason {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.all.get(mt.TxnSlot.SenderID, mt.TxnSlot.Nonce)
	if found != nil {
		if found.TxnSlot.TxType() == BlobTxnType && mt.TxnSlot.TxType() != BlobTxnType {
			return txpoolcfg.BlobTxReplace
		}
		priceBump := p.cfg.PriceBump

		if mt.TxnSlot.TxType() == BlobTxnType {
			//Blob txn threshold checks for replace txn
			priceBump = p.cfg.BlobPriceBump
			blobFeeThreshold, overflow := (&uint256.Int{}).MulDivOverflow(
				found.TxnSlot.GetBlobFeeCap(),
				uint256.NewInt(100+priceBump),
				uint256.NewInt(100),
			)
			if mt.TxnSlot.GetBlobFeeCap().Lt(blobFeeThreshold) && !overflow {
				if bytes.Equal(found.TxnSlot.IDHash[:], mt.TxnSlot.IDHash[:]) {
					return txpoolcfg.NotSet
				}
				return txpoolcfg.ReplaceUnderpriced // TODO: This is the same as NotReplaced
			}
		}

		//Regular txn threshold checks
		tipThreshold := uint256.NewInt(0)
		tipThreshold = tipThreshold.Mul(found.TxnSlot.GetTipCap(), uint256.NewInt(100+priceBump))
		tipThreshold.Div(tipThreshold, &u256.N100)
		feecapThreshold := uint256.NewInt(0)
		feecapThreshold.Mul(found.TxnSlot.GetFeeCap(), uint256.NewInt(100+priceBump))
		feecapThreshold.Div(feecapThreshold, &u256.N100)

		if mt.TxnSlot.GetValue().Cmp(found.TxnSlot.GetValue()) > 0 {
			//Potential latent overdraft attack
			tipThreshold.Mul(tipThreshold, uint256.NewInt(uint64(p.all.count(mt.TxnSlot.SenderID))))
		}
		if mt.TxnSlot.GetTipCap().Cmp(tipThreshold) < 0 || mt.TxnSlot.GetFeeCap().Cmp(feecapThreshold) < 0 {
			// Both tip and feecap need to be larger than previously to replace the transaction
			// In case if the transition is stuck, "poke" it to rebroadcast
			if mt.subPool&IsLocal != 0 && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.TxnSlot.TxType(), found.TxnSlot.Size, found.TxnSlot.IDHash[:])
			}
			if bytes.Equal(found.TxnSlot.IDHash[:], mt.TxnSlot.IDHash[:]) {
				return txpoolcfg.NotSet
			}
			return txpoolcfg.NotReplaced
		}

		p.removeFromSubPool(found, "add")
		p.discardLocked(found, txpoolcfg.ReplacedByHigherTip)
	}

	// Don't add blob txn to queued if it's less than current pending blob base fee
	if mt.TxnSlot.TxType() == BlobTxnType && mt.TxnSlot.GetBlobFeeCap().LtUint64(p.pendingBlobFee.Load()) {
		return txpoolcfg.FeeTooLow
	}

	// Do not allow transaction from this same (sender + nonce) if sender has existing pooled authorization as authority
	senderAddr, ok := p.senders.senderID2Addr[mt.TxnSlot.SenderID]
	if !ok {
		p.logger.Info("senderID not registered, discarding transaction for safety")
		return txpoolcfg.InvalidSender
	}
	if _, ok := p.auths[AuthAndNonce{senderAddr, mt.TxnSlot.Nonce}]; ok {
		return txpoolcfg.ErrAuthorityReserved
	}

	// Check if we have txn with same authorization in the pool
	if mt.TxnSlot.TxType() == SetCodeTxnType {
		for _, a := range mt.TxnSlot.AuthAndNonces {
			// Self authorization nonce should be senderNonce + 1
			if a.authority == senderAddr && a.nonce != mt.TxnSlot.Nonce+1 {
				p.logger.Debug("Self authorization nonce should be senderNonce + 1", "authority", a.authority, "txn", fmt.Sprintf("%x", mt.TxnSlot.IDHash))
				return txpoolcfg.NonceTooLow
			}
			if _, ok := p.auths[AuthAndNonce{a.authority, a.nonce}]; ok {
				p.logger.Debug("setCodeTxn ", "DUPLICATE authority", a.authority, "at nonce", a.nonce, "txn", fmt.Sprintf("%x", mt.TxnSlot.IDHash))
				return txpoolcfg.ErrAuthorityReserved
			}
		}
		for _, a := range mt.TxnSlot.AuthAndNonces {
			p.auths[AuthAndNonce{a.authority, a.nonce}] = mt
		}
	}

	hashStr := string(mt.TxnSlot.IDHash[:])
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
	if blobHashes := mt.TxnSlot.GetBlobHashes(); len(blobHashes) > 0 {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t + uint64(len(blobHashes)))
		for i, b := range blobHashes {
			p.blobHashToTxn[b] = struct {
				index   int
				txnHash common.Hash
			}{i, mt.TxnSlot.IDHash}
		}
	}

	// Remove from mined cache as we are now "resurrecting" it to a sub-pool
	p.deleteMinedBlobTxn(hashStr)
	return txpoolcfg.NotSet
}

// dropping transaction from all sub-structures and from db
// Important: don't call it while iterating by all
func (p *TxPool) discardLocked(mt *metaTxn, reason txpoolcfg.DiscardReason) {
	hashStr := string(mt.TxnSlot.IDHash[:])
	delete(p.byHash, hashStr)
	p.deletedTxns = append(p.deletedTxns, mt)
	p.all.delete(mt, reason, p.logger)
	p.discardReasonsLRU.Add(hashStr, reason)
	if mt.TxnSlot.TxType() == BlobTxnType {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t - uint64(len(mt.TxnSlot.GetBlobHashes())))
	}
	if mt.TxnSlot.TxType() == SetCodeTxnType {
		for _, a := range mt.TxnSlot.AuthAndNonces {
			delete(p.auths, a)
		}
	}
}

// sweepDormantQueued evicts all queued transactions from senders that have had no on-chain
// state change for longer than cfg.QueuedDormancyDuration. It also prunes stale entries from
// the senderLastActivity map (senders that are no longer in the queued sub-pool).
//
// The dormancy threshold is expressed in blocks, not wall-clock time. It is computed as:
//
//	dormancyBlocks = QueuedDormancyDuration / avgBlockTimeMs  (EWMA of real block-to-block intervals)
//
// This means dormancyBlocks adapts to actual network conditions. On Ethereum mainnet the
// configured 3 h maps to ~900 blocks at the seeded 12 s/block, but once the node reaches
// chain tip and the EWMA updates with real inter-block timings (~13 s on mainnet due to
// missed slots), dormancyBlocks converges to ~830. In wall-clock terms the dormancy
// remains ~3 h; only the block count changes. The log line includes "avgBlockMs" so the
// derivation is transparent without needing to inspect the source.
//
// During fast-sync the EWMA does not update: inter-block gaps as seen by the txpool exceed
// the 10-min filter, so the block time stays at the 12 s seed and dormancyBlocks stays at 900.
//
// Returns the number of senders evicted and the current queued pool size (for interval tuning).
// Must NOT be called while holding p.lock; it acquires the lock internally.
func (p *TxPool) sweepDormantQueued(ctx context.Context, currentBlock uint64, logger log.Logger) (evictedSenders, queuedLen int) {
	if p.cfg.QueuedDormancyDuration <= 0 {
		return 0, 0
	}

	// Compute the dormancy threshold in blocks using the current EWMA block time.
	avgMs := p.avgBlockTimeMs.Load()
	if avgMs <= 0 {
		avgMs = 12_000
	}
	dormancyBlocks := uint64(p.cfg.QueuedDormancyDuration.Milliseconds()) / uint64(avgMs)
	if dormancyBlocks == 0 {
		dormancyBlocks = 1
	}

	// snapshot is populated inside the lock and persisted outside it, following the same
	// pattern as flushLocked: hold the lock only for in-memory operations.
	var snapshot map[uint64]uint64

	func() {
		p.lock.Lock()
		defer p.lock.Unlock()

		for senderID, lastBlock := range p.senderLastActivity {
			// Self-healing: remove map entries for senders no longer in the queued pool.
			hasQueued := false
			p.all.ascend(senderID, func(mt *metaTxn) bool {
				if mt.currentSubPool == QueuedSubPool {
					hasQueued = true
					return false
				}
				return true
			})
			if !hasQueued {
				delete(p.senderLastActivity, senderID)
				continue
			}

			// Skip senders still within their grace period.
			if currentBlock <= lastBlock || currentBlock-lastBlock <= dormancyBlocks {
				continue
			}

			// Collect queued txns for this sender then evict.
			var toEvict []*metaTxn
			p.all.ascend(senderID, func(mt *metaTxn) bool {
				if mt.currentSubPool == QueuedSubPool {
					toEvict = append(toEvict, mt)
				}
				return true
			})
			for _, mt := range toEvict {
				p.queued.Remove(mt, "dormant sender", logger)
				p.discardLocked(mt, txpoolcfg.QueuedDormant)
			}
			delete(p.senderLastActivity, senderID)
			evictedSenders++
			logger.Debug("[txpool] evicted dormant queued sender",
				"senderID", senderID,
				"txns", len(toEvict),
				"dormantBlocks", currentBlock-lastBlock,
				"dormancyThreshold", dormancyBlocks,
			)
		}

		queuedLen = p.queued.Len()

		// Snapshot the map for DB persistence while still holding the lock.
		snapshot = make(map[uint64]uint64, len(p.senderLastActivity))
		for k, v := range p.senderLastActivity {
			snapshot[k] = v
		}
	}()

	if evictedSenders > 0 {
		// dormancyBlocks is derived from the EWMA block time (avgMs): dormancyBlocks = QueuedDormancyDuration / avgMs.
		// It adapts to real network conditions (e.g. mainnet averages ~13 s/block due to missed slots, not 12 s),
		// so dormancyBlocks may differ slightly from the naive 3 h / 12 s = 900 value. avgBlockMs is logged
		// alongside dormancyBlocks so the derivation is transparent without needing to inspect the source.
		logger.Info("[txpool] dormancy sweep evicted senders from queued pool",
			"senders", evictedSenders,
			"block", currentBlock,
			"dormancyBlocks", dormancyBlocks,
			"avgBlockMs", avgMs,
		)
	}

	// Persist the snapshot outside the lock. At sweep frequency (~10 min) this is cheap;
	// a 10-min staleness window is negligible against the 3 h dormancy grace period.
	if err := p.poolDB.Update(ctx, func(tx kv.RwTx) error {
		return SaveSenderLastActivity(tx, snapshot)
	}); err != nil {
		logger.Warn("[txpool] failed to persist sender last activity", "err", err)
	}

	return evictedSenders, queuedLen
}

// nextDormancySweepInterval computes the wall-clock delay until the next dormancy sweep.
//
// Base interval = QueuedDormancyDuration / 10 so that ~10 sweeps occur per dormancy period.
// Pressure factor: if the queued pool is >50% or >80% full, sweep more aggressively.
// Back-off: after a clean sweep (no evictions) the interval is stretched by 1.5× (up to 4×
// the base) to avoid spinning when the pool is stable.
// All results are clamped to [30 s, 10 min].
func (p *TxPool) nextDormancySweepInterval(backoff *float64, lastEvicted, queuedLen int) time.Duration {
	if p.cfg.QueuedDormancyDuration <= 0 {
		return 24 * time.Hour // effectively disabled
	}

	base := p.cfg.QueuedDormancyDuration / 10

	// Pressure factor based on queued pool fill ratio.
	pressure := 1.0
	if cap := p.cfg.QueuedSubPoolLimit; cap > 0 {
		fill := float64(queuedLen) / float64(cap)
		switch {
		case fill > 0.8:
			pressure = 0.25 // 4× faster when >80% full
		case fill > 0.5:
			pressure = 0.5 // 2× faster when >50% full
		}
	}

	// Back-off: grow on clean sweeps, reset on evictions.
	if lastEvicted == 0 {
		*backoff = min(*backoff*1.5, 4.0)
	} else {
		*backoff = 1.0
	}

	interval := time.Duration(float64(base) * pressure * *backoff)

	// Clamp to [30 s, 10 min].
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	if interval > 10*time.Minute {
		interval = 10 * time.Minute
	}
	return interval
}

func (p *TxPool) getBlobsAndProofByBlobHashLocked(blobHashes []common.Hash) []PoolBlobBundle {
	p.lock.Lock()
	defer p.lock.Unlock()
	blobBundles := make([]PoolBlobBundle, len(blobHashes))
	for i, h := range blobHashes {
		th, ok := p.blobHashToTxn[h]
		if !ok {
			continue
		}
		mt, ok := p.byHash[string(th.txnHash[:])]
		if !ok || mt == nil {
			continue
		}
		if th.index < len(mt.TxnSlot.BlobBundles) {
			blobBundles[i] = mt.TxnSlot.BlobBundles[th.index]
		}
	}
	return blobBundles
}

func (p *TxPool) GetBlobs(blobHashes []common.Hash) []PoolBlobBundle {
	return p.getBlobsAndProofByBlobHashLocked(blobHashes)
}

// Cache recently mined blobs in anticipation of reorg, delete finalized ones
func (p *TxPool) processMinedFinalizedBlobs(minedTxns []*TxnSlot, finalizedBlock uint64) error {
	p.lastFinalizedBlock.Store(finalizedBlock)
	// Remove blobs in the finalized block and older, loop through all entries
	for l := len(p.minedBlobTxnsByBlock); l > 0 && finalizedBlock > 0; l-- {
		// delete individual hashes
		for _, mt := range p.minedBlobTxnsByBlock[finalizedBlock] {
			delete(p.minedBlobTxnsByHash, string(mt.TxnSlot.IDHash[:]))
		}
		// delete the map entry for this block num
		delete(p.minedBlobTxnsByBlock, finalizedBlock)
		// move on to older blocks, if present
		finalizedBlock--
	}

	// Add mined blobs
	minedBlock := p.lastSeenBlock.Load()
	p.minedBlobTxnsByBlock[minedBlock] = make([]*metaTxn, 0)
	for _, txn := range minedTxns {
		if txn.TxType() == BlobTxnType {
			mt := &metaTxn{TxnSlot: txn, minedBlockNum: minedBlock}
			p.minedBlobTxnsByBlock[minedBlock] = append(p.minedBlobTxnsByBlock[minedBlock], mt)
			mt.bestIndex = len(p.minedBlobTxnsByBlock[minedBlock]) - 1
			p.minedBlobTxnsByHash[string(txn.IDHash[:])] = mt
		}
	}
	return nil
}

// Delete individual hash entries from minedBlobTxns cache
func (p *TxPool) deleteMinedBlobTxn(hash string) {
	mt, exists := p.minedBlobTxnsByHash[hash]
	if !exists {
		return
	}
	l := len(p.minedBlobTxnsByBlock[mt.minedBlockNum])
	if l > 1 {
		p.minedBlobTxnsByBlock[mt.minedBlockNum][mt.bestIndex] = p.minedBlobTxnsByBlock[mt.minedBlockNum][l-1]
	}
	p.minedBlobTxnsByBlock[mt.minedBlockNum] = p.minedBlobTxnsByBlock[mt.minedBlockNum][:l-1]
	delete(p.minedBlobTxnsByHash, hash)
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
func (p *TxPool) removeMined(byNonce *BySenderAndNonce, minedTxns []*TxnSlot) error {
	noncesToRemove := map[uint64]uint64{}
	for _, txn := range minedTxns {
		nonce, ok := noncesToRemove[txn.SenderID]
		if !ok || txn.Nonce > nonce {
			noncesToRemove[txn.SenderID] = txn.Nonce // TODO: after 7702 nonce can be incremented more than once, may affect this
		}
	}

	var toDel []*metaTxn // can't delete items while iterate them

	discarded := 0

	for senderID, nonce := range noncesToRemove {
		byNonce.ascend(senderID, func(mt *metaTxn) bool {
			if mt.TxnSlot.Nonce > nonce {
				if mt.TxnSlot.Traced {
					p.logger.Debug("[txpool] removing mined, cmp nonces", "tx.nonce", mt.TxnSlot.Nonce, "sender.nonce", nonce)
				}

				return false
			}

			if mt.TxnSlot.Traced {
				p.logger.Info("TX TRACING: removeMined", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderId", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "currentSubPool", mt.currentSubPool)
			}

			toDel = append(toDel, mt)
			p.removeFromSubPool(mt, "remove-mined")
			return true
		})

		discarded += len(toDel)

		for _, mt := range toDel {
			p.discardLocked(mt, txpoolcfg.Mined)
		}
		toDel = toDel[:0]
	}

	if discarded > 0 {
		p.logger.Debug("Discarded transactions", "count", discarded)
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
	var toDel []*metaTxn                       // can't delete items while iterate them
	var toDelReasons []txpoolcfg.DiscardReason // parallel reasons slice for toDel

	p.all.ascend(senderID, func(mt *metaTxn) bool {
		deleteAndContinueReasonLog := ""
		discardReason := txpoolcfg.NonceTooLow
		if senderNonce > mt.TxnSlot.Nonce {
			deleteAndContinueReasonLog = "low nonce"
		} else if p.cfg.MaxNonceGap > 0 && mt.TxnSlot.Nonce > noGapsNonce && mt.TxnSlot.Nonce-noGapsNonce > p.cfg.MaxNonceGap {
			// Evict "zombie" queued transactions whose nonce is so far ahead of the sender's
			// on-chain nonce (accounting for any consecutive txns already in the pool) that they
			// can practically never become pending. This prevents unbounded pool bloat from accounts
			// that submitted transactions with impossibly large nonce gaps (e.g. nonce 144968 when
			// on-chain nonce is 6398). The gap threshold is configurable via MaxNonceGap (default 64).
			deleteAndContinueReasonLog = "nonce gap too large"
			discardReason = txpoolcfg.NonceTooDistant
		} else if mt.TxnSlot.Nonce != noGapsNonce && mt.TxnSlot.TxType() == BlobTxnType { // Discard nonce-gapped blob txns
			deleteAndContinueReasonLog = "nonce-gapped blob txn"
		}
		if deleteAndContinueReasonLog != "" {
			if mt.TxnSlot.Traced {
				logger.Info("TX TRACING: onSenderStateChange loop iteration remove", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderID", senderID, "senderNonce", senderNonce, "txn.nonce", mt.TxnSlot.Nonce, "currentSubPool", mt.currentSubPool, "reason", deleteAndContinueReasonLog)
			}
			p.removeFromSubPool(mt, deleteAndContinueReasonLog)
			toDel = append(toDel, mt)
			toDelReasons = append(toDelReasons, discardReason)
			return true
		}

		if minFeeCap.Gt(mt.TxnSlot.GetFeeCap()) {
			*minFeeCap = *mt.TxnSlot.GetFeeCap()
		}
		mt.minFeeCap = *minFeeCap
		if mt.TxnSlot.GetTipCap().IsUint64() {
			minTip = min(minTip, mt.TxnSlot.GetTipCap().Uint64())
		}
		mt.minTip = minTip

		mt.nonceDistance = 0
		if mt.TxnSlot.Nonce > senderNonce { // no uint underflow
			mt.nonceDistance = mt.TxnSlot.Nonce - senderNonce
		}

		needBalance := requiredBalance(mt.TxnSlot)

		// 2. Absence of nonce gaps. Set to 1 for transactions whose nonce is N, state nonce for
		// the sender is M, and there are transactions for all nonces between M and N from the same
		// sender. Set to 0 is the transaction's nonce is divided from the state nonce by one or more nonce gaps.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.TxnSlot.Nonce {
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
		if mt.TxnSlot.Nonce >= senderNonce {
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
		if mt.TxnSlot.GetGas() < blockGasLimit {
			mt.subPool |= NotTooMuchGas
		}

		if mt.TxnSlot.Traced {
			logger.Info("TX TRACING: onSenderStateChange loop iteration update", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderId", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "subPool", mt.currentSubPool)
		}

		// Some fields of mt might have changed, need to fix the invariants in the subpool best and worst queues
		p.updateInSubPool(mt)
		return true
	})

	for i, mt := range toDel {
		p.discardLocked(mt, toDelReasons[i])
	}

	logger.Trace("[txpool] onSenderStateChange", "sender", senderID, "count", p.all.count(senderID), "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
}

// promote reasserts invariants of the subpool and returns the list of transactions that ended up
// being promoted to the pending or basefee pool, for re-broadcasting
func (p *TxPool) promote(pendingBaseFee uint64, pendingBlobFee uint64, announcements *Announcements, logger log.Logger) {
	// Demote worst transactions that do not qualify for pending sub pool anymore, to other sub pools, or discard
	for worst := p.pending.Worst(); p.pending.Len() > 0 && (worst.subPool < BaseFeePoolBits || worst.minFeeCap.LtUint64(pendingBaseFee) || (worst.TxnSlot.TxType() == BlobTxnType && worst.TxnSlot.GetBlobFeeCap().LtUint64(pendingBlobFee))); worst = p.pending.Worst() {
		tx := p.pending.PopWorst()
		if worst.subPool >= BaseFeePoolBits {
			p.baseFee.Add(tx, "demote-pending", logger)
		} else {
			p.queued.Add(tx, "demote-pending", logger)
		}
	}

	// Promote best transactions from base fee pool to pending pool while they qualify
	for best := p.baseFee.Best(); p.baseFee.Len() > 0 && best.subPool >= BaseFeePoolBits && best.minFeeCap.CmpUint64(pendingBaseFee) >= 0 && (best.TxnSlot.TxType() != BlobTxnType || best.TxnSlot.GetBlobFeeCap().CmpUint64(pendingBlobFee) >= 0); best = p.baseFee.Best() {
		tx := p.baseFee.PopBest()
		announcements.Append(tx.TxnSlot.TxType(), tx.TxnSlot.Size, tx.TxnSlot.IDHash[:])
		p.pending.Add(tx, logger)
	}

	// Demote worst transactions that do not qualify for base fee pool anymore, to queued sub pool, or discard
	for worst := p.baseFee.Worst(); p.baseFee.Len() > 0 && worst.subPool < BaseFeePoolBits; worst = p.baseFee.Worst() {
		tx := p.baseFee.PopWorst()
		p.queued.Add(tx, "demote-base", logger)
	}

	// Promote best transactions from the queued pool to either pending or base fee pool, while they qualify
	for best := p.queued.Best(); p.queued.Len() > 0 && best.subPool >= BaseFeePoolBits; best = p.queued.Best() {
		tx := p.queued.PopBest()
		if best.minFeeCap.Cmp(uint256.NewInt(pendingBaseFee)) >= 0 {
			announcements.Append(tx.TxnSlot.TxType(), tx.TxnSlot.Size, tx.TxnSlot.IDHash[:])
			p.pending.Add(tx, logger)
		} else {
			p.baseFee.Add(tx, "promote-queued", logger)
		}
	}

	// Discard worst transactions from the queued sub pool if they do not qualify
	// <FUNCTIONALITY REMOVED>

	// Discard worst transactions from pending pool until it is within capacity limit
	for p.pending.Len() > p.pending.limit {
		tx := p.pending.PopWorst()
		p.discardLocked(tx, txpoolcfg.PendingPoolOverflow)
	}

	// Discard worst transactions from pending sub pool until it is within capacity limits
	for p.baseFee.Len() > p.baseFee.limit {
		tx := p.baseFee.PopWorst()
		p.discardLocked(tx, txpoolcfg.BaseFeePoolOverflow)
	}

	// Discard worst transactions from the queued sub pool until it is within its capacity limits
	for p.queued.Len() > p.queued.limit {
		tx := p.queued.PopWorst()
		p.discardLocked(tx, txpoolcfg.QueuedPoolOverflow)
	}
}

// Run - does:
// send pending byHash to p2p:
//   - new byHash
//   - all pooled byHash to recently connected peers
//   - all local pooled byHash to random peers periodically
//
// promote/demote transactions
// reorgs
func (p *TxPool) Run(ctx context.Context) error {
	defer p.logger.Info("[txpool] stopped")
	defer p.poolDB.Close()
	defer func() {
		p.lock.Lock()
		p.lastSeenCond.Broadcast() // to unblock .best() wait on cond
		p.lock.Unlock()
	}()
	p.p2pFetcher.ConnectCore()
	p.p2pFetcher.ConnectSentries()
	defer p.p2pFetcher.Wait()

	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxnsEvery := time.NewTicker(p.cfg.ProcessRemoteTxnsEvery)
	defer processRemoteTxnsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	// Dormancy sweep: use a one-shot timer (not a ticker) so the interval can be adjusted
	// dynamically after each sweep based on pool pressure and back-off.
	sweepBackoff := 1.0
	initialSweepInterval := p.nextDormancySweepInterval(&sweepBackoff, 0, 0)
	sweepBackoff = 1.0 // reset after the dry-run call above
	dormancySweep := time.NewTimer(initialSweepInterval)
	defer dormancySweep.Stop()

	if err := p.start(ctx); err != nil {
		p.logger.Error("[txpool] Failed to start", "err", err)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			_, flushErr := p.flush(context.Background()) // need background ctx since the other one is cancelled
			if flushErr != nil {
				err = fmt.Errorf("%w: %w", flushErr, err)
			}
			return err
		case <-logEvery.C:
			p.logStats()
		case <-processRemoteTxnsEvery.C:
			if !p.Started() {
				continue
			}

			if err := p.processRemoteTxns(ctx); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue
				}

				p.logger.Error("[txpool] process batch remote txns", "err", err)
			}
		case <-commitEvery.C:
			if p.poolDB != nil && p.Started() {
				t := time.Now()
				written, err := p.flush(ctx)
				if err != nil {
					p.logger.Error("[txpool] flush is local history", "err", err)
					continue
				}
				writeToDBBytesCounter.SetUint64(written)
				p.logger.Debug("[txpool] Commit", "written_kb", written/1024, "in", time.Since(t))
			}
		case <-dormancySweep.C:
			if p.Started() {
				evicted, queuedLen := p.sweepDormantQueued(ctx, p.lastSeenBlock.Load(), p.logger)
				next := p.nextDormancySweepInterval(&sweepBackoff, evicted, queuedLen)
				dormancySweep.Reset(next)
			} else {
				dormancySweep.Reset(30 * time.Second)
			}
		case announcements := <-p.newPendingTxns:
			go func() {
				for i := 0; i < 16; i++ { // drain more events from channel, then merge and dedup them
					select {
					case a := <-p.newPendingTxns:
						announcements.AppendOther(a)
						continue
					default:
					}
					break
				}
				if announcements.Len() == 0 {
					return
				}
				defer propagateNewTxnsTimer.ObserveDuration(time.Now())

				announcements = announcements.DedupCopy()

				p.builderNotifyNewTxns()

				if p.cfg.NoGossip {
					// drain newTxns for emptying newTxn channel
					// newTxn channel will be filled only with local transactions
					// early return to avoid outbound transaction propagation
					p.logger.Debug("[txpool] txn gossip disabled", "state", "drain new transactions")
					return
				}

				var localTxnTypes []byte
				var localTxnSizes []uint32
				var localTxnHashes Hashes
				var localTxnRlps [][]byte
				var remoteTxnTypes []byte
				var remoteTxnSizes []uint32
				var remoteTxnHashes Hashes
				var remoteTxnRlps [][]byte
				var broadcastHashes Hashes
				slotsRlp := make([][]byte, 0, announcements.Len())

				if err := p.poolDB.View(ctx, func(tx kv.Tx) error {
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
						slotRlp, err2 := types.UnwrapTxPlayloadRlp(slotRlp)
						if err2 != nil {
							continue
						}

						// Empty rlp can happen if a transaction we want to broadcast has just been mined, for example
						slotsRlp = append(slotsRlp, slotRlp)
						if p.IsLocal(hash) {
							localTxnTypes = append(localTxnTypes, t)
							localTxnSizes = append(localTxnSizes, size)
							localTxnHashes = append(localTxnHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if t != BlobTxnType {
								localTxnRlps = append(localTxnRlps, slotRlp)
								broadcastHashes = append(broadcastHashes, hash...)
							}
						} else {
							remoteTxnTypes = append(remoteTxnTypes, t)
							remoteTxnSizes = append(remoteTxnSizes, size)
							remoteTxnHashes = append(remoteTxnHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if t != BlobTxnType && len(slotRlp) < txMaxBroadcastSize {
								remoteTxnRlps = append(remoteTxnRlps, slotRlp)
							}
						}
					}
					return nil
				}); err != nil {
					p.logger.Error("[txpool] collect info to propagate", "err", err)
					return
				}
				if p.newSlotsStreams != nil {
					p.newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: slotsRlp}, p.logger)
				}

				// broadcast local transactions
				const localTxnsBroadcastMaxPeers uint64 = 10
				txnSentTo := p.p2pSender.BroadcastPooledTxns(localTxnRlps, localTxnsBroadcastMaxPeers)
				for i, peer := range txnSentTo {
					p.logger.Trace("Local txn broadcast", "txHash", hex.EncodeToString(broadcastHashes.At(i)), "to peer", peer)
				}
				hashSentTo := p.p2pSender.AnnouncePooledTxns(localTxnTypes, localTxnSizes, localTxnHashes, localTxnsBroadcastMaxPeers*2)
				for i := 0; i < localTxnHashes.Len(); i++ {
					hash := localTxnHashes.At(i)
					p.logger.Trace("Local txn announced", "txHash", hex.EncodeToString(hash), "to peer", hashSentTo[i], "baseFee", p.pendingBaseFee.Load())
				}

				// broadcast remote transactions
				const remoteTxnsBroadcastMaxPeers uint64 = 3
				p.p2pSender.BroadcastPooledTxns(remoteTxnRlps, remoteTxnsBroadcastMaxPeers)
				p.p2pSender.AnnouncePooledTxns(remoteTxnTypes, remoteTxnSizes, remoteTxnHashes, remoteTxnsBroadcastMaxPeers*2)
			}()
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			if p.cfg.NoGossip {
				// avoid transaction gossiping for new peers
				p.logger.Debug("[txpool] txn gossip disabled", "state", "sync new peers")
				continue
			}
			t := time.Now()
			var hashes Hashes
			var types []byte
			var sizes []uint32
			types, sizes, hashes = p.AppendAllAnnouncements(types, sizes, hashes[:0])
			go p.p2pSender.PropagatePooledTxnsToPeersList(newPeers, types, sizes, hashes)
			propagateToNewPeerTimer.ObserveDuration(t)
		}
	}
}

func (p *TxPool) flushNoFsync(ctx context.Context) (written uint64, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db txn is done inside lock, to make last writes visible for all read operations
	if err := p.poolDB.UpdateNosync(ctx, func(tx kv.RwTx) error {
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

func (p *TxPool) flush(ctx context.Context) (written uint64, err error) {
	defer writeToDBTimer.ObserveDuration(time.Now())
	// 1. get global lock on txpool and flush it to db, without fsync (to release lock asap)
	// 2. then fsync db without txpool lock
	written, err = p.flushNoFsync(ctx)
	if err != nil {
		return 0, err
	}

	// fsync. increase state version - just to make RwTx non-empty (mdbx skips empty RwTx)
	if err := p.poolDB.Update(ctx, func(tx kv.RwTx) error {
		v, err := tx.GetOne(kv.PoolInfo, PoolStateVersion)
		if err != nil {
			return err
		}
		var version uint64
		if len(v) == 8 {
			version = binary.BigEndian.Uint64(v)
		}
		version++
		return tx.Put(kv.PoolInfo, PoolStateVersion, hexutil.EncodeTs(version))
	}); err != nil {
		return 0, err
	}
	return written, nil
}

func (p *TxPool) flushLocked(tx kv.RwTx) (err error) {
	for i, mt := range p.deletedTxns {
		if mt == nil {
			continue
		}
		id := mt.TxnSlot.SenderID
		idHash := mt.TxnSlot.IDHash[:]
		if !p.all.hasTxns(id) {
			addr, ok := p.senders.senderID2Addr[id]
			if ok {
				delete(p.senders.senderID2Addr, id)
				delete(p.senders.senderIDs, addr)
			}
		}
		//fmt.Printf("del:%d,%d,%d\n", mt.TxnSlot.senderID, mt.TxnSlot.nonce, mt.TxnSlot.tip)
		has, err := tx.Has(kv.PoolTransaction, idHash)
		if err != nil {
			return err
		}
		if has {
			if err := tx.Delete(kv.PoolTransaction, idHash); err != nil {
				return err
			}
		}
		p.deletedTxns[i] = nil // for gc
	}

	txHashes := p.isLocalLRU.Keys()
	encID := make([]byte, 8)
	if err := tx.ClearTable(kv.RecentLocalTransaction); err != nil {
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
		if metaTx.TxnSlot.Rlp == nil {
			continue
		}
		v = common.EnsureEnoughSize(v, 20+len(metaTx.TxnSlot.Rlp))

		addr, ok := p.senders.senderID2Addr[metaTx.TxnSlot.SenderID]
		if !ok {
			p.logger.Warn("[txpool] flush: sender address not found by ID", "senderID", metaTx.TxnSlot.SenderID)
			continue
		}

		copy(v[:20], addr.Bytes())
		copy(v[20:], metaTx.TxnSlot.Rlp)

		has, err := tx.Has(kv.PoolTransaction, []byte(txHash))
		if err != nil {
			return err
		}
		if !has {
			if err := tx.Put(kv.PoolTransaction, []byte(txHash), v); err != nil {
				return err
			}
		}
		metaTx.TxnSlot.Rlp = nil
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

	// clean - in-memory data structure as later as possible - because if during this txn will happen error,
	// DB will stay consistent but some in-memory structures may be already cleaned, and retry will not work
	// failed write transaction must not create side-effects
	p.deletedTxns = p.deletedTxns[:0]
	return nil
}

func (p *TxPool) fromDB(ctx context.Context, tx kv.Tx, coreTx kv.TemporalTx) error {
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
		// last seen to make sure that the txn pool is in
		// sync with the processed blocks

		p.lastSeenBlock.Store(lastSeenProgress)
	}

	cacheView, err := p._stateCache.View(ctx, coreTx)
	if err != nil {
		return err
	}
	it, err := tx.Range(kv.RecentLocalTransaction, nil, nil, order.Asc, kv.Unlim)
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

	txns := TxnSlots{}
	parseCtx := NewTxnParseContext(p.chainID)
	parseCtx.WithSender(false)

	i := 0
	it, err = tx.Range(kv.PoolTransaction, nil, nil, order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		addr, txnRlp := *(*[20]byte)(v[:20]), v[20:]
		txn := &TxnSlot{}

		// TODO(eip-4844) ensure wrappedWithBlobs when transactions are saved to the DB
		_, err = parseCtx.ParseTransaction(txnRlp, 0, txn, nil, false /* hasEnvelope */, true /*wrappedWithBlobs*/, nil)
		if err != nil {
			err = fmt.Errorf("err: %w, rlp: %x", err, txnRlp)
			p.logger.Warn("[txpool] fromDB: parseTransaction", "err", err)
			continue
		}
		txn.Rlp = nil // means that we don't need store it in db anymore

		txn.SenderID, txn.Traced = p.senders.getOrCreateID(addr, p.logger)
		isLocalTx := p.isLocalLRU.Contains(string(k))

		if reason := p.validateTx(txn, isLocalTx, cacheView); reason != txpoolcfg.NotSet && reason != txpoolcfg.Success {
			return nil // TODO: Clarify - if one of the txns has the wrong reason, no pooled txns!
		}
		txns.Resize(uint(i + 1))
		txns.Txns[i] = txn
		txns.IsLocal[i] = isLocalTx
		copy(txns.Senders.At(i), addr[:])
		i++
	}

	var pendingBaseFee, pendingBlobFee, minBlobGasPrice, blockGasLimit uint64

	if p.feeCalculator != nil {
		pendingBaseFee, pendingBlobFee, minBlobGasPrice, blockGasLimit, err = p.feeCalculator.CurrentFees(p.chainConfig, coreTx)
		if err != nil {
			return err
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
		if p.chainConfig.DefaultBlockGasLimit != nil {
			blockGasLimit = *p.chainConfig.DefaultBlockGasLimit
		} else {
			blockGasLimit = ethconfig.DefaultBlockGasLimit
		}
	}

	err = p.senders.registerNewSenders(&txns, p.logger)
	if err != nil {
		return err
	}
	if _, _, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, txns,
		pendingBaseFee, pendingBlobFee, blockGasLimit, false, p.logger); err != nil {
		return err
	}

	// Restore persisted dormancy state. On the first run after this feature was introduced
	// the table is empty, so persistedActivity will be empty and all senders fall through
	// to the cold-start block below.
	if persistedActivity, err := LoadSenderLastActivity(tx); err != nil {
		p.logger.Warn("[txpool] failed to load sender last activity from db, using cold-start fallback", "err", err)
	} else {
		p.senderLastActivity = persistedActivity
	}

	// Cold-start fallback: for any queued sender not found in the persisted map, assign
	// the current block as the baseline so they receive a full grace period. This covers
	// the first run after feature introduction and senders added after the last sweep.
	coldStartBlock := p.lastSeenBlock.Load()
	p.all.ascendAll(func(mt *metaTxn) bool {
		if mt.currentSubPool == QueuedSubPool {
			senderID := mt.TxnSlot.SenderID
			if _, ok := p.senderLastActivity[senderID]; !ok {
				p.senderLastActivity[senderID] = coldStartBlock
			}
		}
		return true
	})

	// Initialise cached pendingBaseFee values in all queues so that their
	// comparators use the correct base fee even before the first OnNewBlock.
	var pendingBaseFee256 uint256.Int
	pendingBaseFee256.SetUint64(pendingBaseFee)
	p.setPendingBaseFeeOnQueues(pendingBaseFee256)
	p.pendingBaseFee.Store(pendingBaseFee)
	p.pendingBlobFee.Store(pendingBlobFee)
	p.blockGasLimit.Store(blockGasLimit)
	return nil
}

// nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%s\n", j.TxnSlot.SenderID, j.TxnSlot.Nonce, j.TxnSlot.GetTipCap())
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for _, mt := range p.pending.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.pending: %b,%d,%d,%s", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.GetTipCap()))
	}
	for _, mt := range p.baseFee.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.baseFee : %b,%d,%d,%s", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.GetTipCap()))
	}
	for _, mt := range p.queued.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.queued : %b,%d,%d,%s", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.GetTipCap()))
	}
}

func (p *TxPool) logStats() {
	if !p.Started() {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	ctx := []any{
		"pending", p.pending.Len(),
		"baseFee", p.baseFee.Len(),
		"queued", p.queued.Len(),
	}
	cacheKeys := p._stateCache.Len()
	if cacheKeys > 0 {
		ctx = append(ctx, "cache_keys", cacheKeys)
	}
	p.logger.Info("[txpool] stat", ctx...)
	pendingSubCounter.SetInt(p.pending.Len())
	basefeeSubCounter.SetInt(p.baseFee.Len())
	queuedSubCounter.SetInt(p.queued.Len())
}

// Deprecated need switch to streaming-like
func (p *TxPool) deprecatedForEach(f func(rlp []byte, sender common.Address, t SubPoolType), tx kv.Tx) {
	var txns []*metaTxn
	var senders []common.Address
	var subPoolTypes []SubPoolType
	var rlpValues [][]byte

	p.lock.Lock()

	p.all.ascendAll(func(mt *metaTxn) bool {
		if sender, found := p.senders.senderID2Addr[mt.TxnSlot.SenderID]; found {
			txns = append(txns, mt)
			senders = append(senders, sender)
			subPoolTypes = append(subPoolTypes, mt.currentSubPool)
			rlpValues = append(rlpValues, mt.TxnSlot.Rlp)
		}

		return true
	})

	p.lock.Unlock()

	for i := range txns {
		slotRlp := rlpValues[i]
		if slotRlp == nil {
			v, err := tx.GetOne(kv.PoolTransaction, txns[i].TxnSlot.IDHash[:])
			if err != nil {
				p.logger.Warn("[txpool] foreach: get txn from db", "err", err)
				continue
			}
			if v == nil {
				p.logger.Warn("[txpool] foreach: txn not found in db")
				continue
			}
			slotRlp = v[20:]
		}

		f(slotRlp, senders[i], subPoolTypes[i])
	}
}
