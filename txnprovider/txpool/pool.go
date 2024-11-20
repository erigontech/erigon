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
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/u256"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

const DefaultBlockGasLimit = uint64(30000000)

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
	OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) error
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	FilterKnownIdHashes(tx kv.Tx, hashes Hashes) (unknownHashes Hashes, err error)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)

	AddNewGoodPeer(peerID PeerID)
}

var _ Pool = (*TxPool)(nil) // compile-time interface check

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// txpool doesn't start any goroutines - "leave concurrency to user" design
// txpool has no DB-TX fields - "leave db transactions management to user" design
// txpool has _chainDB field - but it must maximize local state cache hit-rate - and perform minimum _chainDB transactions
//
// It preserve TxnSlot objects immutable
type TxPool struct {
	_chainDB               kv.RoDB // remote db - use it wisely
	_stateCache            kvcache.Cache
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
	pragueTime              *uint64
	isPostPrague            atomic.Bool
	maxBlobsPerBlock        uint64
	feeCalculator           FeeCalculator
	logger                  log.Logger
}

type FeeCalculator interface {
	CurrentFees(chainConfig *chain.Config, db kv.Getter) (baseFee uint64, blobFee uint64, minBlobGasPrice, blockGasLimit uint64, err error)
}

func New(
	newTxns chan Announcements,
	coreDB kv.RoDB,
	cfg txpoolcfg.Config,
	cache kvcache.Cache,
	chainID uint256.Int,
	shanghaiTime *big.Int,
	agraBlock *big.Int,
	cancunTime *big.Int,
	pragueTime *big.Int,
	maxBlobsPerBlock uint64,
	feeCalculator FeeCalculator,
	logger log.Logger,
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
		tree:              btree.NewG[*metaTxn](32, SortByNonceLess),
		search:            &metaTxn{TxnSlot: &TxnSlot{}},
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
		_chainDB:                coreDB,
		cfg:                     cfg,
		chainID:                 chainID,
		unprocessedRemoteTxns:   &TxnSlots{},
		unprocessedRemoteByHash: map[string]int{},
		minedBlobTxnsByBlock:    map[uint64][]*metaTxn{},
		minedBlobTxnsByHash:     map[string]*metaTxn{},
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
	if pragueTime != nil {
		if !pragueTime.IsUint64() {
			return nil, errors.New("pragueTime overflow")
		}
		pragueTimeU64 := pragueTime.Uint64()
		res.pragueTime = &pragueTimeU64
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

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remote.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) error {
	defer newBlockTimer.ObserveDuration(time.Now())

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
		p.logger.Debug("[txpool] New block", "block", block, "unwound", len(unwindTxns.Txns), "mined", len(minedTxns.Txns), "baseFee", baseFee, "pending-pre", available, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len(), "err", err)
	}()

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
		p.all.ascendAll(func(mt *metaTxn) bool {
			var updated bool
			if mt.TxnSlot.Gas < stateChanges.BlockGasLimit {
				updated = (mt.subPool & NotTooMuchGas) > 0
				mt.subPool |= NotTooMuchGas
			} else {
				updated = (mt.subPool & NotTooMuchGas) == 0
				mt.subPool &^= NotTooMuchGas
			}

			if mt.TxnSlot.Traced {
				p.logger.Info("TX TRACING: on block gas limit update", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderId", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "subPool", mt.currentSubPool, "updated", updated)
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

	for i, txn := range unwindBlobTxns.Txns {
		if txn.Type == BlobTxnType {
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

	announcements, _, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, newTxns,
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
		case p.newPendingTxns <- p.promoted.Copy():
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

func (p *TxPool) AppendLocalAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for hash, txn := range p.byHash {
		if txn.subPool&IsLocal == 0 {
			continue
		}
		types = append(types, txn.TxnSlot.Type)
		sizes = append(sizes, txn.TxnSlot.Size)
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
		types = append(types, txn.TxnSlot.Type)
		sizes = append(sizes, txn.TxnSlot.Size)
		hashes = append(hashes, hash...)
	}
	for hash, txIdx := range p.unprocessedRemoteByHash {
		txnSlot := p.unprocessedRemoteTxns.Txns[txIdx]
		types = append(types, txnSlot.Type)
		sizes = append(sizes, txnSlot.Size)
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

func (p *TxPool) best(n uint16, txns *TxnsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64, yielded mapset.Set[[32]byte]) (bool, int, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for last := p.lastSeenBlock.Load(); last < onTopOf; last = p.lastSeenBlock.Load() {
		p.logger.Debug("[txpool] Waiting for block", "expecting", onTopOf, "lastSeen", last, "txRequested", n, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
		p.lastSeenCond.Wait()
	}

	best := p.pending.best

	isShanghai := p.isShanghai() || p.isAgra()

	txns.Resize(uint(min(int(n), len(best.ms))))
	var toRemove []*metaTxn
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

		if yielded.Contains(mt.TxnSlot.IDHash) {
			continue
		}

		if mt.TxnSlot.Gas >= p.blockGasLimit.Load() {
			// Skip transactions with very large gas limit
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
		blobCount := uint64(len(mt.TxnSlot.BlobHashes))
		if blobCount*fixedgas.BlobGasPerBlob > availableBlobGas {
			continue
		}
		availableBlobGas -= blobCount * fixedgas.BlobGasPerBlob

		// make sure we have enough gas in the caller to add this transaction.
		// not an exact science using intrinsic gas but as close as we could hope for at
		// this stage
		authorizationLen := uint64(len(mt.TxnSlot.Authorizations))
		intrinsicGas, _ := txpoolcfg.CalcIntrinsicGas(uint64(mt.TxnSlot.DataLen), uint64(mt.TxnSlot.DataNonZeroLen), authorizationLen, nil, mt.TxnSlot.Creation, true, true, isShanghai)
		if intrinsicGas > availableGas {
			// we might find another txn with a low enough intrinsic gas to include so carry on
			continue
		}
		availableGas -= intrinsicGas

		txns.Txns[count] = rlpTxn
		copy(txns.Senders.At(count), sender.Bytes())
		txns.IsLocal[count] = isLocal
		yielded.Add(mt.TxnSlot.IDHash)
		count++
	}

	txns.Resize(uint(count))
	if len(toRemove) > 0 {
		for _, mt := range toRemove {
			p.pending.Remove(mt, "best", p.logger)
		}
	}
	return true, count, nil
}

func (p *TxPool) YieldBest(n uint16, txns *TxnsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64, toSkip mapset.Set[[32]byte]) (bool, int, error) {
	return p.best(n, txns, tx, onTopOf, availableGas, availableBlobGas, toSkip)
}

func (p *TxPool) PeekBest(n uint16, txns *TxnsRlp, tx kv.Tx, onTopOf, availableGas, availableBlobGas uint64) (bool, error) {
	set := mapset.NewThreadUnsafeSet[[32]byte]()
	onTime, _, err := p.YieldBest(n, txns, tx, onTopOf, availableGas, availableBlobGas, set)
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

func toBlobs(_blobs [][]byte) []gokzg4844.Blob {
	blobs := make([]gokzg4844.Blob, len(_blobs))
	for i, _blob := range _blobs {
		var b gokzg4844.Blob
		copy(b[:], _blob)
		blobs[i] = b
	}
	return blobs
}

func (p *TxPool) validateTx(txn *TxnSlot, isLocal bool, stateCache kvcache.CacheView) txpoolcfg.DiscardReason {
	isShanghai := p.isShanghai() || p.isAgra()
	if isShanghai && txn.Creation && txn.DataLen > fixedgas.MaxInitCodeSize {
		return txpoolcfg.InitCodeTooLarge // EIP-3860
	}
	if txn.Type == BlobTxnType {
		if !p.isCancun() {
			return txpoolcfg.TypeNotActivated
		}
		if txn.Creation {
			return txpoolcfg.InvalidCreateTxn
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

	authorizationLen := len(txn.Authorizations)
	if txn.Type == SetCodeTxnType {
		if !p.isPrague() {
			return txpoolcfg.TypeNotActivated
		}
		if txn.Creation {
			return txpoolcfg.InvalidCreateTxn
		}
		if authorizationLen == 0 {
			return txpoolcfg.NoAuthorizations
		}
	}

	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && uint256.NewInt(p.cfg.MinFeeCap).Cmp(&txn.FeeCap) == 1 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx underpriced idHash=%x local=%t, feeCap=%d, cfg.MinFeeCap=%d", txn.IDHash, isLocal, txn.FeeCap, p.cfg.MinFeeCap))
		}
		return txpoolcfg.UnderPriced
	}
	gas, reason := txpoolcfg.CalcIntrinsicGas(uint64(txn.DataLen), uint64(txn.DataNonZeroLen), uint64(authorizationLen), nil, txn.Creation, true, true, isShanghai)
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
func requiredBalance(txn *TxnSlot) *uint256.Int {
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

	headBlock, err := chain.CurrentBlockNumber(tx)
	if headBlock == nil || err != nil {
		return false
	}
	// A new block is built on top of the head block, so when the head is agraBlock-1,
	// the new block should use the Agra rules.
	activated := (*headBlock + 1) >= agraBlock
	if activated {
		p.isPostAgra.Swap(true)
	}
	return activated
}

func (p *TxPool) isCancun() bool {
	return isTimeBasedForkActivated(&p.isPostCancun, p.cancunTime)
}

func (p *TxPool) isPrague() bool {
	return isTimeBasedForkActivated(&p.isPostPrague, p.pragueTime)
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
		blobTxnMaxSize = 800_000
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
		return ErrRlpTooBig
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

	if err = p.senders.registerNewSenders(&newTxns, p.logger); err != nil {
		return nil, err
	}

	reasons, newTxns, err := p.validateTxns(&newTxns, cacheView)
	if err != nil {
		return nil, err
	}

	announcements, addReasons, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, newTxns,
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

	reasons = fillDiscardReasons(reasons, newTxns, p.discardReasonsLRU)
	for i, reason := range reasons {
		if reason == txpoolcfg.Success {
			txn := newTxns.Txns[i]
			if txn.Traced {
				p.logger.Info(fmt.Sprintf("TX TRACING: AddLocalTxns promotes idHash=%x, senderId=%d", txn.IDHash, txn.SenderID))
			}
			p.promoted.Append(txn.Type, txn.Size, txn.IDHash[:])
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

func (p *TxPool) coreDBWithCache() (kv.RoDB, kvcache.Cache) {
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
				announcements.Append(found.TxnSlot.Type, found.TxnSlot.Size, found.TxnSlot.IDHash[:])
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
func (p *TxPool) addTxnsOnNewBlock(blockNum uint64, cacheView kvcache.CacheView, stateChanges *remote.StateChangeBatch,
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

func (p *TxPool) addLocked(mt *metaTxn, announcements *Announcements) txpoolcfg.DiscardReason {
	// Insert to pending pool, if pool doesn't have txn with same Nonce and bigger Tip
	found := p.all.get(mt.TxnSlot.SenderID, mt.TxnSlot.Nonce)
	if found != nil {
		if found.TxnSlot.Type == BlobTxnType && mt.TxnSlot.Type != BlobTxnType {
			return txpoolcfg.BlobTxReplace
		}
		priceBump := p.cfg.PriceBump

		//Blob txn threshold checks for replace txn
		if mt.TxnSlot.Type == BlobTxnType {
			priceBump = p.cfg.BlobPriceBump
			blobFeeThreshold, overflow := (&uint256.Int{}).MulDivOverflow(
				&found.TxnSlot.BlobFeeCap,
				uint256.NewInt(100+priceBump),
				uint256.NewInt(100),
			)
			if mt.TxnSlot.BlobFeeCap.Lt(blobFeeThreshold) && !overflow {
				if bytes.Equal(found.TxnSlot.IDHash[:], mt.TxnSlot.IDHash[:]) {
					return txpoolcfg.NotSet
				}
				return txpoolcfg.ReplaceUnderpriced // TODO: This is the same as NotReplaced
			}
		}

		//Regular txn threshold checks
		tipThreshold := uint256.NewInt(0)
		tipThreshold = tipThreshold.Mul(&found.TxnSlot.Tip, uint256.NewInt(100+priceBump))
		tipThreshold.Div(tipThreshold, u256.N100)
		feecapThreshold := uint256.NewInt(0)
		feecapThreshold.Mul(&found.TxnSlot.FeeCap, uint256.NewInt(100+priceBump))
		feecapThreshold.Div(feecapThreshold, u256.N100)
		if mt.TxnSlot.Tip.Cmp(tipThreshold) < 0 || mt.TxnSlot.FeeCap.Cmp(feecapThreshold) < 0 {
			// Both tip and feecap need to be larger than previously to replace the transaction
			// In case if the transition is stuck, "poke" it to rebroadcast
			if mt.subPool&IsLocal != 0 && (found.currentSubPool == PendingSubPool || found.currentSubPool == BaseFeeSubPool) {
				announcements.Append(found.TxnSlot.Type, found.TxnSlot.Size, found.TxnSlot.IDHash[:])
			}
			if bytes.Equal(found.TxnSlot.IDHash[:], mt.TxnSlot.IDHash[:]) {
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

	// Don't add blob txn to queued if it's less than current pending blob base fee
	if mt.TxnSlot.Type == BlobTxnType && mt.TxnSlot.BlobFeeCap.LtUint64(p.pendingBlobFee.Load()) {
		return txpoolcfg.FeeTooLow
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
	if mt.TxnSlot.Type == BlobTxnType {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t + (uint64(len(mt.TxnSlot.BlobHashes))))
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
	if mt.TxnSlot.Type == BlobTxnType {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t - uint64(len(mt.TxnSlot.BlobHashes)))
	}
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
		if txn.Type == BlobTxnType {
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
			noncesToRemove[txn.SenderID] = txn.Nonce
		}
	}

	var toDel []*metaTxn // can't delete items while iterate them

	discarded := 0
	pendingRemoved := 0
	baseFeeRemoved := 0
	queuedRemoved := 0

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
	var toDel []*metaTxn // can't delete items while iterate them

	p.all.ascend(senderID, func(mt *metaTxn) bool {
		deleteAndContinueReasonLog := ""
		if senderNonce > mt.TxnSlot.Nonce {
			deleteAndContinueReasonLog = "low nonce"
		} else if mt.TxnSlot.Nonce != noGapsNonce && mt.TxnSlot.Type == BlobTxnType { // Discard nonce-gapped blob txns
			deleteAndContinueReasonLog = "nonce-gapped blob txn"
		}
		if deleteAndContinueReasonLog != "" {
			if mt.TxnSlot.Traced {
				logger.Info("TX TRACING: onSenderStateChange loop iteration remove", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderID", senderID, "senderNonce", senderNonce, "txn.nonce", mt.TxnSlot.Nonce, "currentSubPool", mt.currentSubPool, "reason", deleteAndContinueReasonLog)
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

		if minFeeCap.Gt(&mt.TxnSlot.FeeCap) {
			*minFeeCap = mt.TxnSlot.FeeCap
		}
		mt.minFeeCap = *minFeeCap
		if mt.TxnSlot.Tip.IsUint64() {
			minTip = min(minTip, mt.TxnSlot.Tip.Uint64())
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
		if mt.TxnSlot.Gas < blockGasLimit {
			mt.subPool |= NotTooMuchGas
		}

		if mt.TxnSlot.Traced {
			logger.Info("TX TRACING: onSenderStateChange loop iteration update", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderId", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "subPool", mt.currentSubPool)
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
func (p *TxPool) promote(pendingBaseFee uint64, pendingBlobFee uint64, announcements *Announcements, logger log.Logger) {
	// Demote worst transactions that do not qualify for pending sub pool anymore, to other sub pools, or discard
	for worst := p.pending.Worst(); p.pending.Len() > 0 && (worst.subPool < BaseFeePoolBits || worst.minFeeCap.LtUint64(pendingBaseFee) || (worst.TxnSlot.Type == BlobTxnType && worst.TxnSlot.BlobFeeCap.LtUint64(pendingBlobFee))); worst = p.pending.Worst() {
		if worst.subPool >= BaseFeePoolBits {
			tx := p.pending.PopWorst()
			announcements.Append(tx.TxnSlot.Type, tx.TxnSlot.Size, tx.TxnSlot.IDHash[:])
			p.baseFee.Add(tx, "demote-pending", logger)
		} else {
			p.queued.Add(p.pending.PopWorst(), "demote-pending", logger)
		}
	}

	// Promote best transactions from base fee pool to pending pool while they qualify
	for best := p.baseFee.Best(); p.baseFee.Len() > 0 && best.subPool >= BaseFeePoolBits && best.minFeeCap.CmpUint64(pendingBaseFee) >= 0 && (best.TxnSlot.Type != BlobTxnType || best.TxnSlot.BlobFeeCap.CmpUint64(pendingBlobFee) >= 0); best = p.baseFee.Best() {
		tx := p.baseFee.PopBest()
		announcements.Append(tx.TxnSlot.Type, tx.TxnSlot.Size, tx.TxnSlot.IDHash[:])
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
			announcements.Append(tx.TxnSlot.Type, tx.TxnSlot.Size, tx.TxnSlot.IDHash[:])
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

// MainLoop - does:
// send pending byHash to p2p:
//   - new byHash
//   - all pooled byHash to recently connected peers
//   - all local pooled byHash to random peers periodically
//
// promote/demote transactions
// reorgs
func MainLoop(ctx context.Context, db kv.RwDB, p *TxPool, newTxns chan Announcements, send *Send, newSlotsStreams *NewSlotsStreams, notifyMiningAboutNewSlots func()) {
	syncToNewPeersEvery := time.NewTicker(p.cfg.SyncToNewPeersEvery)
	defer syncToNewPeersEvery.Stop()
	processRemoteTxnsEvery := time.NewTicker(p.cfg.ProcessRemoteTxnsEvery)
	defer processRemoteTxnsEvery.Stop()
	commitEvery := time.NewTicker(p.cfg.CommitEvery)
	defer commitEvery.Stop()
	logEvery := time.NewTicker(p.cfg.LogEvery)
	defer logEvery.Stop()

	if err := p.Start(ctx, db); err != nil {
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
		case announcements := <-newTxns:
			go func() {
				for i := 0; i < 16; i++ { // drain more events from channel, then merge and dedup them
					select {
					case a := <-newTxns:
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

				notifyMiningAboutNewSlots()

				if p.cfg.NoGossip {
					// drain newTxns for emptying newTxn channel
					// newTxn channel will be filled only with local transactions
					// early return to avoid outbound transaction propagation
					log.Debug("[txpool] txn gossip disabled", "state", "drain new transactions")
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
				if newSlotsStreams != nil {
					newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: slotsRlp}, p.logger)
				}

				// broadcast local transactions
				const localTxnsBroadcastMaxPeers uint64 = 10
				txnSentTo := send.BroadcastPooledTxns(localTxnRlps, localTxnsBroadcastMaxPeers)
				for i, peer := range txnSentTo {
					p.logger.Trace("Local txn broadcast", "txHash", hex.EncodeToString(broadcastHashes.At(i)), "to peer", peer)
				}
				hashSentTo := send.AnnouncePooledTxns(localTxnTypes, localTxnSizes, localTxnHashes, localTxnsBroadcastMaxPeers*2)
				for i := 0; i < localTxnHashes.Len(); i++ {
					hash := localTxnHashes.At(i)
					p.logger.Trace("Local txn announced", "txHash", hex.EncodeToString(hash), "to peer", hashSentTo[i], "baseFee", p.pendingBaseFee.Load())
				}

				// broadcast remote transactions
				const remoteTxnsBroadcastMaxPeers uint64 = 3
				send.BroadcastPooledTxns(remoteTxnRlps, remoteTxnsBroadcastMaxPeers)
				send.AnnouncePooledTxns(remoteTxnTypes, remoteTxnSizes, remoteTxnHashes, remoteTxnsBroadcastMaxPeers*2)
			}()
		case <-syncToNewPeersEvery.C: // new peer
			newPeers := p.recentlyConnectedPeers.GetAndClean()
			if len(newPeers) == 0 {
				continue
			}
			if p.cfg.NoGossip {
				// avoid transaction gossiping for new peers
				log.Debug("[txpool] txn gossip disabled", "state", "sync new peers")
				continue
			}
			t := time.Now()
			var hashes Hashes
			var types []byte
			var sizes []uint32
			types, sizes, hashes = p.AppendAllAnnouncements(types, sizes, hashes[:0])
			go send.PropagatePooledTxnsToPeersList(newPeers, types, sizes, hashes)
			propagateToNewPeerTimer.ObserveDuration(t)
		}
	}
}

func (p *TxPool) flushNoFsync(ctx context.Context, db kv.RwDB) (written uint64, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	//it's important that write db txn is done inside lock, to make last writes visible for all read operations
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
	for i, mt := range p.deletedTxns {
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

	err = p.senders.registerNewSenders(&txns, p.logger)
	if err != nil {
		return err
	}
	if _, _, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, p.senders, txns,
		pendingBaseFee, pendingBlobFee, blockGasLimit, false, p.logger); err != nil {
		return err
	}
	p.pendingBaseFee.Store(pendingBaseFee)
	p.pendingBlobFee.Store(pendingBlobFee)
	p.blockGasLimit.Store(blockGasLimit)
	return nil
}

// nolint
func (p *TxPool) printDebug(prefix string) {
	fmt.Printf("%s.pool.byHash\n", prefix)
	for _, j := range p.byHash {
		fmt.Printf("\tsenderID=%d, nonce=%d, tip=%d\n", j.TxnSlot.SenderID, j.TxnSlot.Nonce, j.TxnSlot.Tip)
	}
	fmt.Printf("%s.pool.queues.len: %d,%d,%d\n", prefix, p.pending.Len(), p.baseFee.Len(), p.queued.Len())
	for _, mt := range p.pending.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.pending: %b,%d,%d,%d", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.Tip))
	}
	for _, mt := range p.baseFee.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.baseFee : %b,%d,%d,%d", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.Tip))
	}
	for _, mt := range p.queued.best.ms {
		mt.TxnSlot.PrintDebug(fmt.Sprintf("%s.queued : %b,%d,%d,%d", prefix, mt.subPool, mt.TxnSlot.SenderID, mt.TxnSlot.Nonce, mt.TxnSlot.Tip))
	}
}

func (p *TxPool) logStats() {
	if !p.Started() {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	ctx := []interface{}{
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
	p.all.ascendAll(func(mt *metaTxn) bool {
		slot := mt.TxnSlot
		slotRlp := slot.Rlp
		if slot.Rlp == nil {
			v, err := tx.GetOne(kv.PoolTransaction, slot.IDHash[:])
			if err != nil {
				p.logger.Warn("[txpool] foreach: get txn from db", "err", err)
				return true
			}
			if v == nil {
				p.logger.Warn("[txpool] foreach: txn not found in db")
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
