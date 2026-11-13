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
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/go-stack/stack"
	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common"
	libkzg "github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
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

// Bound deferred admission work and retained network payloads independently of pool capacity.
const (
	remoteTxnQueueLimit     = maxTransactionsPerPacket
	remoteTxnQueueByteLimit = 32 * 1024 * 1024
)

// Pool is interface for the transaction pool
// This interface exists for the convenience of testing, and not yet because
// there are multiple implementations
//
//go:generate mockgen -typed=true -destination=./pool_mock.go -package=txpool . Pool
type Pool interface {
	// Handle 3 main events - new remote txns from p2p, new local txns from RPC, new blocks from execution layer
	AddRemoteTxns(ctx context.Context, newTxns TxnSlots, peerID PeerID, sentry sentryproto.SentryClient)
	AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error)
	OnNewBlock(ctx context.Context, stateChanges *remoteproto.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) error
	MarkStateStale()
	RetainBogotaActivation(changes *remoteproto.StateChangeBatch)
	// IdHashKnown check whether transaction with given Id hash is known to the pool
	IdHashKnown(tx kv.Tx, hash []byte) (bool, error)
	FilterKnownIdHashes(tx kv.Tx, hashes Hashes) (unknownHashes Hashes, err error)
	HeldOrQueuedIdHashes(tx kv.Tx, hashes Hashes) (held, queued Hashes, err error)
	RejectFramePrefix(hash common.Hash)
	Started() bool
	GetRlp(tx kv.Tx, hash []byte) ([]byte, error)
	GetBlobs(blobhashes []common.Hash) []PoolBlobBundle
	AddNewGoodPeer(peerID PeerID)
}

var (
	_ Pool                    = (*TxPool)(nil) // compile-time interface check
	_ txnprovider.TxnProvider = (*TxPool)(nil)
)

// remoteSource keeps peer attribution when remote admission splits a batch.
type remoteSource struct {
	peerID PeerID
	sentry sentryproto.SentryClient
}

type blobCells map[common.Hash]*[goethkzg.CellsPerExtBlob]*goethkzg.Cell

func (c blobCells) add(hash common.Hash, cells *[goethkzg.CellsPerExtBlob]*goethkzg.Cell) {
	if _, ok := c[hash]; !ok && len(c) >= blobCellsCacheSize {
		return
	}
	c[hash] = cells
}

// TxPool - holds all pool-related data structures and lock-based tiny methods
// most of logic implemented by pure tests-friendly functions
//
// Run owns the remote frame worker and stops it before closing the pool database
// txpool has no DB-TX fields - "leave db transactions management to user" design
// txpool has _chainDB field - but it must maximize local state cache hit-rate - and perform minimum _chainDB transactions
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
	unprocessedRemotePeers  []remoteSource      // index-aligned with unprocessedRemoteTxns
	unprocessedRemoteByHash map[string]*TxnSlot // to reject duplicates
	unprocessedRemoteBytes  uint64
	byHash                  map[string]*metaTxn                             // All pooled transactions by hash, including parked frames
	discardReasonsLRU       *simplelru.LRU[string, txpoolcfg.DiscardReason] // txn_hash => discard_reason : non-persisted
	pending                 *PendingPool
	baseFee                 *SubPool
	queued                  *SubPool
	minedBlobTxnsByBlock    map[uint64][]*metaTxn            // (blockNum => slice): cache of recently mined blobs
	minedBlobTxnsByHash     map[string]*metaTxn              // (hash => mt): map of recently mined blobs
	isLocalLRU              *simplelru.LRU[string, struct{}] // txn_hash => is_local : to restore isLocal flag of unwinded transactions
	newPendingTxns          chan Announcements               // notifications about new txns in Pending sub-pool
	all                     *BySenderAndNonce                // Ordered by sender ID, nonce domain (account or keyed), then nonce
	deletedTxns             []*metaTxn                       // list of discarded txns since last db commit
	cfg                     txpoolcfg.Config
	chainID                 uint256.Int
	chainConfig             *chain.Config
	stateGeneration         uint64
	stateViewStale          bool
	stateBlockHash          common.Hash       // Head of the pool's state view; pins frame validation to the same block
	headIsBogota            bool              // Last completed update's activation, independent of pending-block time
	pendingActivation       *bogotaActivation // Activation observations from skipped batches, retained until an update succeeds
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
	cancunTime              *uint64
	isPostCancun            atomic.Bool
	pragueTime              *uint64
	isPostPrague            atomic.Bool
	osakaTime               *uint64
	isPostOsaka             atomic.Bool
	amsterdamTime           *uint64
	isPostAmsterdam         atomic.Bool
	bogotaTime              *uint64
	isPostBogota            atomic.Bool
	feeCalculator           FeeCalculator
	p2pFetcher              *Fetch
	p2pSender               *Send
	newSlotsStreams         *NewSlotsStreams
	ethBackend              remoteproto.ETHBACKENDClient
	builderNotifyNewTxns    func()
	logger                  log.Logger
	auths                   map[AuthAndNonce]*metaTxn // All authority accounts with a pooled authorization
	blobs                   *blobStore
	frameValidationGate     chan struct{}
	remoteFrames            remoteFrameQueue
	frameReservations       map[common.Address]frameReservation
	frameAccountsToRefresh  map[common.Address]struct{}
	frameRejections         *simplelru.LRU[string, frameRejection]

	// Dormancy eviction: tracks the block number of the last on-chain state change (nonce or
	// balance) for each sender that has transactions in the queued sub-pool. Senders absent
	// from on-chain activity for longer than cfg.QueuedDormancyDuration are evicted. The timer
	// resets only on real on-chain state changes — not on new transaction submissions — so
	// senders cannot game it by re-submitting transactions.
	senderLastActivity   map[uint64]uint64 // senderID → block of last on-chain state change
	avgBlockTimeMs       atomic.Int64      // EWMA of block-to-block wall-clock interval (ms); default 12 000
	lastBlockTimestampMs atomic.Int64      // unix-ms timestamp of the last processed block

	hasUnprocessedRemoteTxns atomic.Bool // lock-free way to check if pool needs to flush buffered remote txs
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
	frameRejections, err := simplelru.NewLRU[string, frameRejection](10_000, nil)
	if err != nil {
		return nil, err
	}
	remoteFrames, err := newRemoteFrameQueue()
	if err != nil {
		return nil, err
	}

	byNonce := &BySenderAndNonce{
		tree:              btree.NewG[*metaTxn](32, SortByNonceLess),
		search:            &metaTxn{TxnSlot: &TxnSlot{}},
		senderIDTxnCount:  map[uint64]int{},
		senderIDBlobCount: map[uint64]uint64{},
		senderIDFrameTxn:  map[uint64]*metaTxn{},
	}
	tracedSenders := make(map[common.Address]struct{})
	for _, sender := range cfg.TracedSenders {
		tracedSenders[common.BytesToAddress([]byte(sender))] = struct{}{}
	}

	configChainID := chainConfig.ChainID

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
		unprocessedRemoteByHash: map[string]*TxnSlot{},
		minedBlobTxnsByBlock:    map[uint64][]*metaTxn{},
		minedBlobTxnsByHash:     map[string]*metaTxn{},
		feeCalculator:           options.feeCalculator,
		ethBackend:              ethBackend,
		builderNotifyNewTxns:    builderNotifyNewTxns,
		newSlotsStreams:         newSlotsStreams,
		logger:                  logger,
		auths:                   make(map[AuthAndNonce]*metaTxn),
		blobs:                   newBlobStore(),
		frameValidationGate:     make(chan struct{}, 1),
		remoteFrames:            remoteFrames,
		frameAccountsToRefresh:  make(map[common.Address]struct{}),
		frameRejections:         frameRejections,
		senderLastActivity:      make(map[uint64]uint64),
	}
	// Seed the EWMA block time with 12 s (Ethereum mainnet slot time). The tracker adjusts
	// automatically after a few blocks, so the seed only affects the very first sweep interval.
	res.avgBlockTimeMs.Store(12_000)

	res.shanghaiTime = chainConfig.ShanghaiTime
	res.cancunTime = chainConfig.CancunTime
	res.pragueTime = chainConfig.PragueTime
	res.osakaTime = chainConfig.OsakaTime
	res.amsterdamTime = chainConfig.AmsterdamTime
	res.bogotaTime = chainConfig.BogotaTime

	res.p2pFetcher = NewFetch(ctx, sentryClients, res, stateChangesClient, poolDB, res.chainID, logger, opts...)
	res.p2pSender = NewSend(ctx, sentryClients, logger, opts...)

	return res, nil
}

func (p *TxPool) start(ctx context.Context) error {
	if p.started.Load() {
		return nil
	}

	if err := p.fromDB(ctx); err != nil {
		return fmt.Errorf("loading pool from DB: %w", err)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.started.CompareAndSwap(false, true) {
		p.logger.Info("[txpool] Started")
		p.lastSeenCond.Broadcast()
	}
	return nil
}

// MarkStateStale invalidates simulations and blocks selection and admission until
// a notification restores a usable state view.
func (p *TxPool) MarkStateStale() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.markStateStaleLocked()
}

func (p *TxPool) markStateStaleLocked() {
	if p.stateViewStale {
		return
	}
	p.stateViewStale = true
	p.stateGeneration++
	for _, mt := range p.all.senderIDFrameTxn {
		p.parkFrameLocked(mt, "state recovery pending")
	}
	p.lastSeenCond.Broadcast()
	p.publishDepthMetrics()
}

var errStateViewSuperseded = errors.New("announced state version has been superseded")

// Incomplete notifications require their exact committed state: combining their
// account changes with a newer snapshot can restore values from an orphaned block.
// Wait without holding the pool lock or a database snapshot.
func beginBlockState(ctx context.Context, db kv.TemporalRoDB, changes *remoteproto.StateChangeBatch) (kv.TemporalTx, error) {
	if !changes.StateChangesIncomplete {
		return db.BeginTemporalRo(ctx)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // caller owns the returned transaction; stale snapshots are rolled back below
		if err != nil {
			return nil, err
		}
		version, err := rawdb.GetStateVersion(tx)
		if err == nil && version == changes.StateVersionId {
			return tx, nil
		}
		tx.Rollback()
		if err != nil {
			return nil, err
		}
		if version > changes.StateVersionId {
			return nil, fmt.Errorf("%w: announced=%d committed=%d", errStateViewSuperseded, changes.StateVersionId, version)
		}
		if err := common.Sleep(waitCtx, 10*time.Millisecond); err != nil {
			return nil, fmt.Errorf("wait for announced state version %d: %w", changes.StateVersionId, err)
		}
	}
}

func (p *TxPool) OnNewBlock(ctx context.Context, stateChanges *remoteproto.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots) (err error) {
	defer newBlockTimer.ObserveDuration(time.Now())
	defer func() {
		if err != nil {
			p.lock.Lock()
			defer p.lock.Unlock()
			p.markStateStaleLocked()
			if errors.Is(err, errStateViewSuperseded) {
				activation := p.bogotaActivationAfter(stateChanges)
				p.pendingActivation = &activation
			}
		}
	}()
	p.lock.Lock()
	stateStale := p.stateViewStale
	if stateChanges.StateChangesIncomplete {
		p.markStateStaleLocked()
	}
	p.lock.Unlock()
	// A complete notification cannot repair a failed update on its own. The pool
	// needs a full refresh, without changing the batch shared with other consumers.
	if stateStale && !stateChanges.StateChangesIncomplete {
		stateChanges = proto.Clone(stateChanges).(*remoteproto.StateChangeBatch)
		stateChanges.StateChangesIncomplete = true
	}
	for _, txn := range unwindTxns.Txns {
		p.recoverAuthorizations(txn)
	}

	coreDB, cache := p.chainDB()
	coreTx, err := beginBlockState(ctx, coreDB, stateChanges)
	if err != nil {
		return err
	}
	defer coreTx.Rollback()
	cache.OnNewBlock(stateChanges)

	var poolTx kv.Tx
	if p.poolDB != nil {
		poolTx, err = p.poolDB.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer poolTx.Rollback()
	}

	if err := minedTxns.Valid(); err != nil {
		return err
	}

	cacheView, err := cache.View(ctx, coreTx)
	if err != nil {
		return err
	}

	frames, err := p.applyBlockUpdate(stateChanges, unwindTxns, unwindBlobTxns, minedTxns, cacheView, poolTx)
	if err != nil {
		return err
	}
	if err := p.revalidateFrames(ctx, frames.parked, frames.generation, frames.blockHash); err != nil {
		return err
	}
	// Retained activation transitions must survive failed or superseded updates.
	p.lock.Lock()
	if frames.generation == p.stateGeneration {
		p.headIsBogota = p.chainConfig.IsBogota(stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockTime)
		p.pendingActivation = nil
	}
	p.lock.Unlock()
	return nil
}

// applyBlockUpdate applies the announced state under the pool lock and returns
// revalidation work that needs frame simulation after releasing it.
func (p *TxPool) applyBlockUpdate(stateChanges *remoteproto.StateChangeBatch, unwindTxns, unwindBlobTxns, minedTxns TxnSlots,
	cacheView kvcache.CacheView, poolTx kv.Tx,
) (frames frameRevalidationBatch, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	block := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHeight
	baseFee := stateChanges.PendingBlockBaseFee
	p.stateGeneration++
	frames.generation = p.stateGeneration
	p.stateBlockHash = common.Hash{}
	if hash := stateChanges.ChangeBatch[len(stateChanges.ChangeBatch)-1].BlockHash; hash != nil {
		p.stateBlockHash = gointerfaces.ConvertH256ToHash(hash)
	}
	frames.blockHash = p.stateBlockHash
	defer func() {
		if err != nil {
			// Selection must not observe a partially applied update after the lock is released.
			p.markStateStaleLocked()
		} else {
			p.stateViewStale = false
			if block > p.lastSeenBlock.Load() {
				p.recordBlockTime(stateChanges.StateChangesIncomplete)
			}
			p.lastSeenBlock.Store(block)
		}
		p.lastSeenCond.Broadcast()
	}()

	pendingPre := p.pending.Len()
	defer func() {
		p.logger.Debug("[txpool] New block", "block", block,
			"unwound", len(unwindTxns.Txns), "mined", len(minedTxns.Txns), "blockBaseFee", baseFee,
			"pending-pre", pendingPre, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len(),
			"err", err)
	}()

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
			p.updateGasLimitMarker(mt, stateChanges.BlockGasLimit)
			p.updateInSubPool(mt)
			return true
		})
	}

	if err := p.senders.registerNewSenders(&minedTxns, p.logger); err != nil {
		return frames, err
	}
	defer p.forgetUnusedSenders(senderIDsOf(&minedTxns))
	if stateChanges.StateChangesIncomplete {
		// A missed unwind can leave an orphaned transaction marked as mined.
		// Check the recovered nonce state before removing pooled replacements.
		// An unused nonce alone does not establish an unwind or restore pool membership.
		minedTxns, _, err = p.partitionByNonceState(minedTxns, cacheView)
		if err != nil {
			return frames, err
		}
	}
	unwindCells := make(blobCells)
	for i, txn := range unwindBlobTxns.Txns {
		if txn.HasBlobs() {
			if poolTx == nil {
				continue
			}
			knownBlobTxn, err := p.getCachedBlobTxnLocked(poolTx, txn.IDHash[:])
			if err != nil {
				return frames, err
			}
			if knownBlobTxn != nil && p.validateBlobSidecar(knownBlobTxn.TxnSlot, unwindCells) == txpoolcfg.Success {
				unwindTxns.Append(knownBlobTxn.TxnSlot, unwindBlobTxns.Senders.At(i), false)
			}
		}
	}
	if err := p.senders.registerNewSenders(&unwindTxns, p.logger); err != nil {
		return frames, err
	}
	if stateChanges.StateChangesIncomplete {
		// A retained unwind may have been re-mined in an omitted block. Reconcile
		// its nonce before parking it, even when frame validation is unavailable.
		defer p.forgetUnusedSenders(senderIDsOf(&unwindTxns))
		var consumed TxnSlots
		consumed, unwindTxns, err = p.partitionByNonceState(unwindTxns, cacheView)
		if err != nil {
			return frames, err
		}
		for i, txn := range consumed.Txns {
			minedTxns.Append(txn, consumed.Senders.At(i), false)
		}
	}
	unwindTxns, unwindFrames := p.splitRestoredTxns(unwindTxns)

	_, unwindTxns, cells, err := p.validateTxns(&unwindTxns, cacheView, nil, nil)
	if err != nil {
		return frames, err
	}
	maps.Copy(cells, unwindCells)

	if dbg.AssertEnabled {
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

	p.processMinedFinalizedBlobs(minedTxns.Txns, block, stateChanges.FinalizedBlock)

	if err := p.removeMined(p.all, minedTxns.Txns); err != nil {
		return frames, err
	}
	p.restoreFramesLocked(block, unwindFrames)

	var announcements Announcements
	announcements, err = p.addTxnsOnNewBlock(block, cacheView, stateChanges, unwindTxns, cells)
	if err != nil {
		return frames, err
	}
	frames.parked = p.invalidateFrames(stateChanges)

	p.pending.EnforceWorstInvariants()
	p.baseFee.EnforceInvariants()
	p.queued.EnforceInvariants()
	if err := p.promote(cacheView, pendingBaseFee, pendingBlobFee, &announcements); err != nil {
		return frames, err
	}
	p.notifyPendingTxns(announcements)

	p.publishDepthMetrics()
	return frames, nil
}

func (p *TxPool) partitionByNonceState(txns TxnSlots, view kvcache.CacheView) (consumed, unconsumed TxnSlots, err error) {
	for i, txn := range txns.Txns {
		used, err := p.nonceConsumed(txn, view)
		if err != nil {
			return consumed, unconsumed, err
		}
		if used {
			consumed.Append(txn, txns.Senders.At(i), false)
		} else {
			unconsumed.Append(txn, txns.Senders.At(i), false)
		}
	}
	return consumed, unconsumed, nil
}

func (p *TxPool) nonceConsumed(txn *TxnSlot, view kvcache.CacheView) (bool, error) {
	if !txn.hasKeyedNonce() {
		nonce, _, _, err := p.senders.info(view, txn.SenderID)
		return nonce > txn.Nonce, err
	}
	frame := txn.Txn.(*types.FrameTransaction)
	var storageKey [length.Addr + length.Hash]byte
	copy(storageKey[:length.Addr], types.FrameNonceManager[:])
	for _, key := range frame.NonceKeys {
		slot := types.FrameNonceSlot(frame.SenderAddress, key)
		copy(storageKey[length.Addr:], slot[:])
		value, err := view.Get(storageKey[:])
		if err != nil {
			return false, err
		}
		var sequence uint256.Int
		sequence.SetBytes(value)
		if sequence.GtUint64(txn.Nonce) {
			return true, nil
		}
	}
	return false, nil
}

// Record only successful head advances, so retries and subscription resets do not
// become block-time samples. Incomplete batches restart the clock after catch-up.
func (p *TxPool) recordBlockTime(incomplete bool) {
	nowMs := time.Now().UnixMilli()
	if prev := p.lastBlockTimestampMs.Load(); prev > 0 && !incomplete {
		// Ignore pauses over ten minutes; the EWMA (alpha=0.1) tracks arrivals at the tip.
		if delta := nowMs - prev; delta > 0 && delta < 600_000 {
			p.avgBlockTimeMs.Store((9*p.avgBlockTimeMs.Load() + delta) / 10)
		}
	}
	p.lastBlockTimestampMs.Store(nowMs)
}

// Publish at block cadence, including parked frames as queued, as in RPC.
// Caller must hold p.lock.
func (p *TxPool) publishDepthMetrics() {
	pending, baseFee, queued := p.countContent()
	pendingSubCounter.SetInt(pending)
	basefeeSubCounter.SetInt(baseFee)
	queuedSubCounter.SetInt(queued)
}

// forgetUnusedSenders drops the id of every sender in the batch that has no
// txn left in p.all. Validation rejects a txn before it becomes a metaTxn, so
// it never enters p.deletedTxns and the flush-time eviction never sees it.
// Callers must hold p.lock.
func (p *TxPool) forgetUnusedSenders(ids []uint64) {
	for _, id := range ids {
		if p.all.hasTxns(id) {
			continue
		}
		delete(p.senderLastActivity, id)
		if addr, ok := p.senders.senderID2Addr[id]; ok {
			delete(p.senders.senderID2Addr, id)
			delete(p.senders.senderIDs, addr)
		}
	}
}

// senderIDsOf snapshots the batch's sender ids, which forgetUnusedSenders needs
// after the batch itself has been reset.
func senderIDsOf(txns *TxnSlots) []uint64 {
	ids := make([]uint64, len(txns.Txns))
	for i, txn := range txns.Txns {
		ids[i] = txn.SenderID
	}
	return ids
}

func (p *TxPool) processRemoteTxns(ctx context.Context) (err error) {
	if !p.Started() {
		return errors.New("txpool not started yet")
	}
	if !p.hasUnprocessedRemoteTxns.Load() {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, stack.Trace().String())
		}
	}()

	defer processBatchTxnsTimer.ObserveDuration(time.Now())
	_, err = p.processRemoteBatch(ctx)
	return err
}

func (p *TxPool) removeProcessedRemoteTxns(n int) {
	if n == len(p.unprocessedRemoteTxns.Txns) {
		p.unprocessedRemoteByHash = make(map[string]*TxnSlot)
		p.unprocessedRemoteBytes = 0
	} else {
		for _, txn := range p.unprocessedRemoteTxns.Txns[:n] {
			delete(p.unprocessedRemoteByHash, string(txn.IDHash[:]))
			p.unprocessedRemoteBytes -= uint64(txn.Size)
		}
	}
	p.unprocessedRemoteTxns.Txns = slices.Delete(p.unprocessedRemoteTxns.Txns, 0, n)
	p.unprocessedRemoteTxns.Senders = slices.Delete(p.unprocessedRemoteTxns.Senders, 0, n*length.Addr)
	p.unprocessedRemoteTxns.IsLocal = slices.Delete(p.unprocessedRemoteTxns.IsLocal, 0, n)
	p.unprocessedRemotePeers = slices.Delete(p.unprocessedRemotePeers, 0, n)
	p.hasUnprocessedRemoteTxns.Store(len(p.unprocessedRemoteTxns.Txns) > 0)
}

// kickKZGOffenders drops the devp2p peer that delivered each KZG-failed blob txn.
// Called with p.lock held; the actual PenalizePeer RPC is dispatched off-lock to
// avoid blocking the pool on sentry I/O.
func (p *TxPool) kickKZGOffenders(ctx context.Context, reasons []txpoolcfg.DiscardReason, sources []remoteSource) {
	var offenders []remoteSource
	for i, src := range sources {
		if reasons[i] != txpoolcfg.UnmatchedBlobTxExt || src.peerID == nil || src.sentry == nil {
			continue
		}
		offenders = append(offenders, src)
	}
	if len(offenders) == 0 {
		return
	}
	go func() {
		for _, src := range offenders {
			p.logger.Debug("[txpool] penalizing peer for KZG-fail blob txn", "peer", src.peerID)
			if _, err := src.sentry.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{
				PeerId:  src.peerID,
				Penalty: sentryproto.PenaltyKind_Kick,
			}); err != nil {
				p.logger.Debug("[txpool] PenalizePeer failed", "err", err)
			}
		}
	}()
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
	return bytes.Clone(rlpTx), err
}

func (p *TxPool) AppendAllAnnouncements(types []byte, sizes []uint32, hashes []byte) ([]byte, []uint32, []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for hash, txn := range p.byHash {
		types = append(types, txn.TxnSlot.TxType())
		sizes = append(sizes, txn.TxnSlot.Size)
		hashes = append(hashes, hash...)
	}
	for hash, txnSlot := range p.unprocessedRemoteByHash {
		if txnSlot.TxType() == FrameTxnType {
			continue
		}
		types = append(types, txnSlot.TxType())
		sizes = append(sizes, txnSlot.Size)
		hashes = append(hashes, hash...)
	}
	return types, sizes, hashes
}

func (p *TxPool) idHashKnown(tx kv.Tx, hash []byte, hashS string) (bool, error) {
	if _, ok := p.remoteFrames.pending[hashS]; ok {
		return true, nil
	}
	if _, ok := p.unprocessedRemoteByHash[hashS]; ok {
		return true, nil
	}
	if _, ok := p.byHash[hashS]; ok {
		return true, nil
	}
	if _, ok := p.minedBlobTxnsByHash[hashS]; ok {
		return true, nil
	}
	if rejection, ok := p.frameRejections.Get(hashS); ok {
		// Expired rejections permit a retry even if the discarded transaction's
		// persisted pool row has not been removed yet.
		return rejection.applies(p, p.stateGeneration), nil
	}
	if reason, ok := p.discardReasonsLRU.Get(hashS); ok {
		return !refetchableDiscard(reason), nil
	}
	return tx.Has(kv.PoolTransaction, hash)
}

func (p *TxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	hashS := string(hash)
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.idHashKnown(tx, hash, hashS)
}

// KnownTxnReason returns AlreadyKnown only for transactions currently in the pool.
// Otherwise it returns a cached rejection that still applies,
// or NotSet if admission is needed.
func (p *TxPool) KnownTxnReason(hash []byte) txpoolcfg.DiscardReason {
	p.lock.Lock()
	defer p.lock.Unlock()
	hashS := string(hash)
	if _, ok := p.byHash[hashS]; ok {
		return txpoolcfg.AlreadyKnown
	}
	if reason, ok := p.discardReasonsLRU.Get(hashS); ok && reason == txpoolcfg.InvalidAuthorizationNonce {
		return reason
	}
	// Sender-policy rejections still need admission's current account-nonce check.
	if rejection, ok := p.frameRejections.Get(hashS); ok && rejection.blocker == (common.Hash{}) && rejection.applies(p, p.stateGeneration) {
		return rejection.reason
	}
	return txpoolcfg.NotSet
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

// refetchableDiscard reports a discard another body can undo: a retryable failure (a nonce gap, frame validation) can
// clear before the discarded pool row is removed, and a blob txn's hash does not cover its sidecar, so one peer's bad
// sidecar says nothing about the txn.
func refetchableDiscard(reason txpoolcfg.DiscardReason) bool {
	switch reason {
	case txpoolcfg.NonceTooDistant, txpoolcfg.BlobNonceGap, txpoolcfg.FrameValidationUnavailable, txpoolcfg.FrameStateChanged,
		txpoolcfg.UnequalBlobTxExt, txpoolcfg.BlobHashCheckFail, txpoolcfg.UnmatchedBlobTxExt:
		return true
	}
	return false
}

// HeldOrQueuedIdHashes splits hashes into the ones the pool holds and the ones queued for its validation.
// A discarded txn that a refetch would drop as known counts as held; one a refetch can fix is neither.
func (p *TxPool) HeldOrQueuedIdHashes(tx kv.Tx, hashes Hashes) (held, queued Hashes, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := 0; i < len(hashes); i += 32 {
		hash, hashS := hashes[i:i+32], string(hashes[i:i+32])
		_, inQueue := p.unprocessedRemoteByHash[hashS]
		_, simulating := p.remoteFrames.pending[hashS] // a frame leaves the queue before its simulation ends
		if inQueue || simulating {
			queued = append(queued, hash...)
			continue
		}
		known, err := p.idHashKnown(tx, hash, hashS)
		if err != nil {
			return held, queued, err
		}
		if known {
			held = append(held, hash...)
		}
	}
	return held, queued, nil
}

func (p *TxPool) getCachedBlobTxnLocked(tx kv.Tx, hash []byte) (*metaTxn, error) {
	if mt, ok := p.minedBlobTxnsByHash[string(hash)]; ok {
		return mt, nil
	}
	if txn, ok := p.unprocessedRemoteByHash[string(hash)]; ok {
		return newMetaTxn(txn, false, 0), nil
	}
	if mt, ok := p.byHash[string(hash)]; ok {
		return mt, nil
	}
	v, err := tx.GetOne(kv.PoolTransaction, hash)
	if err != nil {
		return nil, fmt.Errorf("TxPool.getCachedBlobTxnLocked: Get: %d, %w", len(hash), err)
	}
	if len(v) == 0 {
		return nil, nil
	}
	if len(v) < 20 {
		p.logger.Warn("[txpool] getCachedBlobTxnLocked: truncated row", "hash", hex.EncodeToString(hash), "len", len(v))
		return nil, nil
	}
	txnRlp := bytes.Clone(v[20:])
	parseCtx := NewTxnParseContext(p.chainID)
	parseCtx.WithSender(false)
	txnSlot := &TxnSlot{}
	if _, err := parseCtx.ParseTransaction(txnRlp, 0, txnSlot, nil, false, true, nil); err != nil {
		p.logger.Warn("[txpool] getCachedBlobTxnLocked: parseTransaction", "hash", hex.EncodeToString(hash), "err", err)
		return nil, nil
	}
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
func (p *TxPool) best(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64,
	parentBlockWaitStop <-chan struct{},
	availableGas mdgas.FullMdGas,
	yielded mapset.Set[[32]byte], availableRlpSpace int,
) (bool, int, error) {
	// sync.Cond has no notion of a context, so a caller that goes away while parked below would
	// sleep until the next block broadcast the condition, or forever while the chain is stalled.
	// Broadcasting on cancellation or on a stopped wait wakes it; every waiter rechecks its own
	// condition anyway. The broadcast takes the lock so it cannot land between the check below and
	// the wait, which is the window where a wakeup would be lost.
	watchParentBlockWaitStop := parentBlockWaitStop
	// An already closed stop needs no watcher; its broadcast and join would contend on every later call.
	select {
	case <-watchParentBlockWaitStop:
		watchParentBlockWaitStop = nil
	default:
	}
	stopWatching, watcherDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
		case <-watchParentBlockWaitStop:
		case <-stopWatching:
			return
		}
		p.lock.Lock()
		p.lastSeenCond.Broadcast()
		p.lock.Unlock()
	}()
	// Joined rather than merely told to stop: with the context already ended the watcher can pick
	// either case, and one that picked cancellation would otherwise take the lock after this call
	// had returned. Registered before the lock is taken, so it runs after the lock is released.
	defer func() {
		close(stopWatching)
		<-watcherDone
	}()

	p.lock.Lock()
	for last := p.lastSeenBlock.Load(); last < onTopOf; last = p.lastSeenBlock.Load() {
		select {
		case <-ctx.Done():
			// Leaving with the lock held would stop every later pool operation, not just this one.
			p.lock.Unlock()
			return false, 0, ctx.Err()
		case <-parentBlockWaitStop:
			p.lock.Unlock()
			p.logger.Debug("[txpool] Parent block wait stopped", "expecting", onTopOf, "lastSeen", last)
			return false, 0, ctx.Err()
		default:
			// continue
		}
		p.logger.Trace("[txpool] Waiting for block", "expecting", onTopOf, "lastSeen", last, "txRequested", n, "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
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

	if p.stateViewStale {
		return false, 0, nil
	}

	best := p.pending.best

	isEIP3860 := p.isShanghai()
	isEIP7623 := p.isPrague()
	isAmsterdam := p.isAmsterdam()
	minTxGas := mdgas.MinTxGas(isAmsterdam)

	txns.Resize(uint(min(n, len(best.ms))))
	var toDiscard, toDemote []*metaTxn
	pendingBaseFee, pendingBlobFee := p.pendingBaseFee.Load(), p.pendingBlobFee.Load()
	count := 0
	i := 0

	defer func() {
		p.logger.Debug("[txpool] Processing best request", "last", onTopOf, "txRequested", n, "txAvailable", len(best.ms), "txProcessed", i, "txReturned", count)
	}()

	for ; count < n && i < len(best.ms); i++ {
		// if we wouldn't have enough gas for a standard transaction then quit out early
		if availableGas.Execution < minTxGas {
			break
		}
		if availableRlpSpace <= 0 {
			break
		}

		mt := best.ms[i]

		if yielded != nil && yielded.Contains(mt.TxnSlot.IDHash) {
			continue
		}
		// Pending is ordered without the blob fee, so promotion only demotes a blob txn the blob fee has risen
		// past once it is the worst one.
		if !coversPendingFees(mt, pendingBaseFee, pendingBlobFee) {
			toDemote = append(toDemote, mt)
			continue
		}

		if mt.TxnSlot.TxType() != FrameTxnType && mt.TxnSlot.GetGas() > p.blockGasLimit.Load() {
			// Skip transactions with very large gas limit
			continue
		}
		var limits mdgas.MdGas
		var intrinsicGasResult mdgas.IntrinsicGasCalcResult
		var err error
		frame, isFrame := mt.TxnSlot.Txn.(*types.FrameTransaction)
		if isFrame {
			intrinsicGasResult.ExecutionGas, intrinsicGasResult.FloorGasCost, limits, err = frame.GasLimits()
		} else {
			limits, err = protocol.TransactionGasLimits(mt.TxnSlot.Txn, isAmsterdam)
		}
		if err != nil {
			continue
		}
		executionGas, stateGas := limits.Execution, limits.State
		if executionGas > availableGas.Execution {
			continue
		}
		if stateGas > availableGas.State {
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
			// The snapshot was taken before the pool lock was retaken; a flush in between moved the RLP to a row it does
			// not see. The next call reads it.
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
		if !isFrame {
			authorizationLen := uint64(len(mt.TxnSlot.Txn.GetAuthorizations()))
			to := mt.TxnSlot.Txn.GetTo()
			intrinsicGasResult, _ = mdgas.CalcIntrinsicGas(mdgas.IntrinsicGasCalcArgs{
				Data:               make([]byte, mt.TxnSlot.GetDataLen()),
				DataNonZeroLen:     uint64(mt.TxnSlot.GetDataNonZeroLen()),
				AuthorizationsLen:  authorizationLen,
				AccessListLen:      uint64(mt.TxnSlot.GetAccessListAddrCount()),
				StorageKeysLen:     uint64(mt.TxnSlot.GetAccessListStorCount()),
				IsContractCreation: mt.TxnSlot.IsCreation(),
				IsSelfTransfer:     to != nil && *to == sender,
				HasValue:           !mt.TxnSlot.GetValue().IsZero(),
				IsEIP2:             true,
				IsEIP2028:          true,
				IsEIP3860:          isEIP3860,
				IsEIP7623:          isEIP7623,
				IsEIP7976:          isAmsterdam,
				IsEIP7981:          isAmsterdam,
				IsEIP2780:          isAmsterdam,
			})
		}
		intrinsicGas := intrinsicGasResult.ExecutionGas
		if isEIP7623 && intrinsicGasResult.FloorGasCost > intrinsicGas {
			intrinsicGas = intrinsicGasResult.FloorGasCost
		}
		if intrinsicGas > availableGas.Execution {
			// we might find another txn with a low enough intrinsic gas to include so carry on
			continue
		}
		availableGas.Execution -= intrinsicGas
		availableGas.Blob -= blobCount * params.GasPerBlob
		availableRlpSpace -= len(rlpTxn)
		txns.Txns[count] = rlpTxn
		// For type-3 and frame transactions with blobs, Txn holds only the inner
		// transaction. Leave ParsedTxn nil so the provider decodes the full wrapper
		// and preserves its sidecar.
		if !mt.TxnSlot.HasBlobs() {
			txns.ParsedTxn[count] = mt.TxnSlot.Txn
		}
		copy(txns.Senders.At(count), sender[:])
		txns.IsLocal[count] = isLocal
		if yielded != nil {
			yielded.Add(mt.TxnSlot.IDHash)
		}
		count++
	}

	txns.Resize(uint(count))
	for _, mt := range toDemote {
		p.pending.Remove(mt, "best", p.logger)
		p.baseFee.Add(mt, "demote-best", p.logger)
	}
	for _, mt := range toDiscard {
		p.pending.Remove(mt, "best", p.logger)
		p.discardLocked(mt, txpoolcfg.UnmatchedBlobTxExt)
	}
	if len(toDemote)+len(toDiscard) > 0 {
		p.pending.EnforceBestInvariants() // Remove swaps the last txn into the removed one's place
	}

	return true, count, nil
}

func (p *TxPool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if !p.Started() {
		return nil, ctx.Err()
	}

	provideOptions := txnprovider.ApplyProvideOptions(opts...)
	var txnsRlp TxnsRlp
	_, _, err := p.best(
		ctx,
		provideOptions.Amount,
		&txnsRlp,
		provideOptions.ParentBlockNum,
		provideOptions.ParentBlockWaitStop,
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
	onTime, _, err := p.best(ctx, n, txns, onTopOf, nil,
		mdgas.NewFullMdGas(math.MaxUint64, math.MaxUint64, math.MaxUint64), set, math.MaxInt)
	return onTime, err
}

func (p *TxPool) CountContent() (int, int, int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.countContent()
}

func (p *TxPool) countContent() (int, int, int) {
	pending, baseFee := p.pending.Len(), p.baseFee.Len()
	// Every indexed transaction outside pending and base-fee is queued or parked.
	// RPC reports both as queued.
	queued := len(p.byHash) - pending - baseFee
	return pending, baseFee, queued
}

func (p *TxPool) AddRemoteTxns(_ context.Context, newTxns TxnSlots, peerID PeerID, sentry sentryproto.SentryClient) {
	if p.cfg.NoGossip {
		// if no gossip, then
		// disable adding remote transactions
		// consume remote txn from fetch
		return
	}

	defer addRemoteTxnsTimer.ObserveDuration(time.Now())
	p.lock.Lock()
	defer p.lock.Unlock()
	src := remoteSource{peerID: peerID, sentry: sentry}
	now := time.Now()
	for i, txn := range newTxns.Txns {
		if len(p.unprocessedRemoteTxns.Txns) >= remoteTxnQueueLimit {
			break
		}
		hashS := string(txn.IDHash[:])
		if _, ok := p.remoteFrames.pending[hashS]; ok {
			continue
		}
		_, ok := p.unprocessedRemoteByHash[hashS]
		if ok {
			continue
		}
		if uint64(txn.Size) > remoteTxnQueueByteLimit-p.unprocessedRemoteBytes {
			continue
		}
		if txn.TxType() == FrameTxnType && !p.remoteFrames.reserve(txn, src, now) {
			continue
		}
		// Blob bundles already reference an owned copy. Other RLP slices may
		// retain an entire network packet outside the queue's byte budget.
		if len(txn.BlobBundles) == 0 {
			txn.Rlp = bytes.Clone(txn.Rlp)
		}
		p.unprocessedRemoteByHash[hashS] = txn
		p.unprocessedRemoteBytes += uint64(txn.Size)
		p.unprocessedRemoteTxns.Append(txn, newTxns.Senders.At(i), false)
		p.unprocessedRemotePeers = append(p.unprocessedRemotePeers, src)
	}
	p.hasUnprocessedRemoteTxns.Store(len(p.unprocessedRemoteTxns.Txns) > 0)
}

func toBlobs(_blobs [][]byte) []*goethkzg.Blob {
	blobs := make([]*goethkzg.Blob, len(_blobs))
	for i, _blob := range _blobs {
		blobs[i] = (*goethkzg.Blob)(_blob)
	}
	return blobs
}

func (p *TxPool) validateTx(txn *TxnSlot, isLocal bool, stateCache kvcache.CacheView) (txpoolcfg.DiscardReason, error) {
	return p.validateTxWithCells(txn, isLocal, stateCache, nil)
}

func (p *TxPool) validateTxWithCells(txn *TxnSlot, isLocal bool, stateCache kvcache.CacheView, cells blobCells) (txpoolcfg.DiscardReason, error) {
	isEIP3860 := p.isShanghai()
	isPrague := p.isPrague()
	isAmsterdam := p.isAmsterdam()
	isEIP7954 := isAmsterdam
	if txn.IsCreation() {
		if err := vm.CheckMaxInitCodeSize(uint64(txn.GetDataLen()), isEIP3860, isEIP7954); err != nil {
			return txpoolcfg.InitCodeTooLarge, nil
		}
	}

	if txn.TxType() == types.FrameTxType {
		// Frame policy was checked before simulation; the batch's state generation
		// is verified before reaching this stage.
		if txn.frameValidation == nil {
			return txpoolcfg.InvalidFrame, nil
		}
		if txn.HasBlobs() {
			return p.validateBlobTxnAndSidecar(txn, isLocal, cells), nil
		}
		return txpoolcfg.Success, nil
	}

	// Use the on-tx authorization list length, not len(AuthAndNonces): per
	// EIP-7702 the sender pays for every auth tuple regardless of validity,
	// and AuthAndNonces only holds successfully-recovered entries.
	authorizationLen := len(txn.Txn.GetAuthorizations())
	if txn.TxType() == SetCodeTxnType {
		if !isPrague {
			return txpoolcfg.TypeNotActivated, nil
		}
		if txn.IsCreation() {
			return txpoolcfg.InvalidCreateTxn, nil
		}
		if authorizationLen == 0 {
			return txpoolcfg.NoAuthorizations, nil
		}
	}

	// A no-op for legacy and access-list transactions: GetFeeCap and GetTipCap both return the gas price there
	if txn.GetFeeCap().Lt(txn.GetTipCap()) {
		return txpoolcfg.TipAboveFeeCap, nil
	}

	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !isLocal && uint256.NewInt(p.cfg.MinFeeCap).Cmp(txn.GetFeeCap()) == 1 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx underpriced idHash=%x local=%t, feeCap=%d, cfg.MinFeeCap=%d", txn.IDHash, isLocal, txn.GetFeeCap(), p.cfg.MinFeeCap))
		}
		return txpoolcfg.UnderPriced, nil
	}

	txnSender, senderOk := txn.Txn.GetSender()
	to := txn.Txn.GetTo()
	intrinsicGasResult, overflow := mdgas.CalcIntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               make([]byte, txn.GetDataLen()),
		DataNonZeroLen:     uint64(txn.GetDataNonZeroLen()),
		AuthorizationsLen:  uint64(authorizationLen),
		AccessListLen:      uint64(txn.GetAccessListAddrCount()),
		StorageKeysLen:     uint64(txn.GetAccessListStorCount()),
		IsContractCreation: txn.IsCreation(),
		IsSelfTransfer:     senderOk && to != nil && txnSender.Value() == *to,
		HasValue:           !txn.GetValue().IsZero(),
		IsEIP2:             true,
		IsEIP2028:          true,
		IsEIP3860:          isEIP3860,
		IsEIP7623:          isPrague,
		IsEIP7976:          isAmsterdam,
		IsEIP7981:          isAmsterdam,
		IsEIP2780:          isAmsterdam,
	})
	gas := intrinsicGasResult.ExecutionGas
	if isPrague && intrinsicGasResult.FloorGasCost > gas {
		gas = intrinsicGasResult.FloorGasCost
	}

	if txn.Traced {
		p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas idHash=%x gas=%d", txn.IDHash, gas))
	}
	if overflow {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas calculated failed due to overflow idHash=%x", txn.IDHash))
		}
		return txpoolcfg.GasUintOverflow, nil
	}
	if gas > txn.GetGas() {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx intrinsic gas > txn.gas idHash=%x gas=%d, txn.gas=%d", txn.IDHash, gas, txn.GetGas()))
		}
		return txpoolcfg.IntrinsicGas, nil
	}
	if txn.GetGas() > params.MaxTxnTotalGasLimit {
		return txpoolcfg.GasLimitTooHigh, nil
	}
	if txn.GetGas() > p.blockGasLimit.Load() {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx txn.gas > block gas limit idHash=%x gas=%d, block gas limit=%d", txn.IDHash, txn.GetGas(), p.blockGasLimit.Load()))
		}
		return txpoolcfg.GasLimitTooHigh, nil
	}
	// EIP-7825: Transaction Gas Limit Cap.
	// EIP-8037 (Amsterdam): TX_MAX_GAS_LIMIT applies to the execution gas dimension only.
	// Pre-Amsterdam: cap = full tx gas limit.
	var gasToCap uint64
	if isAmsterdam {
		gasToCap = max(intrinsicGasResult.ExecutionGas, intrinsicGasResult.FloorGasCost)
	} else {
		gasToCap = txn.GetGas()
	}
	if gasToCap > params.MaxTxnGasLimit {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx gas cap exceeded idHash=%x gasToCap=%d, max=%d", txn.IDHash, gasToCap, params.MaxTxnGasLimit))
		}
		return txpoolcfg.GasLimitTooHigh, nil
	}

	if !isLocal && uint64(p.all.count(txn.SenderID)) > p.cfg.AccountSlots {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx marked as spamming idHash=%x slots=%d, limit=%d", txn.IDHash, p.all.count(txn.SenderID), p.cfg.AccountSlots))
		}
		return txpoolcfg.Spammer, nil
	}

	// Check nonce and balance
	senderNonce, senderBalance, senderCodeHash, err := p.senders.info(stateCache, txn.SenderID)
	if err != nil {
		return txpoolcfg.ErrGetSenderInfo, fmt.Errorf("validateTx: sender info for idHash=%x senderID=%d: %w", txn.IDHash, txn.SenderID, err)
	}
	if senderNonce > txn.Nonce {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx nonce too low idHash=%x nonce in state=%d, txn.nonce=%d", txn.IDHash, senderNonce, txn.Nonce))
		}
		return txpoolcfg.NonceTooLow, nil
	}

	// Transactor should have enough funds to cover the costs
	total := requiredBalance(txn)
	if senderBalance.Cmp(total) < 0 {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx insufficient funds idHash=%x balance in state=%d, txn.gas*txn.tip=%d", txn.IDHash, senderBalance, total))
		}
		return txpoolcfg.InsufficientFunds, nil
	}
	if !senderCodeHash.IsEmpty() {
		if existing := p.all.get(txn.SenderID, txn.Nonce); existing == nil {
			if p.all.hasTxns(txn.SenderID) {
				return txpoolcfg.DelegatedTxnLimit, nil
			}
			if txn.Nonce != senderNonce {
				return txpoolcfg.DelegatedNonceGap, nil
			}
		}
	}
	if txn.HasBlobs() {
		return p.validateBlobTxn(txn, isLocal), nil
	}
	return txpoolcfg.Success, nil
}

// validateBlobTxnAndSidecar runs both blob checks, for callers that already hold p.lock.
func (p *TxPool) validateBlobTxnAndSidecar(txn *TxnSlot, isLocal bool, cells blobCells) txpoolcfg.DiscardReason {
	if reason := p.validateBlobTxn(txn, isLocal); reason != txpoolcfg.Success {
		return reason
	}
	return p.validateBlobSidecar(txn, cells)
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

	if !isLocal && (p.all.blobCount(txn.SenderID)+uint64(blobCount)) > p.cfg.BlobSlots {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx sender blob limit exceeded idHash=%x blobs=%d, limit=%d", txn.IDHash, p.all.blobCount(txn.SenderID)+uint64(blobCount), p.cfg.BlobSlots))
		}
		return txpoolcfg.BlobSlotsOverflow
	}
	if p.totalBlobsInPool.Load() >= p.cfg.TotalBlobPoolLimit {
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: validateTx total blobs limit reached in pool limit=%d current blobs=%d", p.cfg.TotalBlobPoolLimit, p.totalBlobsInPool.Load()))
		}
		return txpoolcfg.BlobPoolOverflow
	}

	return txpoolcfg.Success
}

// validateBlobSidecar checks the blob sidecar against the txn and adds its cells to cellsByHash.
// It reads no pool state, so it does not need p.lock.
func (p *TxPool) validateBlobSidecar(txn *TxnSlot, cellsByHash blobCells) txpoolcfg.DiscardReason {
	blobHashes := txn.GetBlobHashes()
	blobCount := len(blobHashes)
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

	for i := range commitments {
		if libkzg.KZGToVersionedHash(commitments[i]) != libkzg.VersionedHash(blobHashes[i]) {
			return txpoolcfg.BlobHashCheckFail
		}
	}

	if p.isOsaka() {
		cells, err := libkzg.VerifyCellProofBatchWithCells(blobs, commitments, proofs)
		if err != nil {
			return txpoolcfg.UnmatchedBlobTxExt
		}
		if p.isAmsterdam() && cellsByHash != nil {
			for i, h := range blobHashes {
				cellsByHash.add(h, cells[i])
			}
		}
	} else {
		// https://github.com/ethereum/consensus-specs/blob/017a8495f7671f5fff2075a9bfc9238c1a0982f8/specs/deneb/polynomial-commitments.md#verify_blob_kzg_proof_batch
		kzgCtx := libkzg.Ctx()
		err := kzgCtx.VerifyBlobKZGProofBatch(toBlobs(blobs), commitments, proofs)
		if err != nil {
			return txpoolcfg.UnmatchedBlobTxExt
		}
	}

	return txpoolcfg.Success
}

var maxUint256 = new(uint256.Int).SetAllOne()

// requiredBalance returns the sender funds needed for pool balance checks.
// Ordinary transactions need gasLimit*feeCap + blobGas*blobFeeCap + value.
// Frame transactions return zero: fees use separate payer validation and reservations,
// and value transfers are checked during execution.
func requiredBalance(txn *TxnSlot) *uint256.Int {
	if txn.TxType() == types.FrameTxType {
		return new(uint256.Int)
	}
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

func (p *TxPool) isBogota() bool {
	return isTimeBasedForkActivated(&p.isPostBogota, p.bogotaTime)
}

func (p *TxPool) GetMaxBlobsPerBlock() uint64 {
	now := time.Now().Unix()
	return p.chainConfig.GetMaxBlobsPerBlock(uint64(now))
}

// ValidateSerializedTxn checks that the serialized transaction does not exceed the size limit for its type.
func ValidateSerializedTxn(serializedTxn []byte) error {
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
	if len(serializedTxn) <= txnMaxSize {
		return nil
	}
	dataPos, dataLen, legacy, err := rlp.Prefix(serializedTxn, 0)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrParseTxn, err)
	}
	txnType := LegacyTxnType
	if !legacy {
		if dataLen == 0 {
			return fmt.Errorf("%w: empty transaction", ErrParseTxn)
		}
		txnType = serializedTxn[dataPos]
		if dataPos > 0 {
			serializedTxn = serializedTxn[dataPos : dataPos+dataLen]
		}
	}
	maxSize := txnMaxSize
	if txnType == BlobTxnType {
		maxSize = blobTxnMaxSize
	} else if txnType == FrameTxnType {
		bodyPos, bodyLen, err := rlp.ParseList(serializedTxn, 1)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrParseTxn, err)
		}
		if bodyLen > 0 && serializedTxn[bodyPos] >= 0xc0 {
			innerPos, innerLen, err := rlp.ParseList(serializedTxn, bodyPos)
			if err != nil {
				return fmt.Errorf("%w: %w", ErrParseTxn, err)
			}
			if 1+innerPos+innerLen-bodyPos <= txnMaxSize {
				maxSize = blobTxnMaxSize
			}
		}
	}
	if len(serializedTxn) > maxSize {
		return fmt.Errorf("%w: type=%d", ErrRlpTooBig, txnType)
	}
	return nil
}

func (p *TxPool) recoverAuthorizations(tx *TxnSlot) {
	if tx.TxType() != SetCodeTxnType || tx.AuthAndNonces != nil {
		return
	}
	auths := tx.Txn.GetAuthorizations()
	authorities := make([]AuthAndNonce, 0, len(auths))
	for i := range auths {
		auth := &auths[i]
		if !auth.ChainID.IsZero() && !auth.ChainID.Eq(&p.chainID) {
			continue
		}
		authority, err := auth.RecoverSigner()
		if err != nil {
			continue
		}
		authorities = append(authorities, AuthAndNonce{authority, auth.Nonce})
	}
	tx.AuthAndNonces = authorities
}

var (
	errStateViewStale = errors.New("pool state recovery pending")
	errPoolNotReady   = errors.New("transaction pool is not ready; retry later")
)

func (p *TxPool) withLockedState(ctx context.Context, txns *TxnSlots, waitForRecovery bool, f func(kv.TemporalTx, kvcache.CacheView) error) error {
	for {
		p.lock.Lock()
		coreDB, cache, generation := p._chainDB, p._stateCache, p.stateGeneration
		stale := p.stateViewStale
		p.lock.Unlock()
		if stale {
			if !waitForRecovery {
				return errStateViewStale
			}
			if !p.waitForStateChange(ctx, generation) {
				return ctx.Err()
			}
			continue
		}

		var retry bool
		// The cache may wait for a block update, so open the view outside the pool lock.
		err := coreDB.ViewTemporal(ctx, func(coreTx kv.TemporalTx) error {
			view, err := cache.View(ctx, coreTx)
			if err != nil {
				return err
			}
			p.lock.Lock()
			defer p.lock.Unlock()
			if generation != p.stateGeneration || p.stateViewStale {
				retry = true
				return nil
			}
			if txns != nil {
				if err := p.senders.registerNewSenders(txns, p.logger); err != nil {
					return err
				}
				defer p.forgetUnusedSenders(senderIDsOf(txns))
			}
			return f(coreTx, view)
		})
		if err != nil || !retry {
			return err
		}
	}
}

// Precheck SetCode and blob transactions under the pool lock, then recover
// authorization signers and verify blob sidecars without it. The caller must
// validate the surviving transactions again with a fresh state view before inserting them.
func (p *TxPool) prepareTxns(ctx context.Context, txns TxnSlots, sources []remoteSource, waitForRecovery bool) ([]txpoolcfg.DiscardReason, blobCells, error) {
	if err := txns.Valid(); err != nil {
		return nil, nil, err
	}
	var prechecked TxnSlots
	var indices []int
	for i, txn := range txns.Txns {
		if txn.TxType() == SetCodeTxnType || txn.TxType() == BlobTxnType {
			prechecked.Append(txn, txns.Senders.At(i), txns.IsLocal[i])
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		return nil, nil, nil
	}
	batchLen := len(txns.Txns)
	hasFrames := slices.ContainsFunc(txns.Txns, func(txn *TxnSlot) bool { return txn.TxType() == FrameTxnType })

	var precheckReasons []txpoolcfg.DiscardReason
	var goodTxns TxnSlots
	err := p.withLockedState(ctx, &prechecked, waitForRecovery, func(_ kv.TemporalTx, view kvcache.CacheView) error {
		var err error
		precheckReasons, goodTxns, _, err = p.validateTxns(&prechecked, view, nil, nil)
		if err != nil || p.cfg.MaxNonceGap == 0 || hasFrames {
			return err
		}
		for i, txn := range prechecked.Txns {
			if txn.TxType() != BlobTxnType || precheckReasons[i] != txpoolcfg.NotSet {
				continue
			}
			reason, found := p.discardReasonsLRU.Peek(string(txn.IDHash[:]))
			if !found || reason != txpoolcfg.NonceTooDistant || p.hasParkedLegacyFrame(txn.SenderID) {
				continue
			}
			nonce, _, _, err := p.senders.info(view, txn.SenderID)
			if err != nil {
				return err
			}
			if txn.Nonce <= nonce {
				continue
			}
			// Pool and batch counts overestimate how far predecessors can advance the nonce.
			predecessors := uint64(p.all.count(txn.SenderID)) + uint64(batchLen) - 1
			gap := txn.Nonce - nonce
			if gap > p.cfg.MaxNonceGap && gap-p.cfg.MaxNonceGap > predecessors {
				precheckReasons[i] = txpoolcfg.NonceTooDistant
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	reasons := make([]txpoolcfg.DiscardReason, len(txns.Txns))
	for i, index := range indices {
		reasons[index] = precheckReasons[i]
	}
	for _, txn := range goodTxns.Txns {
		if txn.TxType() == SetCodeTxnType {
			p.recoverAuthorizations(txn)
		}
	}
	cells := make(blobCells)
	offenders := kzgOffenders{sources: sources}
	for _, i := range indices {
		if txns.Txns[i].TxType() != BlobTxnType || reasons[i] != txpoolcfg.NotSet {
			continue
		}
		remote := !txns.IsLocal[i]
		if remote && offenders.skip(i) {
			continue
		}
		reasons[i] = p.validateBlobSidecar(txns.Txns[i], cells)
		if reasons[i] == txpoolcfg.Success {
			reasons[i] = txpoolcfg.NotSet
		} else if reasons[i] == txpoolcfg.UnmatchedBlobTxExt && remote {
			offenders.add(i)
		}
	}
	return reasons, cells, nil
}

// kzgOffenders tracks the remote sources of txns that failed KZG verification, so their later txns are skipped.
type kzgOffenders struct {
	sources   []remoteSource
	peers     [][64]byte
	allRemote bool // a failure without a peer ID skips every later remote txn
}

func (o *kzgOffenders) skip(i int) bool {
	if o.allRemote {
		return true
	}
	return i < len(o.sources) && o.sources[i].peerID != nil && slices.Contains(o.peers, o.sources[i].peerKey())
}

func (o *kzgOffenders) add(i int) {
	if i < len(o.sources) && o.sources[i].peerID != nil {
		o.peers = append(o.peers, o.sources[i].peerKey())
	} else {
		o.allRemote = true
	}
}

// validateTxns returns per-slot discard reasons and the txns that passed.
// A remote KZG failure skips later txns from that peer; with nil sources,
// it skips all later remote txns. Skipped txns keep NotSet and are not returned.
// Non-nil sources must be index-aligned with txns.
func (p *TxPool) validateTxns(txns *TxnSlots, stateCache kvcache.CacheView, reasons []txpoolcfg.DiscardReason, sources []remoteSource) (_ []txpoolcfg.DiscardReason, goodTxns TxnSlots, cells blobCells, err error) {
	// Keep precheck failures; NotSet entries still need validation.
	if reasons == nil {
		reasons = make([]txpoolcfg.DiscardReason, len(txns.Txns))
	}

	if err := txns.Valid(); err != nil {
		return reasons, goodTxns, cells, err
	}
	cells = make(blobCells)

	offenders := kzgOffenders{sources: sources}
	for i, txn := range txns.Txns {
		if reasons[i] == txpoolcfg.UnmatchedBlobTxExt && !txns.IsLocal[i] {
			offenders.add(i)
		}
		if reasons[i] != txpoolcfg.NotSet || (!txns.IsLocal[i] && offenders.skip(i)) {
			continue
		}
		reason, err := p.validateTxWithCells(txn, txns.IsLocal[i], stateCache, cells)
		if err != nil {
			if reason == txpoolcfg.ErrGetSenderInfo {
				p.logger.Warn("[txpool] validateTxns: sender info", "err", err)
				reasons[i] = reason
				continue
			}
			return reasons, goodTxns, cells, err
		}
		if reason == txpoolcfg.Success {
			goodTxns.Append(txn, txns.Senders.At(i), txns.IsLocal[i])
			// Success here means no DiscardReason yet, so leave it NotSet
			continue
		}
		if reason == txpoolcfg.Spammer {
			p.punishSpammer(txn.SenderID)
		}
		reasons[i] = reason
		if reason == txpoolcfg.UnmatchedBlobTxExt && !txns.IsLocal[i] { // a frame txn's sidecar is verified here
			offenders.add(i)
		}
	}
	return reasons, goodTxns, cells, nil
}

// punishSpammer drops high-nonce transactions without breaking local nonce chains.
func (p *TxPool) punishSpammer(spammer uint64) {
	count := p.all.count(spammer) / 2
	if count > 0 {
		txnsToDelete := make([]*metaTxn, 0, count)
		p.all.descend(spammer, func(mt *metaTxn) bool {
			if mt.subPool&IsLocal != 0 {
				return false
			}
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

const localAdmissionTimeout = 30 * time.Second

func (p *TxPool) AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	ctx, cancel := context.WithTimeout(ctx, localAdmissionTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The first state batch can arrive before persisted transactions are restored.
	// Admission must see those transactions for nonce and replacement checks.
	if !p.Started() {
		return nil, errPoolNotReady
	}
	reasons, err := p.addNewTxns(ctx, newTxns, nil, false)
	if errors.Is(err, errStateViewStale) {
		return nil, errPoolNotReady
	}
	return reasons, err
}

func (p *TxPool) addNewTxns(ctx context.Context, newTxns TxnSlots, sources []remoteSource, waitForRecovery bool) ([]txpoolcfg.DiscardReason, error) {
	for {
		reasons, err := p.tryAddNewTxns(ctx, newTxns, sources, waitForRecovery)
		if !waitForRecovery || !errors.Is(err, errStateViewStale) {
			return reasons, err
		}
		if err := p.waitForStateRecovery(ctx); err != nil {
			return nil, err
		}
	}
}

func (p *TxPool) waitForStateRecovery(ctx context.Context) error {
	if p.waitForCondition(ctx, func() bool { return !p.stateViewStale }) {
		return nil
	}
	return ctx.Err()
}

// The condition is evaluated with the pool lock held.
func (p *TxPool) waitForCondition(ctx context.Context, ready func() bool) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	// Serialize cancellation with Wait so its wakeup cannot be lost.
	stop := context.AfterFunc(ctx, func() {
		p.lock.Lock()
		p.lastSeenCond.Broadcast()
		p.lock.Unlock()
	})
	defer stop()
	for !ready() && ctx.Err() == nil {
		p.lastSeenCond.Wait()
	}
	return ready()
}

// Head-change retries share one simulation deadline. Frame admission returns
// errStateViewStale so recovery can wait outside that budget and restart admission.
func (p *TxPool) tryAddNewTxns(ctx context.Context, newTxns TxnSlots, sources []remoteSource, waitForRecovery bool) ([]txpoolcfg.DiscardReason, error) {
	reasons, preparedCells, err := p.prepareTxns(ctx, newTxns, sources, waitForRecovery)
	if err != nil {
		return nil, err
	}
	precheckReasons := reasons
	hasFrames := slices.ContainsFunc(newTxns.Txns, func(txn *TxnSlot) bool { return txn.TxType() == FrameTxnType })
	simulationCtx := ctx
	if hasFrames {
		var cancel context.CancelFunc
		simulationCtx, cancel = context.WithTimeout(ctx, frameValidationTimeout)
		defer cancel()
	}
	for {
		var frames frameValidationBatch
		if hasFrames {
			p.lock.Lock()
			stale := p.stateViewStale
			p.lock.Unlock()
			if stale {
				return nil, errStateViewStale
			}
			frames = p.prepareFrames(simulationCtx, newTxns)
		}
		err = p.withLockedState(ctx, &newTxns, waitForRecovery && !hasFrames, func(_ kv.TemporalTx, cacheView kvcache.CacheView) error {
			preparedReasons, err := p.applyFrameValidation(simulationCtx, newTxns, frames, precheckReasons, cacheView)
			if err != nil {
				return err
			}
			reasons = preparedReasons
			validatedReasons, goodTxns, cells, err := p.validateTxns(&newTxns, cacheView, reasons, sources)
			if err != nil {
				return err
			}
			maps.Copy(cells, preparedCells)
			reasons = validatedReasons
			p.kickKZGOffenders(ctx, reasons, sources)

			announcements, addReasons, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, goodTxns, cells,
				p.pendingBaseFee.Load(), p.pendingBlobFee.Load(), true)
			if err != nil {
				return err
			}
			// reasons is indexed by newTxns; addReasons is indexed by goodTxns.
			// Walk reasons and advance j only on slots that survived validation.
			for i, j := 0, 0; i < len(reasons) && j < len(addReasons); i++ {
				if reasons[i] != txpoolcfg.NotSet || newTxns.Txns[i] != goodTxns.Txns[j] {
					continue
				}
				reasons[i] = addReasons[j]
				j++
			}
			reasons = fillDiscardReasons(reasons, newTxns, p.discardReasonsLRU)
			p.notifyPendingTxns(announcements)
			return nil
		})
		if !errors.Is(err, errFrameStateChanged) {
			return reasons, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (p *TxPool) chainDB() (kv.TemporalRoDB, kvcache.Cache) {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p._chainDB, p._stateCache
}

func (p *TxPool) addTxns(blockNum uint64, cacheView kvcache.CacheView,
	newTxns TxnSlots, cells blobCells, pendingBaseFee, pendingBlobFee uint64, collect bool,
) (Announcements, []txpoolcfg.DiscardReason, error) {
	if dbg.AssertEnabled {
		for _, txn := range newTxns.Txns {
			if txn.SenderID == 0 {
				panic("senderID can't be zero")
			}
		}
	}

	// Refresh senders after inserting the batch so their nonce-chain and balance
	// checks account for all accepted transactions in the batch.
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

		if reason := p.addLocked(mt, &announcements, cells); reason != txpoolcfg.NotSet {
			discardReasons[i] = reason
			continue
		}
		if txn.Traced {
			p.logger.Info(fmt.Sprintf("TX TRACING: schedule sendersWithChangedState idHash=%x senderId=%d", txn.IDHash, mt.TxnSlot.SenderID))
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
		if err := p.refreshSender(cacheView, senderID); err != nil {
			return announcements, discardReasons, err
		}
	}

	if err := p.promote(cacheView, pendingBaseFee, pendingBlobFee, &announcements); err != nil {
		return announcements, discardReasons, err
	}
	return announcements, discardReasons, nil
}

// TODO: Looks like a copy of the above
func (p *TxPool) addTxnsOnNewBlock(blockNum uint64, cacheView kvcache.CacheView, stateChanges *remoteproto.StateChangeBatch,
	newTxns TxnSlots, cells blobCells,
) (Announcements, error) {
	if dbg.AssertEnabled {
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
	sendersWithChangedState := map[uint64]struct{}{}
	announcements := Announcements{}
	for i, txn := range newTxns.Txns {
		if _, ok := p.byHash[string(txn.IDHash[:])]; ok {
			continue
		}
		mt := newMetaTxn(txn, newTxns.IsLocal[i], blockNum)
		if reason := p.addLocked(mt, &announcements, cells); reason != txpoolcfg.NotSet {
			p.deletedTxns = append(p.deletedTxns, mt)
			continue
		}
		sendersWithChangedState[mt.TxnSlot.SenderID] = struct{}{}
	}
	// add senders changed in state to `sendersWithChangedState` list
	for _, changesList := range stateChanges.ChangeBatch {
		for _, change := range changesList.Changes {
			switch change.Action {
			case remoteproto.Action_UPSERT, remoteproto.Action_UPSERT_CODE, remoteproto.Action_REMOVE:
				if change.Incarnation > 0 {
					continue
				}
				addr := gointerfaces.ConvertH160toAddress(change.Address)
				id, ok := p.senders.getID(addr)
				if !ok {
					continue
				}
				sendersWithChangedState[id] = struct{}{}
			}
		}
	}

	// Senders with queued txns that didn't appear in stateChanges still need
	// re-evaluation: a tx misclassified into queued at AddLocalTxns time —
	// e.g. because senders.info returned a stale sender nonce due to the
	// pre-commit dispatch race introduced in #20195 — would otherwise stay
	// queued forever, since the only re-evaluation trigger is the sender
	// appearing in a block's stateChanges, which doesn't happen if the
	// queued tx is the sender's only outstanding tx.
	sendersToRefresh := map[uint64]struct{}{}
	if stateChanges.StateChangesIncomplete {
		// Missing account changes can leave mined transactions spending a frame payer's balance.
		for senderID := range p.all.senderIDTxnCount {
			sendersToRefresh[senderID] = struct{}{}
		}
	} else {
		for _, mt := range p.queued.best.ms {
			sendersToRefresh[mt.TxnSlot.SenderID] = struct{}{}
		}
	}

	for senderID := range sendersWithChangedState {
		delete(sendersToRefresh, senderID)
		// Reset the dormancy timer: this sender had a real on-chain state change.
		p.senderLastActivity[senderID] = blockNum
		if err := p.refreshSender(cacheView, senderID); err != nil {
			return announcements, err
		}
	}

	// Refreshing eligibility without an announced account change must not extend the dormancy timer.
	for senderID := range sendersToRefresh {
		if err := p.refreshSender(cacheView, senderID); err != nil {
			return announcements, err
		}
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

func (p *TxPool) addLocked(mt *metaTxn, announcements *Announcements, cells blobCells) txpoolcfg.DiscardReason {
	// Find a replacement candidate within the sender's nonce domain.
	// Account nonces and keyed sequences can have equal values without conflicting.
	found, _ := p.all.tree.Get(mt)
	if mt.TxnSlot.TxType() == FrameTxnType {
		if reason := p.checkFrameAdmission(mt.TxnSlot, found); reason != txpoolcfg.NotSet {
			return reason
		}
	}
	if found != nil {
		decision := p.checkReplacement(mt, found)
		if decision.rebroadcast {
			// A local replacement attempt can help the existing transaction propagate.
			announcements.Append(found.TxnSlot.TxType(), found.TxnSlot.Size, found.TxnSlot.IDHash[:])
		}
		if decision.reason != txpoolcfg.NotSet {
			return decision.reason
		}
	}

	// Don't add blob txn to queued if it's less than current pending blob base fee
	if mt.TxnSlot.HasBlobs() && mt.TxnSlot.GetBlobFeeCap().LtUint64(p.pendingBlobFee.Load()) {
		return txpoolcfg.FeeTooLow
	}

	// Do not allow transaction from this same (sender + nonce) if sender has existing pooled authorization as authority
	senderAddr, ok := p.senders.senderID2Addr[mt.TxnSlot.SenderID]
	if !ok {
		p.logger.Info("senderID not registered, discarding transaction for safety")
		return txpoolcfg.InvalidSender
	}
	if owner, ok := p.auths[AuthAndNonce{senderAddr, mt.TxnSlot.Nonce}]; !mt.keyedNonce && ok && owner != found {
		return txpoolcfg.ErrAuthorityReserved
	}

	// Check if we have txn with same authorization in the pool
	if mt.TxnSlot.TxType() == SetCodeTxnType {
		for _, a := range mt.TxnSlot.AuthAndNonces {
			// Self authorization nonce should be senderNonce + 1
			if a.authority == senderAddr && a.nonce != mt.TxnSlot.Nonce+1 {
				p.logger.Debug("Self authorization nonce should be senderNonce + 1", "authority", a.authority, "txn", fmt.Sprintf("%x", mt.TxnSlot.IDHash))
				// This pool rule is stricter than execution. Its rejection depends only on
				// the transaction, so cache it to avoid repeating authorization recovery.
				p.discardReasonsLRU.Add(string(mt.TxnSlot.IDHash[:]), txpoolcfg.InvalidAuthorizationNonce)
				return txpoolcfg.InvalidAuthorizationNonce
			}
			if owner, ok := p.auths[AuthAndNonce{a.authority, a.nonce}]; ok && owner != found {
				p.logger.Debug("setCodeTxn ", "duplicateAuthority", a.authority, "nonce", a.nonce, "txn", fmt.Sprintf("%x", mt.TxnSlot.IDHash))
				return txpoolcfg.ErrAuthorityReserved
			}
		}
	}

	if found != nil {
		p.removeFromSubPool(found, "add")
		p.discardLocked(found, txpoolcfg.ReplacedByHigherTip)
	}
	if mt.TxnSlot.TxType() == SetCodeTxnType {
		for _, a := range mt.TxnSlot.AuthAndNonces {
			p.auths[AuthAndNonce{a.authority, a.nonce}] = mt
		}
	}

	hashStr := string(mt.TxnSlot.IDHash[:])
	p.byHash[hashStr] = mt
	p.reserveFrame(mt.TxnSlot.frameValidation)
	p.discardReasonsLRU.Remove(hashStr)
	p.frameRejections.Remove(hashStr)

	if replaced := p.all.replaceOrInsert(mt, p.logger); replaced != nil {
		if dbg.AssertEnabled {
			panic("must never happen")
		}
	}

	if mt.subPool&IsLocal != 0 {
		p.isLocalLRU.Add(hashStr, struct{}{})
	}
	// Restored frames wait outside the sub-pools until simulation establishes
	// their payer reservation. New submissions must already have a validation result.
	if mt.TxnSlot.TxType() != FrameTxnType || mt.TxnSlot.frameValidation != nil {
		p.queued.Add(mt, "addLocked", p.logger)
	}
	if blobHashes := mt.TxnSlot.GetBlobHashes(); len(blobHashes) > 0 {
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t + uint64(len(blobHashes)))
		for i, b := range blobHashes {
			bundle := mt.TxnSlot.BlobBundles[i]
			bundle.Cells = cells[b]
			p.blobs.put(b, mt.TxnSlot.IDHash, bundle)
		}
	}

	// Remove from mined cache as we are now "resurrecting" it to a sub-pool
	p.deleteMinedBlobTxn(hashStr)
	return txpoolcfg.NotSet
}

// dropping transaction from all sub-structures and from db
// Important: don't call it while iterating by all
func (p *TxPool) discardLocked(mt *metaTxn, reason txpoolcfg.DiscardReason) {
	p.releaseFrame(mt.TxnSlot.frameValidation)
	if frame, ok := mt.TxnSlot.Txn.(*types.FrameTransaction); ok && frame.UsesLegacyNonce() {
		p.frameAccountsToRefresh[frame.SenderAddress] = struct{}{}
	}
	hashStr := string(mt.TxnSlot.IDHash[:])
	delete(p.byHash, hashStr)
	p.deletedTxns = append(p.deletedTxns, mt)
	p.all.delete(mt, reason, p.logger)
	p.discardReasonsLRU.Add(hashStr, reason)
	if mt.TxnSlot.HasBlobs() {
		blobHashes := mt.TxnSlot.GetBlobHashes()
		t := p.totalBlobsInPool.Load()
		p.totalBlobsInPool.Store(t - uint64(len(blobHashes)))
		p.blobs.remove(mt.TxnSlot.IDHash, blobHashes)
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
			logger.Debug(
				"[txpool] evicted dormant queued sender",
				"senderID", senderID,
				"txns", len(toEvict),
				"dormantBlocks", currentBlock-lastBlock,
				"dormancyThreshold", dormancyBlocks,
			)
		}

		queuedLen = p.queued.Len()

		// Snapshot the map for DB persistence while still holding the lock.
		snapshot = make(map[uint64]uint64, len(p.senderLastActivity))
		maps.Copy(snapshot, p.senderLastActivity)
	}()

	if evictedSenders > 0 {
		// dormancyBlocks is derived from the EWMA block time (avgMs): dormancyBlocks = QueuedDormancyDuration / avgMs.
		// It adapts to real network conditions (e.g. mainnet averages ~13 s/block due to missed slots, not 12 s),
		// so dormancyBlocks may differ slightly from the naive 3 h / 12 s = 900 value. avgBlockMs is logged
		// alongside dormancyBlocks so the derivation is transparent without needing to inspect the source.
		logger.Info(
			"[txpool] dormancy sweep evicted senders from queued pool",
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

	// Clamp to [30 s, 10 min].
	interval := time.Duration(float64(base) * pressure * *backoff)
	interval = max(interval, 30*time.Second)
	interval = min(interval, 10*time.Minute)
	return interval
}

func (p *TxPool) GetBlobs(blobHashes []common.Hash) []PoolBlobBundle {
	return p.blobs.get(blobHashes)
}

// Cache recently mined blobs in anticipation of reorg, delete finalized ones
func (p *TxPool) processMinedFinalizedBlobs(minedTxns []*TxnSlot, minedBlock, finalizedBlock uint64) {
	p.lastFinalizedBlock.Store(finalizedBlock)
	// A retry may repeat already-applied transaction changes. Keep each cached
	// blob transaction in only one block bucket.
	for _, txn := range minedTxns {
		if !txn.HasBlobs() {
			continue
		}
		hash := string(txn.IDHash[:])
		// Mined txns arrive without blobs; the pooled or an earlier cached copy still has them.
		if pooled, ok := p.byHash[hash]; ok {
			txn = pooled.TxnSlot
		} else if queued, ok := p.unprocessedRemoteByHash[hash]; ok {
			full := *queued
			txn = &full
		} else if cached, ok := p.minedBlobTxnsByHash[hash]; ok {
			txn = cached.TxnSlot
		}
		p.deleteMinedBlobTxn(hash)
		mt := &metaTxn{TxnSlot: txn, minedBlockNum: minedBlock, bestIndex: len(p.minedBlobTxnsByBlock[minedBlock])}
		p.minedBlobTxnsByBlock[minedBlock] = append(p.minedBlobTxnsByBlock[minedBlock], mt)
		p.minedBlobTxnsByHash[hash] = mt
	}

	// Remove blobs of finalized blocks, and of blocks too deep to reorg, after the re-mined txns took their copies.
	for b, mts := range p.minedBlobTxnsByBlock {
		if (finalizedBlock > 0 && b <= finalizedBlock) || b+dbg.MaxReorgDepth < minedBlock {
			for _, mt := range mts {
				delete(p.minedBlobTxnsByHash, string(mt.TxnSlot.IDHash[:]))
			}
			delete(p.minedBlobTxnsByBlock, b)
		}
	}
}

// Delete individual hash entries from minedBlobTxns cache
func (p *TxPool) deleteMinedBlobTxn(hash string) {
	mt, exists := p.minedBlobTxnsByHash[hash]
	if !exists {
		return
	}
	items := p.minedBlobTxnsByBlock[mt.minedBlockNum]
	last := len(items) - 1
	if mt.bestIndex != last {
		moved := items[last]
		items[mt.bestIndex] = moved
		moved.bestIndex = mt.bestIndex
	}
	items[last] = nil
	items = items[:last]
	p.minedBlobTxnsByBlock[mt.minedBlockNum] = items
	mt.bestIndex = -1
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
		if txn.hasKeyedNonce() {
			if mt := p.byHash[string(txn.IDHash[:])]; mt != nil {
				p.removeFromSubPool(mt, "remove-mined")
				p.discardLocked(mt, txpoolcfg.Mined)
			}
			continue
		}
		nonce, ok := noncesToRemove[txn.SenderID]
		if !ok || txn.Nonce > nonce {
			noncesToRemove[txn.SenderID] = txn.Nonce // TODO: after 7702 nonce can be incremented more than once, may affect this
		}
	}

	var toDel []*metaTxn // can't delete items while iterate them

	discarded := 0

	for senderID, nonce := range noncesToRemove {
		byNonce.ascend(senderID, func(mt *metaTxn) bool {
			if mt.TxnSlot.hasKeyedNonce() {
				return true
			}
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

func (p *TxPool) updateGasLimitMarker(mt *metaTxn, blockGasLimit uint64) {
	limits, err := protocol.TransactionGasLimits(mt.TxnSlot.Txn, p.isAmsterdam())
	mt.subPool &^= NotTooMuchGas
	if err == nil && limits.Execution <= blockGasLimit && limits.State <= blockGasLimit {
		mt.subPool |= NotTooMuchGas
	}
}

func (p *TxPool) refreshSender(cacheView kvcache.CacheView, senderID uint64) error {
	nonce, balance, codeHash, err := p.senders.info(cacheView, senderID)
	if err != nil {
		return err
	}
	p.onSenderStateChange(senderID, nonce, balance, codeHash, p.blockGasLimit.Load())
	return nil
}

// onSenderStateChange refreshes pool eligibility and ordering for one sender.
// Transactions using account nonces depend on earlier nonces and cumulative sender costs.
// Keyed frames bypass that chain: simulation checks their nonces, and payer
// reservations cover their fees.
func (p *TxPool) onSenderStateChange(senderID uint64, senderNonce uint64, senderBalance uint256.Int, senderCodeHash accounts.CodeHash, blockGasLimit uint64) {
	sender := p.senders.senderID2Addr[senderID]
	delete(p.frameAccountsToRefresh, sender)
	// A legacy frame awaiting restoration or revalidation cannot advance the account
	// nonce. Keep its chain queued and defer gap eviction until it is usable or removed.
	nonceChainBlocked := p.hasParkedLegacyFrame(senderID)
	reserved := p.frameReservations[sender]
	if senderBalance.Gt(&reserved.cost) {
		senderBalance.Sub(&senderBalance, &reserved.cost)
	} else {
		senderBalance.Clear()
	}
	noGapsNonce := senderNonce
	cumulativeRequiredBalance := uint256.NewInt(0)
	minFeeCap := uint256.NewInt(0).SetAllOne()
	minBlobFeeCap := uint64(math.MaxUint64)
	minTip := uint64(math.MaxUint64)
	followsRemote := false
	senderHasCode := !senderCodeHash.IsEmpty()
	senderTxnKept := false
	var toDel []*metaTxn                       // can't delete items while iterate them
	var toDelReasons []txpoolcfg.DiscardReason // parallel reasons slice for toDel

	p.all.ascend(senderID, func(mt *metaTxn) bool {
		deleteAndContinueReasonLog := ""
		discardReason := txpoolcfg.NotSet
		switch {
		case !nonceChainBlocked && senderHasCode && senderTxnKept:
			deleteAndContinueReasonLog = "delegated sender transaction limit"
			discardReason = txpoolcfg.DelegatedTxnLimit
		case mt.keyedNonce:
			// Keyed frames share the delegated sender limit, but not the account nonce checks.
		case senderNonce > mt.TxnSlot.Nonce:
			deleteAndContinueReasonLog = "low nonce"
			discardReason = txpoolcfg.NonceTooLow
		case !nonceChainBlocked && senderHasCode && mt.TxnSlot.Nonce != senderNonce:
			deleteAndContinueReasonLog = "delegated sender nonce gap"
			discardReason = txpoolcfg.DelegatedNonceGap
		case !nonceChainBlocked && p.cfg.MaxNonceGap > 0 && mt.TxnSlot.Nonce > noGapsNonce && mt.TxnSlot.Nonce-noGapsNonce > p.cfg.MaxNonceGap:
			deleteAndContinueReasonLog = "nonce gap too large"
			discardReason = txpoolcfg.NonceTooDistant
		case !nonceChainBlocked && mt.TxnSlot.Nonce != noGapsNonce && mt.TxnSlot.HasBlobs(): // Discard nonce-gapped blob txns
			deleteAndContinueReasonLog = "nonce-gapped blob txn"
			discardReason = txpoolcfg.BlobNonceGap
		}
		if deleteAndContinueReasonLog != "" {
			if mt.TxnSlot.Traced {
				p.logger.Info("TX TRACING: onSenderStateChange loop iteration remove", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderID", senderID, "senderNonce", senderNonce, "txn.nonce", mt.TxnSlot.Nonce, "currentSubPool", mt.currentSubPool, "reason", deleteAndContinueReasonLog)
			}
			p.removeFromSubPool(mt, deleteAndContinueReasonLog)
			toDel = append(toDel, mt)
			toDelReasons = append(toDelReasons, discardReason)
			return true
		}
		if senderHasCode {
			senderTxnKept = true
		}

		if mt.keyedNonce {
			mt.minFeeCap = *mt.TxnSlot.GetFeeCap()
			mt.minBlobFeeCap = blobFeeCap(mt.TxnSlot, math.MaxUint64)
			mt.minTip = math.MaxUint64
			if mt.TxnSlot.GetTipCap().IsUint64() {
				mt.minTip = mt.TxnSlot.GetTipCap().Uint64()
			}
			mt.nonceDistance, mt.cumulativeBalanceDistance = 0, 0
			mt.subPool |= NoNonceGaps | EnoughBalance
			p.updateGasLimitMarker(mt, blockGasLimit)
			p.updateInSubPool(mt)
			return true
		}

		if minFeeCap.Gt(mt.TxnSlot.GetFeeCap()) {
			*minFeeCap = *mt.TxnSlot.GetFeeCap()
		}
		mt.minFeeCap = *minFeeCap
		minBlobFeeCap = blobFeeCap(mt.TxnSlot, minBlobFeeCap)
		mt.minBlobFeeCap = minBlobFeeCap
		if mt.TxnSlot.GetTipCap().IsUint64() {
			minTip = min(minTip, mt.TxnSlot.GetTipCap().Uint64())
		}
		mt.minTip = minTip
		mt.followsRemote = followsRemote
		followsRemote = followsRemote || mt.subPool&IsLocal == 0

		mt.nonceDistance = 0
		if mt.TxnSlot.Nonce > senderNonce { // no uint underflow
			mt.nonceDistance = mt.TxnSlot.Nonce - senderNonce
		}

		needBalance := requiredBalance(mt.TxnSlot)

		// NoNonceGaps requires every account nonce from the sender's current nonce
		// through this transaction's nonce to be present.
		mt.subPool &^= NoNonceGaps
		if noGapsNonce == mt.TxnSlot.Nonce {
			// Parking blocks selection and successors, but keeps the frame's own
			// nonce readiness for eviction ordering.
			if !nonceChainBlocked || (mt.currentSubPool == 0 && mt.TxnSlot.TxType() == FrameTxnType) {
				mt.subPool |= NoNonceGaps
			}
			noGapsNonce++
		}

		// Compare ordinary costs in account-nonce order with the unreserved balance.
		// Frame fees are already reserved against the payer, which may differ from the sender.
		mt.subPool &^= EnoughBalance
		mt.cumulativeBalanceDistance = math.MaxUint64
		if mt.TxnSlot.Nonce >= senderNonce {
			cumulativeRequiredBalance = cumulativeRequiredBalance.Add(cumulativeRequiredBalance, needBalance) // already deleted all transactions with nonce < sender.nonce
			if senderBalance.Gt(cumulativeRequiredBalance) || senderBalance.Eq(cumulativeRequiredBalance) {
				mt.subPool |= EnoughBalance
			} else if cumulativeRequiredBalance.IsUint64() && senderBalance.IsUint64() {
				mt.cumulativeBalanceDistance = cumulativeRequiredBalance.Uint64() - senderBalance.Uint64()
			}
		}

		p.updateGasLimitMarker(mt, blockGasLimit)

		if mt.TxnSlot.Traced {
			p.logger.Info("TX TRACING: onSenderStateChange loop iteration update", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "senderId", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "subPool", mt.currentSubPool)
		}

		// Some fields of mt might have changed, need to fix the invariants in the subpool best and worst queues
		p.updateInSubPool(mt)
		return true
	})

	for i, mt := range toDel {
		p.discardLocked(mt, toDelReasons[i])
	}

	p.logger.Trace("[txpool] onSenderStateChange", "sender", senderID, "count", p.all.count(senderID), "pending", p.pending.Len(), "baseFee", p.baseFee.Len(), "queued", p.queued.Len())
}

func (p *TxPool) notifyPendingTxns(announcements Announcements) {
	if announcements.Len() == 0 {
		return
	}
	select {
	case p.newPendingTxns <- announcements.Copy():
	default:
	}
}

func announceFirstPending(announcements *Announcements, mt *metaTxn) {
	if mt.announced {
		return
	}
	mt.announced = true
	announcements.Append(mt.TxnSlot.TxType(), mt.TxnSlot.Size, mt.TxnSlot.IDHash[:])
}

// promote reasserts the sub-pool invariants and adds to announcements each txn that enters pending
// for the first time.
func (p *TxPool) promote(cacheView kvcache.CacheView, pendingBaseFee uint64, pendingBlobFee uint64, announcements *Announcements) error {
	for {
		if err := p.refreshFrameAccounts(cacheView); err != nil {
			return err
		}
		p.promoteSubPools(pendingBaseFee, pendingBlobFee, announcements)
		// Eviction can release payer funds or break a sender's nonce chain.
		// Refresh affected accounts before finishing promotion.
		if len(p.frameAccountsToRefresh) == 0 {
			p.pending.EnforceBestInvariants()
			return nil
		}
	}
}

// coversPendingFees reports whether a txn, and every txn before it in its sender's nonce chain, pays the pending
// block's base fee and blob fee. The txn's own blob fee cap is checked too, as the chain minimum is only set once
// onSenderStateChange has seen the txn.
func coversPendingFees(mt *metaTxn, pendingBaseFee, pendingBlobFee uint64) bool {
	return mt.minFeeCap.CmpUint64(pendingBaseFee) >= 0 && mt.minBlobFeeCap >= pendingBlobFee &&
		(!mt.TxnSlot.HasBlobs() || mt.TxnSlot.GetBlobFeeCap().CmpUint64(pendingBlobFee) >= 0)
}

func (p *TxPool) promoteSubPools(pendingBaseFee uint64, pendingBlobFee uint64, announcements *Announcements) {
	// Demote worst transactions that do not qualify for pending sub pool anymore, to other sub pools, or discard
	for worst := p.pending.Worst(); p.pending.Len() > 0 && (worst.subPool < BaseFeePoolBits || !coversPendingFees(worst, pendingBaseFee, pendingBlobFee)); worst = p.pending.Worst() {
		tx := p.pending.PopWorst()
		if worst.subPool >= BaseFeePoolBits {
			p.baseFee.Add(tx, "demote-pending", p.logger)
		} else {
			p.queued.Add(tx, "demote-pending", p.logger)
		}
	}

	// Promote best transactions from base fee pool to pending pool while they qualify. The pool is ordered by fee cap,
	// so a txn that waits only for the blob fee, its own or that of a blob txn before it in its sender's nonce chain,
	// is set aside rather than holding back the txns after it.
	var blobFeeBlocked []*metaTxn
	for best := p.baseFee.Best(); p.baseFee.Len() > 0 && best.subPool >= BaseFeePoolBits && best.minFeeCap.CmpUint64(pendingBaseFee) >= 0; best = p.baseFee.Best() {
		tx := p.baseFee.PopBest()
		if !coversPendingFees(tx, pendingBaseFee, pendingBlobFee) {
			blobFeeBlocked = append(blobFeeBlocked, tx)
			continue
		}
		announceFirstPending(announcements, tx)
		p.pending.Add(tx, p.logger)
	}
	for _, tx := range blobFeeBlocked {
		p.baseFee.Add(tx, "blob-fee", p.logger)
	}

	// Demote worst transactions that do not qualify for base fee pool anymore, to queued sub pool, or discard
	for worst := p.baseFee.Worst(); p.baseFee.Len() > 0 && worst.subPool < BaseFeePoolBits; worst = p.baseFee.Worst() {
		tx := p.baseFee.PopWorst()
		p.queued.Add(tx, "demote-base", p.logger)
	}

	// Promote best transactions from the queued pool to either pending or base fee pool, while they qualify
	for best := p.queued.Best(); p.queued.Len() > 0 && best.subPool >= BaseFeePoolBits; best = p.queued.Best() {
		tx := p.queued.PopBest()
		if coversPendingFees(best, pendingBaseFee, pendingBlobFee) {
			announceFirstPending(announcements, tx)
			p.pending.Add(tx, p.logger)
		} else {
			p.baseFee.Add(tx, "promote-queued", p.logger)
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

	// A parked frame temporarily blocks its account-nonce chain. Count that chain
	// against total capacity rather than evicting it at the queued limit.
	var blocked []*metaTxn
	if len(p.all.senderIDFrameTxn) > 0 && p.queued.Len() > p.queued.limit {
		for _, tx := range p.queued.best.ms {
			if !tx.keyedNonce && p.hasParkedLegacyFrame(tx.TxnSlot.SenderID) {
				blocked = append(blocked, tx)
			}
		}
		for _, tx := range blocked {
			p.queued.Remove(tx, "frame validation pending", p.logger)
		}
	}
	for p.queued.Len() > p.queued.limit {
		tx := p.queued.PopWorst()
		p.discardLocked(tx, txpoolcfg.QueuedPoolOverflow)
	}
	for _, tx := range blocked {
		p.queued.Add(tx, "frame validation pending", p.logger)
	}
	p.enforceTotalCapacity(pendingBaseFee)
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
	frameCtx, cancelFrames := context.WithCancel(ctx)
	var frames sync.WaitGroup
	frames.Go(func() { p.runRemoteFrames(frameCtx) })
	defer func() {
		cancelFrames()
		frames.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			frames.Wait()
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
				for range 16 { // drain more events from channel, then merge and dedup them
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
						wrappedSize := len(slotRlp)
						slotRlp, err2 := types.UnwrapTxPlayloadRlp(slotRlp)
						if err2 != nil {
							continue
						}

						hasBlobs := t == BlobTxnType || len(slotRlp) != wrappedSize

						// Empty rlp can happen if a transaction we want to broadcast has just been mined, for example
						slotsRlp = append(slotsRlp, slotRlp)
						if p.IsLocal(hash) {
							localTxnTypes = append(localTxnTypes, t)
							localTxnSizes = append(localTxnSizes, size)
							localTxnHashes = append(localTxnHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if !hasBlobs {
								localTxnRlps = append(localTxnRlps, slotRlp)
								broadcastHashes = append(broadcastHashes, hash...)
							}
						} else {
							remoteTxnTypes = append(remoteTxnTypes, t)
							remoteTxnSizes = append(remoteTxnSizes, size)
							remoteTxnHashes = append(remoteTxnHashes, hash...)

							// "Nodes MUST NOT automatically broadcast blob transactions to their peers" - EIP-4844
							if !hasBlobs && len(slotRlp) < txMaxBroadcastSize {
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
					p.logger.Trace("Local txn broadcast", "txHash", hex.EncodeToString(broadcastHashes.At(i)), "toPeer", peer)
				}
				hashSentTo := p.p2pSender.AnnouncePooledTxns(localTxnTypes, localTxnSizes, localTxnHashes, localTxnsBroadcastMaxPeers*2)
				for i := 0; i < localTxnHashes.Len(); i++ {
					hash := localTxnHashes.At(i)
					p.logger.Trace("Local txn announced", "txHash", hex.EncodeToString(hash), "toPeer", hashSentTo[i], "baseFee", p.pendingBaseFee.Load())
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
	// it's important that write db txn is done inside lock, to make last writes visible for all read operations
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
			delete(p.senderLastActivity, id)
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
		addr, ok := p.senders.senderID2Addr[metaTx.TxnSlot.SenderID]
		if !ok {
			p.logger.Warn("[txpool] flush: sender address not found by ID", "senderID", metaTx.TxnSlot.SenderID)
			continue
		}

		has, err := tx.Has(kv.PoolTransaction, []byte(txHash))
		if err != nil {
			return err
		}
		if !has {
			v = common.EnsureEnoughSize(v, 20+len(metaTx.TxnSlot.Rlp))
			copy(v[:20], addr[:])
			copy(v[20:], metaTx.TxnSlot.Rlp)
			if err := tx.Put(kv.PoolTransaction, []byte(txHash), v); err != nil {
				return err
			}
		}
		// Blobs alias Rlp, so keeping it costs no memory and lets a mined blob txn return on unwind.
		if !metaTx.TxnSlot.HasBlobs() {
			metaTx.TxnSlot.Rlp = nil
		}
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

func (p *TxPool) fromDB(ctx context.Context) error {
	var frames frameRevalidationBatch
	for {
		if err := p.waitForStateRecovery(ctx); err != nil {
			return err
		}
		// Recovery also needs database readers. If the view goes stale after the
		// wait, release this reader before retrying; simulation opens its own reads.
		err := p.poolDB.View(ctx, func(tx kv.Tx) error {
			return p.withLockedState(ctx, nil, false, func(coreTx kv.TemporalTx, cacheView kvcache.CacheView) error {
				err := p.fromDBLocked(tx, coreTx, cacheView, &frames.parked)
				frames.generation, frames.blockHash = p.stateGeneration, p.stateBlockHash
				return err
			})
		})
		if errors.Is(err, errStateViewStale) {
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	// Each restored frame gets its own initial budget. Retryable failures leave
	// the indexed entry parked for the next block update.
	for _, mt := range frames.parked {
		if err := p.revalidateFrames(ctx, []*metaTxn{mt}, frames.generation, frames.blockHash); err != nil {
			return err
		}
	}
	return nil
}

func (p *TxPool) fromDBLocked(tx kv.Tx, coreTx kv.TemporalTx, cacheView kvcache.CacheView, parked *[]*metaTxn) error {
	header, err := rawdb.ReadHeaderByHash(coreTx, p.stateBlockHash)
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

	pendingBaseFee, pendingBlobFee := p.pendingBaseFee.Load(), p.pendingBlobFee.Load()
	// A state reset supplies fees and gas limits for the restored state. Only fall
	// back to the database when startup has not received a block notification.
	if p.blockGasLimit.Load() == 0 {
		var minBlobGasPrice, blockGasLimit uint64

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
			blockGasLimit = ethconfig.DefaultBlockGasLimitByChain(p.chainConfig)
		}

		// The validation and insertion below read these from p, so publish them first.
		var pendingBaseFee256 uint256.Int
		pendingBaseFee256.SetUint64(pendingBaseFee)
		p.setPendingBaseFeeOnQueues(pendingBaseFee256)
		p.pendingBaseFee.Store(pendingBaseFee)
		p.pendingBlobFee.Store(pendingBlobFee)
		p.blockGasLimit.Store(blockGasLimit)
	}

	var restored TxnSlots
	parseCtx := NewTxnParseContext(p.chainID)
	parseCtx.WithSender(false)

	it, err = tx.Range(kv.PoolTransaction, nil, nil, order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return err
		}
		addr, txnRlp := *(*[20]byte)(v[:20]), bytes.Clone(v[20:])
		txn := &TxnSlot{}

		// TODO(eip-4844) ensure wrappedWithBlobs when transactions are saved to the DB
		_, err = parseCtx.ParseTransaction(txnRlp, 0, txn, nil, false /* hasEnvelope */, true /*wrappedWithBlobs*/, nil)
		if err != nil {
			err = fmt.Errorf("err: %w, rlp: %x", err, txnRlp)
			p.logger.Warn("[txpool] fromDB: parseTransaction", "err", err)
			continue
		}
		if !txn.HasBlobs() {
			txn.Rlp = nil // means that we don't need store it in db anymore
		}
		restored.Append(txn, addr[:], false)
	}

	immediate, frames := p.splitRestoredTxns(restored)
	*parked = p.restoreFramesLocked(p.lastSeenBlock.Load(), frames)
	if header != nil {
		// Expired roots must not occupy a sender's frame slot while validation is unavailable.
		pendingSlot := protocol.FrameValidationSlot(header.SlotNumber)
		*parked = slices.DeleteFunc(*parked, func(mt *metaTxn) bool {
			roots := protocol.FrameTransactionRecentRoots(mt.TxnSlot.Txn.(*types.FrameTransaction))
			invalidRoots := slices.ContainsFunc(roots, func(ref protocol.RecentRootReference) bool {
				return !ref.ValidAt(pendingSlot)
			})
			if invalidRoots {
				p.discardLocked(mt, txpoolcfg.FrameStateChanged)
			}
			return invalidRoots
		})
	}
	var txns TxnSlots
	cells := make(blobCells)
	for i, txn := range immediate.Txns {
		addr := immediate.Senders.AddressAt(i)
		isLocalTx := immediate.IsLocal[i]
		txn.SenderID, txn.Traced = p.senders.getOrCreateID(addr, p.logger)

		reason, err := p.validateTxWithCells(txn, isLocalTx, cacheView, cells)
		if err != nil {
			return err
		}
		if reason == txpoolcfg.Success && txn.TxType() == BlobTxnType {
			reason = p.validateBlobSidecar(txn, cells)
		}
		if reason != txpoolcfg.NotSet && reason != txpoolcfg.Success {
			// The flush-time delete needs only the hash and the sender; dropping the body frees blobs now.
			p.deletedTxns = append(p.deletedTxns, newMetaTxn(&TxnSlot{IDHash: txn.IDHash, SenderID: txn.SenderID}, false, 0))
			continue
		}
		p.recoverAuthorizations(txn)
		txns.Append(txn, addr[:], isLocalTx)
	}

	err = p.senders.registerNewSenders(&txns, p.logger)
	if err != nil {
		return err
	}
	if _, _, err := p.addTxns(p.lastSeenBlock.Load(), cacheView, txns, cells,
		pendingBaseFee, pendingBlobFee, false); err != nil {
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

	// Cold-start fallback: for any queued or parked sender not found in the persisted map, assign
	// the current block as the baseline so they receive a full grace period. This covers
	// the first run after feature introduction and senders added after the last sweep.
	coldStartBlock := p.lastSeenBlock.Load()
	p.all.ascendAll(func(mt *metaTxn) bool {
		if mt.currentSubPool == QueuedSubPool || mt.currentSubPool == 0 {
			senderID := mt.TxnSlot.SenderID
			if _, ok := p.senderLastActivity[senderID]; !ok {
				p.senderLastActivity[senderID] = coldStartBlock
			}
		}
		return true
	})

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
	p.publishDepthMetrics()
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
			subPool := mt.currentSubPool
			if subPool == 0 && mt.TxnSlot.TxType() == FrameTxnType {
				// Parked frames appear as queued in RPC, but stay outside the
				// scheduling sub-pools until validation succeeds.
				subPool = QueuedSubPool
			}
			txns = append(txns, mt)
			senders = append(senders, sender)
			subPoolTypes = append(subPoolTypes, subPool)
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
