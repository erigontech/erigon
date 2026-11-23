// Copyright 2024 The Erigon Authors
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

package execmodule

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

var ErrMissingChainSegment = errors.New("missing chain segment")

func makeErrMissingChainSegment(blockHash common.Hash) error {
	return errors.Join(ErrMissingChainSegment, errors.New("block hash: "+blockHash.String()))
}

func GetBlockHashFromMissingSegmentError(err error) (common.Hash, bool) {
	if !errors.Is(err, ErrMissingChainSegment) {
		return common.Hash{}, false
	}
	// Otherwise, we assume the error is a joined error from makeErrMissingChainSegment.
	// We define an interface to get access to the underlying errors.
	type unwrapper interface {
		Unwrap() []error
	}
	uw, ok := err.(unwrapper)
	if !ok {
		return common.Hash{}, false
	}

	// iterate through suberrors to find one that contains the block hash info.
	var hashStr string
	const prefix = "block hash: "
	for _, subErr := range uw.Unwrap() {
		msg := subErr.Error()
		if after, ok := strings.CutPrefix(msg, prefix); ok {
			hashStr = after
			break
		}
	}
	if hashStr == "" {
		return common.Hash{}, false
	}

	// Convert the extracted string into a common.Hash.
	// This assumes the existence of common.ParseHash.
	return common.HexToHash(hashStr), true
}

// Cache bridges RPC reads to SharedDomains and the caller's DB tx.
// Pinned transactions keep their selected SD. Unpinned transactions select
// the exec module's current SD, or the published SD if none is available.
// The exec module's read lock protects selection of its current SD, not later reads.
//
// OnNewBlock is intentionally a no-op: in the embedded (non-remote) rpcdaemon
// the SD is the authoritative source, so the coherent cache's state-tracking
// machinery is unnecessary.
type Cache struct {
	execModule  *ExecModule
	publishedSD func() *execctx.SharedDomains // returns the latest published SD from Events
}

// SetPublishedSD sets the published SD provider used by unpinned transactions
// when no exec module is attached or its currentContext is nil.
func (c *Cache) SetPublishedSD(provider func() *execctx.SharedDomains) {
	c.publishedSD = provider
}

var (
	_ kvcache.Cache     = (*Cache)(nil)     // compile-time interface check
	_ kvcache.CacheView = (*CacheView)(nil) // compile-time interface check
)

func (c *Cache) View(ctx context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	// A pinned read view must keep its selected state generation, including an
	// explicit no-overlay selection, even when a newer generation is published.
	overlay, pinned := membatchwithdb.ViewOverlay(tx)
	if pinned && overlay != nil {
		sd, ok := overlay.DomainReader.(*execctx.SharedDomains)
		if !ok {
			return nil, fmt.Errorf("unsupported selected domain reader %T", overlay.DomainReader)
		}
		return &CacheView{context: sd, getter: sd.AsSnapshotGetter(tx)}, nil
	}

	var stateCache *cache.StateCache
	if c.execModule != nil {
		stateCache = c.execModule.stateCache
	}
	var sd *execctx.SharedDomains
	if !pinned {
		if c.execModule != nil {
			c.execModule.lock.RLock()
			sd = c.execModule.currentContext
			c.execModule.lock.RUnlock()
		}
		// Fall back to the published SD from Events while an FCU commits
		// (currentContext is nil but the SD is still valid in memory).
		if sd == nil && c.publishedSD != nil {
			sd = c.publishedSD()
		}
	}

	if sd != nil {
		return &CacheView{context: sd, getter: sd.AsGetter(tx)}, nil
	}
	return &CacheView{getter: execctx.NewCachedTemporalTxStateGetter(tx, stateCache)}, nil
}

// CodeByHash is the execution's code store for RPC replays.
func (c *Cache) CodeByHash() state.CodeByHash {
	if c.execModule == nil || c.execModule.stateCache == nil {
		return nil
	}
	return c.execModule.stateCache
}

func (c *Cache) OnNewBlock(sc *remoteproto.StateChangeBatch) {}
func (c *Cache) Evict() int                                  { return 0 }
func (c *Cache) Len() int                                    { return 0 }
func (c *Cache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*kvcache.CacheValidationResult, error) {
	return &kvcache.CacheValidationResult{Enabled: false}, nil
}

type CacheView struct {
	context *execctx.SharedDomains
	// getter is built once per view: it carries the per-tx cache ReadView, so
	// per-read getter construction would cost an allocation on every call.
	getter kv.TemporalGetter
}

func (c *CacheView) Get(k []byte) ([]byte, error) {
	if len(k) == 20 {
		v, _, err := c.getter.GetLatest(kv.AccountsDomain, k, kv.GetLatestOptions{})
		return v, err
	}
	v, _, err := c.getter.GetLatest(kv.StorageDomain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *CacheView) GetCode(k []byte) ([]byte, error) {
	v, _, err := c.getter.GetLatest(kv.CodeDomain, k, kv.GetLatestOptions{})
	return v, err
}

func (c *CacheView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	if c.context != nil {
		// Disabled in-memory history is not a cache miss: the backing
		// transaction may not contain the requested state.
		if len(key) == 20 {
			return c.context.GetAsOf(kv.AccountsDomain, key, ts)
		}
		return c.context.GetAsOf(kv.StorageDomain, key, ts)
	}
	return nil, false, nil
}

type ExecModule struct {
	backgroundCtx context.Context
	// census* are the running totals behind [exec-census]: what was offered to execution and what
	// came out into a body. See filterStreamLocked.
	censusOffered, censusKept, censusStale, censusFuture, censusSeen, censusLastLogged uint64
	censusOfferedSet, censusKeptSet                                                    map[common.Hash]struct{}
	censusBySender                                                                     map[accounts.Address]senderCensus
	// Snapshots + MDBX
	blockReader dbservices.FullBlockReader

	// MDBX database
	db kv.TemporalRwDB // main database
	// semaphore is the module's single mutual-exclusion domain: it guards the
	// pipeline Sync and all FCU state. Ops either TryAcquire and report Busy
	// (retried by the CL) or block, and the background FCU commit/prune
	// goroutines inherit the semaphore, releasing it only when their work is done.
	semaphore *semaphore.Weighted
	// semHolder names whatever currently holds the semaphore, so a refusal can say who refused it rather
	// than leaving it to be guessed. Written only under the hold; read without one.
	semHolder atomic.Value
	// semAcquires counts every successful take, so a waiter can measure how many others got in ahead of
	// it. TryAcquire callers BARGE: they take the lock whenever it is momentarily free, jumping the queue
	// that Acquire waits in.
	semAcquires atomic.Int64
	semSince    atomic.Int64

	// roundMu is the INNER LOOP's own mutual-exclusion domain: one pre-execution round at a time.
	//
	// ⚠ IT IS DELIBERATELY NOT THE SEMAPHORE ABOVE. A round used to take that one and hold it for its
	// whole execution, which made the two operations that MUST overlap to build a block on time —
	// executing into the block, and sealing it — mutually exclusive. AssembleBlock TryAcquires, so it
	// returned Busy whenever a round was running; the seal then missed its slot, the CL retried with
	// identical attributes, the duplicate-build short circuit handed back a PayloadID for a build that
	// never ran, and production stalled. Measured on live328: 115 "failed to produce execution payload"
	// over two minutes. Measured on live331: a round waiting 164ms on the semaphore to do 12ms of work.
	//
	// The domains are genuinely separate — the semaphore guards the pipeline Sync and FCU state, while a
	// round writes into the producer's OWN per-generation SharedDomains (preexec_frontier.go) and its own
	// flash state. Lock order is ONE WAY: an outer op (assemble) may take the semaphore and then this; a
	// round takes ONLY this, so there is no cycle to deadlock on.
	// assembleMu is ASSEMBLY's own domain: one block assembled at a time. Assembly builds a NEW block and
	// rewrites no state, so it must not queue behind validation — see AssembleBlock.
	assembleMu    sync.Mutex
	roundMu       sync.Mutex
	forkValidator *ForkValidator
	// preExec is the PRODUCER's pre-executed block chain — separate from the fork validator's candidate
	// slot, which is validation space (see preexec_frontier.go).
	preExec          *preExecFrontier
	pipelineExecutor *PipelineExecutor

	logger log.Logger
	// Block building
	nextPayloadId       uint64
	builderFunc         builder.BlockBuilderFunc
	inclusionListFunc   builder.InclusionListFunc
	builders            map[uint64]*builderEntry
	buildersByTimestamp map[uint64]uint64
	// preconfirmedBlocks holds payloads the PRECONFIRM assemble variant sealed synchronously from the
	// maintained (preconfirmed) extending-fork flashblock — keyed by payload id, returned by
	// GetAssembledBlock. Distinct from `builders` (the from-scratch async path). See assemblePreconfirmed.
	preconfirmedBlocks map[uint64]*types.BlockWithReceipts

	// blockAssembler, when set (a DAG-driven L2), makes AssembleBlock DEFER block production to the
	// ordering layer instead of building from-scratch from the txpool. The CL's FCU-with-attributes calls
	// in; the assembler inserts a block-end MARKER carrying those attrs into the DAG and WAITS (bounded,
	// inside the CL assembly delay) for the marker to appear in the committed stream — the point at which
	// every node agrees the block ends ([[dag_start_end_system_tx]]). It then seals the pre-executed body
	// (zero re-execution) and returns it. nil ⇒ standard txpool/from-scratch builder. Set via
	// SetBlockAssembler (dependency inversion: the interface lives here, the cocoon rollup driver
	// implements it — erigon cannot import cocoon).
	blockAssembler BlockAssembler
	// pendingBlock maps a payload id to the build params for a DAG boundary assemble whose marker was
	// inserted by AssembleBlock (AssembleBlock) but whose body+seal is completed lazily in GetAssembledBlock
	// (GetAssembledBlock + assemblePreconfirmed). Guarded by pendingBlockMu because AssembleBlock (writer,
	// holds the semaphore) and GetAssembledBlock (reader, must check BEFORE acquiring the semaphore) race.
	pendingBlock   map[uint64]*builder.Parameters
	pendingBlockMu sync.Mutex
	// preconfirmedByParent holds a boundary block SEALED by the marker handler (SealBlock) when the
	// block-end marker committed in consensus, keyed by the block's PARENT hash. This is the UNIVERSAL
	// close — it runs on every node at the marker, decoupled from the CL role — so GetAssembledBlock
	// (proposer) just RETRIEVES the already-sealed block by its build params' ParentHash instead of
	// re-sealing. Guarded by pendingBlockMu.
	preconfirmedByParent map[common.Hash]*types.BlockWithReceipts

	// sealedByHash records every block this node SEALED locally (marker-driven close), keyed by its sealed
	// hash. newPayload/ValidateChain consults it to ACCEPT a block this node produced instead of re-executing
	// it: the block was produced+validated once on the frontier SD, so re-running it on a fresh SD parented to
	// (lagging) canonical state is both redundant AND WRONG — the fresh SD lacks the frontier predecessor's
	// state/txNum, so it computes a different root or fails (nonce-too-low / can't-find-header) and falsely
	// marks our own valid block INVALID. Accept-by-hash keeps the architecture's single-execution property.
	// Guarded by pendingBlockMu. Pruned as blocks canonicalize (updateForkChoice).
	sealedByHash map[common.Hash]*types.Header

	// flash is the exec-owned in-progress flashblock body (see flashBodyState / PreExecuteFlashblock): the
	// execution half maintains the filtered block body here so the driver only streams unfiltered txs.
	flash flashBodyState

	// frontierHeader is the exec-owned run-ahead FRONTIER head — the sealed block a newly-opening flashblock
	// chains onto (set by SealBlock at the marker close; also settable when the CL accepts a head). It is
	// EXECUTION state (a sealed header exec produced), so it lives here, not mirrored under a driver lock. Read
	// LOCK-FREE via FrontierHeader(): AssembleBlock holds the exec semaphore and its anchor path reads this, so
	// it must NOT touch the semaphore or a lock the semaphore-holder already contends on (the ibMu↔semaphore
	// deadlock this ownership move removes). atomic.Pointer gives a consistent, lock-free snapshot.
	frontierHeader atomic.Pointer[types.Header]

	// execCost is the sliding-window per-tx execution-cost tracker (seal time / txs). The driver reads its
	// upper-quartile to bound how many txs it feeds into a block, so a block always seals within the CL's assemble
	// timeout (never miss the boundary / strand work). Lazily created on first use so a nil field is safe.
	execCost *execCostWindow
	// gasOut is the asked-vs-used table: what transactions declaring a given gas limit actually spend. It
	// supplies the batch sizer's per-tx gas, sampled once per TRANSACTION rather than once per sealed block.
	gasOut *gasOutturn

	// Changes accumulator
	hook  *stageloop.Hook
	accum *Accumulation

	// configuration
	config  *chain.Config
	syncCfg ethconfig.Sync
	// rules engine
	engine         rules.Engine
	balRegenerator *bal.Regenerator

	fcuBackgroundPrune      bool
	onlySnapDownloadOnStart bool
	nextForkActivated       bool
	// gas-weighted EWMA: accumulate gas and time separately so near-empty blocks don't skew the average
	accumGasMgas float64
	accumTimeSec float64

	lock           sync.RWMutex
	currentContext *execctx.SharedDomains
	publishedSD    func() *execctx.SharedDomains // fallback while an FCU commits

	// stateCache is a cache for state data (accounts, storage, code)
	stateCache  *cache.StateCache
	readAheader *exec.BlockReadAheader

	stateTransitionObserver StateTransitionObserver

	stopNode func() error
}

var _ ExecutionModule = (*ExecModule)(nil) // compile-time interface check

// ExecModuleOption configures execution-module construction.
type ExecModuleOption func(*ExecModule)

// WithStateTransitionObserver enables deterministic execution lifecycle hooks
// for integration tests.
func WithStateTransitionObserver(observer StateTransitionObserver) ExecModuleOption {
	return func(module *ExecModule) {
		module.stateTransitionObserver = observer
	}
}

func NewExecModule(
	ctx context.Context,
	blockReader dbservices.FullBlockReader,
	db kv.TemporalRwDB,
	pipelineExecutor *PipelineExecutor,
	currentBlockNumber uint64,
	config *chain.Config,
	builderFunc builder.BlockBuilderFunc,
	inclusionListFunc builder.InclusionListFunc,
	hook *stageloop.Hook,
	accum *Accumulation,
	stateCache *Cache,
	stateCacheBudget datasize.ByteSize,
	logger log.Logger,
	engine rules.Engine,
	syncCfg ethconfig.Sync,
	fcuBackgroundPrune bool,
	onlySnapDownloadOnStart bool,
	readAheader *exec.BlockReadAheader,
	stopNode func() error,
	opts ...ExecModuleOption,
) *ExecModule {
	domainCache := newDomainStateCache(stateCacheBudget)
	execctx.GuardAggregatorForCache(db, domainCache)
	preExec := newPreExecFrontier()
	forkValidator := newForkValidator(ctx, currentBlockNumber, pipelineExecutor, blockReader, syncCfg.MaxReorgDepth, preExec, syncCfg.SlowBlockThreshold)

	em := &ExecModule{
		blockReader:             blockReader,
		db:                      db,
		logger:                  logger,
		forkValidator:           forkValidator,
		preExec:                 preExec,
		pipelineExecutor:        pipelineExecutor,
		builders:                make(map[uint64]*builderEntry),
		buildersByTimestamp:     make(map[uint64]uint64),
		preconfirmedBlocks:      make(map[uint64]*types.BlockWithReceipts),
		pendingBlock:            make(map[uint64]*builder.Parameters),
		preconfirmedByParent:    make(map[common.Hash]*types.BlockWithReceipts),
		sealedByHash:            make(map[common.Hash]*types.Header),
		builderFunc:             builderFunc,
		inclusionListFunc:       inclusionListFunc,
		config:                  config,
		semaphore:               semaphore.NewWeighted(1),
		execCost:                newExecCostWindow(64),
		gasOut:                  newGasOutturn(256),
		hook:                    hook,
		accum:                   accum,
		engine:                  engine,
		balRegenerator:          bal.NewRegenerator(blockReader, engine, logger),
		syncCfg:                 syncCfg,
		backgroundCtx:           ctx,
		fcuBackgroundPrune:      fcuBackgroundPrune,
		onlySnapDownloadOnStart: onlySnapDownloadOnStart,
		stateCache:              domainCache,
		readAheader:             readAheader,
		stopNode:                stopNode,
	}
	for _, opt := range opts {
		opt(em)
	}

	// Wire the process-global state cache into the read-ahead so its
	// prefetches populate the same hashmap that SharedDomains.GetLatest
	// probes on the EVM hot path. Reth's "same hashmap" pattern.
	if readAheader != nil {
		readAheader.SetStateCache(domainCache)
	}

	if stateCache != nil {
		stateCache.execModule = em
	}
	return em
}

// WaitIdle blocks until any in-flight updateForkChoice goroutine finishes or
// ctx ends.
func (e *ExecModule) WaitIdle(ctx context.Context) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return // context cancelled — best effort
	}
	e.semaphore.Release(1)
}

// Drain waits without a local timeout for serialized execution work to finish.
// Callers must stop producers first because backing resources are unsafe to
// close while execution still holds a transaction.
func (e *ExecModule) Drain() {
	e.WaitIdle(context.Background())
}

// newDomainStateCache is the module's one construction site of the domain
// state cache: USE_STATE_CACHE=false builds none, so nothing upstream can
// allocate a cache that would only be discarded. A budget > 0 overrides the
// production per-domain byte budget (test harnesses keep per-fixture modules
// small); 0 means the production default.
func newDomainStateCache(budget datasize.ByteSize) *cache.StateCache {
	if !dbg.UseStateCache {
		return nil
	}
	if budget > 0 {
		return cache.NewStateCache(budget, budget, budget, budget)
	}
	return cache.NewDefaultStateCache()
}

// Close releases the domain state cache's reservation in the shared memory
// envelope.
func (e *ExecModule) Close() {
	if e.stateCache != nil {
		e.stateCache.Close()
	}
}

// closeModuleContext closes and clears e.currentContext. The nil swap happens
// under e.lock first, so getters holding the read lock (beginOverlayOrRo) can
// never obtain a SharedDomains that is about to be closed.
func (e *ExecModule) closeModuleContext() {
	e.lock.Lock()
	old := e.currentContext
	e.currentContext = nil
	e.lock.Unlock()
	if old != nil {
		old.Close()
	}
}

// ForkValidator returns the fork validator owned by this module.
func (e *ExecModule) ForkValidator() *ForkValidator { return e.forkValidator }

// SetPublishedSD wires the ExecModule to fall back to the published SD from Events
// when currentContext is nil (e.g. while an FCU commits).
func (e *ExecModule) SetPublishedSD(provider func() *execctx.SharedDomains) {
	e.publishedSD = provider
}

func (e *ExecModule) getHeader(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Header, error) {
	if e.blockReader == nil {
		return rawdb.ReadHeader(tx, blockHash, blockNumber), nil
	}

	return e.blockReader.Header(ctx, tx, blockHash, blockNumber)
}

func (e *ExecModule) getTD(_ context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*uint256.Int, error) {
	return rawdb.ReadTd(tx, blockHash, blockNumber)
}

func (e *ExecModule) getRawBody(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.RawBody, error) {
	if e.blockReader == nil {
		return rawdb.ReadRawBody(tx, blockHash, blockNumber)
	}
	return e.blockReader.BodyWithRawTransactions(ctx, tx, blockHash, blockNumber)
}

func (e *ExecModule) canonicalHash(ctx context.Context, tx kv.Tx, blockNumber uint64) (common.Hash, error) {
	var canonical common.Hash
	var err error
	if e.blockReader == nil {
		canonical, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		var ok bool
		canonical, ok, err = e.blockReader.CanonicalHash(ctx, tx, blockNumber)
		if err != nil {
			return common.Hash{}, err
		}
		if !ok {
			return common.Hash{}, nil
		}
	}

	return canonical, nil
}

// suspendReadAhead prevents raw-database warmup from filling the shared state
// cache while an unwind's staged state is being read or published. It returns
// the context error rather than allowing the unwind to proceed unsuspended.
func (e *ExecModule) suspendReadAhead(ctx context.Context) (func(), error) {
	if e.readAheader == nil {
		return func() {}, nil
	}
	return e.readAheader.SuspendWarmup(ctx)
}

// unwindToCommonCanonical re-aligns the chain to the common canonical ancestor of header. It is
// newPayload/FCU processing and belongs ONLY to the canonical path (validateChainLocked) — do not call it
// from pre-exec. Pre-exec runs a private staged-sync instance inside the generation's overlay and puts the
// block where it is itself: every round re-establishes canonical[N], the head header, the txNum index and
// the stage progress at the header's current hash. Asking this there discards the block's own accumulated
// state — measured on live48, where a stale canonical row after a withdrawals re-stamp made the close unwind
// the block it was about to seal, so it sealed a body its state transition never applied.
// Read-ahead stays suspended after staging an unwind; the caller resumes it only after all reads of the
// staged state have ended.
func (e *ExecModule) unwindToCommonCanonical(sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header, ensureReadAheadSuspended func() error) error {
	currentHeader := header
	for {
		isCanonical, err := e.isCanonicalHash(e.backgroundCtx, tx, currentHeader.Hash())
		if err != nil {
			return err
		}
		if isCanonical {
			break
		}
		parentBlockHash, parentBlockNum := currentHeader.ParentHash, currentHeader.Number.Uint64()-1
		currentHeader, err = e.getHeader(e.backgroundCtx, tx, parentBlockHash, parentBlockNum)
		if err != nil {
			return err
		}
		if currentHeader == nil {
			return makeErrMissingChainSegment(parentBlockHash)
		}
	}
	// Check if you can skip unwind by comparing the current header number with the progress of all stages.
	// If they are equal, then we are safely already at the common canonical and can skip unwind.
	unwindPoint := currentHeader.Number.Uint64()
	commonProgress, allEqual, err := stages.GetStageProgressIfAllEqual(tx,
		stages.Headers, stages.Senders, stages.Execution)
	if err != nil {
		return err
	}
	if allEqual && commonProgress == unwindPoint {
		return nil
	}

	if err := ensureReadAheadSuspended(); err != nil {
		return fmt.Errorf("suspend read-ahead: %w", err)
	}
	return e.pipelineExecutor.UnwindForValidation(sd, tx, unwindPoint)
}

const nextForkBanner = `
:'######:::'##::::::::::'###::::'##::::'##::'######::'########:'########:'########::'########:::::'###::::'##::::'##:
'##... ##:: ##:::::::::'## ##::: ###::'###:'##... ##:... ##..:: ##.....:: ##.... ##: ##.... ##:::'## ##::: ###::'###:
 ##:::..::: ##::::::::'##:. ##:: ####'####: ##:::..::::: ##:::: ##::::::: ##:::: ##: ##:::: ##::'##:. ##:: ####'####:
 ##::'####: ##:::::::'##:::. ##: ## ### ##:. ######::::: ##:::: ######::: ########:: ##:::: ##:'##:::. ##: ## ### ##:
 ##::: ##:: ##::::::: #########: ##. #: ##::..... ##:::: ##:::: ##...:::: ##.. ##::: ##:::: ##: #########: ##. #: ##:
 ##::: ##:: ##::::::: ##.... ##: ##:.:: ##:'##::: ##:::: ##:::: ##::::::: ##::. ##:: ##:::: ##: ##.... ##: ##:.:: ##:
. ######::: ########: ##:::: ##: ##:::: ##:. ######::::: ##:::: ########: ##:::. ##: ########:: ##:::: ##: ##:::: ##:
:......::::........::..:::::..::..:::::..:::......::::::..:::::........::..:::::..::........:::..:::::..::..:::::..::
=============================================== GLAMSTERDAM ACTIVATED ===============================================
`

func (e *ExecModule) ValidateChain(ctx context.Context, block *types.Block) (ValidationResult, error) {
	blockHash, blockNumber := block.Hash(), block.NumberU64()
	defer validateChainDuration.ObserveDuration(time.Now())
	// ACCEPT a block this node already SEALED via the marker-driven close (recorded by
	// ingestSealedFlashblockLocked, keyed by sealed hash) — pure LOOKUP, never a re-execution. It was executed
	// once on the frontier SD; re-running it on a fresh SD parented to lagging canonical state lacks the
	// frontier predecessor's state/txNum and would compute a wrong root or fail (nonce-too-low /
	// can't-find-header), falsely invalidating our own valid block (the tip stall). The block's hash commits to
	// every header field, so a hash match IS a full header match. This is the DAG-L2 tip path: every tip block
	// is marker-sealed, so it is always accepted here and never re-executed.
	//
	// A block NOT in the sealed set falls through to base erigon's execute-and-validate path below — the
	// engine block DOWNLOADER (sync of FOREIGN peer blocks) and non-DAG chains (dev-l1) rely on it, and base
	// erigon's own execmodule tests drive the seal THROUGH this method. Accept-if-sealed is purely additive.
	//
	// ALREADY-COMMITTED fast-path: a block at/below the FCU-committed height (fv.committedHeight) is already
	// accepted and its state durably applied — never re-validate it. Caplin re-validates such LAGGING blocks
	// (its NewPayload backfill); by then the sealedByHash entry has been pruned (forkchoice), so the seal path
	// below misses and validateChainLocked would build a FRESH SharedDomains that ValidatePayload ADOPTS as
	// fv.sharedDom — DISPLACING the DAG frontier/extending-fork SD the next SealBlock needs, which wedges L2
	// block production (block-end markers stop sealing). committedHeight is the reliable "already applied"
	// signal (independent of the pruned sealed set and the not-yet-durable overlay canonical row). The caller
	// uses only status + LatestValidHash (ComputedRoot is ignored). Only fires for number <= committed, so a
	// freshly-produced tip block (dev-l1 or L2, number > committed) still executes normally — it does NOT
	// short-circuit an un-applied block (the canonicalHeaderIfAny/HasValidHash mistake that froze dev-l1).
	if blockNumber <= e.preExec.CommittedHeight() {
		var executedTo uint64
		var canonical bool
		var stateIsCanonical bool
		if err := e.db.View(ctx, func(tx kv.Tx) (err error) {
			if canonical, err = e.blockReader.IsCanonical(ctx, tx, blockHash, blockNumber); err != nil || !canonical {
				return err
			}
			if executedTo, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
				return err
			}
			executedHash, err := rawdb.ReadCanonicalHash(tx, executedTo)
			if err != nil {
				return err
			}
			stateIsCanonical = executedHash == rawdb.ReadHeadBlockHash(tx)
			return nil
		}); err != nil {
			return ValidationResult{}, err
		}
		if canonical && stateIsCanonical && blockNumber <= executedTo {
			return ValidationResult{ValidationStatus: ExecutionStatusSuccess, LatestValidHash: blockHash}, nil
		}
	}
	sealed, err := e.awaitPreExecSeal(ctx, blockHash, blockNumber, block.ParentHash())
	if err != nil {
		return ValidationResult{ValidationStatus: ExecutionStatusBusy}, nil
	}
	if sealed != nil && sealed.Number.Uint64() == blockNumber {
		// This is newPayload for a block THIS node produced, so it is where the pre-executed generation
		// crosses into validation space — the FCU that follows canonicalises it through the ordinary merge
		// path, exactly as it would a block a follower had executed. Without the adopt the block is accepted
		// but no candidate is installed, and the FCU falls through to re-executing it from canonical state.
		//
		// It is also where the block's data crosses over. The seal keeps the sealed block in pre-exec space; the fork
		// choice looks its header up in the module context, so it is staged there now — here, and nowhere earlier.
		if err := e.semaphore.Acquire(ctx, 1); err != nil {
			return ValidationResult{ValidationStatus: ExecutionStatusBusy}, nil
		}
		e.semTake("ValidateChain:stageSealed")
		stageErr := e.stageSealedForCanonicalLocked(ctx, blockHash, blockNumber)
		e.semDrop()
		e.semaphore.Release(1)
		if stageErr != nil {
			return ValidationResult{}, fmt.Errorf("ValidateChain: %w", stageErr)
		}
		adopted := e.forkValidator.AdoptPreExecuted(blockHash, blockNumber)
		e.logger.Debug("[execmodule] ValidateChain: accepting locally-sealed block (no re-exec)",
			"number", blockNumber, "hash", blockHash, "root", sealed.Root, "adopted", adopted)
		return ValidationResult{
			ValidationStatus: ExecutionStatusSuccess,
			LatestValidHash:  blockHash,
			ComputedRoot:     sealed.Root,
		}, nil
	}
	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.ValidateChain: ExecutionStatus_Busy")
		return ValidationResult{ValidationStatus: ExecutionStatusBusy}, nil
	}
	defer e.semaphore.Release(1)
	e.semTake("ValidateChain")
	defer e.semDrop()
	return e.validateChain(ctx, block, false)
}

// awaitPreExecSeal returns this node's sealed block for hash, first waiting out a pre-exec still building that height:
// the payload's hash only exists once the local seal at that height has run.
func (e *ExecModule) awaitPreExecSeal(ctx context.Context, hash common.Hash, number uint64, parent common.Hash) (*types.Header, error) {
	for {
		e.flash.mu.Lock()
		changed := e.flash.changedLocked()
		buildingOn := e.flash.parent
		if e.flash.valid {
			buildingOn = e.flash.built.Parent
		}
		building := (e.flash.num == number && buildingOn == parent) ||
			e.flash.rebuilding == rebuildOf{number: number, parent: parent}
		e.flash.mu.Unlock()
		e.pendingBlockMu.Lock()
		sealed := e.sealedByHash[hash]
		sealedAtHeight := sealed != nil
		for _, h := range e.sealedByHash {
			if !sealedAtHeight && h.Number.Uint64() == number {
				sealedAtHeight = true
			}
		}
		e.pendingBlockMu.Unlock()
		if sealedAtHeight || !building {
			return sealed, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (e *ExecModule) ValidateChainAndView(ctx context.Context, block *types.Block, wait time.Duration, fn BlockStateView) (ValidationResult, func() error, error) {
	blockHash := block.Hash()
	valid := ValidationResult{ValidationStatus: ExecutionStatusSuccess, LatestValidHash: blockHash}
	if committed, view, err := e.viewCommittedBlock(ctx, blockHash, block.NumberU64(), fn); err != nil || committed {
		return valid, view, err
	}
	acquireCtx, cancel := context.WithTimeout(ctx, wait)
	err := e.semaphore.Acquire(acquireCtx, 1)
	cancel()
	if err != nil {
		// A forkchoice update is answered before its commit, which can land while the module stays busy.
		if committed, view, err := e.viewCommittedBlock(ctx, blockHash, block.NumberU64(), fn); err != nil || committed {
			return valid, view, err
		}
		e.logger.Trace("ethereumExecutionModule.ValidateChainAndView: ExecutionStatus_Busy")
		return ValidationResult{ValidationStatus: ExecutionStatusBusy}, nil, nil
	}
	defer e.semaphore.Release(1)
	if held, view, err := e.viewRetainedTip(ctx, blockHash, fn); err != nil || held {
		return valid, view, err
	}
	if committed, view, err := e.viewCommittedBlock(ctx, blockHash, block.NumberU64(), fn); err != nil || committed {
		return valid, view, err
	}
	start := time.Now()
	result, err := e.validateChain(ctx, block, true)
	validateChainDuration.ObserveDuration(start)
	if err != nil || result.ValidationStatus != ExecutionStatusSuccess {
		return result, nil, err
	}
	_, view, err := e.viewRetainedTip(ctx, blockHash, fn)
	return result, view, err
}

// closeWithoutView closes a resource that no returned view owns.
func closeWithoutView(view *func() error, close func()) {
	if *view == nil {
		close()
	}
}

// validateChain with revalidate executes again a non-canonical block that passed validation before, so the fork
// validator holds its state.
func (e *ExecModule) validateChain(ctx context.Context, block *types.Block, revalidate bool) (ValidationResult, error) {
	blockHash, blockNumber := block.Hash(), block.NumberU64()
	if e.readAheader != nil {
		defer e.readAheader.CancelWarmup()
	}
	e.hook.LastNewBlockSeen(blockNumber) // used by eth_syncing
	if e.currentContext != nil {
		e.currentContext.ResetPendingUpdates()
	}
	e.logger.Debug("[execmodule] validating chain", "number", blockNumber, "hash", blockHash)
	header, body := block.HeaderNoCopy(), block.Body()
	var (
		currentBlockNumber *uint64
		payloadExecuted    bool
		canonical          bool
		missingSegment     bool
		err                error
	)
	alreadyExecuted := func(tx kv.Getter) (bool, error) {
		var err error
		canonical, err = e.blockReader.IsCanonical(ctx, tx, blockHash, blockNumber)
		if err != nil || !canonical {
			return false, err
		}
		if blockNumber > rawdb.ReadForkchoiceFinalizedNum(tx) {
			return false, nil
		}
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		return progress >= blockNumber, err
	}
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		overlay := e.currentContext.BlockOverlay()
		roTx, err := e.db.BeginTemporalRo(ctx)
		if err != nil {
			return ValidationResult{}, err
		}
		defer roTx.Rollback()
		overlay.UpdateTxn(roTx)
		storedHeader, err := e.blockReader.Header(ctx, overlay, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		if storedHeader == nil {
			return ValidationResult{ValidationStatus: ExecutionStatusMissingSegment}, nil
		}
		e.readAheader.AddHeaderAndBody(ctx, e.db, overlay, header, body)
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(overlay)
		payloadExecuted, err = alreadyExecuted(overlay)
		if err != nil {
			return ValidationResult{}, err
		}
	} else {
		if err := e.db.View(ctx, func(tx kv.Tx) error {
			storedHeader, err := e.blockReader.Header(ctx, tx, blockHash, blockNumber)
			if err != nil {
				return err
			}
			if storedHeader == nil {
				missingSegment = true
				return nil
			}
			e.readAheader.AddHeaderAndBody(ctx, e.db, tx, header, body)
			currentBlockNumber = rawdb.ReadCurrentBlockNumber(tx)
			payloadExecuted, err = alreadyExecuted(tx)
			return err
		}); err != nil {
			return ValidationResult{}, err
		}
		if missingSegment {
			return ValidationResult{ValidationStatus: ExecutionStatusMissingSegment}, nil
		}
	}
	e.forkValidator.ClearWithUnwind()

	if math.AbsoluteDifference(*currentBlockNumber, blockNumber) >= e.syncCfg.MaxReorgDepth {
		return ValidationResult{
			ValidationStatus: ExecutionStatusTooFarAway,
			LatestValidHash:  common.Hash{},
		}, nil
	}
	// Finality fixes canonical hashes even after their unwind history is pruned.
	if payloadExecuted || (!canonical && !revalidate && e.forkValidator.IsValidated(blockHash)) {
		return ValidationResult{
			ValidationStatus: ExecutionStatusSuccess,
			LatestValidHash:  blockHash,
		}, nil
	}
	// Use the overlay-as-rwTx pattern: the validation pipeline writes through
	// a fresh BlockOverlay on a new SharedDomains. This mirrors updateForkChoice
	// (forkchoice.go:239-251) and is required by the parallel exec path —
	// executeBlocks opens its own roTx in a separate goroutine and reads
	// recently-inserted block data via te.doms.BlockOverlay().NewReadView,
	// which shares the overlay's mem layer. A plain BeginTemporalRwNosync
	// would leave doms with no overlay and the parallel goroutine could not
	// see uncommitted block headers/bodies.
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return ValidationResult{}, err
	}
	defer roTx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, roTx, e.logger, execctx.WithLocalCacheUnwind())
	if err != nil {
		if doms != nil {
			doms.Close()
		}
		if errors.Is(err, commitmentdb.ErrBehindCommitment) {
			return ValidationResult{ValidationStatus: ExecutionStatusTooFarAway}, nil
		}
		return ValidationResult{}, err
	}
	// Do not defer doms.Close(): on the success path ownership transfers to
	// forkValidator.sharedDom inside ValidatePayload and later phases close it,
	// so we Close explicitly only on the early-return error paths below.
	if err := doms.InitBlockOverlay(roTx); err != nil {
		doms.Close()
		return ValidationResult{}, fmt.Errorf("ValidateChain: init block overlay: %w", err)
	}
	var tx kv.TemporalRwTx = doms.BlockOverlay()
	// Chain the validation SD to the canonical generation (e.currentContext) for
	// any payload with a parent, not just head-extending ones: head-extending
	// payloads read its not-yet-committed domain state instead of stale MDBX, and
	// fork payloads reach the canonical generation's pastChangesAccumulator (via
	// GetDiffset's parent chain) to build the unwind set — without the link the
	// unwind runs empty, leaving the BranchCache unmasked and corrupting the root.
	if e.currentContext != nil {
		doms.SetParent(e.currentContext)
	}
	// Flush block overlay data (headers, bodies, TDs from InsertBlocks) into
	// the validation overlay so unwindToCommonCanonical and ValidatePayload —
	// and the parallel exec goroutine via NewReadView — see this block data.
	// The InsertBlocks overlay on e.currentContext retains its data unchanged.
	// Do NOT UpdateTxn on e.currentContext.BlockOverlay() here — that would
	// reassign its backing db to our soon-to-be-rolled-back roTx and leave
	// e.currentContext in an inconsistent state for UpdateForkChoice.
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		if err := e.currentContext.BlockOverlay().Flush(ctx, tx); err != nil {
			doms.Close()
			return ValidationResult{}, fmt.Errorf("ValidateChain: flush overlay to validation tx: %w", err)
		}
	}
	// Set state cache in SharedDomains for use during state reading
	doms.SetStateCache(e.stateCache)
	// Either unwind path may run, and both can run in one validation. Share one
	// lazy suspension so it spans every staged-state read without penalising the
	// common case where validation needs no unwind.
	var resumeReadAhead func()
	var suspendReadAheadErr error
	var suspendReadAheadOnce sync.Once
	ensureReadAheadSuspended := func() error {
		suspendReadAheadOnce.Do(func() {
			resumeReadAhead, suspendReadAheadErr = e.suspendReadAhead(ctx)
		})
		return suspendReadAheadErr
	}
	defer func() {
		if resumeReadAhead != nil {
			resumeReadAhead()
		}
	}()

	if err := e.unwindToCommonCanonical(doms, tx, header, ensureReadAheadSuspended); err != nil {
		doms.Close()
		return ValidationResult{}, err
	}

	// Validation only. The producer's close of its own pre-executed block is not a validation question and does not
	// come through here: pre-exec has its own close (closePreExecutedLocked).
	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body, revalidate, ensureReadAheadSuspended, e.logger)
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}

	// Validation writes and cache unwind remain local until adoption.
	validationStatus := ExecutionStatusSuccess
	if status == engine_types.AcceptedStatus {
		validationStatus = ExecutionStatusMissingSegment
	}
	isInvalidChain := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	// Only open a second tx when we actually need to write (bad-chain purge).
	// On the valid-chain path (the common case at tip) opening + empty-committing
	// a second RwTx just produces no-op commits with openTxs>=2, pinning freelist
	// pages against concurrent readers.
	if isInvalidChain {
		purgeTx, err := e.db.BeginTemporalRwNosync(ctx)
		if err != nil {
			return ValidationResult{}, err
		}
		defer purgeTx.Rollback()
		if (lvh != common.Hash{}) && lvh != blockHash {
			if err := e.purgeBadChain(ctx, purgeTx, lvh, blockHash); err != nil {
				return ValidationResult{}, err
			}
			if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
				if err := e.purgeBadChain(ctx, e.currentContext.BlockOverlay(), lvh, blockHash); err != nil {
					return ValidationResult{}, err
				}
			}
		}
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain is invalid", "hash", blockHash)
		validationStatus = ExecutionStatusBadBlock
		if err := purgeTx.Commit(); err != nil {
			return ValidationResult{}, err
		}
	}
	if !e.nextForkActivated && validationStatus == ExecutionStatusSuccess && e.config.IsAmsterdam(header.Time) {
		e.nextForkActivated = true
		e.logger.Info(nextForkBanner)
	}
	result := ValidationResult{
		ValidationStatus: validationStatus,
		LatestValidHash:  lvh,
	}
	if validationError != nil {
		result.ValidationError = validationError.Error()
	}
	// The computed root ONLY on success: on a FAILED validation the fork validator CLOSES the SD, so
	// GetCommitmentContext() returns nil and reading the trie would nil-deref — a bad block must return BadBlock.
	if validationStatus == ExecutionStatusSuccess {
		if cc := doms.GetCommitmentContext(); cc != nil {
			if root, rerr := cc.Trie().RootHash(); rerr == nil && len(root) > 0 {
				result.ComputedRoot = common.BytesToHash(root)
			}
		}
		// The output side of this validation's own execution, off the receipts it produced.
		receipts := doms.FlashblockReceipts()
		var cum uint64
		for i, r := range receipts {
			cum += r.GasUsed
			r.CumulativeGasUsed = cum
			// What this transaction ASKED for against what it SPENT. Both halves are already in hand here and
			// nowhere else — body.Transactions is decoded and the receipts are in the same order — so the table
			// costs one map lookup per transaction and no decoding.
			if i < len(body.Transactions) && body.Transactions[i] != nil {
				e.gasOut.record(body.Transactions[i].GetGasLimit(), r.GasUsed)
			}
		}
		result.FlashblockReceiptCount = len(receipts)
		result.GasUsed = cum
		result.ReceiptHash = types.DeriveSha(receipts)
		result.Bloom = types.CreateBloom(receipts)
	}
	return result, nil
}

// GetPreExecutedBody returns this node's locally pre-executed in-progress flashblock body (the txs it
// accumulated across PreExecute rounds from the DAG) plus the deferred in-progress hash and number. The
// newPayload for a DAG-preconfirmed flashblock is body-LESS — it carries only the sealed HEADER — so each
// node supplies the body from HERE rather than from the wire (the transmission optimization: the body was
// already delivered as DAG tx hashes and pre-executed). Empty extending fork ⇒ error.
func (e *ExecModule) GetPreExecutedBody(ctx context.Context) (*types.RawBody, common.Hash, uint64, error) {
	oldHash, number, sd := e.preExec.Active()
	if sd == nil || oldHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: no in-progress flashblock")
	}
	// From the generation's own overlay: the module context is the canonical path's, and a fork choice tears it down.
	ov := e.generationOverlay(oldHash, number)
	if ov == nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: no block overlay")
	}
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: begin ro: %w", err)
	}
	defer roTx.Rollback()
	ov.UpdateTxn(roTx)
	body, err := e.blockReader.BodyWithTransactions(ctx, ov, oldHash, number)
	if err != nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: read body: %w", err)
	}
	if body == nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: body %x not found", oldHash)
	}
	txs, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: encode body: %w", err)
	}
	return &types.RawBody{Transactions: txs, Uncles: body.Uncles, Withdrawals: body.Withdrawals}, oldHash, number, nil
}

// filterCandidatesByNonce keeps only the candidate txs applicable at the given state reader: it DROPS
// nonce-too-low (already-sealed/stale), keeps the applicable ones in order, and RE-ORDERS nonce-too-high to be
// reconsidered once earlier nonces apply — the nonce dimension of the standard block-assembly candidate filter
// (builder.filterBadTransactions). A per-sender speculative next-nonce tracks in-batch accepts so a run of
// consecutive nonces all pass. Called at the START of the pre-exec cycle (PreExecute) against the SD execution
// itself uses, so an invalid candidate is filtered rather than breaking block execution. (Fee/balance/EOA are
// already enforced by the txpool the candidates came from.)
func filterCandidatesByNonce(reader state.StateReader, signer *types.Signer, txs []types.Transaction) []types.Transaction {
	if len(txs) == 0 {
		return txs
	}
	next := make(map[accounts.Address]uint64, len(txs))
	nextNonce := func(a accounts.Address) uint64 {
		if n, ok := next[a]; ok {
			return n
		}
		if acc, aerr := reader.ReadAccountData(a); aerr == nil && acc != nil {
			return acc.Nonce
		}
		return 0
	}
	kept := make([]types.Transaction, 0, len(txs))
	remaining := append([]types.Transaction(nil), txs...)
	missed := 0
	for len(remaining) > 0 && missed != len(remaining) {
		tx := remaining[0]
		s, ok := tx.GetSender()
		if !ok {
			if rec, serr := signer.Sender(tx); serr == nil {
				tx.SetSender(rec)
				s, ok = rec, true
			}
		}
		if !ok {
			remaining = remaining[1:] // unrecoverable sender — drop
			continue
		}
		exp := nextNonce(s)
		switch {
		case tx.GetNonce() < exp:
			remaining = remaining[1:] // stale (already sealed) — drop
		case tx.GetNonce() > exp:
			missed++
			remaining = append(remaining[1:], tx) // future — requeue for a later pass
		default:
			missed = 0
			kept = append(kept, tx)
			next[s] = exp + 1
			remaining = remaining[1:]
		}
	}
	return kept
}

// IngestSealedFlashblock is the newPayload step for a freshly-sealed flashblock: given only the sealed
// HEADER H1 (produced by the assemble/CLOSE, carrying the real Root/GasUsed/ReceiptHash/Bloom — the
// payload message is body-LESS), it materialises H1 in the currentContext block overlay by pairing it
// with the node's OWN pre-executed body (GetPreExecutedBody — NOT a transmitted body), copies the
// deferred block's TD onto H1.Hash(), and re-points the extending fork from the deferred hash to H1 with
// NO re-execution (the body already executed during PreExecute, and the sealed state is the extending
// fork's maintained SharedDomains). A subsequent NORMAL FCU(H1) then takes the merge-extending-fork fast
// path and canonicalises the correct real-root header ("FCU works as before"). Idempotent when H1 is
// already the extending-fork head.
func (e *ExecModule) IngestSealedFlashblock(ctx context.Context, sealed *types.Header) error {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: semaphore acquire: %w", err)
	}
	defer e.semaphore.Release(1)
	e.semTake("IngestSealedFlashblock")
	defer e.semDrop()
	return e.ingestSealedFlashblockLocked(ctx, sealed)
}

// ingestSealedFlashblockLocked is IngestSealedFlashblock's body with the exec-module semaphore ALREADY
// held, so the preconfirm-assemble path (AssembleBlock holds the semaphore) can re-key the sealed block
// without a re-entrant acquire. Callers MUST hold e.semaphore.
func (e *ExecModule) ingestSealedFlashblockLocked(ctx context.Context, sealed *types.Header) error {
	body, oldHash, number, err := e.GetPreExecutedBody(ctx)
	if err != nil {
		return err
	}
	newHash := sealed.Hash()

	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("IngestSealedFlashblock: begin ro: %w", err)
	}
	defer roTx.Rollback()

	// Re-key in the generation's own overlay — where the rounds put the in-progress block, and where SealActive
	// rewrites the head and canonical entries below. The module context is the canonical path's: a fork choice can
	// have torn it down since the last round.
	ov := e.generationOverlay(oldHash, number)
	if ov == nil {
		return fmt.Errorf("IngestSealedFlashblock: no block overlay for %x", oldHash)
	}
	ov.UpdateTxn(roTx)
	if newHash == oldHash {
		// Already sealed in place: nothing to re-key, but its ids are still unsettled until here.
		bfs, rerr := rawdb.ReadBodyForStorageByKey(ov, dbutils.BlockBodyKey(number, oldHash))
		if rerr != nil || bfs == nil {
			return fmt.Errorf("IngestSealedFlashblock: read in-place body: %v", rerr)
		}
		if err := sealPreExecBodyIds(ov, bfs); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: %w", err)
		}
		traceBodyIds(e.logger, "seal", ov, newHash, number, bfs.BaseTxnID.U64(), true)
		return nil
	}
	// Pre-exec space only. The canonical path gets the sealed block when newPayload hands it over.
	targets := []kv.RwTx{ov}

	td, err := rawdb.ReadTd(ov, oldHash, number)
	if err != nil {
		return fmt.Errorf("IngestSealedFlashblock: read TD: %w", err)
	}
	if td == nil {
		td = new(uint256.Int)
	}
	for _, t := range targets {
		if err := rawdb.WriteHeader(t, sealed); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: write header: %w", err)
		}
		if err := rawdb.WriteTd(t, newHash, number, *td); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: write TD: %w", err)
		}
	}
	// RE-KEY the body, do not write a second copy. The sealed block carries EXACTLY the transactions of the
	// in-progress block it closes — only the header hash changes — so its transactions are already in kv.EthTx,
	// written there by the accumulation rounds under oldHash. WriteRawBody would allocate a FRESH BaseTxnID
	// range from the kv.EthTx sequence and write every transaction again; that allocation is not durable from
	// here, because the sequence the NEXT block is seeded from comes from the committed DB and does not carry
	// it. The successor was therefore handed this block's ids and OVERWROTE its transactions: the sealed block
	// read back a mixture of its own body and its successor's, so it could not be re-executed ("nonce too
	// high" on receipt derivation) even though its state was correct. Re-keying reuses the range the rounds
	// already wrote — one record write instead of a full body rewrite per block.
	bfs, rerr := rawdb.ReadBodyForStorageByKey(ov, dbutils.BlockBodyKey(number, oldHash))
	if rerr != nil || bfs == nil || bfs.TxCount != types.TxCountToTxAmount(len(body.Transactions)) {
		// No in-progress record to re-key (or it disagrees with the sealed body) — write one, at the same base the
		// rounds use: the sequence has not been advanced yet, and is advanced once below.
		base, err := ov.ReadSequence(kv.EthTx)
		if err != nil {
			return fmt.Errorf("IngestSealedFlashblock: read txn id sequence: %w", err)
		}
		if err := writePreExecBody(ov, newHash, number, body, base); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: write body: %w", err)
		}
		if bfs, rerr = rawdb.ReadBodyForStorageByKey(ov, dbutils.BlockBodyKey(number, newHash)); rerr != nil || bfs == nil {
			return fmt.Errorf("IngestSealedFlashblock: read back written body: %v", rerr)
		}
	}
	for _, t := range targets {
		if err := rawdb.WriteBodyForStorage(t, newHash, number, bfs); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: re-key body: %w", err)
		}
	}
	// The body is final: advance the txn id sequence once, past its end system slot, and drop anything a dropped
	// round left at or above that slot. Only the generation's overlay carries the sequence the successor seeds from.
	if err := sealPreExecBodyIds(ov, bfs); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: %w", err)
	}
	traceBodyIds(e.logger, "seal", ov, newHash, number, bfs.BaseTxnID.U64(), true)
	// Remove the DEFERRED (zero-output) in-progress block so the post-newPayload state is IDENTICAL to a
	// normal newPayload: exactly ONE block at this height (the sealed H1). The deferred ibHash is a scratch
	// artifact of the flashblock accumulation — leaving it would strand an orphan header/body/TD at height N.
	for _, t := range targets {
		rawdb.DeleteHeader(t, oldHash, number)
		rawdb.DeleteBody(t, oldHash, number)
		if err := t.Delete(kv.HeaderTD, dbutils.HeaderKey(number, oldHash)); err != nil {
			return fmt.Errorf("IngestSealedFlashblock: delete deferred TD: %w", err)
		}
	}
	if err := e.preExec.SealActive(oldHash, newHash, number, body.Transactions); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: seal in place: %w", err)
	}
	// The block is now fully sealed (its block-end has run), so record the sealed hash as validated: a
	// newPayload of it short-circuits rather than re-executing.
	e.forkValidator.MarkValid(newHash)
	// Record the sealed block so newPayload/ValidateChain ACCEPTS it (this node produced+validated it once on
	// the frontier) instead of re-executing it on a fresh SD against lagging canonical state.
	e.pendingBlockMu.Lock()
	e.sealedByHash[newHash] = sealed
	e.pendingBlockMu.Unlock()
	e.flash.mu.Lock()
	e.flash.signalLocked()
	e.flash.mu.Unlock()
	e.logger.Debug("[execmodule] flashblock sealed (newPayload ingest)",
		"number", number, "deferredHash", oldHash, "sealedHash", newHash, "root", sealed.Root)
	return nil
}

// stageSealedForCanonicalLocked hands a block this node sealed over to the canonical path. newPayload is where a
// pre-executed block crosses into it: the fork choice that follows resolves the head — and the next block's insert its
// parent — from the module context, so the sealed header, TD and body record are copied there from the generation's
// overlay, where the seal put them. The body is not rewritten: its transactions are already stored, and a second
// write would allocate them a second id range. Caller holds e.semaphore.
func (e *ExecModule) stageSealedForCanonicalLocked(ctx context.Context, hash common.Hash, number uint64) error {
	gen := e.generationOverlay(hash, number)
	if gen == nil {
		// Not in pre-exec space (never there, or already retired): there is nothing to hand over.
		return nil
	}
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("stage sealed block: begin ro: %w", err)
	}
	defer roTx.Rollback()
	gen.UpdateTxn(roTx)
	mod, err := e.moduleContextLocked(ctx, roTx)
	if err != nil {
		return fmt.Errorf("stage sealed block: module context: %w", err)
	}
	dst := mod.BlockOverlay()
	if dst == gen {
		return nil
	}
	hdr := rawdb.ReadHeader(gen, hash, number)
	if hdr == nil {
		return fmt.Errorf("stage sealed block %d %x: header not in its generation", number, hash)
	}
	td, err := rawdb.ReadTd(gen, hash, number)
	if err != nil {
		return fmt.Errorf("stage sealed block: read TD: %w", err)
	}
	if td == nil {
		td = new(uint256.Int)
	}
	bfs, err := rawdb.ReadBodyForStorageByKey(gen, dbutils.BlockBodyKey(number, hash))
	if err != nil || bfs == nil {
		return fmt.Errorf("stage sealed block %d %x: body record: %v", number, hash, err)
	}
	if err := rawdb.WriteHeader(dst, hdr); err != nil {
		return fmt.Errorf("stage sealed block: write header: %w", err)
	}
	if err := rawdb.WriteTd(dst, hash, number, *td); err != nil {
		return fmt.Errorf("stage sealed block: write TD: %w", err)
	}
	if err := rawdb.WriteBodyForStorage(dst, hash, number, bfs); err != nil {
		return fmt.Errorf("stage sealed block: write body record: %w", err)
	}
	return nil
}

// viewRetainedTip views blockHash when it is the tip of the fork the fork validator retains, and must run under the
// module semaphore. A tip on any other parent than the committed head gets its parent state by unwinding, in memory,
// the tip's changeset from the fork's state.
func (e *ExecModule) viewRetainedTip(ctx context.Context, blockHash common.Hash, fn BlockStateView) (held bool, view func() error, err error) {
	head, number, sd := e.forkValidator.ExtendingFork()
	if sd == nil || head != blockHash {
		return false, nil, nil
	}
	tx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return false, nil, err
	}
	defer closeWithoutView(&view, tx.Rollback)
	blockTx := sd.BlockOverlayTemporalTx(tx)
	if blockTx == nil {
		return false, nil, nil
	}
	header, err := e.blockReader.Header(ctx, blockTx, blockHash, number)
	if err != nil || header == nil {
		return false, nil, err
	}
	executedTo, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, nil, err
	}
	if number == executedTo+1 {
		committedParent, err := rawdb.ReadCanonicalHash(tx, executedTo)
		if err != nil {
			return false, nil, err
		}
		if committedParent == header.ParentHash {
			return e.viewTipOverCommittedHead(ctx, tx, sd, blockTx, blockHash, number, fn)
		}
	}
	tip := sd.GetChangesetByHash(number, blockHash)
	if tip == nil {
		return false, nil, nil
	}
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	for d := range tip.Diffs {
		diffs[d] = tip.Diffs[d].GetDiffSet()
	}
	tipFrom, err := e.blockReader.TxnumReader().Min(ctx, blockTx, number)
	if err != nil {
		return false, nil, err
	}
	parentSD, err := execctx.NewSharedDomains(ctx, tx, e.logger, execctx.WithoutSharedBranchCache(), execctx.WithLocalCacheUnwind())
	if parentSD != nil {
		defer parentSD.Close()
	}
	if err != nil {
		return false, nil, err
	}
	// The parent state reads the fork validator's state, which the module drops once it is released.
	parentSD.SetParent(sd)
	parentSD.Unwind(tipFrom, &diffs)
	return true, nil, fn(blockTx, parentSD.AsSnapshotGetter(tx))
}

// viewTipOverCommittedHead views a tip whose parent is the committed head. The view copies the block overlay and the
// values the block writes, so it reads none of the fork validator's state and runs after the module is released. A
// returned view owns tx.
func (e *ExecModule) viewTipOverCommittedHead(ctx context.Context, tx kv.TemporalTx, sd *execctx.SharedDomains, blockTx kv.TemporalTx, blockHash common.Hash, number uint64, fn BlockStateView) (bool, func() error, error) {
	changes := sd.GetChangesetByHash(number, blockHash)
	if changes == nil {
		return true, nil, fn(blockTx, tx)
	}
	lastTxNum, err := e.blockReader.TxnumReader().Max(ctx, blockTx, number)
	if err != nil {
		return false, nil, err
	}
	written, err := newBlockEndState(sd, changes, lastTxNum+1)
	if err != nil {
		return false, nil, err
	}
	overlay, err := membatchwithdb.NewMemoryBatch(tx)
	if err != nil {
		return false, nil, err
	}
	if err := sd.BlockOverlay().Flush(ctx, overlay); err != nil {
		overlay.Close()
		return false, nil, err
	}
	overlay.DomainReader = written
	return true, func() error {
		defer tx.Rollback()
		defer overlay.Close()
		return fn(overlay.NewTemporalReadView(tx), tx)
	}, nil
}

// blockEndState holds the values a block leaves in the keys it writes. A read before the block end falls through to
// the database, which holds the parent state.
type blockEndState struct {
	end    uint64
	values [kv.DomainLen]map[string][]byte
}

func newBlockEndState(sd *execctx.SharedDomains, changes *changeset.StateChangeSet, end uint64) (*blockEndState, error) {
	s := &blockEndState{end: end}
	for d := range changes.Diffs {
		diffs := changes.Diffs[d].GetDiffSet()
		s.values[d] = make(map[string][]byte, len(diffs))
		for _, diff := range diffs {
			k := diff.Key[:len(diff.Key)-8]
			v, ok, err := sd.GetAsOf(kv.Domain(d), []byte(k), end)
			if errors.Is(err, kv.ErrInMemHistoryDisabled) {
				v, _, ok = sd.GetLatestFromMemory(kv.Domain(d), []byte(k))
			} else if err != nil {
				return nil, err
			}
			if ok {
				s.values[d][k] = bytes.Clone(v)
			}
		}
	}
	return s, nil
}

func (s *blockEndState) GetAsOf(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	if ts < s.end {
		return nil, false, nil
	}
	v, ok := s.values[name][string(k)]
	return v, ok, nil
}

func (s *blockEndState) HistorySeek(kv.Domain, []byte, uint64) ([]byte, bool, error) {
	return nil, false, nil
}

// viewCommittedBlock reports whether blockHash is committed. It returns a view only when the changesets of the blocks from
// blockHash to the committed head are still kept for reorgs, and reads the parent state by unwinding them in memory.
// It reads one database snapshot, so it needs no module semaphore.
func (e *ExecModule) viewCommittedBlock(ctx context.Context, blockHash common.Hash, number uint64, fn BlockStateView) (committed bool, view func() error, err error) {
	tx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return false, nil, err
	}
	defer closeWithoutView(&view, tx.Rollback)
	executedTo, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, nil, err
	}
	if number == 0 || number > executedTo {
		return false, nil, nil
	}
	if canonical, err := e.blockReader.IsCanonical(ctx, tx, blockHash, number); err != nil || !canonical {
		return false, nil, err
	}
	var unwind *[kv.DomainLen][]kv.DomainEntryDiff
	for n := executedTo; n >= number; n-- {
		hash, err := rawdb.ReadCanonicalHash(tx, n)
		if err != nil {
			return false, nil, err
		}
		diffs, ok, err := changeset.ReadDiffSet(tx, n, hash)
		if err != nil || !ok {
			return true, nil, err
		}
		if unwind == nil {
			unwind = &diffs
			continue
		}
		for d := range diffs {
			unwind[d] = changeset.MergeDiffSets(unwind[d], diffs[d])
		}
	}
	unwindFrom, err := e.blockReader.TxnumReader().Min(ctx, tx, number)
	if err != nil {
		return false, nil, err
	}
	sd, err := execctx.NewSharedDomains(ctx, tx, e.logger, execctx.WithoutSharedBranchCache(), execctx.WithLocalCacheUnwind())
	if sd != nil {
		defer closeWithoutView(&view, sd.Close)
	}
	if err != nil {
		return false, nil, err
	}
	sd.Unwind(unwindFrom, unwind)
	return true, func() error {
		defer tx.Rollback()
		defer sd.Close()
		return fn(tx, sd.AsSnapshotGetter(tx))
	}, nil
}

func (e *ExecModule) purgeBadChain(ctx context.Context, tx kv.RwTx, latestValidHash, headHash common.Hash) error {
	tip, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return err
	}
	if tip == nil {
		// Block is not in tx — nothing to purge.
		return nil
	}

	dbHeadHash := rawdb.ReadHeadBlockHash(tx)

	currentHash := headHash
	currentNumber := *tip
	for currentHash != latestValidHash {
		currentHeader, err := e.getHeader(ctx, tx, currentHash, currentNumber)
		if err != nil {
			return err
		}

		// TODO: find a better way to handle this
		if currentHash == dbHeadHash {
			// We can't delete the head block stored in the database as that is our canonical reconnection point.
			return nil
		}

		rawdb.DeleteHeader(tx, currentHash, currentNumber)
		currentHash = currentHeader.ParentHash
		currentNumber--
	}
	return nil
}

// haltOnInitialSyncFailure reports whether a non-routine initial-sync failure
// must stop the process. Post-sync publication errors do not halt.
func haltOnInitialSyncFailure(err error) bool {
	return err != nil && !isRoutineInitialSyncStop(err) && !isInitialSyncPublicationError(err)
}

// gRPC client cancellation does not unwrap to context.Canceled, but errors.Is
// matches an equivalent status. Keep this target inside the all-causes check.
var grpcContextCanceled = status.FromContextError(context.Canceled).Err()

func isRoutineInitialSyncStop(err error) bool {
	return commonerrors.IsOnly(err, context.Canceled, common.ErrStopped, grpcContextCanceled)
}

func isInitialSyncPublicationError(err error) bool {
	var publicationErr *initialSyncPublicationError
	return errors.As(err, &publicationErr)
}

// processFrozenBlocks binds the state cache after the startup sync: its commit
// advances the state version without publishing to the cache.
func (e *ExecModule) processFrozenBlocks(ctx context.Context, hook *stageloop.Hook) error {
	err := e.pipelineExecutor.ProcessFrozenBlocks(ctx, hook, e.onlySnapDownloadOnStart)
	if e.stateCache != nil {
		initErr := execctx.InitStateCacheVersion(ctx, e.db, e.stateCache)
		if initErr != nil && !commonerrors.IsOnlyCanceled(initErr) {
			e.logger.Warn("[exec] state cache serves RPC only after the first executed block", "err", initErr)
		}
	}
	return err
}

func (e *ExecModule) Start(ctx context.Context, hook *stageloop.Hook) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		if !commonerrors.IsOnlyCanceled(err) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.semaphore.Release(1)
	e.semTake("Start")
	defer e.semDrop()

	if err := e.processFrozenBlocks(ctx, hook); err != nil {
		if !isRoutineInitialSyncStop(err) {
			if isInitialSyncPublicationError(err) {
				e.logger.Error("Could not publish initial sync updates", "err", err)
			} else {
				e.logger.Error("Could not start execution service", "err", err)
			}
		}
		if haltOnInitialSyncFailure(err) {
			e.logger.Error("Initial sync failed — halting process")
			go func() {
				if stopErr := e.stopNode(); stopErr != nil {
					e.logger.Error("Could not stop node on initial sync failure", "err", stopErr)
				}
			}()
			return
		}
	}
	// Notify the fork validator of the current execution height after startup sync.
	if err := e.db.View(ctx, func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		e.forkValidator.NotifyCurrentHeight(progress)
		return nil
	}); err != nil && !commonerrors.IsOnlyCanceled(err) {
		e.logger.Warn("Could not notify fork validator of current height", "err", err)
	}
}

func (e *ExecModule) Ready(ctx context.Context) (bool, error) {
	// setup a timeout for the context to avoid waiting indefinitely
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := <-e.blockReader.Ready(ctxWithTimeout); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Trace("ethereumExecutionModule.Ready: context deadline exceeded")
			return false, nil
		}
		return false, err
	}

	if !e.semaphore.TryAcquire(1) {
		e.logger.Trace("ethereumExecutionModule.Ready: ExecutionStatus_Busy")
		return false, nil
	}
	defer e.semaphore.Release(1)
	e.semTake("Ready")
	defer e.semDrop()
	return true, nil
}

func (e *ExecModule) AddSendersRecovery(blockHash common.Hash, recovery *exec.SendersRecovery) {
	e.readAheader.AddSendersRecovery(blockHash, recovery)
}

func (e *ExecModule) HasBlock(ctx context.Context, blockHash *common.Hash, _ *uint64) (bool, error) {
	if blockHash == nil {
		return false, errors.New("block hash is nil, HasBlock supports lookup by hash only")
	}
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	num, _ := e.blockReader.HeaderNumber(ctx, tx, *blockHash)
	if num == nil {
		return false, nil
	}
	if *num <= e.blockReader.FrozenBlocks() {
		return true, nil
	}
	dbKey := dbutils.HeaderKey(*num, *blockHash)
	has, err := tx.Has(kv.Headers, dbKey)
	if err != nil {
		return false, err
	}
	if !has {
		return false, nil
	}
	has, err = tx.Has(kv.BlockBody, dbKey)
	if err != nil {
		return false, err
	}
	return has, nil
}

// semTake records who holds the module semaphore and since when, so a refusal can say who refused it and
// for how long rather than leaving either to be guessed.
func (e *ExecModule) semTake(name string) {
	e.semAcquires.Add(1)
	e.semHolder.Store(name)
	e.semSince.Store(time.Now().UnixNano())
}

func (e *ExecModule) semDrop() {
	if at := e.semSince.Load(); at > 0 {
		if held := time.Since(time.Unix(0, at)); held > 50*time.Millisecond {
			e.logger.Warn("[sem-hold] module semaphore held", "by", e.semHolderName(), "heldMs", held.Milliseconds())
		}
	}
	e.semHolder.Store("")
	e.semSince.Store(0)
}

func (e *ExecModule) semHolderName() string {
	s, _ := e.semHolder.Load().(string)
	if s == "" {
		return "none-recorded"
	}
	return s
}

// semHeldFor reports how long the current holder has had it, for a refusal to print.
func (e *ExecModule) semHeldFor() int64 {
	at := e.semSince.Load()
	if at == 0 {
		return -1
	}
	return time.Since(time.Unix(0, at)).Milliseconds()
}
