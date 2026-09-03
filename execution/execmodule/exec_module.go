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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

var ErrMissingChainSegment = errors.New("missing chain segment")

var inMemHistoryReads = dbg.EnvBool("ERIGON_IN_MEM_HISTORY", true)

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

// Cache bridges RPC reads to the execution module's in-memory state via
// SharedDomains. When View is called, it grabs the current SD under a read
// lock so that domain reads (accounts, storage, code) see uncommitted writes
// from the pipeline, falling through to the caller's DB tx for committed data.
//
// OnNewBlock is intentionally a no-op: in the embedded (non-remote) rpcdaemon
// the SD is the authoritative source, so the coherent cache's state-tracking
// machinery is unnecessary.
//
// This shim predates SharedDomains' current capabilities and will be simplified
// as part of #19623 (2-cache IBS rationalization) once the StateReader/CacheView
// interfaces stabilize. See also #19798 (event stream extraction) and #19855
// (TransactionState/BlockState separation).
type Cache struct {
	execModule  *ExecModule
	publishedSD func() *execctx.SharedDomains // returns the latest published SD from Events (for background commit)
}

// SetPublishedSD wires the Cache to fall back to the published SD from Events
// when the exec module's currentContext is nil (e.g. during background commit).
func (c *Cache) SetPublishedSD(provider func() *execctx.SharedDomains) {
	c.publishedSD = provider
}

var _ kvcache.Cache = (*Cache)(nil)         // compile-time interface check
var _ kvcache.CacheView = (*CacheView)(nil) // compile-time interface check

func (c *Cache) View(_ context.Context, tx kv.TemporalTx) (kvcache.CacheView, error) {
	var context *execctx.SharedDomains
	if c.execModule != nil {
		c.execModule.lock.RLock()
		context = c.execModule.currentContext
		c.execModule.lock.RUnlock()
	}
	// Fall back to the published SD from Events during background commits
	// (currentContext is nil but the SD is still valid in memory).
	if context == nil && c.publishedSD != nil {
		context = c.publishedSD()
	}

	return &CacheView{context: context, tx: tx}, nil
}
func (c *Cache) OnNewBlock(sc *remoteproto.StateChangeBatch) {}
func (c *Cache) Evict() int                                  { return 0 }
func (c *Cache) Len() int                                    { return 0 }
func (c *Cache) ValidateCurrentRoot(_ context.Context, _ kv.TemporalTx) (*kvcache.CacheValidationResult, error) {
	return &kvcache.CacheValidationResult{Enabled: false}, nil
}

type CacheView struct {
	context *execctx.SharedDomains
	tx      kv.TemporalTx
}

func (c *CacheView) Get(k []byte) ([]byte, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	if len(k) == 20 {
		v, _, err := getter.GetLatest(kv.AccountsDomain, k)
		return v, err
	}
	v, _, err := getter.GetLatest(kv.StorageDomain, k)
	return v, err
}
func (c *CacheView) GetCode(k []byte) ([]byte, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	v, _, err := getter.GetLatest(kv.CodeDomain, k)
	return v, err
}

func (c *CacheView) GetAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	if c.context != nil {
		if len(key) == 20 {
			return c.context.GetAsOf(kv.AccountsDomain, key, ts)
		}
		return c.context.GetAsOf(kv.StorageDomain, key, ts)
	}
	return nil, false, nil
}

func (c *CacheView) HasStorage(address common.Address) (bool, error) {
	var getter kv.TemporalGetter = c.tx
	if c.context != nil {
		getter = c.context.AsGetter(c.tx)
	}
	_, _, hasStorage, err := getter.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}

type ExecModule struct {
	bacgroundCtx context.Context
	// Snapshots + MDBX
	blockReader services.FullBlockReader

	// MDBX database
	db kv.TemporalRwDB // main database
	// semaphore is the module's single mutual-exclusion domain: it guards the
	// pipeline Sync and all FCU state. Ops either TryAcquire and report Busy
	// (retried by the CL) or block, and the background FCU commit/prune
	// goroutines inherit the semaphore, releasing it only when their work is done.
	semaphore        *semaphore.Weighted
	forkValidator    *ForkValidator
	pipelineExecutor *PipelineExecutor

	logger log.Logger
	// Block building
	nextPayloadId  uint64
	lastParameters *builder.Parameters
	builderFunc    builder.BlockBuilderFunc
	builders       map[uint64]*builder.BlockBuilder
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

	// Changes accumulator
	hook  *stageloop.Hook
	accum *Accumulation

	// configuration
	config  *chain.Config
	syncCfg ethconfig.Sync
	// rules engine
	engine rules.Engine

	fcuBackgroundPrune      bool
	fcuBackgroundCommit     bool
	onlySnapDownloadOnStart bool
	nextForkActivated       bool
	// gas-weighted EWMA: accumulate gas and time separately so near-empty blocks don't skew the average
	accumGasMgas float64
	accumTimeSec float64

	lock           sync.RWMutex
	currentContext *execctx.SharedDomains
	publishedSD    func() *execctx.SharedDomains // fallback for background commit

	// stateCache is a cache for state data (accounts, storage, code)
	stateCache  *cache.StateCache
	readAheader *exec.BlockReadAheader

	stopNode func() error
}

var _ ExecutionModule = (*ExecModule)(nil) // compile-time interface check

func NewExecModule(
	ctx context.Context,
	blockReader services.FullBlockReader,
	db kv.TemporalRwDB,
	pipelineExecutor *PipelineExecutor,
	currentBlockNumber uint64,
	config *chain.Config,
	builderFunc builder.BlockBuilderFunc,
	hook *stageloop.Hook,
	accum *Accumulation,
	stateCache *Cache,
	logger log.Logger,
	engine rules.Engine,
	syncCfg ethconfig.Sync,
	fcuBackgroundPrune bool,
	fcuBackgroundCommit bool,
	onlySnapDownloadOnStart bool,
	readAheader *exec.BlockReadAheader,
	stopNode func() error,
) *ExecModule {
	domainCache := cache.NewDefaultStateCache()
	forkValidator := newForkValidator(ctx, currentBlockNumber, pipelineExecutor, blockReader, syncCfg.MaxReorgDepth)

	em := &ExecModule{
		blockReader:             blockReader,
		db:                      db,
		logger:                  logger,
		forkValidator:           forkValidator,
		pipelineExecutor:        pipelineExecutor,
		builders:                make(map[uint64]*builder.BlockBuilder),
		preconfirmedBlocks:      make(map[uint64]*types.BlockWithReceipts),
		pendingBlock:         make(map[uint64]*builder.Parameters),
		preconfirmedByParent:    make(map[common.Hash]*types.BlockWithReceipts),
		sealedByHash:            make(map[common.Hash]*types.Header),
		builderFunc:             builderFunc,
		config:                  config,
		semaphore:               semaphore.NewWeighted(1),
		execCost:                newExecCostWindow(64),
		hook:                    hook,
		accum:                   accum,
		engine:                  engine,
		syncCfg:                 syncCfg,
		bacgroundCtx:            ctx,
		fcuBackgroundPrune:      fcuBackgroundPrune,
		fcuBackgroundCommit:     fcuBackgroundCommit,
		onlySnapDownloadOnStart: onlySnapDownloadOnStart,
		stateCache:              domainCache,
		readAheader:             readAheader,
		stopNode:                stopNode,
	}

	if stateCache != nil {
		stateCache.execModule = em
	}
	return em
}

// WaitIdle blocks until any in-flight updateForkChoice goroutine finishes.
// Call before closing the database to avoid waitTxsAllDoneOnClose hangs.
func (e *ExecModule) WaitIdle(ctx context.Context) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return // context cancelled — best effort
	}
	e.semaphore.Release(1)
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
// when currentContext is nil (e.g. during background commit).
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

func (e *ExecModule) getBody(ctx context.Context, tx kv.Tx, blockHash common.Hash, blockNumber uint64) (*types.Body, error) {
	if e.blockReader == nil {
		body, _, _ := rawdb.ReadBody(tx, blockHash, blockNumber)
		return body, nil
	}
	return e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
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

func (e *ExecModule) unwindToCommonCanonical(sd *execctx.SharedDomains, tx kv.TemporalRwTx, header *types.Header) error {
	currentHeader := header
	for isCanonical, err := e.isCanonicalHash(e.bacgroundCtx, tx, currentHeader.Hash()); !isCanonical && err == nil; isCanonical, err = e.isCanonicalHash(e.bacgroundCtx, tx, currentHeader.Hash()) {
		parentBlockHash, parentBlockNum := currentHeader.ParentHash, currentHeader.Number.Uint64()-1
		currentHeader, err = e.getHeader(e.bacgroundCtx, tx, parentBlockHash, parentBlockNum)
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

	if err := e.hook.BeforeRun(tx, true); err != nil {
		return err
	}

	if err := e.pipelineExecutor.UnwindTo(unwindPoint, stagedsync.ExecUnwind, tx); err != nil {
		return err
	}
	if err := e.pipelineExecutor.RunUnwind(sd, tx); err != nil {
		return err
	}
	return nil
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

func (e *ExecModule) ValidateChain(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
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
	if blockNumber <= e.forkValidator.CommittedHeight() {
		return ValidationResult{ValidationStatus: ExecutionStatusSuccess, LatestValidHash: blockHash}, nil
	}
	e.pendingBlockMu.Lock()
	sealed := e.sealedByHash[blockHash]
	e.pendingBlockMu.Unlock()
	if sealed != nil && sealed.Number.Uint64() == blockNumber {
		e.logger.Info("[execmodule] ValidateChain: accepting locally-sealed block (no re-exec)",
			"number", blockNumber, "hash", blockHash, "root", sealed.Root)
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
	return e.validateChainLocked(ctx, blockHash, blockNumber)
}

// validateChainLocked is ValidateChain's body with the exec-module semaphore ALREADY held. Split out so
// the preconfirm-assemble path (AssembleBlock holds the semaphore) can drive the same close/seal without
// a re-entrant TryAcquire that would return Busy. Callers MUST hold e.semaphore.
func (e *ExecModule) validateChainLocked(ctx context.Context, blockHash common.Hash, blockNumber uint64) (ValidationResult, error) {
	e.hook.LastNewBlockSeen(blockNumber) // used by eth_syncing
	e.currentContext.ResetPendingUpdates()
	e.logger.Debug("[execmodule] validating chain", "number", blockNumber, "hash", blockHash)
	var (
		header             *types.Header
		body               *types.Body
		currentBlockNumber *uint64
		err                error
	)
	// Read header/body from the block overlay on currentContext if available
	// (block data written by InsertBlocks hasn't been flushed to DB yet),
	// falling back to a plain DB read otherwise.
	if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
		overlay := e.currentContext.BlockOverlay()
		roTx, err := e.db.BeginTemporalRo(ctx)
		if err != nil {
			return ValidationResult{}, err
		}
		defer roTx.Rollback()
		overlay.UpdateTxn(roTx)
		header, err = e.blockReader.Header(ctx, overlay, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		body, err = e.blockReader.BodyWithTransactions(ctx, overlay, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		e.readAheader.AddHeaderAndBody(ctx, e.db, header, body)
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(overlay)
	} else {
		if err := e.db.View(ctx, func(tx kv.Tx) error {
			header, err = e.blockReader.Header(ctx, tx, blockHash, blockNumber)
			if err != nil {
				return err
			}

			body, err = e.blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNumber)
			if err != nil {
				return err
			}
			e.readAheader.AddHeaderAndBody(ctx, e.db, header, body)
			currentBlockNumber = rawdb.ReadCurrentBlockNumber(tx)
			return nil
		}); err != nil {
			return ValidationResult{}, err
		}
	}
	if header == nil || body == nil {
		return ValidationResult{
			LatestValidHash:  common.Hash{},
			ValidationStatus: ExecutionStatusMissingSegment,
		}, nil
	}

	// Flashblock detection: check whether this block is a prefix-extension
	// of an in-progress flashblock before clearing the fork validator state.
	// body.Transactions is needed for the prefix comparison.
	flashUpdate := e.forkValidator.CheckFlashblockUpdate(blockNumber, body.Transactions)
	if flashUpdate.IsUpdate {
		e.logger.Debug("[execmodule] flashblock update detected",
			"number", blockNumber, "hash", blockHash,
			"prevTxs", flashUpdate.PrefixLen,
			"newTxs", len(body.Transactions))
	} else {
		e.forkValidator.ClearWithUnwind()
	}

	if math.AbsoluteDifference(*currentBlockNumber, blockNumber) >= e.syncCfg.MaxReorgDepth {
		return ValidationResult{
			ValidationStatus: ExecutionStatusTooFarAway,
			LatestValidHash:  common.Hash{},
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

	// A flashblock CLOSE reuses the maintained accumulating SD (fv.sharedDom) directly as the
	// validate SD — "the preexec SD becomes the validate SD." No fresh SD, no VersionedIO transfer:
	// the block's whole body already executed into THIS SD across the PreExecute rounds, and its
	// commitment trie has folded each round's diff. The close unsets FlashblockAccumulating (so the
	// block-end task runs engine.Finalize) and resumes execution PAST the full prefix (SetPreExecStart
	// at the block-end position) so no body tx re-executes — only the block-end/seal runs. The final
	// ComputeCommitment folds any remaining diff and yields the SAME root a one-shot full execution
	// would (the trie is a function of the final key→value set, not the fold order/splitting).
	reuseClose := flashUpdate.IsUpdate && flashUpdate.SD != nil

	var doms *execctx.SharedDomains
	var tx kv.TemporalRwTx
	if reuseClose {
		doms = flashUpdate.SD
		doms.BlockOverlay().UpdateTxn(roTx)
		doms.SetInMemHistoryReads(true)
		doms.SetFlashblockAccumulating(false)
		// SINGLE-CHAIN ([[consensus_advance_untested_regression]]): resolve this block's Min txNum through
		// the block OVERLAY, not the base roTx. On a frontier block the predecessor's MaxTxNum index entry
		// lives only in the (copied) overlay — never the committed DB — so Min via roTx computes off stale
		// committed state (Max(prev) misses the frontier predecessor) and lands the resume point inside the
		// PRIOR block. The overlay merges the copied index → Min returns THIS block's true start.
		if minTxNum, merr := e.blockReader.TxnumReader().Min(ctx, doms.BlockOverlay(), blockNumber); merr == nil {
			// minTxNum is the block-START system txNum. The maintained SD already ran block-start (and the
			// PrefixLen already-executed regular txs) during the pre-exec rounds, so the close must resume PAST
			// both: +1 skips the block-start txNum, +PrefixLen skips the executed regular prefix. Omitting the +1
			// made an EMPTY block (PrefixLen==0) resume AT block-start and RE-RUN it; re-touching the block-start
			// system keys re-folds the commitment and, for some account/trie layouts, yields a root that diverges
			// from the pre-exec (open) root — the empty-successor seal flake. (For a non-empty block PrefixLen>=1
			// pushed the resume past block-start, so the bug only surfaced on empty blocks.)
			doms.SetPreExecStart(minTxNum + 1 + uint64(flashUpdate.PrefixLen))
			defer doms.ClearPreExecStart()
		}
		tx = doms.BlockOverlay()
	} else {
		doms, err = execctx.NewSharedDomains(ctx, roTx, e.logger)
		if err != nil {
			return ValidationResult{}, err
		}
		// NOTE: do NOT defer doms.Close(). On the success path, ownership of
		// doms transfers to forkValidator.sharedDom inside ValidatePayload —
		// later phases (MergeExtendingFork, NotifyCurrentHeight) close it.
		// We Close explicitly only on the early-return error paths below.
		doms.SetInMemHistoryReads(inMemHistoryReads)

		if err := doms.InitBlockOverlay(roTx, roTx.Debug().Dirs().Tmp); err != nil {
			doms.Close()
			return ValidationResult{}, fmt.Errorf("ValidateChain: init block overlay: %w", err)
		}
		tx = doms.BlockOverlay()

		// Chain to the canonical generation so head-extending reads and fork unwind sets resolve via the parent link.
		if e.currentContext != nil {
			doms.SetParent(e.currentContext)
		}
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
			if !reuseClose {
				doms.Close()
			}
			return ValidationResult{}, fmt.Errorf("ValidateChain: flush overlay to validation tx: %w", err)
		}
	}

	// Set state cache in SharedDomains for use during state reading
	doms.SetStateCache(e.stateCache)
	if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
		if !reuseClose {
			doms.Close()
		}
		return ValidationResult{}, err
	}

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}

	// Record tx hashes for flashblock prefix detection on subsequent updates.
	if status == engine_types.ValidStatus && body != nil {
		e.forkValidator.RecordFlashblockTxHashes(body.Transactions)

		// Intra-block notification: tell subscribers these txs are validated.
		// Noop when no subscribers (standard Ethereum). In flashblock mode
		// a subscriber removes the txs from the txpool.
		if dispatcher := e.pipelineExecutor.Dispatcher(); dispatcher != nil && len(body.Transactions) > 0 {
			txHashes := make([]common.Hash, len(body.Transactions))
			for i, tx := range body.Transactions {
				txHashes[i] = tx.Hash()
			}
			dispatcher.OnTransactionValidated(txHashes)
		}
	}

	// Clear state cache on invalid block
	isInvalid := status == engine_types.InvalidStatus || status == engine_types.InvalidBlockHashStatus || validationError != nil
	if e.stateCache != nil && isInvalid {
		e.stateCache.ClearWithHash(header.ParentHash)
	}

	// Validation tx is the SD's BlockOverlay; defer doms.Close() above handles
	// its rollback. By design we do not persist validation-run writes — there
	// is no Flush/Commit on this path.

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
		}
		e.logger.Warn("ethereumExecutionModule.ValidateChain: chain is invalid", "hash", blockHash)
		validationStatus = ExecutionStatusBadBlock
		// Discard the block overlay — it may contain the bad block's data.
		if e.currentContext != nil && e.currentContext.BlockOverlay() != nil {
			e.currentContext.BlockOverlay().Close()
		}
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
	// Surface the sealed output side ONLY on a successful close. On a FAILED validation the fork validator
	// CLOSES the SD, so GetCommitmentContext() returns nil and reading the trie would nil-deref and crash
	// the node — a bad block must return BadBlock, not panic. Read the sealed root off the validated SD's
	// commitment trie (same pattern as preexecute.go), then seal the OUTPUT-SIDE header fields off the
	// accumulated flashblock receipts (zero re-exec): block-end ran, so these are final. Each round used a
	// per-round gas pool, so restamp CumulativeGasUsed as the running sum across the full body (else GasUsed
	// AND ReceiptHash diverge from a one-shot full execution).
	if validationStatus == ExecutionStatusSuccess {
		if cc := doms.GetCommitmentContext(); cc != nil {
			if root, rerr := cc.Trie().RootHash(); rerr == nil && len(root) > 0 {
				result.ComputedRoot = common.BytesToHash(root)
			}
		}
		// FRONTIER POSITION SAVE ([[consensus_advance_untested_regression]], user 2026-08-24 "parent view"):
		// the fork-validation close reads the root off the folded trie but — unlike the normal FCU path
		// (computeAndCheckCommitmentV3) — never persists the commitment "state" marker (KeyCommitmentState)
		// into THIS SD. On the frontier chain that SD is PARKED as the successor block's read-through parent,
		// and BOTH the successor's SeekCommitment AND the FCU merge (otherTxNum) read block N's position from
		// it. Without the marker they fall through to the predecessor's position → the successor builds on the
		// wrong block and the merge is a zero-diff (the DB position never advances). Persist it here (trie is
		// already folded → ComputeCommitment hits the updateCount==0 save-only path, cheap). reuseClose only:
		// the frontier flow parks this SD; a plain fork-validation SD is discarded, so the write is harmless
		// there but unnecessary.
		if reuseClose {
			bn := header.Number.Uint64()
			if blockTxNum, terr := e.blockReader.TxnumReader().Max(ctx, tx, bn); terr == nil {
				if _, cerr := doms.ComputeCommitment(ctx, tx, true, bn, blockTxNum, "frontier-close", nil); cerr != nil {
					return ValidationResult{}, fmt.Errorf("frontier close: save commitment state: %w", cerr)
				}
			}
		}
		// Seal the OUTPUT-SIDE header fields off the accumulated flashblock receipts (zero re-exec).
		// UNCONDITIONAL, including the EMPTY (0-tx heartbeat) block: DeriveSha(nil) = EmptyRootHash
		// (0x56e81f17…) and CreateBloom(nil) = the zero bloom — exactly what a full re-execution +
		// BlockPostValidation computes for an empty block. Gating this on len>0 left ReceiptHash at its
		// ZERO value for an empty block, so the sealed header carried ReceiptHash=0x00.. and re-validation
		// rejected it ("receiptHash mismatch: 56e81f17… != 0000…"). That is the cold-boot empty heartbeat
		// block-1 that froze the DAG-L2 at block 0 (BlockAdvance only exercised NON-empty blocks, so it
		// never caught this). GasUsed=0 and an empty bloom are the correct output side for a 0-tx block.
		fbReceipts := doms.FlashblockReceipts()
		var cum uint64
		for _, r := range fbReceipts {
			cum += r.GasUsed
			r.CumulativeGasUsed = cum
		}
		result.FlashblockReceiptCount = len(fbReceipts)
		result.GasUsed = cum
		result.ReceiptHash = types.DeriveSha(fbReceipts)
		result.Bloom = types.CreateBloom(fbReceipts)
	}
	// The CLOSE is pure COMPUTE (assemble): it runs block-end over the maintained SD and returns the
	// sealed output side, but does NOT write the sealed block or re-key the fork validator. Writing the
	// real-root header H1 into the overlay + re-pointing the extending fork is the newPayload step —
	// IngestSealedFlashblock — so a proposer and its peers share ONE ingest path (getPayload assembles,
	// newPayload ingests, FCU canonicalises).
	return result, nil
}

// GetPreExecutedBody returns this node's locally pre-executed in-progress flashblock body (the txs it
// accumulated across PreExecute rounds from the DAG) plus the deferred in-progress hash and number. The
// newPayload for a DAG-preconfirmed flashblock is body-LESS — it carries only the sealed HEADER — so each
// node supplies the body from HERE rather than from the wire (the transmission optimization: the body was
// already delivered as DAG tx hashes and pre-executed). Empty extending fork ⇒ error.
func (e *ExecModule) GetPreExecutedBody(ctx context.Context) (*types.RawBody, common.Hash, uint64, error) {
	oldHash, number, sd := e.forkValidator.ExtendingFork()
	if sd == nil || oldHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: no in-progress flashblock")
	}
	if e.currentContext == nil || e.currentContext.BlockOverlay() == nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: no block overlay")
	}
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: begin ro: %w", err)
	}
	defer roTx.Rollback()
	ov := e.currentContext.BlockOverlay()
	ov.UpdateTxn(roTx)
	body, err := e.blockReader.BodyWithTransactions(ctx, ov, oldHash, number)
	if err != nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: read body: %w", err)
	}
	if body == nil {
		return nil, common.Hash{}, 0, fmt.Errorf("GetPreExecutedBody: body %x not found", oldHash)
	}
	return body.RawBody(), oldHash, number, nil
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
	if newHash == oldHash {
		return nil // already sealed in place
	}

	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("IngestSealedFlashblock: begin ro: %w", err)
	}
	defer roTx.Rollback()

	ov := e.currentContext.BlockOverlay()
	ov.UpdateTxn(roTx)

	td, err := rawdb.ReadTd(ov, oldHash, number)
	if err != nil {
		return fmt.Errorf("IngestSealedFlashblock: read TD: %w", err)
	}
	if td == nil {
		td = new(uint256.Int)
	}
	if err := rawdb.WriteHeader(ov, sealed); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: write header: %w", err)
	}
	if err := rawdb.WriteTd(ov, newHash, number, *td); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: write TD: %w", err)
	}
	if _, err := rawdb.WriteRawBodyIfNotExists(ov, newHash, number, body); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: write body: %w", err)
	}
	// Remove the DEFERRED (zero-output) in-progress block so the post-newPayload state is IDENTICAL to a
	// normal newPayload: exactly ONE block at this height (the sealed H1). The deferred ibHash is a scratch
	// artifact of the flashblock accumulation — leaving it would strand an orphan header/body/TD at height N.
	rawdb.DeleteHeader(ov, oldHash, number)
	rawdb.DeleteBody(ov, oldHash, number)
	if err := ov.Delete(kv.HeaderTD, dbutils.HeaderKey(number, oldHash)); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: delete deferred TD: %w", err)
	}
	if err := e.forkValidator.SealInPlace(oldHash, newHash, number); err != nil {
		return fmt.Errorf("IngestSealedFlashblock: seal in place: %w", err)
	}
	// Record the sealed block so newPayload/ValidateChain ACCEPTS it (this node produced+validated it once on
	// the frontier) instead of re-executing it on a fresh SD against lagging canonical state.
	e.pendingBlockMu.Lock()
	e.sealedByHash[newHash] = sealed
	e.pendingBlockMu.Unlock()
	e.logger.Debug("[execmodule] flashblock sealed (newPayload ingest)",
		"number", number, "deferredHash", oldHash, "sealedHash", newHash, "root", sealed.Root)
	return nil
}

func (e *ExecModule) purgeBadChain(ctx context.Context, tx kv.RwTx, latestValidHash, headHash common.Hash) error {
	tip, err := e.blockReader.HeaderNumber(ctx, tx, headHash)
	if err != nil {
		return err
	}
	if tip == nil {
		// Block only existed in the overlay (not yet committed to DB) — nothing to purge.
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

func (e *ExecModule) Start(ctx context.Context, hook *stageloop.Hook) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.semaphore.Release(1)

	if err := e.pipelineExecutor.ProcessFrozenBlocks(ctx, hook, e.onlySnapDownloadOnStart); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		// During parallel execution, an invalid block in initial sync (ProcessFrozenBlocks)
		// is unrecoverable: the parallel executor cannot unwind and retrying will hit the
		// same block forever, pushing Caplin's backward target further back.
		// Exit the process so the operator can investigate.
		if dbg.Exec3Parallel && errors.Is(err, rules.ErrInvalidBlock) {
			e.logger.Error("Invalid block during parallel initial sync — halting process")
			go func() {
				if stopErr := e.stopNode(); stopErr != nil {
					e.logger.Error("Could not stop node on invalid block", "err", stopErr)
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
	}); err != nil {
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
	return true, nil
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
	has, err := tx.Has(kv.Headers, dbutils.HeaderKey(*num, *blockHash))
	if err != nil {
		return false, err
	}
	if !has {
		return false, nil
	}
	has, err = tx.Has(kv.BlockBody, dbutils.HeaderKey(*num, *blockHash))
	if err != nil {
		return false, err
	}
	return has, nil
}
