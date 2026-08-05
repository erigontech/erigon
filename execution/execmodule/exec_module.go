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
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
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
	// Read the latest *published* SharedDomains — the most recently executed
	// block's stable leaf snapshot. Deliberately NOT e.currentContext: while
	// an FCU is in progress currentContext is that block's half-written SD,
	// actively mutated by the executing pipeline (the coinbase nonce is
	// already bumped mid-block). A consumer that grabbed currentContext would
	// see a not-yet-final block — e.g. GetTransactionCount and txpool
	// validateTx in the same SubmitTransfer disagreeing by one nonce. The
	// published leaf only advances at block completion, so it is stable.
	var context *execctx.SharedDomains
	if c.publishedSD != nil {
		context = c.publishedSD()
	}

	view := &CacheView{context: context, getter: tx}
	if context != nil {
		view.getter = context.AsGetter(tx)
	}
	return view, nil
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
		v, _, err := c.getter.GetLatest(kv.AccountsDomain, k)
		return v, err
	}
	v, _, err := c.getter.GetLatest(kv.StorageDomain, k)
	return v, err
}
func (c *CacheView) GetCode(k []byte) ([]byte, error) {
	v, _, err := c.getter.GetLatest(kv.CodeDomain, k)
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
	_, _, hasStorage, err := c.getter.HasPrefix(kv.StorageDomain, address[:])
	return hasStorage, err
}

type ExecModule struct {
	bacgroundCtx context.Context
	// Snapshots + MDBX
	blockReader dbservices.FullBlockReader

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
	fcuBackgroundCommit     bool
	onlySnapDownloadOnStart bool
	nextForkActivated       bool
	// gas-weighted EWMA: accumulate gas and time separately so near-empty blocks don't skew the average
	accumGasMgas float64
	accumTimeSec float64

	lock           sync.RWMutex
	currentContext *execctx.SharedDomains
	publishedSD    func() *execctx.SharedDomains // fallback for background commit

	// fgMu guards fgCount + gens; fgIdle is signalled when fgCount reaches zero
	// so the commit worker can run in a foreground-free window (see bg_commit.go).
	fgMu            sync.Mutex
	fgCount         int
	fgIdle          *sync.Cond
	gens            []*commitGen
	uncommittedGens int
	// genEpoch is bumped (under fgMu) whenever the generation set is discarded
	// wholesale (closeAllGens, e.g. a SetHead unwind). A commit worker holding a
	// generation from an earlier epoch skips it rather than committing an
	// already-closed SharedDomains.
	genEpoch uint64
	commitCh chan *commitGen
	// commitWorker lifecycle: commitWorkerStop signals the worker to drain
	// and exit; commitWg tracks it so shutdown (WaitIdle) waits for the
	// worker — and its in-flight commit txs — to finish before DB-close.
	commitWorkerStop chan struct{}
	commitStopOnce   sync.Once
	commitWg         sync.WaitGroup
	// commitFatalOnce ensures a failed durable commit triggers node shutdown at
	// most once, even as the worker drains further generations that also fail.
	commitFatalOnce sync.Once
	// commitPoisoned (guarded by fgMu) is set when a durable commit fails: each
	// generation flushes only its own delta, so a queued descendant must not
	// commit over the hole left by a failed parent while shutdown propagates.
	commitPoisoned bool

	// stateCache is a cache for state data (accounts, storage, code)
	stateCache *cache.StateCache
	// codeStore is the persistent codehash-keyed code cache (in-mem + MDBX backing).
	codeStore   *cache.CodeStore
	readAheader *exec.BlockReadAheader

	stopNode func() error
}

var _ ExecutionModule = (*ExecModule)(nil) // compile-time interface check

func NewExecModule(
	ctx context.Context,
	blockReader dbservices.FullBlockReader,
	db kv.TemporalRwDB,
	pipelineExecutor *PipelineExecutor,
	currentBlockNumber uint64,
	config *chain.Config,
	builderFunc builder.BlockBuilderFunc,
	hook *stageloop.Hook,
	accum *Accumulation,
	stateCache *Cache,
	stateCacheBudget datasize.ByteSize,
	logger log.Logger,
	engine rules.Engine,
	syncCfg ethconfig.Sync,
	fcuBackgroundPrune bool,
	fcuBackgroundCommit bool,
	onlySnapDownloadOnStart bool,
	readAheader *exec.BlockReadAheader,
	stopNode func() error,
) *ExecModule {
	domainCache := newDomainStateCache(stateCacheBudget)
	execctx.GuardAggregatorForCache(db, domainCache)
	var codeStore *cache.CodeStore
	if dbg.UseCodeStore {
		codeStore = cache.NewCodeStore(cache.DefaultCodeStoreMemBytes, cache.DefaultCodeStoreTableBytes)
	}
	forkValidator := newForkValidator(ctx, currentBlockNumber, pipelineExecutor, blockReader, syncCfg.MaxReorgDepth)

	em := &ExecModule{
		blockReader:             blockReader,
		db:                      db,
		logger:                  logger,
		forkValidator:           forkValidator,
		pipelineExecutor:        pipelineExecutor,
		builders:                make(map[uint64]*builder.BlockBuilder),
		builderFunc:             builderFunc,
		config:                  config,
		semaphore:               semaphore.NewWeighted(1),
		hook:                    hook,
		accum:                   accum,
		engine:                  engine,
		balRegenerator:          bal.NewRegenerator(blockReader, engine, logger),
		syncCfg:                 syncCfg,
		bacgroundCtx:            ctx,
		fcuBackgroundPrune:      fcuBackgroundPrune,
		fcuBackgroundCommit:     fcuBackgroundCommit,
		onlySnapDownloadOnStart: onlySnapDownloadOnStart,
		stateCache:              domainCache,
		codeStore:               codeStore,
		readAheader:             readAheader,
		stopNode:                stopNode,
	}

	// Route the read-ahead's prefetches through the published SharedDomains so
	// reads see in-flight tip state and the SD's own read-fill warms the
	// process-global cache the EVM probes — keeping cache population an SD concern.
	if readAheader != nil {
		readAheader.SetPublishedSD(func() *execctx.SharedDomains {
			if em.publishedSD != nil {
				return em.publishedSD()
			}
			return nil
		})
	}

	if stateCache != nil {
		stateCache.execModule = em
	}

	// Start the background-commit worker. It pulls completed
	// generations off commitCh and commits each in a foreground-free
	// window. The buffered channel keeps the foreground FCU's hand-off
	// non-blocking. WaitIdle stops the worker before DB-close.
	em.fgIdle = sync.NewCond(&em.fgMu)
	em.commitCh = make(chan *commitGen, 1024)
	em.commitWorkerStop = make(chan struct{})
	em.commitWg.Add(1)
	go em.commitWorker()

	return em
}

// WaitIdle blocks until any in-flight updateForkChoice goroutine finishes.
// Call before closing the database to avoid waitTxsAllDoneOnClose hangs.
func (e *ExecModule) WaitIdle(ctx context.Context) {
	if e.fgAcquire(ctx) == nil {
		// Foreground is idle. Drain + commit any queued generations (the worker's
		// stop path runs them), then release the generation chain now that no
		// reader can be in flight — drainCommittedGens keeps the newest alive as
		// Events.LatestSD until here.
		e.fgRelease()
		e.stopCommitWorker()
		e.closeAllGens()
		return
	}
	// Timed out with a foreground op still holding the semaphore: closing the
	// generation chain now would wipe SDs that op is still reading through, and
	// parking its later commit in the stopped worker's channel would leak an open
	// roTx and hang chainDB.Close. Poison so that late enqueueCommit rolls back
	// instead of parking, stop the worker, leave the gens for the owning op (the
	// process is exiting anyway), and log loudly.
	e.poisonCommits()
	e.stopCommitWorker()
	e.logger.Warn("WaitIdle: foreground op still active at shutdown; not closing generations")
}

// stopCommitWorker signals the background-commit worker to drain and exit,
// then waits for it. Idempotent.
func (e *ExecModule) stopCommitWorker() {
	e.commitStopOnce.Do(func() { close(e.commitWorkerStop) })
	e.commitWg.Wait()
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

// drainReadAhead blocks until any in-flight block-assembly warmup finishes.
// warmBody is fire-and-forget and fills the shared state cache; if
// it is still running when an unwind bumps the cache epoch, it can fill a
// pre-unwind (dead-fork) value stamped with the post-unwind epoch — IsStale then
// returns false and the stale value is served as canonical (wrong root). Fill
// admission does not cover this direction: an unwind lowers the applied
// frontier, so a pre-unwind view passes. Call before any unwind epoch-bump.
func (e *ExecModule) drainReadAhead() {
	if e.readAheader == nil {
		return
	}
	ctx := e.bacgroundCtx
	if ctx == nil {
		ctx = context.Background()
	}
	e.readAheader.WaitForWarmup(ctx)
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

	e.drainReadAhead()
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
	if !e.fgTryAcquire() {
		e.logger.Trace("ethereumExecutionModule.ValidateChain: ExecutionStatus_Busy")
		return ValidationResult{
			ValidationStatus: ExecutionStatusBusy,
		}, nil
	}
	defer e.fgRelease()

	e.hook.LastNewBlockSeen(blockNumber) // used by eth_syncing
	// currentContext is nil while a background commit holds the previous
	// FCU's SD — guard the access.
	if e.currentContext != nil {
		e.currentContext.ResetPendingUpdates()
	}
	e.forkValidator.ClearWithUnwind()
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
		// currentContext is nil — read block data through the newest
		// in-flight commit generation's overlay when one exists (gate item
		// 2), so the previous FCU's not-yet-committed headers/bodies/TDs
		// are visible; otherwise a plain DB read.
		roTx, err := e.db.BeginTemporalRo(ctx)
		if err != nil {
			return ValidationResult{}, err
		}
		defer roTx.Rollback()
		src := e.overlayBaseFor(roTx)
		header, err = e.blockReader.Header(ctx, src, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		body, err = e.blockReader.BodyWithTransactions(ctx, src, blockHash, blockNumber)
		if err != nil {
			return ValidationResult{}, err
		}
		e.readAheader.AddHeaderAndBody(ctx, e.db, header, body)
		currentBlockNumber = rawdb.ReadCurrentBlockNumber(src)
	}
	if header == nil || body == nil {
		return ValidationResult{
			LatestValidHash:  common.Hash{},
			ValidationStatus: ExecutionStatusMissingSegment,
		}, nil
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

	doms, err := execctx.NewSharedDomains(ctx, roTx, e.logger)
	if err != nil {
		return ValidationResult{}, err
	}
	// Do not defer doms.Close(): on the success path ownership transfers to
	// forkValidator.sharedDom inside ValidatePayload and later phases close it,
	// so we Close explicitly only on the early-return error paths below.
	doms.SetReadCoordinator(e.beginCoordinatedRo)
	doms.SetInMemHistoryReads(inMemHistoryReads)

	// Chain the validation SD to the latest in-memory canonical generation:
	// e.currentContext when present, otherwise the newest in-flight commit
	// generation (the prior FCU cleared currentContext and
	// handed its SD to the background commit). This parent link is the single
	// lookup path: the overlay base below is derived from it so block-data reads
	// and domain-state reads traverse the identical generation chain.
	//
	// The parent link serves two roles:
	//
	//  1. Head-extending payloads read the canonical generation's
	//     not-yet-committed domain state instead of stale MDBX.
	//
	//  2. Fork payloads: unwindToCommonCanonical below must build an unwind
	//     set, and the diffsets of the canonical blocks it unwinds live in
	//     the canonical generation's pastChangesAccumulator — reachable only
	//     through this parent link (GetDiffset chains to the parent). Without
	//     it the unwind silently runs with no unwind set, leaving the
	//     BranchCache unmasked and corrupting the computed root.
	//
	// For a fork payload the parent does NOT shadow the unwound base: once
	// unwindToCommonCanonical has run, doms.mem.unwindChangeset holds every
	// key the unwound canonical blocks touched, and TemporalMemBatch.getLatest
	// resolves those from the unwind set before ever consulting the parent.
	if e.currentContext != nil {
		// Refresh the in-progress top's parent to the current in-flight tip so
		// its chain reaches generations pushed since currentContext was created;
		// otherwise its stale/nil parent leaves in-flight block data unreachable.
		if parent := e.latestGen(); parent != nil {
			e.currentContext.SetParent(parent)
		}
		doms.SetParent(e.currentContext)
	} else if parent := e.latestGen(); parent != nil {
		doms.SetParent(parent)
	}

	// Back the validation overlay by the SD's OWN parent chain so block-data
	// reads cascade through the same generations that domain-state reads do —
	// never a separate, divergent capture. doms has no overlay of its own yet
	// (InitBlockOverlay is next), so OverlayTemporalTx here yields exactly the
	// parent chain.
	valOverlayBase := kv.TemporalTx(roTx)
	if v := doms.OverlayTemporalTx(roTx); v != nil {
		valOverlayBase = v
	}
	if err := doms.InitBlockOverlay(valOverlayBase, roTx.Debug().Dirs().Tmp); err != nil {
		doms.Close()
		return ValidationResult{}, fmt.Errorf("ValidateChain: init block overlay: %w", err)
	}
	var tx kv.TemporalRwTx = doms.BlockOverlay()

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
	doms.SetCodeStore(e.codeStore)
	if err = e.unwindToCommonCanonical(doms, tx, header); err != nil {
		doms.Close()
		return ValidationResult{}, err
	}

	status, lvh, validationError, criticalError := e.forkValidator.ValidatePayload(ctx, doms, tx, header, body.RawBody(), e.logger)
	if criticalError != nil {
		return ValidationResult{}, criticalError
	}

	// No cache invalidation needed on an invalid payload: the state cache is
	// populated only at flush (committed, fork-agnostic state) and this
	// validation path never flushes, so a rejected payload leaves nothing
	// fork-specific in the cache. Reads during validation only add canonical
	// committed bytes. (Cache invalidation happens solely on unwind.)

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
	return result, nil
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
	if err := e.fgAcquire(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			e.logger.Error("Could not start execution service", "err", err)
		}
		return
	}
	defer e.fgRelease()

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
	}); err != nil && !errors.Is(err, context.Canceled) {
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

	if !e.fgTryAcquire() {
		e.logger.Trace("ethereumExecutionModule.Ready: ExecutionStatus_Busy")
		return false, nil
	}
	defer e.fgRelease()
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
