package commitmentdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	mxCommitmentRunning = metrics.GetOrCreateGauge("domain_running_commitment")
	mxCommitmentTook    = metrics.GetOrCreateSummary("domain_commitment_took")

	// logCacheFingerprint, when true, makes ComputeCommitment emit a
	// "[cache-fp]" log line at end-of-block with (block, root, fp,
	// divergences). Diff two builds' logs offline to localise the first
	// block at which their BranchCache states diverge. Off by default —
	// gate via env BRANCH_CACHE_FINGERPRINT=true.
	logCacheFingerprint = dbg.EnvBool("BRANCH_CACHE_FINGERPRINT", false)
)

type sd interface {
	SetTxNum(blockNum uint64)
	AsGetter(tx kv.TemporalTx) kv.TemporalGetter
	AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel
	StepSize() uint64
	Trace() bool
	CommitmentCapture() bool

	// ProbeReadLayers samples sd.mem, parent.mem and tx-direct (MDBX)
	// for one key. Used by the BranchCache divergence-detection probe
	// to localise which state layer holds bytes that diverge from
	// cache. Read-only.
	ProbeReadLayers(domain kv.Domain, tx kv.TemporalTx, key []byte) (mem, parentMem, mdbx []byte, memOk, parentOk bool)

	// Metrics exposes the per-SD DomainMetrics so callers can read
	// per-domain (cache, db, file) read counters. Used by the
	// cache-fp log line to break the aggregate `files=N` count down
	// per domain (Storage value loads vs Commitment branch reads
	// vs Account loads).
	Metrics() *changeset.DomainMetrics
}

type SharedDomainsCommitmentContext struct {
	sharedDomains sd
	updates       *commitment.Updates
	patriciaTrie  commitment.Trie
	justRestored  atomic.Bool // set to true when commitment trie was just restored from snapshot
	trace         bool
	stateReader   StateReader
	paraTrieDB    kv.TemporalRoDB // DB used for para trie and/or parallel trie warmup
	trieWarmup    bool            // toggle for parallel trie warmup of MDBX page cache during commitment
	tmpDir        string          // temp directory for ETL collectors

	// deferCommitmentUpdates when true, deferred branch updates are stored as a pending update
	// instead of being applied inline after Process(). Used during fork validation.
	deferCommitmentUpdates bool
	// pendingUpdate stores a single deferred branch update to be flushed at the next ComputeCommitment call.
	pendingUpdate *commitment.PendingCommitmentUpdate
}

// SetStateReader can be used to set a custom state reader (otherwise the default one is set in SharedDomainsCommitmentContext.trieContext).
func (sdc *SharedDomainsCommitmentContext) SetStateReader(stateReader StateReader) {
	sdc.stateReader = stateReader
}

// EnableTrieWarmup enables parallel warmup of MDBX page cache during commitment.
// When set, ComputeCommitment will pre-fetch Branch data in parallel before processing.
// It requires a DB to be set by calling EnableParaTrieDB
func (sdc *SharedDomainsCommitmentContext) EnableTrieWarmup(trieWarmup bool) {
	sdc.trieWarmup = trieWarmup
}

func (sdc *SharedDomainsCommitmentContext) EnableParaTrieDB(db kv.TemporalRoDB) {
	sdc.paraTrieDB = db
}

func (sdc *SharedDomainsCommitmentContext) SetDeferBranchUpdates(deferBranchUpdates bool) {
	if sdc.patriciaTrie.Variant() == commitment.VariantHexPatriciaTrie {
		sdc.patriciaTrie.(*commitment.HexPatriciaHashed).SetDeferBranchUpdates(deferBranchUpdates)
	}
}

// SetDeferCommitmentUpdates enables or disables deferred commitment updates.
// When enabled, branch updates from Process() are stored as a pending update
// instead of being applied inline. Used during fork validation where the update is
// flushed later via FlushPendingUpdate.
func (sdc *SharedDomainsCommitmentContext) SetDeferCommitmentUpdates(defer_ bool) {
	sdc.deferCommitmentUpdates = defer_
}

// TakePendingUpdate returns the pending update and clears the field.
// Caller takes ownership of the returned value.
func (sdc *SharedDomainsCommitmentContext) TakePendingUpdate() *commitment.PendingCommitmentUpdate {
	upd := sdc.pendingUpdate
	sdc.pendingUpdate = nil
	return upd
}

// SetPendingUpdate sets the pending update (used during Merge to transfer from another context).
func (sdc *SharedDomainsCommitmentContext) SetPendingUpdate(upd *commitment.PendingCommitmentUpdate) {
	sdc.pendingUpdate = upd
}

// ResetPendingUpdates clears the pending update, returning deferred updates to the pool.
func (sdc *SharedDomainsCommitmentContext) ResetPendingUpdates() {
	if sdc.pendingUpdate != nil {
		sdc.pendingUpdate.Clear()
		sdc.pendingUpdate = nil
	}
}

// HasPendingUpdate returns true if there is a pending update to flush.
func (sdc *SharedDomainsCommitmentContext) HasPendingUpdate() bool {
	return sdc.pendingUpdate != nil
}

// flushPendingUpdate applies the pending commitment update using the given putter.
// The update is written with its original TxNum. Called at the start of ComputeCommitment
// to ensure the previously deferred update is applied before processing new ones.
func (sdc *SharedDomainsCommitmentContext) flushPendingUpdate(putter kv.TemporalPutDel) error {
	upd := sdc.pendingUpdate
	putBranch := func(prefix, data, prevData []byte) error {
		return putter.DomainPut(kv.CommitmentDomain, prefix, data, upd.TxNum, prevData)
	}
	if _, err := commitment.ApplyDeferredBranchUpdates(upd.Deferred, runtime.NumCPU(), putBranch); err != nil {
		return err
	}
	upd.Clear()
	sdc.pendingUpdate = nil
	return nil
}

// SetHistoryStateReader sets the state reader to read *full* historical state at specified txNum.
func (sdc *SharedDomainsCommitmentContext) SetHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewHistoryStateReader(roTx, limitReadAsOfTxNum))
}

func (sdc *SharedDomainsCommitmentContext) SetCustomHistoryStateReader(stateReader StateReader) {
	sdc.SetStateReader(stateReader)
}

func (sdc *SharedDomainsCommitmentContext) SetTrace(trace bool) {
	sdc.trace = trace
	sdc.patriciaTrie.SetTrace(trace)
}

// SetCapture enables/disables trie operation capture for diagnosis.
func (sdc *SharedDomainsCommitmentContext) SetCapture(capture []string) {
	sdc.patriciaTrie.SetCapture(capture)
}

// GetUpdates returns the current updates buffer. Used by the commitment
// calculator to swap in its accumulated touches.
func (sdc *SharedDomainsCommitmentContext) GetUpdates() *commitment.Updates {
	return sdc.updates
}

// SetUpdates replaces the updates buffer. Used by the commitment calculator
// to install its accumulated touches before calling ComputeCommitment.
func (sdc *SharedDomainsCommitmentContext) SetUpdates(updates *commitment.Updates) {
	sdc.updates = updates
}

func (sdc *SharedDomainsCommitmentContext) EnableCsvMetrics(filePathPrefix string) {
	sdc.patriciaTrie.EnableCsvMetrics(filePathPrefix)
}

// NewSharedDomainsCommitmentContext constructs a per-SharedDomains
// commitment context. branchCache is the aggregator-scope cache shared
// across all SDs derived from the same Aggregator; pass nil from test
// helpers that don't have an aggregator (a fresh per-init cache will be
// created inside InitializeTrieAndUpdates as a fallback).
func NewSharedDomainsCommitmentContext(sd sd, mode commitment.Mode, trieVariant commitment.TrieVariant, tmpDir string, branchCache *commitment.BranchCache) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
		tmpDir:        tmpDir,
	}
	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, tmpDir, branchCache)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) trieContext(tx kv.TemporalTx, blockNum, txNum uint64) *TrieContext {
	mainTtx := &TrieContext{
		getter:   sdc.sharedDomains.AsGetter(tx),
		putter:   sdc.sharedDomains.AsPutDel(tx),
		stepSize: sdc.sharedDomains.StepSize(),
		txNum:    txNum,
		blockNum: blockNum,
		probeSd:  sdc.sharedDomains,
		probeTx:  tx,
	}
	if sdc.stateReader != nil {
		mainTtx.stateReader = sdc.stateReader.Clone(tx)
	} else {
		mainTtx.stateReader = NewLatestStateReader(tx, sdc.sharedDomains)
	}
	sdc.patriciaTrie.ResetContext(mainTtx)
	return mainTtx
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
	sdc.patriciaTrie.Release()
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
}

func (sdc *SharedDomainsCommitmentContext) GetCapture(truncate bool) []string {
	return sdc.patriciaTrie.GetCapture(truncate)
}

func (sdc *SharedDomainsCommitmentContext) ClearRam() {
	sdc.updates.Reset()
	sdc.Reset()
}

func (sdc *SharedDomainsCommitmentContext) KeysCount() uint64 {
	return sdc.updates.Size()
}

func (sdc *SharedDomainsCommitmentContext) Trie() commitment.Trie {
	return sdc.patriciaTrie
}

// TouchKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (sdc *SharedDomainsCommitmentContext) TouchKey(d kv.Domain, key string, val []byte) {
	if sdc.updates.Mode() == commitment.ModeDisabled {
		return
	}
	if dbg.TraceTouchKey {
		fmt.Printf("TOUCHKEY %s key=%x val=%x\n", d, key, val)
	}

	switch d {
	case kv.AccountsDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchAccount)
	case kv.CodeDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchCode)
	case kv.StorageDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.TouchStorage)
	//case kv.CommitmentDomain, kv.ReceiptDomain:
	default:
		//panic(fmt.Errorf("TouchKey: unknown domain %s", d))
	}
}

// TouchHashedKey touches a hashed key which can be anywhere from 1 to 128 nibbles
// This can be used to generate witnesses for intermediate trie nodes
func (sdc *SharedDomainsCommitmentContext) TouchHashedKey(hashedKey []byte) {
	if sdc.updates.Mode() == commitment.ModeDisabled {
		return
	}
	sdc.updates.TouchHashedKey(hashedKey)
}

func (sdc *SharedDomainsCommitmentContext) Witness(ctx context.Context, codeReads map[common.Hash]witnesstypes.CodeWithHash, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, codeReads, logPrefix)
	}

	return nil, nil, errors.New("shared domains commitment context doesn't have HexPatriciaHashed")
}

// SetCollapseTracer sets a callback that will be invoked when a node collapse occurs
// during commitment calculation. This is used by witness generation to capture paths
// to HashNodes that need resolution when a FullNode is reduced to a single child.
func (sdc *SharedDomainsCommitmentContext) SetCollapseTracer(tracer commitment.CollapseTracer) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		hexPatriciaHashed.SetCollapseTracer(tracer)
	}
}

// ComputeCommitment Evaluates commitment for gathered updates.
// If warmup was set via EnableTrieWarmup, pre-warms MDBX page cache by reading Branch data in parallel before processing.
// ComputeCommitment should normally be called via SharedDomains.ComputeCommitment,
// which flushes pending deferred updates first. Direct callers must ensure
// pendingUpdate is nil (i.e. deferred mode is not active or was flushed).
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveState bool, blockNum uint64, txNum uint64, logPrefix string, onProgress func(*commitment.CommitProgress)) (rootHash []byte, err error) {
	if sdc.pendingUpdate != nil {
		panic("sdCtx.ComputeCommitment called directly with non-nil pendingUpdate; use SharedDomains.ComputeCommitment wrapper instead")
	}
	if dbg.KVReadLevelledMetrics {
		mxCommitmentRunning.Inc()
		defer mxCommitmentRunning.Dec()
		defer mxCommitmentTook.ObserveDuration(time.Now())
	}

	updateCount := sdc.updates.Size()
	start := time.Now()
	defer func() {
		took := time.Since(start)
		var keysPerSec uint64
		if took > 0 {
			keysPerSec = uint64(float64(updateCount) / took.Seconds())
		}
		log.Debug("[commitment] processed", "block", blockNum, "txNum", txNum, "keys", common.PrettyCounter(updateCount), "keys/s", common.PrettyCounter(keysPerSec), "mode", sdc.updates.Mode(), "spent", took, "rootHash", hex.EncodeToString(rootHash))

		// Per-block BranchCache fingerprint — gated by BRANCH_CACHE_FINGERPRINT
		// env. Emit a single log line of (block, root, cache_fp, divergences)
		// so two builds running the same workload can be diffed offline to
		// localise the first block at which their caches diverge. See
		// commitment.BranchCache.Fingerprint for the hash semantics.
		// took + keys exposed at Info so the calculator's per-block compute
		// time is visible without enabling debug logs (used by the
		// snapshot-vs-mdbx perf-equivalence investigation to attribute
		// the gap inside newPayload).
		if logCacheFingerprint {
			if hph, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
				if bc := hph.BranchCache(); bc != nil {
					// Skip/load/reset counters are process-cumulative; the
					// per-block delta requires subtracting consecutive lines.
					// Used to answer: "what fraction of computeCellHash calls
					// actually fetched the underlying value vs reused a
					// memoized stateHash?"
					load, skipped, reset, diskSto, diskAcc := commitment.SkipLoadResetCounters()
					hasStoMiss := commitment.HasStorageMissCount()
					xfTrue, xfFalse := existence.ContainsHashStats()
					ssIns, ssUpd, ssDel, _ := commitment.SstoreClassificationCounts()
					wHit, wEmpty := commitment.WarmerBranchOutcomeStats()
					// Per-domain file-read counts: lets us decompose the
					// aggregate `files=N` from [domain reads] into Commitment
					// (branch reads), Storage (value loads), Account, Code.
					// All cumulative; deltas via successive lines.
					var aFiles, sFiles, cFiles, mFiles int64
					var aUniq, sUniq, cUniq, mUniq int64
					var mLens [7]int64
					if m := sdc.sharedDomains.Metrics(); m != nil {
						m.RLock()
						if d := m.Domains[kv.AccountsDomain]; d != nil {
							aFiles = d.FileReadCount
							aUniq = d.UniqueFileReadCount
						}
						if d := m.Domains[kv.StorageDomain]; d != nil {
							sFiles = d.FileReadCount
							sUniq = d.UniqueFileReadCount
						}
						if d := m.Domains[kv.CodeDomain]; d != nil {
							cFiles = d.FileReadCount
							cUniq = d.UniqueFileReadCount
						}
						if d := m.Domains[kv.CommitmentDomain]; d != nil {
							mFiles = d.FileReadCount
							mUniq = d.UniqueFileReadCount
							mLens = d.UniqueLenBuckets
						}
						m.RUnlock()
					}
					// commLens compact form: "1B:N0|2-4B:N1|5-8B:N2|9-16B:N3|17-32B:N4|33-64B:N5|>64B:N6"
					commLens := fmt.Sprintf("1B:%d|2-4B:%d|5-8B:%d|9-16B:%d|17-32B:%d|33-64B:%d|>64B:%d",
						mLens[0], mLens[1], mLens[2], mLens[3], mLens[4], mLens[5], mLens[6])
					log.Info("[commitment][cache-fp]",
						"block", blockNum,
						"root", hex.EncodeToString(rootHash),
						"fp", fmt.Sprintf("%016x", bc.Fingerprint()),
						"divergences", bc.VerifyDivergences(),
						"took", took,
						"keys", common.PrettyCounter(updateCount),
						"load", load,
						"skipped", skipped,
						"reset", reset,
						"disk_sto", diskSto,
						"disk_acc", diskAcc,
						"has_sto_miss", hasStoMiss,
						"xf_true", xfTrue,
						"xf_false", xfFalse,
						"ss_ins", ssIns,
						"ss_upd", ssUpd,
						"ss_del", ssDel,
						"w_hit", wHit,
						"w_empty", wEmpty,
						"files_acc", aFiles,
						"files_sto", sFiles,
						"files_code", cFiles,
						"files_comm", mFiles,
						"uniq_acc", aUniq,
						"uniq_sto", sUniq,
						"uniq_code", cUniq,
						"uniq_comm", mUniq,
						"comm_lens", commLens)
				}
			}
		}
	}()
	if updateCount == 0 {
		rootHash, err = sdc.patriciaTrie.RootHash()
		return rootHash, err
	}

	// data accessing functions should be set when domain is opened/shared context updated

	sdc.patriciaTrie.SetTrace(sdc.trace)
	sdc.patriciaTrie.SetTraceDomain(sdc.sharedDomains.Trace())
	if sdc.sharedDomains.CommitmentCapture() {
		if sdc.patriciaTrie.GetCapture(false) == nil {
			sdc.patriciaTrie.SetCapture([]string{})
		}
	}

	trieContext := sdc.trieContext(tx, blockNum, txNum)

	// If trie trace is configured, wrap the context with a recorder.
	// Block-targeted: when TrieTraceBlock is set, only record that specific block.
	var recorder *commitment.RecordingContext
	traceFile := dbg.TrieTraceFile
	if traceFile == "" && dbg.TrieTraceBlock != 0 && blockNum == dbg.TrieTraceBlock {
		// Auto-generate filename when only TRIE_TRACE_BLOCK is set without TRIE_TRACE_FILE.
		traceFile = fmt.Sprintf("/tmp/trie-trace-block-%d.toml", blockNum)
	} else if dbg.TrieTraceBlock != 0 && blockNum != dbg.TrieTraceBlock {
		traceFile = "" // skip recording — not the target block
	}
	if traceFile != "" {
		recorder = commitment.NewRecordingContext(trieContext)
		sdc.patriciaTrie.ResetContext(recorder)
		// Capture input keys before Process consumes them — fold operations may
		// read Account/Storage for neighboring cells, and we must not include
		// those reads as input updates in the trace.
		inputKeys := sdc.updates.PlainKeys()

		// Capture internal trie state before Process — required for replay.
		// In production the trie has been restored via seekCommitment/SetState;
		// without this snapshot, replay starts from empty state and diverges.
		var trieState []byte
		switch trie := sdc.patriciaTrie.(type) {
		case *commitment.HexPatriciaHashed:
			trieState, err = trie.EncodeCurrentState(nil)
		case *commitment.ConcurrentPatriciaHashed:
			trieState, err = trie.RootTrie().EncodeCurrentState(nil)
		}
		if err != nil {
			log.Warn("[commitment] failed to encode trie state for trace", "err", err)
			trieState = nil // non-fatal, continue without state
			err = nil
		}

		defer func() {
			if recorder == nil {
				return
			}
			trace, traceErr := commitment.BuildTrieTrace(recorder, inputKeys, trieState)
			if traceErr != nil {
				log.Warn("[commitment] failed to build trie trace", "err", traceErr)
				return
			}
			trace.BlockNum = blockNum
			trace.TxNum = txNum

			savePath := traceFile
			// Save trace on error too — this is the primary debugging use case.
			// Partial data (whatever was read before the error) is still valuable.
			if err != nil {
				trace.Error = err.Error()
				savePath = commitment.ErrorTracePath(traceFile)
			}
			if traceErr = trace.Save(savePath); traceErr != nil {
				log.Warn("[commitment] failed to save trie trace", "path", savePath, "err", traceErr)
				return
			}
			if err != nil {
				log.Info("[commitment] trie trace saved (Process error captured)", "path", savePath, "error", trace.Error, "branches", len(trace.Branches), "accounts", len(trace.Accounts), "storages", len(trace.Storages))
			} else {
				log.Info("[commitment] trie trace saved", "path", savePath, "branches", len(trace.Branches), "accounts", len(trace.Accounts), "storages", len(trace.Storages), "updates", len(trace.Updates))
			}
		}()
	}

	// Note: a previous EnableWarmupCache(false)+defer(true) guard lived here
	// to ensure RecordingContext saw every read during trace capture. Now
	// that the Go-side WarmupCache is gone, every read already goes through
	// ctx.* (observable to the recorder by construction); no guard needed.

	var warmupConfig commitment.WarmupConfig
	var drainCollectors func() []*etl.Collector
	if sdc.paraTrieDB != nil {
		if _, ok := sdc.patriciaTrie.(*commitment.ConcurrentPatriciaHashed); ok && sdc.updates.IsConcurrentCommitment() {
			warmupConfig.CtxFactory, drainCollectors = sdc.concurrentTrieContextFactory(ctx, sdc.paraTrieDB, txNum)
		} else {
			warmupConfig.CtxFactory = sdc.trieContextFactory(ctx, sdc.paraTrieDB, txNum)
		}
		warmupConfig.Enabled = sdc.trieWarmup
		warmupConfig.NumWorkers = dbg.TipTrieWarmupers
		warmupConfig.MaxDepth = commitment.WarmupMaxDepth
		warmupConfig.LogPrefix = logPrefix
	}

	// Note: pending deferred updates are flushed by SharedDomains.ComputeCommitment
	// (the public wrapper) BEFORE this method is called. The wrapper routes the flush
	// through FlushPendingUpdates which writes into the correct block's changeset.

	// When deferring commitment updates, tell Process() to leave deferred updates
	// on the branch encoder instead of applying inline — we'll take them after.
	if hph, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok && sdc.deferCommitmentUpdates {
		hph.SetLeaveDeferredForCaller(true)
		defer hph.SetLeaveDeferredForCaller(false)
	}

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix, onProgress, warmupConfig)

	if err != nil {
		if drainCollectors != nil {
			for _, c := range drainCollectors() {
				c.Close()
			}
		}
		return nil, err
	}
	// Merge per-goroutine ETL collectors into the main writer via PutBranch.
	// This ensures all callers (rebuild, exec3, RPC, etc.) get the data merged
	// automatically without needing to drain externally.
	if drainCollectors != nil {
		collectors := drainCollectors()
		defer func() {
			for _, c := range collectors {
				c.Close()
			}
		}()
		for _, c := range collectors {
			if loadErr := c.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
				return trieContext.PutBranch(k, v, nil)
			}, etl.TransformArgs{}); loadErr != nil {
				return nil, loadErr
			}
		}
	}

	// Handle deferred branch updates left by Process() on the branch encoder.
	if hph, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok && hph.HasPendingDeferredUpdates() {
		// Store deferred updates for later flushing (fork validation path).
		// This path is reached only when deferCommitmentUpdates is true because
		// Process() applies inline by default.
		sdc.pendingUpdate = &commitment.PendingCommitmentUpdate{
			BlockNum: blockNum,
			TxNum:    txNum,
			Deferred: hph.TakeDeferredUpdates(),
		}
	}

	sdc.justRestored.Store(false)

	if saveState {
		if err = sdc.encodeAndStoreCommitmentState(trieContext, blockNum, txNum); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

func (sdc *SharedDomainsCommitmentContext) trieContextFactory(ctx context.Context, db kv.TemporalRoDB, txNum uint64) commitment.TrieContextFactory {
	// avoid races like this
	stepSize := sdc.sharedDomains.StepSize()
	return func() (commitment.PatriciaContext, func()) {
		roTx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			return &errorTrieContext{err: err}, func() {}
		}
		warmupCtx := &TrieContext{
			getter:   sdc.sharedDomains.AsGetter(roTx),
			putter:   sdc.sharedDomains.AsPutDel(roTx),
			stepSize: stepSize,
			txNum:    txNum,
		}
		if sdc.stateReader != nil {
			warmupCtx.stateReader = sdc.stateReader.Clone(roTx)
		} else {
			warmupCtx.stateReader = NewLatestStateReader(roTx, sdc.sharedDomains)
		}
		cleanup := func() {
			roTx.Rollback()
		}
		return warmupCtx, cleanup
	}
}

// concurrentTrieContextFactory is like trieContextFactory but also creates a per-goroutine
// etl.Collector for each context so that PutBranch writes are isolated (no shared writer race).
// Returns the factory and a drain function that collects all created collectors.
func (sdc *SharedDomainsCommitmentContext) concurrentTrieContextFactory(ctx context.Context, db kv.TemporalRoDB, txNum uint64) (commitment.TrieContextFactory, func() []*etl.Collector) {
	stepSize := sdc.sharedDomains.StepSize()
	var mu sync.Mutex
	var collectors []*etl.Collector

	factory := func() (commitment.PatriciaContext, func()) {
		roTx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			return &errorTrieContext{err: err}, func() {}
		}

		collector := etl.NewCollector("[concurrent_branch]", sdc.tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/16), log.Root()) //nolint:gocritic
		collector.LogLvl(log.LvlDebug)

		mu.Lock()
		collectors = append(collectors, collector)
		mu.Unlock()

		warmupCtx := &TrieContext{
			getter:         sdc.sharedDomains.AsGetter(roTx),
			putter:         sdc.sharedDomains.AsPutDel(roTx),
			stepSize:       stepSize,
			txNum:          txNum,
			localCollector: collector,
		}
		if sdc.stateReader != nil {
			warmupCtx.stateReader = sdc.stateReader.Clone(roTx)
		} else {
			warmupCtx.stateReader = NewLatestStateReader(roTx, sdc.sharedDomains)
		}
		cleanup := func() {
			roTx.Rollback()
		}
		return warmupCtx, cleanup
	}

	drain := func() []*etl.Collector {
		mu.Lock()
		defer mu.Unlock()
		c := collectors
		collectors = nil
		return c
	}

	return factory, drain
}

// errorTrieContext is a PatriciaContext that always returns an error.
// Used when transaction creation fails in warmup factory.
type errorTrieContext struct {
	err error
}

func (e *errorTrieContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return nil, 0, e.err
}

func (e *errorTrieContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	return e.err
}

func (e *errorTrieContext) Account(plainKey []byte) (*commitment.Update, error) {
	return nil, e.err
}

func (e *errorTrieContext) Storage(plainKey []byte) (*commitment.Update, error) {
	return nil, e.err
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var KeyCommitmentState = []byte(keyCommitmentStateS)

var ErrBehindCommitment = errors.New("behind commitment")

func DecodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:16])
}

// LatestCommitmentState searches for last encoded state for CommitmentContext.
// Found value does not become current state.
func (sdc *SharedDomainsCommitmentContext) LatestCommitmentState(trieContext *TrieContext) (blockNum, txNum uint64, state []byte, err error) {
	if sdc.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie && sdc.patriciaTrie.Variant() != commitment.VariantConcurrentHexPatricia {
		return 0, 0, nil, errors.New("state storing is only supported hex patricia trie")
	}
	var step kv.Step

	state, step, err = trieContext.Branch(KeyCommitmentState)
	if err != nil {
		return 0, 0, nil, err
	}

	if err = trieContext.stateReader.CheckDataAvailable(kv.CommitmentDomain, step); err != nil {
		return 0, 0, nil, err
	}

	if len(state) < 16 {
		return 0, 0, nil, nil
	}

	txNum, blockNum = DecodeTxBlockNums(state)
	return blockNum, txNum, state, nil
}

// enable concurrent commitment if we are using concurrent patricia trie and this trie diverges on very top (first branch is straight at nibble 0)
func (sdc *SharedDomainsCommitmentContext) enableConcurrentCommitmentIfPossible() error {
	if pt, ok := sdc.patriciaTrie.(*commitment.ConcurrentPatriciaHashed); ok {
		nextConcurrent, err := pt.CanDoConcurrentNext()
		if err != nil {
			return err
		}
		sdc.updates.SetConcurrentCommitment(nextConcurrent)
	}
	return nil
}

// SeekCommitment searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (sdc *SharedDomainsCommitmentContext) SeekCommitment(ctx context.Context, tx kv.TemporalTx) (txNum, blockNum uint64, err error) {
	trieContext := sdc.trieContext(tx, 0, 0) // blockNum/txNum not yet known; trieContext only used for reading here

	_, _, state, err := sdc.LatestCommitmentState(trieContext)
	if err != nil {
		return 0, 0, err
	}
	if state != nil {
		blockNum, txNum, err = sdc.restorePatriciaState(state)
		if err != nil {
			return 0, 0, err
		}
		if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
			return 0, 0, err
		}
		return txNum, blockNum, nil
	}
	// handle case when we have no commitment, but have executed blocks
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution"))
	if err != nil {
		return 0, 0, err
	}
	if len(bnBytes) == 8 {
		blockNum = binary.BigEndian.Uint64(bnBytes)
		// blockNum=0 means genesis (block 0) executed — NOT "no progress".
		// The "no progress" case is len(bnBytes)==0 (no SyncStageProgress
		// entry at all) which is handled by the outer if branch leaving
		// blockNum/txNum at their zero defaults. When the entry exists,
		// blockNum is the last executed block and TxNums.Max(blockNum)
		// returns its last txNum — for genesis, that's 1 (txIndex=-1 at
		// txNum=0 + block-end at txNum=1, per genesiswrite.WriteGenesisBesideState
		// which calls TxNums.Append(tx, 0, len(txs)+1)). Returning txNum=0
		// for blockNum=0 would falsely match the "never executed" case and
		// make exec3.go's skip logic re-execute genesis on subsequent cycles.
		txNum, err = rawdbv3.TxNums.Max(ctx, tx, blockNum)
		if err != nil {
			return 0, 0, err
		}
	}
	if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
		return 0, 0, err
	}
	return txNum, blockNum, nil
}

// encodes current trie state and saves it in SharedDomains
func (sdc *SharedDomainsCommitmentContext) encodeAndStoreCommitmentState(trieContext *TrieContext, blockNum, txNum uint64) error {
	if trieContext == nil {
		return errors.New("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, txNum)
	if err != nil {
		return err
	}
	prevState, _, err := trieContext.Branch(KeyCommitmentState)
	if err != nil {
		return err
	}
	if len(prevState) == 0 && prevState != nil {
		prevState = nil
	}
	// state could be equal but txnum/blocknum could be different.
	// We do skip only full matches
	if bytes.Equal(prevState, encodedState) {
		//fmt.Printf("[commitment] skip store txn %d block %d (prev b=%d t=%d) rh %x\n",/
		//	binary.BigEndian.Uint64(prevState[8:16]), binary.BigEndian.Uint64(prevState[:8]), dc.ht.iit.txNum, blockNum, rh)
		return nil
	}

	return trieContext.PutBranch(KeyCommitmentState, encodedState, prevState)
}

// Encodes current trie state and returns it
func (sdc *SharedDomainsCommitmentContext) encodeCommitmentState(blockNum, txNum uint64) ([]byte, error) {
	var state []byte
	var err error

	switch trie := (sdc.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	case *commitment.ConcurrentPatriciaHashed:
		state, err = trie.RootTrie().EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state storing for patricia trie type: %T", sdc.patriciaTrie)
	}

	cs := &commitmentState{trieState: state, blockNum: blockNum, txNum: txNum}
	encoded, err := cs.Encode()
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// After commitment state is retored, method .Reset() should NOT be called until new updates.
// Otherwise state should be restorePatriciaState()d again.
func (sdc *SharedDomainsCommitmentContext) restorePatriciaState(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	tv := sdc.patriciaTrie.Variant()

	var hext *commitment.HexPatriciaHashed
	if tv == commitment.VariantHexPatriciaTrie {
		var ok bool
		hext, ok = sdc.patriciaTrie.(*commitment.HexPatriciaHashed)
		if !ok {
			return 0, 0, errors.New("cannot typecast hex patricia trie")
		}
	}
	if tv == commitment.VariantConcurrentHexPatricia {
		phext, ok := sdc.patriciaTrie.(*commitment.ConcurrentPatriciaHashed)
		if !ok {
			return 0, 0, errors.New("cannot typecast parallel hex patricia trie")
		}
		hext = phext.RootTrie()
	}
	if tv == commitment.VariantBinPatriciaTrie || hext == nil {
		return 0, 0, errors.New("state storing is only supported hex patricia trie")
	}

	if err := hext.SetState(cs.trieState); err != nil {
		return 0, 0, fmt.Errorf("failed restore state : %w", err)
	}
	sdc.justRestored.Store(true) // to prevent double reset
	if sdc.trace {
		rootHash, err := hext.RootHash()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to get root hash after state restore: %w", err)
		}
		log.Debug(fmt.Sprintf("[commitment] restored state: block=%d txn=%d rootHash=%x\n", cs.blockNum, cs.txNum, rootHash))
	}
	return cs.blockNum, cs.txNum, nil
}

type TrieContext struct {
	getter   kv.TemporalGetter
	putter   kv.TemporalPutDel
	txNum    uint64
	blockNum uint64

	stepSize       uint64
	trace          bool
	stateReader    StateReader
	localCollector *etl.Collector // per-goroutine collector for concurrent PutBranch

	// probeSd / probeTx feed ProbeStateLayers — diagnostics-only fields
	// populated when this TrieContext is constructed from a
	// SharedDomainsCommitmentContext that has both a SharedDomains
	// and a tx in scope. Both nil for read-only / test contexts where
	// the probe is not available.
	probeSd sd
	probeTx kv.TemporalTx
}

// NewTrieContextRo creates a read-only TrieContext suitable for TrieReader lookups.
// Only Branch() is functional; PutBranch/Account/Storage will return errors or nil.
func NewTrieContextRo(reader StateReader, stepSize uint64) *TrieContext {
	return &TrieContext{stateReader: reader, stepSize: stepSize}
}

func (sdc *TrieContext) Branch(pref []byte) ([]byte, kv.Step, error) {
	enc, step, err := sdc.readDomain(kv.CommitmentDomain, pref)
	if err != nil {
		return nil, 0, err
	}
	// Branch reads feed Merge(prev,update), branchEncoder/merger internal buffers,
	// deferred-update queues, and unfoldBranchNode reads. The slice returned by the
	// underlying state cache / getter aliases shared storage that another goroutine
	// (concurrent commitment workers) can recycle. Own the bytes at the trie-context
	// boundary so all downstream consumers are safe.
	return common.Copy(enc), step, nil
}

// ProbeStateLayers samples the underlying state layers for one key —
// sd.mem, sd.parent.mem (if any), and tx-direct (MDBX) — for
// divergence-detection diagnostics. Returns empty / not-ok when
// constructed without a probe-capable SharedDomains (e.g.
// NewTrieContextRo). Read-only.
func (sdc *TrieContext) ProbeStateLayers(domain kv.Domain, key []byte) (mem, parentMem, mdbx []byte, memOk, parentOk bool) {
	if sdc.probeSd == nil {
		return
	}
	return sdc.probeSd.ProbeReadLayers(domain, sdc.probeTx, key)
}

// SiteIdentity returns a stable string identifying the underlying
// SharedDomains pointer. Used by cache write sites to tag entries with
// the SD lineage that produced them, so divergence diagnostics can tell
// parent-SD writes apart from fork-SD writes when the cache is shared.
func (sdc *TrieContext) SiteIdentity() string {
	return fmt.Sprintf("sd=%p", sdc.probeSd)
}

func (sdc *TrieContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	if sdc.stateReader.WithHistory() { // do not store branches if explicitly operate on history
		return nil
	}
	if sdc.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	if sdc.localCollector != nil {
		return sdc.localCollector.Collect(prefix, data)
	}
	return sdc.putter.DomainPut(kv.CommitmentDomain, prefix, data, sdc.txNum, prevData)
}

// readDomain reads data from domain, dereferences key and returns encoded value and step.
// Step returned only when reading from domain files, otherwise it is always 0.
// Step is used in Trie for memo stats and file depth access statistics.
func (sdc *TrieContext) readDomain(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	enc, step, err = sdc.stateReader.Read(d, plainKey, sdc.stepSize)
	return enc, step, err
}

func (sdc *TrieContext) Account(plainKey []byte) (u *commitment.Update, err error) {
	encAccount, _, err := sdc.readDomain(kv.AccountsDomain, plainKey)
	if err != nil {
		return nil, err
	}

	u = &commitment.Update{CodeHash: empty.CodeHash}
	if len(encAccount) == 0 {
		u.Flags = commitment.DeleteUpdate
		return u, nil
	}

	acc := new(accounts.Account)
	if err = accounts.DeserialiseV3(acc, encAccount); err != nil {
		return nil, err
	}

	u.Flags |= commitment.NonceUpdate
	u.Nonce = acc.Nonce

	u.Flags |= commitment.BalanceUpdate
	u.Balance = acc.Balance

	if !acc.CodeHash.IsZero() {
		u.Flags |= commitment.CodeUpdate
		u.CodeHash = acc.CodeHash.Value()
	}

	if assert.Enable { // verify code hash from account encoding matches stored code
		code, _, err := sdc.readDomain(kv.CodeDomain, plainKey)
		if err != nil {
			return nil, err
		}
		if len(code) > 0 {
			u.CodeHash = crypto.HashData(code)
			u.Flags |= commitment.CodeUpdate
		}
		if acc.CodeHash.Value() != u.CodeHash {
			return nil, fmt.Errorf("code hash mismatch: account '%x' != codeHash '%x'", acc.CodeHash, u.CodeHash[:])
		}
	}
	return u, nil
}

func (sdc *TrieContext) Storage(plainKey []byte) (u *commitment.Update, err error) {
	enc, _, err := sdc.readDomain(kv.StorageDomain, plainKey)
	if err != nil {
		return nil, err
	}
	u = &commitment.Update{
		Flags:      commitment.DeleteUpdate,
		StorageLen: int8(len(enc)),
	}

	if u.StorageLen > 0 {
		u.Flags = commitment.StorageUpdate
		copy(u.Storage[:u.StorageLen], enc)
	}

	return u, nil
}

type ValueMerger func(prev, current []byte) (merged []byte, err error)

// TODO revisit encoded commitmentState.
//   - Add versioning
//   - add trie variant marker
//   - simplify decoding. Rn it's 3 embedded structure: RootNode encoded, Trie state encoded and commitmentState wrapper for search.
//     | search through states seems mostly useless so probably commitmentState should become header of trie state.
type commitmentState struct {
	txNum     uint64
	blockNum  uint64
	trieState []byte
}

func NewCommitmentState(txNum uint64, blockNum uint64, trieState []byte) *commitmentState {
	return &commitmentState{txNum, blockNum, trieState}
}

func (cs *commitmentState) Decode(buf []byte) error {
	if len(buf) < 10 {
		return fmt.Errorf("ivalid commitment state buffer size %d, expected at least 10b", len(buf))
	}
	pos := 0
	cs.txNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.blockNum = binary.BigEndian.Uint64(buf[pos : pos+8])
	pos += 8
	cs.trieState = make([]byte, binary.BigEndian.Uint16(buf[pos:pos+2]))
	pos += 2
	if len(cs.trieState) == 0 && len(buf) == 10 {
		return nil
	}
	copy(cs.trieState, buf[pos:pos+len(cs.trieState)])
	return nil
}

func (cs *commitmentState) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	var v [18]byte
	binary.BigEndian.PutUint64(v[:], cs.txNum)
	binary.BigEndian.PutUint64(v[8:16], cs.blockNum)
	binary.BigEndian.PutUint16(v[16:18], uint16(len(cs.trieState)))
	if _, err := buf.Write(v[:]); err != nil {
		return nil, err
	}
	if _, err := buf.Write(cs.trieState); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func LatestBlockNumWithCommitment(tx kv.TemporalGetter) (uint64, error) {
	stateVal, _, err := tx.GetLatest(kv.CommitmentDomain, KeyCommitmentState)
	if err != nil {
		return 0, err
	}
	if len(stateVal) < 16 {
		return 0, nil
	}
	_, minUnwindable := DecodeTxBlockNums(stateVal)
	return minUnwindable, nil
}
