package commitmentdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	mxCommitmentRunning = metrics.GetOrCreateGauge("domain_running_commitment")
	mxCommitmentTook    = metrics.GetOrCreateSummary("domain_commitment_took")
)

type sd interface {
	SetTxNum(blockNum uint64)
	AsGetter(tx kv.TemporalTx) kv.TemporalGetter
	AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel
	StepSize() uint64
	TxNum() uint64

	Trace() bool
	CommitmentCapture() bool
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

// SetLimitedHistoryStateReader sets the state reader to read *limited* (i.e. *without-recent-files*) historical state at specified txNum.
func (sdc *SharedDomainsCommitmentContext) SetLimitedHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewLimitedHistoryStateReader(roTx, sdc.sharedDomains, limitReadAsOfTxNum))
}

func (sdc *SharedDomainsCommitmentContext) SetTrace(trace bool) {
	sdc.trace = trace
	sdc.patriciaTrie.SetTrace(trace)
}

// EnableWarmupCache enables/disables warmup cache during commitment processing.
func (sdc *SharedDomainsCommitmentContext) EnableWarmupCache(enable bool) {
	sdc.patriciaTrie.EnableWarmupCache(enable)
}

func (sdc *SharedDomainsCommitmentContext) EnableCsvMetrics(filePathPrefix string) {
	sdc.patriciaTrie.EnableCsvMetrics(filePathPrefix)
}

func NewSharedDomainsCommitmentContext(sd sd, mode commitment.Mode, trieVariant commitment.TrieVariant, tmpDir string) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
	}
	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, tmpDir)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) trieContext(tx kv.TemporalTx, txNum uint64) *TrieContext {
	mainTtx := &TrieContext{
		getter:   sdc.sharedDomains.AsGetter(tx),
		putter:   sdc.sharedDomains.AsPutDel(tx),
		stepSize: sdc.sharedDomains.StepSize(),
		txNum:    txNum,
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

func (sdc *SharedDomainsCommitmentContext) Witness(ctx context.Context, codeReads map[common.Hash]witnesstypes.CodeWithHash, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, codeReads, logPrefix)
	}

	return nil, nil, errors.New("shared domains commitment context doesn't have HexPatriciaHashed")
}

// ComputeCommitment Evaluates commitment for gathered updates.
// If warmup was set via EnableTrieWarmup, pre-warms MDBX page cache by reading Branch data in parallel before processing.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveState bool, blockNum uint64, txNum uint64, logPrefix string, commitProgress chan *commitment.CommitProgress) (rootHash []byte, err error) {
	if dbg.KVReadLevelledMetrics {
		mxCommitmentRunning.Inc()
		defer mxCommitmentRunning.Dec()
		defer mxCommitmentTook.ObserveDuration(time.Now())
	}

	updateCount := sdc.updates.Size()
	start := time.Now()
	defer func() {
		log.Debug("[commitment] processed", "block", blockNum, "txNum", txNum, "keys", common.PrettyCounter(updateCount), "mode", sdc.updates.Mode(), "spent", time.Since(start), "rootHash", hex.EncodeToString(rootHash))
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

	trieContext := sdc.trieContext(tx, txNum)

	var warmupConfig commitment.WarmupConfig
	if sdc.paraTrieDB != nil {
		warmupConfig = commitment.WarmupConfig{
			Enabled:    sdc.trieWarmup,
			CtxFactory: sdc.trieContextFactory(ctx, sdc.paraTrieDB, txNum),
			NumWorkers: 16,
			MaxDepth:   commitment.WarmupMaxDepth,
			LogPrefix:  logPrefix,
		}
	}

	// Flush pending commitment update before processing new ones.
	if sdc.pendingUpdate != nil {
		if err = sdc.flushPendingUpdate(trieContext.putter); err != nil {
			return nil, err
		}
	}

	// When deferring commitment updates, tell Process() to leave deferred updates
	// on the branch encoder instead of applying inline — we'll take them after.
	if hph, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok && sdc.deferCommitmentUpdates {
		hph.SetLeaveDeferredForCaller(true)
		defer hph.SetLeaveDeferredForCaller(false)
	}

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix, commitProgress, warmupConfig)
	if err != nil {
		return nil, err
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
			return &errorTrieContext{err: err}, nil
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

func (e *errorTrieContext) TxNum() uint64 {
	return 0
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var KeyCommitmentState = []byte(keyCommitmentStateS)

var ErrBehindCommitment = errors.New("behind commitment")

func _decodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
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

	txNum, blockNum = _decodeTxBlockNums(state)
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
	trieContext := sdc.trieContext(tx, 0) // txNum not yet known; trieContext only used for reading here

	_, _, state, err := sdc.LatestCommitmentState(trieContext)
	if err != nil {
		return 0, 0, err
	}
	if state != nil {
		blockNum, txNum, err = sdc.restorePatriciaState(state)
		if err != nil {
			return 0, 0, err
		}
		if blockNum > 0 {
			lastBn, _, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return 0, 0, err
			}
			if lastBn < blockNum {
				return 0, 0, fmt.Errorf("%w: TxNums index is at block %d and behind commitment %d", ErrBehindCommitment, lastBn, blockNum)
			}
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
		txNum = uint64(0)
		if blockNum > 0 {
			txNum, err = rawdbv3.TxNums.Max(ctx, tx, blockNum)
		}
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
	getter kv.TemporalGetter
	putter kv.TemporalPutDel
	txNum  uint64

	stepSize    uint64
	trace       bool
	stateReader StateReader
}

func (sdc *TrieContext) Branch(pref []byte) ([]byte, kv.Step, error) {
	return sdc.readDomain(kv.CommitmentDomain, pref)
}

func (sdc *TrieContext) PutBranch(prefix []byte, data []byte, prevData []byte) error {
	if sdc.stateReader.WithHistory() { // do not store branches if explicitly operate on history
		return nil
	}
	if sdc.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}

	return sdc.putter.DomainPut(kv.CommitmentDomain, prefix, data, sdc.txNum, prevData)
}

func (sdc *TrieContext) TxNum() uint64 {
	return sdc.txNum
}

// readDomain reads data from domain, dereferences key and returns encoded value and step.
// Step returned only when reading from domain files, otherwise it is always 0.
// Step is used in Trie for memo stats and file depth access statistics.
func (sdc *TrieContext) readDomain(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}
	return sdc.stateReader.Read(d, plainKey, sdc.stepSize)
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
			copy(u.CodeHash[:], crypto.Keccak256(code))
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
	if len(stateVal) == 0 {
		return 0, nil
	}
	_, minUnwindale := _decodeTxBlockNums(stateVal)
	return minUnwindale, nil
}
