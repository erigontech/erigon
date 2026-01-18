package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

var (
	mxCommitmentRunning = metrics.GetOrCreateGauge("domain_running_commitment")
	mxCommitmentTook    = metrics.GetOrCreateSummary("domain_commitment_took")
)

type StateReader interface {
	WithHistory() bool
	CheckDataAvailable(d kv.Domain, step kv.Step) error
	Read(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error)
	Clone(roTx kv.TemporalTx, getter kv.TemporalGetter) StateReader
}

type LatestStateReader struct {
	getter kv.TemporalGetter
}

func NewLatestStateReader(getter kv.TemporalGetter) *LatestStateReader {
	return &LatestStateReader{getter: getter}
}

func (r *LatestStateReader) WithHistory() bool {
	return false
}

func (r *LatestStateReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	// we're processing the latest state - in which case it needs to be writable
	if frozenSteps := r.getter.StepsInFiles(d); step < frozenSteps {
		return fmt.Errorf("%q state out of date: step %d, expected step %d", d, step, frozenSteps)
	}
	return nil
}

func (r *LatestStateReader) Read(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	enc, step, err = r.getter.GetLatest(d, plainKey)
	if err != nil {
		return nil, 0, fmt.Errorf("LatestStateReader(GetLatest) %q: %w", d, err)
	}
	return enc, step, nil
}

func (r *LatestStateReader) Clone(_ kv.TemporalTx, getter kv.TemporalGetter) StateReader {
	return NewLatestStateReader(getter)
}

// HistoryStateReader reads *full* historical state at specified txNum.
// `limitReadAsOfTxNum` here is used as timestamp for usual GetAsOf.
type HistoryStateReader struct {
	roTx               kv.TemporalTx
	limitReadAsOfTxNum uint64
}

func NewHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) *HistoryStateReader {
	return &HistoryStateReader{
		roTx:               roTx,
		limitReadAsOfTxNum: limitReadAsOfTxNum,
	}
}

func (r *HistoryStateReader) WithHistory() bool {
	return true
}

func (r *HistoryStateReader) CheckDataAvailable(kv.Domain, kv.Step) error {
	return nil
}

func (r *HistoryStateReader) Read(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	enc, _, err = r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
	if err != nil {
		return enc, 0, fmt.Errorf("HistoryStateReader(GetAsOf) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, err)
	}
	return enc, kv.Step(r.limitReadAsOfTxNum / r.roTx.StepSize()), nil
}

func (r *HistoryStateReader) Clone(tx kv.TemporalTx, _ kv.TemporalGetter) StateReader {
	return NewHistoryStateReader(tx, r.limitReadAsOfTxNum)
}

// LimitedHistoryStateReader reads from *limited* (i.e. *without-recent-files*) state at specified txNum, otherwise from *latest*.
// `limitReadAsOfTxNum` here is used for unusual operation: "hide recent .kv files and read the latest state from files".
type LimitedHistoryStateReader struct {
	HistoryStateReader
	getter kv.TemporalGetter
}

func NewLimitedHistoryStateReader(tx kv.TemporalTx, getter kv.TemporalGetter, limitReadAsOfTxNum uint64) *LimitedHistoryStateReader {
	return &LimitedHistoryStateReader{
		HistoryStateReader: HistoryStateReader{
			roTx:               tx,
			limitReadAsOfTxNum: limitReadAsOfTxNum,
		},
		getter: getter,
	}
}

func (r *LimitedHistoryStateReader) WithHistory() bool {
	return false
}

// Reason why we have `kv.TemporalDebugTx.GetLatestFromFiles' call here: `state.RebuildCommitmentFiles` can build commitment.kv from account.kv.
// Example: we have account.0-16.kv and account.16-18.kv, let's generate commitment.0-16.kv => it means we need to make account.16-18.kv invisible
// and then read "latest state" like there is no account.16-18.kv
func (r *LimitedHistoryStateReader) Read(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	var ok bool
	var endTxNum uint64
	// reading from domain files this way will dereference domain key correctly,
	// GetAsOf itself does not dereference keys in commitment domain values
	enc, ok, _, endTxNum, err = r.roTx.Debug().GetLatestFromFiles(d, plainKey, r.limitReadAsOfTxNum)
	if err != nil {
		return nil, 0, fmt.Errorf("LimitedHistoryStateReader(GetLatestFromFiles) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, err)
	}
	if !ok {
		enc = nil
	} else {
		step = kv.Step(endTxNum / r.roTx.StepSize())
	}
	if enc == nil {
		enc, step, err = r.getter.GetLatest(d, plainKey)
		if err != nil {
			return nil, 0, fmt.Errorf("LimitedHistoryStateReader(GetLatest) %q: %w", d, err)
		}
	}
	return enc, step, nil
}

func (r *LimitedHistoryStateReader) Clone(tx kv.TemporalTx, getter kv.TemporalGetter) StateReader {
	return NewLimitedHistoryStateReader(tx, getter, r.limitReadAsOfTxNum)
}

type CommitmentContext struct {
	updates           *Updates
	patriciaTrie      Trie
	justRestored      atomic.Bool // set to true when commitment trie was just restored from snapshot
	trace             bool
	traceDomain       bool
	commitmentCapture bool
	stateReader       StateReader
	warmupDB          kv.TemporalRoDB // if set, enables parallel warmup of MDBX page cache during commitment
	paraTrieDB        kv.TemporalRoDB // if set, it's used to set up a trie ctx factory for para trie without warmup (otherwise it uses warmupDB if enabled)
}

// SetStateReader can be used to set a custom state reader (otherwise the default one is set in CommitmentContext.trieContext).
func (sdc *CommitmentContext) SetStateReader(stateReader StateReader) {
	sdc.stateReader = stateReader
}

// SetWarmupDB sets the database used for parallel warmup of MDBX page cache during commitment.
// When set, ComputeCommitment will pre-fetch Branch data in parallel before processing.
func (sdc *CommitmentContext) SetWarmupDB(db kv.TemporalRoDB) {
	sdc.warmupDB = db
}

func (sdc *CommitmentContext) SetParaTrieDB(db kv.TemporalRoDB) {
	sdc.paraTrieDB = db
}

// SetHistoryStateReader sets the state reader to read *full* historical state at specified txNum.
func (sdc *CommitmentContext) SetHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewHistoryStateReader(roTx, limitReadAsOfTxNum))
}

// SetLimitedHistoryStateReader sets the state reader to read *limited* (i.e. *without-recent-files*) historical state at specified txNum.
func (sdc *CommitmentContext) SetLimitedHistoryStateReader(roTx kv.TemporalTx, getter kv.TemporalGetter, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewLimitedHistoryStateReader(roTx, getter, limitReadAsOfTxNum))
}

func (sdc *CommitmentContext) SetTrace(trace bool) {
	sdc.trace = trace
	sdc.patriciaTrie.SetTrace(trace)
}

func (sdc *CommitmentContext) SetTraceDomain(b, capture bool) []string {
	sdc.traceDomain = b
	sdc.commitmentCapture = capture
	return sdc.GetCapture(true)
}

func (sdc *CommitmentContext) EnableCsvMetrics(filePathPrefix string) {
	sdc.patriciaTrie.EnableCsvMetrics(filePathPrefix)
}

func NewCommitmentContext(mode Mode, trieVariant TrieVariant, tmpDir string) *CommitmentContext {
	ctx := &CommitmentContext{}
	ctx.patriciaTrie, ctx.updates = InitializeTrieAndUpdates(trieVariant, mode, tmpDir)
	return ctx
}

func (sdc *CommitmentContext) trieContext(tx kv.TemporalTx, getter kv.TemporalGetter, putter kv.TemporalPutDel, txNum uint64) *TrieContext {
	mainTtx := &TrieContext{
		getter: getter,
		putter: putter,
		txNum:  txNum,
	}
	if sdc.stateReader != nil {
		mainTtx.stateReader = sdc.stateReader.Clone(tx, getter)
	} else {
		mainTtx.stateReader = NewLatestStateReader(getter)
	}
	sdc.patriciaTrie.ResetContext(mainTtx)
	return mainTtx
}

func (sdc *CommitmentContext) Close() {
	sdc.updates.Close()
}

func (sdc *CommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
}

func (sdc *CommitmentContext) GetCapture(truncate bool) []string {
	return sdc.patriciaTrie.GetCapture(truncate)
}

func (sdc *CommitmentContext) ClearRam() {
	sdc.updates.Reset()
	sdc.Reset()
}

func (sdc *CommitmentContext) KeysCount() uint64 {
	return sdc.updates.Size()
}

func (sdc *CommitmentContext) Trie() Trie {
	return sdc.patriciaTrie
}

func (sdc *CommitmentContext) TouchKey(d kv.Domain, key string, val []byte) {
	if sdc.updates.Mode() == ModeDisabled {
		return
	}

	switch d {
	case kv.AccountsDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.touchAccount)
	case kv.CodeDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.touchCode)
	case kv.StorageDomain:
		sdc.updates.TouchPlainKey(key, val, sdc.updates.touchStorage)
	//case kv.CommitmentDomain, kv.ReceiptDomain:
	default:
		//panic(fmt.Errorf("TouchKey: unknown domain %s", d))
	}
}

func (sdc *CommitmentContext) TouchAccount(addr accounts.Address, val *accounts.Account) {
	sdc.updates.TouchAccount(addr, val)
}

func (sdc *CommitmentContext) TouchStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	sdc.updates.TouchStorage(addr, key, val)
}

func (sdc *CommitmentContext) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	sdc.updates.DelStorage(addr, key)
}

func (sdc *CommitmentContext) TouchCode(addr accounts.Address, val []byte) {
	sdc.updates.TouchCode(addr, val)
}

func (sdc *CommitmentContext) Witness(ctx context.Context, codeReads map[common.Hash]witnesstypes.CodeWithHash, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, codeReads, logPrefix)
	}

	return nil, nil, errors.New("shared domains commitment context doesn't have HexPatriciaHashed")
}

type executionContext interface {
	AsGetter(tx kv.TemporalTx) kv.TemporalGetter
	AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel
}

// Evaluates commitment for gathered updates.
func (sdc *CommitmentContext) ComputeCommitment(ctx context.Context, sd executionContext, tx kv.TemporalTx, saveState bool, blockNum uint64, txNum uint64, logPrefix string, commitProgress chan *CommitProgress) (rootHash []byte, err error) {
	mxCommitmentRunning.Inc()
	defer mxCommitmentRunning.Dec()
	defer func(s time.Time) { mxCommitmentTook.ObserveDuration(s) }(time.Now())

	updateCount := sdc.updates.Size()
	if sdc.trace {
		start := time.Now()
		defer func() {
			log.Trace("ComputeCommitment", "block", blockNum, "keys", common.PrettyCounter(updateCount), "mode", sdc.updates.Mode(), "spent", time.Since(start))
		}()
	}
	if updateCount == 0 {
		rootHash, err = sdc.patriciaTrie.RootHash()
		return rootHash, err
	}

	// data accessing functions should be set when domain is opened/shared context updated

	sdc.patriciaTrie.SetTrace(sdc.trace)
	sdc.patriciaTrie.SetTraceDomain(sdc.traceDomain)
	if sdc.commitmentCapture {
		if sdc.patriciaTrie.GetCapture(false) == nil {
			sdc.patriciaTrie.SetCapture([]string{})
		}
	}

	trieContext := sdc.trieContext(tx, sd.AsGetter(tx), sd.AsPutDel(tx), txNum)

	var warmupConfig WarmupConfig
	if sdc.warmupDB != nil {
		warmupConfig = WarmupConfig{
			Enabled:    true,
			CtxFactory: sdc.trieContextFactory(ctx, sd, sdc.warmupDB, txNum),
			NumWorkers: 16,
			MaxDepth:   WarmupMaxDepth,
			LogPrefix:  logPrefix,
		}
	} else if sdc.paraTrieDB != nil {
		// temporarily use WarmupConfig to easily pass CtxFactory to the para trie without too many changes (can improve in future PR)
		warmupConfig = WarmupConfig{
			CtxFactory: sdc.trieContextFactory(ctx, sd, sdc.paraTrieDB, txNum),
		}
	}

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix, commitProgress, warmupConfig)
	if err != nil {
		return nil, err
	}
	sdc.justRestored.Store(false)

	if saveState {
		if err = sdc.encodeAndStoreCommitmentState(trieContext, blockNum, txNum, rootHash); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

func (sdc *CommitmentContext) trieContextFactory(ctx context.Context, sd executionContext, db kv.TemporalRoDB, txNum uint64) TrieContextFactory {
	return func() (PatriciaContext, func()) {
		roTx, err := db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			return &errorTrieContext{err: err}, nil
		}
		warmupCtx := &TrieContext{
			getter: sd.AsGetter(roTx),
			putter: sd.AsPutDel(roTx),
			txNum:  txNum,
		}
		if sdc.stateReader != nil {
			warmupCtx.stateReader = sdc.stateReader.Clone(roTx, sd.AsGetter(roTx))
		} else {
			warmupCtx.stateReader = NewLatestStateReader(sd.AsGetter(roTx))
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

func (e *errorTrieContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
	return e.err
}

func (e *errorTrieContext) Account(plainKey []byte) (*Update, error) {
	return nil, e.err
}

func (e *errorTrieContext) Storage(plainKey []byte) (*Update, error) {
	return nil, e.err
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
func (sdc *CommitmentContext) LatestCommitmentState(trieContext *TrieContext) (blockNum, txNum uint64, state []byte, err error) {
	if sdc.patriciaTrie.Variant() != VariantHexPatriciaTrie && sdc.patriciaTrie.Variant() != VariantConcurrentHexPatricia {
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
func (sdc *CommitmentContext) enableConcurrentCommitmentIfPossible() error {
	if pt, ok := sdc.patriciaTrie.(*ConcurrentPatriciaHashed); ok {
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
func (sdc *CommitmentContext) SeekCommitment(ctx context.Context, getter kv.TemporalGetter, tx kv.TemporalTx) (blockNum, txNum uint64, ok bool, err error) {
	trieContext := sdc.trieContext(tx, getter, nil, 0)

	_, _, state, err := sdc.LatestCommitmentState(trieContext)
	if err != nil {
		return 0, 0, false, err
	}
	if state != nil {
		blockNum, txNum, err = sdc.restorePatriciaState(state)
		if err != nil {
			return 0, 0, false, err
		}
		if blockNum > 0 {
			lastBn, _, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return 0, 0, false, err
			}
			if lastBn < blockNum {
				return 0, 0, false, fmt.Errorf("%w: TxNums index is at block %d and behind commitment %d", ErrBehindCommitment, lastBn, blockNum)
			}
		}
		if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
			return 0, 0, false, err
		}
		return blockNum, txNum, true, nil
	}
	// handle case when we have no commitment, but have executed blocks
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution"))
	if err != nil {
		return 0, 0, false, err
	}
	if len(bnBytes) == 8 {
		blockNum = binary.BigEndian.Uint64(bnBytes)
		txNum, err = rawdbv3.TxNums.Max(ctx, tx, blockNum)
		if err != nil {
			return 0, 0, false, err
		}
	}
	if blockNum == 0 && txNum == 0 {
		return 0, 0, true, nil
	}

	if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
		return 0, 0, false, err
	}
	return blockNum, txNum, true, nil
}

// encodes current trie state and saves it in SharedDomains
func (sdc *CommitmentContext) encodeAndStoreCommitmentState(trieContext *TrieContext, blockNum, txNum uint64, rootHash []byte) error {
	if trieContext == nil {
		return errors.New("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, txNum)
	if err != nil {
		return err
	}
	prevState, prevStep, err := trieContext.Branch(KeyCommitmentState)
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

	log.Debug("[commitment] store state", "block", blockNum, "txNum", txNum, "rootHash", hex.EncodeToString(rootHash))
	return trieContext.PutBranch(KeyCommitmentState, encodedState, prevState, prevStep)
}

// Encodes current trie state and returns it
func (sdc *CommitmentContext) encodeCommitmentState(blockNum, txNum uint64) ([]byte, error) {
	var state []byte
	var err error

	switch trie := (sdc.patriciaTrie).(type) {
	case *HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	case *ConcurrentPatriciaHashed:
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
func (sdc *CommitmentContext) restorePatriciaState(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	tv := sdc.patriciaTrie.Variant()

	var hext *HexPatriciaHashed
	if tv == VariantHexPatriciaTrie {
		var ok bool
		hext, ok = sdc.patriciaTrie.(*HexPatriciaHashed)
		if !ok {
			return 0, 0, errors.New("cannot typecast hex patricia trie")
		}
	}
	if tv == VariantConcurrentHexPatricia {
		phext, ok := sdc.patriciaTrie.(*ConcurrentPatriciaHashed)
		if !ok {
			return 0, 0, errors.New("cannot typecast parallel hex patricia trie")
		}
		hext = phext.RootTrie()
	}
	if tv == VariantBinPatriciaTrie || hext == nil {
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

	trace       bool
	stateReader StateReader
}

func (sdc *TrieContext) Branch(pref []byte) ([]byte, kv.Step, error) {
	return sdc.readDomain(kv.CommitmentDomain, pref)
}

func (sdc *TrieContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
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

	return sdc.putter.DomainPut(kv.CommitmentDomain, prefix, data, sdc.txNum, prevData, prevStep)
}

// readDomain reads data from domain, dereferences key and returns encoded value and step.
// Step returned only when reading from domain files, otherwise it is always 0.
// Step is used in Trie for memo stats and file depth access statistics.
func (sdc *TrieContext) readDomain(d kv.Domain, plainKey []byte) (enc []byte, step kv.Step, err error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}
	return sdc.stateReader.Read(d, plainKey)
}

func (sdc *TrieContext) Account(plainKey []byte) (u *Update, err error) {
	encAccount, _, err := sdc.readDomain(kv.AccountsDomain, plainKey)
	if err != nil {
		return nil, err
	}

	u = &Update{CodeHash: empty.CodeHash}
	if len(encAccount) == 0 {
		u.Flags = DeleteUpdate
		return u, nil
	}

	acc := new(accounts.Account)
	if err = accounts.DeserialiseV3(acc, encAccount); err != nil {
		return nil, err
	}

	u.Flags |= NonceUpdate
	u.Nonce = acc.Nonce

	u.Flags |= BalanceUpdate
	u.Balance = acc.Balance

	if !acc.CodeHash.IsZero() {
		u.Flags |= CodeUpdate
		u.CodeHash = acc.CodeHash.Value()
	}

	if assert.Enable { // verify code hash from account encoding matches stored code
		code, _, err := sdc.readDomain(kv.CodeDomain, plainKey)
		if err != nil {
			return nil, err
		}
		if len(code) > 0 {
			copy(u.CodeHash[:], crypto.Keccak256(code))
			u.Flags |= CodeUpdate
		}
		if acc.CodeHash.Value() != u.CodeHash {
			return nil, fmt.Errorf("code hash mismatch: account '%x' != codeHash '%x'", acc.CodeHash, u.CodeHash[:])
		}
	}
	return u, nil
}

func (sdc *TrieContext) Storage(plainKey []byte) (u *Update, err error) {
	enc, _, err := sdc.readDomain(kv.StorageDomain, plainKey)
	if err != nil {
		return nil, err
	}
	u = &Update{
		Flags:      DeleteUpdate,
		StorageLen: int8(len(enc)),
	}

	if u.StorageLen > 0 {
		u.Flags = StorageUpdate
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
