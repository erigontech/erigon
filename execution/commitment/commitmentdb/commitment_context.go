package commitmentdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
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
	SetBlockNum(blockNum uint64)
	SetTxNum(blockNum uint64)
	AsGetter(tx kv.TemporalTx) kv.TemporalGetter
	AsPutDel(tx kv.TemporalTx) kv.TemporalPutDel
	StepSize() uint64
	TxNum() uint64

	Trace() bool
	CommitmentCapture() bool
}

type StateReader interface {
	WithHistory() bool
	CheckDataAvailable(d kv.Domain, step kv.Step) error
	Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error)
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

func (r *LatestStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	enc, step, err = r.getter.GetLatest(d, plainKey)
	if err != nil {
		return nil, 0, fmt.Errorf("LatestStateReader(GetLatest) %q: %w", d, err)
	}
	return enc, step, nil
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

func (r *HistoryStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	enc, _, err = r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
	if err != nil {
		return enc, 0, fmt.Errorf("HistoryStateReader(GetAsOf) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, err)
	}
	return enc, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
}

// LimitedHistoryStateReader reads from *limited* (i.e. *without-recent-files*) state at specified txNum, otherwise from *latest*.
// `limitReadAsOfTxNum` here is used for unusual operation: "hide recent .kv files and read the latest state from files".
type LimitedHistoryStateReader struct {
	HistoryStateReader
	getter kv.TemporalGetter
}

func NewLimitedHistoryStateReader(roTx kv.TemporalTx, getter kv.TemporalGetter, limitReadAsOfTxNum uint64) *LimitedHistoryStateReader {
	return &LimitedHistoryStateReader{
		HistoryStateReader: HistoryStateReader{
			roTx:               roTx,
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
func (r *LimitedHistoryStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
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
		step = kv.Step(endTxNum / stepSize)
	}
	if enc == nil {
		enc, step, err = r.getter.GetLatest(d, plainKey)
		if err != nil {
			return nil, 0, fmt.Errorf("LimitedHistoryStateReader(GetLatest) %q: %w", d, err)
		}
	}
	return enc, step, nil
}

type SharedDomainsCommitmentContext struct {
	sharedDomains sd
	updates       *commitment.Updates
	patriciaTrie  commitment.Trie
	justRestored  atomic.Bool // set to true when commitment trie was just restored from snapshot
	trace         bool
	stateReader   StateReader
}

// SetStateReader can be used to set a custom state reader (otherwise the default one is set in SharedDomainsCommitmentContext.trieContext).
func (sdc *SharedDomainsCommitmentContext) SetStateReader(stateReader StateReader) {
	sdc.stateReader = stateReader
}

// SetHistoryStateReader sets the state reader to read *full* historical state at specified txNum.
func (sdc *SharedDomainsCommitmentContext) SetHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewHistoryStateReader(roTx, limitReadAsOfTxNum))
}

// SetLimitedHistoryStateReader sets the state reader to read *limited* (i.e. *without-recent-files*) historical state at specified txNum.
func (sdc *SharedDomainsCommitmentContext) SetLimitedHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) {
	sdc.SetStateReader(NewLimitedHistoryStateReader(roTx, sdc.sharedDomains.AsGetter(roTx), limitReadAsOfTxNum))
}

func (sdc *SharedDomainsCommitmentContext) SetTrace(trace bool) {
	sdc.trace = trace
	sdc.patriciaTrie.SetTrace(trace)
}

func NewSharedDomainsCommitmentContext(sd sd, mode commitment.Mode, trieVariant commitment.TrieVariant, tmpDir string) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
	}
	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, tmpDir)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) trieContext(tx kv.TemporalTx) *TrieContext {
	mainTtx := &TrieContext{
		roTtx:    tx,
		getter:   sdc.sharedDomains.AsGetter(tx),
		putter:   sdc.sharedDomains.AsPutDel(tx),
		stepSize: sdc.sharedDomains.StepSize(),
		txNum:    sdc.sharedDomains.TxNum(),
	}
	if sdc.stateReader != nil {
		mainTtx.stateReader = sdc.stateReader
	} else {
		mainTtx.stateReader = &LatestStateReader{mainTtx.getter}
	}
	sdc.patriciaTrie.ResetContext(mainTtx)
	return mainTtx
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
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

// Evaluates commitment for gathered updates.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, tx kv.TemporalTx, saveState bool, blockNum uint64, txNum uint64, logPrefix string, commitProgress chan *commitment.CommitProgress) (rootHash []byte, err error) {
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
	sdc.patriciaTrie.SetTraceDomain(sdc.sharedDomains.Trace())
	if sdc.sharedDomains.CommitmentCapture() {
		if sdc.patriciaTrie.GetCapture(false) == nil {
			sdc.patriciaTrie.SetCapture([]string{})
		}
	}

	trieContext := sdc.trieContext(tx)
	sdc.Reset()

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix, commitProgress)
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
func (sdc *SharedDomainsCommitmentContext) SeekCommitment(ctx context.Context, tx kv.TemporalTx) (blockNum, txNum uint64, ok bool, err error) {
	trieContext := sdc.trieContext(tx)

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
		sdc.sharedDomains.SetBlockNum(blockNum)
		sdc.sharedDomains.SetTxNum(txNum)
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
		txNum, err = rawdbv3.TxNums.Max(tx, blockNum)
		if err != nil {
			return 0, 0, false, err
		}
	}
	sdc.sharedDomains.SetBlockNum(blockNum)
	sdc.sharedDomains.SetTxNum(txNum)
	if blockNum == 0 && txNum == 0 {
		return 0, 0, true, nil
	}
	//
	//newRh, err := sdc.rebuildCommitment(ctx, tx, blockNum, txNum)
	//if err != nil {
	//	return 0, 0, false, err
	//}
	//if bytes.Equal(newRh, empty.RootHash.Bytes()) {
	//	sdc.sharedDomains.SetBlockNum(0)
	//	sdc.sharedDomains.SetTxNum(0)
	//	return 0, 0, false, err
	//}
	//if sdc.trace {
	//	fmt.Printf("rebuilt commitment %x bn=%d txn=%d\n", newRh, blockNum, txNum)
	//}
	if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
		return 0, 0, false, err
	}
	return blockNum, txNum, true, nil
}

// encodes current trie state and saves it in SharedDomains
func (sdc *SharedDomainsCommitmentContext) encodeAndStoreCommitmentState(trieContext *TrieContext, blockNum, txNum uint64, rootHash []byte) error {
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
		//fmt.Printf("[commitment] skip store txn %d block %d (prev b=%d t=%d) rh %x\n",
		//	binary.BigEndian.Uint64(prevState[8:16]), binary.BigEndian.Uint64(prevState[:8]), dc.ht.iit.txNum, blockNum, rh)
		return nil
	}

	log.Debug("[commitment] store state", "block", blockNum, "txNum", txNum, "rootHash", hex.EncodeToString(rootHash))
	return trieContext.PutBranch(KeyCommitmentState, encodedState, prevState, prevStep)
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

// Dummy way to rebuild commitment. Dummy because works for small state only.
// To rebuild commitment correctly for any state size - use RebuildCommitmentFiles.
func (sdc *SharedDomainsCommitmentContext) rebuildCommitment(ctx context.Context, roTx kv.TemporalTx, blockNum, txNum uint64) ([]byte, error) {
	it, err := roTx.HistoryRange(kv.StorageDomain, int(txNum), math.MaxInt64, order.Asc, -1)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sdc.TouchKey(kv.AccountsDomain, string(k), nil)
	}

	it, err = roTx.HistoryRange(kv.StorageDomain, int(txNum), math.MaxInt64, order.Asc, -1)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sdc.TouchKey(kv.StorageDomain, string(k), nil)
	}

	sdc.Reset()
	return sdc.ComputeCommitment(ctx, roTx, true, blockNum, txNum, "rebuild commit", nil)
}

type TrieContext struct {
	roTtx  kv.TemporalTx
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

	if ch := acc.CodeHash.Bytes(); len(ch) > 0 {
		u.Flags |= commitment.CodeUpdate
		u.CodeHash = acc.CodeHash
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
		if acc.CodeHash != u.CodeHash {
			return nil, fmt.Errorf("code hash mismatch: account '%x' != codeHash '%x'", acc.CodeHash.Bytes(), u.CodeHash[:])
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
