package state

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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	witnesstypes "github.com/erigontech/erigon/execution/types/witness"
)

type SharedDomainsCommitmentContext struct {
	//mu            sync.Mutex // protects reads from sharedDomains when trie is concurrent
	sharedDomains *SharedDomains
	mainTtx       *TrieContext

	updates      *commitment.Updates
	patriciaTrie commitment.Trie
	justRestored atomic.Bool // set to true when commitment trie was just restored from snapshot

	trace bool
}

// Limits max txNum for read operations. If set to 0, all read operations will be from latest value.
// If domainOnly=true and txNum > 0, then read operations will be limited to domain files only.
func (sdc *SharedDomainsCommitmentContext) SetLimitReadAsOfTxNum(txNum uint64, domainOnly bool) {
	sdc.mainTtx.SetLimitReadAsOfTxNum(txNum, domainOnly)
}

func NewSharedDomainsCommitmentContext(sd *SharedDomains, tx kv.TemporalTx, mode commitment.Mode, trieVariant commitment.TrieVariant, tmpDir string) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sharedDomains: sd,
	}

	ctx.patriciaTrie, ctx.updates = commitment.InitializeTrieAndUpdates(trieVariant, mode, tmpDir)
	trieCtx := &TrieContext{
		roTtx:  tx,
		getter: sd.AsGetter(tx),
		putter: sd.AsPutDel(tx),

		stepSize: sd.StepSize(),
	}
	ctx.mainTtx = trieCtx
	ctx.patriciaTrie.ResetContext(trieCtx)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
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

func (sdc *SharedDomainsCommitmentContext) Witness(ctx context.Context, codeReads map[common.Hash]witnesstypes.CodeWithHash, expectedRoot []byte, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, codeReads, expectedRoot, logPrefix)
	}

	return nil, nil, errors.New("shared domains commitment context doesn't have HexPatriciaHashed")
}

// Evaluates commitment for gathered updates.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctx context.Context, saveState bool, blockNum uint64, txNum uint64, logPrefix string) (rootHash []byte, err error) {
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
	sdc.Reset()

	rootHash, err = sdc.patriciaTrie.Process(ctx, sdc.updates, logPrefix)
	if err != nil {
		return nil, err
	}
	sdc.justRestored.Store(false)

	if saveState {
		if err = sdc.encodeAndStoreCommitmentState(blockNum, txNum, rootHash); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

// by that key stored latest root hash and tree state
const keyCommitmentStateS = "state"

var keyCommitmentState = []byte(keyCommitmentStateS)

var ErrBehindCommitment = errors.New("behind commitment")

func _decodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:16])
}

// LatestCommitmentState searches for last encoded state for CommitmentContext.
// Found value does not become current state.
func (sdc *SharedDomainsCommitmentContext) LatestCommitmentState() (blockNum, txNum uint64, state []byte, err error) {
	if sdc.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie && sdc.patriciaTrie.Variant() != commitment.VariantConcurrentHexPatricia {
		return 0, 0, nil, errors.New("state storing is only supported hex patricia trie")
	}
	state, _, err = sdc.mainTtx.Branch(keyCommitmentState)
	if err != nil {
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
	_, _, state, err := sdc.LatestCommitmentState()
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
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution")) //TODO: move stages to erigon-lib
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
func (sdc *SharedDomainsCommitmentContext) encodeAndStoreCommitmentState(blockNum, txNum uint64, rootHash []byte) error {
	if sdc.mainTtx == nil {
		return errors.New("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, txNum)
	if err != nil {
		return err
	}
	prevState, prevStep, err := sdc.mainTtx.Branch(keyCommitmentState)
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
	return sdc.mainTtx.PutBranch(keyCommitmentState, encodedState, prevState, prevStep)
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
	return sdc.ComputeCommitment(ctx, true, blockNum, txNum, "rebuild commit")
}

type TrieContext struct {
	roTtx  kv.TemporalTx
	getter kv.TemporalGetter
	putter kv.TemporalPutDel
	txNum  uint64

	limitReadAsOfTxNum uint64
	stepSize           uint64
	withHistory        bool // if true, do not use history reader and limit to domain files only
	trace              bool
}

func (sdc *TrieContext) Branch(pref []byte) ([]byte, kv.Step, error) {
	return sdc.readDomain(kv.CommitmentDomain, pref)
}

func (sdc *TrieContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep kv.Step) error {
	if sdc.limitReadAsOfTxNum > 0 && sdc.withHistory { // do not store branches if explicitly operate on history
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

	if sdc.limitReadAsOfTxNum > 0 {
		if sdc.withHistory {
			enc, _, err = sdc.roTtx.GetAsOf(d, plainKey, sdc.limitReadAsOfTxNum)
		}

		if enc == nil {
			var ok bool
			// reading from domain files this way will dereference domain key correctly,
			// rotx.GetAsOf itself does not dereference keys in commitment domain values
			enc, ok, _, _, err = sdc.roTtx.Debug().GetLatestFromFiles(d, plainKey, sdc.limitReadAsOfTxNum)
			if !ok {
				enc = nil
			}
		}
		if err != nil {
			return nil, 0, fmt.Errorf("readDomain %q: (limitTxNum=%d): %w", d, sdc.limitReadAsOfTxNum, err)
		}
	}

	if enc == nil {
		enc, step, err = sdc.getter.GetLatest(d, plainKey)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("readDomain %q: %w", d, err)
	}
	return enc, step, nil
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
	u.Balance.Set(&acc.Balance)

	if ch := acc.CodeHash.Bytes(); len(ch) > 0 {
		u.Flags |= commitment.CodeUpdate
		copy(u.CodeHash[:], acc.CodeHash.Bytes())
	}

	if assert.Enable {
		code, _, err := sdc.readDomain(kv.CodeDomain, plainKey)
		if err != nil {
			return nil, err
		}
		if len(code) > 0 {
			copy(u.CodeHash[:], crypto.Keccak256(code))
			u.Flags |= commitment.CodeUpdate
		}
		if !bytes.Equal(acc.CodeHash.Bytes(), u.CodeHash[:]) {
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
		StorageLen: len(enc),
	}

	if u.StorageLen > 0 {
		u.Flags = commitment.StorageUpdate
		copy(u.Storage[:u.StorageLen], enc)
	}

	return u, nil
}

// Limits max txNum for read operations. If set to 0, all read operations will be from latest value.
// If domainOnly=true and txNum > 0, then read operations will be limited to domain files only.
func (sdc *TrieContext) SetLimitReadAsOfTxNum(txNum uint64, domainOnly bool) {
	sdc.limitReadAsOfTxNum = txNum
	sdc.withHistory = !domainOnly
}
