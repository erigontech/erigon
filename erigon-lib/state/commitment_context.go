package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/erigontech/erigon-lib/common/length"
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types/accounts"
)

type SharedDomainsCommitmentContext struct {
	//mu            sync.Mutex // protects reads from sharedDomains when trie is concurrent
	sharedDomains *SharedDomains
	mainTtx       *TrieContext
	subTtx        [16]*CursorContext
	warmupTries   [16]commitment.Trie

	updates      *commitment.Updates
	patriciaTrie commitment.Trie
	justRestored atomic.Bool // set to true when commitment trie was just restored from snapshot

	trace bool
}

func (sdc *SharedDomainsCommitmentContext) SetTtx(i int, tx *AggregatorRoTx, c kv.Cursor) {
	if i < 0 || i >= len(sdc.subTtx) {
		panic(fmt.Sprintf("index %d out of range", i))
	}
	sdc.subTtx[i] = &CursorContext{
		c:     c,                 // cursor out of apply txn
		aggTx: tx,                // separate aggtx
		sd:    sdc.sharedDomains, //to read from sd cache as well and centralised writes

		limitReadAsOfTxNum: sdc.mainTtx.limitReadAsOfTxNum,
		stepSize:           sdc.mainTtx.stepSize,
		domainsOnly:        sdc.mainTtx.domainsOnly,
		trace:              sdc.mainTtx.trace,
	}
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
	if commitment.COM_WARMUP {
		ctx.updates.Warmup = func(hashedKey []byte) error {
			if hashedKey[0] > 16 {
				return nil
			}
			n0 := hashedKey[0]
			if ctx.warmupTries[n0] != nil {
				err := ctx.warmupTries[n0].Warmup(ctx.subTtx[n0], hashedKey)
				if err != nil {
					return err
				}
			}
			return nil
		}

		ctx.openSubTtx(sd)
		// } else {
		// 	ctx.updates.Warmup = func(hashedKey []byte) error {
		// 	}
		//
	}
	ctx.patriciaTrie.ResetContext(trieCtx)
	return ctx
}

func (sdc *SharedDomainsCommitmentContext) openSubTtx(sd *SharedDomains) {
	for i := 0; i < len(sdc.subTtx); i++ {
		AggTx(sd.)
		agg := sd.roTtx.AggTx().(*AggregatorRoTx).Agg()
		if agg == nil {
			panic("agg is nil")
		}
		actx := agg.BeginFilesRo()
		var c kv.Cursor
		var err error
		if Schema.CommitmentDomain.largeValues {
			c, err = sd.roTtx.Cursor(Schema.CommitmentDomain.valuesTable)
		} else {
			c, err = sd.roTtx.CursorDupSort(Schema.CommitmentDomain.valuesTable)
		}
		if err != nil {
			panic(fmt.Sprintf("failed to create cursor for commitment domain: %v", err))
		}

		sdc.subTtx[i] = &CursorContext{
			c:                  c,
			roTtx:              sd.roTtx,
			aggTx:              actx,
			sd:                 sd,
			limitReadAsOfTxNum: sdc.mainTtx.limitReadAsOfTxNum,
			stepSize:           sdc.mainTtx.stepSize,
			domainsOnly:        sdc.mainTtx.domainsOnly,
			trace:              sdc.mainTtx.trace,
		}
		sdc.warmupTries[i] = commitment.NewHexPatriciaHashed(length.Addr, sdc.subTtx[i])
	}
}

func (sdc *SharedDomainsCommitmentContext) Close() {
	sdc.updates.Close()
	for i := 0; i < len(sdc.subTtx); i++ {
		if sdc.subTtx[i] != nil {
			sdc.subTtx[i].Close()
			//sdc.subTtx[i].roTtx.Rollback()
		}
	}
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
	for i := 0; i < len(sdc.subTtx); i++ {
		if sdc.subTtx[i] != nil {
			sdc.subTtx[i].Close()
		}
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

func (sdc *SharedDomainsCommitmentContext) Witness(ctx context.Context, expectedRoot []byte, logPrefix string) (proofTrie *trie.Trie, rootHash []byte, err error) {
	hexPatriciaHashed, ok := sdc.Trie().(*commitment.HexPatriciaHashed)
	if ok {
		return hexPatriciaHashed.GenerateWitness(ctx, sdc.updates, nil, expectedRoot, logPrefix)
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
		return 0, 0, nil, fmt.Errorf("state storing is only supported hex patricia trie")
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

	newRh, err := sdc.rebuildCommitment(ctx, tx, blockNum, txNum)
	if err != nil {
		return 0, 0, false, err
	}
	if bytes.Equal(newRh, empty.RootHash.Bytes()) {
		sdc.sharedDomains.SetBlockNum(0)
		sdc.sharedDomains.SetTxNum(0)
		return 0, 0, false, err
	}
	if sdc.trace {
		fmt.Printf("rebuilt commitment %x bn=%d txn=%d\n", newRh, blockNum, txNum)
	}
	if err = sdc.enableConcurrentCommitmentIfPossible(); err != nil {
		return 0, 0, false, err
	}
	return blockNum, txNum, true, nil
}

// encodes current trie state and saves it in SharedDomains
func (sdc *SharedDomainsCommitmentContext) encodeAndStoreCommitmentState(blockNum, txNum uint64, rootHash []byte) error {
	if sdc.mainTtx == nil {
		return fmt.Errorf("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, txNum)
	if err != nil {
		return err
	}
	for ti := 0; ti < len(sdc.warmupTries); ti++ {
		if sdc.warmupTries[ti] == nil {
			continue
		}
		hph := sdc.warmupTries[ti].(*commitment.HexPatriciaHashed)
		if err := hph.SetState(encodedState); err != nil {
			return fmt.Errorf("failed to set state for warmup trie %d: %w", ti, err)
		}
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

	log.Debug("[commitment] store state", "block", blockNum, "txNum", txNum, "rootHash", fmt.Sprintf("%x", rootHash))
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
	for ti := 0; ti < len(sdc.warmupTries); ti++ {
		if sdc.warmupTries[ti] != nil {
			wti := sdc.warmupTries[ti].(*commitment.HexPatriciaHashed)
			if err := wti.SetState(cs.trieState); err != nil {
				return 0, 0, fmt.Errorf("failed restore state for warmup trie %d: %w", ti, err)
			}
		}
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
	domainsOnly        bool // if true, do not use history reader and limit to domain files only
	trace              bool
}

func (sdc *TrieContext) Branch(pref []byte) ([]byte, uint64, error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}
	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update.
	// Keep dereferenced version inside sd commitmentDomain map ready to read again
	if !sdc.domainsOnly && sdc.limitReadAsOfTxNum > 0 {
		branch, _, err := sdc.roTtx.GetAsOf(kv.CommitmentDomain, pref, sdc.limitReadAsOfTxNum)
		if sdc.trace {
			fmt.Printf("[SDC] Branch @%d: %x: %x\n%s\n", sdc.limitReadAsOfTxNum, pref, branch, commitment.BranchData(branch).String())
		}
		if err != nil {
			return nil, 0, fmt.Errorf("branch history read failed: %w", err)
		}
		return branch, sdc.limitReadAsOfTxNum / sdc.stepSize, nil
	}

	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update.
	// Dereferenced branch is kept inside sharedDomains commitment domain map (but not written into buffer so not flushed into db, unless updated)
	v, step, err := sdc.getter.GetLatest(kv.CommitmentDomain, pref)
	if err != nil {
		return nil, 0, fmt.Errorf("branch failed: %w", err)
	}
	if sdc.trace {
		fmt.Printf("[SDC] Branch: %x: %x\n", pref, v)
	}
	if len(v) == 0 {
		return nil, 0, nil
	}
	return v, step, nil
}

func (sdc *TrieContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error {
	if sdc.limitReadAsOfTxNum > 0 && !sdc.domainsOnly { // do not store branches if explicitly operate on history
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

func (sdc *TrieContext) readDomain(d kv.Domain, plainKey []byte) (enc []byte, err error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}

	if sdc.limitReadAsOfTxNum > 0 {
		if sdc.domainsOnly {
			var ok bool
			enc, ok, _, _, err = sdc.roTtx.Debug().GetLatestFromFiles(d, plainKey, sdc.limitReadAsOfTxNum)
			if !ok {
				enc = nil
			}
		} else {
			enc, _, err = sdc.roTtx.GetAsOf(d, plainKey, sdc.limitReadAsOfTxNum)
		}
	} else {
		enc, _, err = sdc.getter.GetLatest(d, plainKey)
	}

	if err != nil {
		return nil, fmt.Errorf("readDomain %q: failed to read latest storage (latest=%t): %w", d, sdc.limitReadAsOfTxNum == 0, err)
	}
	return enc, nil
}

func (sdc *TrieContext) Account(plainKey []byte) (u *commitment.Update, err error) {
	encAccount, err := sdc.readDomain(kv.AccountsDomain, plainKey)
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
		code, err := sdc.readDomain(kv.CodeDomain, plainKey)
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
	enc, err := sdc.readDomain(kv.StorageDomain, plainKey)
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
	sdc.domainsOnly = domainOnly
}

type CursorContext struct {
	c                  kv.Cursor
	roTtx              kv.TemporalTx
	aggTx              *AggregatorRoTx
	sd                 *SharedDomains
	limitReadAsOfTxNum uint64
	stepSize           uint64
	domainsOnly        bool // if true, do not use history reader and limit to domain files only
	trace              bool
}

func (sdc *CursorContext) Close() {
	sdc.c.Close()
	//sdc.roTtx.Rollback()
	sdc.aggTx.Close()
}

func (sdc *CursorContext) Storage(plainKey []byte) (u *commitment.Update, err error) {
	enc, err := sdc.readDomain(kv.StorageDomain, plainKey)
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

func (sdc *CursorContext) SetLimitReadAsOfTxNum(txNum uint64, domainOnly bool) {
	sdc.limitReadAsOfTxNum = txNum
	sdc.domainsOnly = domainOnly
}

func (sdc *CursorContext) Branch(pref []byte) ([]byte, uint64, error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}
	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update.
	// Keep dereferenced version inside sd commitmentDomain map ready to read again
	if !sdc.domainsOnly && sdc.limitReadAsOfTxNum > 0 {
		branch, _, err := sdc.roTtx.GetAsOf(kv.CommitmentDomain, pref, sdc.limitReadAsOfTxNum)
		if sdc.trace {
			fmt.Printf("[SDC] Branch @%d: %x: %x\n%s\n", sdc.limitReadAsOfTxNum, pref, branch, commitment.BranchData(branch).String())
		}
		if err != nil {
			return nil, 0, fmt.Errorf("branch history read failed: %w", err)
		}
		return branch, sdc.limitReadAsOfTxNum / sdc.stepSize, nil
	}

	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update.
	// Dereferenced branch is kept inside sharedDomains commitment domain map (but not written into buffer so not flushed into db, unless updated)
	v, step, err := sdc.sd.GetWithCursor(kv.CommitmentDomain, pref, sdc.c, sdc.aggTx)
	if err != nil {
		return nil, 0, fmt.Errorf("branch failed: %w", err)
	}
	if sdc.trace {
		fmt.Printf("[SDC] Branch: %x: %x\n", pref, v)
	}
	if len(v) == 0 {
		return nil, 0, nil
	}
	return v, step, nil
}

func (sdc *CursorContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error {
	panic("put branch")
	if sdc.limitReadAsOfTxNum > 0 && !sdc.domainsOnly { // do not store branches if explicitly operate on history
		return nil
	}
	if sdc.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}

	return sdc.sd.DomainPut(kv.CommitmentDomain, nil, prefix, data, sdc.sd.txNum, prevData, prevStep)
}

func (sdc *CursorContext) readDomain(d kv.Domain, plainKey []byte) (enc []byte, err error) {
	//if sdc.patriciaTrie.Variant() == commitment.VariantConcurrentHexPatricia {
	//	sdc.mu.Lock()
	//	defer sdc.mu.Unlock()
	//}

	if sdc.limitReadAsOfTxNum > 0 {
		if sdc.domainsOnly {
			var ok bool
			enc, ok, _, _, err = sdc.roTtx.Debug().GetLatestFromFiles(d, plainKey, sdc.limitReadAsOfTxNum)
			if !ok {
				enc = nil
			}
		} else {
			enc, _, err = sdc.roTtx.GetAsOf(d, plainKey, sdc.limitReadAsOfTxNum)
		}
	} else {
		// cursor should be of correct table,but we are using only Commitment
		enc, _, err = sdc.roTtx.GetLatest(d, plainKey)
		//enc, _, err = sdc.sd.GetLatest(d, plainKey, sdc.c, sdc.aggTx)
	}

	if err != nil {
		return nil, fmt.Errorf("readDomain %q: failed to read latest storage (latest=%t): %w", d, sdc.limitReadAsOfTxNum == 0, err)
	}
	return enc, nil
}

func (sdc *CursorContext) Account(plainKey []byte) (u *commitment.Update, err error) {
	encAccount, err := sdc.readDomain(kv.AccountsDomain, plainKey)
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
		code, err := sdc.readDomain(kv.CodeDomain, plainKey)
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
