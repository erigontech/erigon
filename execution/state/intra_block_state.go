// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

var _ evmtypes.IntraBlockState = new(IntraBlockState) // compile-time interface-check

type revision struct {
	id           int
	journalIndex int
	logCount     int
}

type revisions struct {
	nextId int
	valid  []revision
	buf    [16]revision
}

func (r *revisions) init() {
	r.valid = r.buf[:0]
}

func (r *revisions) snapshot(journal *journal, logCount int) int {
	id := r.nextId
	r.nextId++
	r.valid = append(r.valid, revision{id, journal.length(), logCount})
	return id
}

func (r *revisions) returnSnapshot(id int) {
	if lv := len(r.valid); lv > 0 && r.valid[lv-1].id == id {
		r.valid = r.valid[0 : lv-1]
		if r.nextId == id+1 {
			r.nextId = id
		}
	}
}

func (r *revisions) reset() {
	if cap(r.valid) > maxRetainedRevisionsCap {
		r.valid = r.buf[:0]
	} else {
		r.valid = r.valid[:0]
	}
	r.nextId = 0
}

func (r *revisions) revertToSnapshot(revid int) revision {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(r.valid), func(i int) bool {
		return r.valid[i].id >= revid
	})
	if idx == len(r.valid) || r.valid[idx].id != revid {
		var id int
		if idx < len(r.valid) {
			id = r.valid[idx].id
		}
		panic(fmt.Errorf("revision id %v cannot be reverted (idx=%v,len=%v,id=%v)", revid, idx, len(r.valid), id))
	}
	snapshot := r.valid[idx]
	r.valid = r.valid[:idx]
	if r.nextId == snapshot.id+1 {
		r.nextId = snapshot.id
	}
	return snapshot
}

// A revisions slice grown beyond this cap indicates broken push/pop discipline;
// reset falls back to the inline buf rather than retaining it for the IBS lifetime.
const maxRetainedRevisionsCap = 2048

// BalanceIncrease represents the increase of balance of an account that did not require
// reading the account first
type BalanceIncrease struct {
	increase    uint256.Int
	transferred bool // Set to true when the corresponding stateObject is created and balance increase is transferred to the stateObject
	count       int  // Number of increases - this needs tracking for proper reversion
}

type accessOptions struct {
	revertable bool
}

type AccessSet map[accounts.Address]accessOptions

func (aa AccessSet) Merge(other AccessSet) AccessSet {
	if len(other) == 0 {
		return aa
	}
	dst := make(AccessSet, len(aa)+len(other))
	maps.Copy(dst, aa)
	maps.Copy(dst, other)
	return dst
}

// IntraBlockState is responsible for caching and managing state changes
// that occur during block's execution.
// NOT THREAD SAFE!
type IntraBlockState struct {
	stateReader StateReader
	codeAccess  codeAccessTracker // stateReader, if it tracks code access

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects      map[accounts.Address]*stateObject
	stateObjectsDirty map[accounts.Address]struct{}

	nilAccounts map[accounts.Address]struct{} // Remember non-existent account to avoid reading them again

	// The refund counter, also used by state transitioning.
	refund uint64

	txIndex  int
	blockNum uint64
	logs     logArena

	txOutput     []byte
	txOutputFree bool

	// Per-transaction access list
	accessList accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal          *journal
	stateObjectArena stateObjectArena // same lifetime with `journal`. used only if `noMaterialize == true`
	trace            bool
	tracingHooks     *tracing.Hooks
	balanceInc       map[accounts.Address]*BalanceIncrease // Map of balance increases (without first reading the account)
	recordAccess     bool                                  // gates MarkAddressAccess — enabled in Prepare

	// Versioned storage used for parallel tx processing, versions
	// are maintaned across transactions until they are reset
	// at the block level.
	versionMap *VersionMap
	// waitCommit blocks a read that observes an in-flight (estimate) predecessor
	// write until that predecessor commits, so execution pauses in place instead
	// of aborting via ErrDependency. Returns false on shutdown.
	waitCommit func(depTxIndex int) bool
	// wakeGen counts resumes from a dep-wait pause. A lower-txIndex writer may
	// commit a new value while this worker sleeps in waitCommit, so any frame
	// read cached before the pause can be stale on wake. ReadStamp folds wakeGen
	// in, so a resume invalidates the vm's frame caches on the next lookup — what
	// makes those caches safe under the dep-wait executor.
	wakeGen         uint64
	versionedWrites WriteSet
	versionedReads  ReadSet
	// versionedOrigins collects committed pre-block base cells (at originIndex) that a cold
	// read resolves this tx. Reads record here (tx-local, lock-free) instead of writing the
	// shared versionMap mid-execution; the executor publishes them batched at flush, so the
	// Block-STM invariant holds (versionMap mutated only at pre-exec seed and flush).
	versionedOrigins WriteSet
	// committedBase memoizes the per-tx committed (pre-block) account fallback for
	// versionedAccountBase. The base reader can serve the published writes of lower txs,
	// so the memo keeps the attempt's view consistent; its read-only callers share the
	// cached pointer. Reset per tx.
	committedBase       map[accounts.Address]*accounts.Account
	accountReadDuration time.Duration
	accountReadCount    int64
	storageReadDuration time.Duration
	storageReadCount    int64
	codeReadDuration    time.Duration
	codeReadCount       int64
	version             Incarnation
	dep                 int
	stateReadErr        error

	// Per-attempt memo of the shared-versionMap SelfDestruct probe: it reads only
	// prior-tx SD writes, stable within one execution attempt. sdProbeEpoch is
	// bumped on Reset/SetTxContext to discard the memo without a per-tx map clear.
	sdProbe      map[accounts.Address]sdProbeEntry
	sdProbeEpoch uint64

	// noMaterialize suppresses the stateObject cache on the parallel execution
	// path: create/write flows record only versioned cells and committed reads
	// resolve from the state reader. Left false for genesis/RPC/serial, which
	// still commit via FinalizeTx→so.data.
	noMaterialize       bool
	noConflictDetection bool

	revisions revisions

	lastCode accounts.Code // last code stored by SetCode
	// Intern tables survive Reset and the pool on purpose: an idle pooled state keeps ~51 KB of
	// tables and their unique handles alive, which is the reuse the next call pays for.
	internCache *storageKeyCache
	addrCache   *addressCache
	internOps   uint32
	addrOps     uint32
}

type sdProbeEntry struct {
	epoch      uint64
	res        ReadResult
	destructed bool
	ok         bool
}

// Create a new state from a given trie
func New(stateReader StateReader) *IntraBlockState {
	ibs := &IntraBlockState{
		stateReader:       stateReader,
		stateObjects:      map[accounts.Address]*stateObject{},
		stateObjectsDirty: map[accounts.Address]struct{}{},
		nilAccounts:       map[accounts.Address]struct{}{},
		journal:           newJournal(),
		accessList:        accessList{addresses: make(map[accounts.Address]int)},
		transientStorage:  newTransientStorage(),
		balanceInc:        map[accounts.Address]*BalanceIncrease{},
		recordAccess:      false,
		txIndex:           0,
		trace:             false,
		dep:               UnknownDep,
	}
	ibs.codeAccess, _ = stateReader.(codeAccessTracker)
	ibs.revisions.init()
	return ibs
}

func NewWithVersionMap(stateReader StateReader, mvhm *VersionMap) *IntraBlockState {
	ibs := New(stateReader)
	ibs.SetVersionMap(mvhm)
	return ibs
}

// NewForSequentialCommitment builds a versioned IBS that keeps the stateObject cache on; only genesis needs both.
func NewForSequentialCommitment(stateReader StateReader) *IntraBlockState {
	ibs := New(stateReader)
	ibs.versionMap = NewVersionMap(nil)
	ibs.noMaterialize = false
	return ibs
}

func (ibs *IntraBlockState) ReadDuration() time.Duration {
	return ibs.accountReadDuration + ibs.storageReadDuration + ibs.codeReadDuration
}

func (ibs *IntraBlockState) ReadCount() int64 {
	return ibs.accountReadCount + ibs.storageReadCount + ibs.codeReadCount
}

func (ibs *IntraBlockState) AccountReadDuration() time.Duration {
	return ibs.accountReadDuration
}

func (ibs *IntraBlockState) AccountReadCount() int64 {
	return ibs.accountReadCount
}

func (ibs *IntraBlockState) StorageReadDuration() time.Duration {
	return ibs.storageReadDuration
}

func (ibs *IntraBlockState) StorageReadCount() int64 {
	return ibs.storageReadCount
}

func (ibs *IntraBlockState) CodeReadDuration() time.Duration {
	return ibs.codeReadDuration
}

func (ibs *IntraBlockState) CodeReadCount() int64 {
	return ibs.codeReadCount
}

// SetVersionMap binds the IBS to a versionMap; noMaterialize follows versionMap presence.
func (ibs *IntraBlockState) SetVersionMap(versionMap *VersionMap) {
	noMaterialize := versionMap != nil
	if dbg.AssertEnabled && noMaterialize != ibs.noMaterialize && !ibs.stateObjectArena.empty() {
		panic("noMaterialize changed with arena slots outstanding")
	}
	ibs.versionMap = versionMap
	ibs.noMaterialize = noMaterialize
}

// StateReader returns the committed-base reader. The parallel executor uses it to
// retune a per-task overlay (PrevBlockReader.SetBlock) without rebuilding the IBS.
func (ibs *IntraBlockState) StateReader() StateReader {
	return ibs.stateReader
}

// SetWaitCommit installs the mid-flow dep-pause hook (self-loop). See waitCommit.
func (ibs *IntraBlockState) SetWaitCommit(f func(depTxIndex int) bool) {
	ibs.waitCommit = f
}

func (ibs *IntraBlockState) VersionMap() *VersionMap {
	return ibs.versionMap
}

func (ibs *IntraBlockState) IsVersioned() bool {
	return ibs.versionMap != nil
}

func (ibs *IntraBlockState) SetHooks(hooks *tracing.Hooks) {
	ibs.tracingHooks = hooks
}

func (ibs *IntraBlockState) SetTrace(trace bool) {
	ibs.trace = trace
}

func (ibs *IntraBlockState) hasWrite(addr accounts.Address, path AccountPath, key accounts.StorageKey) bool {
	return ibs.versionedWrites.Has(WriteHeader{Address: addr, Path: path, Key: key})
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (ibs *IntraBlockState) Reset() {
	clear(ibs.nilAccounts)
	ibs.lastCode = accounts.Code{}
	for _, so := range ibs.stateObjects {
		so.release()
	}
	clear(ibs.stateObjects)
	clear(ibs.stateObjectsDirty)
	ibs.logs.reset()
	clear(ibs.balanceInc)
	ibs.clearJournalAndRefund()
	ibs.txIndex = 0
	ibs.sdProbeEpoch++
	ibs.accessList.Reset()
	clear(ibs.transientStorage)
	ibs.versionMap = nil
	// noMaterialize is meaningful only alongside a versionMap; clear it with the
	// map so a reused IBS can't run unversioned with the stateObject cache still
	// suppressed (which would silently drop writes). The versioned worker re-sets
	// both right after Reset; the block assembler never calls Reset mid-block.
	ibs.noMaterialize = false
	ibs.noConflictDetection = false
	clear(ibs.committedBase)
	// Read side rebinds to a fresh empty set: VersionedReads() at end of
	// tx hands the per-path maps to result.TxIn, so rebinding leaves the
	// handed-over maps intact while the next tx lazily reallocs.
	ibs.versionedReads = ReadSet{}
	// Write side: VersionedWrites() returns Cloned snapshots, so the
	// originals in ibs.versionedWrites are no longer referenced after the
	// boundary call, so recycle can take every VW back.
	ibs.versionedWrites.recycle()
	ibs.versionedOrigins.ReleaseAndReset()
	ibs.recordAccess = false
	ibs.accountReadDuration = 0
	ibs.accountReadCount = 0
	ibs.storageReadDuration = 0
	ibs.storageReadCount = 0
	ibs.codeReadDuration = 0
	ibs.codeReadCount = 0
	ibs.dep = UnknownDep
	ibs.stateReadErr = nil
}

// statePool is sync.Pool's surface, so a test can substitute one that keeps
// what it is given: sync.Pool may discard any item.
type statePool interface {
	Get() any
	Put(any)
}

var ibsPool statePool = &sync.Pool{}

// NewPooled is New over an IntraBlockState reused from an earlier call, whose
// maps keep their capacity. Release it with ReleasePooled.
func NewPooled(stateReader StateReader) *IntraBlockState {
	ibs, ok := ibsPool.Get().(*IntraBlockState)
	if !ok {
		return New(stateReader)
	}
	ibs.stateReader = stateReader
	ibs.codeAccess, _ = stateReader.(codeAccessTracker)
	return ibs
}

// ReleasePooled hands ibs to the next NewPooled, or closes it when it grew too
// large to keep.
func ReleasePooled(ibs *IntraBlockState) {
	if ibs.stateObjects == nil { // closed: its maps are gone
		return
	}
	if ibs.resetForPool() {
		ibsPool.Put(ibs)
		return
	}
	ibs.Close()
}

// Maps never shrink, so a call that grew past this must not pin its capacity
// for every later one.
const maxPooledEntries = 16 * 1024

// resetForPool clears everything one call left and reports whether ibs is
// small enough to pool.
func (ibs *IntraBlockState) resetForPool() bool {
	reads := ibs.versionedReads
	poolable := len(ibs.stateObjects)+len(ibs.nilAccounts)+ibs.accessList.entries()+reads.entries() <= maxPooledEntries
	ibs.Reset()
	if !poolable {
		return false
	}
	// One call never hands its read set out, so the maps keep their capacity
	// instead of the empty set Reset installs.
	reads.clearForReuse()
	ibs.versionedReads = reads
	// Reset only bumps the probe epoch; a pooled ibs would collect every
	// address later calls touch.
	clear(ibs.sdProbe)
	ibs.tracingHooks = nil
	ibs.trace = false
	// The caller reads this call's output after the ibs goes back to the pool.
	ibs.txOutput = nil
	ibs.stateReader, ibs.codeAccess = nil, nil
	// Reset keeps the tx context; New starts it at zero.
	ibs.blockNum, ibs.version = 0, 0
	return true
}

// SetNoConflictDetection marks an execution that neither ValidateVersion checks
// nor a block access list is built from, such as eth_call. CreateAccount's
// balance read serves both, so skipping it needs both to be absent. Reset
// clears it.
func (ibs *IntraBlockState) SetNoConflictDetection() { ibs.noConflictDetection = true }

// Release Deprecated use Close
func (ibs *IntraBlockState) Release(bool) { ibs.Close() }

// Close returns pooled resources (like journal, stateObjects, versioned writes)
// back to their pools. Call this when the IntraBlockState is no longer needed.
// Call Reset() to re-use IntraBlockState object
// Idempotent, thread-unsafe
func (ibs *IntraBlockState) Close() {
	if ibs == nil || ibs.stateObjects == nil {
		return
	}

	stateObjects, journal := ibs.stateObjects, ibs.journal
	ibs.stateObjects, ibs.journal = nil, nil
	ibs.stateObjectArena.release()
	ibs.logs.release()
	ibs.revisions.reset()
	// Safe to pool: VersionedWrites/FinalizedWrites hand out deep clones, and the
	// set is unexported, so nothing outside holds a raw VersionedWrite.
	ibs.versionedWrites.ReleaseAndReset()
	ibs.versionedOrigins.ReleaseAndReset()

	releaseResources(stateObjects, journal)
}

// The noMaterialize path never releases what it takes, so a pool draw there
// would be a one-way drain on the materializing paths.
func (ibs *IntraBlockState) allocStateObject() *stateObject {
	if ibs.noMaterialize {
		if so := ibs.stateObjectArena.alloc(); so != nil {
			return so
		}
		return newHeapObject()
	}
	return stateObjectPool.Get().(*stateObject)
}

func releaseResources(stateObjects map[accounts.Address]*stateObject, journal *journal) {
	for _, so := range stateObjects {
		so.release()
	}
	if journal != nil {
		journal.release()
	}
}

// TxOutputBuffer gives the first top-level frame after Prepare a buffer for its
// output that the next transaction reuses, and nil to any other caller. Whoever
// keeps a transaction's output past the next Prepare must copy it.
func (ibs *IntraBlockState) TxOutputBuffer() *[]byte {
	if !ibs.txOutputFree {
		return nil
	}
	ibs.txOutputFree = false
	if cap(ibs.txOutput) > maxKeptTxOutput {
		ibs.txOutput = nil
	}
	return &ibs.txOutput
}

// maxKeptTxOutput bounds the output buffer one transaction leaves to the next.
const maxKeptTxOutput = int(datasize.MB)

// AllocLog reserves the next log slot of the current tx and returns it sized for
// numTopics/dataSize. The caller must write every topic and every data byte, then
// call NotifyLog; whatever it leaves unwritten belongs to whichever transaction
// held the entry before. The entry is owned by the arena and handed to a later
// transaction, so it must never be passed on without copying.
func (ibs *IntraBlockState) AllocLog(addr common.Address, numTopics, dataSize int) *types.Log {
	return ibs.logs.alloc(addr, ibs.txIndex, numTopics, dataSize)
}

// NotifyLog runs the OnLog hook after a log's fields are populated.
func (ibs *IntraBlockState) NotifyLog(lp *types.Log) {
	if dbg.TraceLogs && (ibs.trace || dbg.TraceAccount(accounts.InternAddress(lp.Address).Handle())) {
		var topics string
		for i := 0; i < len(lp.Topics); i++ {
			topics += "[" + hex.EncodeToString(lp.Topics[i][:]) + "]"
		}
		if topics == "" {
			topics = "[]"
		}
		fmt.Printf("%d (%d.%d) Log: Index:%d Account:%x Topics: %s Data:%x\n", ibs.blockNum, ibs.txIndex, ibs.version, lp.Index, lp.Address, topics, lp.Data)
	}
	if ibs.tracingHooks != nil && ibs.tracingHooks.OnLog != nil {
		// The hook may retain the value; the arena entry is reused by later blocks.
		ibs.tracingHooks.OnLog(lp.Copy())
	}
}

// AddLog copies log into the next slot. TxIndex and Index are assigned by the
// state; every other field comes from the caller.
func (ibs *IntraBlockState) AddLog(log *types.Log) {
	lp := ibs.AllocLog(log.Address, len(log.Topics), len(log.Data))
	copy(lp.Topics, log.Topics)
	copy(lp.Data, log.Data)
	lp.Removed = log.Removed
	lp.BlockNumber = log.BlockNumber
	lp.TxHash, lp.BlockHash = log.TxHash, log.BlockHash
	ibs.NotifyLog(lp)
}

// GetLogs deep-copies the tx's logs, so the result is safe to hold after the
// arena reuses the entry.
func (ibs *IntraBlockState) GetLogs(txIndex int, txnHash common.Hash, blockNumber uint64, blockHash common.Hash) types.Logs {
	logs := ibs.logs.forTx(txIndex).Copy()
	for _, l := range logs {
		l.TxHash = txnHash
		l.BlockHash = blockHash
		l.BlockNumber = hexutil.Uint64(blockNumber)
	}
	return logs
}

// GetRawLogs - is like GetLogs, but allow postpone calculation of `txn.Hash()`.
// Example: if you need filter logs and only then set `txn.Hash()` for filtered logs - then no reason to calc for all transactions.
func (ibs *IntraBlockState) GetRawLogs(txIndex int) types.Logs {
	return ibs.logs.forTx(txIndex).Copy()
}

func (ibs *IntraBlockState) Logs() types.Logs {
	if len(ibs.logs.entries) == 0 {
		return nil
	}
	return ibs.logs.entries.Copy()
}

// LogsRlpHash is rlpHash of Logs, without building the flattened slice.
func (ibs *IntraBlockState) LogsRlpHash() common.Hash {
	return types.RlpHashLogs(ibs.logs.entries)
}

// AddRefund adds gas to the refund counter
func (ibs *IntraBlockState) AddRefund(gas uint64) {
	ibs.journal.refundChange(ibs.refund)
	ibs.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (ibs *IntraBlockState) SubRefund(gas uint64) error {
	ibs.journal.refundChange(ibs.refund)
	if gas > ibs.refund {
		return errors.New("refund counter below zero")
	}
	ibs.refund -= gas
	return nil
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for self destructed accounts.
func (ibs *IntraBlockState) Exist(addr accounts.Address) (exists bool, err error) {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			fmt.Printf("%d (%d.%d) Exists %x: %v\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, exists)
		}()
	}
	if ibs.versionMap == nil {
		s, err := ibs.getStateObject(addr, true)
		if err != nil {
			return false, err
		}
		return s != nil && !s.deleted, nil
	}

	// Existence needs only the base record + self-destruct gate, not the
	// per-field overlay. A same-tx self-destruct is still alive (EIP-6780); a
	// cross-tx one makes versionedAccountBase return nil.
	readAccount, _, _, err := ibs.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	return readAccount != nil, nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (ibs *IntraBlockState) Empty(addr accounts.Address) (empty bool, err error) {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			fmt.Printf("%d (%d.%d) Empty %x: %v\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, empty)
		}()
	}
	if ibs.versionMap == nil {
		so, err := ibs.getStateObject(addr, true)
		if err != nil {
			return false, err
		}

		return so == nil || so.deleted || so.data.Empty(), nil
	}
	// Existence + the self-destruct/revival gate, without reconstructing the
	// whole account: the EIP-161 verdict needs only the current balance, nonce
	// and code hash, read per-field below (short-circuiting), so the per-field
	// overlay and a full-account allocation are avoided.
	account, _, _, err := ibs.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	if account == nil {
		ibs.touchAccount(addr)
		// Do NOT call accountRead here: versionedAccountBase already recorded the
		// AddressPath read with Val=nil. Overwriting it with a non-nil empty
		// account would make downstream reads treat the account as existing,
		// skipping createObject and its AddressPath write that OCC needs.
		// The empty verdict depends on the balance being zero; record that balance
		// read anchored on the destruct so a later funding write invalidates this
		// reader. The destruct records a balance-clear write at this version (real
		// SELFDESTRUCT and EIP-161 removal alike), so the read matches the cell and
		// only a funding write bumps it. It is conflict-detection only — Empty on an
		// absent account is not a real EVM balance access, so it stays out of the
		// block access list.
		if destructed, sdRes, ok := ibs.readSelfDestructMemo(addr); ok && sdRes.resolved() && destructed {
			ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{
				ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}, internal: true},
			})
		}
		return true, nil
	}

	// EIP-6780: an account self-destructed in THIS tx stays alive until end-of-tx
	// cleanup, so it must not read as empty. On the noMaterialize path the
	// self-destruct has already cleared the versioned nonce/code-hash/balance cells,
	// so recognize the own-tx SelfDestruct write directly. CreateAccount records
	// SelfDestructPath=false for a new or revived account, so only a true write
	// counts as destroyed.
	if sd, ok := ibs.versionedWriteSelfDestruct(addr); ok && sd {
		return false, nil
	}

	return ibs.emptyFromVersionedFields(addr, account)
}

// emptyFromVersionedFields computes the EIP-161 emptiness verdict for an
// account that versionedAccountBase resolved as existing, reading the current
// balance/nonce/codeHash per-field (short-circuiting on the first non-empty
// field) instead of reconstructing the whole account. The per-field refresh
// reads apply the same self-destruct gate as the whole-account path.
func (ibs *IntraBlockState) emptyFromVersionedFields(addr accounts.Address, account *accounts.Account) (bool, error) {
	balance, _, _, err := refreshBalance(ibs, addr, account.Balance)
	if err != nil {
		return false, err
	}
	if !balance.IsZero() {
		return false, nil
	}
	nonce, _, _, err := refreshNonce(ibs, addr, account.Nonce)
	if err != nil {
		return false, err
	}
	if nonce != 0 {
		return false, nil
	}
	codeHash, _, _, err := refreshCodeHash(ibs, addr, account.CodeHash)
	if err != nil {
		return false, err
	}
	return codeHash == accounts.EmptyCodeHash, nil
}

// GetBalance retrieves the balance from the given address or 0 if object not found
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetBalance(addr accounts.Address) (uint256.Int, error) {
	balance, _, err := ibs.getBalance(addr)
	return balance, err
}

func (ibs *IntraBlockState) getBalance(addr accounts.Address) (uint256.Int, bool, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return u256.Num0, false, err
		}
		if stateObject != nil && !stateObject.deleted {
			if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
				balance := stateObject.Balance()
				fmt.Printf("%d (%d.%d) GetBalance %x: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, balance.String())
			}
			return stateObject.Balance(), true, nil
		}
		return u256.Num0, false, nil
	}

	balance, source, _, err := readBalance(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetBalance %x: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, balance.String())
	}
	return balance, source == StorageRead || source == MapRead, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetNonce(addr accounts.Address) (uint64, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject != nil && !stateObject.deleted {
			return stateObject.Nonce(), nil
		}
		return 0, nil
	}

	nonce, _, _, err := readNonce(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetNonce %x: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, nonce)
	}

	return nonce, err
}

// TxIndex returns the current transaction index set by Prepare.
func (ibs *IntraBlockState) TxnIndex() int {
	return ibs.txIndex
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetCode(addr accounts.Address) ([]byte, error) {
	code, err := ibs.getCode(addr)
	return code.Bytes, err
}

func (ibs *IntraBlockState) getCode(addr accounts.Address) (accounts.Code, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return accounts.Code{}, err
		}
		if stateObject != nil && !stateObject.deleted {
			code, err := stateObject.CodeTyped()
			if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
				if err != nil {
					fmt.Printf("%d (%d.%d) GetCode (%s) %x: err: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, StorageRead, addr, err)
				} else {
					fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, StorageRead, addr, code.Len())
				}
			}
			if err == nil {
				ibs.callCodeAccessHook(addr, code.Bytes)
			}
			return code, err
		}
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, StorageRead, addr, 0)
		}
		return accounts.Code{}, nil
	}
	code, source, _, err := readCode(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		if err != nil {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: err: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, err)
		} else {
			fmt.Printf("%d (%d.%d) GetCode (%s) %x: size: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, code.Len())
		}
	}
	if err == nil {
		ibs.callCodeAccessHook(addr, code.Bytes)
	}

	return code, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetCodeSize(addr accounts.Address) (int, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject == nil || stateObject.deleted {
			return 0, nil
		}
		if stateObject.code.Bytes != nil {
			ibs.callCodeAccessHook(addr, stateObject.code.Bytes)
			return stateObject.code.Len(), nil
		}
		if stateObject.data.CodeHash.IsEmpty() {
			return 0, nil
		}
		// Size-only read: ReadAccountCodeSize, not ReadAccountCode. It routes
		// through the size-only cache layer, and is correct on the Stateless
		// reader — a size-only witness node has the size but no bytes, so
		// ReadAccountCode there returns nil (EXTCODESIZE 0) and diverges from
		// consensus.
		size, err := ibs.stateReader.ReadAccountCodeSize(addr)
		ibs.recordStateReadError(err)
		if err != nil {
			return 0, err
		}
		return size, nil
	}

	size, source, _, err := readCodeSize(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCodeSize (%s) %x: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, size)
	}

	return size, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
// codeAccessTracker lets a stateReader observe code accesses (EIP-7928 BAL /
// EIP-7702 delegation). No-op when the reader doesn't implement it.
type codeAccessTracker interface {
	OnCodeAccess(accounts.Address, []byte)
}

func (ibs *IntraBlockState) callCodeAccessHook(addr accounts.Address, code []byte) {
	if ibs.codeAccess != nil {
		ibs.codeAccess.OnCodeAccess(addr, code)
	}
}

func (ibs *IntraBlockState) GetCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return accounts.NilCodeHash, err
		}
		if stateObject == nil || stateObject.deleted {
			return accounts.NilCodeHash, nil
		}
		return stateObject.data.CodeHash, nil
	}

	hash, _, _, err := readCodeHash(ibs, addr, false)
	if err != nil {
		return accounts.NilCodeHash, err
	}
	// EIP-6780: a contract self-destructed in THIS tx stays alive until end-of-tx
	// cleanup, so EXTCODEHASH within the same tx must return its real code hash.
	// The SELFDESTRUCT cleared the CodeHashPath cell but the code is still present,
	// so recompute the hash from it.
	if hash == accounts.EmptyCodeHash && ibs.hasWrite(addr, SelfDestructPath, accounts.NilKey) {
		if cw, ok := ibs.versionedWrites.GetCode(addr); ok && len(cw.Val.Bytes) > 0 {
			return accounts.InternCodeHash(crypto.Keccak256Hash(cw.Val.Bytes)), nil
		}
	}
	if hash == accounts.NilCodeHash {
		// A prior tx's balance-preserving SELFDESTRUCT leaves an empty-code account,
		// but its CodeHashPath is dropped from the version map, so recover the
		// codehash from the reconstructed account (EmptyCodeHash), distinguishing it
		// from a genuinely absent account (NilCodeHash). Branch-agnostic: a burned
		// account reconstructs as nil here and falls through to NilCodeHash.
		acc, _, _, err := ibs.getVersionedAccount(addr, false)
		if err != nil {
			return accounts.NilCodeHash, err
		}
		if acc != nil {
			return acc.CodeHash, nil
		}
	}
	return hash, err
}

// GetCommittedCodeHash returns the code hash at tx start (this-tx-exclusive),
// read through versionedReadCore so a predecessor's in-flight code write is seen
// as a dependency rather than silently falling back to the pre-block value. It is
// the code-hash analogue of GetCommittedState; SetCode's net-zero baseline uses
// it. Absent/code-less accounts normalise to EmptyCodeHash.
func (ibs *IntraBlockState) GetCommittedCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	hash, _, _, err := readCodeHash(ibs, addr, true)
	if err != nil {
		return accounts.EmptyCodeHash, err
	}
	if hash == accounts.NilCodeHash || hash.IsEmpty() {
		return accounts.EmptyCodeHash, nil
	}
	return hash, nil
}

// ResolveCode returns the code a call to addr executes, following an EIP-7702 delegation. The
// code hash comes from the same read as the code.
func (ibs *IntraBlockState) ResolveCode(addr accounts.Address) (accounts.Code, error) {
	// committed=false so the tx's own writes (e.g. EIP-7702 authorization) are
	// visible rather than stale delegation code from the version map.
	code, err := ibs.getCode(addr)
	// eip-7702
	if delegation, ok := types.ParseDelegation(code.Bytes); ok {
		return ibs.getCode(delegation)
	}
	if err != nil {
		return accounts.Code{}, err
	}
	return code, nil
}

func (ibs *IntraBlockState) GetDelegatedDesignation(addr accounts.Address) (accounts.Address, bool, error) {
	// eip-7702 - for account read recording we don't count this as
	// it may not result in an actual gas recorded access - if it
	// is it will be marked via a direct call
	if ibs.versionMap != nil {
		// Read through the version-aware CodePath so validation can reject a
		// speculative execution that raced a prior tx's CodePath publish. readCode
		// (not getCode) avoids reporting a BAL code access for non-delegated code.
		code, _, _, err := readCode(ibs, addr)
		if err != nil {
			return accounts.ZeroAddress, false, err
		}
		if delegation, ok := types.ParseDelegation(code.Bytes); ok {
			ibs.callCodeAccessHook(addr, code.Bytes)
			return delegation, true, nil
		}
		return accounts.ZeroAddress, false, nil
	}
	stateObject, err := ibs.getStateObject(addr, false)
	if err != nil {
		return accounts.ZeroAddress, false, err
	}
	if stateObject != nil && !stateObject.deleted {
		code, err := stateObject.Code()
		if err != nil {
			return accounts.ZeroAddress, false, err
		}
		if delegation, ok := types.ParseDelegation(code); ok {
			ibs.callCodeAccessHook(addr, code)
			return delegation, true, nil
		}
	}
	return accounts.ZeroAddress, false, nil
}

// GetState retrieves a value from the given account's storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := readState(ibs, addr, key)

	if dbg.TraceTransactionIO && (ibs.trace || (dbg.TraceAccount(addr.Handle()) && traceKey(key))) {
		fmt.Printf("%d (%d.%d) GetState (%s) %x, %x=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, key, versionedValue.Hex()[2:])
	}

	return versionedValue, err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetCommittedState(addr accounts.Address, key accounts.StorageKey) (uint256.Int, error) {
	versionedValue, source, _, err := readCommittedState(ibs, addr, key)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetCommittedState (%s) %x, %x=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, key, versionedValue.Hex()[2:])
	}

	return versionedValue, err
}

func (ibs *IntraBlockState) HasSelfdestructed(addr accounts.Address) (bool, error) {
	destructed, _, _, err := readSelfDestruct(ibs, addr)
	return destructed, err
}

func (ibs *IntraBlockState) ReadVersion(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) ReadResult {
	return ibs.versionMap.ReadStatus(addr, path, key, txIdx)
}

// writeBalanceVersioned records a balance change on the versionMap write-set and
// the journal without materializing the stateObject on the common existing-alive
// path; an already-materialized one is kept in step. An absent or
// destroyed-no-revival account is materialized via GetOrNewStateObject so
// createObject records the AddressPath write OCC needs; the create path never
// reads balance (matching the old stateObject path). The journal prev comes from a
// caller that already read the balance, or, when nil, is read only in the existing
// branch so a create does not widen the OCC read-set with a spurious BalancePath
// read.
func (ibs *IntraBlockState) writeBalanceVersioned(addr accounts.Address, prev *uint256.Int, update uint256.Int, wasCommited bool, reason tracing.BalanceChangeReason) error {
	base, _, _, err := ibs.versionedAccountBase(addr, true)
	if err != nil {
		return err
	}
	if base != nil && prev == nil {
		cur, _, err := ibs.getBalance(addr)
		if err != nil {
			return err
		}
		prev = &cur
	}
	if base == nil || ibs.accountLifecycle(addr) {
		stateObject, err := ibs.GetOrNewStateObject(addr)
		if err != nil {
			return err
		}
		// A destroyed-then-revived account's transient lags this tx's own balance
		// write, so seed the live balance first or a revert restores the wrong
		// one. The base==nil create path never read balance, so leave it untouched.
		if base != nil {
			stateObject.setBalance(*prev)
		}
		stateObject.SetBalance(update, wasCommited, reason)
		ibs.recordWriteBalance(addr, update)
		return nil
	}
	ibs.journal.balanceChange(addr, *prev, wasCommited)
	if ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil {
		ibs.tracingHooks.OnBalanceChange(addr, *prev, update, reason)
	}
	if so, ok := ibs.stateObjects[addr]; ok {
		so.setBalance(update)
	}
	ibs.recordWriteBalance(addr, update)
	return nil
}

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) AddBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if ibs.versionMap == nil {
		// If this account has not been read, add to the balance increment map
		if _, needAccount := ibs.stateObjects[addr]; !needAccount && addr == ripemd && amount.IsZero() {
			ibs.journal.balanceIncrease(addr, amount)

			bi, ok := ibs.balanceInc[addr]
			if !ok {
				bi = &BalanceIncrease{}
				ibs.balanceInc[addr] = bi
			}

			if ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil {
				prev := new(uint256.Int)
				amount := amount
				if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle()))) {
					ibs.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", ibs.blockNum, ibs.txIndex, ibs.version))
				}
				var readStart time.Time
				if dbg.KVReadLevelledMetrics {
					readStart = time.Now()
				}
				account, _ := ibs.stateReader.ReadAccountDataForDebug(addr)
				if dbg.KVReadLevelledMetrics {
					ibs.accountReadDuration += time.Since(readStart)
					ibs.accountReadCount++
				}
				ibs.stateReader.SetTrace(false, "")
				if account != nil {
					prev.Add(&account.Balance, &bi.increase)
				} else {
					prev.Add(prev, &bi.increase)
				}

				ibs.tracingHooks.OnBalanceChange(addr, *prev, *new(uint256.Int).Add(prev, &amount), reason)
			}

			bi.increase = u256.Add(bi.increase, amount)
			bi.count++
			return nil
		}
	}

	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.IsZero() {
		return ibs.TouchAccount(addr)
	}

	prev, wasCommited, err := ibs.getBalance(addr)
	if err != nil {
		return err
	}

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := ibs.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
			expected := (&uint256.Int{}).Add(&prev, &amount)
			if bal.Cmp(expected) != 0 {
				panic(fmt.Sprintf("add failed: expected: %d got: %s", expected, bal.String()))
			}
			fmt.Printf("%d (%d.%d) AddBalance %x, %s+%s=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, prev.String(), amount.String(), bal.String())
		}()
	}

	update := u256.Add(prev, amount)

	if ibs.versionMap != nil {
		return ibs.writeBalanceVersioned(addr, &prev, update, wasCommited, reason)
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(update, wasCommited, reason)
	ibs.recordWriteBalance(addr, update)
	return nil
}

func (ibs *IntraBlockState) touchAccount(addr accounts.Address) {
	ibs.journal.touchAccount(addr, false, uint256.Int{})
	if addr == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		ibs.journal.dirty(addr)
	}
}

// TouchAccount materializes an empty account and records the zero-balance touch
// needed for state clearing and trie consistency.
func (ibs *IntraBlockState) TouchAccount(addr accounts.Address) error {
	// An own balance write settles the touch: zero already is the touch, non-zero is a non-empty account.
	if ibs.versionMap != nil && addr != ripemd {
		if _, ok := ibs.versionedWrites.GetBalance(addr); ok {
			return nil
		}
	}
	markTouched := func() {
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Touch %x\n", ibs.blockNum, ibs.txIndex, ibs.version, addr)
		}
		if ibs.versionMap != nil {
			// Versioned path: pair the BalancePath=0 write with a journal entry
			// that reverts it, so the write-set stays in step through reverts
			// without any dirties re-processing (deprecated on this path).
			prevWrite, had := ibs.versionedWrites.GetBalance(addr)
			var prev uint256.Int
			if had {
				prev = prevWrite.Val
			}
			ibs.recordWriteBalance(addr, uint256.Int{})
			ibs.journal.touchAccount(addr, !had, prev)
			return
		}
		ibs.recordWriteBalance(addr, uint256.Int{})
		if _, ok := ibs.journal.dirties[addr]; !ok {
			ibs.touchAccount(addr)
		}
	}

	if ibs.versionMap != nil {
		// The touch only depends on emptiness. For an existing account compute
		// it from field reads without materializing/reconstructing the
		// stateObject; only an absent account needs GetOrNewStateObject so
		// createObject records the AddressPath write (OCC create detection).
		account, _, _, err := ibs.versionedAccountBase(addr, true)
		if err != nil {
			return err
		}
		if account != nil {
			empty, err := ibs.emptyFromVersionedFields(addr, account)
			if err != nil {
				return err
			}
			if empty {
				markTouched()
			}
			return nil
		}
		// No cell and no committed record: getStateObject would resolve the same absence again.
		if ibs.noMaterialize && ibs.versionMap.load(addr) == nil {
			ibs.createObject(addr, nil)
			markTouched()
			return nil
		}
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject.data.Empty() {
		markTouched()
	}

	return nil
}

// sdProbeMemo returns the SelfDestruct probe memoized for the current execution
// attempt, or a zero entry, without probing the version map.
func (ibs *IntraBlockState) sdProbeMemo(addr accounts.Address) (sdProbeEntry, bool) {
	if e, hit := ibs.sdProbe[addr]; hit && e.epoch == ibs.sdProbeEpoch {
		return e, true
	}
	return sdProbeEntry{}, false
}

// readSelfDestructMemo returns the shared-versionMap SelfDestruct probe for the
// current execution attempt, caching it so a warm multi-field read does not
// re-acquire the versionMap RWMutex per field. The probe reads only prior-tx SD
// writes, so the memoized value is stable for the attempt.
func (ibs *IntraBlockState) readSelfDestructMemo(addr accounts.Address) (bool, ReadResult, bool) {
	if e, hit := ibs.sdProbeMemo(addr); hit {
		return e.destructed, e.res, e.ok
	}
	destructed, res, ok := ibs.versionMap.ReadSelfDestruct(addr, ibs.txIndex)
	// An address with no cells costs no lock to probe; memoizing it only grows the map.
	if !ok && ibs.versionMap.load(addr) == nil {
		return destructed, res, ok
	}
	if ibs.sdProbe == nil {
		ibs.sdProbe = make(map[accounts.Address]sdProbeEntry, 8)
	}
	ibs.sdProbe[addr] = sdProbeEntry{epoch: ibs.sdProbeEpoch, res: res, destructed: destructed, ok: ok}
	return destructed, res, ok
}

// eip8246PreservedAccount reconstructs the live account a prior tx left behind
// when EIP-8246 removed the SELFDESTRUCT burn: the balance survives, code and
// nonce are cleared at destruction, and any later per-field map writes overlay
// the reconstruction so account-level reads agree with the field-level ones.
// Returns nil when the result is EIP-161 empty: the balance was moved out and
// no later tx re-established a nonce or code, so the account is gone.
func (ibs *IntraBlockState) eip8246PreservedAccount(addr accounts.Address) (*accounts.Account, error) {
	bal, _, _, err := readBalance(ibs, addr)
	if err != nil {
		return nil, err
	}
	nonce, _, _, err := readNonce(ibs, addr)
	if err != nil {
		return nil, err
	}
	codeHash, _, _, err := readCodeHash(ibs, addr, false)
	if err != nil {
		return nil, err
	}
	acc := accounts.NewAccount()
	acc.Balance = bal
	acc.Nonce = nonce
	if codeHash != accounts.NilCodeHash && !codeHash.IsZero() {
		acc.CodeHash = codeHash
	}
	if acc.Empty() {
		return nil, nil
	}
	return &acc, nil
}

func (ibs *IntraBlockState) getVersionedAccount(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	return ibs.versionedAccountBase(addr, readStorage)
}

// readAbsentFromMap reports whether this tx already read addr as absent from the
// version map (a destruct with no revival). That verdict is final for the tx: the
// committed record must not revive it, and validation catches a later re-creation.
func (ibs *IntraBlockState) readAbsentFromMap(addr accounts.Address) bool {
	tr, ok := ibs.versionedReads.GetAddress(addr)
	return ok && tr.Source == MapRead && (tr.Val == nil || tr.Val.Account() == nil)
}

// readAbsent is like readAbsentFromMap, but also counts an absence read from the committed state:
// the base reader can serve a lower tx's creation later in the attempt.
func (ibs *IntraBlockState) readAbsent(addr accounts.Address) bool {
	tr, ok := ibs.versionedReads.GetAddress(addr)
	return ok && (tr.Val == nil || tr.Val.Account() == nil)
}

// absenceIsFinal is readAbsentFromMap, except for an absence recorded at a destruct that
// kept a balance: this tx read the kept account there, and validation rejects it if a
// re-creation stays.
func (ibs *IntraBlockState) absenceIsFinal(addr accounts.Address) bool {
	if !ibs.readAbsentFromMap(addr) {
		return false
	}
	tr, _ := ibs.versionedReads.GetAddress(addr)
	return !ibs.versionMap.destructKeptBalance(addr, tr.Version.TxIndex)
}

// versionedAccountBase resolves account existence via the AddressPath read (and
// storage fallback), applying the self-destruct/revival gate, but does NOT
// overlay the per-field versionMap cells. It returns nil when the account is
// absent or was destroyed with no revival. The AddressPath read it performs
// records the nil-read that OCC uses to detect create/absent conflicts.
func (ibs *IntraBlockState) versionedAccountBase(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	if ibs.versionMap == nil {
		return nil, UnknownSource, UnknownVersion, nil
	}

	readAccount, source, version, err := readAccount(ibs, addr)
	if err != nil {
		return nil, UnknownSource, UnknownVersion, err
	}

	// A prior tx's balance-preserving SELFDESTRUCT (EIP-8246) keeps the account
	// alive (balance kept, code/nonce cleared); AddressPath reads zero under SD, so
	// reconstruct the surviving account from the version map here, covering both
	// committed and in-block-created accounts — unless a later tx re-created it.
	// Branch-agnostic: a burn stamps the balance ValueDeleted, so the
	// reconstruction yields nil and this falls through.
	if readAccount == nil {
		if ibs.versionMap.absentAccountHook != nil {
			ibs.versionMap.absentAccountHook()
		}
		// Prefer the destruct that the read above saw, so that an in-flight re-creation
		// does not hide it; without one, probe the version map, as a destruct may have
		// been flushed since.
		memo, _ := ibs.sdProbeMemo(addr)
		destructed, sdRes, ok := memo.destructed, memo.res, memo.ok
		if !ok || !sdRes.resolved() || !destructed {
			destructed, sdRes, ok = ibs.versionMap.ReadSelfDestruct(addr, ibs.txIndex)
		}
		if ok && sdRes.resolved() && destructed {
			destructTxIndex := sdRes.DepIdx()
			// Only a genuine re-creation (a later CreateAccount writing AddressPath)
			// skips reconstruction. Later Balance/Nonce/CodeHash writes are updates
			// to the still-preserved account, not a revival.
			revived := false
			if hi, ok := ibs.versionMap.LatestTxIndex(addr, AddressPath, accounts.NilKey, ibs.txIndex-1); ok && hi > destructTxIndex {
				// The re-creation may have been flushed after the read above. If the
				// re-read finds no account either and this tx has no final absence of it,
				// the re-creation was withdrawn, or this tx read the account before the
				// re-creation, which validation rejects. The reconstruction applies.
				readAccount, source, version, err = readAccountInternal(ibs, addr)
				if err != nil {
					return nil, UnknownSource, UnknownVersion, err
				}
				revived = readAccount != nil || ibs.absenceIsFinal(addr)
			}
			if !revived {
				preserved, err := ibs.eip8246PreservedAccount(addr)
				if err != nil {
					return nil, StorageRead, UnknownVersion, err
				}
				if preserved != nil {
					return preserved, MapRead, Version{TxIndex: destructTxIndex}, nil
				}
			}
		}
	}

	if readAccount == nil {
		if ibs.readAbsentFromMap(addr) {
			return nil, StorageRead, UnknownVersion, nil
		}
		if readStorage {
			readAccount, err = ibs.committedAccount(addr)
			source = StorageRead
		}

		if readAccount == nil || err != nil {
			return nil, StorageRead, UnknownVersion, err
		}

		// The base reader bypasses the versionMap, so a prior in-block SD'd
		// address still returns its pre-SD record. Without this gate the
		// stale nonce/codeHash flows through the per-field refresh (which
		// only overwrites fields a versionMap cell exists for), so Empty()
		// returns false and the EVM misses CallNewAccountGas.
		if ibs.versionMap.IsNetAbsent(addr, ibs.txIndex) {
			return nil, StorageRead, UnknownVersion, nil
		}
	}

	return readAccount, source, version, nil
}

// committedAccount reads the committed (pre-block) account once per tx.
func (ibs *IntraBlockState) committedAccount(addr accounts.Address) (*accounts.Account, error) {
	if cached, ok := ibs.committedBase[addr]; ok {
		return cached, nil
	}
	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle()))) {
		ibs.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", ibs.blockNum, ibs.txIndex, ibs.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	acc, err := ibs.stateReader.ReadAccountData(addr)
	if dbg.KVReadLevelledMetrics {
		ibs.accountReadDuration += time.Since(readStart)
		ibs.accountReadCount++
	}
	ibs.stateReader.SetTrace(false, "")
	if err != nil {
		// Route a committed-base read failure (e.g. a corrupt account record) operationally
		// like the origin* readers, so the executor surfaces it instead of letting the
		// EVM swallow it as a gas failure and condemn the block as invalid.
		ibs.recordStateReadError(err)
		return nil, err
	}
	if ibs.committedBase == nil {
		ibs.committedBase = make(map[accounts.Address]*accounts.Account)
	}
	ibs.committedBase[addr] = acc
	return acc, nil
}

// SubBalance subtracts amount from the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SubBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if amount.IsZero() {
		if addr == params.SystemAddress {
			// Gnosis/AuRa keeps an empty system account even after Spurious Dragon.
			return ibs.TouchAccount(addr)
		}
		return nil
	}

	prev, wasCommited, err := ibs.getBalance(addr)
	if err != nil {
		return err
	}

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			bal, _ := ibs.GetBalance(addr)
			prev := prev     // avoid capture allocation unless we're tracing
			amount := amount // avoid capture allocation unless we're tracing
			fmt.Printf("%d (%d.%d) SubBalance %x, %s-%s=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, prev.String(), amount.String(), bal.String())
		}()
	}

	update := u256.Sub(prev, amount)

	if ibs.versionMap != nil {
		return ibs.writeBalanceVersioned(addr, &prev, update, wasCommited, reason)
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(update, wasCommited, reason)
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		amount := amount
		fmt.Printf("%d (%d.%d) SetBalance %x, %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, amount.String())
	}
	if ibs.versionMap != nil {
		return ibs.writeBalanceVersioned(addr, nil, amount, !ibs.hasWrite(addr, BalancePath, accounts.NilKey), reason)
	}
	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	stateObject.SetBalance(amount, !ibs.hasWrite(addr, BalancePath, accounts.NilKey), reason)
	ibs.recordWriteBalance(addr, stateObject.Balance())
	return nil
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetNonce(addr accounts.Address, nonce uint64, reason tracing.NonceChangeReason) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetNonce %x, %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, nonce)
	}

	wasCommited := !ibs.hasWrite(addr, NoncePath, accounts.NilKey)
	if ibs.versionMap != nil {
		return ibs.writeNonceVersioned(addr, nonce, wasCommited, reason)
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}

	stateObject.SetNonce(nonce, wasCommited, reason)
	ibs.recordWriteNonce(addr, stateObject.Nonce(), reason)
	return nil
}

// writeNonceVersioned records a nonce write on the parallel (versionMap) path
// without materializing a stateObject for an existing, live account. A nonce SET
// does not depend on the prior value, so prev is read WITHOUT recording an OCC
// read. Absent/destroyed accounts still materialize (account creation).
func (ibs *IntraBlockState) writeNonceVersioned(addr accounts.Address, nonce uint64, wasCommited bool, reason tracing.NonceChangeReason) error {
	base, _, _, err := ibs.versionedAccountBase(addr, true)
	if err != nil {
		return err
	}
	if base == nil || ibs.accountLifecycle(addr) {
		stateObject, err := ibs.GetOrNewStateObject(addr)
		if err != nil {
			return err
		}
		// The object is built from the base record and can lag this tx's own
		// nonce write: seed it so a revert restores the live nonce.
		if vw, ok := ibs.versionedWrites.GetNonce(addr); ok {
			stateObject.setNonce(vw.Val)
		}
		stateObject.SetNonce(nonce, wasCommited, reason)
		ibs.recordWriteNonce(addr, nonce, reason)
		return nil
	}
	prev := base.Nonce
	// Keep an already-materialized stateObject's so.data in step so the
	// so.data-based commit paths (genesis FinalizeTx, RPC) stay correct.
	if so, ok := ibs.stateObjects[addr]; ok {
		prev = so.data.Nonce
		so.setNonce(nonce)
	}
	// The tx's own nonce cell is authoritative for the journal prev: so.data can
	// lag if the object was materialized after this tx's earlier writes.
	if vw, ok := ibs.versionedWrites.GetNonce(addr); ok {
		prev = vw.Val
	}
	ibs.journal.nonceChange(addr, prev, wasCommited)
	if ibs.tracingHooks != nil {
		if ibs.tracingHooks.OnNonceChangeV2 != nil {
			ibs.tracingHooks.OnNonceChangeV2(addr, prev, nonce, reason)
		} else if ibs.tracingHooks.OnNonceChange != nil {
			ibs.tracingHooks.OnNonceChange(addr, prev, nonce)
		}
	}
	ibs.recordWriteNonce(addr, nonce, reason)
	return nil
}

func printCode(c []byte) (int, string) {
	lenc := len(c)

	if lenc == 0 {
		return 0, ""
	}

	if lenc > 41 {
		return lenc, fmt.Sprintf("%x...", c[0:40])
	}

	return lenc, fmt.Sprintf("%x...", c)
}

// SetCode keeps code, also after a revert or Reset: the caller must not modify it afterwards.
//
// DESCRIBED: docs/programmers_guide/guide.md#code-hash
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetCode(addr accounts.Address, code []byte, reason tracing.CodeChangeReason) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		lenc, cs := printCode(code)
		fmt.Printf("%d (%d.%d) SetCode %x, %d: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, lenc, cs)
	}

	// Factories deploy the same bytes many times: reuse the last hash instead of re-hashing.
	canonical := ibs.lastCode
	if len(code) == 0 || !bytes.Equal(code, canonical.Bytes) {
		canonical = accounts.NewCode(code)
		ibs.lastCode = canonical
	}
	// A live account this tx created: no own self-destruct, and createObject wrote its AddressPath.
	if ibs.noMaterialize && !ibs.warmReadable(addr) && ibs.hasWrite(addr, AddressPath, accounts.NilKey) && !ibs.accountLifecycle(addr) {
		return ibs.setCreatedCode(addr, canonical, reason)
	}
	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	codeHash := canonical.Hash
	baseCodeHash := stateObject.data.CodeHash
	origHash := stateObject.original.CodeHash
	if ibs.versionMap != nil {
		// baseCodeHash ("what this SetCode saw") is the current cell, including
		// this tx's own earlier code writes. origHash (the cumulative net-zero
		// baseline) is the versionMap floor at txIndex — the tx-start value.
		if ch, chErr := ibs.GetCodeHash(addr); chErr == nil {
			baseCodeHash = ch
		}
		origHash, err = ibs.GetCommittedCodeHash(addr)
		if err != nil {
			return err
		}
	}
	if ibs.noMaterialize {
		seed, err := ibs.codeSeed(addr, baseCodeHash)
		if err != nil {
			return err
		}
		stateObject.setCode(seed)
	}
	written, err := stateObject.SetCode(canonical, !ibs.hasWrite(addr, CodePath, accounts.NilKey), reason)
	if err != nil {
		return err
	}
	if written {
		// Skip when the new code matches either the value seen by THIS SetCode
		// call (revert to in-tx base) or the pre-tx original (cumulative net-zero,
		// e.g. an EIP-7702 delegate-then-reset in the same tx). The net-zero case
		// is disabled for newly created objects, whose original holds the
		// pre-creation snapshot — deleting the code writes there corrupts the trie.
		matchesOriginal := !stateObject.newlyCreated && codeHash == origHash
		unchanged := codeHash == baseCodeHash || matchesOriginal
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			if unchanged {
				fmt.Printf("%d (%d.%d) SetCode SKIP (matches base) %x codeHash=%x baseHash=%x originalHash=%x codeLen=%d\n",
					ibs.blockNum, ibs.txIndex, ibs.version, addr, codeHash, baseCodeHash, stateObject.original.CodeHash, len(code))
			} else {
				fmt.Printf("%d (%d.%d) SetCode WRITE %x codeHash=%x baseHash=%x codeLen=%d\n",
					ibs.blockNum, ibs.txIndex, ibs.version, addr, codeHash, baseCodeHash, len(code))
			}
		}
		ibs.writeCode(addr, canonical, unchanged)
	}
	return nil
}

// writeCode records code as this tx's Code, CodeHash and CodeSize writes, or
// drops those writes when the code is unchanged.
func (ibs *IntraBlockState) writeCode(addr accounts.Address, code accounts.Code, unchanged bool) {
	if unchanged {
		ibs.versionedWrites.DelCode(addr)
		ibs.versionedWrites.DelCodeHash(addr)
		ibs.versionedWrites.DelCodeSize(addr)
		return
	}
	ibs.recordWriteCode(addr, code)
	ibs.recordWriteCodeHash(addr, code.Hash)
	ibs.recordWriteCodeSize(addr, code.Len())
}

// journalCodeChange journals a code change and calls the tracing hooks.
func (ibs *IntraBlockState) journalCodeChange(addr accounts.Address, prevHash accounts.CodeHash, prevCode []byte, code accounts.Code, wasCommited bool, reason tracing.CodeChangeReason) {
	ibs.journal.codeChange(addr, prevCode, prevHash, wasCommited)
	if ibs.tracingHooks != nil && ibs.tracingHooks.OnCodeChangeV2 != nil {
		ibs.tracingHooks.OnCodeChangeV2(addr, prevHash, prevCode, code.Hash, code.Bytes, reason)
	} else if ibs.tracingHooks != nil && ibs.tracingHooks.OnCodeChange != nil {
		ibs.tracingHooks.OnCodeChange(addr, prevHash, prevCode, code.Hash, code.Bytes)
	}
}

// setCreatedCode is SetCode for an account this tx created, on the noMaterialize
// path: its prior code is this tx's own cells, so no transient stateObject is
// rebuilt, and the net-zero check against the tx-start code does not apply.
func (ibs *IntraBlockState) setCreatedCode(addr accounts.Address, code accounts.Code, reason tracing.CodeChangeReason) error {
	cw, hadCode := ibs.versionedWrites.GetCode(addr)
	hw, hadHash := ibs.versionedWrites.GetCodeHash(addr)
	var prev accounts.Code
	switch {
	case hadCode:
		prev = cw.Val
	case hadHash && hw.Val == accounts.EmptyCodeHash:
		prev = accounts.Code{Hash: accounts.EmptyCodeHash}
	default:
		// An unchanged SetCode drops the code hash write createObject made.
		baseCodeHash, err := ibs.GetCodeHash(addr)
		if err != nil {
			return err
		}
		if prev, err = ibs.codeSeed(addr, baseCodeHash); err != nil {
			return err
		}
	}
	if prev.Hash == code.Hash && bytes.Equal(prev.Bytes, code.Bytes) {
		return nil
	}
	ibs.journalCodeChange(addr, prev.Hash, prev.Bytes, code, !hadCode, reason)
	ibs.writeCode(addr, code, code.Hash == prev.Hash)
	return nil
}

var tracedKeys map[accounts.StorageKey]struct{}

func traceKey(key accounts.StorageKey) bool {
	if tracedKeys == nil {
		tracedKeys = map[accounts.StorageKey]struct{}{}
		for _, key := range dbg.TraceStateKeys {
			key, _ = strings.CutPrefix(strings.ToLower(key), "Ox")
			tracedKeys[accounts.InternKey(common.HexToHash(key))] = struct{}{}
		}
	}
	_, ok := tracedKeys[key]
	return len(tracedKeys) == 0 || ok
}

func (ibs *IntraBlockState) Trace() bool {
	return ibs.trace || dbg.Trace
}

func (ibs *IntraBlockState) BlockNumber() uint64 {
	return ibs.blockNum
}

func (ibs *IntraBlockState) TxIndex() int {
	return ibs.txIndex
}

func (ibs *IntraBlockState) Incarnation() int {
	return int(ibs.version)
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	return ibs.setState(addr, key, value, false)
}

func (ibs *IntraBlockState) setState(addr accounts.Address, key accounts.StorageKey, value uint256.Int, force bool) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetState %x, %x=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, key, value.Hex())
	}

	// The EVM SSTORE path (force==false) writes through cells without
	// materializing a stateObject. force==true (replay) and a fakeStorage
	// override (eth_simulate) still need the object.
	if ibs.versionMap != nil && !force {
		if so, ok := ibs.stateObjects[addr]; !ok || so.fakeStorage == nil {
			return ibs.setStateVersioned(addr, key, value)
		}
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	set, err := stateObject.SetState(key, value, force)
	if err != nil {
		return err
	}
	if set {
		// Record even when value equals origin: dropping the write entry leaves
		// the journal nothing to restore when a nested write is reverted.
		prev, _ := stateObject.GetCommittedState(key)
		ibs.recordWriteStorage(addr, key, value, prev)
	}
	return nil
}

// setStateVersioned records a storage write on the parallel (versionMap) path
// without materializing a stateObject. It mirrors stateObject.SetState's set
// decision and journalling. An already-materialized stateObject is kept in step
// so the so.data-based commit paths (genesis FinalizeTx, RPC) stay correct.
func (ibs *IntraBlockState) setStateVersioned(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	prev, source, _, commited, err := readStateForSet(ibs, addr, key)
	if err != nil {
		return err
	}
	// See stateObject.SetState: a value resolved from a cached read or the
	// version map has no versioned write for this key this tx, so this is the
	// first write and commited must be true for storageChange.revert to delete
	// (not update) the cell.
	if source != WriteSetRead && source != UnknownSource && source != StorageRead {
		commited = true
	}
	if source != UnknownSource && prev == value {
		return nil
	}
	ibs.journal.storageChange(addr, key, prev, commited)
	if ibs.tracingHooks != nil && ibs.tracingHooks.OnStorageChange != nil {
		ibs.tracingHooks.OnStorageChange(addr, key, prev, value)
	}
	if so, ok := ibs.stateObjects[addr]; ok {
		so.setState(key, value)
	}
	ibs.recordWriteStorage(addr, key, value, prev)
	return nil
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (ibs *IntraBlockState) SetStorage(addr accounts.Address, storage Storage) error {
	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if ibs.versionMap != nil {
		// An incarnation cell wipes storage and code for later transactions: a slot
		// or code without a newer cell reads as empty, so the code is written again.
		code, err := ibs.getCode(addr)
		if err != nil {
			return err
		}
		if code.Len() > 0 {
			ibs.writeCode(addr, code, false)
		}
		ibs.recordWriteIncarnation(addr, stateObject.data.Incarnation)
		// The incarnation cell does not hide a slot cell of an earlier transaction.
		for _, key := range ibs.versionMap.StorageKeys(addr) {
			prev, _, _ := ibs.versionMap.ReadStorage(addr, key, ibs.txIndex)
			ibs.recordWriteStorage(addr, key, uint256.Int{}, prev)
		}
		for key, value := range storage {
			prev, _, _ := ibs.versionMap.ReadStorage(addr, key, ibs.txIndex)
			ibs.recordWriteStorage(addr, key, value, prev)
		}
		return nil
	}
	if stateObject != nil {
		stateObject.SetStorage(storage)
	}
	return nil
}

// SetIncarnation sets incarnation for account if account exists
func (ibs *IntraBlockState) SetIncarnation(addr accounts.Address, incarnation uint64) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetIncarnation %x, %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, incarnation)
	}

	stateObject, err := ibs.GetOrNewStateObject(addr)
	if err != nil {
		return err
	}
	if stateObject != nil {
		stateObject.setIncarnation(incarnation)
		ibs.recordWriteIncarnation(addr, stateObject.data.Incarnation)
	}
	return nil
}

func (ibs *IntraBlockState) GetIncarnation(addr accounts.Address) (uint64, error) {
	if ibs.versionMap == nil {
		stateObject, err := ibs.getStateObject(addr, true)
		if err != nil {
			return 0, err
		}
		if stateObject != nil {
			return stateObject.data.Incarnation, nil
		}
		return 0, nil
	}

	incarnation, _, _, err := readIncarnation(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetIncarnation %x: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, incarnation)
	}

	return incarnation, err
}

// Selfdestruct marks the given account as suicided. When preserveBalance is
// false the account balance is burned (pre-EIP-6780/6780 behaviour); when true
// the balance is left untouched (EIP-8246) and only cleared at finalization if
// the account ends up empty.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (ibs *IntraBlockState) Selfdestruct(addr accounts.Address, preserveBalance bool) (bool, error) {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SelfDestruct %x\n", ibs.blockNum, ibs.txIndex, ibs.version, addr)
	}
	if ibs.versionMap != nil {
		return ibs.selfdestructVersioned(addr, preserveBalance)
	}
	stateObject, err := ibs.getStateObject(addr, true)
	if err != nil {
		return false, err
	}
	if stateObject == nil || stateObject.deleted {
		return false, nil
	}
	prevBalance := stateObject.Balance()
	ibs.journal.selfdestructChange(addr, stateObject.selfdestructed, prevBalance, !ibs.hasWrite(addr, SelfDestructPath, accounts.NilKey))

	if !preserveBalance && ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		ibs.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
	}

	stateObject.markSelfdestructed()
	stateObject.createdContract = false

	ibs.recordWriteIncarnation(addr, stateObject.data.Incarnation)
	ibs.recordWriteSelfDestruct(addr, stateObject.selfdestructed)
	if preserveBalance && !prevBalance.IsZero() {
		ibs.recordWriteBalance(addr, prevBalance)
	} else {
		stateObject.data.Balance.Clear()
		ibs.recordWriteBalanceDeleted(addr)
	}

	// Deliberately no StoragePath=0 writes for the dirty slots: the account
	// stays alive until end-of-tx, so a re-entry's GetState must still see the
	// dirty values.

	return true, nil
}

// selfdestructVersioned records a self-destruct on the parallel (versionMap)
// path without materializing a stateObject. An already-materialized stateObject
// is kept in step for the so.data-based commit paths (genesis FinalizeTx, RPC).
func (ibs *IntraBlockState) selfdestructVersioned(addr accounts.Address, preserveBalance bool) (bool, error) {
	sdW, hadSD := ibs.versionedWrites.GetSelfDestruct(addr)
	balW, hadBalance := ibs.versionedWrites.GetBalance(addr)
	incW, hadIncarnation := ibs.versionedWrites.GetIncarnation(addr)
	// Destructed earlier in this tx and sent nothing since: every cell below already holds its value.
	if !preserveBalance && hadSD && sdW.Val && hadBalance && balW.Val.IsZero() && hadIncarnation {
		return true, nil
	}
	base, _, _, err := ibs.versionedAccountBase(addr, true)
	if err != nil {
		return false, err
	}
	// base is nil for an absent account and for one destroyed in a prior tx and
	// not revived. A same-tx repeat SELFDESTRUCT still proceeds.
	if base == nil {
		return false, nil
	}

	prev := hadSD && sdW.Val
	prevBalance := base.Balance
	// Capture the pre-destruct versioned incarnation/balance writes, which the
	// self-destruct clears below, so a revert restores them rather than the
	// cleared value.
	var prevIncarnation uint64
	var prevBalanceVersioned uint256.Int
	if hadIncarnation {
		prevIncarnation = incW.Val
	}
	if hadBalance {
		prevBalance, prevBalanceVersioned = balW.Val, balW.Val
	}
	ibs.journal.selfdestructChangeVersioned(addr, prev, prevBalance,
		!ibs.hasWrite(addr, SelfDestructPath, accounts.NilKey),
		hadIncarnation, prevIncarnation, hadBalance, prevBalanceVersioned)

	if !preserveBalance && ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		ibs.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
	}

	if so, ok := ibs.stateObjects[addr]; ok {
		so.markSelfdestructed()
		so.createdContract = false
		if !preserveBalance {
			so.data.Balance.Clear()
		}
	}

	ibs.recordWriteSelfDestruct(addr, true)
	// A later revival or re-creation starts from incarnation 0, as in serial.
	ibs.recordWriteIncarnation(addr, 0)
	// Stamp the balance with the branch-agnostic delete/preserve instruction. A
	// balance-preserving destruct (EIP-8246) keeps the account alive — even at zero
	// balance, so a later same-tx credit still lands on an existing account and
	// end-of-tx EIP-161 removes it only if it stays empty. Every other destruct
	// deletes the leaf (ValueDeleted → nil). Derived from preserveBalance, not the
	// value, so a burn that happens to leave a non-zero balance (e.g. a pre-Cancun
	// self-beneficiary) still reads deleted.
	if preserveBalance {
		ibs.recordWriteBalance(addr, prevBalance)
	} else {
		ibs.recordWriteBalanceDeleted(addr)
	}
	// Nonce and code hash are not written here: Snapshot (finalize) drops them for a
	// self-destructed account, and a balance-preserving destruct re-stamps the cleared
	// values there so the version map carries the full codeless-EOA state. Explicit
	// cells here would also make a same-tx re-creation abort on a phantom collision.
	return true, nil
}

var zeroBalance uint256.Int

// Used for EIP-6780
func (ibs *IntraBlockState) IsNewContract(addr accounts.Address) (bool, error) {
	stateObject, err := ibs.getStateObject(addr, true)
	if err != nil {
		return false, err
	}
	if stateObject == nil {
		return false, nil
	}
	if !stateObject.newlyCreated {
		return false, nil
	}
	code, err := ibs.GetCode(addr)
	if err != nil {
		return false, err
	}
	_, delegated := types.ParseDelegation(code)
	return !delegated, nil
}

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (ibs *IntraBlockState) SetTransientState(addr accounts.Address, key *TransientKey, value *uint256.Int) {
	ibs.transientStorage.Set(ibs.journal, addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (ibs *IntraBlockState) GetTransientState(addr accounts.Address, key TransientKey) uint256.Int {
	return ibs.transientStorage.Get(addr, key)
}

func (ibs *IntraBlockState) stateObjectForAccount(addr accounts.Address, account *accounts.Account) *stateObject {
	obj := newObject(ibs, addr, account, account)
	if ibs.noMaterialize {
		ibs.reconstructCellFlags(obj, addr)
		return obj
	}
	ibs.setStateObject(addr, obj)
	return obj
}

func (ibs *IntraBlockState) getStateObject(addr accounts.Address, recordRead bool) (*stateObject, error) {
	// A cached object is returned without re-reading the versionMap: the
	// materializing flows keep so.data in step with the cells on every write, and
	// the noMaterialize path never populates this cache.
	if so, ok := ibs.stateObjects[addr]; ok {
		return so, nil
	}

	// Load the object from the database.
	if _, ok := ibs.nilAccounts[addr]; ok {
		if bi, ok := ibs.balanceInc[addr]; ok && !bi.transferred && ibs.versionMap == nil {
			return ibs.createObject(addr, nil), nil
		}
		return nil, nil
	}

	account, _, _, err := ibs.getVersionedAccount(addr, false)
	if err != nil {
		return nil, err
	}

	if account != nil {
		return ibs.stateObjectForAccount(addr, account), nil
	}
	if ibs.readAbsent(addr) {
		return nil, nil
	}

	if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle()))) {
		ibs.stateReader.SetTrace(true, fmt.Sprintf("%d (%d.%d)", ibs.blockNum, ibs.txIndex, ibs.version))
	}
	var readStart time.Time
	if dbg.KVReadLevelledMetrics {
		readStart = time.Now()
	}
	readAccount, err := ibs.stateReader.ReadAccountData(addr)
	if dbg.KVReadLevelledMetrics {
		ibs.accountReadDuration += time.Since(readStart)
		ibs.accountReadCount++
	}
	ibs.stateReader.SetTrace(false, "")

	accountSource := StorageRead
	accountVersion := ibs.Version()

	if err != nil {
		// Record so the executor surfaces it operationally rather than continuing
		// with a wrong value; the serial (non-versioned) account read reaches the DB
		// here, bypassing refreshAccount and the typed readers that already record.
		ibs.recordStateReadError(err)
		return nil, err
	}

	if readAccount == nil {
		if ibs.versionMap != nil {
			readAccount, accountSource, accountVersion, err = refreshAccount(ibs, addr)

			if readAccount == nil || err != nil {
				return nil, err
			}

			destructed, _, _, err := refreshSelfDestruct(ibs, addr)

			if destructed || err != nil {
				if !ibs.noMaterialize {
					so := ibs.allocStateObject()
					so.db = ibs
					so.address = addr
					so.selfdestructed = destructed
					so.deleted = destructed
					ibs.setStateObject(addr, so)
				}
				return nil, err
			}
		} else {
			ibs.nilAccounts[addr] = struct{}{}
			if bi, ok := ibs.balanceInc[addr]; ok && !bi.transferred {
				return ibs.createObject(addr, nil), nil
			}
			return nil, nil
		}
	}

	var code accounts.Code

	if ibs.versionMap != nil {
		account = readAccount

		// A prior tx may have self-destructed this account while stateReader still
		// returns the committed value; read SelfDestructPath directly from the
		// versionMap (versionedReadCore short-circuits on the same flag).
		if destructed, res, ok := ibs.versionMap.ReadSelfDestruct(addr, ibs.txIndex); ok && res.resolved() && destructed {
			// Only honour if the current tx hasn't already resurrected.
			localResurrected := false
			if sdVal, ok := ibs.versionedWriteSelfDestruct(addr); ok {
				if !sdVal {
					localResurrected = true
				}
			}
			if !localResurrected {
				if !ibs.noMaterialize {
					so := ibs.allocStateObject()
					so.db = ibs
					so.address = addr
					so.selfdestructed = true
					so.deleted = true
					ibs.setStateObject(addr, so)
				}
				return nil, nil
			}
		}

		code, err = refreshCode(ibs, addr)
		if err != nil {
			return nil, err
		}
	} else {
		account = readAccount
	}

	if recordRead {
		ibs.accountRead(addr, account, accountSource, accountVersion)
	}
	obj := newObject(ibs, addr, account, account)
	if code.Bytes != nil {
		// Code loaded from the version map must carry a matching CodeHash: a stale
		// hash lets SetCode's revert-to-original optimisation wrongly delete code
		// writes.
		obj.code = code
		if code.Hash != obj.data.CodeHash {
			obj.data.CodeHash = code.Hash
			obj.original.CodeHash = code.Hash
		}
	}
	if ibs.noMaterialize {
		ibs.reconstructCellFlags(obj, addr)
		return obj, nil
	}
	ibs.setStateObject(addr, obj)
	return obj, nil
}

func (ibs *IntraBlockState) setStateObject(addr accounts.Address, object *stateObject) {
	if dbg.AssertEnabled && object.arena {
		// stateObjects lives for the block, an arena slot only for the transaction.
		panic(fmt.Sprintf("arena slot cached in stateObjects: %x", addr))
	}
	if bi, ok := ibs.balanceInc[addr]; ok && !bi.transferred && ibs.versionMap == nil {
		object.data.Balance = u256.Add(object.data.Balance, bi.increase)
		bi.transferred = true
		ibs.journal.balanceIncreaseTransfer(bi)
	}
	ibs.stateObjects[addr] = object
}

// Retrieve a state object or create a new state object if nil.
func (ibs *IntraBlockState) GetOrNewStateObject(addr accounts.Address) (*stateObject, error) {
	stateObject, err := ibs.getStateObject(addr, true)
	if err != nil {
		return nil, err
	}
	if stateObject == nil || stateObject.deleted {
		stateObject = ibs.createObject(addr, stateObject /* previous */)
	}
	return stateObject, nil
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten.
func (ibs *IntraBlockState) createObject(addr accounts.Address, previous *stateObject) (newobj *stateObject) {
	account := &accounts.Account{}
	var original *accounts.Account
	if previous == nil {
		original = &accounts.Account{}
	} else {
		original = &previous.original
	}

	account.Root.SetBytes(empty.RootHash[:]) // old storage should be ignored
	newobj = newObject(ibs, addr, account, original)
	newobj.setNonce(0) // sets the object to dirty
	if previous == nil {
		ibs.journal.createObjectChange(addr)
	} else {
		var prevWrites *createWriteSnapshot
		if ibs.versionMap != nil {
			prevWrites = ibs.versionedWrites.snapshotCreateFields(addr)
		}
		// previous may be built from an account record that lags the cached
		// object: journal the cache entry (nil when none) so the revert restores
		// or drops it.
		ibs.journal.resetObjectChange(addr, ibs.stateObjects[addr], prevWrites)
	}
	newobj.newlyCreated = true
	if !ibs.noMaterialize {
		ibs.setStateObject(addr, newobj)
	}
	ibs.recordWriteAddress(addr, &newobj.data)
	// Write CodeHashPath so that any stale versionedReads cache entry
	// (e.g. from the pre-creation GetCodeHash check in EVM create()) is
	// invalidated.  newObject normalises the zero-value CodeHash to
	// EmptyCodeHash, so this records keccak256("") for a fresh account.
	ibs.recordWriteCodeHash(addr, newobj.data.CodeHash)
	// Write NoncePath only when a prior incarnation's nonce still sits in the version-map
	// floor, which a commitment read would otherwise resurrect. A genuinely fresh account
	// has no floor, so a nonce=0 write there is a net-zero change the BAL wrongly reports.
	if previous != nil {
		ibs.recordWriteNonce(addr, newobj.data.Nonce, tracing.NonceChangeNewContract)
	} else if ibs.versionMap != nil {
		if _, res, ok := ibs.versionMap.ReadNonce(addr, ibs.txIndex); ok && res.resolved() {
			ibs.recordWriteNonce(addr, newobj.data.Nonce, tracing.NonceChangeNewContract)
		}
	}
	return newobj
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//  1. sends funds to sha(account ++ (nonce + 1))
//  2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (ibs *IntraBlockState) CreateAccount(addr accounts.Address, contractCreation bool) (err error) {
	var prevInc uint64
	var previous *stateObject

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer func() {
			var creatingContract string
			if contractCreation {
				creatingContract = " (contract)"
			}
			if err != nil {
				fmt.Printf("%d (%d.%d) Create Account%s: %x, err=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, creatingContract, addr, err)
			} else {
				var bal uint256.Int
				if previous != nil {
					bal = previous.data.Balance
				}
				fmt.Printf("%d (%d.%d) Create Account%s: %x, balance=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, creatingContract, addr, bal.String())
			}
		}()
	}

	if ibs.versionMap == nil {
		previous, err = ibs.getStateObject(addr, true)
		if err != nil {
			return err
		}
	} else {
		readAccount, _, _, err := ibs.getVersionedAccount(addr, true)
		if err != nil {
			return err
		}

		if readAccount != nil {
			account := readAccount

			destructed, _, _, err := refreshSelfDestruct(ibs, addr)
			if err != nil {
				return err
			}
			_, ownSD := ibs.versionedWriteSelfDestruct(addr)

			// Reuse the cached stateObject as `previous` so the selfdestructed flag
			// and accumulated incarnation carry into the new object's
			// PrevIncarnation.
			if !destructed {
				if so, ok := ibs.stateObjects[addr]; ok && so.selfdestructed {
					// Accumulated-IBS path (e.g. GenerateChain): the cache marks it
					// selfdestructed but versionedReadCore returned false via the
					// so.deleted early exit; reuse the cached object.
					previous = so
				} else if ibs.versionMap != nil {
					// Fresh-IBS worker path: no cache, but the versionMap may hold a
					// prior tx's SelfDestructPath=true (versionedReadCore short-circuits
					// it), so read the versionMap directly.
					if d, res, ok := ibs.versionMap.ReadSelfDestruct(addr, ibs.txIndex); ok && res.resolved() && d {
						destructed = true
					}
				}
			}

			// A later tx that left the account non-empty revived it after a prior
			// tx's self-destruct. Check the field cells, as the record lags them.
			if destructed && !ownSD {
				if destructed, err = ibs.emptyFromVersionedFields(addr, account); err != nil {
					return err
				}
			}

			if previous == nil {
				previous = newObject(ibs, addr, account, account)
				previous.selfdestructed = destructed
			}
		} else if so, ok := ibs.stateObjects[addr]; ok && so.deleted {
			// Selfdestructed in an earlier tx this block with no committed base
			// (created within the block); preserve the deleted object as `previous`
			// so CommitBlock can still emit DeleteAccount after a REVERT.
			previous = so
		} else if so, ok := ibs.stateObjects[addr]; ok {
			// The serial block builder does not flush per-tx writes to the version
			// map, so a same-block credit lives only in the cache; reuse it as
			// `previous` to keep the balance carry-over below.
			previous = so
		} else if sd, ok := ibs.versionedWriteSelfDestruct(addr); ok && sd {
			// Cache-free parallel path: a within-tx create→self-destruct leaves no
			// base record and no cached object. Rebuild `previous` as destroyed so
			// the resurrect write is emitted.
			previous = newObject(ibs, addr, &accounts.Account{}, &accounts.Account{})
			previous.selfdestructed = true
		}
	}

	if err != nil {
		return err
	}
	if previous != nil && previous.selfdestructed {
		prevInc = previous.data.Incarnation
	} else {
		prevInc = 0
	}
	if previous != nil && prevInc < previous.data.PrevIncarnation {
		prevInc = previous.data.PrevIncarnation
	}
	// Capture each path's own (source, version) for the synthetic reads stamped
	// below; inheriting the account-record version would trip the validator on
	// the recursive AddressPath check.
	incSource, incVersion := StorageRead, UnknownVersion
	if ibs.versionMap != nil {
		if inc, res, ok := ibs.versionMap.ReadIncarnation(addr, ibs.txIndex); ok && res.resolved() {
			incSource = MapRead
			incVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
			if inc > prevInc {
				prevInc = inc
			}
		}
	}
	balSource, balVersion := StorageRead, UnknownVersion
	var preTxBalance uint256.Int
	balWiped := false
	if ibs.versionMap != nil && !ibs.noConflictDetection {
		if bal, res, ok := ibs.versionMap.ReadBalance(addr, ibs.txIndex); ok && res.resolved() {
			balSource = MapRead
			balVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
			preTxBalance = bal
			// A burning destruct at or above the cell wipes the balance. As on the read
			// path, the zero depends on the destruct, not on the wiped cell.
			if destructed, sdRes, ok := ibs.versionMap.ReadSelfDestruct(addr, ibs.txIndex); ok && sdRes.resolved() && destructed &&
				sdRes.DepIdx() >= res.DepIdx() && res.ValStatus() == ValueDeleted {
				preTxBalance.Clear()
				balWiped = true
				ibs.versionedReads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}},
					Val:        true,
				})
			}
		}
	}
	// Capture the balance BEFORE createObject writes the fresh zero-balance
	// record: previous.data.Balance can lag an in-block credit or committed
	// prefund, and reading after createObject would see the just-written zero.
	var carryBalance uint256.Int
	carryBalanceValid := previous != nil && !previous.selfdestructed
	if previous != nil {
		b, _, err := ibs.getBalance(addr)
		if err != nil {
			return err
		}
		// Carry the live balance over the create. Normally zero for a self-destructed
		// previous, but a same-tx credit may have revived the account before this create
		// (the EVM preserves funds sent to a not-yet-created address), so carry a non-zero
		// revived balance too.
		if carryBalanceValid || !b.IsZero() {
			carryBalance = b
			carryBalanceValid = true
		}
	}
	newObj := ibs.createObject(addr, previous)
	if previous != nil && previous.selfdestructed {
		// The reset-object journal entry marks addr dirty, but that mark is dropped
		// on revert; this un-journalled increment keeps a resurrected address in
		// journal.dirties across an intra-tx revert.
		ibs.journal.dirty(addr)
	}
	if carryBalanceValid {
		newObj.data.Balance.Set(&carryBalance)
	} else if previous == nil && balSource == MapRead {
		// This tx already read the account destroyed, so there is no record, but a
		// prior tx's fee or transfer revived it: carry that recorded pre-tx balance.
		newObj.data.Balance.Set(&preTxBalance)
	}
	newObj.data.PrevIncarnation = prevInc

	if contractCreation {
		newObj.createdContract = true
		newObj.data.Incarnation = prevInc + 1
		// Record contract creation so FinalizedWrites does not remove a newly
		// deployed contract as empty.
		ibs.recordWriteCreateContract(addr, true)
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) New Incarnation %x: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, newObj.data.Incarnation)
		}
	} else {
		newObj.selfdestructed = false
	}

	// Synthetic reads let creation clashes between transactions be detected. The
	// balance read carries the pre-tx balance (zero without a cell: the account was
	// absent), since later reads, also after a revert, are served from it. An earlier
	// internal read is promoted to give the balance write a block-access-list baseline.
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap != nil && !ibs.noConflictDetection {
		if vr, seen := ibs.versionedReads.GetBalance(addr); !seen && !balWiped {
			ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader{Source: balSource, Version: balVersion}, preTxBalance})
		} else if seen && vr.internal {
			vr.internal = false
			ibs.versionedReads.SetBalance(addr, vr)
		}
		ibs.versionedReads.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader{Source: incSource, Version: incVersion}, prevInc})
	}
	ibs.recordWriteBalance(addr, newObj.Balance())
	ibs.recordWriteIncarnation(addr, newObj.data.Incarnation)
	if previous == nil || previous.selfdestructed && !newObj.selfdestructed {
		ibs.recordWriteSelfDestruct(addr, false)
	}

	return nil
}

// Snapshot returns an identifier for the current revision of the state.
func (ibs *IntraBlockState) PushSnapshot() int {
	return ibs.revisions.snapshot(ibs.journal, len(ibs.logs.entries))
}

func (ibs *IntraBlockState) PopSnapshot(snapshot int) {
	ibs.revisions.returnSnapshot(snapshot)
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (ibs *IntraBlockState) RevertToSnapshot(revid int, err error) {
	var traced bool
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TracingAccounts()) {
		for addr := range ibs.journal.dirties {
			if ibs.trace || dbg.TraceAccount(addr.Handle()) {
				traced = true
				if err == nil {
					fmt.Printf("%d (%d.%d) Reverting %x, revid: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, revid)
				} else {
					fmt.Printf("%d (%d.%d) Reverting %x, revid: %d: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, revid, err)
				}
			}
		}
	}

	snapshot := ibs.revisions.revertToSnapshot(revid)
	// Replay the journal to undo changes and remove invalidated snapshots
	ibs.journal.revert(ibs, snapshot.journalIndex)
	ibs.logs.truncate(snapshot.logCount)

	if traced {
		fmt.Printf("%d (%d.%d) Reverted: %d:%d\n", ibs.blockNum, ibs.txIndex, ibs.version, revid, snapshot.journalIndex)
	}
}

// GetRefund returns the current value of the refund counter.
func (ibs *IntraBlockState) GetRefund() uint64 {
	return ibs.refund
}

// EIP161EmptyRemoval reports whether an empty account at addr is removed under
// EIP-161 (SpuriousDragon). AuRa retains its SystemAddress even when empty, to
// match the reference implementation.
func EIP161EmptyRemoval(eip161Enabled, isAura bool, addr accounts.Address) bool {
	return eip161Enabled && (!isAura || addr != params.SystemAddress)
}

func updateAccount(eip161Enabled bool, isAura bool, stateWriter StateWriter, addr accounts.Address, stateObject *stateObject, isDirty bool, trace bool, tracingHooks *tracing.Hooks, useBlockOrigin bool, eip8246 bool) error {
	stateObject.db.journal.epoch++ // storage moves to committed, deletions apply — bump the slot-cache stamp
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	// EIP-8246: a self-destructed account that still holds a balance is reset to
	// a balance-only account (nonce 0, empty code, empty storage) not deleted.
	sdPreserveBalance := eip8246 && stateObject.selfdestructed && !stateObject.data.Balance.IsZero()
	if (stateObject.selfdestructed && !sdPreserveBalance) || (isDirty && emptyRemoval) {
		balance := stateObject.Balance()
		if tracingHooks != nil && tracingHooks.OnBalanceChange != nil && !(&balance).IsZero() && stateObject.selfdestructed {
			tracingHooks.OnBalanceChange(stateObject.address, balance, uint256.Int{}, tracing.BalanceDecreaseSelfdestructBurn)
		}
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (trace || dbg.TraceAccount(addr.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Delete Account: %x selfdestructed=%v stack=%s\n", stateObject.db.blockNum, stateObject.db.txIndex, stateObject.db.version, addr, stateObject.selfdestructed, dbg.Stack())
			}
		}
		if err := stateWriter.DeleteAccount(addr, &stateObject.original); err != nil {
			return err
		}
		stateObject.deleted = true
	}
	if sdPreserveBalance {
		stateObject.data.Nonce = 0
		stateObject.data.CodeHash = accounts.EmptyCodeHash
		stateObject.data.Incarnation = 0
		stateObject.code = accounts.Code{}
		stateObject.deleted = false
		if err := stateWriter.CreateContract(addr); err != nil {
			return err
		}
		if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
			return err
		}
	} else if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		stateObject.deleted = false
		// A recreate below the original incarnation makes the writer clear the old code
		// and storage on the account write, so that write must land before the new ones.
		downRev := stateObject.original.Incarnation > stateObject.data.Incarnation
		if downRev {
			if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
				return err
			}
		}
		// Write any contract code associated with the state object; dirtyCode is
		// set only when code actually changed, so a clear-to-empty must still
		// write through (empty CodeDomain, consistent with the empty codeHash).
		if stateObject.dirtyCode {
			if err := stateWriter.UpdateAccountCode(addr, stateObject.data.Incarnation, stateObject.data.CodeHash, stateObject.code.Bytes); err != nil {
				return err
			}
		}
		if stateObject.createdContract {
			if err := stateWriter.CreateContract(addr); err != nil {
				return err
			}
		}
		if err := stateObject.updateStorage(stateWriter, useBlockOrigin); err != nil {
			return err
		}
		if dbg.TraceDomainIO || (dbg.TraceTransactionIO && (trace || dbg.TraceAccount(addr.Handle()))) {
			if _, ok := stateWriter.(*NoopWriter); !ok || dbg.TraceNoopIO {
				fmt.Printf("%d (%d.%d) Update Account Data (%T): %x balance:%d,nonce:%d,codehash:%x\n",
					stateObject.db.blockNum, stateObject.db.txIndex, stateObject.db.version, stateWriter, addr, &stateObject.data.Balance, stateObject.data.Nonce, stateObject.data.CodeHash)
			}
		}
		if !downRev {
			if err := stateWriter.UpdateAccountData(addr, &stateObject.original, &stateObject.data); err != nil {
				return err
			}
		}
		// In parallel mode only modified fields land in the versionMap WriteSet.
	}
	return nil
}

func printAccount(eip161Enabled bool, isAura bool, addr accounts.Address, stateObject *stateObject, isDirty bool) {
	emptyRemoval := EIP161EmptyRemoval(eip161Enabled, isAura, addr) && stateObject.data.Empty()
	if stateObject.selfdestructed || (isDirty && emptyRemoval) {
		fmt.Printf("delete: %x\n", addr)
	}
	if isDirty && (stateObject.createdContract || !stateObject.selfdestructed) && !emptyRemoval {
		// Write any contract code associated with the state object
		if stateObject.code.Bytes != nil && stateObject.dirtyCode {
			fmt.Printf("UpdateCode: %x,%x\n", addr, stateObject.data.CodeHash)
		}
		if stateObject.createdContract {
			fmt.Printf("CreateContract: %x\n", addr)
		}
		stateObject.printTrie()
		fmt.Printf("UpdateAccountData: %x, balance=%s, nonce=%d\n", addr, stateObject.data.Balance.String(), stateObject.data.Nonce)
	}
}

// FinalizeTx should be called after every transaction.
func (ibs *IntraBlockState) FinalizeTx(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range ibs.balanceInc {
		if !bi.transferred {
			if _, err := ibs.getStateObject(addr, true); err != nil {
				return err
			}
		}
	}
	for addr := range ibs.journal.dirties {
		so, exist := ibs.stateObjects[addr]
		if !exist {
			// ripeMD is 'touched' at block 1714175, in txn 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That txn goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `ibs.journal.dirties` but not in `ibs.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}

		if err := updateAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, stateWriter, addr, so, true, ibs.trace, ibs.tracingHooks, false, chainRules.IsAmsterdam); err != nil {
			return err
		}

		// EIP-6780 + EIP-7928: SELFDESTRUCT of a same-tx created contract wipes
		// storage at end-of-tx, so the BAL records dirty slots as reads. Zero the
		// storage writes so AsBlockAccessList folds them away. Must run before
		// so.newlyCreated = false.
		if ibs.versionMap != nil && so.selfdestructed && so.newlyCreated {
			for key := range so.dirtyStorage {
				ibs.recordWriteStorage(addr, key, uint256.Int{}, uint256.Int{})
			}
		}

		// EIP-8246: a balance-preserving SELFDESTRUCT leaves the account alive. The
		// block assembler reuses one IBS across txs, so replace the destroyed
		// object with a clean balance-only one, after the cleanup above that still
		// needs the selfdestructed marker.
		if so.selfdestructed && !so.deleted {
			preserved := accounts.NewAccount()
			preserved.Balance = so.data.Balance
			ibs.stateObjects[addr] = newObject(ibs, addr, &preserved, &preserved)
		}

		so.newlyCreated = false
		ibs.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	ibs.clearJournalAndRefund()
	return nil
}

func (ibs *IntraBlockState) SoftFinalise() {
	for addr := range ibs.journal.dirties {
		// On the parallel path a write can be recorded without materializing a
		// stateObject, so dirtiness comes from the journal, not object existence.
		// The serial path keeps the object gate: a touched-but-reverted address
		// lingers in journal.dirties without a stateObject.
		if _, exist := ibs.stateObjects[addr]; !exist && ibs.versionMap == nil {
			continue
		}
		ibs.stateObjectsDirty[addr] = struct{}{}
	}
	// Invalidate journal because reverting across transactions is not allowed.
	ibs.clearJournalAndRefund()
}

// CommitBlock finalizes the state by removing the self destructed objects
// and clears the journal as well as the refunds.
func (ibs *IntraBlockState) CommitBlock(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr, bi := range ibs.balanceInc {
		if !bi.transferred {
			if _, err := ibs.getStateObject(addr, true); err != nil {
				return err
			}
		}
	}
	return ibs.MakeWriteSet(chainRules, stateWriter)
}

// ExtractAndClearDirty snapshots the current stateObjectsDirty set and clears it.
// Used by eth_simulateV1 to separate accounts dirtied by stateOverrides from those
// dirtied by actual transaction execution, so CommitBlock does not apply EIP-161 to
// override-only accounts.
func (ibs *IntraBlockState) ExtractAndClearDirty() map[accounts.Address]struct{} {
	dirty := maps.Clone(ibs.stateObjectsDirty)
	clear(ibs.stateObjectsDirty)
	return dirty
}

// CommitOverrideDirtyAccounts writes state-override accounts that were not subsequently
// touched by any transaction (and therefore not handled by CommitBlock).  EIP-161 is
// intentionally disabled: override accounts are simulation-only mutations and must not
// be removed simply because they are "empty" by consensus rules.
func (ibs *IntraBlockState) CommitOverrideDirtyAccounts(chainRules *chain.Rules, stateWriter StateWriter, overrideDirty map[accounts.Address]struct{}) error {
	for addr := range overrideDirty {
		if _, alsoTxDirty := ibs.stateObjectsDirty[addr]; alsoTxDirty {
			continue // CommitBlock already handled this address
		}
		so, exists := ibs.stateObjects[addr]
		if !exists || so.deleted {
			continue
		}
		if err := updateAccount(false, chainRules.IsAura, stateWriter, addr, so, true, ibs.trace, ibs.tracingHooks, true, chainRules.IsAmsterdam); err != nil {
			return err
		}
	}
	return nil
}

func (ibs *IntraBlockState) BalanceIncreaseSet() map[accounts.Address]uint256.Int {
	s := make(map[accounts.Address]uint256.Int, len(ibs.balanceInc))
	for addr, bi := range ibs.balanceInc {
		if !bi.transferred {
			s[addr] = bi.increase
		}
	}
	return s
}

func (ibs *IntraBlockState) MakeWriteSet(chainRules *chain.Rules, stateWriter StateWriter) error {
	for addr := range ibs.journal.dirties {
		ibs.stateObjectsDirty[addr] = struct{}{}
	}
	for addr, stateObject := range ibs.stateObjects {
		_, isDirty := ibs.stateObjectsDirty[addr]
		if dbg.TraceAccount(addr.Handle()) {
			var updated *uint256.Int
			if w, ok := ibs.versionedWrites.GetBalance(addr); ok {
				val := w.Val
				updated = &val
			}
			var dirty string
			if isDirty {
				dirty = " (dirty)"
			}
			if updated != nil {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %s (%d)\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, dirty, stateObject.data.Balance.String(), updated)
			} else {
				fmt.Printf("%d (%d.%d) Updated Balance: %x%s: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, dirty, stateObject.data.Balance.String())
			}
		}
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Update Account %x\n", ibs.blockNum, ibs.txIndex, ibs.version, addr)
		}
		if err := updateAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, stateWriter, addr, stateObject, isDirty, ibs.trace, ibs.tracingHooks, true, chainRules.IsAmsterdam); err != nil {
			return err
		}
		// EIP-6780 + EIP-7928: SELFDESTRUCT of a same-tx created contract clears
		// storage at end-of-tx, so the BAL records dirty slots as reads. Zero the
		// storage writes so AsBlockAccessList folds them away.
		if ibs.versionMap != nil && stateObject.selfdestructed && stateObject.newlyCreated {
			for key := range stateObject.dirtyStorage {
				ibs.recordWriteStorage(addr, key, uint256.Int{}, uint256.Int{})
			}
		}
	}

	var reverted []accounts.Address

	ibs.versionedWrites.forEachAddr(func(addr accounts.Address) {
		if _, isDirty := ibs.stateObjectsDirty[addr]; !isDirty {
			reverted = append(reverted, addr)
		}
	})

	for _, addr := range reverted {
		ibs.versionMap.DeleteAll(addr, ibs.txIndex)
		ibs.versionedWrites.deleteAddr(addr)
	}

	// Invalidate journal because reverting across transactions is not allowed.
	ibs.clearJournalAndRefund()
	return nil
}

// FinalizedWrites applies EIP-6780 normalization and EIP-161 filtering, then
// returns a detached committable snapshot. The EIP-161 filtering reads pre-tx state,
// so it returns the IBS state read error, which ResetVersionedIO would clear.
func (ibs *IntraBlockState) FinalizedWrites(chainRules *chain.Rules) (*WriteSet, error) {
	// Publish this tx's cold-read committed origins to the versionMap before EIP-161
	// composition below reads it (encodeExistingEmptyRemovals builds a versionMap-backed
	// account view). Every FinalizedWrites caller is covered here, so no read path needs
	// to remember to publish separately.
	ibs.PublishOrigins()
	writes := ibs.versionedWrites.Finalize()
	ibs.withholdCreatedEmptyAccounts(chainRules, writes)
	ibs.encodeExistingEmptyRemovals(chainRules, writes)
	return writes, ibs.StateReadError()
}

// encodeExistingEmptyRemovals emits the EIP-161 delete for a pre-existing account
// left empty (balance 0, nonce 0, empty code): its account-field writes are
// dropped and replaced with a SelfDestructPath write, so the domain apply and the
// commitment calculator both remove the leaf from the writes without re-deriving
// emptiness. Created-empty accounts are already withheld by
// withholdCreatedEmptyAccounts, so only pre-existing empties reach here.
func (ibs *IntraBlockState) encodeExistingEmptyRemovals(chainRules *chain.Rules, writes *WriteSet) {
	if chainRules == nil || ibs.versionMap == nil {
		return
	}
	for addr := range writes.addrs() {
		if !EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr) {
			continue
		}
		view := NewVersionedAccountView(addr, ibs.txIndex, ibs.versionMap, ibs.stateReader)
		if err := view.Err(); err != nil {
			// A failed base read zeroes the composed fields, which would read as
			// an EIP-161 empty and delete a live account. Surface the error and
			// leave the account's writes intact instead.
			ibs.recordStateReadError(err)
			continue
		}
		bal, nonce, codeHash := view.GetBalance(), view.GetNonce(), view.GetCodeHash()
		if w, ok := writes.GetBalance(addr); ok {
			bal = w.Val
		}
		if w, ok := writes.GetNonce(addr); ok {
			nonce = w.Val
		}
		if w, ok := writes.GetCodeHash(addr); ok {
			codeHash = w.Val
		}
		if !bal.IsZero() || nonce != 0 || !codeHash.IsEmpty() {
			continue
		}
		// Clear the whole account footprint, not just the fields: a surviving AddressPath
		// write alongside the SelfDestruct reads as a same-tx SD+CREATE2 recreate, so
		// AccountLifecycleAt would see the removed account as Revived. A real SELFDESTRUCT
		// was a contract: keep the CodeHash ValueDeleted "code removed" signal so the fee
		// calc burns its delayed fee instead of reviving it as an EOA.
		if sd, ok := writes.GetSelfDestruct(addr); ok && sd.Val {
			writes.SetContractRemoval(addr, ibs.Version())
		} else {
			writes.SetEmptyRemoval(addr, ibs.Version())
		}
	}
}

// withholdCreatedEmptyAccounts drops writes for accounts the tx observed as
// absent and left empty (EIP-161): such an account is absent both before and
// after the tx, so omitting its writes is exact and no deletion marker is
// needed. An account that existed before is kept even when it ends empty —
// clearing it needs an explicit delete, which encodeExistingEmptyRemovals emits.
func (ibs *IntraBlockState) withholdCreatedEmptyAccounts(chainRules *chain.Rules, writes *WriteSet) {
	if chainRules == nil {
		return
	}
	for addr := range writes.address {
		if !EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr) {
			continue
		}
		read, ok := ibs.versionedReads.GetAddress(addr)
		if (read.Val != nil && !read.Val.IsNil()) || !writes.createdEmpty(addr) {
			continue
		}
		// With no address read this tx, the tx didn't observe the account's
		// pre-tx state — which happens when an empty account created earlier in
		// the block lingers in the IBS (e.g. a coinbase touched by a zero tip).
		// Fall back to the pre-block base: a loaded account carries a non-zero
		// CodeHash (EmptyCodeHash for an EOA), so a zero CodeHash means the
		// account never existed and its created-empty writes are withheld;
		// otherwise it existed and needs an explicit delete, so keep the writes.
		if !ok {
			if so, soOk := ibs.stateObjects[addr]; soOk && so.original.CodeHash != (accounts.CodeHash{}) {
				continue
			}
		}
		// Stamp the uniform removal rather than silently dropping the writes: the
		// during-tx exec cells already live in the version map, so a bare deleteAddr
		// here leaves the commitment reading the created-empty account as present. A
		// balance ValueDeleted at the finalize index removes the leaf, matching the
		// domain delete.
		writes.SetEmptyRemoval(addr, ibs.Version())
	}
}

// MergeTxIOInto folds the current transaction's reads and supplied writes into io.
func (ibs *IntraBlockState) MergeTxIOInto(io *VersionedIO, writes *WriteSet) {
	version := Version{BlockNum: ibs.blockNum, TxIndex: ibs.txIndex, Incarnation: ibs.version}
	io.mergeTx(version, ibs.versionedReads, writes)
}

// FlushWritesToVersionMap publishes the supplied writes to this state's version map.
func (ibs *IntraBlockState) FlushWritesToVersionMap(writes *WriteSet) {
	if ibs.versionMap == nil {
		return
	}
	writes.assertNotArena("flush")
	ibs.versionMap.FlushVersionedWrites(writes, true)
}

// WriteFlushedTo hands the current tx's writes, already flushed to the versionMap, to
// stateWriter the way FinalizeTx hands a materialized tx's: each account as it reads
// before and after the tx, each storage slot with its value at tx start.
func (ibs *IntraBlockState) WriteFlushedTo(writes *WriteSet, stateWriter StateWriter) error {
	for addr := range writes.addrs() {
		before := NewVersionedAccountView(addr, ibs.txIndex, ibs.versionMap, ibs.stateReader)
		original := before.Account()
		if err := before.Err(); err != nil {
			return err
		}
		if original == nil {
			original = &accounts.Account{}
		}
		after := NewVersionedAccountView(addr, ibs.txIndex+1, ibs.versionMap, ibs.stateReader)
		account := after.Account()
		if err := after.Err(); err != nil {
			return err
		}
		// A ValueDeleted balance write is the removal verdict (an EIP-161 touched-empty or a
		// burning self-destruct) encoded as data at finalization; compose() does not gate on
		// it, so force the account absent to take the delete / EIP-8246-preserve path below.
		if bal, ok := writes.GetBalance(addr); ok && bal.Deletes() {
			account = nil
		}
		if account == nil {
			if bal, ok := writes.GetBalance(addr); ok && !bal.Deletes() && !bal.Val.IsZero() {
				// An EIP-8246 SELFDESTRUCT leaves a balance-only account, as updateAccount does.
				preserved := accounts.NewAccount()
				preserved.Balance = bal.Val
				if err := stateWriter.CreateContract(addr); err != nil {
					return err
				}
				if err := stateWriter.UpdateAccountData(addr, original, &preserved); err != nil {
					return err
				}
				continue
			}
			if err := stateWriter.DeleteAccount(addr, original); err != nil {
				return err
			}
			continue
		}
		if code, ok := writes.GetCode(addr); ok {
			if err := stateWriter.UpdateAccountCode(addr, account.Incarnation, account.CodeHash, code.Val.Bytes); err != nil {
				return err
			}
		}
		if created, ok := writes.GetCreateContract(addr); ok && created.Val {
			if err := stateWriter.CreateContract(addr); err != nil {
				return err
			}
		}
		for key, vw := range writes.storage[addr] {
			if err := stateWriter.WriteAccountStorage(addr, account.Incarnation, key, vw.orig, vw.Val); err != nil {
				return err
			}
		}
		if err := stateWriter.UpdateAccountData(addr, original, account); err != nil {
			return err
		}
	}
	return nil
}

func (ibs *IntraBlockState) Print(chainRules chain.Rules, all bool) {
	for addr, stateObject := range ibs.stateObjects {
		_, isDirty := ibs.stateObjectsDirty[addr]
		_, isDirty2 := ibs.journal.dirties[addr]

		printAccount(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr, stateObject, all || isDirty || isDirty2)
	}
}

// SetTxContext sets the current transaction index which
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (ibs *IntraBlockState) SetTxContext(bn uint64, ti int) {
	/* Not sure what this test is for it seems to break some tests
	if len(ibs.logs.entries) > 0 && ti == 0 {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. len(ibs.logs.entries)=%d, ti=%d", len(ibs.logs.entries), ti)
		panic(err)
	}
	if ibs.txIndex >= 0 && ibs.txIndex > ti {
		err := fmt.Errorf("seems you forgot `ibs.Reset` or `ibs.TxIndex()`. ibs.txIndex=%d, ti=%d", ibs.txIndex, ti)
		panic(err)
	}
	*/
	ibs.txIndex = ti
	ibs.blockNum = bn
	ibs.sdProbeEpoch++
}

// ResumeLogIndexAt continues a block's log numbering at idx, for a caller that
// starts execution in the middle of a block and has to hand in the count the
// transactions it skipped left behind. Called mid-block it renumbers everything
// after it, so it belongs before the first log of the first transaction.
func (ibs *IntraBlockState) ResumeLogIndexAt(idx uint32) {
	ibs.logs.indexInBlock = uint(idx)
}

// ResetLogs empties the block's logs and takes the numbering back to zero,
// keeping the state changes Reset would drop.
func (ibs *IntraBlockState) ResetLogs() {
	ibs.logs.reset()
}

// no not lock
func (ibs *IntraBlockState) clearJournalAndRefund() {
	ibs.journal.Reset()
	ibs.revisions.reset()
	ibs.refund = uint64(0)
	if dbg.AssertEnabled && !ibs.noMaterialize && !ibs.stateObjectArena.empty() {
		// Slots are rewound per transaction, so only the path that caches nothing
		// may draw them.
		panic("stateObjectArena not empty with noMaterialize=false")
	}
	ibs.stateObjectArena.reset() // same lifetime with `journal`
}

// Prepare handles the preparatory steps for executing a state transition.
// This method must be invoked before state transition.
//
// Berlin fork:
// - Add sender to access list (EIP-2929)
// - Add destination to access list (EIP-2929)
// - Add precompiles to access list (EIP-2929)
// - Add the contents of the optional txn access list (EIP-2930)
//
// Shanghai fork:
// - Add coinbase to access list (EIP-3651)
//
// Cancun fork:
// - Reset transient storage (EIP-1153)
func (ibs *IntraBlockState) Prepare(rules *chain.Rules, sender, coinbase accounts.Address, dst accounts.Address,
	precompiles []accounts.Address, list types.AccessList,
) {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(sender.Handle()) || !dst.IsNil() && dbg.TraceAccount(dst.Handle())) {
		fmt.Printf("%d (%d.%d) ibs.Prepare: sender: %x, coinbase: %x, dest: %x, %x, %v, %v\n", ibs.blockNum, ibs.txIndex, ibs.version, sender, coinbase, dst, precompiles, list, rules)
	}
	ibs.txOutputFree = true
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		ibs.accessList.Reset()
		al := &ibs.accessList

		al.AddAddress(sender)
		if !dst.IsNil() {
			al.AddAddress(dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			address := accounts.InternAddress(el.Address)
			al.AddAddress(address)
			for _, key := range el.StorageKeys {
				al.AddSlot(address, accounts.InternKey(key))
			}
		}
		if rules.IsShanghai { // EIP-3651: warm coinbase
			al.AddAddress(coinbase)
		}
	}
	// Reset transient storage at the beginning of transaction execution
	clear(ibs.transientStorage)
	ibs.versionedReads.access = nil
	ibs.recordAccess = true

	// EIP-7928 records the EIP-3651 coinbase access even without a priority fee.
	// The coinbase is the block's fee recipient, a real (non-revertable) access,
	// so the system address is kept in the BAL when it is itself the coinbase.
	if rules.IsShanghai {
		ibs.MarkAddressAccess(coinbase, false)
	}
}

// AddAddressToAccessList adds the given address to the access list
func (ibs *IntraBlockState) AddAddressToAccessList(addr accounts.Address) (addrMod bool) {
	addrMod = ibs.accessList.AddAddress(addr)
	if addrMod {
		ibs.journal.accessListAddAccountChange(addr)
	}
	return addrMod
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (ibs *IntraBlockState) AddSlotToAccessList(addr accounts.Address, slot accounts.StorageKey) (addrMod, slotMod bool) {
	addrMod, slotMod = ibs.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		ibs.journal.accessListAddAccountChange(addr)
	}
	if slotMod {
		ibs.journal.accessListAddSlotChange(addr, slot)
	}
	return addrMod, slotMod
}

// AddressInAccessList returns true if the given address is in the access list.
func (ibs *IntraBlockState) AddressInAccessList(addr accounts.Address) bool {
	return ibs.accessList.ContainsAddress(addr)
}

func (ibs *IntraBlockState) SlotInAccessList(addr accounts.Address, slot accounts.StorageKey) (addressPresent bool, slotPresent bool) {
	return ibs.accessList.Contains(addr, slot)
}

// SlotKnownWarm is a conservative fast check: true means the (addr, slot) pair
// is warm; false means unknown — callers must fall back to AddSlotToAccessList.
// It stays cheap enough to inline at every SLOAD/SSTORE gas-charge site.
func (ibs *IntraBlockState) SlotKnownWarm(addr accounts.Address, slot accounts.StorageKey) bool {
	return ibs.accessList.lastSlots != nil && ibs.accessList.lastAddr == addr && slot == ibs.accessList.lastWarmSlot
}

func (ibs *IntraBlockState) MarkAddressAccess(addr accounts.Address, revertable bool) {
	if !ibs.recordAccess {
		return
	}
	if ibs.versionedReads.access == nil {
		ibs.versionedReads.access = make(AccessSet)
	}
	if opts, ok := ibs.versionedReads.access[addr]; ok {
		if opts.revertable && !revertable {
			opts.revertable = false
			ibs.versionedReads.access[addr] = opts
		}
	} else {
		ibs.versionedReads.access[addr] = accessOptions{revertable: revertable}
	}
}

// StartAccessRecording enables versioned access tracking until ResetVersionedIO.
// Block finalization re-enables it (Prepare only runs for user txs) so that an
// address touched but left absent — e.g. a zero-amount withdrawal recipient —
// still reaches the BAL as an access-only entry: its reads are validation-only
// and FinalizedWrites withholds its created-empty writes.
func (ibs *IntraBlockState) StartAccessRecording() {
	ibs.recordAccess = true
}

// StopAccessRecording disables versioned access tracking and drops the recorded
// access set, used on the NoBAL path (eth_call) where no block access list is built.
func (ibs *IntraBlockState) StopAccessRecording() {
	ibs.recordAccess = false
	ibs.versionedReads.access = nil
}

// MarkReadsInternal marks all versioned reads for addr as internal.
// Internal reads are kept for parallel-execution conflict detection
// but excluded from the block access list (BAL).  This is used when
// a state read was performed for gas calculation but the operation
// was rejected (e.g. CALL with value inside STATICCALL).
func (ibs *IntraBlockState) MarkReadsInternal(addr accounts.Address) {
	ibs.versionedReads.ScanAddr(addr, func(_ AccountPath, _ accounts.StorageKey, hdr *ReadHeader) {
		hdr.internal = true
	})
}

func (ibs *IntraBlockState) AccessedAddr(addr accounts.Address) bool {
	_, ok := ibs.versionedReads.access[addr]
	return ok
}

func (ibs *IntraBlockState) accountRead(addr accounts.Address, account *accounts.Account, source ReadSource, version Version) {
	if ibs.versionMap != nil {
		ibs.MarkAddressAccess(addr, true)
		if source == WriteSetRead {
			// A read satisfied by this tx's own earlier write carries no
			// cross-tx dependency; recording it would make the validator
			// (floored below the tx's own writes) return None and wrongly
			// invalidate the tx.
			return
		}
		data := *account
		// Demote a sub-field MapRead promotion when AddressPath itself has no cell,
		// or the validator non-converges on its recursive AddressPath check.
		if source == MapRead {
			if _, res, ok := ibs.versionMap.ReadAddress(addr, ibs.txIndex); !ok || !res.resolved() {
				source = StorageRead
				version = UnknownVersion
			}
		}
		ibs.versionedReads.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: source, Version: version},
			Val:        NewAccountView(&data),
		})
	}
}

// recordWriteX helpers record a versioned write at the specified path
// directly into the typed per-path map.  No generic dispatcher / runtime
// type switch — each helper is monomorphic by path.

// recordWrite* — typed write recorders for each AccountPath.  Pool fast
// path: a repeat write to the same (addr[,key]) reuses the existing
// *VersionedWrite[T] in place (no alloc, no map churn).  Only the first
// write per (addr[,key]) per tx hits getVW* + SetX.  WriteSet.ReleaseAndReset
// takes every VW back.

func (ibs *IntraBlockState) recordWriteBalance(addr accounts.Address, val uint256.Int) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetBalance(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetBalance(addr, vw)
	traceWrite(ibs, vw)
}

// recordWriteBalanceDeleted records a balance write that deletes the account's
// leaf (ValueDeleted ⇒ nil on the projection, absent to the reader). Used by the
// self-destruct burn so the delete decision rides the write, not the balance value.
func (ibs *IntraBlockState) recordWriteBalanceDeleted(addr accounts.Address) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetBalance(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = uint256.Int{}
		vw.valStatus = ValueDeleted
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath, Version: ibs.Version(), valStatus: ValueDeleted}
	vw.Val = uint256.Int{}
	ibs.versionedWrites.SetBalance(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteNonce(addr accounts.Address, val uint64, reason tracing.NonceChangeReason) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetNonce(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		vw.NonceReason = reason
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWNonce()
	vw.WriteHeader = WriteHeader{Address: addr, Path: NoncePath, Version: ibs.Version(), NonceReason: reason}
	vw.Val = val
	ibs.versionedWrites.SetNonce(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteIncarnation(addr accounts.Address, val uint64) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetIncarnation(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWIncarnation()
	vw.WriteHeader = WriteHeader{Address: addr, Path: IncarnationPath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetIncarnation(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteSelfDestruct(addr accounts.Address, val bool) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetSelfDestruct(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWSelfDestruct()
	vw.WriteHeader = WriteHeader{Address: addr, Path: SelfDestructPath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetSelfDestruct(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCreateContract(addr accounts.Address, val bool) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetCreateContract(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWCreateContract()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CreateContractPath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetCreateContract(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCode(addr accounts.Address, val accounts.Code) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetCode(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWCode()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodePath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetCode(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCodeHash(addr accounts.Address, val accounts.CodeHash) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetCodeHash(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWCodeHash()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeHashPath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetCodeHash(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCodeSize(addr accounts.Address, val int) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetCodeSize(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWCodeSize()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeSizePath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetCodeSize(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteAddress(addr accounts.Address, account *accounts.Account) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	// A copy, made only here: the caller's account keeps changing.
	val := ibs.versionedWrites.copyAccount(account)
	if vw, ok := ibs.versionedWrites.GetAddress(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWAddress()
	vw.WriteHeader = WriteHeader{Address: addr, Path: AddressPath, Version: ibs.Version()}
	vw.Val = val
	ibs.versionedWrites.SetAddress(addr, vw)
	traceWrite(ibs, vw)
}

// storageValueStatus classifies a storage write relative to what the tx read (prev),
// which already reflects a same-tx destruct, so no re-derivation or lifecycle guard is
// needed. A zero slot is an absent leaf, so presence is prev != 0.
func storageValueStatus(prev, val uint256.Int) valueStatus {
	if prev.Eq(&val) {
		return ValueUnchanged
	}
	if prev.IsZero() {
		return ValueCreated
	}
	if val.IsZero() {
		return ValueDeleted
	}
	return ValueChanged
}

// recordWriteStorage records a storage write, stamping valStatus against the tx's
// committed origin for the key. prev is the value the tx read for this key; on the
// first write it is that committed origin and is captured, so a later re-write (an
// intra-tx oscillation) still classifies against the origin rather than the prior step.
func (ibs *IntraBlockState) recordWriteStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int, prev uint256.Int) {
	ibs.MarkAddressAccess(addr, true)
	if ibs.versionMap == nil {
		return
	}
	if vw, ok := ibs.versionedWrites.GetStorage(addr, key); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		vw.valStatus = storageValueStatus(vw.orig, val)
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.versionedWrites.newVWStorage()
	vw.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: ibs.Version(), valStatus: storageValueStatus(prev, val)}
	vw.orig = prev
	vw.Val = val
	ibs.versionedWrites.SetStorage(addr, key, vw)
	traceWrite(ibs, vw)
}

func traceWrite[T any](ibs *IntraBlockState, vw *VersionedWrite[T]) {
	if !dbg.TraceTransactionIO {
		return
	}
	hdr := vw.WriteHeader
	if !(ibs.trace || (dbg.TraceAccount(hdr.Address.Handle()) && (hdr.Key == accounts.NilKey || traceKey(hdr.Key)))) {
		return
	}
	fmt.Printf("%d (%d.%d) WRT %x %s: %v (%d.%d)\n", ibs.blockNum, ibs.txIndex, ibs.version,
		hdr.Address, AccountKey{Path: hdr.Path, Key: hdr.Key}, vw.Val, hdr.Version.TxIndex, hdr.Version.Incarnation)
}

// accountLifecycle returns the self-destruct verdict for the current tx, layering
// the tx's own field-level SelfDestruct write over the versionMap floor. An own-tx
// write is authoritative (true after a same-tx SD, false after a same-tx recreate);
// with no own write, the floor's destroyed-and-not-revived verdict applies.
func (ibs *IntraBlockState) accountLifecycle(addr accounts.Address) (destroyed bool) {
	if own, ok := ibs.versionedWriteSelfDestruct(addr); ok {
		return own
	}
	return ibs.versionMap.IsNetAbsent(addr, ibs.txIndex)
}

func (ibs *IntraBlockState) versionedWriteSelfDestruct(addr accounts.Address) (bool, bool) {
	if ibs.versionMap == nil {
		return false, false
	}
	vw, ok := ibs.versionedWrites.GetSelfDestruct(addr)
	if !ok {
		return false, false
	}
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false, false
	}
	return vw.Val, true
}

// versionedWriteCreateContract reports whether this tx's own writes created a
// contract at addr (the CreateContract cell). Guarded by journal.dirties so a
// stale entry from a reverted create is ignored.
func (ibs *IntraBlockState) versionedWriteCreateContract(addr accounts.Address) (bool, bool) {
	if ibs.versionMap == nil {
		return false, false
	}
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false, false
	}
	vw, ok := ibs.versionedWrites.GetCreateContract(addr)
	if !ok {
		return false, false
	}
	return vw.Val, true
}

// reconstructCellFlags stamps the transient (uncached) stateObject's
// create/self-destruct flags from this tx's own versioned-write cells. Under
// noMaterialize the stateObject is rebuilt on every getStateObject call, so the
// createdContract / newlyCreated / selfdestructed state that a materialized
// object would have carried must be recovered from the cells instead.
func (ibs *IntraBlockState) reconstructCellFlags(obj *stateObject, addr accounts.Address) {
	if obj == nil {
		return
	}
	if _, isDirty := ibs.journal.dirties[addr]; isDirty {
		if vw, ok := ibs.versionedWrites.GetCreateContract(addr); ok && vw.Val {
			obj.createdContract = true
		}
		// An own AddressPath write means this tx created the account: createObject is
		// the only writer, and reverting a creation drops or restores the write.
		if _, ok := ibs.versionedWrites.GetAddress(addr); ok {
			obj.newlyCreated = true
		}
		if vw, ok := ibs.versionedWrites.GetSelfDestruct(addr); ok && vw.Val {
			obj.selfdestructed = true
		}
		// The AddressPath record's CodeHash can lag the Code cells, so seed the code
		// from this tx's own Code write; otherwise it is refreshed from the
		// versionMap below. An own write wins even when it clears code, else a
		// prior-tx delegation would come back.
		if vw, ok := ibs.versionedWrites.GetCode(addr); ok {
			obj.code = vw.Val
			obj.data.CodeHash = vw.Val.Hash
			return
		}
	}
	// An account this tx created has no code until its own Code write.
	if obj.code.Bytes != nil || obj.newlyCreated {
		return
	}
	code, err := refreshCode(ibs, addr)
	if err != nil || code.Bytes == nil {
		return
	}
	obj.code = code
	obj.data.CodeHash = code.Hash
	obj.original.CodeHash = code.Hash
}

// versionedWriteHit probes the dirty per-tx write set for a write at
// (addr, path, key) and, when present, populates the corresponding
// per-typed pointer field on r.  Returns true when a write was found.
// The non-storage paths share a single non-nil typed field; the storage
// path uses r.vwStorage.
func (ibs *IntraBlockState) versionedWriteHit(addr accounts.Address, path AccountPath, key accounts.StorageKey, r *readPathResult) bool {
	if ibs.versionMap == nil {
		return false
	}
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false
	}
	switch path {
	case AddressPath:
		if vw, ok := ibs.versionedWrites.GetAddress(addr); ok {
			r.vwAddress = vw
			return true
		}
	case BalancePath:
		if vw, ok := ibs.versionedWrites.GetBalance(addr); ok {
			r.vwBalance = vw
			return true
		}
	case NoncePath:
		if vw, ok := ibs.versionedWrites.GetNonce(addr); ok {
			r.vwNonce = vw
			return true
		}
	case IncarnationPath:
		if vw, ok := ibs.versionedWrites.GetIncarnation(addr); ok {
			r.vwIncarnation = vw
			return true
		}
	case SelfDestructPath:
		if vw, ok := ibs.versionedWrites.GetSelfDestruct(addr); ok {
			r.vwSelfDestruct = vw
			return true
		}
	case CreateContractPath:
		if vw, ok := ibs.versionedWrites.GetCreateContract(addr); ok {
			r.vwCreateContract = vw
			return true
		}
	case CodePath:
		if vw, ok := ibs.versionedWrites.GetCode(addr); ok {
			r.vwCode = vw
			return true
		}
	case CodeHashPath:
		if vw, ok := ibs.versionedWrites.GetCodeHash(addr); ok {
			r.vwCodeHash = vw
			return true
		}
	case CodeSizePath:
		if vw, ok := ibs.versionedWrites.GetCodeSize(addr); ok {
			r.vwCodeSize = vw
			return true
		}
	case StoragePath:
		if vw, ok := ibs.versionedWrites.GetStorage(addr, key); ok {
			r.vwStorage = vw
			return true
		}
	}
	return false
}

func (ibs *IntraBlockState) HadInvalidRead() bool {
	return ibs.dep >= 0
}

func (ibs *IntraBlockState) DepTxIndex() int {
	return ibs.dep
}

func (ibs *IntraBlockState) StateReadError() error {
	return ibs.stateReadErr
}

func (ibs *IntraBlockState) recordStateReadError(err error) {
	if err != nil && ibs.stateReadErr == nil {
		ibs.stateReadErr = err
	}
}

func (ibs *IntraBlockState) SetVersion(inc Incarnation) {
	ibs.version = inc
}

func (ibs *IntraBlockState) Version() Version {
	return Version{
		BlockNum:    ibs.blockNum,
		TxIndex:     ibs.txIndex,
		Incarnation: ibs.version,
	}
}

// VersionedReads returns the in-flight per-path read set.  The returned
// value shares the underlying maps with the IBS; it is handed over at
// end of tx (RecordReads / TxIn), after which ResetVersionedIO rebinds
// the IBS field to a fresh set.
func (ibs *IntraBlockState) VersionedReads() ReadSet {
	return ibs.versionedReads
}

func (ibs *IntraBlockState) ResetVersionedIO() {
	ibs.versionedReads = ReadSet{}
	ibs.versionedWrites.recycle()
	ibs.dep = UnknownDep
	ibs.recordAccess = false
	ibs.stateReadErr = nil
}

// ResetVersionedReads clears tracked versioned reads without affecting writes.
func (ibs *IntraBlockState) ResetVersionedReads() {
	ibs.versionedReads = ReadSet{}
}

// VersionedWrites returns a frozen typed snapshot of this tx's recorded writes.
// The snapshot logic lives on the write-set itself (WriteSet.Snapshot); this is
// the IntraBlockState accessor for it.
func (ibs *IntraBlockState) VersionedWrites() *WriteSet {
	return ibs.versionedWrites.Snapshot()
}

// recordStorageOrigin / recordAddressOrigin collect a cold committed base cell into the
// tx-local origin set (at originIndex) instead of writing the shared versionMap mid-read.
func (ibs *IntraBlockState) recordStorageOrigin(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	vw := getVWStorage()
	vw.WriteHeader = WriteHeader{Address: addr, Key: key, Path: StoragePath, Version: Version{TxIndex: originIndex}}
	vw.Val = val
	ibs.versionedOrigins.SetStorage(addr, key, vw)
}

func (ibs *IntraBlockState) recordAddressOrigin(addr accounts.Address, acc *accounts.Account) {
	vw := getVWAddress()
	vw.WriteHeader = WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: originIndex}}
	vw.Val = acc
	ibs.versionedOrigins.SetAddress(addr, vw)
}

// PublishOrigins publishes this tx's collected committed-base origins into the versionMap's
// lock-free origin layer (write-once), so later reads resolve the pre-block base without taking
// an account's e.mu. The origin is immutable pre-block state, so it lives outside the OCC cells.
func (ibs *IntraBlockState) PublishOrigins() {
	if ibs.versionMap == nil || ibs.versionedOrigins.IsEmpty() {
		return
	}
	ws := &ibs.versionedOrigins
	ws.forEachAddr(func(addr accounts.Address) {
		if vw, ok := ws.address[addr]; ok {
			ibs.versionMap.StoreOriginAccount(addr, vw.Val)
		}
		if inner, ok := ws.storage[addr]; ok {
			for key, vw := range inner {
				ibs.versionMap.StoreOriginStorage(addr, key, vw.Val)
			}
		}
	})
	ws.ReleaseAndReset()
}

// Apply entries in a given write set to StateDB. Note that this function does not change MVHashMap nor write set
// of the current StateDB.
func (ibs *IntraBlockState) ApplyVersionedWrites(writes *WriteSet) error {
	if writes == nil {
		return nil
	}
	// Deterministic (Address, Path, Key) order: Code/SelfDestruct load the state
	// object and may record an extra read depending on whether a prior
	// same-address write already loaded it, which changes the EIP-7928 BAL hash.
	headers := make([]WriteHeader, 0, writes.Count())
	for h := range writes.AllHeaders() {
		headers = append(headers, h)
	}
	sortWriteHeaders(headers)
	for _, hdr := range headers {
		addr := hdr.Address

		switch hdr.Path {
		case AddressPath:
			continue
		case StoragePath:
			vw, ok := writes.GetStorage(addr, hdr.Key)
			if !ok {
				continue
			}
			if err := ibs.setState(addr, hdr.Key, vw.Val, true); err != nil {
				return err
			}
		case BalancePath:
			vw, ok := writes.GetBalance(addr)
			if !ok {
				continue
			}
			if err := ibs.SetBalance(addr, vw.Val, hdr.Reason); err != nil {
				return err
			}
		case NoncePath:
			vw, ok := writes.GetNonce(addr)
			if !ok {
				continue
			}
			if err := ibs.SetNonce(addr, vw.Val, hdr.NonceReason); err != nil {
				return err
			}
		case IncarnationPath:
			vw, ok := writes.GetIncarnation(addr)
			if !ok {
				continue
			}
			if err := ibs.SetIncarnation(addr, vw.Val); err != nil {
				return err
			}
			// Re-emit so the finalize IBS's writes flush to the global versionMap.
			ibs.recordWriteIncarnation(addr, vw.Val)
		case CodePath:
			vwCode, ok := writes.GetCode(addr)
			if !ok {
				continue
			}
			code := vwCode.Val
			stateObject, err := ibs.GetOrNewStateObject(addr)
			if err != nil {
				return err
			}
			// Force-set code bypassing stateObject.SetCode's equality check.
			// The finalize IBS uses a VersionedStateReader whose ReadSet may
			// contain the post-write code value (when the worker read the code
			// after a SetCodeTx modified it), causing SetCode's equality
			// comparison to incorrectly skip the update and leave dirtyCode unset.
			ibs.journal.codeChange(addr, stateObject.code.Bytes, stateObject.data.CodeHash, !ibs.hasWrite(addr, CodePath, accounts.NilKey))
			stateObject.setCode(code)
			ibs.recordWriteCode(addr, code)
			ibs.recordWriteCodeHash(addr, code.Hash)
			ibs.recordWriteCodeSize(addr, code.Len())
		case CodeHashPath, CodeSizePath:
			// set by CodePath case above
		case SelfDestructPath:
			vw, ok := writes.GetSelfDestruct(addr)
			if !ok {
				continue
			}
			if vw.Val {
				// Ensure the state object exists before calling Selfdestruct.
				// For newly-created accounts (with no pre-block DB entry)
				// getStateObject returns nil and Selfdestruct silently no-ops, so
				// materialize the object first to keep the selfdestructed marking.
				if _, err := ibs.GetOrNewStateObject(addr); err != nil {
					return err
				}
				if _, err := ibs.Selfdestruct(addr, true); err != nil {
					return err
				}
			} else {
				// SelfDestructPath=false means resurrection this block. createdContract
				// is not a versioned write path and is lost in the finalize IBS, so
				// restore it here to ensure CreateContract clears old storage on commit.
				so, err := ibs.GetOrNewStateObject(addr)
				if err != nil {
					return err
				}
				if so != nil {
					so.selfdestructed = false
					so.createdContract = true
				}
				// Re-emit SelfDestructPath=false so the global versionMap reflects the
				// resurrection; subsequent workers reading SelfDestructPath will see the
				// updated value and not mistake the account for still being selfdestructed.
				ibs.recordWriteSelfDestruct(addr, false)
			}
		case CreateContractPath:
			// A same-tx self-destruct dominates the creation marker: skip applying
			// CreateContract so the account is not resurrected as a live contract.
			if sw, ok := writes.GetSelfDestruct(addr); ok && sw.Val {
				continue
			}
			// Contract creation: set createdContract flag on the stateObject.
			so, err := ibs.GetOrNewStateObject(addr)
			if err != nil {
				return err
			}
			if so != nil {
				so.createdContract = true
			}
		default:
			return fmt.Errorf("unknown key type: %d", hdr.Path)
		}
	}
	return nil
}
