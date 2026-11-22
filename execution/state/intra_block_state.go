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
	journal      *journal
	trace        bool
	tracingHooks *tracing.Hooks
	recordAccess bool // gates MarkAddressAccess — enabled in Prepare

	// Versioned storage used for parallel tx processing, versions
	// are maintaned across transactions until they are reset
	// at the block level.
	stateUpdates *VersionMap
	writes       WriteSet
	reads        ReadSet
	// origins collects committed pre-block base cells (at originIndex) that a cold
	// read resolves this tx. Reads record here (tx-local, lock-free) instead of writing the
	// shared versionMap mid-execution; the executor publishes them batched at flush, so the
	// Block-STM invariant holds (versionMap mutated only at pre-exec seed and flush).
	origins WriteSet
	// committedBase memoizes the per-tx committed (pre-block) account fallback for
	// accountBase. The base reader can serve the published writes of lower txs,
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

	noConflictDetection bool
	codeByHash          CodeByHash

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
		stateReader:      stateReader,
		journal:          newJournal(),
		accessList:       accessList{addresses: make(map[accounts.Address]int)},
		transientStorage: newTransientStorage(),
		recordAccess:     false,
		txIndex:          0,
		trace:            false,
		dep:              UnknownDep,
	}
	ibs.codeAccess, _ = stateReader.(codeAccessTracker)
	ibs.revisions.init()
	// The IBS is always versioned: it reads through the version-map view and never
	// materializes a stateObject. Callers that need a shared block-level map rebind
	// it via SetVersionMap (NewWithVersionMap); the rest get a private empty map.
	ibs.SetVersionMap(NewVersionMap(nil))
	ibs.SetTxContext(0, 0)
	return ibs
}

func NewWithVersionMap(stateReader StateReader, mvhm *VersionMap) *IntraBlockState {
	ibs := New(stateReader)
	ibs.SetVersionMap(mvhm)
	return ibs
}

// NewVersionedBlock returns a state that reads through the version-map view, for
// re-executing a block's txs in order (a later tx may re-create an account an
// earlier one destroyed, so conflict detection stays on).
func NewVersionedBlock(stateReader StateReader) *IntraBlockState {
	ibs := NewWithVersionMap(stateReader, NewVersionMap(nil))
	ibs.SetTxContext(0, 0)
	return ibs
}

// NewVersionedCall is NewVersionedBlock for a single call, with cross-tx conflict
// detection off.
func NewVersionedCall(stateReader StateReader) *IntraBlockState {
	ibs := NewVersionedBlock(stateReader)
	ibs.SetNoConflictDetection()
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

// SetVersionMap binds the IBS to a non-nil version map. The IBS always reads and
// writes through a version map; a nil map is an invariant violation, not a state.
func (ibs *IntraBlockState) SetVersionMap(versionMap *VersionMap) {
	if versionMap == nil {
		panic("IntraBlockState requires a non-nil version map")
	}
	ibs.stateUpdates = versionMap
}

// StateReader returns the committed-base reader. The parallel executor uses it to
// retune a per-task overlay (PrevBlockReader.SetBlock) without rebuilding the IBS.
func (ibs *IntraBlockState) StateReader() StateReader {
	return ibs.stateReader
}

func (ibs *IntraBlockState) VersionMap() *VersionMap {
	return ibs.stateUpdates
}

func (ibs *IntraBlockState) SetHooks(hooks *tracing.Hooks) {
	ibs.tracingHooks = hooks
}

func (ibs *IntraBlockState) SetTrace(trace bool) {
	ibs.trace = trace
}

func (ibs *IntraBlockState) hasWrite(addr accounts.Address, path AccountPath, key accounts.StorageKey) bool {
	return ibs.writes.Has(WriteHeader{Address: addr, Path: path, Key: key})
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (ibs *IntraBlockState) Reset() {
	ibs.lastCode = accounts.Code{}
	ibs.logs.reset()
	ibs.clearJournalAndRefund()
	ibs.txIndex = 0
	ibs.sdProbeEpoch++
	ibs.accessList.Reset()
	clear(ibs.transientStorage)
	ibs.SetVersionMap(NewVersionMap(nil))
	ibs.noConflictDetection = false
	ibs.codeByHash = nil
	clear(ibs.committedBase)
	// Read side rebinds to a fresh empty set: VersionedReads() at end of
	// tx hands the per-path maps to result.TxIn, so rebinding leaves the
	// handed-over maps intact while the next tx lazily reallocs.
	ibs.reads = ReadSet{}
	// Write side: VersionedWrites() returns Cloned snapshots, so the
	// originals in ibs.writes are no longer referenced after the
	// boundary call, so recycle can take every VW back.
	ibs.writes.recycle()
	ibs.origins.ReleaseAndReset()
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

// NewPooled is NewVersionedCall over an IntraBlockState reused from an earlier
// call, whose maps keep their capacity. Release it with ReleasePooled.
func NewPooled(stateReader StateReader) *IntraBlockState {
	ibs, ok := ibsPool.Get().(*IntraBlockState)
	if !ok {
		return NewVersionedCall(stateReader)
	}
	ibs.stateReader = stateReader
	ibs.codeAccess, _ = stateReader.(codeAccessTracker)
	ibs.SetVersionMap(NewVersionMap(nil))
	ibs.SetTxContext(0, 0)
	ibs.SetNoConflictDetection()
	return ibs
}

// ReleasePooled hands ibs to the next NewPooled, or closes it when it grew too
// large to keep.
func ReleasePooled(ibs *IntraBlockState) {
	if ibs.journal == nil { // closed
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
	reads := ibs.reads
	poolable := ibs.accessList.entries()+reads.entries() <= maxPooledEntries
	ibs.Reset()
	if !poolable {
		return false
	}
	// One call never hands its read set out, so the maps keep their capacity
	// instead of the empty set Reset installs.
	reads.clearForReuse()
	ibs.reads = reads
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

func (ibs *IntraBlockState) SetCodeByHash(c CodeByHash) { ibs.codeByHash = c }

// Release Deprecated use Close
func (ibs *IntraBlockState) Release(bool) { ibs.Close() }

// Close returns pooled resources (like journal, versioned writes)
// back to their pools. Call this when the IntraBlockState is no longer needed.
// Call Reset() to re-use IntraBlockState object
// Idempotent, thread-unsafe
func (ibs *IntraBlockState) Close() {
	if ibs == nil || ibs.journal == nil {
		return
	}

	journal := ibs.journal
	ibs.journal = nil
	ibs.logs.release()
	ibs.revisions.reset()
	// Safe to pool: VersionedWrites/FinalizedWrites hand out deep clones, and the
	// set is unexported, so nothing outside holds a raw VersionedWrite.
	ibs.writes.ReleaseAndReset()
	ibs.origins.ReleaseAndReset()

	releaseResources(journal)
}

func releaseResources(journal *journal) {
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

func (ibs *IntraBlockState) LogCount() int { return len(ibs.logs.entries) }

// LogsSince deep-copies entries added after a saved LogCount.
func (ibs *IntraBlockState) LogsSince(first int) types.Logs {
	logs := ibs.logs.entries[first:]
	if len(logs) == 0 {
		return nil
	}
	return logs.Copy()
}

func (ibs *IntraBlockState) Logs() types.Logs { return ibs.LogsSince(0) }

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
	// Existence needs only the base record + self-destruct gate, not the
	// per-field overlay. A same-tx self-destruct is still alive (EIP-6780); a
	// cross-tx one makes accountBase return nil.
	readAccount, _, _, err := ibs.accountBase(addr, true)
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
	if warmEmpty, ok := ibs.emptyFromWarmReads(addr); ok {
		return warmEmpty, nil
	}
	// Existence + the self-destruct/revival gate, without reconstructing the
	// whole account: the EIP-161 verdict needs only the current balance, nonce
	// and code hash, read per-field below (short-circuiting), so the per-field
	// overlay and a full-account allocation are avoided.
	account, source, _, err := ibs.accountBase(addr, true)
	if err != nil {
		return false, err
	}
	if account == nil {
		if source == ReadSetRead {
			return true, nil
		}
		// Do NOT call accountRead here: accountBase already recorded the
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
			ibs.reads.SetBalance(addr, VersionedRead[uint256.Int]{
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
	if sd, ok := ibs.ownSelfDestruct(addr); ok && sd {
		return false, nil
	}

	return ibs.emptyFromFields(addr, account)
}

// emptyFromFields computes the EIP-161 emptiness verdict for an
// account that accountBase resolved as existing, reading the current
// balance/nonce/codeHash per-field (short-circuiting on the first non-empty
// field) instead of reconstructing the whole account. The per-field refresh
// reads apply the same self-destruct gate as the whole-account path.
// emptyFromWarmReads answers Empty from reads this attempt already recorded, so that a
// repeat Empty looks each one up once. ok is false when the full path must decide: addr
// has an own write, a read is missing, or the account read found no account (a field cell
// can exist with no account record, so a recorded balance does not prove existence).
func (ibs *IntraBlockState) emptyFromWarmReads(addr accounts.Address) (empty, ok bool) {
	if !ibs.warmReadable(addr) {
		return false, false
	}
	account, ok := ibs.reads.GetAddress(addr)
	if !ok || !warmSource(account.Source) || account.Val == nil || account.Val.IsNil() {
		return false, false
	}
	balance, ok := ibs.reads.GetBalance(addr)
	if !ok || !warmSource(balance.Source) {
		return false, false
	}
	if !balance.Val.IsZero() {
		return false, true
	}
	nonce, ok := ibs.reads.GetNonce(addr)
	if !ok || !warmSource(nonce.Source) {
		return false, false
	}
	if nonce.Val != 0 {
		return false, true
	}
	codeHash, ok := ibs.reads.GetCodeHash(addr)
	if !ok || !warmSource(codeHash.Source) {
		return false, false
	}
	return codeHash.Val == accounts.EmptyCodeHash, true
}

func (ibs *IntraBlockState) emptyFromFields(addr accounts.Address, account *accounts.Account) (bool, error) {
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
	balance, source, _, err := readBalance(ibs, addr)

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) GetBalance %x: %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, balance.String())
	}
	return balance, source == StorageRead || source == MapRead, err
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) GetNonce(addr accounts.Address) (uint64, error) {
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

// ReadCodeSegmentTo copies the code of addr from offset from into dst, zero-padded past its
// end. On a code-cache miss a reader that serves segments copies without caching the code; any
// other reader reads the whole code, which fills the cache. Without a version map, or with a
// code-access hook, it always reads the whole code.
func (ibs *IntraBlockState) ReadCodeSegmentTo(addr accounts.Address, from uint64, dst []byte) error {
	if ibs.codeAccess != nil {
		code, err := ibs.GetCode(addr)
		common.CopySegment(dst, code, from)
		return err
	}
	seg := codeSegment{from: from, dst: dst}
	code, source, _, err := readCodeSegment(ibs, addr, &seg)
	if !seg.copied {
		common.CopySegment(dst, code.Bytes, from)
	}
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) ReadCodeSegmentTo (%s) %x: from %d, %d bytes, err: %v\n", ibs.blockNum, ibs.txIndex, ibs.version, source, addr, from, len(dst), err)
	}
	return err
}

func (ibs *IntraBlockState) getCode(addr accounts.Address) (accounts.Code, error) {
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
	if ibs.codeAccess != nil {
		code, err := ibs.getCode(addr)
		return code.Len(), err
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
	OnCodeChange(accounts.CodeHash) func()
}

func (ibs *IntraBlockState) callCodeAccessHook(addr accounts.Address, code []byte) {
	if ibs.codeAccess != nil {
		ibs.codeAccess.OnCodeAccess(addr, code)
	}
}

func (ibs *IntraBlockState) GetCodeHash(addr accounts.Address) (accounts.CodeHash, error) {
	hash, _, _, err := readCodeHash(ibs, addr, false)
	if err != nil {
		return accounts.NilCodeHash, err
	}
	// EIP-6780: a contract self-destructed in THIS tx stays alive until end-of-tx
	// cleanup, so EXTCODEHASH within the same tx must return its real code hash.
	// The SELFDESTRUCT cleared the CodeHashPath cell but the code is still present,
	// so recompute the hash from it.
	if hash == accounts.EmptyCodeHash && ibs.hasWrite(addr, SelfDestructPath, accounts.NilKey) {
		if cw, ok := ibs.writes.GetCode(addr); ok && len(cw.Val.Bytes) > 0 {
			return accounts.InternCodeHash(crypto.Keccak256Hash(cw.Val.Bytes)), nil
		}
	}
	if hash == accounts.NilCodeHash {
		// A prior tx's balance-preserving SELFDESTRUCT leaves an empty-code account,
		// but its CodeHashPath is dropped from the version map, so recover the
		// codehash from the reconstructed account (EmptyCodeHash), distinguishing it
		// from a genuinely absent account (NilCodeHash). Branch-agnostic: a burned
		// account reconstructs as nil here and falls through to NilCodeHash.
		acc, _, _, err := ibs.getAccount(addr, false)
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
// read through readCore so a predecessor's in-flight code write is seen
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

// ResolveCode returns the code a call to addr executes. With followDelegation set, it follows an
// EIP-7702 delegation; otherwise a delegation designator is ordinary code. The code hash comes
// from the same read as the code.
func (ibs *IntraBlockState) ResolveCode(addr accounts.Address, followDelegation bool) (accounts.Code, error) {
	// committed=false so the tx's own writes (e.g. EIP-7702 authorization) are
	// visible rather than stale delegation code from the version map.
	code, err := ibs.getCode(addr)
	if followDelegation {
		if delegation, ok := types.ParseDelegation(code.Bytes); ok {
			return ibs.getCode(delegation)
		}
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
	code, _, _, err := readCode(ibs, addr)
	if err != nil {
		return accounts.ZeroAddress, false, err
	}
	ibs.callCodeAccessHook(addr, code.Bytes)
	if delegation, ok := types.ParseDelegation(code.Bytes); ok {
		return delegation, true, nil
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
	destructed, source, _, err := readSelfDestruct(ibs, addr)
	// A destruct read from the version map is an earlier transaction's, not this one's.
	return destructed && source != MapRead, err
}

func (ibs *IntraBlockState) ReadVersion(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) ReadResult {
	return ibs.stateUpdates.ReadStatus(addr, path, key, txIdx)
}

// writeBalance records a balance change on the versionMap write-set and the
// journal. An absent account (base == nil) is created via create, which records
// the AddressPath write OCC needs; the create path never reads balance, so its
// journal prev is the fresh account's zero. A live account — including one with
// an in-block lifecycle event such as a SELFDESTRUCT — keeps its record and
// journals its own balance as prev. The journal prev comes from a caller that
// already read the balance, or, when nil, is read only in the live
// branch so a create does not widen the OCC read-set with a spurious BalancePath
// read.
func (ibs *IntraBlockState) writeBalance(addr accounts.Address, prev *uint256.Int, update uint256.Int, wasCommited bool, reason tracing.BalanceChangeReason) error {
	base, _, _, err := ibs.accountBase(addr, true)
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
		// The lifecycle case (base != nil) journals *prev, the account's own balance,
		// so a revert restores it; the create case never read balance, so its prev is
		// the fresh account's zero.
		var prevBal uint256.Int
		if base != nil {
			prevBal = *prev
		}
		// Only an absent account is created here; a live account with an in-block
		// lifecycle event (accountLifecycle) keeps its record and code.
		if base == nil {
			ibs.create(addr, false)
		}
		ibs.journal.balanceChange(addr, prevBal, wasCommited, ibs.writes.balanceDeleted(addr))
		if ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil {
			ibs.tracingHooks.OnBalanceChange(addr, prevBal, update, reason)
		}
		ibs.recordWriteBalance(addr, update)
		return nil
	}
	ibs.journal.balanceChange(addr, *prev, wasCommited, ibs.writes.balanceDeleted(addr))
	if ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil {
		ibs.tracingHooks.OnBalanceChange(addr, *prev, update, reason)
	}
	ibs.recordWriteBalance(addr, update)
	return nil
}

// AddBalance adds amount to the account associated with addr.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) AddBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
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

	return ibs.writeBalance(addr, &prev, update, wasCommited, reason)
}

// TouchAccount materializes an empty account and records the zero-balance touch
// needed for state clearing and trie consistency.
func (ibs *IntraBlockState) TouchAccount(addr accounts.Address) error {
	// An own balance write settles the touch: zero already is the touch, non-zero is a non-empty account.
	if addr != ripemd {
		if _, ok := ibs.writes.GetBalance(addr); ok {
			return nil
		}
	}
	markTouched := func() {
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) Touch %x\n", ibs.blockNum, ibs.txIndex, ibs.version, addr)
		}
		// Versioned path: pair the BalancePath=0 write with a journal entry
		// that reverts it, so the write-set stays in step through reverts
		// without any dirties re-processing (deprecated on this path).
		prevWrite, had := ibs.writes.GetBalance(addr)
		var prev uint256.Int
		if had {
			prev = prevWrite.Val
		}
		ibs.recordWriteBalance(addr, uint256.Int{})
		ibs.journal.touchAccount(addr, !had, prev)
	}

	// The touch only depends on emptiness. For an existing account compute it
	// from field reads without reconstructing the stateObject.
	account, _, _, err := ibs.accountBase(addr, true)
	if err != nil {
		return err
	}
	if account != nil {
		empty, err := ibs.emptyFromFields(addr, account)
		if err != nil {
			return err
		}
		if empty {
			markTouched()
		}
		return nil
	}
	// Absent or destroyed-no-revival: (re)create as empty and touch, recording
	// the AddressPath write for OCC create detection. A fresh account is empty.
	ibs.create(addr, false)
	markTouched()
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
	destructed, res, ok := ibs.stateUpdates.ReadSelfDestruct(addr, ibs.txIndex)
	// An address with no cells costs no lock to probe; memoizing it only grows the map.
	if !ok && ibs.stateUpdates.load(addr) == nil {
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

func (ibs *IntraBlockState) getAccount(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
	return ibs.accountBase(addr, readStorage)
}

// readAbsentFromMap reports whether this tx already read addr as absent from the
// version map (a destruct with no revival). That verdict is final for the tx: the
// committed record must not revive it, and validation catches a later re-creation.
func (ibs *IntraBlockState) readAbsentFromMap(addr accounts.Address) bool {
	tr, ok := ibs.reads.GetAddress(addr)
	return ok && tr.Source == MapRead && (tr.Val == nil || tr.Val.Account() == nil)
}

// readAbsentFromCommitted reports whether this tx already read addr as absent from the committed state.
func (ibs *IntraBlockState) readAbsentFromCommitted(addr accounts.Address) bool {
	tr, ok := ibs.reads.GetAddress(addr)
	return ok && tr.Source == StorageRead && (tr.Val == nil || tr.Val.Account() == nil)
}

// readAbsent is like readAbsentFromMap, but also counts an absence read from the committed state:
// the base reader can serve a lower tx's creation later in the attempt.
func (ibs *IntraBlockState) readAbsent(addr accounts.Address) bool {
	tr, ok := ibs.reads.GetAddress(addr)
	return ok && (tr.Val == nil || tr.Val.Account() == nil)
}

// absenceIsFinal is readAbsentFromMap, except for an absence recorded at a destruct that
// kept a balance: this tx read the kept account there, and validation rejects it if a
// re-creation stays.
func (ibs *IntraBlockState) absenceIsFinal(addr accounts.Address) bool {
	if !ibs.readAbsentFromMap(addr) {
		return false
	}
	tr, _ := ibs.reads.GetAddress(addr)
	return !ibs.stateUpdates.destructKeptBalance(addr, tr.Version.TxIndex)
}

// accountBase resolves account existence via the AddressPath read (and
// storage fallback), applying the self-destruct/revival gate, but does NOT
// overlay the per-field versionMap cells. It returns nil when the account is
// absent or was destroyed with no revival. The AddressPath read it performs
// records the nil-read that OCC uses to detect create/absent conflicts.
func (ibs *IntraBlockState) accountBase(addr accounts.Address, readStorage bool) (*accounts.Account, ReadSource, Version, error) {
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
	// A repeat read keeps a recorded committed absence: validation rejects it if a
	// destruct or re-creation was flushed since.
	repeatedCommittedAbsence := readAccount == nil && source == StorageRead && ibs.readAbsentFromCommitted(addr)
	if readAccount == nil && !repeatedCommittedAbsence {
		if ibs.stateUpdates.absentAccountHook != nil {
			ibs.stateUpdates.absentAccountHook()
		}
		// Prefer the destruct that the read above saw, so that an in-flight re-creation
		// does not hide it; without one, probe the version map, as a destruct may have
		// been flushed since.
		memo, _ := ibs.sdProbeMemo(addr)
		destructed, sdRes, ok := memo.destructed, memo.res, memo.ok
		if !ok || !sdRes.resolved() || !destructed {
			destructed, sdRes, ok = ibs.stateUpdates.ReadSelfDestruct(addr, ibs.txIndex)
		}
		if ok && sdRes.resolved() && destructed {
			destructTxIndex := sdRes.DepIdx()
			// Only a genuine re-creation (a later CreateAccount writing AddressPath)
			// skips reconstruction. Later Balance/Nonce/CodeHash writes are updates
			// to the still-preserved account, not a revival.
			revived := false
			if hi, ok := ibs.stateUpdates.LatestTxIndex(addr, AddressPath, accounts.NilKey, ibs.txIndex-1); ok && hi > destructTxIndex {
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
				// A destruct this attempt has read stays in force even if its cell becomes an
				// estimate again, so the committed record must not revive the account, here or
				// in a later load of it. Only a re-creation by the destroying tx itself is left to
				// the committed reader.
				if _, res, _ := ibs.stateUpdates.ReadAddress(addr, destructTxIndex+1); res.DepIdx() != destructTxIndex || res.accSource() == AccountFromBAL {
					destructVersion := sdRes.Version()
					if _, ok := ibs.reads.GetAddress(addr); !ok {
						ibs.reads.SetAddress(addr, VersionedRead[AccountView]{ReadHeader: ReadHeader{Source: MapRead, Version: destructVersion}})
					}
					return nil, MapRead, destructVersion, nil
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

		if readAccount == nil && err == nil && repeatedCommittedAbsence {
			return nil, ReadSetRead, UnknownVersion, nil
		}
		if readAccount == nil || err != nil {
			return nil, StorageRead, UnknownVersion, err
		}

		// The base reader bypasses the versionMap, so a prior in-block SD'd
		// address still returns its pre-SD record. Without this gate the
		// stale nonce/codeHash flows through the per-field refresh (which
		// only overwrites fields a versionMap cell exists for), so Empty()
		// returns false and the EVM misses CallNewAccountGas.
		state, destructDep := ibs.stateUpdates.AccountLifecycleWithDestructDep(addr, ibs.txIndex)
		if state == LifecycleAbsent {
			return nil, StorageRead, UnknownVersion, nil
		}
		// An in-flight destruct below is not net-absent yet, but the committed record predates it.
		if destructDep != UnknownDep {
			ibs.dep = max(ibs.dep, destructDep)
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
		ibs.RecordStateReadError(err)
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

	return ibs.writeBalance(addr, &prev, update, wasCommited, reason)
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetBalance(addr accounts.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		amount := amount
		fmt.Printf("%d (%d.%d) SetBalance %x, %s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, amount.String())
	}
	return ibs.writeBalance(addr, nil, amount, !ibs.hasWrite(addr, BalancePath, accounts.NilKey), reason)
}

// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func (ibs *IntraBlockState) SetNonce(addr accounts.Address, nonce uint64, reason tracing.NonceChangeReason) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetNonce %x, %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, nonce)
	}

	wasCommited := !ibs.hasWrite(addr, NoncePath, accounts.NilKey)
	return ibs.writeNonce(addr, nonce, wasCommited, reason)
}

// writeNonce records a nonce write on the parallel (versionMap) path without
// materializing a stateObject. A nonce SET does not depend on the prior value,
// so prev is read WITHOUT recording an OCC read. An absent account (base == nil)
// is created via create with no stateObject; a live account with an in-block
// lifecycle event keeps its record and code.
func (ibs *IntraBlockState) writeNonce(addr accounts.Address, nonce uint64, wasCommited bool, reason tracing.NonceChangeReason) error {
	base, _, _, err := ibs.accountBase(addr, true)
	if err != nil {
		return err
	}
	if base == nil || ibs.accountLifecycle(addr) {
		// The journal prev is this tx's own nonce cell when present (it can lead the
		// base record), else the live account's nonce, else the fresh account's zero.
		prev := uint64(0)
		if base != nil {
			prev = base.Nonce
		}
		if vw, ok := ibs.writes.GetNonce(addr); ok {
			prev = vw.Val
		}
		// Only an absent/destroyed account is created here; a live account with an
		// in-block lifecycle event (accountLifecycle) keeps its record and code.
		if base == nil {
			ibs.create(addr, false)
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
	prev := base.Nonce
	// The tx's own nonce cell is authoritative for the journal prev.
	if vw, ok := ibs.writes.GetNonce(addr); ok {
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
	if !ibs.warmReadable(addr) && ibs.hasWrite(addr, AddressPath, accounts.NilKey) && !ibs.accountLifecycle(addr) {
		return ibs.setCreatedCode(addr, canonical, reason)
	}
	return ibs.writeCode(addr, canonical, reason)
}

// writeCodeCells records code as this tx's Code, CodeHash and CodeSize writes, or
// drops those writes when the code is unchanged.
func (ibs *IntraBlockState) writeCodeCells(addr accounts.Address, code accounts.Code, unchanged bool) {
	if unchanged {
		ibs.writes.DelCode(addr)
		ibs.writes.DelCodeHash(addr)
		ibs.writes.DelCodeSize(addr)
		return
	}
	ibs.recordWriteCode(addr, code)
	ibs.recordWriteCodeHash(addr, code.Hash)
	ibs.recordWriteCodeSize(addr, code.Len())
}

// journalCodeChange journals a code change and calls the tracing hooks.
func (ibs *IntraBlockState) journalCodeChange(addr accounts.Address, prevHash accounts.CodeHash, prevCode []byte, code accounts.Code, wasCommited bool, reason tracing.CodeChangeReason) {
	entry := ibs.journal.codeChange(addr, prevCode, prevHash, wasCommited)
	if ibs.codeAccess != nil {
		if undo := ibs.codeAccess.OnCodeChange(code.Hash); undo != nil {
			if entry.extra == nil {
				entry.extra = &journalExtra{prevhash: accounts.EmptyCodeHash}
			}
			entry.extra.undoCodeChange = undo
		}
	}
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
	cw, hadCode := ibs.writes.GetCode(addr)
	hw, hadHash := ibs.writes.GetCodeHash(addr)
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
	ibs.writeCodeCells(addr, code, code.Hash == prev.Hash)
	return nil
}

// writeCode is SetCode on the parallel (versionMap) path for an account
// not caught by setCreatedCode (an existing, warm or lifecycle account). It
// mirrors stateObject.SetCode's set decision and the net-zero drop without a
// transient stateObject. An absent account is created first (as GetOrNewStateObject
// would), which makes the pre-write code empty and disables the net-zero check.
// getOrCreate ensures addr has a record on the versioned path — creating one when
// the version map has none — and reports whether this tx created it (its own
// AddressPath write, from the create here or an earlier op in this tx). It is the
// versioned form of the old GetOrNewStateObject.
func (ibs *IntraBlockState) getOrCreate(addr accounts.Address) (created bool, err error) {
	base, _, _, err := ibs.accountBase(addr, false)
	if err != nil {
		return false, err
	}
	if base == nil {
		ibs.create(addr, false)
	}
	_, created = ibs.writes.GetAddress(addr)
	return created, nil
}

func (ibs *IntraBlockState) writeCode(addr accounts.Address, code accounts.Code, reason tracing.CodeChangeReason) error {
	// createdThisTx is the own AddressPath write (from the create here or an earlier
	// op in this tx, e.g. create→SELFDESTRUCT), not base == nil: the create's own
	// AddressPath write already makes base non-nil.
	createdThisTx, err := ibs.getOrCreate(addr)
	if err != nil {
		return err
	}
	// baseCodeHash ("what this SetCode saw") is the current cell, including this
	// tx's own earlier code writes. origHash (the cumulative net-zero baseline) is
	// the versionMap floor at txIndex — the tx-start value.
	baseCodeHash, err := ibs.GetCodeHash(addr)
	if err != nil {
		return err
	}
	origHash, err := ibs.GetCommittedCodeHash(addr)
	if err != nil {
		return err
	}
	seed, err := ibs.codeSeed(addr, baseCodeHash)
	if err != nil {
		return err
	}
	// bytes.Equal guards the codeHash-without-code case: a matching hash against
	// empty prev bytes must still heal the CodeDomain, not skip.
	if seed.Hash == code.Hash && bytes.Equal(seed.Bytes, code.Bytes) {
		return nil
	}
	// Pass seed.Hash (the actual previous code's hash), not baseCodeHash (the
	// CodeHashPath cell, which can lag this tx's own Code write), so a revert restores
	// exactly what the materialized path restored.
	ibs.journalCodeChange(addr, seed.Hash, seed.Bytes, code, !ibs.hasWrite(addr, CodePath, accounts.NilKey), reason)
	// Skip when the new code matches either the value seen by this SetCode call
	// (revert to in-tx base) or the pre-tx original (cumulative net-zero, e.g. an
	// EIP-7702 delegate-then-reset in the same tx). The net-zero case is disabled
	// for an account this tx created — its origHash is a pre-creation snapshot, not a
	// value the tx reverts to.
	matchesOriginal := !createdThisTx && code.Hash == origHash
	unchanged := code.Hash == baseCodeHash || matchesOriginal
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		if unchanged {
			fmt.Printf("%d (%d.%d) SetCode SKIP (matches base) %x codeHash=%x baseHash=%x originalHash=%x codeLen=%d\n",
				ibs.blockNum, ibs.txIndex, ibs.version, addr, code.Hash, baseCodeHash, origHash, len(code.Bytes))
		} else {
			fmt.Printf("%d (%d.%d) SetCode WRITE %x codeHash=%x baseHash=%x codeLen=%d\n",
				ibs.blockNum, ibs.txIndex, ibs.version, addr, code.Hash, baseCodeHash, len(code.Bytes))
		}
	}
	ibs.writeCodeCells(addr, code, unchanged)
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
	return ibs.setState(addr, key, value)
}

// setState records a storage write directly into the version map. It mirrors the
// original stateObject.SetState set decision and journalling.
func (ibs *IntraBlockState) setState(addr accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetState %x, %x=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, key, value.Hex())
	}
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
	ibs.recordWriteStorage(addr, key, value, prev)
	return nil
}

type Storage map[accounts.StorageKey]uint256.Int

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (ibs *IntraBlockState) SetStorage(addr accounts.Address, storage Storage) error {
	if _, err := ibs.getOrCreate(addr); err != nil {
		return err
	}
	incarnation, err := ibs.GetIncarnation(addr)
	if err != nil {
		return err
	}
	// An incarnation cell wipes storage and code for later transactions: a slot
	// or code without a newer cell reads as empty, so the code is written again.
	code, err := ibs.getCode(addr)
	if err != nil {
		return err
	}
	if code.Len() > 0 {
		ibs.writeCodeCells(addr, code, false)
	}
	ibs.recordWriteIncarnation(addr, incarnation)
	// The incarnation cell does not hide a slot cell of an earlier transaction.
	for _, key := range ibs.stateUpdates.StorageKeys(addr) {
		prev, _, _ := ibs.stateUpdates.ReadStorage(addr, key, ibs.txIndex)
		ibs.recordWriteStorage(addr, key, uint256.Int{}, prev)
	}
	for key, value := range storage {
		prev, _, _ := ibs.stateUpdates.ReadStorage(addr, key, ibs.txIndex)
		ibs.recordWriteStorage(addr, key, value, prev)
	}
	return nil
}

// SetIncarnation sets incarnation for account if account exists
func (ibs *IntraBlockState) SetIncarnation(addr accounts.Address, incarnation uint64) error {
	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		fmt.Printf("%d (%d.%d) SetIncarnation %x, %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, incarnation)
	}

	if _, err := ibs.getOrCreate(addr); err != nil {
		return err
	}
	ibs.recordWriteIncarnation(addr, incarnation)
	return nil
}

func (ibs *IntraBlockState) GetIncarnation(addr accounts.Address) (uint64, error) {
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
	return ibs.selfdestruct(addr, preserveBalance)
}

// selfdestruct records a self-destruct on the parallel (versionMap)
// path without materializing a stateObject. An already-materialized stateObject
// is kept in step for the so.data-based commit paths (genesis FinalizeTx, RPC).
func (ibs *IntraBlockState) selfdestruct(addr accounts.Address, preserveBalance bool) (bool, error) {
	sdW, hadSD := ibs.writes.GetSelfDestruct(addr)
	balW, hadBalance := ibs.writes.GetBalance(addr)
	incW, hadIncarnation := ibs.writes.GetIncarnation(addr)
	// Destructed earlier in this tx and sent nothing since: every cell below already holds its value.
	if !preserveBalance && hadSD && sdW.Val && hadBalance && balW.Val.IsZero() && hadIncarnation {
		return true, nil
	}
	base, _, _, err := ibs.accountBase(addr, true)
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
	ibs.journal.selfdestructChange(addr, prev, prevBalance,
		!ibs.hasWrite(addr, SelfDestructPath, accounts.NilKey),
		hadIncarnation, prevIncarnation, hadBalance, prevBalanceVersioned, hadBalance && balW.Deletes())

	if !preserveBalance && ibs.tracingHooks != nil && ibs.tracingHooks.OnBalanceChange != nil && !prevBalance.IsZero() {
		ibs.tracingHooks.OnBalanceChange(addr, prevBalance, zeroBalance, tracing.BalanceDecreaseSelfdestruct)
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
	// self-destructed account. Explicit cells here would also make a same-tx
	// re-creation abort on a phantom collision.
	return true, nil
}

var zeroBalance uint256.Int

// ContractCreatedInTx reports whether the current transaction created a contract at addr.
// The marker survives SELFDESTRUCT and is cleared if the creation is rolled back.
func (ibs *IntraBlockState) ContractCreatedInTx(addr accounts.Address) bool {
	created, _ := ibs.ownCreateContract(addr)
	return created
}

// Used for EIP-6780
func (ibs *IntraBlockState) IsNewContract(addr accounts.Address) (bool, error) {
	if !ibs.createdThisTx(addr) {
		return false, nil
	}
	code, err := ibs.GetCode(addr)
	if err != nil {
		return false, err
	}
	_, delegated := types.ParseDelegation(code)
	return !delegated, nil
}

// createdThisTx reports whether this transaction created the account: createObject
// is the only writer of an AddressPath cell, and reverting a creation drops or
// restores that write, so an own address write under a journal-dirty address is
// exactly the newly-created signal.
func (ibs *IntraBlockState) createdThisTx(addr accounts.Address) bool {
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false
	}
	_, ok := ibs.writes.GetAddress(addr)
	return ok
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

// create records the creation of addr into the version map and journal on the
// versioned path with no stateObject (createObject's versioned recording, extracted).
// hadPrevious is createObject's previous!=nil: the account resolved to a live record
// (getStateObject returns nil for a cross-tx-destructed account on this path), so the
// create is a reset whose fields the journal snapshots for revert rather than a fresh
// createObject entry.
func (ibs *IntraBlockState) create(addr accounts.Address, hadPrevious bool) {
	account := accounts.NewAccount() // Root=empty.RootHash, CodeHash=EmptyCodeHash: newObject's normalized output
	if !hadPrevious {
		ibs.journal.createObjectChange(addr)
	} else {
		prevWrites := ibs.writes.snapshotCreateFields(addr)
		ibs.journal.resetObjectChange(addr, prevWrites)
	}
	ibs.recordWriteAddress(addr, &account)
	ibs.recordWriteCodeHash(addr, account.CodeHash)
	if hadPrevious {
		ibs.recordWriteNonce(addr, account.Nonce, tracing.NonceChangeNewContract)
	} else {
		if _, res, ok := ibs.stateUpdates.ReadNonce(addr, ibs.txIndex); ok && res.resolved() {
			ibs.recordWriteNonce(addr, account.Nonce, tracing.NonceChangeNewContract)
		}
		ibs.recordWriteBalance(addr, account.Balance)
	}
}

// createAccount is CreateAccount on the versioned path: it resolves the
// prior account's lifecycle (existence, self-destruct, carried balance, prior
// incarnation) from the version map and base reader with no stateObject.
// traceCreateAccount is the deferred CreateAccount logger; the caller passes its
// error and resolved-balance closure (evaluated at defer time).
func (ibs *IntraBlockState) traceCreateAccount(addr accounts.Address, contractCreation bool, errp *error, balance func() string) func() {
	return func() {
		var creatingContract string
		if contractCreation {
			creatingContract = " (contract)"
		}
		if *errp != nil {
			fmt.Printf("%d (%d.%d) Create Account%s: %x, err=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, creatingContract, addr, *errp)
			return
		}
		fmt.Printf("%d (%d.%d) Create Account%s: %x, balance=%s\n", ibs.blockNum, ibs.txIndex, ibs.version, creatingContract, addr, balance())
	}
}

func (ibs *IntraBlockState) createAccount(addr accounts.Address, contractCreation bool) (err error) {
	var hadPrevious, prevSelfdestructed bool
	var prevIncarnation, prevPrevIncarnation uint64
	var prevBalance uint256.Int

	if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
		defer ibs.traceCreateAccount(addr, contractCreation, &err, func() string { return prevBalance.String() })()
	}

	readAccount, _, _, err := ibs.getAccount(addr, true)
	if err != nil {
		return err
	}
	if readAccount != nil {
		hadPrevious = true
		prevIncarnation = readAccount.Incarnation
		prevPrevIncarnation = readAccount.PrevIncarnation
		prevBalance = readAccount.Balance

		destructed, _, _, err := refreshSelfDestruct(ibs, addr)
		if err != nil {
			return err
		}
		_, ownSD := ibs.ownSelfDestruct(addr)
		// The versionMap may hold a prior tx's SelfDestructPath=true that
		// readCore short-circuits, so read the versionMap directly.
		if !destructed {
			if d, res, ok := ibs.stateUpdates.ReadSelfDestruct(addr, ibs.txIndex); ok && res.resolved() && d {
				destructed = true
			}
		}
		// A later tx that left the account non-empty revived it after a prior
		// tx's self-destruct. Check the field cells, as the record lags them.
		if destructed && !ownSD {
			if destructed, err = ibs.emptyFromFields(addr, readAccount); err != nil {
				return err
			}
		}
		prevSelfdestructed = destructed
	} else if sd, ok := ibs.ownSelfDestruct(addr); ok && sd {
		// A within-tx create→self-destruct leaves no base record and no cell; treat
		// the destroyed account as the previous so the resurrect write is emitted.
		hadPrevious = true
		prevSelfdestructed = true
	}

	var prevInc uint64
	if prevSelfdestructed {
		prevInc = prevIncarnation
	}
	if hadPrevious && prevInc < prevPrevIncarnation {
		prevInc = prevPrevIncarnation
	}
	// Capture each path's own (source, version) for the synthetic reads stamped
	// below; inheriting the account-record version would trip the validator on
	// the recursive AddressPath check.
	incSource, incVersion := StorageRead, UnknownVersion
	if inc, res, ok := ibs.stateUpdates.ReadIncarnation(addr, ibs.txIndex); ok && res.resolved() {
		incSource = MapRead
		incVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
		if inc > prevInc {
			prevInc = inc
		}
	}
	balSource, balVersion := StorageRead, UnknownVersion
	var preTxBalance uint256.Int
	balWiped := false
	if !ibs.noConflictDetection {
		if bal, res, ok := ibs.stateUpdates.ReadBalance(addr, ibs.txIndex); ok && res.resolved() {
			balSource = MapRead
			balVersion = Version{TxIndex: res.DepIdx(), Incarnation: res.Incarnation()}
			preTxBalance = bal
			// A burning destruct at or above the cell wipes the balance. As on the read
			// path, the zero depends on the destruct, not on the wiped cell.
			if destructed, sdRes, ok := ibs.stateUpdates.ReadSelfDestruct(addr, ibs.txIndex); ok && sdRes.resolved() && destructed &&
				sdRes.DepIdx() >= res.DepIdx() && res.ValStatus() == ValueDeleted {
				preTxBalance.Clear()
				balWiped = true
				ibs.reads.SetSelfDestruct(addr, VersionedRead[bool]{
					ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: sdRes.DepIdx(), Incarnation: sdRes.Incarnation()}},
					Val:        true,
				})
			}
		}
	}
	// Carry the live balance over the create (the EVM preserves funds sent to a
	// not-yet-created address). Normally zero for a self-destructed previous, but a
	// same-tx credit may have revived the account before this create, so carry a
	// non-zero revived balance too.
	var carryBalance uint256.Int
	carryBalanceValid := hadPrevious && !prevSelfdestructed
	if hadPrevious {
		b, _, err := ibs.getBalance(addr)
		if err != nil {
			return err
		}
		if carryBalanceValid || !b.IsZero() {
			carryBalance = b
			carryBalanceValid = true
		}
	}

	ibs.create(addr, hadPrevious)
	if prevSelfdestructed {
		// The reset-object journal entry marks addr dirty, but that mark is dropped
		// on revert; this un-journalled increment keeps a resurrected address in
		// journal.dirties across an intra-tx revert.
		ibs.journal.dirty(addr)
	}

	var writeBalance uint256.Int
	if carryBalanceValid {
		writeBalance.Set(&carryBalance)
	} else if !hadPrevious && balSource == MapRead {
		// This tx already read the account destroyed, so there is no record, but a
		// prior tx's fee or transfer revived it: carry that recorded pre-tx balance.
		writeBalance.Set(&preTxBalance)
	}

	var incarnation uint64
	if contractCreation {
		// Record contract creation so FinalizedWrites does not remove a newly
		// deployed contract as empty.
		ibs.recordWriteCreateContract(addr, true)
		incarnation = prevInc + 1
		if dbg.TraceTransactionIO && (ibs.trace || dbg.TraceAccount(addr.Handle())) {
			fmt.Printf("%d (%d.%d) New Incarnation %x: %d\n", ibs.blockNum, ibs.txIndex, ibs.version, addr, incarnation)
		}
	}

	// Synthetic reads let creation clashes between transactions be detected. The
	// balance read carries the pre-tx balance (zero without a cell: the account was
	// absent), since later reads, also after a revert, are served from it. An earlier
	// internal read is promoted to give the balance write a block-access-list baseline.
	ibs.MarkAddressAccess(addr, true)
	if !ibs.noConflictDetection {
		if vr, seen := ibs.reads.GetBalance(addr); !seen && !balWiped {
			ibs.reads.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader{Source: balSource, Version: balVersion}, preTxBalance})
		} else if seen && vr.internal {
			vr.internal = false
			ibs.reads.SetBalance(addr, vr)
		}
		ibs.reads.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader{Source: incSource, Version: incVersion}, prevInc})
	}
	ibs.recordWriteBalance(addr, writeBalance)
	ibs.recordWriteIncarnation(addr, incarnation)
	if !hadPrevious || prevSelfdestructed {
		ibs.recordWriteSelfDestruct(addr, false)
	}
	return nil
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
	return ibs.createAccount(addr, contractCreation)
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

// BurnSelfDestructs makes every self-destruct of this state delete its account, also one
// that ran with balance-preserving (EIP-8246) rules. For genesis only, which finalizes
// without EIP-8246: a block keeps the balance that EIP-8246 preserves.
func (ibs *IntraBlockState) BurnSelfDestructs() {
	for addr, sd := range ibs.writes.SelfDestructs() {
		if b, ok := ibs.writes.GetBalance(addr); sd.Val && ok && !b.Deletes() {
			ibs.recordWriteBalanceDeleted(addr)
		}
	}
}

// FinalizedWrites applies EIP-6780 normalization and EIP-161 filtering, then
// returns a detached committable snapshot. The EIP-161 filtering reads pre-tx state,
// so it returns the IBS state read error, which ResetIO would clear.
func (ibs *IntraBlockState) FinalizedWrites(chainRules *chain.Rules) (*WriteSet, error) {
	// Publish this tx's cold-read committed origins to the versionMap before EIP-161
	// composition below reads it (encodeExistingEmptyRemovals builds a versionMap-backed
	// account view). Every FinalizedWrites caller is covered here, so no read path needs
	// to remember to publish separately.
	ibs.PublishOrigins()
	writes := ibs.writes.Finalize()
	var replaced map[accounts.Address]struct{}
	if chainRules != nil && chainRules.IsAmsterdam {
		replaced = ibs.publishCreateDestructAsBalances(writes)
	}
	// Snapshot the accounts the tx really self-destructed (a SELFDESTRUCT opcode)
	// before the removal encoders synthesize their own SelfDestruct cells, so the
	// contract-vs-EOA removal verdict reads the genuine signal, not a synthesized one.
	var realSelfDestruct map[accounts.Address]struct{}
	for a, w := range writes.SelfDestructs() {
		if w.Val {
			if realSelfDestruct == nil {
				realSelfDestruct = map[accounts.Address]struct{}{}
			}
			realSelfDestruct[a] = struct{}{}
		}
	}
	ibs.withholdCreatedEmptyAccounts(chainRules, writes)
	ibs.encodeExistingEmptyRemovals(chainRules, writes, realSelfDestruct, replaced)
	return writes, ibs.StateReadError()
}

// publishCreateDestructAsBalances replaces the writes of a contract the tx created and destroyed
// with those of an account holding its kept balance: under EIP-8246 that balance is all the tx
// leaves, and withholdCreatedEmptyAccounts then treats an account at an absent address like any
// created account. At an address that existed, the creation stays, as it replaces the old storage,
// with the zero incarnation that the destruct leaves and the dead contract's zeroed slots, which
// readers of the state changes see; such an account that ends empty keeps the destruct, which
// removes it. It returns the replaced addresses.
func (ibs *IntraBlockState) publishCreateDestructAsBalances(writes *WriteSet) map[accounts.Address]struct{} {
	var replaced map[accounts.Address]struct{}
	for addr, sd := range writes.selfDestruct {
		cc, ok := writes.createContract[addr]
		if !sd.Val || !ok || !cc.Val {
			continue
		}
		balance, ok := writes.balance[addr]
		if !ok || balance.Deletes() {
			continue // a burning destruct leaves no balance-only account
		}
		existed := !ibs.readAbsent(addr)
		if existed && balance.Val.IsZero() {
			continue
		}
		slots := writes.storage[addr]
		version := ibs.Version()
		writes.deleteAddr(addr)
		created := accounts.NewAccount()
		writes.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: version}, Val: &created})
		writes.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: version}, Val: accounts.EmptyCodeHash})
		writes.SetBalance(addr, balance)
		if existed {
			writes.SetCreateContract(addr, cc)
			writes.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: version}})
			if slots != nil {
				writes.storage[addr] = slots
			}
		}
		if replaced == nil {
			replaced = map[accounts.Address]struct{}{}
		}
		replaced[addr] = struct{}{}
	}
	return replaced
}

// createdSelfDestructEmpty decides emptiness without an account view for an account this tx created as a contract
// and really self-destructed: the create collision rule means it had nonce 0 and no code before the tx, so its
// balance write is the only field left to check.
func createdSelfDestructEmpty(writes *WriteSet, realSelfDestruct map[accounts.Address]struct{}, addr accounts.Address) (empty, known bool) {
	if _, sd := realSelfDestruct[addr]; !sd {
		return false, false
	}
	if cc, ok := writes.GetCreateContract(addr); !ok || !cc.Val {
		return false, false
	}
	bal, ok := writes.GetBalance(addr)
	if !ok {
		return false, false
	}
	return bal.Val.IsZero(), true
}

func (ibs *IntraBlockState) encodeExistingEmptyRemovals(chainRules *chain.Rules, writes *WriteSet, realSelfDestruct, replaced map[accounts.Address]struct{}) {
	if chainRules == nil {
		return
	}
	for addr := range writes.addrs() {
		if !EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr) {
			continue
		}
		if _, ok := replaced[addr]; ok {
			continue // withholdCreatedEmptyAccounts settled an absent one, and one that existed keeps a balance
		}
		if empty, known := createdSelfDestructEmpty(writes, realSelfDestruct, addr); known {
			if empty {
				writes.SetContractRemoval(addr, ibs.Version())
			}
			continue
		}
		view := NewVersionedAccountView(addr, ibs.txIndex, ibs.stateUpdates, ibs.stateReader)
		if err := view.Err(); err != nil {
			// A failed base read zeroes the composed fields, which would read as
			// an EIP-161 empty and delete a live account. Surface the error and
			// leave the account's writes intact instead.
			ibs.RecordStateReadError(err)
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
		// calc burns its delayed fee instead of reviving it as an EOA. A touched-empty EOA
		// (e.g. a self-destruct beneficiary) carries no real self-destruct, so it is not one.
		if _, sd := realSelfDestruct[addr]; sd {
			writes.SetContractRemoval(addr, ibs.Version())
		} else {
			writes.SetEmptyRemoval(addr, ibs.Version())
		}
	}
}

// withholdCreatedEmptyAccounts drops the writes of accounts the tx observed as absent
// and left empty (EIP-161), or replaces them with the EIP-161 removal when the version
// map holds cells for the account. An account that existed
// before is kept even when it ends empty — clearing it needs an explicit delete,
// which encodeExistingEmptyRemovals emits.
func (ibs *IntraBlockState) withholdCreatedEmptyAccounts(chainRules *chain.Rules, writes *WriteSet) {
	if chainRules == nil {
		return
	}
	for addr := range writes.address {
		if !EIP161EmptyRemoval(chainRules.IsEIP161Enabled(), chainRules.IsAura, addr) {
			continue
		}
		read, _ := ibs.reads.GetAddress(addr)
		if (read.Val != nil && !read.Val.IsNil()) || !writes.createdEmpty(addr) {
			continue
		}
		// With no version-map cell the account is absent before and after the tx, so dropping the
		// writes is exact; a removal stamp would make every later toucher depend on this tx.
		if ibs.stateUpdates.load(addr) == nil {
			writes.deleteAddr(addr)
			continue
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
	io.mergeTx(version, ibs.reads, writes)
}

// FlushWritesToVersionMap publishes the supplied writes to this state's version map.
func (ibs *IntraBlockState) FlushWritesToVersionMap(writes *WriteSet) {
	writes.assertNotArena("flush")
	ibs.stateUpdates.FlushWrites(writes, true)
}

// WriteFlushedTo hands the current tx's writes, already flushed to the versionMap, to
// stateWriter the way FinalizeTx hands a materialized tx's: each account as it reads
// before and after the tx, each storage slot with its value at tx start.
func (ibs *IntraBlockState) WriteFlushedTo(writes *WriteSet, stateWriter StateWriter) error {
	for addr := range writes.addrs() {
		before := NewVersionedAccountView(addr, ibs.txIndex, ibs.stateUpdates, ibs.stateReader)
		original := before.Account()
		if err := before.Err(); err != nil {
			return err
		}
		if original == nil {
			original = &accounts.Account{}
		}
		after := NewVersionedAccountView(addr, ibs.txIndex+1, ibs.stateUpdates, ibs.stateReader)
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
		// See updateAccount: the account write of a recreate below the original incarnation clears
		// the old code and storage, so it must land before the new ones.
		downRev := original.Incarnation > account.Incarnation
		if downRev {
			if err := stateWriter.UpdateAccountData(addr, original, account); err != nil {
				return err
			}
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
		if !downRev {
			if err := stateWriter.UpdateAccountData(addr, original, account); err != nil {
				return err
			}
		}
	}
	return nil
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

// EndTransaction is the versioned per-transaction boundary: it clears the
// journal and refund so a later transaction cannot revert this one's changes.
// Writes already live in the version map (taken via FinalizedWrites); no
// StateWriter is involved.
func (ibs *IntraBlockState) EndTransaction() {
	ibs.clearJournalAndRefund()
}

func (ibs *IntraBlockState) clearJournalAndRefund() {
	ibs.journal.Reset()
	ibs.revisions.reset()
	ibs.refund = uint64(0)
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
	ibs.reads.access = nil
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

// AddSlotOnlyToAccessList preserves account coldness for EIP-7906 storage lookups.
func (ibs *IntraBlockState) AddSlotOnlyToAccessList(addr accounts.Address, slot accounts.StorageKey) {
	if ibs.accessList.AddSlotOnly(addr, slot) {
		ibs.journal.accessListAddSlotOnlyChange(addr, slot)
	}
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
	if ibs.reads.access == nil {
		ibs.reads.access = make(AccessSet)
	}
	if opts, ok := ibs.reads.access[addr]; ok {
		if opts.revertable && !revertable {
			opts.revertable = false
			ibs.reads.access[addr] = opts
		}
	} else {
		ibs.reads.access[addr] = accessOptions{revertable: revertable}
	}
}

// StartAccessRecording enables versioned access tracking until ResetIO.
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
	ibs.reads.access = nil
}

// MarkReadsInternal marks all versioned reads for addr as internal.
// Internal reads are kept for parallel-execution conflict detection
// but excluded from the block access list (BAL).  This is used when
// a state read was performed for gas calculation but the operation
// was rejected (e.g. CALL with value inside STATICCALL).
func (ibs *IntraBlockState) MarkReadsInternal(addr accounts.Address) {
	ibs.reads.ScanAddr(addr, func(_ AccountPath, _ accounts.StorageKey, hdr *ReadHeader) {
		hdr.internal = true
	})
}

func (ibs *IntraBlockState) AccessedAddr(addr accounts.Address) bool {
	_, ok := ibs.reads.access[addr]
	return ok
}

func (ibs *IntraBlockState) accountRead(addr accounts.Address, account *accounts.Account, source ReadSource, version Version) {
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
		if _, res, ok := ibs.stateUpdates.ReadAddress(addr, ibs.txIndex); !ok || !res.resolved() {
			source = StorageRead
			version = UnknownVersion
		}
	}
	ibs.reads.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: source, Version: version},
		Val:        NewAccountView(&data),
	})
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
	if vw, ok := ibs.writes.GetBalance(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetBalance(addr, vw)
	traceWrite(ibs, vw)
}

// recordWriteBalanceDeleted records a balance write that deletes the account's
// leaf (ValueDeleted ⇒ nil on the projection, absent to the reader). Used by the
// self-destruct burn so the delete decision rides the write, not the balance value.
func (ibs *IntraBlockState) recordWriteBalanceDeleted(addr accounts.Address) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetBalance(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = uint256.Int{}
		vw.valStatus = ValueDeleted
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWBalance()
	vw.WriteHeader = WriteHeader{Address: addr, Path: BalancePath, Version: ibs.Version(), valStatus: ValueDeleted}
	vw.Val = uint256.Int{}
	ibs.writes.SetBalance(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteNonce(addr accounts.Address, val uint64, reason tracing.NonceChangeReason) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetNonce(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		vw.NonceReason = reason
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWNonce()
	vw.WriteHeader = WriteHeader{Address: addr, Path: NoncePath, Version: ibs.Version(), NonceReason: reason}
	vw.Val = val
	ibs.writes.SetNonce(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteIncarnation(addr accounts.Address, val uint64) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetIncarnation(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWIncarnation()
	vw.WriteHeader = WriteHeader{Address: addr, Path: IncarnationPath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetIncarnation(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteSelfDestruct(addr accounts.Address, val bool) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetSelfDestruct(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWSelfDestruct()
	vw.WriteHeader = WriteHeader{Address: addr, Path: SelfDestructPath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetSelfDestruct(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCreateContract(addr accounts.Address, val bool) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetCreateContract(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWCreateContract()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CreateContractPath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetCreateContract(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCode(addr accounts.Address, val accounts.Code) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetCode(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWCode()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodePath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetCode(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCodeHash(addr accounts.Address, val accounts.CodeHash) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetCodeHash(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWCodeHash()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeHashPath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetCodeHash(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteCodeSize(addr accounts.Address, val int) {
	ibs.MarkAddressAccess(addr, true)
	if vw, ok := ibs.writes.GetCodeSize(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWCodeSize()
	vw.WriteHeader = WriteHeader{Address: addr, Path: CodeSizePath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetCodeSize(addr, vw)
	traceWrite(ibs, vw)
}

func (ibs *IntraBlockState) recordWriteAddress(addr accounts.Address, account *accounts.Account) {
	ibs.MarkAddressAccess(addr, true)
	// A copy, made only here: the caller's account keeps changing.
	val := ibs.writes.copyAccount(account)
	if vw, ok := ibs.writes.GetAddress(addr); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWAddress()
	vw.WriteHeader = WriteHeader{Address: addr, Path: AddressPath, Version: ibs.Version()}
	vw.Val = val
	ibs.writes.SetAddress(addr, vw)
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
	if vw, ok := ibs.writes.GetStorage(addr, key); ok {
		vw.Version = ibs.Version()
		vw.Val = val
		vw.valStatus = storageValueStatus(vw.orig, val)
		traceWrite(ibs, vw)
		return
	}
	vw := ibs.writes.newVWStorage()
	vw.WriteHeader = WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: ibs.Version(), valStatus: storageValueStatus(prev, val)}
	vw.orig = prev
	vw.Val = val
	ibs.writes.SetStorage(addr, key, vw)
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
	if own, ok := ibs.ownSelfDestruct(addr); ok {
		return own
	}
	return ibs.stateUpdates.IsNetAbsent(addr, ibs.txIndex)
}

func (ibs *IntraBlockState) ownSelfDestruct(addr accounts.Address) (bool, bool) {
	vw, ok := ibs.writes.GetSelfDestruct(addr)
	if !ok {
		return false, false
	}
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false, false
	}
	return vw.Val, true
}

// ownCreateContract reports whether this tx's own writes created a
// contract at addr (the CreateContract cell). Guarded by journal.dirties so a
// stale entry from a reverted create is ignored.
func (ibs *IntraBlockState) ownCreateContract(addr accounts.Address) (bool, bool) {
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false, false
	}
	vw, ok := ibs.writes.GetCreateContract(addr)
	if !ok {
		return false, false
	}
	return vw.Val, true
}

// writeHit probes the dirty per-tx write set for a write at
// (addr, path, key) and, when present, populates the corresponding
// per-typed pointer field on r.  Returns true when a write was found.
// The non-storage paths share a single non-nil typed field; the storage
// path uses r.vwStorage.
func (ibs *IntraBlockState) writeHit(addr accounts.Address, path AccountPath, key accounts.StorageKey, r *readPathResult) bool {
	if _, isDirty := ibs.journal.dirties[addr]; !isDirty {
		return false
	}
	switch path {
	case AddressPath:
		if vw, ok := ibs.writes.GetAddress(addr); ok {
			r.vwAddress = vw
			return true
		}
	case BalancePath:
		if vw, ok := ibs.writes.GetBalance(addr); ok {
			r.vwBalance = vw
			return true
		}
	case NoncePath:
		if vw, ok := ibs.writes.GetNonce(addr); ok {
			r.vwNonce = vw
			return true
		}
	case IncarnationPath:
		if vw, ok := ibs.writes.GetIncarnation(addr); ok {
			r.vwIncarnation = vw
			return true
		}
	case SelfDestructPath:
		if vw, ok := ibs.writes.GetSelfDestruct(addr); ok {
			r.vwSelfDestruct = vw
			return true
		}
	case CreateContractPath:
		if vw, ok := ibs.writes.GetCreateContract(addr); ok {
			r.vwCreateContract = vw
			return true
		}
	case CodePath:
		if vw, ok := ibs.writes.GetCode(addr); ok {
			r.vwCode = vw
			return true
		}
	case CodeHashPath:
		if vw, ok := ibs.writes.GetCodeHash(addr); ok {
			r.vwCodeHash = vw
			return true
		}
	case CodeSizePath:
		if vw, ok := ibs.writes.GetCodeSize(addr); ok {
			r.vwCodeSize = vw
			return true
		}
	case StoragePath:
		if vw, ok := ibs.writes.GetStorage(addr, key); ok {
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

func (ibs *IntraBlockState) RecordStateReadError(err error) {
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

// Reads returns the in-flight per-path read set.  The returned
// value shares the underlying maps with the IBS; it is handed over at
// end of tx (RecordReads / TxIn), after which ResetIO rebinds
// the IBS field to a fresh set.
func (ibs *IntraBlockState) Reads() ReadSet {
	return ibs.reads
}

func (ibs *IntraBlockState) ResetIO() {
	ibs.reads = ReadSet{}
	ibs.writes.recycle()
	ibs.dep = UnknownDep
	ibs.recordAccess = false
	ibs.stateReadErr = nil
}

// ResetReads clears tracked reads without affecting writes.
func (ibs *IntraBlockState) ResetReads() {
	ibs.reads = ReadSet{}
}

// Writes returns a frozen typed snapshot of this tx's recorded writes.
// The snapshot logic lives on the write-set itself (WriteSet.Snapshot); this is
// the IntraBlockState accessor for it.
func (ibs *IntraBlockState) Writes() *WriteSet {
	return ibs.writes.Snapshot()
}

// recordStorageOrigin / recordAddressOrigin collect a cold committed base cell into the
// tx-local origin set (at originIndex) instead of writing the shared versionMap mid-read.
func (ibs *IntraBlockState) recordStorageOrigin(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	vw := getVWStorage()
	vw.WriteHeader = WriteHeader{Address: addr, Key: key, Path: StoragePath, Version: Version{TxIndex: originIndex}}
	vw.Val = val
	ibs.origins.SetStorage(addr, key, vw)
}

func (ibs *IntraBlockState) recordAddressOrigin(addr accounts.Address, acc *accounts.Account) {
	vw := getVWAddress()
	vw.WriteHeader = WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: originIndex}}
	vw.Val = acc
	ibs.origins.SetAddress(addr, vw)
}

// PublishOrigins publishes this tx's collected committed-base origins into the versionMap's
// lock-free origin layer (write-once), so later reads resolve the pre-block base without taking
// an account's e.mu. The origin is immutable pre-block state, so it lives outside the OCC cells.
func (ibs *IntraBlockState) PublishOrigins() {
	ws := &ibs.origins
	for addr, vw := range ws.address {
		ibs.stateUpdates.StoreOriginAccount(addr, vw.Val)
	}
	for addr, inner := range ws.storage {
		for key, vw := range inner {
			ibs.stateUpdates.StoreOriginStorage(addr, key, vw.Val)
		}
	}
	ws.ReleaseAndReset()
}
