package stagedsync

import (
	"fmt"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// calcAccountState holds the accumulated account state for the commitment calculator.
type calcAccountState struct {
	Balance     uint256.Int
	Nonce       uint64
	CodeHash    [32]byte
	Incarnation uint64
	Deleted     bool
	// dirty tracks whether this account was modified in the current block
	dirty bool
	// queued means the address is already in calcState.queuedAccounts, so a
	// second write before the next commitment pass does not enqueue it twice.
	queued bool
}

// slotFlags is storageDirty's value: dirty mirrors calcAccountState.dirty,
// queued mirrors its queued.
type slotFlags struct{ dirty, queued bool }

// storageRef names one queued slot.
type storageRef struct {
	addr accounts.Address
	key  accounts.StorageKey
}

// calcDomainReader provides lazy-load reads for calcState using the
// asOfStateReader. This ensures all reads (both lazy-load and trie
// fold/unfold sibling reads) go through the same GetAsOf path,
// seeing state at the calculator's txNum.
type calcDomainReader struct {
	reader *asOfStateReader
}

func (r *calcDomainReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	addrVal := addr.Value()
	enc, _, err := r.reader.Read(kv.AccountsDomain, addrVal[:], 0)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	acc := new(accounts.Account)
	if err := accounts.DeserialiseV3(acc, enc); err != nil {
		return nil, err
	}
	return acc, nil
}

func (r *calcDomainReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addrVal := addr.Value()
	keyVal := key.Value()
	composite := make([]byte, 20+32)
	copy(composite, addrVal[:])
	copy(composite[20:], keyVal[:])
	enc, _, err := r.reader.Read(kv.StorageDomain, composite, 0)
	if err != nil {
		return uint256.Int{}, false, err
	}
	if len(enc) == 0 {
		return uint256.Int{}, false, nil
	}
	var val uint256.Int
	val.SetBytes(enc)
	return val, true, nil
}

// accountBaselineReader supplies an address's pre-write account fields.
type accountBaselineReader interface {
	ReadAccountData(addr accounts.Address) (*accounts.Account, error)
}

// storageEnumerator lists every persisted storage slot under an address.
type storageEnumerator interface {
	EachStorageSlot(addr accounts.Address, fn func(key accounts.StorageKey) error) error
}

// calcState is the commitment calculator's local state accumulator.
// It maintains the current state for every account/storage key that has been
// touched. On first touch, values are lazy-loaded from the domain via the
// asOfStateReader. Subsequent writes overwrite the local copy. At block boundary,
// the accumulated state is fed to the trie's Updates buffer.
type calcState struct {
	accounts map[accounts.Address]*calcAccountState
	// storageState holds the accumulated value for each slot
	storageState map[accounts.Address]map[accounts.StorageKey]uint256.Int
	// storageDirty tracks which slots were modified in the current block
	storageDirty map[accounts.Address]map[accounts.StorageKey]slotFlags

	// queuedAccounts and queuedStorage are the keys written since the last
	// commitment pass, in write order. A pass consumes them; block end consumes
	// them too, having covered a superset via the dirty flags. Holding the work
	// as a queue rather than a predicate is what keeps a pass proportional to
	// what changed since the previous one.
	queuedAccounts []accounts.Address
	queuedStorage  []storageRef

	// Work-amplification guard (ERIGON_ASSERT): a mid-block rehash may only
	// cover keys written since the previous one, so its total over a block can
	// never exceed the writes the block made. Reading `dirty` there instead —
	// the bug this pass was split to fix — breaks the bound immediately.
	writesThisBlock uint64
	coveredMidBlock uint64

	// domainReader provides lazy-load from the domain via asOfStateReader.
	domainReader accountBaselineReader

	// storageEnum is a test injection point; production leaves it nil. The
	// self-destruct path no longer reads it — the account delete collapses the
	// subtree — so it exists only to assert that in tests.
	storageEnum storageEnumerator

	// lazyLoadErr captures the first error encountered during ensureAccount /
	// ensureStorage. Sticky — never cleared — so the calculator can fail the
	// next compute instead of silently producing wrong updates from a missing
	// baseline. Surface via LazyLoadErr().
	lazyLoadErr error

	logger    log.Logger
	logPrefix string
}

// LazyLoadErr returns the first error encountered during ensureAccount
// lazy-loads, or nil. The calculator must check this before
// computing — a missing baseline yields a wrong trie root that is hard to
// attribute back to the original I/O error.
func (cs *calcState) LazyLoadErr() error { return cs.lazyLoadErr }

func newCalcState(reader *asOfStateReader, logger log.Logger, logPrefix string) *calcState {
	return &calcState{
		accounts:     make(map[accounts.Address]*calcAccountState),
		storageState: make(map[accounts.Address]map[accounts.StorageKey]uint256.Int),
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]slotFlags),
		domainReader: &calcDomainReader{reader: reader},
		logger:       logger,
		logPrefix:    logPrefix,
	}
}

// writesCoverBaseline reports whether writes set every field ensureAccount would
// lazy-load, making the domain read dead. Normalize fills all three for every
// address it does not drop, so this holds for all but self-destructed ones.
func writesCoverBaseline(writes *state.WriteSet, addr accounts.Address) bool {
	return writes.Has(state.WriteHeader{Address: addr, Path: state.BalancePath}) &&
		writes.Has(state.WriteHeader{Address: addr, Path: state.NoncePath}) &&
		writes.Has(state.WriteHeader{Address: addr, Path: state.CodeHashPath})
}

// ensureAccount returns the account state, lazy-loading from domain on first touch.
func (cs *calcState) ensureAccount(addr accounts.Address, writes *state.WriteSet) *calcAccountState {
	if acc, ok := cs.accounts[addr]; ok {
		return acc
	}

	acc := &calcAccountState{
		CodeHash: empty.CodeHash,
	}
	if cs.domainReader != nil && !writesCoverBaseline(writes, addr) {
		dbAcc, err := cs.domainReader.ReadAccountData(addr)
		if err != nil {
			// Sticky — recorded so the next compute fails fast instead of
			// silently producing wrong updates on top of zero state.
			if cs.lazyLoadErr == nil {
				cs.lazyLoadErr = fmt.Errorf("ensureAccount(%x): %w", addr.Value(), err)
			}
			if cs.logger != nil {
				cs.logger.Warn("["+cs.logPrefix+"] commitmentCalculator: lazy-load ReadAccountData failed", "addr", addr, "err", err)
			}
		} else if dbAcc != nil {
			acc.Balance = dbAcc.Balance
			acc.Nonce = dbAcc.Nonce
			acc.CodeHash = dbAcc.CodeHash.Value()
		}
	}
	cs.accounts[addr] = acc
	return acc
}

// ApplyWrites folds a tx's typed write collections into the local state.
//
// Self-destruct is applied before the field writes so the priority is explicit
// in loop order: a SELFDESTRUCT marks the account Deleted and zeros its fields
// and storage subtree, then a same-address non-zero field write (a same-tx
// recreate) revives it by clearing Deleted. A zero field write does not revive
// a self-destructed address; for a non-self-destructed address any field write
// — even zero — means it is alive (clears Deleted).
func (cs *calcState) ApplyWrites(writes *state.WriteSet, eip8246 bool) {
	if dbg.AssertEnabled {
		cs.writesThisBlock += uint64(writes.Count())
	}
	sdThisCall := make(map[accounts.Address]bool)
	for addr, vw := range writes.SelfDestructs() {
		sdThisCall[addr] = vw.Val
		if vw.Val {
			acc := cs.ensureAccount(addr, writes)
			acc.Deleted = true
			cs.markWritten(addr, acc)
			cs.deleteStorageSubtree(addr)
		}
	}
	clearsDeleted := func(addr accounts.Address, nonZero bool) bool {
		return nonZero || !sdThisCall[addr]
	}
	for addr, vw := range writes.Balances() {
		acc := cs.ensureAccount(addr, writes)
		acc.Balance = vw.Val
		cs.markWritten(addr, acc)
		if clearsDeleted(addr, !acc.Balance.IsZero()) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Nonces() {
		acc := cs.ensureAccount(addr, writes)
		acc.Nonce = vw.Val
		cs.markWritten(addr, acc)
		if clearsDeleted(addr, acc.Nonce != 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.CodeHashes() {
		acc := cs.ensureAccount(addr, writes)
		acc.CodeHash = vw.Val.Value()
		cs.markWritten(addr, acc)
		if clearsDeleted(addr, vw.Val.Value() != empty.CodeHash) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Codes() {
		acc := cs.ensureAccount(addr, writes)
		acc.CodeHash = vw.Val.Hash.Value()
		cs.markWritten(addr, acc)
		if clearsDeleted(addr, vw.Val.Len() > 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Incarnations() {
		acc := cs.ensureAccount(addr, writes)
		acc.Incarnation = vw.Val
		cs.markWritten(addr, acc)
	}
	for addr, inner := range writes.Storages() {
		// Skip lazy-loading the prior slot value: the only downstream consumer
		// (FlushToUpdates) reads exactly the value set below, so the cold
		// GetAsOf seek it would cost is wasted.
		slots := cs.storageState[addr]
		if slots == nil {
			slots = make(map[accounts.StorageKey]uint256.Int)
			cs.storageState[addr] = slots
		}
		for key, vw := range inner {
			slots[key] = vw.Val
			cs.markSlotWritten(addr, key)
		}
	}
	// An account still Deleted after the field writes (no reviving non-zero
	// write) must be all-zero — matching serial's DomainDel leaf removal — even
	// though IBS emits the pre-SD IncarnationPath/BalancePath values.
	for addr := range sdThisCall {
		if acc, ok := cs.accounts[addr]; ok && acc.Deleted {
			if !eip8246 {
				acc.Balance = uint256.Int{}
			}
			acc.Nonce = 0
			acc.CodeHash = empty.CodeHash
			acc.Incarnation = 0
		}
	}
}

// markWritten records an account write: dirty for block end, queued once for
// the next commitment pass.
func (cs *calcState) markWritten(addr accounts.Address, acc *calcAccountState) {
	acc.dirty = true
	if !acc.queued {
		acc.queued = true
		cs.queuedAccounts = append(cs.queuedAccounts, addr)
	}
}

// markSlotWritten is markWritten for a storage slot.
func (cs *calcState) markSlotWritten(addr accounts.Address, key accounts.StorageKey) {
	slots := cs.storageDirty[addr]
	if slots == nil {
		slots = make(map[accounts.StorageKey]slotFlags)
		cs.storageDirty[addr] = slots
	}
	f := slots[key]
	f.dirty = true
	if !f.queued {
		f.queued = true
		cs.queuedStorage = append(cs.queuedStorage, storageRef{addr: addr, key: key})
	}
	slots[key] = f
}

// deleteStorageSubtree handles a self-destructed account's storage. Only slots
// touched this window (already in the maps) get explicit deletes; the account's
// own DeleteUpdate collapses the rest of the subtree, so untouched on-disk slots
// need not be read.
func (cs *calcState) deleteStorageSubtree(addr accounts.Address) {
	slots := cs.storageState[addr]
	if len(slots) == 0 {
		return
	}
	for key := range slots {
		slots[key] = uint256.Int{}
		cs.markSlotWritten(addr, key)
	}
}

// LoadFromBAL populates calcState from an EIP-7928 Block Access List rather
// than the per-tx VersionedWrites stream: it takes each field's block-end value
// and feeds the existing ApplyWrites. The BAL carries no deletion marker, so an
// account whose block-end state is empty (EIP-161) must be reconstructed as a
// delete here: after the field changes and lazy-loaded pre-block fields are
// merged, a touched all-zero account is marked Deleted so FlushToUpdates removes
// its leaf instead of writing a zero-valued one. Storage reads are ignored.
func (cs *calcState) LoadFromBAL(blockAccessList types.BlockAccessList, emptyRemoval bool, isAura bool, eip8246 bool) {
	cs.LoadFromBALUpTo(blockAccessList, math.MaxUint32, emptyRemoval, isAura, eip8246)
}

// LoadFromBALUpTo is LoadFromBAL restricted to changes at tx index ≤ maxTxIndex,
// i.e. the state as of that point within the block. Used to fold a block up to a
// mid-block step boundary (checkpoint) from the same per-tx BAL, then fold the
// remainder — the BAL carries every change's tx index, so no re-execution is
// needed. maxTxIndex == math.MaxUint32 is the whole block (== LoadFromBAL).
func (cs *calcState) LoadFromBALUpTo(blockAccessList types.BlockAccessList, maxTxIndex uint32, emptyRemoval bool, isAura bool, eip8246 bool) {
	cs.ApplyWrites(bal.ToWriteSet(blockAccessList, maxTxIndex), eip8246)

	// EIP-161: a touched account whose merged block-end state is empty is
	// removed from the trie. The BAL carries no deletion marker, so reconstruct
	// it here, gated exactly as the incremental path (Normalize).
	for i := range blockAccessList {
		ac := &blockAccessList[i]
		acc := cs.accounts[ac.Address]
		if acc == nil || !acc.dirty || acc.Deleted {
			continue
		}
		if acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash &&
			state.EIP161EmptyRemoval(emptyRemoval, isAura, ac.Address) {
			acc.Deleted = true
			acc.Incarnation = 0
		}
	}
}

// FlushToUpdates writes the accumulated dirty state to a commitment.Updates
// buffer. Only keys modified in this block are emitted. Account updates
// always include the full current state (all fields) so the trie sees
// complete values.
func (cs *calcState) FlushToUpdates(updates *commitment.Updates) {
	for addr, acc := range cs.accounts {
		if !acc.dirty {
			continue
		}
		cs.emitAccount(updates, addr, acc)
	}
	for addr, slots := range cs.storageDirty {
		for key, f := range slots {
			if !f.dirty {
				continue
			}
			cs.emitSlot(updates, addr, key)
		}
	}
}

// FlushQueuedToUpdates emits only the keys written since the previous
// commitment pass. Block end still calls FlushToUpdates, so the set of keys a
// block reports — and therefore its changeset — is unchanged.
func (cs *calcState) FlushQueuedToUpdates(updates *commitment.Updates) {
	for _, addr := range cs.queuedAccounts {
		if acc, ok := cs.accounts[addr]; ok {
			cs.emitAccount(updates, addr, acc)
		}
	}
	for _, ref := range cs.queuedStorage {
		cs.emitSlot(updates, ref.addr, ref.key)
	}
	if dbg.AssertEnabled {
		cs.coveredMidBlock += uint64(len(cs.queuedAccounts) + len(cs.queuedStorage))
	}
}

func (cs *calcState) emitAccount(updates *commitment.Updates, addr accounts.Address, acc *calcAccountState) {
	address := addr.Value()
	key := string(address[:])

	// A "Deleted" account only encodes as serial's leaf-removing DeleteUpdate
	// when every field is actually zero; a Deleted account that still holds a
	// non-zero balance/nonce/code (or a retained incarnation) keeps its leaf,
	// so emit a regular UPDATE with the real values instead.
	isAllZero := acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash
	switch {
	case acc.Deleted && acc.Incarnation > 0 && isAllZero:
		updates.TouchPlainKeyDirect(key, &commitment.Update{
			Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
			Balance:  uint256.Int{},
			Nonce:    0,
			CodeHash: empty.CodeHash,
		})
	case acc.Deleted && isAllZero:
		updates.TouchPlainKeyDirect(key, &commitment.Update{
			Flags:    commitment.DeleteUpdate,
			CodeHash: empty.CodeHash,
		})
	default:
		// Either not Deleted, or Deleted-with-retained-values.
		updates.TouchPlainKeyDirect(key, &commitment.Update{
			Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
			Balance:  acc.Balance,
			Nonce:    acc.Nonce,
			CodeHash: acc.CodeHash,
		})
	}
}

func (cs *calcState) emitSlot(updates *commitment.Updates, addr accounts.Address, key accounts.StorageKey) {
	address := addr.Value()
	keyVal := key.Value()
	composite := make([]byte, 20+32)
	copy(composite, address[:])
	copy(composite[20:], keyVal[:])

	val := cs.storageState[addr][key]
	vBytes := val.Bytes()
	var u commitment.Update
	if len(vBytes) == 0 {
		u.Flags = commitment.DeleteUpdate
	} else {
		u.Flags = commitment.StorageUpdate
		u.StorageLen = int8(len(vBytes))
		copy(u.Storage[:], vBytes)
	}
	updates.TouchPlainKeyDirect(string(composite), &u)
}

// DrainQueued empties the queue and clears the queued marks. Call it only after
// the pass has landed: draining first would drop those keys from it.
func (cs *calcState) DrainQueued() {
	for _, addr := range cs.queuedAccounts {
		if acc, ok := cs.accounts[addr]; ok {
			acc.queued = false
		}
	}
	cs.queuedAccounts = cs.queuedAccounts[:0]
	for _, ref := range cs.queuedStorage {
		if slots := cs.storageDirty[ref.addr]; slots != nil {
			f := slots[ref.key]
			f.queued = false
			slots[ref.key] = f
		}
	}
	cs.queuedStorage = cs.queuedStorage[:0]
}

// AssertWorkBound panics when the mid-block rehashes covered more keys than the
// block wrote, which means one of them read the block's dirty flags instead of the queue.
// Call it at block end, before the flags are cleared.
func (cs *calcState) AssertWorkBound(blockNum uint64) {
	if cs.coveredMidBlock > cs.writesThisBlock {
		panic(fmt.Sprintf("calcState: block %d mid-block rehashes covered %d keys but the block wrote %d",
			blockNum, cs.coveredMidBlock, cs.writesThisBlock))
	}
}

// ResetBlockFlags clears the per-block dirty flags while keeping the
// accumulated state values. Called after commitment computation to
// prepare for the next block.
func (cs *calcState) ResetBlockFlags() {
	cs.DrainQueued()
	for _, acc := range cs.accounts {
		acc.dirty = false
	}
	for addr := range cs.storageDirty {
		delete(cs.storageDirty, addr)
	}
	cs.writesThisBlock, cs.coveredMidBlock = 0, 0
}
