package stagedsync

import (
	"fmt"
	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
	storageDirty map[accounts.Address]map[accounts.StorageKey]bool

	// domainReader provides lazy-load from the domain via asOfStateReader.
	domainReader *calcDomainReader

	// storageEnum enumerates an account's persisted storage subtree for
	// self-destruct; nil disables the on-disk subtree wipe.
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
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]bool),
		domainReader: &calcDomainReader{reader: reader},
		storageEnum:  &asOfStorageEnumerator{reader: reader},
		logger:       logger,
		logPrefix:    logPrefix,
	}
}

// ensureAccount returns the account state, lazy-loading from domain on first touch.
func (cs *calcState) ensureAccount(addr accounts.Address) *calcAccountState {
	if acc, ok := cs.accounts[addr]; ok {
		return acc
	}

	acc := &calcAccountState{
		CodeHash: empty.CodeHash,
	}
	if cs.domainReader != nil {
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
	sdThisCall := make(map[accounts.Address]bool)
	for addr, vw := range writes.SelfDestructs() {
		sdThisCall[addr] = vw.Val
		if vw.Val {
			acc := cs.ensureAccount(addr)
			acc.Deleted = true
			acc.dirty = true
			cs.deleteStorageSubtree(addr)
		}
	}
	clearsDeleted := func(addr accounts.Address, nonZero bool) bool {
		return nonZero || !sdThisCall[addr]
	}
	for addr, vw := range writes.Balances() {
		acc := cs.ensureAccount(addr)
		acc.Balance = vw.Val
		acc.dirty = true
		if clearsDeleted(addr, !acc.Balance.IsZero()) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Nonces() {
		acc := cs.ensureAccount(addr)
		acc.Nonce = vw.Val
		acc.dirty = true
		if clearsDeleted(addr, acc.Nonce != 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.CodeHashes() {
		acc := cs.ensureAccount(addr)
		acc.CodeHash = vw.Val.Value()
		acc.dirty = true
		if clearsDeleted(addr, vw.Val.Value() != empty.CodeHash) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Codes() {
		acc := cs.ensureAccount(addr)
		acc.CodeHash = vw.Val.Hash.Value()
		acc.dirty = true
		if clearsDeleted(addr, vw.Val.Len() > 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Incarnations() {
		acc := cs.ensureAccount(addr)
		acc.Incarnation = vw.Val
		acc.dirty = true
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
		dirty := cs.storageDirty[addr]
		if dirty == nil {
			dirty = make(map[accounts.StorageKey]bool)
			cs.storageDirty[addr] = dirty
		}
		for key, vw := range inner {
			slots[key] = vw.Val
			dirty[key] = true
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

// deleteStorageSubtree zeroes and dirties every storage slot under a
// self-destructed account, including untouched on-disk slots pulled via
// storageEnum, so FlushToUpdates emits a DeleteUpdate for each.
func (cs *calcState) deleteStorageSubtree(addr accounts.Address) {
	slots := cs.storageState[addr]
	if slots == nil {
		slots = make(map[accounts.StorageKey]uint256.Int)
		cs.storageState[addr] = slots
	}
	dirty := cs.storageDirty[addr]
	if dirty == nil {
		dirty = make(map[accounts.StorageKey]bool)
		cs.storageDirty[addr] = dirty
	}
	for key := range slots {
		slots[key] = uint256.Int{}
		dirty[key] = true
	}
	if cs.storageEnum == nil {
		return
	}
	if err := cs.storageEnum.EachStorageSlot(addr, func(key accounts.StorageKey) error {
		if _, ok := slots[key]; !ok {
			slots[key] = uint256.Int{}
			dirty[key] = true
		}
		return nil
	}); err != nil {
		// Sticky — a partial subtree wipe yields a wrong root, so fail the
		// next compute rather than silently leaving stale slots.
		if cs.lazyLoadErr == nil {
			cs.lazyLoadErr = fmt.Errorf("deleteStorageSubtree(%x): %w", addr.Value(), err)
		}
		if cs.logger != nil {
			cs.logger.Warn("["+cs.logPrefix+"] commitmentCalculator: SD storage enumeration failed", "addr", addr, "err", err)
		}
	}
}

// hasTxIndex is the BAL change-element constraint: every change
// (*BalanceChange/*NonceChange/*CodeChange/*StorageChange) carries the tx index
// within the block at which it was written.
type hasTxIndex interface{ GetIndex() uint32 }

// finalChangeUpTo returns the latest change whose tx index is ≤ maxTxIndex — the
// field's value as of that point in the block, for a mid-block (step-boundary)
// checkpoint fold. Indices are strictly increasing (BlockAccessList.Validate), so
// a reverse scan stops at the first in-range element. maxTxIndex == MaxUint32
// selects the block-end value (the whole block).
func finalChangeUpTo[T hasTxIndex](changes []T, maxTxIndex uint32) (T, bool) {
	for i := len(changes) - 1; i >= 0; i-- {
		if changes[i].GetIndex() <= maxTxIndex {
			return changes[i], true
		}
	}
	var zero T
	return zero, false
}

// LoadFromBAL populates calcState from an EIP-7928 Block Access List rather
// than the per-tx VersionedWrites stream: it takes each field's block-end value
// and feeds the existing ApplyWrites. The BAL carries no deletion marker, so an
// account whose block-end state is empty (EIP-161) must be reconstructed as a
// delete here: after the field changes and lazy-loaded pre-block fields are
// merged, a touched all-zero account is marked Deleted so FlushToUpdates removes
// its leaf instead of writing a zero-valued one. Storage reads are ignored.
func (cs *calcState) LoadFromBAL(bal types.BlockAccessList, emptyRemoval bool, isAura bool, eip8246 bool) {
	cs.LoadFromBALUpTo(bal, math.MaxUint32, emptyRemoval, isAura, eip8246)
}

// LoadFromBALUpTo is LoadFromBAL restricted to changes at tx index ≤ maxTxIndex,
// i.e. the state as of that point within the block. Used to fold a block up to a
// mid-block step boundary (checkpoint) from the same per-tx BAL, then fold the
// remainder — the BAL carries every change's tx index, so no re-execution is
// needed. maxTxIndex == math.MaxUint32 is the whole block (== LoadFromBAL).
func (cs *calcState) LoadFromBALUpTo(bal types.BlockAccessList, maxTxIndex uint32, emptyRemoval bool, isAura bool, eip8246 bool) {
	writes := &state.WriteSet{}
	for _, ac := range bal {
		addr := ac.Address
		if bc, ok := finalChangeUpTo(ac.BalanceChanges, maxTxIndex); ok {
			writes.SetBalance(addr, &state.VersionedWrite[uint256.Int]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: bc.Value,
			})
		}
		if nc, ok := finalChangeUpTo(ac.NonceChanges, maxTxIndex); ok {
			writes.SetNonce(addr, &state.VersionedWrite[uint64]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: nc.Value,
			})
		}
		if cc, ok := finalChangeUpTo(ac.CodeChanges, maxTxIndex); ok {
			writes.SetCode(addr, &state.VersionedWrite[accounts.Code]{
				WriteHeader: state.WriteHeader{Address: addr, Path: state.CodePath}, Val: accounts.NewCode(cc.Bytecode),
			})
		}
		for _, sc := range ac.StorageChanges {
			if chg, ok := finalChangeUpTo(sc.Changes, maxTxIndex); ok {
				writes.SetStorage(addr, sc.Slot, &state.VersionedWrite[uint256.Int]{
					WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: sc.Slot}, Val: chg.Value,
				})
			}
		}
	}
	cs.ApplyWrites(writes, eip8246)

	// EIP-161: a touched account whose merged block-end state is empty is
	// removed from the trie. The BAL carries no deletion marker, so reconstruct
	// it here, gated exactly as the incremental path (normalizeWriteSet).
	for _, ac := range bal {
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

	for addr, dirtySlots := range cs.storageDirty {
		address := addr.Value()
		slots := cs.storageState[addr]
		for key := range dirtySlots {
			val := slots[key]
			keyVal := key.Value()
			composite := make([]byte, 20+32)
			copy(composite, address[:])
			copy(composite[20:], keyVal[:])

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
	}
}

// ResetBlockFlags clears the per-block dirty flags while keeping the
// accumulated state values. Called after commitment computation to
// prepare for the next block.
func (cs *calcState) ResetBlockFlags() {
	for _, acc := range cs.accounts {
		acc.dirty = false
	}
	for addr := range cs.storageDirty {
		delete(cs.storageDirty, addr)
	}
}
