package stagedsync

import (
	"fmt"

	"math"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	fieldBal   uint8 = 1 << 0
	fieldNonce uint8 = 1 << 1
	fieldCode  uint8 = 1 << 2
)

// calcAccountState holds the accumulated account state for the commitment calculator.
type calcAccountState struct {
	Balance     uint256.Int
	Nonce       uint64
	CodeHash    [32]byte
	Incarnation uint64
	Deleted     bool
	dirty       bool
}

// calcDomainReader provides lazy-load reads for calcState through the
// asOfStateReader, so all reads see state at the calculator's txNum.
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

// calcState is the commitment calculator's local state accumulator. It holds the
// current state for every touched account/storage key: values are lazy-loaded
// from the domain on first touch, overwritten by later writes, and fed to the
// trie's Updates buffer at block boundary.
type calcState struct {
	accounts     map[accounts.Address]*calcAccountState
	storageState map[accounts.Address]map[accounts.StorageKey]uint256.Int
	storageDirty map[accounts.Address]map[accounts.StorageKey]bool

	// dirtyAccounts lists the accounts touched this block, so the per-block flush
	// and reset iterate only what changed rather than the whole accounts cache
	// (which grows across the batch). Reset to [:0] by ResetBlockFlags.
	dirtyAccounts []accounts.Address

	// sdSubtree holds addresses self-destructed in the current block. Their
	// persisted storage subtree is dropped via the account update's
	// DeleteStorageSubtree flag and GC'd by the committer — no per-slot enumeration.
	sdSubtree map[accounts.Address]bool

	// fieldMask records whether the block explicitly wrote any account field
	// (bit0=balance, bit1=nonce, bit2=codeHash). EIP-161 removal requires at least
	// one field write, so a storage-only dirty account (present only to refold its
	// storageRoot) is not mistaken for touched-empty.
	fieldMask map[accounts.Address]uint8

	// domainReader provides lazy-load from the domain via asOfStateReader.
	domainReader *calcDomainReader

	// lazyLoadErr is the first error from a lazy-load, sticky so the calculator
	// fails the next compute instead of computing on a missing baseline.
	lazyLoadErr error

	logger    log.Logger
	logPrefix string
}

// LazyLoadErr returns the first lazy-load error, or nil. The calculator must
// check this before computing: a missing baseline yields a wrong trie root that
// is hard to attribute back to the original I/O error.
func (cs *calcState) LazyLoadErr() error { return cs.lazyLoadErr }

func newCalcState(reader *asOfStateReader, logger log.Logger, logPrefix string) *calcState {
	return &calcState{
		accounts:     make(map[accounts.Address]*calcAccountState),
		storageState: make(map[accounts.Address]map[accounts.StorageKey]uint256.Int),
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]bool),
		sdSubtree:    make(map[accounts.Address]bool),
		fieldMask:    make(map[accounts.Address]uint8),
		domainReader: &calcDomainReader{reader: reader},
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

// markDirty flags acc changed this block and records it once in dirtyAccounts.
func (cs *calcState) markDirty(addr accounts.Address, acc *calcAccountState) {
	if acc.dirty {
		return
	}
	acc.dirty = true
	cs.dirtyAccounts = append(cs.dirtyAccounts, addr)
}

// ApplyWrites folds a tx's typed write collections into the local state.
//
// Self-destruct is applied before the field writes: a SELFDESTRUCT marks the
// account Deleted and zeros its fields and storage subtree, then a same-tx
// non-zero field write revives it by clearing Deleted. A zero field write does
// not revive a self-destructed address; for any other address a field write
// (even zero) clears Deleted.
func (cs *calcState) ApplyWrites(writes state.WriteSetView, eip8246 bool) {
	sdThisCall := make(map[accounts.Address]bool)
	for addr, vw := range writes.SelfDestructs() {
		sdThisCall[addr] = vw.Val
		if vw.Val {
			acc := cs.ensureAccount(addr)
			acc.Deleted = true
			cs.markDirty(addr, acc)
			cs.sdSubtree[addr] = true
			cs.zeroTouchedStorage(addr)
		}
	}
	clearsDeleted := func(addr accounts.Address, nonZero bool) bool {
		if !sdThisCall[addr] {
			return true
		}
		// A finally-self-destructed account is not revived by a post-SD balance
		// write: pre-EIP-8246 the balance is burned and the leaf deleted; under
		// EIP-8246 a non-zero balance survives as a balance-only account.
		return eip8246 && nonZero
	}
	for addr, vw := range writes.Balances() {
		acc := cs.ensureAccount(addr)
		acc.Balance = vw.Val
		cs.markDirty(addr, acc)
		cs.fieldMask[addr] |= fieldBal
		if clearsDeleted(addr, !acc.Balance.IsZero()) {
			acc.Deleted = false
		}
	}
	// Nonce/codeHash/code writes never revive a finally-self-destructed account:
	// a CREATE-then-SELFDESTRUCT in one tx leaves the pre-SD nonce/codeHash in the
	// versionMap, and reviving on those would resurrect the destroyed contract. A
	// genuine same-tx recreate ends with SelfDestructPath=false (sdThisCall false),
	// so the writes below correctly revive it.
	for addr, vw := range writes.Nonces() {
		acc := cs.ensureAccount(addr)
		acc.Nonce = vw.Val
		cs.markDirty(addr, acc)
		cs.fieldMask[addr] |= fieldNonce
		if !sdThisCall[addr] {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.CodeHashes() {
		acc := cs.ensureAccount(addr)
		acc.CodeHash = vw.Val.Value()
		cs.markDirty(addr, acc)
		cs.fieldMask[addr] |= fieldCode
		if !sdThisCall[addr] {
			acc.Deleted = false
		}
	}
	// codeHash is single-sourced from CodeHashes() above; Codes() carries only the
	// code-presence signal for clearing Deleted, never the hash, so a view that
	// composes empty code cannot clobber the authoritative codeHash.
	for addr := range writes.Codes() {
		acc := cs.ensureAccount(addr)
		cs.markDirty(addr, acc)
		if !sdThisCall[addr] {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Incarnations() {
		acc := cs.ensureAccount(addr)
		acc.Incarnation = vw.Val
		cs.markDirty(addr, acc)
	}
	for addr, inner := range writes.Storages() {
		// A storage change dirties the account so the commitment refolds its
		// storageRoot, which needs the account key in the update set. ensureAccount
		// lazy-loads the real fields so a storage-only account (no field write) is
		// emitted with an otherwise-unchanged account rather than dropped.
		acc := cs.ensureAccount(addr)
		cs.markDirty(addr, acc)
		// Skip lazy-loading the prior slot value: FlushToUpdates reads exactly the
		// value set below, so the cold GetAsOf seek would be wasted.
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
	// An account still Deleted after the field writes must be all-zero, even
	// though IBS emits the pre-SD Incarnation/Balance values.
	for addr := range sdThisCall {
		if acc, ok := cs.accounts[addr]; ok && acc.Deleted {
			if !eip8246 {
				acc.Balance = uint256.Int{}
			}
			acc.Nonce = 0
			acc.CodeHash = empty.CodeHash
			acc.Incarnation = 0
			// Zero slots left by a same-block SSTORE-then-SELFDESTRUCT (applied
			// after the self-destruct above) so the destroyed account doesn't
			// re-emit its pre-SD slots. A revived account is not Deleted.
			cs.zeroTouchedStorage(addr)
		}
	}
}

// zeroTouchedStorage zeroes and dirties the storage slots this block touched for
// a self-destructed account, so FlushToUpdates emits a DeleteUpdate for each.
// Untouched persisted slots are not enumerated — the trie drops the whole subtree
// via the account update's empty-base signal.
func (cs *calcState) zeroTouchedStorage(addr accounts.Address) {
	slots := cs.storageState[addr]
	dirty := cs.storageDirty[addr]
	if dirty == nil {
		dirty = make(map[accounts.StorageKey]bool)
		cs.storageDirty[addr] = dirty
	}
	for key := range slots {
		slots[key] = uint256.Int{}
		dirty[key] = true
	}
}

// LoadFromBAL populates calcState from an EIP-7928 Block Access List instead of
// the per-tx VersionedWrites stream, feeding each field's block-end value into
// ApplyWrites. The BAL carries no deletion marker, so a touched account whose
// block-end state is empty is reconstructed as a delete here. Storage reads are
// ignored.
func (cs *calcState) LoadFromBAL(blockAccessList types.BlockAccessList, emptyRemoval bool, isAura bool, eip8246 bool) {
	cs.LoadFromBALUpTo(blockAccessList, math.MaxUint32, emptyRemoval, isAura, eip8246)
}

// LoadFromBALUpTo is LoadFromBAL restricted to changes at tx index <= maxTxIndex,
// i.e. the state as of that point in the block, used to fold up to a mid-block
// step boundary. maxTxIndex == math.MaxUint32 is the whole block.
func (cs *calcState) LoadFromBALUpTo(blockAccessList types.BlockAccessList, maxTxIndex uint32, emptyRemoval bool, isAura bool, eip8246 bool) {
	cs.ApplyWrites(bal.ToWriteSet(blockAccessList, maxTxIndex), eip8246)
	cs.ApplyEIP161Removal(emptyRemoval, isAura)
}

// ApplyEIP161Removal marks every touched account whose accumulated block-end
// state is empty (balance 0, nonce 0, empty code) as Deleted, so FlushToUpdates
// emits a leaf-removing DeleteUpdate. Neither the BAL nor the raw-view path
// carries a deletion marker, so both reconstruct it here.
func (cs *calcState) ApplyEIP161Removal(emptyRemoval, isAura bool) {
	for _, addr := range cs.dirtyAccounts {
		acc := cs.accounts[addr]
		if acc.Deleted {
			continue
		}
		// A touched-empty account carries at least one field write. An account
		// dirtied solely by a storage write (fieldMask == 0, present only to refold
		// its storageRoot) is not touched-empty — keep it.
		if cs.fieldMask[addr] == 0 {
			continue
		}
		if acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash && acc.Incarnation == 0 &&
			state.EIP161EmptyRemoval(emptyRemoval, isAura, addr) {
			acc.Deleted = true
			acc.Incarnation = 0
			cs.sdSubtree[addr] = true
		}
	}
}

// FlushToUpdates writes the accumulated dirty state to a commitment.Updates
// buffer. Only keys modified this block are emitted; account updates carry the
// full current state so the trie sees complete values.
func (cs *calcState) FlushToUpdates(updates *commitment.Updates) {
	cs.flushToUpdates(updates)
}

func (cs *calcState) flushToUpdates(updates *commitment.Updates) {
	for _, addr := range cs.dirtyAccounts {
		acc := cs.accounts[addr]
		address := addr.Value()
		key := string(address[:])

		// A Deleted account encodes as a leaf-removing DeleteUpdate only when every
		// field is zero; one that still holds a non-zero balance/nonce/code (or a
		// retained incarnation) keeps its leaf via a regular UPDATE.
		isAllZero := acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash
		var u commitment.Update
		// A deleted account removes its leaf (DeleteUpdate) only when it is fully
		// zero AND holds no retained incarnation; a retained incarnation keeps the
		// leaf via the default zero-valued UPDATE (which, under isAllZero, writes the
		// same zeros).
		switch {
		case acc.Deleted && acc.Incarnation == 0 && isAllZero:
			u = commitment.Update{
				Flags:    commitment.DeleteUpdate,
				CodeHash: empty.CodeHash,
			}
		default:
			u = commitment.Update{
				Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
				Balance:  acc.Balance,
				Nonce:    acc.Nonce,
				CodeHash: acc.CodeHash,
			}
		}
		// Self-destructed this block: drop the old storage subtree and rebuild
		// from only this block's slots.
		if cs.sdSubtree[addr] {
			u.DeleteStorageSubtree = true
		}
		updates.TouchPlainKeyDirect(key, &u)
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

// ResetBlockFlags clears the per-block dirty flags while keeping the accumulated
// state values, preparing for the next block.
func (cs *calcState) ResetBlockFlags() {
	for _, addr := range cs.dirtyAccounts {
		cs.accounts[addr].dirty = false
	}
	cs.dirtyAccounts = cs.dirtyAccounts[:0]
	for addr := range cs.storageDirty {
		delete(cs.storageDirty, addr)
	}
	clear(cs.sdSubtree)
	clear(cs.fieldMask)
}
