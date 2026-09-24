package stagedsync

import (
	"fmt"
	"math"
	"slices"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
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
	fed   bool
	hash  [32]byte
}

type calcSlot struct {
	value uint256.Int
	hash  [32]byte
}

type calcStorage struct {
	hash  [32]byte
	slots map[accounts.StorageKey]calcSlot
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
	accounts      map[accounts.Address]*calcAccountState
	dirtyAccounts []accounts.Address
	// storageState holds the accumulated value for each slot
	storageState map[accounts.Address]*calcStorage
	// storageDirty tracks which slots were modified in the current block
	storageDirty map[accounts.Address]map[accounts.StorageKey]bool

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

	feedUpdates []commitment.Update
	feedSlots   []commitment.FeedSlot
	feedValues  []byte

	prefetch *branchPrefetcher
}

// LazyLoadErr returns the first error encountered during ensureAccount
// lazy-loads, or nil. The calculator must check this before
// computing — a missing baseline yields a wrong trie root that is hard to
// attribute back to the original I/O error.
func (cs *calcState) LazyLoadErr() error { return cs.lazyLoadErr }

func newCalcState(reader *asOfStateReader, logger log.Logger, logPrefix string) *calcState {
	return &calcState{
		accounts:     make(map[accounts.Address]*calcAccountState),
		storageState: make(map[accounts.Address]*calcStorage),
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]bool),
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

	address := addr.Value()
	acc := &calcAccountState{
		CodeHash: empty.CodeHash,
		hash:     keccak.Sum256(address[:]),
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

func (cs *calcState) markDirty(addr accounts.Address, acc *calcAccountState) {
	if acc.dirty {
		return
	}
	acc.dirty = true
	cs.dirtyAccounts = append(cs.dirtyAccounts, addr)
	cs.prefetch.add(prefetchItem{account: acc.hash})
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
			acc := cs.ensureAccount(addr, writes)
			acc.Deleted = true
			cs.markDirty(addr, acc)
			cs.deleteStorageSubtree(addr)
		}
	}
	clearsDeleted := func(addr accounts.Address, nonZero bool) bool {
		return nonZero || !sdThisCall[addr]
	}
	for addr, vw := range writes.Balances() {
		acc := cs.ensureAccount(addr, writes)
		acc.Balance = vw.Val
		cs.markDirty(addr, acc)
		if clearsDeleted(addr, !acc.Balance.IsZero()) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Nonces() {
		acc := cs.ensureAccount(addr, writes)
		acc.Nonce = vw.Val
		cs.markDirty(addr, acc)
		if clearsDeleted(addr, acc.Nonce != 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.CodeHashes() {
		acc := cs.ensureAccount(addr, writes)
		acc.CodeHash = vw.Val.Value()
		cs.markDirty(addr, acc)
		if clearsDeleted(addr, vw.Val.Value() != empty.CodeHash) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Codes() {
		acc := cs.ensureAccount(addr, writes)
		acc.CodeHash = vw.Val.Hash.Value()
		cs.markDirty(addr, acc)
		if clearsDeleted(addr, vw.Val.Len() > 0) {
			acc.Deleted = false
		}
	}
	for addr, vw := range writes.Incarnations() {
		acc := cs.ensureAccount(addr, writes)
		acc.Incarnation = vw.Val
		cs.markDirty(addr, acc)
	}
	for addr, inner := range writes.Storages() {
		// Skip lazy-loading the prior slot value: the only downstream consumer
		// (FlushToUpdates) reads exactly the value set below, so the cold
		// GetAsOf seek it would cost is wasted.
		st := cs.storageState[addr]
		if st == nil {
			address := addr.Value()
			st = &calcStorage{hash: keccak.Sum256(address[:]), slots: make(map[accounts.StorageKey]calcSlot)}
			cs.storageState[addr] = st
		}
		dirty := cs.storageDirty[addr]
		if dirty == nil {
			dirty = make(map[accounts.StorageKey]bool)
			cs.storageDirty[addr] = dirty
			cs.prefetch.add(prefetchItem{account: st.hash})
		}
		for key, vw := range inner {
			slot, ok := st.slots[key]
			if !ok {
				k := key.Value()
				slot.hash = keccak.Sum256(k[:])
			}
			slot.value = vw.Val
			st.slots[key] = slot
			if !dirty[key] {
				dirty[key] = true
				cs.prefetch.add(prefetchItem{account: st.hash, slot: slot.hash, storage: true})
			}
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

// deleteStorageSubtree handles a self-destructed account's storage. Only slots
// touched this window (already in the maps) get explicit deletes; the account's
// own DeleteUpdate collapses the rest of the subtree, so untouched on-disk slots
// need not be read.
func (cs *calcState) deleteStorageSubtree(addr accounts.Address) {
	st := cs.storageState[addr]
	if st == nil || len(st.slots) == 0 {
		return
	}
	dirty := cs.storageDirty[addr]
	if dirty == nil {
		dirty = make(map[accounts.StorageKey]bool)
		cs.storageDirty[addr] = dirty
	}
	for key, slot := range st.slots {
		slot.value = uint256.Int{}
		st.slots[key] = slot
		dirty[key] = true
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
		addr := accounts.InternAddress(ac.Address)
		acc := cs.accounts[addr]
		if acc == nil || !acc.dirty || acc.Deleted {
			continue
		}
		if acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash &&
			state.EIP161EmptyRemoval(emptyRemoval, isAura, addr) {
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
	n := len(cs.dirtyAccounts)
	for _, dirtySlots := range cs.storageDirty {
		n += len(dirtySlots)
	}
	updates.Grow(n)
	for _, addr := range cs.dirtyAccounts {
		address := addr.Value()
		update := accountUpdateOf(cs.accounts[addr])
		updates.TouchPlainKeyUnique(string(address[:]), &update)
	}

	for addr, dirtySlots := range cs.storageDirty {
		address := addr.Value()
		slots := cs.storageState[addr]
		for key := range dirtySlots {
			val := slots.slots[key].value
			keyVal := key.Value()
			var composite [20 + 32]byte
			copy(composite[:], address[:])
			copy(composite[20:], keyVal[:])

			var u commitment.Update
			if n := val.ByteLen(); n == 0 {
				u.Flags = commitment.DeleteUpdate
			} else {
				u.Flags = commitment.StorageUpdate
				u.StorageLen = int8(n)
				val.WriteToSlice(u.Storage[:n])
			}
			updates.TouchPlainKeyUnique(string(composite[:]), &u)
		}
	}
}

func (cs *calcState) FlushToFeed(feed *commitment.Feed) {
	slots := 0
	for _, dirty := range cs.storageDirty {
		slots += len(dirty)
	}
	feed.Keys = len(cs.dirtyAccounts) + slots
	feed.Accounts = slices.Grow(feed.Accounts[:0], len(cs.dirtyAccounts)+len(cs.storageDirty))
	cs.feedUpdates = slices.Grow(cs.feedUpdates[:0], len(cs.dirtyAccounts))
	cs.feedSlots = slices.Grow(cs.feedSlots[:0], slots)
	cs.feedValues = slices.Grow(cs.feedValues[:0], length.Hash*slots)
	for addr, dirty := range cs.storageDirty {
		if len(dirty) == 0 {
			continue
		}
		st := cs.storageState[addr]
		start := len(cs.feedSlots)
		for key := range dirty {
			slot := st.slots[key]
			at, n := len(cs.feedValues), slot.value.ByteLen()
			cs.feedValues = cs.feedValues[:at+n]
			slot.value.WriteToSlice(cs.feedValues[at:])
			cs.feedSlots = append(cs.feedSlots, commitment.FeedSlot{Hash: slot.hash, Value: cs.feedValues[at : at+n : at+n]})
		}
		account := commitment.FeedAccount{Hash: st.hash, Slots: cs.feedSlots[start:len(cs.feedSlots):len(cs.feedSlots)]}
		if acc := cs.accounts[addr]; acc != nil && acc.dirty {
			account.Update = cs.feedUpdate(acc)
			acc.fed = true
		}
		feed.Accounts = append(feed.Accounts, account)
	}
	for _, addr := range cs.dirtyAccounts {
		acc := cs.accounts[addr]
		if acc.fed {
			acc.fed = false
			continue
		}
		feed.Accounts = append(feed.Accounts, commitment.FeedAccount{Hash: acc.hash, Update: cs.feedUpdate(acc)})
	}
}

func (cs *calcState) feedUpdate(acc *calcAccountState) *commitment.Update {
	cs.feedUpdates = append(cs.feedUpdates, accountUpdateOf(acc))
	return &cs.feedUpdates[len(cs.feedUpdates)-1]
}

func accountUpdateOf(acc *calcAccountState) commitment.Update {
	// A "Deleted" account only encodes as serial's leaf-removing DeleteUpdate
	// when every field is actually zero; a Deleted account that still holds a
	// non-zero balance/nonce/code (or a retained incarnation) keeps its leaf,
	// so emit a regular UPDATE with the real values instead.
	isAllZero := acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash
	switch {
	case acc.Deleted && acc.Incarnation > 0 && isAllZero:
		return commitment.Update{
			Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
			CodeHash: empty.CodeHash,
		}
	case acc.Deleted && isAllZero:
		return commitment.Update{
			Flags:    commitment.DeleteUpdate,
			CodeHash: empty.CodeHash,
		}
	}
	return commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Balance:  acc.Balance,
		Nonce:    acc.Nonce,
		CodeHash: acc.CodeHash,
	}
}

// ResetBlockFlags clears the per-block dirty flags while keeping the
// accumulated state values. Called after commitment computation to
// prepare for the next block.
func (cs *calcState) ResetBlockFlags() {
	for _, addr := range cs.dirtyAccounts {
		cs.accounts[addr].dirty = false
	}
	cs.dirtyAccounts = cs.dirtyAccounts[:0]
	for addr := range cs.storageDirty {
		delete(cs.storageDirty, addr)
	}
}
