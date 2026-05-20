package stagedsync

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
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

	// lazyLoadErr captures the first error encountered during ensureAccount /
	// ensureStorage. Sticky — never cleared — so the calculator can fail the
	// next compute instead of silently producing wrong updates from a missing
	// baseline. Surface via LazyLoadErr().
	lazyLoadErr error

	logger    log.Logger
	logPrefix string
}

// LazyLoadErr returns the first error encountered during ensureAccount /
// ensureStorage lazy-loads, or nil. The calculator must check this before
// computing — a missing baseline yields a wrong trie root that is hard to
// attribute back to the original I/O error.
func (cs *calcState) LazyLoadErr() error { return cs.lazyLoadErr }

func newCalcState(reader *asOfStateReader, logger log.Logger, logPrefix string) *calcState {
	return &calcState{
		accounts:     make(map[accounts.Address]*calcAccountState),
		storageState: make(map[accounts.Address]map[accounts.StorageKey]uint256.Int),
		storageDirty: make(map[accounts.Address]map[accounts.StorageKey]bool),
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

// ensureStorage returns the storage value, lazy-loading from domain on first touch.
func (cs *calcState) ensureStorage(addr accounts.Address, key accounts.StorageKey) uint256.Int {
	slots, ok := cs.storageState[addr]
	if !ok {
		slots = make(map[accounts.StorageKey]uint256.Int)
		cs.storageState[addr] = slots
	}
	if val, ok := slots[key]; ok {
		return val
	}

	var val uint256.Int
	if cs.domainReader != nil {
		v, found, err := cs.domainReader.ReadAccountStorage(addr, key)
		if err != nil {
			// See ensureAccount: sticky so the next compute fails fast.
			if cs.lazyLoadErr == nil {
				cs.lazyLoadErr = fmt.Errorf("ensureStorage(%x/%x): %w", addr.Value(), key.Value(), err)
			}
			if cs.logger != nil {
				cs.logger.Warn("["+cs.logPrefix+"] commitmentCalculator: lazy-load ReadAccountStorage failed", "addr", addr, "key", key, "err", err)
			}
		} else if found {
			val = v
		}
	}
	slots[key] = val
	return val
}

// ApplyWrites updates the local state with all writes from a TX result.
//
// Two semantic invariants matter for SD-of-pre-existing-contract:
//
// (1) IBS.Selfdestruct emits three versionWritten entries (IncarnationPath
//
//	= preInc, SelfDestructPath=true, BalancePath=0). Without care, the
//	trailing BalancePath=0 in the writeset would clobber Deleted=true
//	via the unconditional `acc.Deleted = false` reset that
//	BalancePath/NoncePath/CodeHashPath/CodePath cases use to reflect
//	a re-creation. Fix: those cases only clear Deleted when the value
//	is non-zero/non-empty. A zero-value write that arrives after SD is
//	part of the SD's own emission and must not undo Deleted=true.
//
// (2) When SelfDestructPath=true is processed, zero Balance/Nonce/
//
//	CodeHash/Incarnation. Without zeroing, lazy-loaded pre-SD values
//	survive in cs.accounts and FlushToUpdates routes into the default
//	regular-UPDATE branch (emitting pre-SD nonce/codeHash) instead of
//	the EIP-161 DeleteUpdate branch — which is what serial's
//	DomainDel produces for a pure SD (leaf removed). Storage slots
//	under the SD'd address must also zero out: vm.StorageKeys only
//	returns slots written in the current tx's version map and SD via
//	Selfdestruct() doesn't write storage explicitly, so without
//	zeroing here, FlushToUpdates emits StorageUpdate with pre-SD
//	values and leaves stale storage in the trie (TestRecreateAndRewind
//	block 4 recreate sees stale storage).
//
// Ordering invariant that (1) relies on: when a single tx
// self-destructs an address and then re-creates it (CREATE2 to the same
// address — pre-Cancun pattern), IBS emits the SELFDESTRUCT-time writes
// (SelfDestructPath=true, BalancePath=0, IncarnationPath=preInc) BEFORE
// the recreate-time writes (BalancePath=newBal, NoncePath=1, CodeHash=…),
// because the EVM runs the opcodes in that order and versionWritten fires
// at opcode time. So the recreate's non-zero Balance/Nonce/CodeHash
// re-clear acc.Deleted after the SD case set it. For SD-only (no recreate)
// the post-SD BalancePath=0 arrives but is zero, so it does NOT re-clear.
// For fresh creates (no prior SD in this writeset) acc.Deleted starts
// false and the conditional is a no-op. (For per-block writesets the calc
// also relies on this — but blocks aren't single txs; the SD-then-recreate
// pattern there spans txs, and last-write-wins on acc.Deleted still holds.)
func (cs *calcState) ApplyWrites(writes state.VersionedWrites) {
	// Pre-scan: which addresses are self-destructed BY THIS tx's writeset.
	// For those, the trailing zero account-field writes that IBS emits as
	// part of the SELFDESTRUCT (BalancePath=0, etc. — when not stripped by
	// normalizeWriteSet) must NOT clear Deleted. For any OTHER address, an
	// account-field write — even a zero one — means the address is alive at
	// the end of this tx (e.g. a 0-value transfer that re-creates a
	// previously-destroyed address as an empty account on a pre-EIP-161
	// fork — EEST frontier/opcodes/test_double_kill under EXEC3_PARALLEL),
	// so it must clear Deleted.
	sdThisCall := make(map[accounts.Address]bool)
	for _, w := range writes {
		if w.Path == state.SelfDestructPath {
			if destructed := w.ValBool; true {
				// Last SelfDestructPath entry wins (matches normalizeWriteSet /
				// applyVersionedWrites): a SELFDESTRUCT followed by a same-tx
				// CREATE2-recreate ends ALIVE, so the recreate's writes must
				// clear Deleted.
				sdThisCall[w.Address] = destructed
			}
		}
	}
	clearsDeleted := func(addr accounts.Address, nonZero bool) bool {
		return nonZero || !sdThisCall[addr]
	}
	for _, w := range writes {
		if false {
			continue
		}

		switch w.Path {
		case state.BalancePath:
			acc := cs.ensureAccount(w.Address)
			acc.Balance = w.ValU256
			acc.dirty = true
			if clearsDeleted(w.Address, !acc.Balance.IsZero()) {
				acc.Deleted = false
			}
		case state.NoncePath:
			acc := cs.ensureAccount(w.Address)
			acc.Nonce = w.ValU64
			acc.dirty = true
			if clearsDeleted(w.Address, acc.Nonce != 0) {
				acc.Deleted = false
			}
		case state.CodeHashPath:
			acc := cs.ensureAccount(w.Address)
			v := w.ValHash
			acc.CodeHash = v.Value()
			acc.dirty = true
			if clearsDeleted(w.Address, v.Value() != empty.CodeHash) {
				acc.Deleted = false
			}
		case state.CodePath:
			acc := cs.ensureAccount(w.Address)
			code := w.ValBytes
			acc.CodeHash = crypto.Keccak256Hash(code)
			acc.dirty = true
			if clearsDeleted(w.Address, len(code) > 0) {
				acc.Deleted = false
			}
		case state.SelfDestructPath:
			if destructed := w.ValBool; destructed {
				acc := cs.ensureAccount(w.Address)
				acc.Deleted = true
				acc.dirty = true
				// Invariant 2: zero account fields so FlushToUpdates
				// routes into the EIP-161 DeleteUpdate branch.
				acc.Balance = uint256.Int{}
				acc.Nonce = 0
				acc.CodeHash = empty.CodeHash
				acc.Incarnation = 0
				// Zero every tracked storage slot and mark dirty so
				// FlushToUpdates emits DeleteUpdate per slot.
				if slots, ok := cs.storageState[w.Address]; ok {
					if cs.storageDirty[w.Address] == nil {
						cs.storageDirty[w.Address] = make(map[accounts.StorageKey]bool)
					}
					for key := range slots {
						slots[key] = uint256.Int{}
						cs.storageDirty[w.Address][key] = true
					}
				}
			}
		case state.StoragePath:
			v := w.ValU256
			// The previous slot value is irrelevant here: the next line
			// overwrites it with the EVM-write value, and the only
			// downstream consumer (FlushToUpdates) reads exactly the
			// value we set. Skip the ensureStorage lazy-load — it
			// triggers a cold .ef GetAsOf seek per first-touched slot
			// (~5,910 wasted seeks per SSTORE-bloat block) and discards
			// the loaded value. Initialize just the inner map.
			slots := cs.storageState[w.Address]
			if slots == nil {
				slots = make(map[accounts.StorageKey]uint256.Int)
				cs.storageState[w.Address] = slots
			}
			slots[w.Key] = v
			if cs.storageDirty[w.Address] == nil {
				cs.storageDirty[w.Address] = make(map[accounts.StorageKey]bool)
			}
			cs.storageDirty[w.Address][w.Key] = true
		case state.IncarnationPath:
			acc := cs.ensureAccount(w.Address)
			acc.Incarnation = w.ValU64
			acc.dirty = true
		}
	}
}

// finalChange returns the change carrying the highest tx Index from a BAL
// per-field / per-slot change list — i.e. the block-end value, since the
// BAL stores changes tx-indexed and the highest index is the last write in
// the block. Scans for the max rather than assuming the list is sorted.
// Returns (zero, false) for an empty list.
func finalChange[T interface{ GetIndex() uint32 }](changes []T) (T, bool) {
	var best T
	var bestIdx uint32
	found := false
	for _, c := range changes {
		if idx := c.GetIndex(); !found || idx >= bestIdx {
			best, bestIdx, found = c, idx, true
		}
	}
	return best, found
}

// LoadFromBAL populates calcState from an EIP-7928 Block Access List
// instead of the per-tx VersionedWrites stream. The BAL declares the
// block's post-state up front, so the commitment calculator can build the
// trie without waiting for execution to stream writes tx-by-tx.
//
// For each touched account it takes the block-end value per field — the
// highest-tx-indexed change — and feeds the existing ApplyWrites, so the
// SELFDESTRUCT / Deleted / EIP-161 routing is reused unchanged rather than
// reimplemented.
//
// Storage *reads* are ignored: commitment only consumes the changed
// (dirty) set. NOT yet modelled — the BAL carries no explicit
// SelfDestructPath or incarnation field, so blocks exercising account
// deletion or fresh-contract incarnation diverge from the incremental
// path; that divergence is the differential test's job to surface and is
// a tracked Stage-1 follow-up.
func (cs *calcState) LoadFromBAL(bal types.BlockAccessList) {
	var writes state.VersionedWrites
	for _, ac := range bal {
		addr := ac.Address
		if bc, ok := finalChange(ac.BalanceChanges); ok {
			writes = append(writes, &state.VersionedWrite{
				Address: addr, Path: state.BalancePath, ValU256: bc.Value,
			})
		}
		if nc, ok := finalChange(ac.NonceChanges); ok {
			writes = append(writes, &state.VersionedWrite{
				Address: addr, Path: state.NoncePath, ValU64: nc.Value,
			})
		}
		if cc, ok := finalChange(ac.CodeChanges); ok {
			writes = append(writes, &state.VersionedWrite{
				Address: addr, Path: state.CodePath, ValBytes: cc.Bytecode,
			})
		}
		for _, sc := range ac.StorageChanges {
			if chg, ok := finalChange(sc.Changes); ok {
				writes = append(writes, &state.VersionedWrite{
					Address: addr, Path: state.StoragePath, Key: sc.Slot, ValU256: chg.Value,
				})
			}
		}
	}
	cs.ApplyWrites(writes)
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

		// Three flavours of "Deleted" writeset, distinguished by whether
		// the account fields actually became zero:
		//   1. (Currently unreachable from production writesets — defensive
		//      only.) SD-of-pre-existing-contract with incarnation > 0
		//      retained: ApplyWrites' SelfDestructPath case now zeros
		//      Incarnation along with Balance/Nonce/CodeHash, so SD always
		//      lands in case 2 below. This branch stays as a safety net for
		//      hand-built writesets and future ApplyWrites changes that
		//      might preserve incarnation; if it fires, emit a zero-account
		//      UPDATE (matches serial's post-DomainDel encoding when the
		//      serialised account retains a non-zero incarnation).
		//   2. SD / EIP-161 emptyRemoval: all fields zero (incarnation too).
		//      Serial's DomainDel removes the leaf. Emit DeleteUpdate.
		//   3. Deleted-but-not-empty (defense-in-depth): if the writeset
		//      has SelfDestructPath=true but balance/nonce/code retain
		//      non-zero values (e.g. OOG-during-CREATE2 with retained
		//      value transfer per EIP-1014, or any write-ordering race
		//      where BalancePath arrives before SelfDestructPath), serial
		//      does NOT remove the leaf — it stays as the actual
		//      balance-only or fully-populated account. Emit a regular
		//      UPDATE with the actual values rather than zeroing them.
		isAllZero := acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash
		switch {
		case acc.Deleted && acc.Incarnation > 0 && isAllZero:
			updates.TouchPlainKeyDirect(key, commitment.Update{
				Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
				Balance:  uint256.Int{},
				Nonce:    0,
				CodeHash: empty.CodeHash,
			})
		case acc.Deleted && isAllZero:
			updates.TouchPlainKeyDirect(key, commitment.Update{
				Flags:    commitment.DeleteUpdate,
				CodeHash: empty.CodeHash,
			})
		default:
			// Either not Deleted, or Deleted-with-retained-values.
			updates.TouchPlainKeyDirect(key, commitment.Update{
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
			updates.TouchPlainKeyDirect(string(composite), u)
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
