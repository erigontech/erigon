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
// IBS.Selfdestruct emits THREE versionWritten entries in this order:
// IncarnationPath, SelfDestructPath, BalancePath=0. If we apply them in
// arrival order, BalancePath's `acc.Deleted = false` reset clobbers the
// `Deleted=true` flag set by SelfDestructPath, leaving the calculator's
// phoenix entry in the wrong state at flush time. Apply the SD case in a
// second pass so SD wins regardless of order in the writeset (matches
// serial's IBS finalization where DeleteAccount is the terminal call).
func (cs *calcState) ApplyWrites(writes state.VersionedWrites) {
	// First pass: account-field, storage, code, etc. writes.
	for _, w := range writes {
		if w.Val == nil {
			continue
		}
		if w.Path == state.SelfDestructPath {
			continue
		}

		switch w.Path {
		case state.BalancePath:
			acc := cs.ensureAccount(w.Address)
			acc.Balance = w.Val.(uint256.Int)
			acc.dirty = true
			acc.Deleted = false
		case state.NoncePath:
			acc := cs.ensureAccount(w.Address)
			acc.Nonce = w.Val.(uint64)
			acc.dirty = true
			acc.Deleted = false
		case state.CodeHashPath:
			acc := cs.ensureAccount(w.Address)
			v := w.Val.(accounts.CodeHash)
			acc.CodeHash = v.Value()
			acc.dirty = true
			acc.Deleted = false
		case state.CodePath:
			acc := cs.ensureAccount(w.Address)
			code := w.Val.([]byte)
			acc.CodeHash = crypto.Keccak256Hash(code)
			acc.dirty = true
			acc.Deleted = false
		// SelfDestructPath handled in second pass — see ApplyWrites docstring.
		case state.StoragePath:
			v := w.Val.(uint256.Int)
			cs.ensureStorage(w.Address, w.Key) // lazy-load if needed
			cs.storageState[w.Address][w.Key] = v
			if cs.storageDirty[w.Address] == nil {
				cs.storageDirty[w.Address] = make(map[accounts.StorageKey]bool)
			}
			cs.storageDirty[w.Address][w.Key] = true
		case state.IncarnationPath:
			// Carries the pre-deletion incarnation when emitted by
			// LightCollector.DeleteAccount alongside SelfDestructPath=true.
			// Used by FlushToUpdates to differentiate self-destruct of a
			// pre-existing contract from EIP-161 emptyRemoval. Direct
			// type-assertion (panic on mismatch) matches the other cases
			// in this function — silently zero-ing Incarnation here would
			// route a real SD into the EIP-161 DeleteUpdate branch and
			// reproduce the very wrong-root bug this PR fixes.
			acc := cs.ensureAccount(w.Address)
			acc.Incarnation = w.Val.(uint64)
			acc.dirty = true
		}
	}

	// Second pass: SelfDestructPath. Applied last so SD always wins over
	// the BalancePath=0 / NoncePath=0 / CodeHashPath=empty writes that
	// IBS.Selfdestruct emits alongside it. See ApplyWrites docstring.
	for _, w := range writes {
		if w.Val == nil || w.Path != state.SelfDestructPath {
			continue
		}
		destructed, ok := w.Val.(bool)
		if !ok || !destructed {
			continue
		}
		acc := cs.ensureAccount(w.Address)
		acc.Deleted = true
		acc.dirty = true
		// Zero ALL fields so FlushToUpdates routes into the
		// `acc.Deleted && isAllZero` DeleteUpdate branch (matching
		// serial's DomainDel behavior). Incarnation also zeroed: a
		// pure SD without recreate emits DeleteUpdate; a recreate
		// (CREATE2 in the same tx) writes a new IncarnationPath
		// AFTER this pass, but recreate writes don't go through this
		// SelfDestructPath case — they come via UpdateAccountData
		// emissions which are handled in the first pass.
		acc.Balance = uint256.Int{}
		acc.Nonce = 0
		acc.CodeHash = empty.CodeHash
		acc.Incarnation = 0
		// Zero every tracked storage slot AND mark dirty so
		// FlushToUpdates emits DeleteUpdate per slot. The previous
		// approach assumed normalizeWriteSet's StoragePath=0 entries
		// (via vm.StorageKeys) would arrive in the same writeset and
		// overwrite cs.storageState's pre-SD values — but vm.StorageKeys
		// only returns slots written in the current tx's version map,
		// and SD via Selfdestruct() doesn't write storage explicitly.
		// Without zeroing here, FlushToUpdates emits StorageUpdate with
		// pre-SD values, leaving stale storage in the trie and producing
		// wrong roots in TestRecreateAndRewind block 4 (recreate sees
		// stale storage). Match serial's DomainDelPrefix behavior of
		// removing every slot under the SD'd account.
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
		//   1. SD of a pre-existing contract: SD zeroes balance (sent to
		//      beneficiary) and incarnation > 0 in writeset. Serial's
		//      DomainDel emits the post-SD encoding (zero fields, leaf
		//      survives because incarnation is preserved in the
		//      serialised account). Emit zero-account UPDATE.
		//   2. EIP-161 emptyRemoval of a touched-empty EOA-shaped account:
		//      all fields zero, incarnation also zero. Serial's DomainDel
		//      emits truly empty bytes. Emit DeleteUpdate.
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
