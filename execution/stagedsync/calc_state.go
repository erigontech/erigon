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
	Balance  uint256.Int
	Nonce    uint64
	CodeHash [32]byte
	Deleted  bool
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
func (cs *calcState) ApplyWrites(writes state.VersionedWrites) {
	for _, w := range writes {
		if w.Val == nil {
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
		case state.SelfDestructPath:
			if destructed, ok := w.Val.(bool); ok && destructed {
				acc := cs.ensureAccount(w.Address)
				acc.Deleted = true
				acc.dirty = true
				// Clear accumulated storage for this account
				delete(cs.storageState, w.Address)
				delete(cs.storageDirty, w.Address)
			}
		case state.StoragePath:
			v := w.Val.(uint256.Int)
			cs.ensureStorage(w.Address, w.Key) // lazy-load if needed
			cs.storageState[w.Address][w.Key] = v
			if cs.storageDirty[w.Address] == nil {
				cs.storageDirty[w.Address] = make(map[accounts.StorageKey]bool)
			}
			cs.storageDirty[w.Address][w.Key] = true
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

		if acc.Deleted {
			updates.TouchPlainKeyDirect(key, &commitment.Update{
				Flags:    commitment.DeleteUpdate,
				CodeHash: empty.CodeHash,
			})
		} else {
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
