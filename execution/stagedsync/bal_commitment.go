package stagedsync

import (
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// balToUpdates reduces a block's EIP-7928 Block Access List to its block-final
// write-set and flushes it into dst. It replays each key's highest-Index value
// through a calcState and reuses calcState.FlushToUpdates, so the emitted
// Updates are identical in shape to the post-execution txResult path. reader
// must be pinned to the block's pre-state: unchanged account fields and trie
// fold siblings are loaded from it.
//
// Returns fallback=true when the block would EIP-161-delete a pre-existing
// empty account — a removal the BAL does not encode (the account appears with
// net-zero changes, or, like SYSTEM_ADDRESS, is excluded from the BAL
// entirely). On target Amsterdam chains no such account exists (§6.3), so this
// only fires on synthetic test/devnet state; the caller then drives commitment
// from the txResult path for that block.
func balToUpdates(bal types.BlockAccessList, reader calcStateReader, dst *commitment.Updates, logger log.Logger, logPrefix string) (fallback bool, err error) {
	cs, fallback, err := balToCalcState(bal, reader, logger, logPrefix)
	if err != nil || fallback {
		return fallback, err
	}
	cs.FlushToUpdates(dst)
	return false, nil
}

// balToCalcState reduces the BAL into a calcState holding the block-final
// account/storage values. The same calcState backs both the Updates buffer
// (FlushToUpdates) and the fold's state reader (balFoldReader), so the trie's
// account loads during storage-root recomputation see the same final values as
// the buffered updates — matching the txResult path's post-state reads.
func balToCalcState(bal types.BlockAccessList, reader calcStateReader, logger log.Logger, logPrefix string) (cs *calcState, fallback bool, err error) {
	cs = newCalcState(reader, logger, logPrefix)
	for _, ac := range bal {
		addr := ac.Address
		for _, sc := range ac.StorageChanges {
			ch := latestStorageChange(sc.Changes)
			if ch == nil {
				continue
			}
			cs.setBALStorage(addr, sc.Slot, ch.Value)
		}
		bc := latestBalanceChange(ac.BalanceChanges)
		nc := latestNonceChange(ac.NonceChanges)
		cc := latestCodeChange(ac.CodeChanges)
		deletes, err := cs.touchedEmptyDeletion(addr, bc, nc, cc)
		if err != nil {
			return nil, false, err
		}
		if deletes {
			return nil, true, nil
		}
		if bc == nil && nc == nil && cc == nil {
			// Storage-only or pure-read account: no account leaf touch,
			// mirroring calcState (StoragePath never marks the account dirty).
			// The fold still re-roots the account from its storage updates and
			// loads its unchanged fields through balFoldReader, which returns
			// the pre-state (== block-final, since the fields didn't change).
			continue
		}
		acc := cs.ensureAccount(addr)
		if bc != nil {
			acc.Balance = bc.Value
		}
		if nc != nil {
			acc.Nonce = nc.Value
		}
		if cc != nil {
			acc.CodeHash = crypto.Keccak256Hash(cc.Bytecode)
		}
		acc.dirty = true
	}
	sysEmpty, err := cs.presentAndEmpty(params.SystemAddress)
	if err != nil {
		return nil, false, err
	}
	if sysEmpty {
		return nil, true, nil
	}
	if lazyErr := cs.LazyLoadErr(); lazyErr != nil {
		return nil, false, lazyErr
	}
	return cs, false, nil
}

// encodeFinalAccount serializes a dirty account's block-final state for the
// fold's state reader, or returns nil when the account is removed/empty. The
// MPT account leaf ignores incarnation, so it is left at zero.
func (cs *calcState) encodeFinalAccount(acc *calcAccountState) []byte {
	if acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash {
		return nil
	}
	a := &accounts.Account{
		Nonce:    acc.Nonce,
		Balance:  acc.Balance,
		CodeHash: accounts.InternCodeHash(acc.CodeHash),
		Root:     empty.RootHash,
	}
	return accounts.SerialiseV3(a)
}

// touchedEmptyDeletion reports whether a BAL-touched account would be removed
// by EIP-161: present in the pre-state with a block-final {0,0,no-code} state.
// bc/nc/cc are the account's block-final changes (nil when absent). It reads
// the pre-state only when the final state could be empty.
func (cs *calcState) touchedEmptyDeletion(addr accounts.Address, bc *types.BalanceChange, nc *types.NonceChange, cc *types.CodeChange) (bool, error) {
	if (bc != nil && !bc.Value.IsZero()) || (nc != nil && nc.Value != 0) || (cc != nil && len(cc.Bytecode) > 0) {
		return false, nil
	}
	dbAcc, err := cs.readAccount(addr)
	if err != nil {
		return false, err
	}
	if dbAcc == nil {
		return false, nil
	}
	finalBalanceZero := dbAcc.Balance.IsZero()
	if bc != nil {
		finalBalanceZero = bc.Value.IsZero()
	}
	finalNonceZero := dbAcc.Nonce == 0
	if nc != nil {
		finalNonceZero = nc.Value == 0
	}
	finalCodeEmpty := dbAcc.IsEmptyCodeHash()
	if cc != nil {
		finalCodeEmpty = len(cc.Bytecode) == 0
	}
	return finalBalanceZero && finalNonceZero && finalCodeEmpty, nil
}

// presentAndEmpty reports whether addr exists in the pre-state and is empty.
func (cs *calcState) presentAndEmpty(addr accounts.Address) (bool, error) {
	dbAcc, err := cs.readAccount(addr)
	if err != nil {
		return false, err
	}
	return dbAcc != nil && dbAcc.Empty(), nil
}

func (cs *calcState) readAccount(addr accounts.Address) (*accounts.Account, error) {
	if cs.domainReader == nil {
		return nil, nil
	}
	return cs.domainReader.ReadAccountData(addr)
}

// latestStorageChange returns the highest-Index (block-final) change, or nil.
func latestStorageChange(changes []*types.StorageChange) *types.StorageChange {
	var out *types.StorageChange
	for _, c := range changes {
		if out == nil || c.Index >= out.Index {
			out = c
		}
	}
	return out
}

func latestBalanceChange(changes []*types.BalanceChange) *types.BalanceChange {
	var out *types.BalanceChange
	for _, c := range changes {
		if out == nil || c.Index >= out.Index {
			out = c
		}
	}
	return out
}

func latestNonceChange(changes []*types.NonceChange) *types.NonceChange {
	var out *types.NonceChange
	for _, c := range changes {
		if out == nil || c.Index >= out.Index {
			out = c
		}
	}
	return out
}

func latestCodeChange(changes []*types.CodeChange) *types.CodeChange {
	var out *types.CodeChange
	for _, c := range changes {
		if out == nil || c.Index >= out.Index {
			out = c
		}
	}
	return out
}
