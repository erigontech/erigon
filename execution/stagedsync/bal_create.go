package stagedsync

import (
	"bytes"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

func CreateBAL(blockNum uint64, txIO *state.VersionedIO) types.BlockAccessList {
	ac := make(map[common.Address]*accountState)
	maxTxIndex := len(txIO.Inputs()) - 1

	for txIndex := -1; txIndex <= maxTxIndex; txIndex++ {
		accessIndex := blockAccessIndex(txIndex)

		// update reads
		txIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
			account := ensureAccountState(ac, vr.Address)
			updateAccountRead(account, vr, accessIndex)
			return true
		})

		// update writes
		for _, vw := range txIO.WriteSet(txIndex) {
			account := ensureAccountState(ac, vw.Address)
			updateAccountWrite(account, vw, accessIndex)
		}
	}

	// construct BAL from map
	bal := make([]*types.AccountChanges, 0)
	for _, account := range ac {
		account.finalize()
		bal = append(bal, account.changes)
	}

	// order everything as per EIP

	// Convert to slice of values for proper logging
	balValues := make([]types.AccountChanges, len(bal))
	for i, changes := range bal {
		balValues[i] = *changes
	}
	log.Info("BAL", "blockNum", blockNum, "accounts", len(bal), "raw", balValues)

	return bal
}

func updateAccountRead(account *accountState, vr *state.VersionedRead, accessIndex uint16) {
	if vr == nil {
		panic("vr should not be nil")
	}

	switch vr.Path {
	case state.StoragePath:
		account.changes.StorageReads = append(account.changes.StorageReads, vr.Key)
	case state.BalancePath:
		if val, ok := vr.Val.(uint256.Int); ok {
			account.balance.recordRead(accessIndex, val)
		}
	case state.NoncePath:
		if val, ok := vr.Val.(uint64); ok {
			account.nonce.recordRead(accessIndex, val)
		}
	case state.CodePath:
		if val, ok := vr.Val.([]byte); ok {
			account.code.recordRead(accessIndex, val)
		}
	default:
		log.Info("unhandled default case", "path", vr.Path)
	}
}

func addStorageUpdate(ac *types.AccountChanges, vw *state.VersionedWrite, txIndex uint16) {
	value := vw.Val.(uint256.Int)
	if ac.StorageChanges == nil {
		ac.StorageChanges = []*types.SlotChanges{{
			Slot:    vw.Key,
			Changes: []*types.StorageChange{{Index: txIndex, Value: value}},
		}}
		return
	}

	for _, slotChange := range ac.StorageChanges {
		if slotChange.Slot == vw.Key {
			slotChange.Changes = append(slotChange.Changes, &types.StorageChange{Index: txIndex, Value: value})
			return
		}
	}

	ac.StorageChanges = append(ac.StorageChanges, &types.SlotChanges{
		Slot:    vw.Key,
		Changes: []*types.StorageChange{{Index: txIndex, Value: value}},
	})
}

func ensureAccountState(accounts map[common.Address]*accountState, addr common.Address) *accountState {
	if account, ok := accounts[addr]; ok {
		return account
	}
	account := &accountState{
		changes: &types.AccountChanges{Address: addr},
	}
	accounts[addr] = account
	return account
}

func updateAccountWrite(account *accountState, vw *state.VersionedWrite, accessIndex uint16) {
	switch vw.Path {
	case state.StoragePath:
		addStorageUpdate(account.changes, vw, accessIndex)
	case state.BalancePath:
		if val, ok := vw.Val.(uint256.Int); ok {
			account.balance.recordWrite(accessIndex, val)
		}
	case state.NoncePath:
		if val, ok := vw.Val.(uint64); ok {
			account.nonce.recordWrite(accessIndex, val)
		}
	case state.CodePath:
		if val, ok := vw.Val.([]byte); ok {
			account.code.recordWrite(accessIndex, val)
		}
	default:
		log.Info("Unknown storage path", "path", vw.Path)
	}
}

func blockAccessIndex(txIndex int) uint16 {
	return uint16(txIndex + 1)
}

type accountState struct {
	changes *types.AccountChanges
	balance balanceTracker
	nonce   nonceTracker
	code    codeTracker
}

// check pre- and post-values, add to BAL if different
func (a *accountState) finalize() {
	a.balance.applyTo(a.changes)
	a.nonce.applyTo(a.changes)
	a.code.applyTo(a.changes)
}

type balanceTracker struct {
	changes changeTracker[uint256.Int]
}

func (bt *balanceTracker) recordRead(idx uint16, value uint256.Int) {
	bt.changes.recordRead(idx, value, func(v uint256.Int) uint256.Int { return v }, func(a, b uint256.Int) bool {
		return a.Eq(&b)
	})
}

func (bt *balanceTracker) recordWrite(idx uint16, value uint256.Int) {
	bt.changes.recordWrite(idx, value, func(v uint256.Int) uint256.Int { return v }, func(a, b uint256.Int) bool {
		return a.Eq(&b)
	})
}

func (bt *balanceTracker) applyTo(ac *types.AccountChanges) {
	bt.changes.apply(func(idx uint16, value uint256.Int) {
		ac.BalanceChanges = append(ac.BalanceChanges, &types.BalanceChange{
			Index: idx,
			Value: value,
		})
	})
}

type nonceTracker struct {
	changes changeTracker[uint64]
}

func (nt *nonceTracker) recordRead(idx uint16, value uint64) {
	nt.changes.recordRead(idx, value, func(v uint64) uint64 { return v }, func(a, b uint64) bool {
		return a == b
	})
}

func (nt *nonceTracker) recordWrite(idx uint16, value uint64) {
	nt.changes.recordWrite(idx, value, func(v uint64) uint64 { return v }, func(a, b uint64) bool {
		return a == b
	})
}

func (nt *nonceTracker) applyTo(ac *types.AccountChanges) {
	nt.changes.apply(func(idx uint16, value uint64) {
		ac.NonceChanges = append(ac.NonceChanges, &types.NonceChange{
			Index: idx,
			Value: value,
		})
	})
}

type codeTracker struct {
	changes changeTracker[[]byte]
}

func (ct *codeTracker) recordRead(idx uint16, value []byte) {
	ct.changes.recordRead(idx, value, cloneBytes, bytes.Equal)
}

func (ct *codeTracker) recordWrite(idx uint16, value []byte) {
	ct.changes.recordWrite(idx, value, cloneBytes, bytes.Equal)
}

func (ct *codeTracker) applyTo(ac *types.AccountChanges) {
	ct.changes.apply(func(idx uint16, value []byte) {
		ac.CodeChanges = append(ac.CodeChanges, &types.CodeChange{
			Index: idx,
			Data:  cloneBytes(value),
		})
	})
}

type changeTracker[T any] struct {
	entries map[uint16]*changeEntry[T]
	equal   func(T, T) bool
}

type changeEntry[T any] struct {
	index      uint16
	read       bool
	readValue  T
	write      bool
	writeValue T
}

func (ct *changeTracker[T]) recordRead(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	ct.ensureEqual(equal)
	entry := ct.ensureEntry(idx)
	if entry.read {
		return
	}
	entry.read = true
	entry.readValue = copyFn(value)
}

func (ct *changeTracker[T]) recordWrite(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	ct.ensureEqual(equal)
	entry := ct.ensureEntry(idx)
	entry.write = true
	entry.writeValue = copyFn(value)
}

func (ct *changeTracker[T]) apply(applyFn func(uint16, T)) {
	if len(ct.entries) == 0 {
		return
	}
	for _, idx := range sortedChangeKeys(ct.entries) {
		entry := ct.entries[idx]
		if !entry.write {
			continue
		}
		if entry.read && ct.equal != nil && ct.equal(entry.writeValue, entry.readValue) {
			continue
		}
		applyFn(entry.index, entry.writeValue)
	}
}

func (ct *changeTracker[T]) ensureEntry(idx uint16) *changeEntry[T] {
	if ct.entries == nil {
		ct.entries = make(map[uint16]*changeEntry[T])
	}
	if entry, ok := ct.entries[idx]; ok {
		return entry
	}
	entry := &changeEntry[T]{index: idx}
	ct.entries[idx] = entry
	return entry
}

func (ct *changeTracker[T]) ensureEqual(equal func(T, T) bool) {
	if ct.equal == nil {
		ct.equal = equal
	}
}

func sortedChangeKeys[T any](entries map[uint16]*changeEntry[T]) []uint16 {
	keys := make([]uint16, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func cloneBytes(input []byte) []byte {
	if len(input) == 0 {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}
