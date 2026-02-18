package stagedsync

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func CreateBAL(blockNum uint64, txIO *state.VersionedIO, dataDir string) types.BlockAccessList {
	ac := make(map[accounts.Address]*accountState)
	maxTxIndex := len(txIO.Inputs()) - 1

	for txIndex := -1; txIndex <= maxTxIndex; txIndex++ {
		txIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
			if vr.Address.IsNil() {
				return true
			}
			// Skip validation-only reads for non-existent accounts.
			// These are recorded by versionedRead when the version map
			// has no entry (MVReadResultNone) so that conflict detection
			// works across transactions, but they should not appear in
			// the block access list.
			if vr.Path == state.AddressPath {
				if val, ok := vr.Val.(*accounts.Account); ok && val == nil {
					return true
				}
			}
			account := ensureAccountState(ac, vr.Address)
			updateAccountRead(account, vr)
			return true
		})

		for _, vw := range txIO.WriteSet(txIndex) {
			if vw.Address.IsNil() {
				continue
			}
			account := ensureAccountState(ac, vw.Address)
			accessIndex := blockAccessIndex(vw.Version.TxIndex)
			updateAccountWrite(account, vw, accessIndex)
		}

		for addr := range txIO.AccessedAddresses(txIndex) {
			if addr.IsNil() {
				continue
			}
			ensureAccountState(ac, addr)
		}
	}

	bal := make([]*types.AccountChanges, 0, len(ac))
	for _, account := range ac {
		account.finalize()
		normalizeAccountChanges(account.changes)
		// The system address is touched during system calls (EIP-4788 beacon root)
		// because it is msg.sender. Exclude it when it has no actual state changes,
		// but keep it when a user tx sends real ETH to it (e.g. SELFDESTRUCT to
		// the system address or a plain value transfer).
		if isSystemBALAddress(account.changes.Address) && !hasAccountChanges(account.changes) {
			continue
		}
		bal = append(bal, account.changes)
	}

	sort.Slice(bal, func(i, j int) bool {
		return bal[i].Address.Cmp(bal[j].Address) < 0
	})

	writeBALToFile(bal, blockNum, dataDir)

	return bal
}

func updateAccountRead(account *accountState, vr *state.VersionedRead) {
	if vr == nil {
		panic("vr should not be nil")
	}

	switch vr.Path {
	case state.StoragePath:
		if hasStorageWrite(account.changes, vr.Key) {
			return
		}
		account.changes.StorageReads = append(account.changes.StorageReads, vr.Key)
	case state.BalancePath:
		if val, ok := vr.Val.(uint256.Int); ok {
			account.setBalanceValue(val)
		}
	default:
		// Only track storage reads for BAL. Balance/nonce/code changes are tracked via writes, others are ignored
	}
}

func addStorageUpdate(ac *types.AccountChanges, vw *state.VersionedWrite, txIndex uint16) {
	val := vw.Val.(uint256.Int)
	// If we already recorded a read for this slot, drop it because a write takes precedence.
	removeStorageRead(ac, vw.Key)

	if ac.StorageChanges == nil {
		ac.StorageChanges = []*types.SlotChanges{{
			Slot:    vw.Key,
			Changes: []*types.StorageChange{{Index: txIndex, Value: val}},
		}}
		return
	}

	for _, slotChange := range ac.StorageChanges {
		if slotChange.Slot == vw.Key {
			slotChange.Changes = append(slotChange.Changes, &types.StorageChange{Index: txIndex, Value: val})
			return
		}
	}

	ac.StorageChanges = append(ac.StorageChanges, &types.SlotChanges{
		Slot:    vw.Key,
		Changes: []*types.StorageChange{{Index: txIndex, Value: val}},
	})
}

func ensureAccountState(accounts map[accounts.Address]*accountState, addr accounts.Address) *accountState {
	if account, ok := accounts[addr]; ok {
		return account
	}
	account := &accountState{
		changes: &types.AccountChanges{Address: addr},
		balance: newBalanceTracker(),
		nonce:   newNonceTracker(),
		code:    newCodeTracker(),
	}
	accounts[addr] = account
	return account
}

func updateAccountWrite(account *accountState, vw *state.VersionedWrite, accessIndex uint16) {
	switch vw.Path {
	case state.StoragePath:
		addStorageUpdate(account.changes, vw, accessIndex)
	case state.SelfDestructPath:
		if deleted, ok := vw.Val.(bool); ok && deleted {
			account.selfDestructed = true
		}
	case state.BalancePath:
		val, ok := vw.Val.(uint256.Int)
		if !ok {
			return
		}
		// Skip non-zero balance writes for selfdestructed accounts.
		// Post-selfdestruct ETH (e.g. priority fee applied during finalize) must
		// not appear in the BAL per EIP-7928 — only the zero-balance write from
		// the selfdestruct itself belongs there.
		if account.selfDestructed && !val.IsZero() {
			return
		}
		// If we haven't seen a balance and the first write is zero, treat it as a touch only.
		if account.balanceValue == nil && val.IsZero() {
			account.setBalanceValue(val)
			return
		}
		// Skip no-op writes.
		if account.balanceValue != nil && val.Eq(account.balanceValue) {
			account.setBalanceValue(val)
			return
		}
		account.setBalanceValue(val)
		account.balance.recordWrite(accessIndex, val, func(v uint256.Int) uint256.Int { return v }, func(a, b uint256.Int) bool {
			return a.Eq(&b)
		})
	case state.NoncePath:
		if val, ok := vw.Val.(uint64); ok {
			account.nonce.recordWrite(accessIndex, val, func(v uint64) uint64 { return v }, func(a, b uint64) bool {
				return a == b
			})
		}
	case state.CodePath:
		if val, ok := vw.Val.([]byte); ok {
			account.code.recordWrite(accessIndex, val, cloneBytes, bytes.Equal)
		}
	default:
	}
}

func isSystemBALAddress(addr accounts.Address) bool {
	return addr == params.SystemAddress
}

func hasAccountChanges(ac *types.AccountChanges) bool {
	return len(ac.StorageChanges) > 0 || len(ac.StorageReads) > 0 ||
		len(ac.BalanceChanges) > 0 || len(ac.NonceChanges) > 0 || len(ac.CodeChanges) > 0
}

func hasStorageWrite(ac *types.AccountChanges, slot accounts.StorageKey) bool {
	for _, sc := range ac.StorageChanges {
		if sc != nil && sc.Slot == slot {
			return true
		}
	}
	return false
}

func removeStorageRead(ac *types.AccountChanges, slot accounts.StorageKey) {
	if len(ac.StorageReads) == 0 {
		return
	}
	out := ac.StorageReads[:0]
	for _, s := range ac.StorageReads {
		if s != slot {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		ac.StorageReads = nil
	} else {
		ac.StorageReads = out
	}
}

func blockAccessIndex(txIndex int) uint16 {
	return uint16(txIndex + 1)
}

type accountState struct {
	changes        *types.AccountChanges
	balance        *fieldTracker[uint256.Int]
	nonce          *fieldTracker[uint64]
	code           *fieldTracker[[]byte]
	balanceValue   *uint256.Int // tracks latest seen balance
	selfDestructed bool         // true once SelfDestructPath=true is seen for this account
}

// check pre- and post-values, add to BAL if different
func (a *accountState) finalize() {
	applyToBalance(a.balance, a.changes)
	applyToNonce(a.nonce, a.changes)
	applyToCode(a.code, a.changes)
}

type fieldTracker[T any] struct {
	changes changeTracker[T]
}

func (ft *fieldTracker[T]) recordWrite(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	ft.changes.recordWrite(idx, value, copyFn, equal)
}

func newBalanceTracker() *fieldTracker[uint256.Int] {
	return &fieldTracker[uint256.Int]{}
}

func applyToBalance(bt *fieldTracker[uint256.Int], ac *types.AccountChanges) {
	bt.changes.apply(func(idx uint16, value uint256.Int) {
		ac.BalanceChanges = append(ac.BalanceChanges, &types.BalanceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newNonceTracker() *fieldTracker[uint64] {
	return &fieldTracker[uint64]{}
}

func applyToNonce(nt *fieldTracker[uint64], ac *types.AccountChanges) {
	nt.changes.apply(func(idx uint16, value uint64) {
		ac.NonceChanges = append(ac.NonceChanges, &types.NonceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newCodeTracker() *fieldTracker[[]byte] {
	return &fieldTracker[[]byte]{}
}

func applyToCode(ct *fieldTracker[[]byte], ac *types.AccountChanges) {
	ct.changes.apply(func(idx uint16, value []byte) {
		ac.CodeChanges = append(ac.CodeChanges, &types.CodeChange{
			Index:    idx,
			Bytecode: cloneBytes(value),
		})
	})
}

type changeTracker[T any] struct {
	entries map[uint16]T
	equal   func(T, T) bool
}

func (ct *changeTracker[T]) recordWrite(idx uint16, value T, copyFn func(T) T, equal func(T, T) bool) {
	if ct.entries == nil {
		ct.entries = make(map[uint16]T)
		ct.equal = equal
	}
	ct.entries[idx] = copyFn(value)
}

func (ct *changeTracker[T]) apply(applyFn func(uint16, T)) {
	if len(ct.entries) == 0 {
		return
	}

	indices := make([]uint16, 0, len(ct.entries))
	for idx := range ct.entries {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, idx := range indices {
		applyFn(idx, ct.entries[idx])
	}
}

func normalizeAccountChanges(ac *types.AccountChanges) {
	if len(ac.StorageChanges) > 1 {
		sort.Slice(ac.StorageChanges, func(i, j int) bool {
			return ac.StorageChanges[i].Slot.Cmp(ac.StorageChanges[j].Slot) < 0
		})
	}

	for _, slotChange := range ac.StorageChanges {
		if len(slotChange.Changes) > 1 {
			sortByIndex(slotChange.Changes)
			slotChange.Changes = dedupByIndex(slotChange.Changes)
		}
	}

	if len(ac.StorageReads) > 1 {
		sortHashes(ac.StorageReads)
		ac.StorageReads = dedupByEquality(ac.StorageReads)
	}

	if len(ac.BalanceChanges) > 1 {
		sortByIndex(ac.BalanceChanges)
		ac.BalanceChanges = dedupByIndex(ac.BalanceChanges)
	}
	if len(ac.NonceChanges) > 1 {
		sortByIndex(ac.NonceChanges)
		ac.NonceChanges = dedupByIndex(ac.NonceChanges)
	}
	if len(ac.CodeChanges) > 1 {
		sortByIndex(ac.CodeChanges)
		ac.CodeChanges = dedupByIndex(ac.CodeChanges)
	}
}

func dedupByIndex[T interface{ GetIndex() uint16 }](changes []T) []T {
	if len(changes) == 0 {
		return changes
	}
	out := changes[:1]
	for i := 1; i < len(changes); i++ {
		if changes[i].GetIndex() == out[len(out)-1].GetIndex() {
			out[len(out)-1] = changes[i]
			continue
		}
		out = append(out, changes[i])
	}
	return out
}

func dedupByEquality[T comparable](items []T) []T {
	if len(items) == 0 {
		return items
	}
	out := items[:1]
	for i := 1; i < len(items); i++ {
		if items[i] == out[len(out)-1] {
			continue
		}
		out = append(out, items[i])
	}
	return out
}

func sortByIndex[T interface{ GetIndex() uint16 }](changes []T) {
	sort.Slice(changes, func(i, j int) bool {
		return changes[i].GetIndex() < changes[j].GetIndex()
	})
}

func sortByBytes[T interface{ GetBytes() []byte }](items []T) {
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].GetBytes(), items[j].GetBytes()) < 0
	})
}

func sortHashes(hashes []accounts.StorageKey) {
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].Cmp(hashes[j]) < 0
	})
}

func cloneBytes(input []byte) []byte {
	if len(input) == 0 {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}

func (a *accountState) setBalanceValue(v uint256.Int) {
	if a.balanceValue == nil {
		a.balanceValue = &uint256.Int{}
	}
	*a.balanceValue = v
}

// writeBALToFile writes the Block Access List to a text file for debugging/analysis
func writeBALToFile(bal types.BlockAccessList, blockNum uint64, dataDir string) {
	if dataDir == "" {
		return
	}

	balDir := filepath.Join(dataDir, "bal")
	if err := os.MkdirAll(balDir, 0755); err != nil {
		log.Warn("Failed to create BAL directory", "dir", balDir, "error", err)
		return
	}

	filename := filepath.Join(balDir, fmt.Sprintf("bal_block_%d.txt", blockNum))

	file, err := os.Create(filename)
	if err != nil {
		log.Warn("Failed to create BAL file", "blockNum", blockNum, "error", err)
		return
	}
	defer file.Close()

	// Write header information
	fmt.Fprintf(file, "Block Access List for Block %d\n", blockNum)
	fmt.Fprintf(file, "Total Accounts: %d\n\n", len(bal))

	// Write each account's changes
	for _, account := range bal {
		fmt.Fprintf(file, "Account: %s\n", account.Address.Value().Hex())

		// Storage changes
		if len(account.StorageChanges) > 0 {
			fmt.Fprintf(file, "  Storage Changes (%d):\n", len(account.StorageChanges))
			for _, slotChange := range account.StorageChanges {
				fmt.Fprintf(file, "    Slot: %s\n", slotChange.Slot.Value().Hex())
				for _, change := range slotChange.Changes {
					fmt.Fprintf(file, "      [%d] -> %s\n", change.Index, change.Value.Hex())
				}
			}
		}

		// Storage reads
		if len(account.StorageReads) > 0 {
			fmt.Fprintf(file, "  Storage Reads (%d):\n", len(account.StorageReads))
			for _, read := range account.StorageReads {
				fmt.Fprintf(file, "    %s\n", read.Value().Hex())
			}
		}

		// Balance changes
		if len(account.BalanceChanges) > 0 {
			fmt.Fprintf(file, "  Balance Changes (%d):\n", len(account.BalanceChanges))
			for _, change := range account.BalanceChanges {
				fmt.Fprintf(file, "    [%d] -> %s\n", change.Index, change.Value.String())
			}
		}

		// Nonce changes
		if len(account.NonceChanges) > 0 {
			fmt.Fprintf(file, "  Nonce Changes (%d):\n", len(account.NonceChanges))
			for _, change := range account.NonceChanges {
				fmt.Fprintf(file, "    [%d] -> %d\n", change.Index, change.Value)
			}
		}

		// Code changes
		if len(account.CodeChanges) > 0 {
			fmt.Fprintf(file, "  Code Changes (%d):\n", len(account.CodeChanges))
			for _, change := range account.CodeChanges {
				fmt.Fprintf(file, "    [%d] -> %d bytes\n", change.Index, len(change.Bytecode))
				if len(change.Bytecode) <= 64 {
					fmt.Fprintf(file, "      Bytecode: %x\n", change.Bytecode)
				} else {
					fmt.Fprintf(file, "      Bytecode: %x... (truncated)\n", change.Bytecode[:64])
				}
			}
		}

		// If no changes, indicate that
		if len(account.StorageChanges) == 0 && len(account.StorageReads) == 0 &&
			len(account.BalanceChanges) == 0 && len(account.NonceChanges) == 0 &&
			len(account.CodeChanges) == 0 {
			fmt.Fprintf(file, "  No changes (accessed only)\n")
		}

		fmt.Fprintf(file, "\n")
	}

	//log.Info("BAL written to file", "blockNum", blockNum, "filename", filename, "accounts", len(bal))
}
