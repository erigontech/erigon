package stagedsync

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCreateBALOrdering(t *testing.T) {
	addrA := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000000001"))
	addrB := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000000002"))
	slot1 := accounts.InternKey(common.BigToHash(big.NewInt(1)))
	slot2 := accounts.InternKey(common.BigToHash(big.NewInt(2)))

	io := state.NewVersionedIO(4)

	readSets := map[int]state.ReadSet{}
	writeSets := map[int]state.VersionedWrites{}

	// Pre-execution read to ensure addrA appears even without writes.
	addBalanceRead(readSets, -1, addrA, uint64(5))

	// Transaction 0: storage reads (out of order, with duplicates) and two writes to same slot.
	addStorageRead(readSets, 0, addrB, slot2)
	addStorageRead(readSets, 0, addrB, slot1)
	addStorageRead(readSets, 0, addrB, slot1)

	addStorageWrite(writeSets, 0, addrB, slot2, 10)
	addStorageWrite(writeSets, 0, addrB, slot2, 20) // later write with same index should win

	// Transaction 1: storage write to another slot.
	addStorageWrite(writeSets, 1, addrB, slot1, 5)

	// Post-execution: balance update for addrA.
	addBalanceWrite(writeSets, 2, addrA, 99)

	recordAll(io, readSets, writeSets)

	bal := io.AsBlockAccessList()

	if len(bal) != 2 {
		t.Fatalf("expected two accounts in BAL, got %d", len(bal))
	}

	// Addresses must be sorted lexicographically.
	if bal[0].Address != addrA || bal[1].Address != addrB {
		t.Fatalf("unexpected account ordering: %x, %x", bal[0].Address, bal[1].Address)
	}

	// addrA should have a single balance change at post-exec index 3 (two txs => len+1).
	if len(bal[0].BalanceChanges) != 1 {
		t.Fatalf("expected 1 balance change for addrA, got %d", len(bal[0].BalanceChanges))
	}
	if bal[0].BalanceChanges[0].Index != 3 {
		t.Fatalf("expected post-exec index 3 for addrA balance change, got %d", bal[0].BalanceChanges[0].Index)
	}
	if bal[0].BalanceChanges[0].Value.Uint64() != 99 {
		t.Fatalf("unexpected balance value for addrA: %s", bal[0].BalanceChanges[0].Value.String())
	}

	accountB := bal[1]

	// Storage reads are only recorded for slots without writes.
	if len(accountB.StorageReads) != 0 {
		t.Fatalf("unexpected storage reads: %+v", accountB.StorageReads)
	}

	// Storage slots should be sorted lexicographically.
	if len(accountB.StorageChanges) != 2 {
		t.Fatalf("expected two storage slots for addrB, got %d", len(accountB.StorageChanges))
	}

	if accountB.StorageChanges[0].Slot != slot1 || accountB.StorageChanges[1].Slot != slot2 {
		t.Fatalf("storage slots not sorted: %x %x", accountB.StorageChanges[0].Slot, accountB.StorageChanges[1].Slot)
	}

	slot1Changes := accountB.StorageChanges[0].Changes
	if len(slot1Changes) != 1 || slot1Changes[0].Index != 2 || slot1Changes[0].Value.Uint64() != 5 {
		changes := make([]types.StorageChange, len(slot1Changes))
		for i, change := range slot1Changes {
			changes[i] = *change
		}
		t.Fatalf("unexpected slot1 change: %+v", changes)
	}

	slot2Changes := accountB.StorageChanges[1].Changes
	if len(slot2Changes) != 1 || slot2Changes[0].Index != 1 || slot2Changes[0].Value.Uint64() != 20 {
		t.Fatalf("slot2 changes not deduplicated or unsorted: %+v", slot2Changes)
	}
}

func addStorageRead(readSets map[int]state.ReadSet, txIdx int, addr accounts.Address, slot accounts.StorageKey) {
	rs := readSets[txIdx]
	if rs == nil {
		rs = state.ReadSet{}
		readSets[txIdx] = rs
	}
	rs.Set(state.VersionedRead{
		Address: addr,
		Path:    state.StoragePath,
		Key:     slot,
	})
}

func addBalanceRead(readSets map[int]state.ReadSet, txIdx int, addr accounts.Address, value uint64) {
	rs := readSets[txIdx]
	if rs == nil {
		rs = state.ReadSet{}
		readSets[txIdx] = rs
	}
	rs.Set(state.VersionedRead{
		Address: addr,
		Path:    state.BalancePath,
		Val:     value,
	})
}

func addStorageWrite(writeSets map[int]state.VersionedWrites, txIdx int, addr accounts.Address, slot accounts.StorageKey, value uint64) {
	writeSets[txIdx] = append(writeSets[txIdx], &state.VersionedWrite{
		Address: addr,
		Path:    state.StoragePath,
		Key:     slot,
		Version: state.Version{TxIndex: txIdx},
		Val:     *uint256.NewInt(value),
	})
}

func addBalanceWrite(writeSets map[int]state.VersionedWrites, txIdx int, addr accounts.Address, value uint64) {
	writeSets[txIdx] = append(writeSets[txIdx], &state.VersionedWrite{
		Address: addr,
		Path:    state.BalancePath,
		Version: state.Version{TxIndex: txIdx},
		Val:     *uint256.NewInt(value),
	})
}

func recordAll(io *state.VersionedIO, reads map[int]state.ReadSet, writes map[int]state.VersionedWrites) {
	for txIdx, rs := range reads {
		io.RecordReads(state.Version{TxIndex: txIdx}, rs)
	}
	for txIdx, ws := range writes {
		io.RecordWrites(state.Version{TxIndex: txIdx}, ws)
	}
}
