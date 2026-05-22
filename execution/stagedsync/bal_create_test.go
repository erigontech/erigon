package stagedsync

import (
	"fmt"
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
	addStorageReadVal(readSets, txIdx, addr, slot, *uint256.NewInt(0))
}

func addStorageReadVal(readSets map[int]state.ReadSet, txIdx int, addr accounts.Address, slot accounts.StorageKey, val uint256.Int) {
	rs := readSets[txIdx]
	if rs == nil {
		rs = state.ReadSet{}
		readSets[txIdx] = rs
	}
	rs.Set(state.VersionedRead{
		Address: addr,
		Path:    state.StoragePath,
		Key:     slot,
		Val:     val,
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
		Val:     *uint256.NewInt(value),
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

// TestBALBlock943Direct constructs the BAL for bal-devnet-2 block 943
// (first Amsterdam block, 0 user txs) directly and verifies the hash
// matches the known-good value from the devnet header.
//
// Block 943 system calls:
//   - txIndex=-1: EIP-4788 beacon root (2 storage writes), EIP-2935 block hash (1 storage write)
//   - txIndex=0 (finalize): EIP-7002 withdrawal request dequeue (4 SLOADs + 4 SSTOREs, all net-zero),
//     EIP-7251 consolidation request dequeue (4 SLOADs + 4 SSTOREs, all net-zero)
//
// The system address (0xff..fe) is filtered out per EIP-7928.
func TestBALBlock943Direct(t *testing.T) {
	expectedHash := common.HexToHash("0x0e9aff2d3f1c6d5083afd44ef786e54858d50004845d5ae2f0437dd61b83d00d")

	// System contract addresses (sorted lexicographically)
	eip7002Addr := accounts.InternAddress(common.HexToAddress("0x00000961Ef480Eb55e80D19ad83579A64c007002")) // EIP-7002 Withdrawal Requests
	eip7251Addr := accounts.InternAddress(common.HexToAddress("0x0000BBdDc7CE488642fb579F8B00f3a590007251")) // EIP-7251 Consolidation Requests
	eip2935Addr := accounts.InternAddress(common.HexToAddress("0x0000F90827F1C53a10cb7A02335B175320002935")) // EIP-2935 Block Hash Store
	eip4788Addr := accounts.InternAddress(common.HexToAddress("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02")) // EIP-4788 Beacon Roots

	// Storage slots and values for block 943 (from RPC)
	slot4788Timestamp := accounts.InternKey(common.BigToHash(big.NewInt(0x1747))) // timestamp % 8191
	slot4788Root := accounts.InternKey(common.BigToHash(big.NewInt(0x3746)))      // (timestamp % 8191) + 8191
	slot2935 := accounts.InternKey(common.BigToHash(big.NewInt(0x3ae)))           // (blockNum-1) % 8191

	val4788Timestamp := uint256.NewInt(0x69862afc)
	val4788Root := new(uint256.Int)
	val4788Root.SetBytes(common.Hex2Bytes("6d29857d40bc9c49248f83435de3c0f4df64b701e77c8550a40b4981802ccc9c"))
	val2935 := new(uint256.Int)
	val2935.SetBytes(common.Hex2Bytes("52b76c49b7b2892ef00b543e27480f8e57ca399e8ddf4a2aba127b263988885c"))

	// EIP-7002 and EIP-7251 dequeue contracts read slots 0-3 (excess, count, head, tail)
	// when queue is empty. All SSTOREs write 0 back to zero-valued slots (net-zero → reads only).
	slot0 := accounts.InternKey(common.BigToHash(big.NewInt(0)))
	slot1 := accounts.InternKey(common.BigToHash(big.NewInt(1)))
	slot2 := accounts.InternKey(common.BigToHash(big.NewInt(2)))
	slot3 := accounts.InternKey(common.BigToHash(big.NewInt(3)))

	// Construct BAL directly. Addresses must be sorted lexicographically:
	// 0x00000961... < 0x0000BBdD... < 0x0000F908... < 0x000F3df6...
	bal := types.BlockAccessList{
		// EIP-7002: only storage reads (empty queue, all writes are net-zero)
		{
			Address:      eip7002Addr,
			StorageReads: []accounts.StorageKey{slot0, slot1, slot2, slot3},
		},
		// EIP-7251: only storage reads (empty queue, all writes are net-zero)
		{
			Address:      eip7251Addr,
			StorageReads: []accounts.StorageKey{slot0, slot1, slot2, slot3},
		},
		// EIP-2935: 1 storage change at accessIndex 0 (system call txIndex=-1)
		{
			Address: eip2935Addr,
			StorageChanges: []*types.SlotChanges{
				{
					Slot:    slot2935,
					Changes: []*types.StorageChange{{Index: 0, Value: *val2935}},
				},
			},
		},
		// EIP-4788: 2 storage changes at accessIndex 0 (system call txIndex=-1)
		{
			Address: eip4788Addr,
			StorageChanges: []*types.SlotChanges{
				{
					Slot:    slot4788Timestamp,
					Changes: []*types.StorageChange{{Index: 0, Value: *val4788Timestamp}},
				},
				{
					Slot:    slot4788Root,
					Changes: []*types.StorageChange{{Index: 0, Value: *val4788Root}},
				},
			},
		},
	}

	if err := bal.Validate(); err != nil {
		t.Fatalf("BAL validation failed: %v", err)
	}

	got := bal.Hash()
	t.Logf("BAL hash: %s", got.Hex())
	t.Logf("BAL debug: %s", bal.DebugString())
	if got != expectedHash {
		t.Fatalf("BAL hash mismatch:\n  got:      %s\n  expected: %s", got.Hex(), expectedHash.Hex())
	}
}

// TestBALBlock943ViaVersionedIO constructs the BAL through VersionedIO
// (the same path used during block execution) and verifies the hash.
func TestBALBlock943ViaVersionedIO(t *testing.T) {
	expectedHash := common.HexToHash("0x0e9aff2d3f1c6d5083afd44ef786e54858d50004845d5ae2f0437dd61b83d00d")

	// System address, system contracts
	systemAddr := accounts.InternAddress(common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe"))
	eip7002Addr := accounts.InternAddress(common.HexToAddress("0x00000961Ef480Eb55e80D19ad83579A64c007002"))
	eip7251Addr := accounts.InternAddress(common.HexToAddress("0x0000BBdDc7CE488642fb579F8B00f3a590007251"))
	eip2935Addr := accounts.InternAddress(common.HexToAddress("0x0000F90827F1C53a10cb7A02335B175320002935"))
	eip4788Addr := accounts.InternAddress(common.HexToAddress("0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02"))

	// Storage slots and values
	slot4788Timestamp := accounts.InternKey(common.BigToHash(big.NewInt(0x1747)))
	slot4788Root := accounts.InternKey(common.BigToHash(big.NewInt(0x3746)))
	slot2935 := accounts.InternKey(common.BigToHash(big.NewInt(0x3ae)))
	slot0 := accounts.InternKey(common.BigToHash(big.NewInt(0)))
	slot1 := accounts.InternKey(common.BigToHash(big.NewInt(1)))
	slot2 := accounts.InternKey(common.BigToHash(big.NewInt(2)))
	slot3 := accounts.InternKey(common.BigToHash(big.NewInt(3)))

	val4788Timestamp := uint256.NewInt(0x69862afc)
	val4788Root := new(uint256.Int)
	val4788Root.SetBytes(common.Hex2Bytes("6d29857d40bc9c49248f83435de3c0f4df64b701e77c8550a40b4981802ccc9c"))
	val2935 := new(uint256.Int)
	val2935.SetBytes(common.Hex2Bytes("52b76c49b7b2892ef00b543e27480f8e57ca399e8ddf4a2aba127b263988885c"))

	// Block 943 has 0 user txs. Tasks: txIndex=-1 (Initialize), txIndex=0 (Finalize).
	// NewVersionedIO(numTx) where numTx is the count of user transactions (0).
	// However, the Finalize task at txIndex=0 needs to be accommodated.
	vio := state.NewVersionedIO(0)

	readSets := map[int]state.ReadSet{}
	writeSets := map[int]state.VersionedWrites{}

	// === txIndex=-1: Initialize system calls (EIP-4788, EIP-2935) ===

	// System address balance reads/writes (from SubBalance with Gnosis exception)
	// Balance is 0x24ac0a — read and write same value (no-op write filtered)
	systemBalance := uint256.NewInt(0x24ac0a)
	addBalanceRead(readSets, -1, systemAddr, systemBalance.Uint64())
	addBalanceWrite(writeSets, -1, systemAddr, systemBalance.Uint64())

	// EIP-4788 contract: balance read+write (no-op), 2 storage writes
	addBalanceRead(readSets, -1, eip4788Addr, 0)
	addBalanceWrite(writeSets, -1, eip4788Addr, 0)
	addStorageWrite(writeSets, -1, eip4788Addr, slot4788Timestamp, val4788Timestamp.Uint64())
	writeSets[-1] = append(writeSets[-1], &state.VersionedWrite{
		Address: eip4788Addr,
		Path:    state.StoragePath,
		Key:     slot4788Root,
		Version: state.Version{TxIndex: -1},
		Val:     *val4788Root,
	})

	// EIP-2935 contract: balance read+write (no-op), 1 storage write
	addBalanceRead(readSets, -1, eip2935Addr, 0)
	addBalanceWrite(writeSets, -1, eip2935Addr, 0)
	writeSets[-1] = append(writeSets[-1], &state.VersionedWrite{
		Address: eip2935Addr,
		Path:    state.StoragePath,
		Key:     slot2935,
		Version: state.Version{TxIndex: -1},
		Val:     *val2935,
	})

	// === txIndex=0: Finalize system calls (EIP-7002, EIP-7251 dequeue) ===
	// Empty queue: read slots 0-3, write 0 back to each (net-zero → reads only)

	// EIP-7002: balance read+write (no-op), storage reads + net-zero writes
	addBalanceRead(readSets, 0, eip7002Addr, 0)
	addBalanceWrite(writeSets, 0, eip7002Addr, 0)
	addStorageRead(readSets, 0, eip7002Addr, slot0)
	addStorageRead(readSets, 0, eip7002Addr, slot1)
	addStorageRead(readSets, 0, eip7002Addr, slot2)
	addStorageRead(readSets, 0, eip7002Addr, slot3)
	addStorageWrite(writeSets, 0, eip7002Addr, slot0, 0) // net-zero: write 0 to 0-valued slot
	addStorageWrite(writeSets, 0, eip7002Addr, slot1, 0)
	addStorageWrite(writeSets, 0, eip7002Addr, slot2, 0)
	addStorageWrite(writeSets, 0, eip7002Addr, slot3, 0)

	// EIP-7251: balance read+write (no-op), storage reads + net-zero writes
	addBalanceRead(readSets, 0, eip7251Addr, 0)
	addBalanceWrite(writeSets, 0, eip7251Addr, 0)
	addStorageRead(readSets, 0, eip7251Addr, slot0)
	addStorageRead(readSets, 0, eip7251Addr, slot1)
	addStorageRead(readSets, 0, eip7251Addr, slot2)
	addStorageRead(readSets, 0, eip7251Addr, slot3)
	addStorageWrite(writeSets, 0, eip7251Addr, slot0, 0)
	addStorageWrite(writeSets, 0, eip7251Addr, slot1, 0)
	addStorageWrite(writeSets, 0, eip7251Addr, slot2, 0)
	addStorageWrite(writeSets, 0, eip7251Addr, slot3, 0)

	// System address balance from Finalize Transfer calls (no-op)
	addBalanceRead(readSets, 0, systemAddr, systemBalance.Uint64())
	addBalanceWrite(writeSets, 0, systemAddr, systemBalance.Uint64())

	recordAll(vio, readSets, writeSets)

	bal := vio.AsBlockAccessList()

	t.Logf("BAL accounts: %d", len(bal))
	for i, ac := range bal {
		t.Logf("  [%d] %s: storage_changes=%d storage_reads=%d balance_changes=%d nonce_changes=%d code_changes=%d",
			i, ac.Address.Value().Hex(),
			len(ac.StorageChanges), len(ac.StorageReads),
			len(ac.BalanceChanges), len(ac.NonceChanges), len(ac.CodeChanges))
		for _, sc := range ac.StorageChanges {
			for _, ch := range sc.Changes {
				t.Logf("    slot %s [%d] -> %s", sc.Slot.Value().Hex(), ch.Index, ch.Value.Hex())
			}
		}
		for _, sr := range ac.StorageReads {
			t.Logf("    read slot %s", sr.Value().Hex())
		}
	}

	got := bal.Hash()
	t.Logf("BAL hash: %s", got.Hex())
	if got != expectedHash {
		t.Logf("BAL debug: %s", bal.DebugString())
		t.Fatalf("BAL hash mismatch:\n  got:      %s\n  expected: %s", got.Hex(), expectedHash.Hex())
	}

	// Verify system address was filtered out
	for _, ac := range bal {
		if ac.Address == systemAddr {
			t.Fatal("system address should have been filtered from BAL")
		}
	}

	_ = fmt.Sprintf // suppress unused import
}
