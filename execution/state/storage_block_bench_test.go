package state

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkStorageBlock runs a BAL block of many one-slot txs on one contract
// through the versioned IO path: each tx reads the slot its predecessor wrote,
// writes its own, records, flushes and validates.
func BenchmarkStorageBlock(b *testing.B) {
	const txs = 2000
	_, tx, domains := NewTestRwTx(b)
	contract := common.Address{0xc0}
	keys := make([]accounts.StorageKey, txs)
	changes := make([]types.SlotChanges, txs)
	for i := range keys {
		var k common.Hash
		k[0], k[30], k[31] = 0x5a, byte(i>>8), byte(i)
		keys[i] = accounts.InternKey(k)
		changes[i] = types.SlotChanges{Slot: keys[i], Changes: []*types.StorageChange{{Index: uint32(i + 1), Value: *uint256.NewInt(uint64(i + 1))}}}
	}
	bal := types.BlockAccessList{{Address: contract, StorageChanges: changes}}
	addr := accounts.InternAddress(contract)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	defer ibs.Close()
	check := func(rv, wv Version) VersionValidity {
		if rv != wv {
			return VersionInvalid
		}
		return VersionValid
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		vm := NewVersionMap(bal)
		vm.ShareSlotIDs()
		io := NewVersionedIO(txs)
		for i := range txs {
			ibs.Reset()
			ibs.SetVersionMap(vm)
			ibs.SetTxContext(1, i)
			if i > 0 {
				if _, err := ibs.GetState(addr, keys[i-1]); err != nil {
					b.Fatal(err)
				}
			}
			if err := ibs.SetState(addr, keys[i], *uint256.NewInt(uint64(i + 1))); err != nil {
				b.Fatal(err)
			}
			v := Version{TxIndex: i}
			io.RecordReads(v, ibs.VersionedReads())
			writes := ibs.VersionedWrites()
			io.RecordWrites(v, writes)
			vm.FlushVersionedWrites(writes, true)
			ibs.ResetVersionedIO()
		}
		for i := range txs {
			if vm.ValidateVersion(i, io, check, false, "") != VersionValid {
				b.Fatalf("tx %d invalid", i)
			}
		}
	}
}
