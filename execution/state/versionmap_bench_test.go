package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkSeedEmptyRemovals(b *testing.B) {
	for _, tc := range []struct{ balances, storage int }{
		{32, 0},
		{256, 0},
		{1024, 0},
		{2, 256},
		{256, 256},
	} {
		b.Run(fmt.Sprintf("balances=%d/storage=%d", tc.balances, tc.storage), func(b *testing.B) {
			bal := types.BlockAccessList{{
				Address:      common.Address{19: 1},
				NonceChanges: []*types.NonceChange{{Index: uint32(2*max(tc.balances, tc.storage) + 1), Value: 1}},
			}}
			for i := range tc.balances {
				bal[0].BalanceChanges = append(bal[0].BalanceChanges, &types.BalanceChange{
					Index: uint32(2*i + 1), Value: *uint256.NewInt(uint64((i + 1) % 2)),
				})
			}
			if tc.storage > 0 {
				changes := make([]*types.StorageChange, tc.storage)
				for i := range changes {
					changes[i] = &types.StorageChange{Index: uint32(2*i + 2), Value: *uint256.NewInt(uint64(i + 1))}
				}
				bal[0].StorageChanges = []types.SlotChanges{{Slot: accounts.InternKey(common.Hash{31: 1}), Changes: changes}}
			}
			base := &countingAccountReader{}
			for b.Loop() {
				b.StopTimer()
				vm := NewVersionMap(bal)
				b.StartTimer()
				require.NoError(b, vm.SeedEmptyRemovals(true, false, base))
			}
		})
	}
}

func BenchmarkWriteTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	const n = 10000
	randInts := make([]int, n)
	for i := range randInts {
		randInts[i] = rand.Intn(1000000000000000)
	}

	for i := 0; b.Loop(); i++ {
		idx := randInts[i%n]
		writeFor(mvh2, ap2, AddressPath, accounts.NilKey, Version{0, 0, idx, 1}, valueFor(AddressPath, idx, 1), true)
	}
}

func BenchmarkReadTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)
	txIdxSlice := []int{}

	for b.Loop() {
		txIdx := rand.Intn(1000000000000000)
		txIdxSlice = append(txIdxSlice, txIdx)
		writeFor(mvh2, ap2, AddressPath, accounts.NilKey, Version{0, 0, txIdx, 1}, valueFor(AddressPath, txIdx, 1), true)
	}

	b.ResetTimer()

	for _, value := range txIdxSlice {
		readFor(mvh2, ap2, AddressPath, accounts.NilKey, value)
	}
}

func BenchmarkMarkWritesValidated(b *testing.B) {
	const n = 4096
	vm := NewVersionMap(nil)
	writes := make([]*WriteSet, n)
	addrs := make([]accounts.Address, 0, 2*n)
	addrAt := func(i int) accounts.Address {
		return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i))))
	}
	for i := range n {
		sender, recipient := addrAt(2*i+1), addrAt(2*i+2)
		v := Version{TxIndex: i, Incarnation: 1}
		ws := &WriteSet{}
		ws.SetBalance(sender, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: sender, Path: BalancePath, Key: accounts.NilKey, Version: v}, Val: *uint256.NewInt(uint64(i))})
		ws.SetNonce(sender, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: sender, Path: NoncePath, Key: accounts.NilKey, Version: v}, Val: uint64(i)})
		ws.SetBalance(recipient, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: recipient, Path: BalancePath, Key: accounts.NilKey, Version: v}, Val: *uint256.NewInt(uint64(i))})
		vm.FlushVersionedWrites(ws, false)
		writes[i] = ws
		addrs = append(addrs, sender, recipient)
	}
	for _, readers := range []int{0, 8} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for r := range readers {
				wg.Go(func() {
					for j := r; ; j++ {
						select {
						case <-stop:
							return
						default:
						}
						vm.ReadBalance(addrs[j%len(addrs)], n+1)
					}
				})
			}
			i := 0
			for b.Loop() {
				vm.MarkWritesValidated(writes[i], nil)
				if i++; i == n {
					i = 0
				}
			}
			close(stop)
			wg.Wait()
		})
	}
}

// BenchmarkValidateAccountReads validates 200 account reads of a block with an access list: reads
// of accounts that the list's existence marker records, reads of accounts that the list does not
// change, absent reads of accounts without cells, and absent reads of accounts that a later tx
// creates.
func BenchmarkValidateAccountReads(b *testing.B) {
	const n = 200
	present, unchanged, absent, created := make([]accounts.Address, n), make([]accounts.Address, n), make([]accounts.Address, n), make([]accounts.Address, n)
	bal := make(types.BlockAccessList, 2*n)
	reader := &accountStateReader{accounts: map[accounts.Address]*accounts.Account{}}
	for i := range n {
		present[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
		unchanged[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(n + i + 1))))
		absent[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(2*n + i + 1))))
		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(1)
		reader.accounts[present[i]] = &acc
		reader.accounts[unchanged[i]] = &acc
		bal[i] = types.AccountChanges{Address: present[i].Value(), BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(2)}}}
		created[i] = accounts.InternAddress(common.BigToAddress(big.NewInt(int64(3*n + i + 1))))
		bal[n+i] = types.AccountChanges{Address: created[i].Value(), BalanceChanges: []*types.BalanceChange{{Index: 5, Value: *uint256.NewInt(2)}}}
	}
	vm := NewVersionMap(bal)
	vm.SeedAddresses()
	for _, tc := range []struct {
		name  string
		addrs []accounts.Address
	}{
		{"present from the marker", present},
		{"present from the base", unchanged},
		{"absent without cells", absent},
		{"absent with cells", created},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ibs := NewWithVersionMap(reader, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 2)
			for _, addr := range tc.addrs {
				_, err := ibs.Exist(addr)
				require.NoError(b, err)
			}
			rs := ReadSet{address: ibs.VersionedReads().address}
			require.Len(b, rs.address, n)
			b.ReportAllocs()
			for b.Loop() {
				if vm.ValidateReadSet(2, rs, validateEqualVersion, false, "") != VersionValid {
					b.Fatal("read set is invalid")
				}
			}
		})
	}
}
