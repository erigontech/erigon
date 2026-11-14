package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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

// transfersBAL mimics a block of n ether transfers: each tx changes a sender's balance and
// nonce and a recipient's balance, and the coinbase balance changes at every tx.
func transfersBAL(n int) types.BlockAccessList {
	coinbase := n
	bal := make(types.BlockAccessList, 2*n+1)
	for i := range bal {
		bal[i].Address = common.BigToAddress(big.NewInt(int64(i + 1)))
	}
	for tx := range n {
		idx := uint32(tx + 1)
		sender, recipient := tx, n+1+tx
		if sender >= coinbase {
			sender++
		}
		bal[sender].BalanceChanges = []*types.BalanceChange{{Index: idx, Value: *uint256.NewInt(uint64(tx))}}
		bal[sender].NonceChanges = []*types.NonceChange{{Index: idx, Value: 1}}
		bal[recipient].BalanceChanges = []*types.BalanceChange{{Index: idx, Value: *uint256.NewInt(uint64(tx))}}
		bal[coinbase].BalanceChanges = append(bal[coinbase].BalanceChanges, &types.BalanceChange{Index: idx, Value: *uint256.NewInt(uint64(tx))})
	}
	return bal
}

func BenchmarkNewVersionMapFromBAL(b *testing.B) {
	bal := transfersBAL(15000)
	for b.Loop() {
		NewVersionMap(bal).SeedAddresses()
	}
}
