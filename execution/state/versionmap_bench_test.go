package state

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"

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

// BenchmarkReadStorageLifeUnwrittenSlot: workers read slots no tx wrote on one hot
// contract that also has written slots, with and without a concurrent cell writer.
func BenchmarkReadStorageLifeUnwrittenSlot(b *testing.B) {
	for _, tc := range []struct {
		name   string
		writer bool
	}{{"noWriter", false}, {"writer", true}} {
		b.Run(tc.name, func(b *testing.B) {
			addr := accounts.InternAddress([20]byte{0xef})
			written := accounts.InternKey([32]byte{1})
			vm := NewVersionMap(nil)
			vm.WriteStorage(addr, written, Version{TxIndex: 0}, *uint256.NewInt(1), true)
			const slots = 1024
			keys := make([]accounts.StorageKey, slots)
			for i := range keys {
				keys[i] = accounts.InternKey([32]byte{2, byte(i >> 8), byte(i)})
				vm.StoreOriginStorage(addr, keys[i], *uint256.NewInt(uint64(i)))
			}

			var stop atomic.Bool
			done := make(chan struct{})
			if tc.writer {
				go func() {
					defer close(done)
					for tx := 1; !stop.Load(); tx = tx%64 + 1 {
						vm.WriteStorage(addr, written, Version{TxIndex: tx}, *uint256.NewInt(uint64(tx)), true)
						time.Sleep(10 * time.Microsecond)
					}
				}()
			} else {
				close(done)
			}

			var worker atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(worker.Add(1)) * 97
				for pb.Next() {
					i++
					if _, res, _, _ := vm.ReadStorageLife(addr, keys[i%slots], 65); res.DepIdx() != originIndex {
						panic("unwritten slot did not read the origin")
					}
				}
			})
			b.StopTimer()
			stop.Store(true)
			<-done
		})
	}
}
