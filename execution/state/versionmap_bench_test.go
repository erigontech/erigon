package state

import (
	"math/rand"
	"testing"

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

// BenchmarkSStoreDirtyTransitions mirrors the sstore_dirty_transitions
// oscillation_6x access shape: one contract, many slots, each slot written
// several times per tx, with prior txs' writes already in the version map.
// Each SSTORE's EIP-2200 gas needs both the current and the original value,
// so every oscillation costs two full read-stack walks.
func BenchmarkSStoreDirtyTransitions(b *testing.B) {
	const (
		slots       = 512
		oscillation = 6
		priorTxs    = 14
		myTx        = 7
	)
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1

	keys := make([]accounts.StorageKey, slots)
	committed := make(map[accounts.StorageKey]uint256.Int, slots)
	for i := range keys {
		var raw [32]byte
		raw[30], raw[31] = byte(i>>8), byte(i)
		keys[i] = accounts.InternKey(raw)
		committed[keys[i]] = *uint256.NewInt(uint64(i) + 1)
	}
	reader := &storageReader{addr: addr, account: &acc, storage: committed}

	vm := NewVersionMap(nil)
	for tx := 0; tx < priorTxs; tx++ {
		if tx == myTx {
			continue
		}
		for i, k := range keys {
			vm.WriteStorage(addr, k, Version{TxIndex: tx}, *uint256.NewInt(uint64(tx*slots + i)), true)
		}
	}

	for _, arm := range []struct {
		name      string
		versioned bool
	}{{"serial", false}, {"versioned", true}} {
		b.Run(arm.name, func(b *testing.B) {
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				runOscillations(b, reader, vm, arm.versioned, addr, keys, oscillation, myTx)
			}
		})
	}
}

func runOscillations(b *testing.B, reader StateReader, vm *VersionMap, versioned bool, addr accounts.Address, keys []accounts.StorageKey, oscillation, myTx int) {
	var ibs *IntraBlockState
	if versioned {
		ibs = NewWithVersionMap(reader, vm)
		ibs.SetNoMaterialize(true)
	} else {
		ibs = New(reader)
	}
	ibs.SetTxContext(100, myTx)
	ibs.SetVersion(0)
	slots := len(keys)
	{
		for o := 0; o < oscillation; o++ {
			for i, k := range keys {
				if _, err := ibs.GetState(addr, k); err != nil {
					b.Fatal(err)
				}
				if _, err := ibs.GetCommittedState(addr, k); err != nil {
					b.Fatal(err)
				}
				if err := ibs.SetState(addr, k, *uint256.NewInt(uint64(o*slots+i) + 1)); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}
