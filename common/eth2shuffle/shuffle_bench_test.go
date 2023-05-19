package eth2shuffle_test

import (
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon/common/eth2shuffle"
)

func BenchmarkPermuteIndex(b *testing.B) {
	listSizes := []uint64{4000000, 40000, 400}

	hashFn := getStandardHashFn()
	// "random" seed for testing. Can be any 32 bytes.
	seed := [32]byte{123, 42}

	// rounds of shuffling, constant in spec
	rounds := uint8(90)

	for _, listSize := range listSizes {
		// benchmark!
		b.Run(fmt.Sprintf("PermuteIndex_%d", listSize), func(ib *testing.B) {
			for i := uint64(0); i < uint64(ib.N); i++ {
				eth2shuffle.PermuteIndex(hashFn, rounds, i%listSize, listSize, seed)
			}
		})
	}
}

func BenchmarkIndexComparison(b *testing.B) {
	// 4M is just too inefficient to even start comparing.
	listSizes := []uint64{40000, 400}

	hashFn := getStandardHashFn()
	// "random" seed for testing. Can be any 32 bytes.
	seed := [32]byte{123, 42}

	// rounds of shuffling, constant in spec
	rounds := uint8(90)

	for _, listSize := range listSizes {
		// benchmark!
		b.Run(fmt.Sprintf("Indexwise_ShuffleList_%d", listSize), func(ib *testing.B) {
			for i := 0; i < ib.N; i++ {
				// Simulate a list-shuffle by running permute-index listSize times.
				for j := uint64(0); j < listSize; j++ {
					eth2shuffle.PermuteIndex(hashFn, rounds, j, listSize, seed)
				}
			}
		})
	}
}

func BenchmarkShuffleList(b *testing.B) {
	listSizes := []uint64{4000000, 40000, 400}

	hashFn := getStandardHashFn()
	// "random" seed for testing. Can be any 32 bytes.
	seed := [32]byte{123, 42}

	// rounds of shuffling, constant in spec
	rounds := uint8(90)

	for _, listSize := range listSizes {
		// list to test
		testIndices := make([]uint64, listSize, listSize)
		// fill
		for i := uint64(0); i < listSize; i++ {
			testIndices[i] = i
		}
		// benchmark!
		b.Run(fmt.Sprintf("ShuffleList_%d", listSize), func(ib *testing.B) {
			for i := 0; i < ib.N; i++ {
				eth2shuffle.ShuffleList(hashFn, testIndices, rounds, seed)
			}
		})
	}
}

//// TODO optimize memory allocations even more by analysis of statistics
//func BenchmarkShuffleListWithAllocsReport(b *testing.B) {
//	b.ReportAllocs()
//	BenchmarkShuffleList(b)
//}
