package state

import (
	"fmt"
	"testing"
)

func BenchmarkColdSloadBlock(b *testing.B) {
	blk := newColdSloadBlock(b)
	sloads := coldSloadTxs*(coldSloadPerTx+1) + (coldSloadTxs-1)*coldSloadHotPerTx
	for _, touched := range []bool{false, true} {
		b.Run(fmt.Sprintf("touched=%t", touched), func(b *testing.B) {
			blk.touched = touched
			b.ReportAllocs()
			for b.Loop() {
				blk.run(b, nil)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*sloads), "ns/sload")
		})
	}
}
