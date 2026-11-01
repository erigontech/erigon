package simpleseq

import (
	"fmt"
	"testing"
)

func BenchmarkSimpleSequenceSeek(b *testing.B) {
	for _, size := range []int{1, 2, 4, 16} {
		s := makeSequence(size)
		minV := s.Min()
		maxV := s.Max()
		midV := s.Get(uint64(size / 2))

		b.Run(fmt.Sprintf("n=%d/hit_first", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(minV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/hit_mid", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(midV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/hit_last", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(maxV)
			}
		})
		b.Run(fmt.Sprintf("n=%d/miss", size), func(b *testing.B) {
			for b.Loop() {
				s.Seek(maxV + 1)
			}
		})
	}
}
