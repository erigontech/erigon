//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// https://blog.golang.org/fuzz-beta
// golang.org/s/draft-fuzzing-design
//gotip doc testing
//gotip doc testing.F
//gotip doc testing.F.Add
//gotip doc testing.F.Fuzz

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool

func FuzzTwoQueue(f *testing.F) {
	f.Add([]uint8{0b11000, 0b00101, 0b000111})
	f.Add([]uint8{0b10101, 0b11110, 0b11101, 0b10001})
	f.Fuzz(func(t *testing.T, in []uint8) {
		t.Parallel()
		assert := assert.New(t)
		{
			sub := NewSubPool()
			for _, i := range in {
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
			}
			assert.Equal(len(in), sub.best.Len())
			assert.Equal(len(in), sub.worst.Len())
			assert.Equal(len(in), sub.Len())

			var prevBest *uint8
			i := sub.Len()
			for sub.Len() > 0 {
				best := uint8(sub.Best().SubPool)
				assert.Equal(best, uint8(sub.PopBest().SubPool))
				if prevBest != nil {
					assert.LessOrEqual(best, *prevBest)
				}
				prevBest = &best
				i--
			}
			assert.Zero(i)
		}

		{
			sub := NewSubPool()
			for _, i := range in {
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
			}
			var prev *uint8
			i := sub.Len()
			for sub.Len() > 0 {
				worst := uint8(sub.Worst().SubPool)
				assert.Equal(worst, uint8(sub.PopWorst().SubPool))
				if prev != nil {
					assert.GreaterOrEqual(worst, *prev)
				}
				prev = &worst
				i--
			}
			assert.Zero(i)
		}
	})
}

func FuzzPromoteStep(f *testing.F) {
	f.Add([]uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000})
	f.Add([]uint8{0b11111}, []uint8{0b11111}, []uint8{0b11110, 0b0, 0b1010})
	f.Add([]uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111})
	f.Fuzz(func(t *testing.T, s1, s2, s3 []uint8) {
		t.Parallel()
		pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
		for _, i := range s1 {
			pending.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
		}
		for _, i := range s2 {
			baseFee.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
		}
		for _, i := range s3 {
			queued.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
		}
		PromoteStep(pending, baseFee, queued)

		best, worst := pending.Best(), pending.Worst()
		_ = best
		if worst != nil && worst.SubPool < 0b11110 {
			t.Fatalf("pending worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}

		best, worst = baseFee.Best(), baseFee.Worst()
		_ = best
		if worst != nil && worst.SubPool < 0b11100 {
			t.Fatalf("baseFee worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}

		best, worst = queued.Best(), queued.Worst()
		_ = best
		if worst != nil && worst.SubPool < 0b10000 {
			t.Fatalf("queued worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}

	})
}
