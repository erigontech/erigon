//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"encoding/binary"
	"testing"
)

// https://blog.golang.org/fuzz-beta
// golang.org/s/draft-fuzzing-design
//gotip doc testing
//gotip doc testing.F
//gotip doc testing.F.Add
//gotip doc testing.F.Fuzz

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool
/*
func FuzzTwoQueue(f *testing.F) {
	f.Add([]uint8{0b11000, 0b00101, 0b000111})
	f.Add([]uint8{0b10101, 0b11110, 0b11101, 0b10001})
	f.Fuzz(func(t *testing.T, in []uint8) {
		t.Parallel()
		for i := range in {
			if in[i] > 0b11111 {
				t.Skip()
			}
		}
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
			assert.Zero(sub.Len())
			assert.Zero(sub.best.Len())
			assert.Zero(sub.worst.Len())
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
			assert.Zero(sub.Len())
			assert.Zero(sub.best.Len())
			assert.Zero(sub.worst.Len())
		}
	})
}
*/

func FuzzPromoteStep2(f *testing.F) {
	var nNonce = [8]byte{1}
	var nAddr = [20]byte{1}

	f.Add([]uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, nNonce[:], nAddr[:])
	f.Add([]uint8{0b11111}, []uint8{0b11111}, []uint8{0b11110, 0b0, 0b1010}, nNonce[:], nAddr[:])
	f.Add([]uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, nNonce[:], nAddr[:])
	f.Fuzz(func(t *testing.T, s1, s2, s3 []uint8, nonce []byte, sender []byte) {
		t.Parallel()
		if len(nonce) == 0 || len(nonce)%8 != 0 || len(sender) == 0 || len(sender)%20 != 0 {
			t.Skip()
		}
		for i := range s1 {
			if s1[i] > 0b11111 {
				t.Skip()
			}
		}
		for i := range s2 {
			if s2[i] > 0b11111 {
				t.Skip()
			}
		}
		for i := range s3 {
			if s3[i] > 0b11111 {
				t.Skip()
			}
		}

		iNonce, iSenders := 0, 0
		pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
		var ss [20]byte
		for _, s := range s1 {
			copy(ss[:], sender[(iSenders*20)%len(sender):])
			pending.Add(&MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss}})
			iNonce++
			iSenders++
		}
		for _, s := range s2 {
			copy(ss[:], sender[(iSenders*20)%len(sender):])
			baseFee.Add(&MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss}})
			iNonce++
			iSenders++
		}
		for _, i := range s3 {
			copy(ss[:], sender[(iSenders*20)%len(sender):])
			queued.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111), Tx: &TxSlot{nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss}})
			iNonce++
			iSenders++
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
