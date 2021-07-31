//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
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
		for i := range in {
			if in[i] > 0b11111 {
				t.Skip()
			}
		}
		assert := assert.New(t)
		{
			sub := NewSubPool()
			for _, i := range in {
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)}, PendingSubPool)
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
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)}, PendingSubPool)
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

func poolsFromFuzzBytes(s1, s2, s3 []uint8, nonce, sender, values []byte) (pending, baseFee, queued *SubPool, ok bool) {
	if len(nonce) == 0 || len(nonce)%8 != 0 {
		return nil, nil, nil, false
	}
	if len(sender) == 0 || len(sender)%20 != 0 {
		return nil, nil, nil, false
	}
	if len(values) == 0 || len(values)%32 != 0 {
		return nil, nil, nil, false
	}

	for i := range s1 {
		if s1[i] > 0b11111 {
			return nil, nil, nil, false
		}
	}
	for i := range s2 {
		if s2[i] > 0b11111 {
			return nil, nil, nil, false
		}
	}
	for i := range s3 {
		if s3[i] > 0b11111 {
			return nil, nil, nil, false
		}
	}

	iNonce, iSender, iValue := 0, 0, 0
	pending, baseFee, queued = NewSubPool(), NewSubPool(), NewSubPool()
	var ss [20]byte
	var vb [4]uint64
	for _, s := range s1 {
		copy(ss[:], sender[(iSender*20)%len(sender):])
		for i := 0; i < 4; i++ {
			vb[i] = binary.BigEndian.Uint64(values[(iValue*8)%len(values):])
			iValue++
		}
		value := uint256.Int(vb)
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss, value: value,
		}}
		pending.Add(mt, PendingSubPool)
		iNonce++
		iSender++
	}
	for _, s := range s2 {
		copy(ss[:], sender[(iSender*20)%len(sender):])
		for i := 0; i < 4; i++ {
			vb[i] = binary.BigEndian.Uint64(values[(iValue*8)%len(values):])
			iValue++
		}
		value := uint256.Int(vb)
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss, value: value,
		}}
		baseFee.Add(mt, BaseFeeSubPool)
		iNonce++
		iSender++
	}
	for _, s := range s3 {
		copy(ss[:], sender[(iSender*20)%len(sender):])
		for i := 0; i < 4; i++ {
			vb[i] = binary.BigEndian.Uint64(values[(iValue*8)%len(values):])
			iValue++
		}
		value := uint256.Int(vb)
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce: binary.BigEndian.Uint64(nonce[(iNonce*8)%len(nonce):]), sender: ss, value: value,
		}}
		queued.Add(mt, QueuedSubPool)
		iNonce++
		iSender++
	}

	return pending, baseFee, queued, true
}

func FuzzPromoteStep4(f *testing.F) {
	var nNonce = [8]byte{1}
	var nAddr = [20]byte{1}
	f.Add([]uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, nNonce[:], nAddr[:], uint256.NewInt(123).Bytes())
	f.Add([]uint8{0b11111}, []uint8{0b11111}, []uint8{0b11110, 0b0, 0b1010}, nNonce[:], nAddr[:], uint256.NewInt(678).Bytes())
	f.Add([]uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, nNonce[:], nAddr[:], uint256.NewInt(987654321).Bytes())
	f.Fuzz(func(t *testing.T, s1, s2, s3 []uint8, nonce, sender, values []byte) {
		t.Parallel()
		assert := assert.New(t)

		pending, baseFee, queued, ok := poolsFromFuzzBytes(s1, s2, s3, nonce, sender, values)
		if !ok {
			t.Skip()
		}
		PromoteStep(pending, baseFee, queued)

		best, worst := pending.Best(), pending.Worst()
		if worst != nil && worst.SubPool < 0b11110 {
			t.Fatalf("pending worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)

		best, worst = baseFee.Best(), baseFee.Worst()
		if worst != nil && worst.SubPool < 0b11100 {
			t.Fatalf("baseFee worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)

		best, worst = queued.Best(), queued.Worst()
		if worst != nil && worst.SubPool < 0b10000 {
			t.Fatalf("queued worst too small %b, input: \n%x\n%x\n%x", worst.SubPool, s1, s2, s3)
		}
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
	})
}
