//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"encoding/binary"
	"testing"

	"github.com/google/btree"
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

func u64Slice(in []byte) ([]uint64, bool) {
	if len(in) == 0 || len(in)%8 != 0 {
		return nil, false
	}
	res := make([]uint64, len(in)/8)
	for i := 0; i < len(res)-1; i++ {
		res[i] = binary.BigEndian.Uint64(in[i*8:])
	}
	return res, true
}
func u256Slice(in []byte) ([]uint256.Int, bool) {
	if len(in) == 0 || len(in)%32 != 0 {
		return nil, false
	}
	res := make([]uint256.Int, len(in)/32)
	for i := 0; i < len(res)-1; i++ {
		res[i].SetBytes(in[i*32 : (i+1)*32])
	}
	return res, true
}

func poolsFromFuzzBytes(s1, s2, s3 []uint8, rawTxNonce, rawValues, rawSender, rawSenderNonce, rawSenderBalance []byte) (pending, baseFee, queued *SubPool, ok bool) {
	txNonce, ok := u64Slice(rawTxNonce)
	if !ok {
		return nil, nil, nil, false
	}
	values, ok := u256Slice(rawValues)
	if !ok {
		return nil, nil, nil, false
	}
	sender, ok := u64Slice(rawSender)
	if !ok {
		return nil, nil, nil, false
	}
	senderNonce, ok := u64Slice(rawSenderNonce)
	if !ok {
		return nil, nil, nil, false
	}
	senderBalance, ok := u256Slice(rawSenderBalance)
	if !ok {
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

	iTx, iSender := 0, 0
	senders := map[uint64]SenderInfo{}

	for _, id := range sender {
		_, ok = senders[id]
		if ok {
			continue
		}

		senders[id] = SenderInfo{
			nonce:      senderNonce[uint(id)%uint(len(senderNonce))],
			balance:    senderBalance[uint(id)%uint(len(senderBalance))],
			txNonce2Tx: &Nonce2Tx{btree.New(32)},
		}
	}

	pending, baseFee, queued = NewSubPool(), NewSubPool(), NewSubPool()
	for _, s := range s1 {
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce:    txNonce[iTx%len(txNonce)],
			senderID: sender[iTx%len(sender)],
			value:    values[iTx%len(values)],
		}}
		senders[mt.Tx.senderID].txNonce2Tx.ReplaceOrInsert(&nonce2TxItem{mt})
		pending.Add(mt, PendingSubPool)
		iTx++
		iSender++
	}
	for _, s := range s2 {
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce:    txNonce[iTx%len(txNonce)],
			senderID: sender[iTx%len(sender)],
			value:    values[iTx%len(values)],
		}}
		senders[mt.Tx.senderID].txNonce2Tx.ReplaceOrInsert(&nonce2TxItem{mt})
		baseFee.Add(mt, BaseFeeSubPool)
		iTx++
		iSender++
	}
	for _, s := range s3 {
		mt := &MetaTx{SubPool: SubPoolMarker(s & 0b11111), Tx: &TxSlot{
			nonce:    txNonce[iTx%len(txNonce)],
			senderID: sender[iTx%len(sender)],
			value:    values[iTx%len(values)],
		}}
		senders[mt.Tx.senderID].txNonce2Tx.ReplaceOrInsert(&nonce2TxItem{mt})
		queued.Add(mt, QueuedSubPool)
		iTx++
		iSender++
	}

	return pending, baseFee, queued, true
}

func FuzzPromoteStep5(f *testing.F) {
	var u64 = [8]byte{1}
	var u256 = [32]byte{1}
	f.Add(
		[]uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000},
		u64[:], u64[:], u64[:], u256[:], u256[:])
	f.Add(
		[]uint8{0b11111}, []uint8{0b11111}, []uint8{0b11110, 0b0, 0b1010},
		u64[:], u64[:], u64[:], u256[:], u256[:])
	f.Add(
		[]uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111}, []uint8{0b11000, 0b00101, 0b000111},
		u64[:], u64[:], u64[:], u256[:], u256[:])
	f.Fuzz(func(t *testing.T, s1, s2, s3 []uint8, txNonce, values, sender, senderNonce, senderBalance []byte) {
		t.Parallel()
		assert := assert.New(t)

		pending, baseFee, queued, ok := poolsFromFuzzBytes(s1, s2, s3, txNonce, values, sender, senderNonce, senderBalance)
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
