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

func poolsFromFuzzBytes(rawTxNonce, rawValues, rawSender, rawSenderNonce, rawSenderBalance []byte) (sendersInfo map[uint64]SenderInfo, txs []*TxSlot, ok bool) {
	if len(rawTxNonce)/8 != len(rawValues)/32 {
		return nil, nil, false
	}
	if len(rawSender)/8 != len(rawSenderNonce)/8 {
		return nil, nil, false
	}
	if len(rawSender)/8 != len(rawSenderBalance)/32 {
		return nil, nil, false
	}

	txNonce, ok := u64Slice(rawTxNonce)
	if !ok {
		return nil, nil, false
	}
	values, ok := u256Slice(rawValues)
	if !ok {
		return nil, nil, false
	}
	sender, ok := u64Slice(rawSender)
	if !ok {
		return nil, nil, false
	}
	senderNonce, ok := u64Slice(rawSenderNonce)
	if !ok {
		return nil, nil, false
	}
	senderBalance, ok := u256Slice(rawSenderBalance)
	if !ok {
		return nil, nil, false
	}

	senders := map[uint64]SenderInfo{}
	for i, id := range sender {
		senders[id] = SenderInfo{
			nonce:      senderNonce[i],
			balance:    senderBalance[i],
			txNonce2Tx: &Nonce2Tx{btree.New(32)},
		}
	}
	for i := range txNonce {
		txs = append(txs, &TxSlot{
			nonce:    txNonce[i],
			value:    values[i],
			senderID: sender[i%len(sender)],
		})
	}
	return senders, txs, true
}

func iterateSubPoolUnordered(subPool *SubPool, f func(tx *MetaTx)) {
	for i := 0; i < subPool.best.Len(); i++ {
		f((*subPool.best)[i])
	}
}

func FuzzOnNewBlocks3(f *testing.F) {
	var u64 = [8]byte{1}
	var u256 = [32]byte{1}
	f.Add(u64[:], u64[:], u64[:], u256[:], u256[:], 123, 456)
	f.Add(u64[:], u64[:], u64[:], u256[:], u256[:], 78, 100)
	f.Add(u64[:], u64[:], u64[:], u256[:], u256[:], 100_000, 101_000)
	f.Fuzz(func(t *testing.T, txNonce, values, sender, senderNonce, senderBalance []byte, protocolBaseFee, blockBaseFee uint64) {
		t.Parallel()
		assert := assert.New(t)

		senders, txs, ok := poolsFromFuzzBytes(txNonce, values, sender, senderNonce, senderBalance)
		if !ok {
			t.Skip()
		}
		pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
		OnNewBlocks(senders, txs, protocolBaseFee, blockBaseFee, pending, baseFee, queued)

		best, worst := pending.Best(), pending.Worst()
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		if worst != nil && worst.SubPool < 0b11110 {
			t.Fatalf("pending worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(pending, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

		best, worst = baseFee.Best(), baseFee.Worst()

		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		if worst != nil && worst.SubPool < 0b11100 {
			t.Fatalf("baseFee worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(baseFee, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

		best, worst = queued.Best(), queued.Worst()
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		if worst != nil && worst.SubPool < 0b10000 {
			t.Fatalf("queued worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(queued, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

	})

}
