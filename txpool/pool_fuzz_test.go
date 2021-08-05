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
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}}, PendingSubPool)
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
				sub.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}}, PendingSubPool)
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

func poolsFromFuzzBytes(rawTxNonce, rawValues, rawTips, rawSender, rawSenderNonce, rawSenderBalance []byte) (sendersInfo map[uint64]*senderInfo, senderIDs map[string]uint64, txs TxSlots, ok bool) {
	if len(rawTxNonce)/8 != len(rawValues)/32 {
		return nil, nil, txs, false
	}
	if len(rawSender)/20 != len(rawSenderNonce)/8 {
		return nil, nil, txs, false
	}
	if len(rawSender)/20 != len(rawSenderBalance)/32 {
		return nil, nil, txs, false
	}
	if len(rawTxNonce)/8 != len(rawTips)/8 {
		return nil, nil, txs, false
	}
	senderNonce, ok := u64Slice(rawSenderNonce)
	if !ok {
		return nil, nil, txs, false
	}
	for i := 0; i < len(senderNonce); i++ {
		if senderNonce[i] == 0 {
			return nil, nil, txs, false
		}
	}

	txNonce, ok := u64Slice(rawTxNonce)
	if !ok {
		return nil, nil, txs, false
	}
	tips, ok := u64Slice(rawTips)
	if !ok {
		return nil, nil, txs, false
	}
	values, ok := u256Slice(rawValues)
	if !ok {
		return nil, nil, txs, false
	}
	senderBalance, ok := u256Slice(rawSenderBalance)
	if !ok {
		return nil, nil, txs, false
	}

	sendersInfo = map[uint64]*senderInfo{}
	senderIDs = map[string]uint64{}
	for i := 0; i < len(senderNonce); i++ {
		sendersInfo[uint64(i)] = newSenderInfo(senderNonce[i], senderBalance[i])
		senderIDs[string(rawSender[i*20:(i+1)*20])] = uint64(i)
	}
	for i := range txNonce {
		txs.txs = append(txs.txs, &TxSlot{
			nonce: txNonce[i],
			value: values[i],
			tip:   tips[i],
		})
		txs.senders = rawSender
	}
	return sendersInfo, senderIDs, txs, true
}

func iterateSubPoolUnordered(subPool *SubPool, f func(tx *MetaTx)) {
	for i := 0; i < subPool.best.Len(); i++ {
		f((*subPool.best)[i])
	}
}

func splitDataset(in TxSlots) (TxSlots, TxSlots, TxSlots, TxSlots) {
	p1, p2, p3, p4 := TxSlots{}, TxSlots{}, TxSlots{}, TxSlots{}
	l := len(in.txs) / 4

	p1.txs = in.txs[:l]
	p1.senders = in.senders[:l]

	p2.txs = in.txs[l : 2*l]
	p2.senders = in.senders[l : 2*l]

	p3.txs = in.txs[2*l : 3*l]
	p2.senders = in.senders[2*l : 3*l]

	return p1, p2, p3, p4
}

func FuzzOnNewBlocks5(f *testing.F) {
	var u64 = [8]byte{1}
	var u256 = [32]byte{1}
	f.Add(u64[:], u64[:], u64[:], u64[:], u256[:], u256[:], 123, 456)
	f.Add(u64[:], u64[:], u64[:], u64[:], u256[:], u256[:], 78, 100)
	f.Add(u64[:], u64[:], u64[:], u64[:], u256[:], u256[:], 100_000, 101_000)
	f.Fuzz(func(t *testing.T, txNonce, values, tips, sender, senderNonce, senderBalance []byte, protocolBaseFee, blockBaseFee uint64) {
		t.Parallel()
		assert := assert.New(t)

		senders, senderIDs, txs, ok := poolsFromFuzzBytes(txNonce, values, tips, sender, senderNonce, senderBalance)
		if !ok {
			t.Skip()
		}

		ch := make(chan Hashes, 100)
		pool := New(ch)
		pool.senderInfo = senders
		pool.senderIDs = senderIDs

		check := func(unwindTxs, minedTxs TxSlots) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued

			best, worst := pending.Best(), pending.Worst()
			assert.LessOrEqual(pending.Len(), PendingSubPoolLimit)
			assert.False(worst != nil && best == nil)
			assert.False(worst == nil && best != nil)
			if worst != nil && worst.SubPool < 0b11110 {
				t.Fatalf("pending worst too small %b", worst.SubPool)
			}
			iterateSubPoolUnordered(pending, func(tx *MetaTx) {
				i := tx.Tx
				assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
				if tx.SubPool&EnoughBalance > 0 {
					assert.True(tx.SenderHasEnoughBalance)
				}

				need := uint256.NewInt(i.gas)
				need = need.Mul(need, uint256.NewInt(i.feeCap))
				assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
				assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))

				// side data structures must have all txs
				assert.True(senders[i.senderID].txNonce2Tx.Has(&nonce2TxItem{tx}))
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok)

				// pools can't have more then 1 tx with same SenderID+Nonce
				iterateSubPoolUnordered(queued, func(mtx2 *MetaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.senderID == i.senderID && tx2.nonce == i.nonce)
				})
				iterateSubPoolUnordered(pending, func(mtx2 *MetaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.senderID == i.senderID && tx2.nonce == i.nonce)
				})
			})

			best, worst = baseFee.Best(), baseFee.Worst()

			assert.False(worst != nil && best == nil)
			assert.False(worst == nil && best != nil)
			assert.LessOrEqual(baseFee.Len(), BaseFeeSubPoolLimit)
			if worst != nil && worst.SubPool < 0b11100 {
				t.Fatalf("baseFee worst too small %b", worst.SubPool)
			}
			iterateSubPoolUnordered(baseFee, func(tx *MetaTx) {
				i := tx.Tx
				assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
				if tx.SubPool&EnoughBalance > 0 {
					assert.True(tx.SenderHasEnoughBalance)
				}

				need := uint256.NewInt(i.gas)
				need = need.Mul(need, uint256.NewInt(i.feeCap))
				assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
				assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))

				assert.True(senders[i.senderID].txNonce2Tx.Has(&nonce2TxItem{tx}))
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok)
			})

			best, worst = queued.Best(), queued.Worst()
			assert.LessOrEqual(queued.Len(), QueuedSubPoolLimit)
			assert.False(worst != nil && best == nil)
			assert.False(worst == nil && best != nil)
			if worst != nil && worst.SubPool < 0b10000 {
				t.Fatalf("queued worst too small %b", worst.SubPool)
			}
			iterateSubPoolUnordered(queued, func(tx *MetaTx) {
				i := tx.Tx
				assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
				if tx.SubPool&EnoughBalance > 0 {
					assert.True(tx.SenderHasEnoughBalance)
				}

				need := uint256.NewInt(i.gas)
				need = need.Mul(need, uint256.NewInt(i.feeCap))
				assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
				assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))

				assert.True(senders[i.senderID].txNonce2Tx.Has(&nonce2TxItem{tx}))
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok)
			})

			// all txs in side data structures must be in some queue
			for _, txn := range pool.byHash {
				assert.True(txn.bestIndex >= 0)
				assert.True(txn.worstIndex >= 0)
			}
			for i := range senders {
				//assert.True(senders[i].txNonce2Tx.Len() > 0)
				senders[i].txNonce2Tx.Ascend(func(i btree.Item) bool {
					mt := i.(*nonce2TxItem).MetaTx
					assert.True(mt.worstIndex >= 0)
					assert.True(mt.bestIndex >= 0)
					return true
				})
			}

			// mined txs must be removed
			for i := range minedTxs.txs {
				_, ok = pool.byHash[string(minedTxs.txs[i].idHash[:])]
				assert.False(ok)
			}
			newHashes := <-ch
			assert.Equal(len(unwindTxs.txs), newHashes.Len())
		}

		unwindTxs, minedTxs, p2pReceived, minedTxs2 := splitDataset(txs)
		err := pool.OnNewBlock(unwindTxs, minedTxs, protocolBaseFee, blockBaseFee)
		assert.NoError(err)
		check(unwindTxs, minedTxs)
		// unwind everything and switch to new fork
		err = pool.OnNewBlock(minedTxs, minedTxs2, protocolBaseFee, blockBaseFee)
		assert.NoError(err)
		check(minedTxs, minedTxs2)
		// add some remote txs from p2p
		err = pool.OnNewTxs(p2pReceived)
		assert.NoError(err)
	})

}
