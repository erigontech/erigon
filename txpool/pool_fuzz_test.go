//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	if len(in) < 8 {
		return nil, false
	}
	res := make([]uint64, len(in)/8)
	for i := 0; i < len(res); i++ {
		res[i] = binary.BigEndian.Uint64(in[i*8:])
	}
	return res, true
}
func u8Slice(in []byte) ([]uint64, bool) {
	if len(in) < 1 {
		return nil, false
	}
	res := make([]uint64, len(in))
	for i := 0; i < len(res); i++ {
		res[i] = uint64(in[i])
	}
	return res, true
}
func u16Slice(in []byte) ([]uint64, bool) {
	if len(in) < 2 {
		return nil, false
	}
	res := make([]uint64, len(in)/2)
	for i := 0; i < len(res); i++ {
		res[i] = uint64(binary.BigEndian.Uint16(in[i*2:]))
	}
	return res, true
}
func u256Slice(in []byte) ([]uint256.Int, bool) {
	if len(in) < 1 {
		return nil, false
	}
	res := make([]uint256.Int, len(in))
	for i := 0; i < len(res); i++ {
		res[i].SetUint64(uint64(in[i]))
	}
	return res, true
}

func parseSenders(in []byte) (senders Addresses, nonces []uint64, balances []uint256.Int) {
	zeroes := [19]byte{}
	for i := 0; i < len(in)-(1+1+1-1); i += 1 + 1 + 1 {
		senders = append(senders, zeroes[:]...)
		senders = append(senders, in[i:i+1]...)
		nonce := uint64(in[i+1])
		if nonce == 0 {
			nonce = 1
		}
		nonces = append(nonces, nonce)
		balances = append(balances, *uint256.NewInt(uint64(in[i+1+1])))
	}
	return
}

func parseTxs(in []byte) (nonces, tips []uint64, values []uint256.Int) {
	for i := 0; i < len(in)-(1+1+1-1); i += 1 + 1 + 1 {
		nonce := uint64(in[i])
		if nonce == 0 {
			nonce = 1
		}
		nonces = append(nonces, nonce)
		tips = append(tips, uint64(in[i+1]))
		values = append(values, *uint256.NewInt(uint64(in[i+1+1])))
	}
	return
}

func poolsFromFuzzBytes(rawTxNonce, rawValues, rawTips, rawSender []byte) (sendersInfo map[uint64]*senderInfo, senderIDs map[string]uint64, txs TxSlots, ok bool) {
	if len(rawTxNonce) < 1 || len(rawValues) < 1 || len(rawTips) < 1 || len(rawSender) < 1+1+1 {
		return nil, nil, txs, false
	}
	senders, senderNonce, senderBalance := parseSenders(rawSender)
	txNonce, ok := u8Slice(rawTxNonce)
	if !ok {
		return nil, nil, txs, false
	}
	tips, ok := u8Slice(rawTips)
	if !ok {
		return nil, nil, txs, false
	}
	values, ok := u256Slice(rawValues)
	if !ok {
		return nil, nil, txs, false
	}

	sendersInfo = map[uint64]*senderInfo{}
	senderIDs = map[string]uint64{}
	for i := 0; i < len(senderNonce); i++ {
		senderID := uint64(i + 1) //non-zero expected
		sendersInfo[senderID] = newSenderInfo(senderNonce[i], senderBalance[i%len(senderBalance)])
		senderIDs[string(senders.At(i%senders.Len()))] = senderID
	}
	for i := range txNonce {
		txs.txs = append(txs.txs, &TxSlot{
			nonce: txNonce[i],
			value: values[i%len(values)],
			tip:   tips[i%len(tips)],
		})
		txs.senders = append(txs.senders, senders.At(i%senders.Len())...)
		txs.isLocal = append(txs.isLocal, false)
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
	p1.isLocal = in.isLocal[:l]
	p1.senders = in.senders[:l*20]

	p2.txs = in.txs[l : 2*l]
	p2.isLocal = in.isLocal[l : 2*l]
	p2.senders = in.senders[l*20 : 2*l*20]

	p3.txs = in.txs[2*l : 3*l]
	p3.isLocal = in.isLocal[2*l : 3*l]
	p3.senders = in.senders[2*l*20 : 3*l*20]

	p4.txs = in.txs[3*l : 4*l]
	p4.isLocal = in.isLocal[3*l : 4*l]
	p4.senders = in.senders[3*l*20 : 4*l*20]

	return p1, p2, p3, p4
}

func FuzzOnNewBlocks7(f *testing.F) {
	var u64 = [1 * 4]byte{1}
	var sender = [1 + 1 + 1]byte{1}
	f.Add(u64[:], u64[:], u64[:], sender[:], 123, 456)
	f.Add(u64[:], u64[:], u64[:], sender[:], 78, 100)
	f.Add(u64[:], u64[:], u64[:], sender[:], 100_000, 101_000)
	f.Fuzz(func(t *testing.T, txNonce, values, tips, sender []byte, protocolBaseFee, blockBaseFee uint64) {
		t.Parallel()
		if protocolBaseFee == 0 || blockBaseFee == 0 {
			t.Skip()
		}
		if len(sender) < 1+1+1 {
			t.Skip()
		}

		senders, senderIDs, txs, ok := poolsFromFuzzBytes(txNonce, values, tips, sender)
		if !ok {
			t.Skip()
		}

		assert := assert.New(t)
		err := txs.Valid()
		assert.NoError(err)

		ch := make(chan Hashes, 100)
		pool := New(ch)
		pool.senderInfo = senders
		pool.senderIDs = senderIDs
		check := func(unwindTxs, minedTxs TxSlots) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued
			if pending.Len() > 0 || baseFee.Len() > 0 || queued.Len() > 0 {
				fmt.Printf("len: %d,%d,%d\n", pending.Len(), baseFee.Len(), queued.Len())
			}

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
		}

		checkNotify := func(unwindTxs, minedTxs TxSlots) {
			select {
			case newHashes := <-ch:
				//assert.Equal(len(unwindTxs.txs), newHashes.Len())
				assert.Greater(len(newHashes), 0)
				for i := 0; i < newHashes.Len(); i++ {
					foundInUnwind := false
					foundInMined := false
					newHash := newHashes.At(i)
					for j := range unwindTxs.txs {
						if bytes.Equal(unwindTxs.txs[j].idHash[:], newHash) {
							foundInUnwind = true
							break
						}
					}
					for j := range minedTxs.txs {
						if bytes.Equal(unwindTxs.txs[j].idHash[:], newHash) {
							foundInMined = true
							break
						}
					}
					assert.True(foundInUnwind)
					assert.False(foundInMined)
				}
			default:

				//TODO: no notifications - means pools must be empty (unchanged)
			}
		}

		// go to first fork
		unwindTxs, minedTxs1, p2pReceived, minedTxs2 := splitDataset(txs)
		err = pool.OnNewBlock(unwindTxs, minedTxs1, protocolBaseFee, blockBaseFee)
		assert.NoError(err)
		check(unwindTxs, minedTxs1)
		checkNotify(unwindTxs, minedTxs1)

		// unwind everything and switch to new fork (need unwind mined now)
		err = pool.OnNewBlock(minedTxs1, minedTxs2, protocolBaseFee, blockBaseFee)
		assert.NoError(err)
		check(minedTxs1, minedTxs2)
		checkNotify(minedTxs1, minedTxs2)

		// add some remote txs from p2p
		err = pool.OnNewTxs(p2pReceived)
		assert.NoError(err)
		check(p2pReceived, TxSlots{})
		checkNotify(p2pReceived, TxSlots{})
	})

}
