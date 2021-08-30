//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// https://blog.golang.org/fuzz-beta
// golang.org/s/draft-fuzzing-design
//gotip doc testing
//gotip doc testing.F
//gotip doc testing.F.OnNewTxs
//gotip doc testing.F.Fuzz

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool

func init() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
}

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
			sub := NewSubPool(PendingSubPool)
			for _, i := range in {
				sub.Add(&metaTx{subPool: SubPoolMarker(i & 0b11111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}})
			}
			assert.Equal(len(in), sub.best.Len())
			assert.Equal(len(in), sub.worst.Len())
			assert.Equal(len(in), sub.Len())

			var prevBest *uint8
			i := sub.Len()
			for sub.Len() > 0 {
				best := uint8(sub.Best().subPool)
				assert.Equal(best, uint8(sub.PopBest().subPool))
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
			sub := NewSubPool(PendingSubPool)
			for _, i := range in {
				sub.Add(&metaTx{subPool: SubPoolMarker(i & 0b11111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}})
			}
			var prev *uint8
			i := sub.Len()
			for sub.Len() > 0 {
				worst := uint8(sub.Worst().subPool)
				assert.Equal(worst, uint8(sub.PopWorst().subPool))
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
		res[i] = uint64(in[i] % 32)
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
		res[i].SetUint64(uint64(in[i] % 32))
	}
	return res, true
}

func parseSenders(in []byte) (senders Addresses, nonces []uint64, balances []uint256.Int) {
	zeroes := [20]byte{}
	for i := 0; i < len(in)-(1+1+1-1); i += 1 + 1 + 1 {
		zeroes[19] = in[i] % 8
		senders = append(senders, zeroes[:]...)
		nonce := uint64(in[i+1] % 8)
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

func poolsFromFuzzBytes(rawTxNonce, rawValues, rawTips, rawFeeCap, rawSender []byte) (sendersInfo map[uint64]*senderInfo, senderIDs map[string]uint64, txs TxSlots, ok bool) {
	if len(rawTxNonce) < 1 || len(rawValues) < 1 || len(rawTips) < 1 || len(rawFeeCap) < 1 || len(rawSender) < 1+1+1 {
		return nil, nil, txs, false
	}
	senders, senderNonce, senderBalance := parseSenders(rawSender)
	txNonce, ok := u8Slice(rawTxNonce)
	if !ok {
		return nil, nil, txs, false
	}
	feeCap, ok := u8Slice(rawFeeCap)
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
	txs.txs = make([]*TxSlot, len(txNonce))
	parseCtx := NewTxParseContext()
	parseCtx.WithSender(false)
	for i := range txNonce {
		txs.txs[i] = &TxSlot{
			nonce:  txNonce[i],
			value:  values[i%len(values)],
			tip:    tips[i%len(tips)],
			feeCap: feeCap[i%len(feeCap)],
		}
		txRlp := fakeRlpTx(txs.txs[i], senders.At(i%senders.Len()))
		_, err := parseCtx.ParseTransaction(txRlp, 0, txs.txs[i], nil)
		if err != nil {
			panic(err)
		}
		txs.senders = append(txs.senders, senders.At(i%senders.Len())...)
		txs.isLocal = append(txs.isLocal, false)
	}

	return sendersInfo, senderIDs, txs, true
}

// fakeRlpTx add anything what identifying tx to `data` to make hash unique
func fakeRlpTx(slot *TxSlot, data []byte) []byte {
	dataLen := rlp.U64Len(1) + //chainID
		rlp.U64Len(slot.nonce) + rlp.U64Len(slot.tip) + rlp.U64Len(slot.feeCap) +
		rlp.U64Len(0) + // gas
		rlp.StringLen(0) + // dest addr
		rlp.U256Len(&slot.value) +
		rlp.StringLen(len(data)) + // data
		rlp.ListPrefixLen(0) + //access list
		+3 // v,r,s

	buf := make([]byte, 1+rlp.ListPrefixLen(dataLen)+dataLen)
	buf[0] = byte(DynamicFeeTxType)
	p := 1
	p += rlp.EncodeListPrefix(dataLen, buf[p:])
	p += rlp.EncodeU64(1, buf[p:])
	p += rlp.EncodeU64(slot.nonce, buf[p:])
	p += rlp.EncodeU64(slot.tip, buf[p:])
	p += rlp.EncodeU64(slot.feeCap, buf[p:])
	p += rlp.EncodeU64(0, buf[p:])           //gas
	p += rlp.EncodeString([]byte{}, buf[p:]) //destrination addr
	bb := bytes.NewBuffer(buf[p:p])
	_ = slot.value.EncodeRLP(bb)
	p += rlp.U256Len(&slot.value)
	p += rlp.EncodeString(data, buf[p:])  //data
	p += rlp.EncodeListPrefix(0, buf[p:]) // access list
	p += rlp.EncodeU64(1, buf[p:])        //v
	p += rlp.EncodeU64(1, buf[p:])        //r
	p += rlp.EncodeU64(1, buf[p:])        //s
	return buf[:]
}

func iterateSubPoolUnordered(subPool *SubPool, f func(tx *metaTx)) {
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

func FuzzOnNewBlocks12(f *testing.F) {
	var u64 = [1 * 4]byte{1}
	var sender = [1 + 1 + 1]byte{1}
	f.Add(u64[:], u64[:], u64[:], u64[:], sender[:], 12)
	f.Add(u64[:], u64[:], u64[:], u64[:], sender[:], 14)
	f.Add(u64[:], u64[:], u64[:], u64[:], sender[:], 123)
	f.Fuzz(func(t *testing.T, txNonce, values, tips, feeCap, sender []byte, currentBaseFee1 uint8) {
		//t.Parallel()

		currentBaseFee := uint64(currentBaseFee1%16 + 1)
		if currentBaseFee == 0 {
			t.Skip()
		}
		if len(sender) < 1+1+1 {
			t.Skip()
		}

		senders, senderIDs, txs, ok := poolsFromFuzzBytes(txNonce, values, tips, feeCap, sender)
		if !ok {
			t.Skip()
		}

		assert, require := assert.New(t), require.New(t)
		err := txs.Valid()
		assert.NoError(err)

		var prevTotal int

		ch := make(chan Hashes, 100)

		db := mdbx.NewMDBX(log.New()).InMem().WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.TxpoolTablesCfg }).MustOpen()
		t.Cleanup(db.Close)

		sendersCache := NewSendersCache()
		cfg := DefaultConfig
		cfg.evictSendersAfterRounds = 1
		pool, err := New(ch, sendersCache, db, cfg)
		assert.NoError(err)
		sendersCache.senderInfo = senders
		sendersCache.senderIDs = senderIDs
		sendersCache.senderID = uint64(len(senderIDs))
		check := func(unwindTxs, minedTxs TxSlots, msg string) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued
			//if pending.Len() > 5 && baseFee.Len() > 5 && queued.Len() > 5 {
			//	fmt.Printf("len %s: %d,%d,%d\n", msg, pending.Len(), baseFee.Len(), queued.Len())
			//}

			best, worst := pending.Best(), pending.Worst()
			assert.LessOrEqual(pending.Len(), PendingSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			if worst != nil && worst.subPool < 0b11110 {
				t.Fatalf("pending worst too small %b", worst.subPool)
			}
			iterateSubPoolUnordered(pending, func(tx *metaTx) {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce, msg)
				}
				if tx.subPool&EnoughBalance > 0 {
					//assert.True(tx.SenderHasEnoughBalance)
				}
				if tx.subPool&EnoughFeeCapProtocol > 0 {
					assert.LessOrEqual(calcProtocolBaseFee(currentBaseFee), tx.Tx.feeCap, msg)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(currentBaseFee, tx.Tx.feeCap, msg)
				}

				// side data structures must have all txs
				assert.True(pool.txNonce2Tx.has(tx), msg)
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok)

				// pools can't have more then 1 tx with same SenderID+Nonce
				iterateSubPoolUnordered(baseFee, func(mtx2 *metaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.senderID == i.senderID && tx2.nonce == i.nonce, msg)
				})
				iterateSubPoolUnordered(queued, func(mtx2 *metaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.senderID == i.senderID && tx2.nonce == i.nonce, msg)
				})
			})

			best, worst = baseFee.Best(), baseFee.Worst()

			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			assert.LessOrEqual(baseFee.Len(), BaseFeeSubPoolLimit, msg)
			if worst != nil && worst.subPool < 0b11100 {
				t.Fatalf("baseFee worst too small %b", worst.subPool)
			}
			iterateSubPoolUnordered(baseFee, func(tx *metaTx) {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce, msg)
				}
				if tx.subPool&EnoughBalance != 0 {
					//assert.True(tx.SenderHasEnoughBalance, msg)
				}
				if tx.subPool&EnoughFeeCapProtocol > 0 {
					assert.LessOrEqual(calcProtocolBaseFee(currentBaseFee), tx.Tx.feeCap, msg)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(currentBaseFee, tx.Tx.feeCap, msg)
				}

				assert.True(pool.txNonce2Tx.has(tx), msg)
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok, msg)
			})

			best, worst = queued.Best(), queued.Worst()
			assert.LessOrEqual(queued.Len(), QueuedSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			if worst != nil && worst.subPool < 0b10000 {
				t.Fatalf("queued worst too small %b", worst.subPool)
			}
			iterateSubPoolUnordered(queued, func(tx *metaTx) {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce, msg)
				}
				if tx.subPool&EnoughBalance > 0 {
					//assert.True(tx.SenderHasEnoughBalance, msg)
				}
				if tx.subPool&EnoughFeeCapProtocol > 0 {
					assert.LessOrEqual(calcProtocolBaseFee(currentBaseFee), tx.Tx.feeCap, msg)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(currentBaseFee, tx.Tx.feeCap, msg)
				}

				assert.True(pool.txNonce2Tx.has(tx), "%s, %d, %x", msg, tx.Tx.nonce, tx.Tx.idHash)
				_, ok = pool.byHash[string(i.idHash[:])]
				assert.True(ok, msg)
			})

			// all txs in side data structures must be in some queue
			for _, txn := range pool.byHash {
				require.True(txn.bestIndex >= 0, msg)
				assert.True(txn.worstIndex >= 0, msg)
			}
			for id := range senders {
				//assert.True(senders[i].txNonce2Tx.Len() > 0)
				pool.txNonce2Tx.ascend(id, func(mt *metaTx) bool {
					if mt.worstIndex < 0 {
						fmt.Printf("here: %d,%d\n", pool.txNonce2Tx.tree.Len(), len(pool.byHash))
					}
					require.True(mt.worstIndex >= 0, msg)
					assert.True(mt.bestIndex >= 0, msg)
					return true
				})
			}

			// mined txs must be removed
			for i := range minedTxs.txs {
				_, ok = pool.byHash[string(minedTxs.txs[i].idHash[:])]
				assert.False(ok, msg)
			}

			if queued.Len() > 3 {
				// Less func must be transitive (choose 3 semi-random elements)
				i := queued.Len() - 1
				a, b, c := (*queued.best)[i], (*queued.best)[i-1], (*queued.best)[i-2]
				if a.Less(b) && b.Less(c) {
					assert.True(a.Less(c))
				}
			}
		}

		checkNotify := func(unwindTxs, minedTxs TxSlots, msg string) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued
			select {
			case newHashes := <-ch:
				//assert.Equal(len(txs1.txs), newHashes.Len())
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
						if bytes.Equal(minedTxs.txs[j].idHash[:], newHash) {
							foundInMined = true
							break
						}
					}
					assert.True(foundInUnwind, msg)
					assert.False(foundInMined, msg)
				}
			default: // no notifications - means pools must be unchanged or drop some txs
				assert.GreaterOrEqual(prevTotal, pending.Len()+baseFee.Len()+queued.Len(), msg)
			}
			prevTotal = pending.Len() + baseFee.Len() + queued.Len()
		}
		checkDB := func(tx kv.Tx) {
			c1, _ := tx.Cursor(kv.PoolSenderID)
			c2, _ := tx.Cursor(kv.PoolSenderIDToAdress)
			count1, _ := c1.Count()
			count2, _ := c2.Count()
			assert.Equal(count1, count2)
		}
		//fmt.Printf("-------\n")

		// go to first fork
		//fmt.Printf("ll1: %d,%d,%d\n", pool.pending.Len(), pool.baseFee.Len(), pool.queued.Len())
		txs1, txs2, p2pReceived, txs3 := splitDataset(txs)
		err = pool.OnNewBlock(map[string]senderInfo{}, txs1, TxSlots{}, currentBaseFee, 1, [32]byte{}, sendersCache)
		assert.NoError(err)
		check(txs1, TxSlots{}, "fork1")
		checkNotify(txs1, TxSlots{}, "fork1")

		_, _, _ = p2pReceived, txs2, txs3
		err = pool.OnNewBlock(map[string]senderInfo{}, TxSlots{}, txs2, currentBaseFee, 1, [32]byte{}, sendersCache)
		check(TxSlots{}, txs2, "fork1 mined")
		checkNotify(TxSlots{}, txs2, "fork1 mined")

		// unwind everything and switch to new fork (need unwind mined now)
		err = pool.OnNewBlock(map[string]senderInfo{}, txs2, TxSlots{}, currentBaseFee, 2, [32]byte{}, sendersCache)
		assert.NoError(err)
		check(txs2, TxSlots{}, "fork2")
		checkNotify(txs2, TxSlots{}, "fork2")

		err = pool.OnNewBlock(map[string]senderInfo{}, TxSlots{}, txs3, currentBaseFee, 2, [32]byte{}, sendersCache)
		assert.NoError(err)
		check(TxSlots{}, txs3, "fork2 mined")
		checkNotify(TxSlots{}, txs3, "fork2 mined")

		// add some remote txs from p2p
		err = pool.OnNewTxs(context.Background(), nil, p2pReceived)
		assert.NoError(err)
		check(p2pReceived, TxSlots{}, "p2pmsg1")
		checkNotify(p2pReceived, TxSlots{}, "p2pmsg1")

		senderIdsBeforeFlush := len(sendersCache.senderIDs)
		senderInfoBeforeFlush := len(sendersCache.senderInfo)

		tx, err := db.BeginRw(context.Background())
		require.NoError(err)
		defer tx.Rollback()

		_, err = pool.flushLocked(tx) // we don't test eviction here, because dedicated test exists
		require.NoError(err)
		check(p2pReceived, TxSlots{}, "after_flush")
		checkDB(tx)
		//checkNotify(p2pReceived, TxSlots{}, "after_flush")

		s2 := NewSendersCache()
		p2, err := New(ch, s2, nil, DefaultConfig)
		assert.NoError(err)
		err = p2.fromDB(context.Background(), tx, nil)
		require.NoError(err)
		checkDB(tx)
		for _, txn := range p2.byHash {
			assert.Nil(txn.Tx.rlp)
		}
		//todo: check that after load from db tx linked to same sender

		check(txs2, TxSlots{}, "fromDB")
		//checkNotify(txs2, TxSlots{}, "fromDB")
		assert.Equal(sendersCache.senderID, s2.senderID)
		assert.Equal(sendersCache.blockHeight.Load(), s2.blockHeight.Load())

		idsCountAfterFlush, idsCountInDbAfterFlush, err := s2.idsCount(tx)
		assert.NoError(err)
		assert.LessOrEqual(senderIdsBeforeFlush, idsCountAfterFlush+idsCountInDbAfterFlush)
		infoCountAfterFlush, infoCountInDbAfterFlush, err := s2.infoCount(tx)
		assert.NoError(err)
		assert.LessOrEqual(senderInfoBeforeFlush, infoCountAfterFlush+infoCountInDbAfterFlush)

		assert.Equal(pool.pending.Len(), p2.pending.Len())
		assert.Equal(pool.baseFee.Len(), p2.baseFee.Len())
		assert.Equal(pool.queued.Len(), p2.queued.Len())
		assert.Equal(pool.currentBaseFee.Load(), p2.currentBaseFee.Load())
		assert.Equal(pool.protocolBaseFee.Load(), p2.protocolBaseFee.Load())
	})

}
