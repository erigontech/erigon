//go:build !nofuzz

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon-lib/types"
)

// https://go.dev/doc/fuzz/
// golang.org/s/draft-fuzzing-design
//go doc testing
//go doc testing.F
//go doc testing.F.AddRemoteTxs
//go doc testing.F.Fuzz

// go test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool

func init() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
}

/*
	func FuzzTwoQueue(f *testing.F) {
		f.Add([]uint8{0b1000, 0b0101, 0b0111})
		f.Add([]uint8{0b0101, 0b1110, 0b1101, 0b0001})
		f.Fuzz(func(t *testing.T, in []uint8) {
			t.Parallel()
			assert := assert.New(t)
			{
				sub := NewPendingSubPool(PendingSubPool, 1024)
				for _, i := range in {
					sub.Add(&metaTx{subPool: SubPoolMarker(i & 0b1111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}})
				}
				sub.EnforceWorstInvariants()
				sub.EnforceBestInvariants()
				assert.Equal(len(in), sub.best.Len())
				assert.Equal(len(in), sub.worst.Len())
				assert.Equal(len(in), sub.Len())

				var prevBest *uint8
				for i := range sub.best.ms {
					current := uint8(sub.best.ms[i].subPool)
					if prevBest != nil {
						assert.LessOrEqual(current, *prevBest)
					}
					assert.Equal(i, sub.best.ms[i].bestIndex)
					prevBest = &current
				}
			}
			{
				sub := NewSubPool(BaseFeeSubPool, 1024)
				for _, i := range in {
					sub.Add(&metaTx{subPool: SubPoolMarker(i & 0b1111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}})
				}
				assert.Equal(len(in), sub.best.Len())
				assert.Equal(len(in), sub.worst.Len())
				assert.Equal(len(in), sub.Len())

				for i := range sub.best.ms {
					assert.Equal(i, (sub.best.ms)[i].bestIndex)
				}
				for i := range sub.worst.ms {
					assert.Equal(i, (sub.worst.ms)[i].worstIndex)
				}

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
				sub := NewSubPool(QueuedSubPool, 1024)
				for _, i := range in {
					sub.Add(&metaTx{subPool: SubPoolMarker(i & 0b1111), Tx: &TxSlot{nonce: 1, value: *uint256.NewInt(1)}})
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
*/
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

func parseSenders(in []byte) (nonces []uint64, balances []uint256.Int) {
	for i := 0; i < len(in)-(1+1-1); i += 1 + 1 {
		nonce := uint64(in[i] % 8)
		if nonce == 0 {
			nonce = 1
		}
		nonces = append(nonces, nonce)
		balances = append(balances, *uint256.NewInt(uint64(in[i+1])))
	}
	return
}

func poolsFromFuzzBytes(rawTxNonce, rawValues, rawTips, rawFeeCap, rawSender []byte) (sendersInfo map[uint64]*sender, senderIDs map[common.Address]uint64, txs types.TxSlots, ok bool) {
	if len(rawTxNonce) < 1 || len(rawValues) < 1 || len(rawTips) < 1 || len(rawFeeCap) < 1 || len(rawSender) < 1+1 {
		return nil, nil, txs, false
	}
	senderNonce, senderBalance := parseSenders(rawSender)
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

	sendersInfo = map[uint64]*sender{}
	senderIDs = map[common.Address]uint64{}
	senders := make(types.Addresses, 20*len(senderNonce))
	for i := 0; i < len(senderNonce); i++ {
		senderID := uint64(i + 1) //non-zero expected
		binary.BigEndian.PutUint64(senders.At(i%senders.Len()), senderID)
		sendersInfo[senderID] = newSender(senderNonce[i], senderBalance[i%len(senderBalance)])
		senderIDs[senders.AddressAt(i%senders.Len())] = senderID
	}
	txs.Txs = make([]*types.TxSlot, len(txNonce))
	parseCtx := types.NewTxParseContext(*u256.N1)
	parseCtx.WithSender(false)
	for i := range txNonce {
		txs.Txs[i] = &types.TxSlot{
			Nonce:  txNonce[i],
			Value:  values[i%len(values)],
			Tip:    *uint256.NewInt(tips[i%len(tips)]),
			FeeCap: *uint256.NewInt(feeCap[i%len(feeCap)]),
		}
		txRlp := fakeRlpTx(txs.Txs[i], senders.At(i%senders.Len()))
		_, err := parseCtx.ParseTransaction(txRlp, 0, txs.Txs[i], nil, false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
		if err != nil {
			panic(err)
		}
		txs.Senders = append(txs.Senders, senders.At(i%senders.Len())...)
		txs.IsLocal = append(txs.IsLocal, true)
	}

	return sendersInfo, senderIDs, txs, true
}

// fakeRlpTx add anything what identifying tx to `data` to make hash unique
func fakeRlpTx(slot *types.TxSlot, data []byte) []byte {
	dataLen := rlp.U64Len(1) + //chainID
		rlp.U64Len(slot.Nonce) + rlp.U256Len(&slot.Tip) + rlp.U256Len(&slot.FeeCap) +
		rlp.U64Len(0) + // gas
		rlp.StringLen([]byte{}) + // dest addr
		rlp.U256Len(&slot.Value) +
		rlp.StringLen(data) + // data
		rlp.ListPrefixLen(0) + //access list
		+3 // v,r,s

	buf := make([]byte, 1+rlp.ListPrefixLen(dataLen)+dataLen)
	buf[0] = types.DynamicFeeTxType
	p := 1
	p += rlp.EncodeListPrefix(dataLen, buf[p:])
	p += rlp.EncodeU64(1, buf[p:]) //chainID
	p += rlp.EncodeU64(slot.Nonce, buf[p:])
	bb := bytes.NewBuffer(buf[p:p])
	_ = slot.Tip.EncodeRLP(bb)
	p += rlp.U256Len(&slot.Tip)
	bb = bytes.NewBuffer(buf[p:p])
	_ = slot.FeeCap.EncodeRLP(bb)
	p += rlp.U256Len(&slot.FeeCap)
	p += rlp.EncodeU64(0, buf[p:])           //gas
	p += rlp.EncodeString([]byte{}, buf[p:]) //destrination addr
	bb = bytes.NewBuffer(buf[p:p])
	_ = slot.Value.EncodeRLP(bb)
	p += rlp.U256Len(&slot.Value)
	p += rlp.EncodeString(data, buf[p:])  //data
	p += rlp.EncodeListPrefix(0, buf[p:]) // access list
	p += rlp.EncodeU64(1, buf[p:])        //v
	p += rlp.EncodeU64(1, buf[p:])        //r
	p += rlp.EncodeU64(1, buf[p:])        //s
	_ = p
	return buf[:]
}

func iterateSubPoolUnordered(subPool *SubPool, f func(tx *metaTx)) {
	for i := 0; i < subPool.best.Len(); i++ {
		f((subPool.best.ms)[i])
	}
}

func splitDataset(in types.TxSlots) (types.TxSlots, types.TxSlots, types.TxSlots, types.TxSlots) {
	p1, p2, p3, p4 := types.TxSlots{}, types.TxSlots{}, types.TxSlots{}, types.TxSlots{}
	l := len(in.Txs) / 4

	p1.Txs = in.Txs[:l]
	p1.IsLocal = in.IsLocal[:l]
	p1.Senders = in.Senders[:l*20]

	p2.Txs = in.Txs[l : 2*l]
	p2.IsLocal = in.IsLocal[l : 2*l]
	p2.Senders = in.Senders[l*20 : 2*l*20]

	p3.Txs = in.Txs[2*l : 3*l]
	p3.IsLocal = in.IsLocal[2*l : 3*l]
	p3.Senders = in.Senders[2*l*20 : 3*l*20]

	p4.Txs = in.Txs[3*l : 4*l]
	p4.IsLocal = in.IsLocal[3*l : 4*l]
	p4.Senders = in.Senders[3*l*20 : 4*l*20]

	return p1, p2, p3, p4
}

func FuzzOnNewBlocks(f *testing.F) {
	var u64 = [1 * 4]byte{1}
	var senderAddr = [1 + 1 + 1]byte{1}
	f.Add(u64[:], u64[:], u64[:], u64[:], senderAddr[:], uint8(12))
	f.Add(u64[:], u64[:], u64[:], u64[:], senderAddr[:], uint8(14))
	f.Add(u64[:], u64[:], u64[:], u64[:], senderAddr[:], uint8(123))
	f.Fuzz(func(t *testing.T, txNonce, values, tips, feeCap, senderAddr []byte, pendingBaseFee1 uint8) {
		//t.Parallel()
		ctx := context.Background()

		pendingBaseFee := uint64(pendingBaseFee1%16 + 1)
		if pendingBaseFee == 0 {
			t.Skip()
		}
		senders, senderIDs, txs, ok := poolsFromFuzzBytes(txNonce, values, tips, feeCap, senderAddr)
		if !ok {
			t.Skip()
		}

		assert, require := assert.New(t), require.New(t)
		assert.NoError(txs.Valid())

		var prevHashes types.Hashes
		ch := make(chan types.Announcements, 100)
		db, coreDB := memdb.NewTestPoolDB(t), memdb.NewTestDB(t)

		cfg := txpoolcfg.DefaultConfig
		sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
		pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil, nil, fixedgas.DefaultMaxBlobsPerBlock, log.New())
		assert.NoError(err)
		pool.senders.senderIDs = senderIDs
		for addr, id := range senderIDs {
			pool.senders.senderID2Addr[id] = addr
		}
		pool.senders.senderID = uint64(len(senderIDs))
		check := func(unwindTxs, minedTxs types.TxSlots, msg string) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued
			best, worst := pending.Best(), pending.Worst()
			assert.LessOrEqual(pending.Len(), cfg.PendingSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			if worst != nil && worst.subPool < 0b1110 {
				t.Fatalf("pending worst too small %b", worst.subPool)
			}
			for _, tx := range pending.best.ms {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg, i.SenderID)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, tx.Tx.FeeCap, msg)
				}

				// side data structures must have all txs
				assert.True(pool.all.has(tx), msg)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok)

				// pools can't have more then 1 tx with same SenderID+Nonce
				iterateSubPoolUnordered(baseFee, func(mtx2 *metaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.SenderID == i.SenderID && tx2.Nonce == i.Nonce, msg)
				})
				iterateSubPoolUnordered(queued, func(mtx2 *metaTx) {
					tx2 := mtx2.Tx
					assert.False(tx2.SenderID == i.SenderID && tx2.Nonce == i.Nonce, msg)
				})
			}

			best, worst = baseFee.Best(), baseFee.Worst()

			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			assert.LessOrEqual(baseFee.Len(), cfg.BaseFeeSubPoolLimit, msg)
			if worst != nil && worst.subPool < 0b1100 {
				t.Fatalf("baseFee worst too small %b", worst.subPool)
			}
			iterateSubPoolUnordered(baseFee, func(tx *metaTx) {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, tx.Tx.FeeCap, msg)
				}

				assert.True(pool.all.has(tx), msg)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok, msg)
			})

			best, worst = queued.Best(), queued.Worst()
			assert.LessOrEqual(queued.Len(), cfg.QueuedSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			iterateSubPoolUnordered(queued, func(tx *metaTx) {
				i := tx.Tx
				if tx.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg, i.SenderID, senders[i.SenderID].nonce)
				}
				if tx.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, tx.Tx.FeeCap, msg)
				}

				assert.True(pool.all.has(tx), "%s, %d, %x", msg, tx.Tx.Nonce, tx.Tx.IDHash)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok, msg)
				assert.GreaterOrEqual(tx.Tx.FeeCap, pool.cfg.MinFeeCap)
			})

			// all txs in side data structures must be in some queue
			for _, txn := range pool.byHash {
				require.True(txn.bestIndex >= 0, msg)
				assert.True(txn.worstIndex >= 0, msg)
			}
			for id := range senders {
				//assert.True(senders[i].all.Len() > 0)
				pool.all.ascend(id, func(mt *metaTx) bool {
					require.True(mt.worstIndex >= 0, msg)
					assert.True(mt.bestIndex >= 0, msg)
					return true
				})
			}

			// mined txs must be removed
			for i := range minedTxs.Txs {
				_, ok = pool.byHash[string(minedTxs.Txs[i].IDHash[:])]
				assert.False(ok, msg)
			}

			if queued.Len() > 3 {
				// Less func must be transitive (choose 3 semi-random elements)
				i := queued.Len() - 1
				if queued.best.Less(i, i-1) && queued.best.Less(i-1, i-2) {
					assert.True(queued.best.Less(i, i-2))
				}
			}
		}

		checkNotify := func(unwindTxs, minedTxs types.TxSlots, msg string) {
			select {
			case newAnnouncements := <-ch:
				assert.Greater(newAnnouncements.Len(), 0)
				for i := 0; i < newAnnouncements.Len(); i++ {
					_, _, newHash := newAnnouncements.At(i)
					for j := range unwindTxs.Txs {
						if bytes.Equal(unwindTxs.Txs[j].IDHash[:], newHash) {
							mt := pool.all.get(unwindTxs.Txs[j].SenderID, unwindTxs.Txs[j].Nonce)
							require.True(mt != nil && mt.currentSubPool == PendingSubPool, msg)
						}
					}
					for j := range minedTxs.Txs {
						if bytes.Equal(minedTxs.Txs[j].IDHash[:], newHash) {
							mt := pool.all.get(unwindTxs.Txs[j].SenderID, unwindTxs.Txs[j].Nonce)
							require.True(mt != nil && mt.currentSubPool == PendingSubPool, msg)
						}
					}
				}
			default: // no notifications - means pools must be unchanged or drop some txs
				pendingHashes := copyHashes(pool.pending)
				require.Zero(extractNewHashes(pendingHashes, prevHashes).Len())
			}
			prevHashes = copyHashes(pool.pending)
		}

		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		// start blocks from 0, set empty hash - then kvcache will also work on this
		h0, h22 := gointerfaces.ConvertHashToH256([32]byte{}), gointerfaces.ConvertHashToH256([32]byte{22})

		var txID uint64
		_ = coreDB.View(ctx, func(tx kv.Tx) error {
			txID = tx.ViewID()
			return nil
		})
		change := &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 0, BlockHash: h0},
			},
		}
		for id, sender := range senders {
			addr := pool.senders.senderID2Addr[id]
			v := make([]byte, types.EncodeSenderLengthForStorage(sender.nonce, sender.balance))
			types.EncodeSender(sender.nonce, sender.balance, v)
			change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
				Action:  remote.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    v,
			})
		}
		// go to first fork
		txs1, txs2, p2pReceived, txs3 := splitDataset(txs)
		err = pool.OnNewBlock(ctx, change, txs1, types.TxSlots{}, tx)
		assert.NoError(err)
		check(txs1, types.TxSlots{}, "fork1")
		checkNotify(txs1, types.TxSlots{}, "fork1")

		_, _, _ = p2pReceived, txs2, txs3
		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 1, BlockHash: h0},
			},
		}
		err = pool.OnNewBlock(ctx, change, types.TxSlots{}, txs2, tx)
		assert.NoError(err)
		check(types.TxSlots{}, txs2, "fork1 mined")
		checkNotify(types.TxSlots{}, txs2, "fork1 mined")

		// unwind everything and switch to new fork (need unwind mined now)
		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 0, BlockHash: h0, Direction: remote.Direction_UNWIND},
			},
		}
		err = pool.OnNewBlock(ctx, change, txs2, types.TxSlots{}, tx)
		assert.NoError(err)
		check(txs2, types.TxSlots{}, "fork2")
		checkNotify(txs2, types.TxSlots{}, "fork2")

		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 1, BlockHash: h22},
			},
		}
		err = pool.OnNewBlock(ctx, change, types.TxSlots{}, txs3, tx)
		assert.NoError(err)
		check(types.TxSlots{}, txs3, "fork2 mined")
		checkNotify(types.TxSlots{}, txs3, "fork2 mined")

		// add some remote txs from p2p
		pool.AddRemoteTxs(ctx, p2pReceived)
		err = pool.processRemoteTxs(ctx)
		assert.NoError(err)
		check(p2pReceived, types.TxSlots{}, "p2pmsg1")
		checkNotify(p2pReceived, types.TxSlots{}, "p2pmsg1")

		err = pool.flushLocked(tx) // we don't test eviction here, because dedicated test exists
		require.NoError(err)
		check(p2pReceived, types.TxSlots{}, "after_flush")
		checkNotify(p2pReceived, types.TxSlots{}, "after_flush")

		p2, err := New(ch, coreDB, txpoolcfg.DefaultConfig, sendersCache, *u256.N1, nil, nil, fixedgas.DefaultMaxBlobsPerBlock, log.New())
		assert.NoError(err)
		p2.senders = pool.senders // senders are not persisted
		err = coreDB.View(ctx, func(coreTx kv.Tx) error { return p2.fromDB(ctx, tx, coreTx) })
		require.NoError(err)
		for _, txn := range p2.byHash {
			assert.Nil(txn.Tx.Rlp)
		}

		check(txs2, types.TxSlots{}, "fromDB")
		checkNotify(txs2, types.TxSlots{}, "fromDB")
		assert.Equal(pool.senders.senderID, p2.senders.senderID)
		assert.Equal(pool.lastSeenBlock.Load(), p2.lastSeenBlock.Load())
		assert.Equal(pool.pending.Len(), p2.pending.Len())
		assert.Equal(pool.baseFee.Len(), p2.baseFee.Len())
		require.Equal(pool.queued.Len(), p2.queued.Len())
		assert.Equal(pool.pendingBaseFee.Load(), p2.pendingBaseFee.Load())
	})
}

func copyHashes(p *PendingPool) (hashes types.Hashes) {
	for i := range p.best.ms {
		hashes = append(hashes, p.best.ms[i].Tx.IDHash[:]...)
	}
	return hashes
}

// extractNewHashes - extract from h1 hashes which do not exist in h2
func extractNewHashes(h1, h2 types.Hashes) (result types.Hashes) {
	for i := 0; i < h1.Len(); i++ {
		found := false
		for j := 0; j < h2.Len(); j++ {
			if bytes.Equal(h1.At(i), h2.At(j)) {
				found = true
				break
			}
		}
		if !found {
			result = append(result, h1.At(i)...)
		}
	}
	return result
}
