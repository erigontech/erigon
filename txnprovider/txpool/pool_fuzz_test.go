// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build !nofuzz

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/fixedgas"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// https://go.dev/doc/fuzz/
// golang.org/s/draft-fuzzing-design
//go doc testing
//go doc testing.F
//go doc testing.F.AddRemoteTxns
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
					sub.Add(&metaTxn{subPool: SubPoolMarker(i & 0b1111), TxnSlot: &TxnSlot{nonce: 1, value: *uint256.NewInt(1)}})
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
					sub.Add(&metaTxn{subPool: SubPoolMarker(i & 0b1111), TxnSlot: &TxnSlot{nonce: 1, value: *uint256.NewInt(1)}})
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
					sub.Add(&metaTxn{subPool: SubPoolMarker(i & 0b1111), TxnSlot: &TxnSlot{nonce: 1, value: *uint256.NewInt(1)}})
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

func poolsFromFuzzBytes(rawTxnNonce, rawValues, rawTips, rawFeeCap, rawSender []byte) (sendersInfo map[uint64]*sender, senderIDs map[common.Address]uint64, txns TxnSlots, ok bool) {
	if len(rawTxnNonce) < 1 || len(rawValues) < 1 || len(rawTips) < 1 || len(rawFeeCap) < 1 || len(rawSender) < 1+1 {
		return nil, nil, txns, false
	}
	senderNonce, senderBalance := parseSenders(rawSender)
	txnNonce, ok := u8Slice(rawTxnNonce)
	if !ok {
		return nil, nil, txns, false
	}
	feeCap, ok := u8Slice(rawFeeCap)
	if !ok {
		return nil, nil, txns, false
	}
	tips, ok := u8Slice(rawTips)
	if !ok {
		return nil, nil, txns, false
	}
	values, ok := u256Slice(rawValues)
	if !ok {
		return nil, nil, txns, false
	}

	sendersInfo = map[uint64]*sender{}
	senderIDs = map[common.Address]uint64{}
	senders := make(Addresses, 20*len(senderNonce))
	for i := 0; i < len(senderNonce); i++ {
		senderID := uint64(i + 1) //non-zero expected
		binary.BigEndian.PutUint64(senders.At(i%senders.Len()), senderID)
		sendersInfo[senderID] = newSender(senderNonce[i], senderBalance[i%len(senderBalance)])
		senderIDs[senders.AddressAt(i%senders.Len())] = senderID
	}
	txns.Txns = make([]*TxnSlot, len(txnNonce))
	parseCtx := NewTxnParseContext(*u256.N1)
	parseCtx.WithSender(false)
	for i := range txnNonce {
		txns.Txns[i] = &TxnSlot{
			Nonce:  txnNonce[i],
			Value:  values[i%len(values)],
			Tip:    *uint256.NewInt(tips[i%len(tips)]),
			FeeCap: *uint256.NewInt(feeCap[i%len(feeCap)]),
		}
		txnRlp := fakeRlpTxn(txns.Txns[i], senders.At(i%senders.Len()))
		_, err := parseCtx.ParseTransaction(txnRlp, 0, txns.Txns[i], nil, false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
		if err != nil {
			panic(err)
		}
		txns.Senders = append(txns.Senders, senders.At(i%senders.Len())...)
		txns.IsLocal = append(txns.IsLocal, true)
	}

	return sendersInfo, senderIDs, txns, true
}

// fakeRlpTxn add anything what identifying txn to `data` to make hash unique
func fakeRlpTxn(slot *TxnSlot, data []byte) []byte {
	dataLen := rlp.U64Len(1) + //chainID
		rlp.U64Len(slot.Nonce) + rlp.U256Len(&slot.Tip) + rlp.U256Len(&slot.FeeCap) +
		rlp.U64Len(0) + // gas
		rlp.StringLen([]byte{}) + // dest addr
		rlp.U256Len(&slot.Value) +
		rlp.StringLen(data) + // data
		rlp.ListPrefixLen(0) + //access list
		+3 // v,r,s

	buf := make([]byte, 1+rlp.ListPrefixLen(dataLen)+dataLen)
	buf[0] = DynamicFeeTxnType
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

func iterateSubPoolUnordered(subPool *SubPool, f func(txn *metaTxn)) {
	for i := 0; i < subPool.best.Len(); i++ {
		f((subPool.best.ms)[i])
	}
}

func splitDataset(in TxnSlots) (TxnSlots, TxnSlots, TxnSlots, TxnSlots) {
	p1, p2, p3, p4 := TxnSlots{}, TxnSlots{}, TxnSlots{}, TxnSlots{}
	l := len(in.Txns) / 4

	p1.Txns = in.Txns[:l]
	p1.IsLocal = in.IsLocal[:l]
	p1.Senders = in.Senders[:l*20]

	p2.Txns = in.Txns[l : 2*l]
	p2.IsLocal = in.IsLocal[l : 2*l]
	p2.Senders = in.Senders[l*20 : 2*l*20]

	p3.Txns = in.Txns[2*l : 3*l]
	p3.IsLocal = in.IsLocal[2*l : 3*l]
	p3.Senders = in.Senders[2*l*20 : 3*l*20]

	p4.Txns = in.Txns[3*l : 4*l]
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
		senders, senderIDs, txns, ok := poolsFromFuzzBytes(txNonce, values, tips, feeCap, senderAddr)
		if !ok {
			t.Skip()
		}

		assert, require := assert.New(t), require.New(t)
		assert.NoError(txns.Valid())

		var prevHashes Hashes
		ch := make(chan Announcements, 100)

		coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		db := memdb.NewTestPoolDB(t)

		cfg := txpoolcfg.DefaultConfig
		sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
		pool, err := New(ch, coreDB, cfg, sendersCache, *u256.N1, nil, nil, nil, nil, fixedgas.DefaultMaxBlobsPerBlock, nil, log.New())
		assert.NoError(err)

		err = pool.Start(ctx, db)
		assert.NoError(err)

		pool.senders.senderIDs = senderIDs
		for addr, id := range senderIDs {
			pool.senders.senderID2Addr[id] = addr
		}
		pool.senders.senderID = uint64(len(senderIDs))
		check := func(unwindTxns, minedTxns TxnSlots, msg string) {
			pending, baseFee, queued := pool.pending, pool.baseFee, pool.queued
			best, worst := pending.Best(), pending.Worst()
			assert.LessOrEqual(pending.Len(), cfg.PendingSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			if worst != nil && worst.subPool < 0b1110 {
				t.Fatalf("pending worst too small %b", worst.subPool)
			}
			for _, txn := range pending.best.ms {
				i := txn.TxnSlot
				if txn.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg, i.SenderID)
				}
				if txn.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, txn.TxnSlot.FeeCap, msg)
				}

				// side data structures must have all txns
				assert.True(pool.all.has(txn), msg)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok)

				// pools can't have more then 1 txn with same SenderID+Nonce
				iterateSubPoolUnordered(baseFee, func(mtxn2 *metaTxn) {
					txn2 := mtxn2.TxnSlot
					assert.False(txn2.SenderID == i.SenderID && txn2.Nonce == i.Nonce, msg)
				})
				iterateSubPoolUnordered(queued, func(mtxn2 *metaTxn) {
					txn2 := mtxn2.TxnSlot
					assert.False(txn2.SenderID == i.SenderID && txn2.Nonce == i.Nonce, msg)
				})
			}

			best, worst = baseFee.Best(), baseFee.Worst()

			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			assert.LessOrEqual(baseFee.Len(), cfg.BaseFeeSubPoolLimit, msg)
			if worst != nil && worst.subPool < 0b1100 {
				t.Fatalf("baseFee worst too small %b", worst.subPool)
			}
			iterateSubPoolUnordered(baseFee, func(txn *metaTxn) {
				i := txn.TxnSlot
				if txn.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg)
				}
				if txn.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, txn.TxnSlot.FeeCap, msg)
				}

				assert.True(pool.all.has(txn), msg)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok, msg)
			})

			best, worst = queued.Best(), queued.Worst()
			assert.LessOrEqual(queued.Len(), cfg.QueuedSubPoolLimit)
			assert.False(worst != nil && best == nil, msg)
			assert.False(worst == nil && best != nil, msg)
			iterateSubPoolUnordered(queued, func(txn *metaTxn) {
				i := txn.TxnSlot
				if txn.subPool&NoNonceGaps > 0 {
					assert.GreaterOrEqual(i.Nonce, senders[i.SenderID].nonce, msg, i.SenderID, senders[i.SenderID].nonce)
				}
				if txn.subPool&EnoughFeeCapBlock > 0 {
					assert.LessOrEqual(pendingBaseFee, txn.TxnSlot.FeeCap, msg)
				}

				assert.True(pool.all.has(txn), "%s, %d, %x", msg, txn.TxnSlot.Nonce, txn.TxnSlot.IDHash)
				_, ok = pool.byHash[string(i.IDHash[:])]
				assert.True(ok, msg)
				assert.GreaterOrEqual(txn.TxnSlot.FeeCap, pool.cfg.MinFeeCap)
			})

			// all txns in side data structures must be in some queue
			for _, txn := range pool.byHash {
				require.True(txn.bestIndex >= 0, msg)
				assert.True(txn.worstIndex >= 0, msg)
			}
			for id := range senders {
				//assert.True(senders[i].all.Len() > 0)
				pool.all.ascend(id, func(mt *metaTxn) bool {
					require.True(mt.worstIndex >= 0, msg)
					assert.True(mt.bestIndex >= 0, msg)
					return true
				})
			}

			// mined txns must be removed
			for i := range minedTxns.Txns {
				_, ok = pool.byHash[string(minedTxns.Txns[i].IDHash[:])]
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

		checkNotify := func(unwindTxns, minedTxns TxnSlots, msg string) {
			select {
			case newAnnouncements := <-ch:
				assert.Greater(newAnnouncements.Len(), 0)
				for i := 0; i < newAnnouncements.Len(); i++ {
					_, _, newHash := newAnnouncements.At(i)
					for j := range unwindTxns.Txns {
						if bytes.Equal(unwindTxns.Txns[j].IDHash[:], newHash) {
							mt := pool.all.get(unwindTxns.Txns[j].SenderID, unwindTxns.Txns[j].Nonce)
							require.True(mt != nil && mt.currentSubPool == PendingSubPool, msg)
						}
					}
					for j := range minedTxns.Txns {
						if bytes.Equal(minedTxns.Txns[j].IDHash[:], newHash) {
							mt := pool.all.get(unwindTxns.Txns[j].SenderID, unwindTxns.Txns[j].Nonce)
							require.True(mt != nil && mt.currentSubPool == PendingSubPool, msg)
						}
					}
				}
			default: // no notifications - means pools must be unchanged or drop some txns
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
			v := make([]byte, EncodeSenderLengthForStorage(sender.nonce, sender.balance))
			EncodeSender(sender.nonce, sender.balance, v)
			change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
				Action:  remote.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    v,
			})
		}
		// go to first fork
		txns1, txns2, p2pReceived, txns3 := splitDataset(txns)
		err = pool.OnNewBlock(ctx, change, txns1, TxnSlots{}, TxnSlots{})
		assert.NoError(err)
		check(txns1, TxnSlots{}, "fork1")
		checkNotify(txns1, TxnSlots{}, "fork1")

		_, _, _ = p2pReceived, txns2, txns3
		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 1, BlockHash: h0},
			},
		}
		err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, txns2)
		assert.NoError(err)
		check(TxnSlots{}, txns2, "fork1 mined")
		checkNotify(TxnSlots{}, txns2, "fork1 mined")

		// unwind everything and switch to new fork (need unwind mined now)
		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 0, BlockHash: h0, Direction: remote.Direction_UNWIND},
			},
		}
		err = pool.OnNewBlock(ctx, change, txns2, TxnSlots{}, TxnSlots{})
		assert.NoError(err)
		check(txns2, TxnSlots{}, "fork2")
		checkNotify(txns2, TxnSlots{}, "fork2")

		change = &remote.StateChangeBatch{
			StateVersionId:      txID,
			PendingBlockBaseFee: pendingBaseFee,
			ChangeBatch: []*remote.StateChange{
				{BlockHeight: 1, BlockHash: h22},
			},
		}
		err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, txns3)
		assert.NoError(err)
		check(TxnSlots{}, txns3, "fork2 mined")
		checkNotify(TxnSlots{}, txns3, "fork2 mined")

		// add some remote txns from p2p
		pool.AddRemoteTxns(ctx, p2pReceived)
		err = pool.processRemoteTxns(ctx)
		assert.NoError(err)
		check(p2pReceived, TxnSlots{}, "p2pmsg1")
		checkNotify(p2pReceived, TxnSlots{}, "p2pmsg1")

		err = pool.flushLocked(tx) // we don't test eviction here, because dedicated test exists
		require.NoError(err)
		check(p2pReceived, TxnSlots{}, "after_flush")
		checkNotify(p2pReceived, TxnSlots{}, "after_flush")

		p2, err := New(ch, coreDB, txpoolcfg.DefaultConfig, sendersCache, *u256.N1, nil, nil, nil, nil, fixedgas.DefaultMaxBlobsPerBlock, nil, log.New())
		assert.NoError(err)

		p2.senders = pool.senders // senders are not persisted
		err = coreDB.View(ctx, func(coreTx kv.Tx) error { return p2.fromDB(ctx, tx, coreTx) })
		require.NoError(err)
		for _, txn := range p2.byHash {
			assert.Nil(txn.TxnSlot.Rlp)
		}

		check(txns2, TxnSlots{}, "fromDB")
		checkNotify(txns2, TxnSlots{}, "fromDB")
		assert.Equal(pool.senders.senderID, p2.senders.senderID)
		assert.Equal(pool.lastSeenBlock.Load(), p2.lastSeenBlock.Load())
		assert.Equal(pool.pending.Len(), p2.pending.Len())
		assert.Equal(pool.baseFee.Len(), p2.baseFee.Len())
		require.Equal(pool.queued.Len(), p2.queued.Len())
		assert.Equal(pool.pendingBaseFee.Load(), p2.pendingBaseFee.Load())
	})
}

func copyHashes(p *PendingPool) (hashes Hashes) {
	for i := range p.best.ms {
		hashes = append(hashes, p.best.ms[i].TxnSlot.IDHash[:]...)
	}
	return hashes
}

// extractNewHashes - extract from h1 hashes which do not exist in h2
func extractNewHashes(h1, h2 Hashes) (result Hashes) {
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
