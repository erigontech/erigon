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

package txpool

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/google/btree"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// BySenderAndNonce - designed to perform most expensive operation in TxPool:
// "recalculate all ephemeral fields of all transactions" by algo
//   - for all senders - iterate over all transactions in nonce growing order
//
// Performances decisions:
//   - All senders stored inside 1 large BTree - because iterate over 1 BTree is faster than over map[senderId]BTree
//   - sortByNonce used as non-pointer wrapper - because iterate over BTree of pointers is 2x slower
type BySenderAndNonce struct {
	tree              *btree.BTreeG[*metaTxn]
	search            *metaTxn
	senderIDTxnCount  map[uint64]int    // count of sender's txns in the pool - may differ from nonce
	senderIDBlobCount map[uint64]uint64 // count of sender's total number of blobs in the pool
}

func (b *BySenderAndNonce) nonce(senderID uint64) (nonce uint64, ok bool) {
	s := b.search
	s.TxnSlot.SenderID = senderID
	s.TxnSlot.Nonce = math.MaxUint64

	b.tree.DescendLessOrEqual(s, func(mt *metaTxn) bool {
		if mt.TxnSlot.SenderID == senderID {
			nonce = mt.TxnSlot.Nonce
			ok = true
		}
		return false
	})
	return nonce, ok
}

func (b *BySenderAndNonce) ascendAll(f func(*metaTxn) bool) {
	b.tree.Ascend(func(mt *metaTxn) bool {
		return f(mt)
	})
}

func (b *BySenderAndNonce) ascend(senderID uint64, f func(*metaTxn) bool) {
	s := b.search
	s.TxnSlot.SenderID = senderID
	s.TxnSlot.Nonce = 0
	b.tree.AscendGreaterOrEqual(s, func(mt *metaTxn) bool {
		if mt.TxnSlot.SenderID != senderID {
			return false
		}
		return f(mt)
	})
}

func (b *BySenderAndNonce) descend(senderID uint64, f func(*metaTxn) bool) {
	s := b.search
	s.TxnSlot.SenderID = senderID
	s.TxnSlot.Nonce = math.MaxUint64
	b.tree.DescendLessOrEqual(s, func(mt *metaTxn) bool {
		if mt.TxnSlot.SenderID != senderID {
			return false
		}
		return f(mt)
	})
}

func (b *BySenderAndNonce) count(senderID uint64) int {
	return b.senderIDTxnCount[senderID]
}

func (b *BySenderAndNonce) blobCount(senderID uint64) uint64 {
	return b.senderIDBlobCount[senderID]
}

func (b *BySenderAndNonce) hasTxs(senderID uint64) bool {
	has := false
	b.ascend(senderID, func(*metaTxn) bool {
		has = true
		return false
	})
	return has
}

func (b *BySenderAndNonce) get(senderID, txNonce uint64) *metaTxn {
	s := b.search
	s.TxnSlot.SenderID = senderID
	s.TxnSlot.Nonce = txNonce
	if found, ok := b.tree.Get(s); ok {
		return found
	}
	return nil
}

// nolint
func (b *BySenderAndNonce) has(mt *metaTxn) bool {
	return b.tree.Has(mt)
}

func (b *BySenderAndNonce) delete(mt *metaTxn, reason txpoolcfg.DiscardReason, logger log.Logger) {
	if _, ok := b.tree.Delete(mt); ok {
		if mt.TxnSlot.Traced {
			logger.Info("TX TRACING: Deleted txn by nonce", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "sender", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce, "reason", reason)
		}

		senderID := mt.TxnSlot.SenderID
		count := b.senderIDTxnCount[senderID]
		if count > 1 {
			b.senderIDTxnCount[senderID] = count - 1
		} else {
			delete(b.senderIDTxnCount, senderID)
		}

		if mt.TxnSlot.Type == BlobTxType && mt.TxnSlot.Blobs != nil {
			accBlobCount := b.senderIDBlobCount[senderID]
			txnBlobCount := len(mt.TxnSlot.Blobs)
			if txnBlobCount > 1 {
				b.senderIDBlobCount[senderID] = accBlobCount - uint64(txnBlobCount)
			} else {
				delete(b.senderIDBlobCount, senderID)
			}
		}
	}
}

func (b *BySenderAndNonce) replaceOrInsert(mt *metaTxn, logger log.Logger) *metaTxn {
	it, ok := b.tree.ReplaceOrInsert(mt)

	if ok {
		if mt.TxnSlot.Traced {
			logger.Info("TX TRACING: Replaced txn by nonce", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "sender", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce)
		}
		return it
	}

	if mt.TxnSlot.Traced {
		logger.Info("TX TRACING: Inserted txn by nonce", "idHash", fmt.Sprintf("%x", mt.TxnSlot.IDHash), "sender", mt.TxnSlot.SenderID, "nonce", mt.TxnSlot.Nonce)
	}

	b.senderIDTxnCount[mt.TxnSlot.SenderID]++
	if mt.TxnSlot.Type == BlobTxType && mt.TxnSlot.Blobs != nil {
		b.senderIDBlobCount[mt.TxnSlot.SenderID] += uint64(len(mt.TxnSlot.Blobs))
	}
	return nil
}

// sendersBatch stores in-memory senders-related objects - which are different from DB (updated/dirty)
// flushing to db periodically. it doesn't play as read-cache (because db is small and memory-mapped - doesn't need cache)
// non thread-safe
type sendersBatch struct {
	senderIDs     map[common.Address]uint64
	senderID2Addr map[uint64]common.Address
	tracedSenders map[common.Address]struct{}
	senderID      uint64
}

func newSendersBatch(tracedSenders map[common.Address]struct{}) *sendersBatch {
	return &sendersBatch{
		senderIDs:     map[common.Address]uint64{},
		senderID2Addr: map[uint64]common.Address{},
		tracedSenders: tracedSenders,
	}
}

func (sc *sendersBatch) getID(addr common.Address) (uint64, bool) {
	id, ok := sc.senderIDs[addr]
	return id, ok
}

var traceAllSenders = false

func (sc *sendersBatch) getOrCreateID(addr common.Address, logger log.Logger) (uint64, bool) {
	_, traced := sc.tracedSenders[addr]

	if !traced {
		//goland:noinspection ALL
		traced = traceAllSenders
	}

	id, ok := sc.senderIDs[addr]
	if !ok {
		sc.senderID++
		id = sc.senderID
		sc.senderIDs[addr] = id
		sc.senderID2Addr[id] = addr
		if traced {
			logger.Info(fmt.Sprintf("TX TRACING: allocated senderID %d to sender %x", id, addr))
		}
	}
	return id, traced
}

func (sc *sendersBatch) info(cacheView kvcache.CacheView, id uint64) (nonce uint64, balance uint256.Int, err error) {
	addr, ok := sc.senderID2Addr[id]
	if !ok {
		panic("must not happen")
	}
	encoded, err := cacheView.Get(addr.Bytes())
	if err != nil {
		return 0, uint256.Int{}, err
	}
	if len(encoded) == 0 {
		return 0, uint256.Int{}, nil
	}
	if cacheView.StateV3() {
		var bp *uint256.Int
		nonce, bp, _ = types2.DecodeAccountBytesV3(encoded)
		balance = *bp
	} else {
		nonce, balance, err = DecodeSender(encoded)
	}
	if err != nil {
		return 0, uint256.Int{}, err
	}
	return nonce, balance, nil
}

func (sc *sendersBatch) registerNewSenders(newTxs *TxnSlots, logger log.Logger) (err error) {
	for i, txn := range newTxs.Txs {
		txn.SenderID, txn.Traced = sc.getOrCreateID(newTxs.Senders.AddressAt(i), logger)
	}
	return nil
}

func (sc *sendersBatch) onNewBlock(stateChanges *remote.StateChangeBatch, unwindTxs, minedTxs TxnSlots, logger log.Logger) error {
	for _, diff := range stateChanges.ChangeBatch {
		for _, change := range diff.Changes { // merge state changes
			addrB := gointerfaces.ConvertH160toAddress(change.Address)
			sc.getOrCreateID(addrB, logger)
		}

		for i, txn := range unwindTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(unwindTxs.Senders.AddressAt(i), logger)
		}

		for i, txn := range minedTxs.Txs {
			txn.SenderID, txn.Traced = sc.getOrCreateID(minedTxs.Senders.AddressAt(i), logger)
		}
	}
	return nil
}

func EncodeSenderLengthForStorage(nonce uint64, balance uint256.Int) uint {
	var structLength uint = 1 // 1 byte for fieldset
	if !balance.IsZero() {
		structLength += uint(balance.ByteLen()) + 1
	}
	if nonce > 0 {
		structLength += uint(common.BitLenToByteLen(bits.Len64(nonce))) + 1
	}
	return structLength
}

// Encode the details of txn sender into the given "buffer" byte-slice that should be big enough
func EncodeSender(nonce uint64, balance uint256.Int, buffer []byte) {
	var fieldSet = 0 // start with first bit set to 0
	var pos = 1
	if nonce > 0 {
		fieldSet = 1
		nonceBytes := common.BitLenToByteLen(bits.Len64(nonce))
		buffer[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}

	// Encoding balance
	if !balance.IsZero() {
		fieldSet |= 2
		balanceBytes := balance.ByteLen()
		buffer[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes //nolint
	}

	buffer[0] = byte(fieldSet)
}

// Decode the sender's balance and nonce from encoded byte-slice
func DecodeSender(enc []byte) (nonce uint64, balance uint256.Int, err error) {
	if len(enc) == 0 {
		return
	}

	var fieldSet = enc[0]
	var pos = 1

	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		nonce = common.BytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return nonce, balance, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		(&balance).SetBytes(enc[pos+1 : pos+decodeLength+1])
	}
	return
}
