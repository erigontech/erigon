// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rawdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/rawdb/utils"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// ReadCanonicalHash retrieves the hash assigned to a canonical block number.
func ReadCanonicalHash(db kv.Getter, number uint64) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.HeaderCanonical, hexutil.EncodeTs(number))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("failed ReadCanonicalHash: %w, number=%d", err, number)
	}
	if data == nil {
		return common.Hash{}, false, nil
	}
	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid canonical hash length %d for block %d", len(data), number)
	}
	return common.BytesToHash(data), true, nil
}

// WriteCanonicalHash stores the hash assigned to a canonical block number.
func WriteCanonicalHash(db kv.Putter, hash common.Hash, number uint64) error {
	if err := db.Put(kv.HeaderCanonical, hexutil.EncodeTs(number), hash[:]); err != nil {
		return fmt.Errorf("failed to store number to hash mapping: %w", err)
	}
	return nil
}

// TruncateCanonicalHash removes all the number to hash canonical mapping from block number N
// Mark chain as bad feature:
//   - BadBlock must be not available by hash
//   - but available by hash+num - if read num from kv.BadHeaderNumber table
//   - prune blocks: must delete Canonical/NonCanonical/BadBlocks also
func TruncateCanonicalHash(tx kv.RwTx, blockFrom uint64, markChainAsBad bool) error {
	if err := tx.ForEach(kv.HeaderCanonical, hexutil.EncodeTs(blockFrom), func(blockNumBytes, blockHash []byte) error {
		if markChainAsBad {
			if err := tx.Delete(kv.HeaderNumber, blockHash); err != nil {
				return err
			}
			if err := tx.Put(kv.BadHeaderNumber, blockHash, blockNumBytes); err != nil {
				return err
			}

			bheapMu.Lock()
			if bheapCache != nil {
				heap.Push(bheapCache, &utils.BlockId{Number: binary.BigEndian.Uint64(blockNumBytes), Hash: common.BytesToHash(blockHash)})
			}
			bheapMu.Unlock()
		}
		return tx.Delete(kv.HeaderCanonical, blockNumBytes)
	}); err != nil {
		return fmt.Errorf("TruncateCanonicalHash: %w", err)
	}
	return nil
}

/* latest bad blocks start */
var (
	bheapCache utils.ExtendedHeap
	bheapMu    sync.RWMutex
)

func GetLatestBadBlocks(tx kv.Tx) ([]*types.Block, error) {
	bheapMu.RLock()
	needsInit := bheapCache == nil
	bheapMu.RUnlock()

	if needsInit {
		if err := ResetBadBlockCache(tx, 100); err != nil {
			return nil, err
		}
	}

	bheapMu.RLock()
	blockIds := bheapCache.SortedValues()
	bheapMu.RUnlock()

	blocks := make([]*types.Block, len(blockIds))
	for i, blockId := range blockIds {
		block, ok, err := ReadBlock(tx, blockId.Hash, blockId.Number)
		if err != nil {
			return nil, err
		}
		if ok {
			blocks[i] = block
		}
	}

	return blocks, nil
}

// mainly for testing purposes
func ResetBadBlockCache(tx kv.Tx, limit int) error {
	bheapMu.Lock()
	bheapCache = utils.NewBlockMaxHeap(limit)
	bheapMu.Unlock()
	// load the heap
	return tx.ForEach(kv.BadHeaderNumber, nil, func(blockHash, blockNumBytes []byte) error {
		bheapMu.Lock()
		heap.Push(bheapCache, &utils.BlockId{Number: binary.BigEndian.Uint64(blockNumBytes), Hash: common.BytesToHash(blockHash)})
		bheapMu.Unlock()
		return nil
	})
}

/* latest bad blocks end */

func IsCanonicalHash(db kv.Getter, hash common.Hash, number uint64) (bool, error) {
	canonicalHash, ok, err := ReadCanonicalHash(db, number)
	if err != nil {
		return false, err
	}
	return ok && canonicalHash == hash, nil
}

// ReadHeaderNumber returns the header number assigned to a hash.
func ReadHeaderNumber(db kv.Getter, hash common.Hash) (uint64, bool, error) {
	data, err := db.GetOne(kv.HeaderNumber, hash[:])
	if err != nil {
		return 0, false, fmt.Errorf("read header number: %w", err)
	}
	if data == nil {
		return 0, false, nil
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("invalid header number length %d", len(data))
	}
	return binary.BigEndian.Uint64(data), true, nil
}

func ReadBadHeaderNumber(db kv.Getter, hash common.Hash) (uint64, bool, error) {
	data, err := db.GetOne(kv.BadHeaderNumber, hash[:])
	if err != nil {
		return 0, false, err
	}
	if data == nil {
		return 0, false, nil
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("invalid bad header number length %d", len(data))
	}
	return binary.BigEndian.Uint64(data), true, nil
}

// WriteHeaderNumber stores the hash->number mapping.
func WriteHeaderNumber(db kv.Putter, hash common.Hash, number uint64) error {
	if err := db.Put(kv.HeaderNumber, hash[:], hexutil.EncodeTs(number)); err != nil {
		return err
	}
	return nil
}

// ReadHeadHeaderHash retrieves the hash of the current canonical head header.
// It is updated in stage_headers, updateForkChoice.
// See: ReadHeadBlockHash
func ReadHeadHeaderHash(db kv.Getter) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.HeadHeaderKey, []byte(kv.HeadHeaderKey))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("read head header hash: %w", err)
	}
	if data == nil {
		return common.Hash{}, false, nil
	}
	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid head header hash length %d", len(data))
	}
	return common.BytesToHash(data), true, nil
}

// WriteHeadHeaderHash stores the hash of the current canonical head header.
// It is updated in stage_headers, updateForkChoice.
// See: WriteHeadBlockHash
func WriteHeadHeaderHash(db kv.Putter, hash common.Hash) error {
	if err := db.Put(kv.HeadHeaderKey, []byte(kv.HeadHeaderKey), hash[:]); err != nil {
		return fmt.Errorf("failed to store last header's hash: %w", err)
	}
	return nil
}

// ReadHeadBlockHash retrieves the hash of the current canonical head header for which its block body is known.
// It is updated in stage_finish.
// See: kv.HeadBlockKey
func ReadHeadBlockHash(db kv.Getter) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.HeadBlockKey, []byte(kv.HeadBlockKey))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("read head block hash: %w", err)
	}
	if data == nil {
		return common.Hash{}, false, nil
	}
	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid head block hash length %d", len(data))
	}
	return common.BytesToHash(data), true, nil
}

// WriteHeadBlockHash stores the hash of the current canonical head header for which its block body is known.
// It is updated in stage_finish.
// See: kv.HeadBlockKey
func WriteHeadBlockHash(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.HeadBlockKey, []byte(kv.HeadBlockKey), hash[:]); err != nil {
		log.Crit("Failed to store last block's hash", "err", err)
	}
}

// ReadForkchoiceHead retrieves headBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceHead(db kv.Getter) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.LastForkchoice, []byte("headBlockHash"))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("read forkchoice head: %w", err)
	}
	if data == nil {
		return common.Hash{}, false, nil
	}
	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid forkchoice head hash length %d", len(data))
	}
	return common.BytesToHash(data), true, nil
}

// WriteForkchoiceHead stores headBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceHead(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("headBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to store head block hash", "err", err)
	}
}

// ReadForkchoiceSafe retrieves safeBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceSafe(db kv.Getter) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.LastForkchoice, []byte("safeBlockHash"))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("read forkchoice safe hash: %w", err)
	}

	if data == nil {
		return common.Hash{}, false, nil
	}

	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid forkchoice safe hash length %d", len(data))
	}
	return common.BytesToHash(data), true, nil
}

// WriteForkchoiceSafe stores safeBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceSafe(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("safeBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to store safe block hash", "err", err)
	}
}

// ReadForkchoiceFinalized retrieves finalizedBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceFinalized(db kv.Getter) (common.Hash, bool, error) {
	data, err := db.GetOne(kv.LastForkchoice, []byte("finalizedBlockHash"))
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("read forkchoice finalized hash: %w", err)
	}

	if data == nil {
		return common.Hash{}, false, nil
	}

	if len(data) != length.Hash {
		return common.Hash{}, false, fmt.Errorf("invalid forkchoice finalized hash length %d", len(data))
	}
	return common.BytesToHash(data), true, nil
}

// WriteForkchoiceFinalized stores finalizedBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceFinalized(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("finalizedBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to safe finalized block hash", "err", err)
	}
}

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db kv.Getter, hash common.Hash, number uint64) (rlp.RawValue, bool, error) {
	data, err := db.GetOne(kv.Headers, dbutils.HeaderKey(number, hash))
	if err != nil {
		return nil, false, fmt.Errorf("read header RLP: %w", err)
	}
	if data == nil {
		return nil, false, nil
	}
	return data, true, nil
}

// ReadHeader retrieves the block header corresponding to the hash.
func ReadHeader(db kv.Getter, hash common.Hash, number uint64) (*types.Header, bool, error) {
	data, ok, err := ReadHeaderRLP(db, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	header := new(types.Header)
	if err := rlp.DecodeBytes(data, header); err != nil {
		return nil, false, fmt.Errorf("invalid block header RLP: hash=%x, number=%d: %w", hash, number, err)
	}
	return header, true, nil
}

func ReadCurrentBlockNumber(db kv.Getter) (uint64, bool, error) {
	headHash, ok, err := ReadHeadHeaderHash(db)
	if err != nil || !ok {
		return 0, false, err
	}
	return ReadHeaderNumber(db, headHash)
}

// ReadCurrentHeader reads the current canonical head header.
// It is updated in stage_headers, updateForkChoice.
// See: ReadHeadHeaderHash, ReadCurrentHeaderHavingBody
func ReadCurrentHeader(db kv.Getter) (*types.Header, bool, error) {
	headHash, ok, err := ReadHeadHeaderHash(db)
	if err != nil || !ok {
		return nil, false, err
	}
	headNumber, ok, err := ReadHeaderNumber(db, headHash)
	if err != nil || !ok {
		return nil, false, err
	}
	return ReadHeader(db, headHash, headNumber)
}

// ReadCurrentHeaderHavingBody reads the current canonical head header for which its block body is known.
// It is updated in stage_finish.
// See: ReadHeadBlockHash, ReadCurrentHeader
func ReadCurrentHeaderHavingBody(db kv.Getter) (*types.Header, bool, error) {
	headHash, ok, err := ReadHeadBlockHash(db)
	if err != nil || !ok {
		return nil, false, err
	}
	headNumber, ok, err := ReadHeaderNumber(db, headHash)
	if err != nil || !ok {
		return nil, false, err
	}
	return ReadHeader(db, headHash, headNumber)
}

func ReadHeadersByNumber(db kv.Tx, number uint64) (res []*types.Header, err error) {
	prefix := hexutil.EncodeTs(number)
	kvs, err := db.Prefix(kv.Headers, prefix)
	if err != nil {
		return nil, err
	}
	defer kvs.Close()
	for kvs.HasNext() {
		k, v, err := kvs.Next()
		if err != nil {
			return nil, err
		}
		header := new(types.Header)
		if err := rlp.DecodeBytes(v, header); err != nil {
			return nil, fmt.Errorf("invalid block header RLP: hash=%x, number=%d, err=%w", k[8:], number, err)
		}
		res = append(res, header)
	}
	return res, nil
}

// WriteHeader stores a block header into the database and also stores the hash-
// to-number mapping.
func WriteHeader(db kv.RwTx, header *types.Header) error {
	var (
		hash      = header.Hash()
		number    = header.Number.Uint64()
		headerKey = dbutils.HeaderKey(number, hash)
	)

	if err := WriteHeaderNumber(db, hash, number); err != nil {
		return fmt.Errorf("HeaderNumber mapping: %w", err)
	}

	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("WriteHeader: %w", err)
	}
	if err := db.Put(kv.Headers, headerKey, data); err != nil {
		return fmt.Errorf("WriteHeader: %w", err)
	}
	return nil
}

func WriteHeaderRaw(db kv.StatelessRwTx, number uint64, hash common.Hash, headerRlp []byte, skipIndexing bool) error {
	if err := db.Put(kv.Headers, dbutils.HeaderKey(number, hash), headerRlp); err != nil {
		return err
	}
	if skipIndexing {
		return nil
	}
	if err := WriteHeaderNumber(db, hash, number); err != nil {
		return err
	}
	return nil
}

// DeleteHeader - dangerous, use PruneBlocks/TruncateBlocks methods
func DeleteHeader(db kv.Putter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.Headers, dbutils.HeaderKey(number, hash)); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(kv.HeaderNumber, hash[:]); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db kv.Tx, hash common.Hash, number uint64) (rlp.RawValue, bool, error) {
	body, ok, err := ReadBodyWithTransactions(db, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	bodyRlp, err := rlp.EncodeToBytes(body)
	if err != nil {
		return nil, false, fmt.Errorf("encode block body RLP: %w", err)
	}
	return bodyRlp, true, nil
}

// Deprecated: use readBodyForStorage
func ReadStorageBodyRLP(db kv.Getter, hash common.Hash, number uint64) (rlp.RawValue, bool, error) {
	bodyRlp, err := db.GetOne(kv.BlockBody, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, false, fmt.Errorf("read storage body RLP: %w", err)
	}
	if bodyRlp == nil {
		return nil, false, nil
	}
	return bodyRlp, true, nil
}

func TxnByIdxInBlock(db kv.Getter, blockHash common.Hash, blockNum uint64, txIdxInBlock int) (types.Transaction, bool, error) {
	b, ok, err := ReadBodyForStorageByKey(db, dbutils.BlockBodyKey(blockNum, blockHash))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	v, err := db.GetOne(kv.EthTx, hexutil.EncodeTs(b.BaseTxnID.At(txIdxInBlock)))
	if err != nil {
		return nil, false, err
	}
	if v == nil {
		return nil, false, nil
	}
	txn, err := types.DecodeTransaction(v)
	if err != nil {
		return nil, false, err
	}
	return txn, true, nil
}

func CanonicalTransactions(db kv.Getter, txnID uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txs := make([]types.Transaction, amount)
	i := uint32(0)
	if err := db.ForAmount(kv.EthTx, hexutil.EncodeTs(txnID), amount, func(k, v []byte) error {
		var err error
		if txs[i], err = types.UnmarshalTransactionFromBinary(v, false /* blobTxnsAreWrappedWithBlobs */); err != nil {
			return err
		}
		i++
		return nil
	}); err != nil {
		return nil, err
	}
	txs = txs[:i] // user may request big "amount", but db can return small "amount". Return as much as we found.
	return txs, nil
}

// Write transactions into DB and use txnID as first identifier
func WriteTransactions(rwTx kv.RwTx, txs []types.Transaction, baseTxnID types.BaseTxnID) error {
	rawTxs := make([][]byte, len(txs))
	for i, txn := range txs {
		raw, err := rlp.EncodeToBytes(txn)
		if err != nil {
			return fmt.Errorf("broken txn rlp: %w", err)
		}
		rawTxs[i] = raw
	}
	return WriteRawTransactions(rwTx, rawTxs, baseTxnID)
}

// Write already encoded transactions into DB and use txnID as first identifier
func WriteRawTransactions(rwTx kv.RwTx, txs [][]byte, baseTxnID types.BaseTxnID) error {
	txIdKey := make([]byte, 8)
	for txi, txn := range txs {
		binary.BigEndian.PutUint64(txIdKey, baseTxnID.At(txi))
		if err := rwTx.Append(kv.EthTx, txIdKey, txn); err != nil {
			return fmt.Errorf("baseTxnID=%d: %w", baseTxnID, err)
		}
	}
	return nil
}

// WriteBodyForStorage stores an RLP encoded block body into the database.
func WriteBodyForStorage(db kv.Putter, hash common.Hash, number uint64, body *types.BodyForStorage) error {
	data, err := rlp.EncodeToBytes(body)
	if err != nil {
		return err
	}
	return db.Put(kv.BlockBody, dbutils.BlockBodyKey(number, hash), data)
}

func ReadBodyWithTransactions(db kv.Getter, hash common.Hash, number uint64) (*types.Body, bool, error) {
	body, firstTxId, txCount, ok, err := ReadBody(db, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	body.Transactions, err = CanonicalTransactions(db, firstTxId, txCount)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func RawTransactionsRange(db kv.Getter, from, to uint64) (res [][]byte, err error) {
	blockKey := make([]byte, dbutils.NumberLength+length.Hash)
	encNum := make([]byte, 8)
	for i := from; i < to+1; i++ {
		binary.BigEndian.PutUint64(encNum, i)
		hash, err := db.GetOne(kv.HeaderCanonical, encNum)
		if err != nil {
			return nil, err
		}
		if len(hash) == 0 {
			continue
		}

		binary.BigEndian.PutUint64(blockKey, i)
		copy(blockKey[dbutils.NumberLength:], hash)
		bodyRlp, err := db.GetOne(kv.BlockBody, blockKey)
		if err != nil {
			return nil, err
		}
		if len(bodyRlp) == 0 {
			continue
		}
		baseTxnID, txCount, err := types.DecodeOnlyTxMetadataFromBody(bodyRlp)
		if err != nil {
			return nil, err
		}
		if txCount <= 2 {
			continue
		}

		// TxCount counts the two system txns, which have no kv.EthTx entries;
		// reading from the system slot drifts into neighbouring blocks' txns.
		binary.BigEndian.PutUint64(encNum, baseTxnID.First())
		if err := db.ForAmount(kv.EthTx, encNum, txCount-2, func(k, v []byte) error {
			res = append(res, v)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return
}

func ReadBodyForStorageByKey(db kv.Getter, k []byte) (*types.BodyForStorage, bool, error) {
	bodyRlp, err := db.GetOne(kv.BlockBody, k)
	if err != nil {
		return nil, false, err
	}
	if bodyRlp == nil {
		return nil, false, nil
	}
	bodyForStorage := new(types.BodyForStorage)
	if err := rlp.DecodeBytes(bodyRlp, bodyForStorage); err != nil {
		return nil, false, err
	}

	return bodyForStorage, true, nil
}

func ReadBody(db kv.Getter, hash common.Hash, number uint64) (*types.Body, uint64, uint32, bool, error) {
	data, ok, err := ReadStorageBodyRLP(db, hash, number)
	if err != nil || !ok {
		return nil, 0, 0, false, err
	}
	bodyForStorage := new(types.BodyForStorage)
	if err := rlp.DecodeBytes(data, bodyForStorage); err != nil {
		return nil, 0, 0, false, fmt.Errorf("invalid block body RLP: hash=%x, number=%d: %w", hash, number, err)
	}
	body := new(types.Body)
	body.Uncles = bodyForStorage.Uncles
	body.Withdrawals = bodyForStorage.Withdrawals

	if bodyForStorage.TxCount < 2 {
		return nil, 0, 0, false, fmt.Errorf("invalid block body transaction count %d for block %d", bodyForStorage.TxCount, number)
	}
	return body, bodyForStorage.BaseTxnID.First(), bodyForStorage.TxCount - 2, true, nil
}

// ReadBlockAccessListBytes reads the RLP-encoded block access list sidecar for a block.
func ReadBlockAccessListBytes(db kv.Getter, hash common.Hash, number uint64) ([]byte, bool, error) {
	data, err := db.GetOne(kv.BlockAccessList, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		return nil, false, nil
	}
	return data, true, nil
}

// ReadBlockAccessList reads and decodes the block access list sidecar.
func ReadBlockAccessList(db kv.Getter, hash common.Hash, number uint64) (types.BlockAccessList, bool, error) {
	data, ok, err := ReadBlockAccessListBytes(db, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	bal, err := types.DecodeBlockAccessListBytes(data)
	if err != nil {
		return nil, false, err
	}
	return bal, true, nil
}

// WriteBlockAccessListBytes stores the RLP-encoded block access list sidecar for
// a block. This is secondary storage (serving, backfill, unwind); the primary
// carry into execution is Block.BlockAccessList(), written via the block overlay
// in InsertBlocks and flushed at commit.
func WriteBlockAccessListBytes(db kv.Putter, hash common.Hash, number uint64, data []byte) error {
	if err := db.Put(kv.BlockAccessList, dbutils.BlockBodyKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block access list: %w", err)
	}
	return nil
}

func HasSenders(db kv.Getter, hash common.Hash, number uint64) (bool, error) {
	return db.Has(kv.Senders, dbutils.BlockBodyKey(number, hash))
}

func ReadSenders(db kv.Getter, hash common.Hash, number uint64) ([]common.Address, bool, error) {
	key := dbutils.BlockBodyKey(number, hash)
	data, err := db.GetOne(kv.Senders, key)
	if err != nil {
		return nil, false, fmt.Errorf("read senders: %w", err)
	}
	if data == nil {
		ok, err := db.Has(kv.Senders, key)
		if err != nil {
			return nil, false, fmt.Errorf("check senders: %w", err)
		}
		if !ok {
			return nil, false, nil
		}
	}
	if len(data)%length.Addr != 0 {
		return nil, false, fmt.Errorf("invalid senders length %d", len(data))
	}
	senders := make([]common.Address, len(data)/length.Addr)
	for i := range senders {
		copy(senders[i][:], data[i*length.Addr:])
	}
	return senders, true, nil
}

func WriteRawBodyIfNotExists(db kv.RwTx, hash common.Hash, number uint64, body *types.RawBody) (ok bool, err error) {
	exists, err := db.Has(kv.BlockBody, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	return WriteRawBody(db, hash, number, body)
}

func WriteRawBody(db kv.RwTx, hash common.Hash, number uint64, body *types.RawBody) (ok bool, err error) {
	baseTxnID, err := db.IncrementSequence(kv.EthTx, uint64(types.TxCountToTxAmount(len(body.Transactions))))
	if err != nil {
		return false, err
	}
	data := types.BodyForStorage{
		BaseTxnID:   types.BaseTxnID(baseTxnID),
		TxCount:     types.TxCountToTxAmount(len(body.Transactions)), /*system txs*/
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}
	if err = WriteBodyForStorage(db, hash, number, &data); err != nil {
		return false, fmt.Errorf("WriteBodyForStorage: %w", err)
	}
	if err = WriteRawTransactions(db, body.Transactions, data.BaseTxnID); err != nil {
		return false, fmt.Errorf("WriteRawTransactions: %w", err)
	}
	return true, nil
}

func WriteBody(db kv.RwTx, hash common.Hash, number uint64, body *types.Body) (err error) {
	// Pre-processing
	body.SendersFromTxs()

	baseTxnID, err := db.IncrementSequence(kv.EthTx, uint64(types.TxCountToTxAmount(len(body.Transactions))))
	if err != nil {
		return err
	}
	data := types.BodyForStorage{
		BaseTxnID:   types.BaseTxnID(baseTxnID),
		TxCount:     types.TxCountToTxAmount(len(body.Transactions)),
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}
	if err = WriteBodyForStorage(db, hash, number, &data); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}
	if err = WriteTransactions(db, body.Transactions, data.BaseTxnID); err != nil {
		return fmt.Errorf("failed to WriteTransactions: %w", err)
	}
	return nil
}

func WriteSenders(db kv.Putter, hash common.Hash, number uint64, senders []common.Address) error {
	data := make([]byte, length.Addr*len(senders))
	for i, sender := range senders {
		copy(data[i*length.Addr:], sender[:])
	}
	if err := db.Put(kv.Senders, dbutils.BlockBodyKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block senders: %w", err)
	}
	return nil
}

// DeleteBody removes all block body data associated with a hash.
func DeleteBody(db kv.Putter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.BlockBody, dbutils.BlockBodyKey(number, hash)); err != nil {
		log.Crit("Failed to delete block body", "err", err)
	}
	if err := db.Delete(kv.BlockAccessList, dbutils.BlockBodyKey(number, hash)); err != nil {
		log.Crit("Failed to delete block access list", "err", err)
	}
}

func AppendCanonicalTxNums(tx kv.RwTx, from uint64) (err error) {
	nextBaseTxNum := 0
	if from > 0 {
		nextBaseTxNumFromDb, err := rawdbv3.TxNums.Max(context.Background(), tx, from-1)
		if err != nil {
			return err
		}
		nextBaseTxNum = int(nextBaseTxNumFromDb)
		nextBaseTxNum++
	}
	for blockNum := from; ; blockNum++ {
		h, ok, err := ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		data, ok, err := ReadStorageBodyRLP(tx, h, blockNum)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		bodyForStorage := types.BodyForStorage{}
		if err := rlp.DecodeBytes(data, &bodyForStorage); err != nil {
			return err
		}

		nextBaseTxNum += int(bodyForStorage.TxCount)
		err = rawdbv3.TxNums.Append(tx, blockNum, uint64(nextBaseTxNum-1))
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadTd retrieves a block's total difficulty corresponding to the hash.
func ReadTd(db kv.Getter, hash common.Hash, number uint64) (*uint256.Int, bool, error) {
	data, err := db.GetOne(kv.HeaderTD, dbutils.HeaderKey(number, hash))
	if err != nil {
		return nil, false, fmt.Errorf("failed ReadTd: %w", err)
	}
	if data == nil {
		return nil, false, nil
	}
	td := new(uint256.Int)
	if err := rlp.DecodeBytes(data, td); err != nil {
		return nil, false, fmt.Errorf("invalid block total difficulty RLP: %x, %w", hash, err)
	}
	return td, true, nil
}

func ReadTdByHash(db kv.Getter, hash common.Hash) (*uint256.Int, bool, error) {
	headNumber, ok, err := ReadHeaderNumber(db, hash)
	if err != nil || !ok {
		return nil, false, err
	}
	return ReadTd(db, hash, headNumber)
}

// WriteTd stores the total difficulty of a block into the database.
func WriteTd(db kv.Putter, hash common.Hash, number uint64, td uint256.Int) error {
	data, err := rlp.EncodeToBytes(&td)
	if err != nil {
		return fmt.Errorf("failed to RLP encode block total difficulty: %w", err)
	}
	if err := db.Put(kv.HeaderTD, dbutils.HeaderKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block total difficulty: %w", err)
	}
	return nil
}

// TruncateTd removes all block total difficulty from block number N
func TruncateTd(tx kv.RwTx, blockFrom uint64) error {
	if err := tx.ForEach(kv.HeaderTD, hexutil.EncodeTs(blockFrom), func(k, _ []byte) error {
		return tx.Delete(kv.HeaderTD, k)
	}); err != nil {
		return fmt.Errorf("TruncateTd: %w", err)
	}
	return nil
}

// ReadBlock retrieves an entire block corresponding to the hash, assembling it
// from the stored header and body.
func ReadBlock(tx kv.Getter, hash common.Hash, number uint64) (*types.Block, bool, error) {
	header, ok, err := ReadHeader(tx, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	body, ok, err := ReadBodyWithTransactions(tx, hash, number)
	if err != nil || !ok {
		return nil, false, err
	}
	block := types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles, body.Withdrawals, nil)
	return block, true, nil
}

func ReadBlockWithSenders(db kv.Getter, hash common.Hash, number uint64) (*types.Block, []common.Address, bool, error) {
	block, ok, err := ReadBlock(db, hash, number)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	senders, _, err := ReadSenders(db, hash, number)
	if err != nil {
		return nil, nil, false, err
	}
	if len(senders) == block.Transactions().Len() {
		block.SendersToTxs(senders)
	}
	return block, senders, true, nil
}

// HasBlock - is more efficient than ReadBlock because doesn't read transactions.
// It's is not equivalent of HasHeader because headers and bodies written by different stages
func HasBlock(db kv.Getter, hash common.Hash, number uint64) (bool, error) {
	_, ok, err := ReadStorageBodyRLP(db, hash, number)
	return ok, err
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlock(db kv.RwTx, block *types.Block) error {
	if err := WriteHeader(db, block.HeaderNoCopy()); err != nil {
		return err
	}
	if err := WriteBody(db, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		return err
	}
	return nil
}

// PruneBlocks - delete [1, to) old blocks after moving it to snapshots.
// keeps genesis in db: [1, to)
// doesn't change sequences of kv.EthTx
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
// Returns false if there is nothing to prune
func PruneBlocks(tx kv.RwTx, blockTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	c, err := tx.Cursor(kv.Headers)
	if err != nil {
		return deleted, err
	}
	defer c.Close()

	// find first non-genesis block
	firstK, _, err := c.Seek(hexutil.EncodeTs(1))
	if err != nil {
		return deleted, err
	}
	if firstK == nil { //nothing to delete
		return deleted, err
	}
	blockFrom := binary.BigEndian.Uint64(firstK)
	stopAtBlock := min(blockTo, blockFrom+uint64(blocksDeleteLimit))

	var b *types.BodyForStorage

	for k, _, err := c.Current(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return deleted, err
		}

		n := binary.BigEndian.Uint64(k)
		if n >= stopAtBlock { // [from, to)
			break
		}

		var ok bool
		b, ok, err = ReadBodyForStorageByKey(tx, k)
		if err != nil {
			return deleted, err
		}
		if !ok {
			log.Debug("PruneBlocks: block body not found", "height", n)
		} else {
			txIDBytes := make([]byte, 8)
			for txID := b.BaseTxnID.U64(); txID <= b.BaseTxnID.LastSystemTx(b.TxCount); txID++ {
				binary.BigEndian.PutUint64(txIDBytes, txID)
				if err := tx.Delete(kv.EthTx, txIDBytes); err != nil {
					return deleted, err
				}
			}
		}
		// Copying k because otherwise the same memory will be reused
		// for the next key and Delete below will end up deleting 1 more record than required
		kCopy := bytes.Clone(k)
		if err := tx.Delete(kv.Senders, kCopy); err != nil {
			return deleted, err
		}
		if err := tx.Delete(kv.BlockBody, kCopy); err != nil {
			return deleted, err
		}
		if err := tx.Delete(kv.BlockAccessList, kCopy); err != nil {
			return deleted, err
		}
		if err := tx.Delete(kv.Headers, kCopy); err != nil {
			return deleted, err
		}

		deleted++
	}

	return deleted, nil
}

func TruncateCanonicalChain(ctx context.Context, db kv.RwTx, from uint64) error {
	return db.ForEach(kv.HeaderCanonical, hexutil.EncodeTs(from), func(k, _ []byte) error {
		return db.Delete(kv.HeaderCanonical, k)
	})
}

// TruncateBlocks - delete block >= blockFrom
// does decrement sequences of kv.EthTx
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func TruncateBlocks(ctx context.Context, tx kv.RwTx, blockFrom uint64) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	if blockFrom < 1 { //protect genesis
		blockFrom = 1
	}
	return tx.ForEach(kv.Headers, hexutil.EncodeTs(blockFrom), func(k, v []byte) error {
		b, ok, err := ReadBodyForStorageByKey(tx, k)
		if err != nil {
			return err
		}
		if ok {
			txIDBytes := make([]byte, 8)
			for txID := b.BaseTxnID.U64(); txID <= b.BaseTxnID.LastSystemTx(b.TxCount); txID++ {
				binary.BigEndian.PutUint64(txIDBytes, txID)
				if err := tx.Delete(kv.EthTx, txIDBytes); err != nil {
					return err
				}
			}
		}
		// Copying k because otherwise the same memory will be reused
		// for the next key and Delete below will end up deleting 1 more record than required
		kCopy := bytes.Clone(k)
		if err := tx.Delete(kv.Senders, kCopy); err != nil {
			return err
		}
		if err := tx.Delete(kv.BlockBody, kCopy); err != nil {
			return err
		}
		if err := tx.Delete(kv.BlockAccessList, kCopy); err != nil {
			return err
		}
		if err := tx.Delete(kv.Headers, kCopy); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("TruncateBlocks", "block", binary.BigEndian.Uint64(kCopy))
		default:
		}
		return nil
	})
}

func ReadHeaderByNumber(db kv.Getter, number uint64) (*types.Header, bool, error) {
	hash, ok, err := ReadCanonicalHash(db, number)
	if err != nil || !ok {
		return nil, false, err
	}

	return ReadHeader(db, hash, number)
}

func ReadFirstNonGenesisHeaderNumber(tx kv.Tx) (uint64, bool, error) {
	v, err := rawdbv3.SecondKey(tx, kv.Headers)
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func ReadHeaderByHash(db kv.Getter, hash common.Hash) (*types.Header, bool, error) {
	number, ok, err := ReadHeaderNumber(db, hash)
	if err != nil || !ok {
		return nil, false, err
	}
	return ReadHeader(db, hash, number)
}

// DeleteNewerEpochs drops [blockNum, ∞)
func DeleteNewerEpochs(tx kv.RwTx, number uint64) error {
	if err := tx.ForEach(kv.PendingEpoch, hexutil.EncodeTs(number), func(k, v []byte) error {
		return tx.Delete(kv.PendingEpoch, k)
	}); err != nil {
		return err
	}
	return tx.ForEach(kv.Epoch, hexutil.EncodeTs(number), func(k, v []byte) error {
		return tx.Delete(kv.Epoch, k)
	})
}

func ReadEpoch(tx kv.Tx, blockNum uint64, blockHash common.Hash) (transitionProof []byte, ok bool, err error) {
	k := make([]byte, dbutils.NumberLength+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[dbutils.NumberLength:], blockHash[:])
	transitionProof, err = tx.GetOne(kv.Epoch, k)
	if err != nil || transitionProof != nil {
		return transitionProof, transitionProof != nil, err
	}
	ok, err = tx.Has(kv.Epoch, k)
	return transitionProof, ok, err
}

func FindEpochBeforeOrEqualNumber(tx kv.Tx, n uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, ok bool, err error) {
	c, err := tx.Cursor(kv.Epoch)
	if err != nil {
		return 0, common.Hash{}, nil, false, err
	}
	defer c.Close()
	seek := hexutil.EncodeTs(n)
	k, v, err := c.Seek(seek)
	if err != nil {
		return 0, common.Hash{}, nil, false, err
	}
	if k != nil {
		if len(k) != dbutils.NumberLength+length.Hash {
			return 0, common.Hash{}, nil, false, fmt.Errorf("invalid epoch key length %d", len(k))
		}
		num := binary.BigEndian.Uint64(k)
		if num == n {
			return n, common.BytesToHash(k[dbutils.NumberLength:]), v, true, nil
		}
	}
	k, v, err = c.Prev()
	if err != nil {
		return 0, common.Hash{}, nil, false, err
	}
	if k == nil {
		return 0, common.Hash{}, nil, false, nil
	}
	if len(k) != dbutils.NumberLength+length.Hash {
		return 0, common.Hash{}, nil, false, fmt.Errorf("invalid epoch key length %d", len(k))
	}
	return binary.BigEndian.Uint64(k), common.BytesToHash(k[dbutils.NumberLength:]), v, true, nil
}

func WriteEpoch(tx kv.RwTx, blockNum uint64, blockHash common.Hash, transitionProof []byte) (err error) {
	k := make([]byte, dbutils.NumberLength+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[dbutils.NumberLength:], blockHash[:])
	return tx.Put(kv.Epoch, k, transitionProof)
}

func ReadPendingEpoch(tx kv.Tx, blockNum uint64, blockHash common.Hash) (transitionProof []byte, ok bool, err error) {
	k := make([]byte, 8+32)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[8:], blockHash[:])
	transitionProof, err = tx.GetOne(kv.PendingEpoch, k)
	if err != nil || transitionProof != nil {
		return transitionProof, transitionProof != nil, err
	}
	ok, err = tx.Has(kv.PendingEpoch, k)
	return transitionProof, ok, err
}

func WritePendingEpoch(tx kv.RwTx, blockNum uint64, blockHash common.Hash, transitionProof []byte) (err error) {
	k := make([]byte, 8+32)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[8:], blockHash[:])
	return tx.Put(kv.PendingEpoch, k, transitionProof)
}

// Transitioned returns true if the block number comes after POS transition or is the last POW block
func Transitioned(db kv.Getter, blockNum uint64, terminalTotalDifficulty *uint256.Int) (trans bool, err error) {
	if terminalTotalDifficulty == nil {
		return false, nil
	}

	if terminalTotalDifficulty.Sign() == 0 {
		return true, nil
	}
	header, ok, err := ReadHeaderByNumber(db, blockNum)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if header.Difficulty.Sign() == 0 {
		return true, nil
	}

	headerTd, ok, err := ReadTd(db, header.Hash(), blockNum)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return headerTd.Cmp(terminalTotalDifficulty) >= 0, nil
}

// IsPosBlock returns true if the block number comes after POS transition or is the last POW block
func IsPosBlock(db kv.Getter, blockHash common.Hash) (trans bool, err error) {
	header, ok, err := ReadHeaderByHash(db, blockHash)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return header.Difficulty.Sign() == 0, nil
}

// PruneTable has `limit` parameter to avoid too large data deletes per one sync cycle - better delete by small portions to reduce db.FreeList size
func PruneTable(tx kv.RwTx, table string, pruneTo uint64, ctx context.Context, limit int, timeout time.Duration, logger log.Logger, logPrefix string) error {
	t := time.Now()
	c, err := tx.RwCursor(table)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	i := 0
	for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		i++
		if i > limit {
			break
		}

		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}

		if err = c.DeleteCurrent(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
		if i%1000 == 0 {
			select {
			case <-ctx.Done():
				return common.ErrStopped
			case <-logEvery.C:
				logger.Info(fmt.Sprintf("[%s] pruning table periodic progress", logPrefix), "table", table, "blockNum", blockNum)
			default:
			}
			if time.Since(t) > timeout {
				break
			}
		}
	}
	return nil
}

func PruneTableDupSort(tx kv.RwTx, table string, logPrefix string, pruneTo uint64, logEvery *time.Ticker, ctx context.Context) error {
	c, err := tx.RwCursorDupSort(table)
	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

	for k, _, err := c.First(); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return fmt.Errorf("failed to move %s cleanup cursor: %w", table, err)
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= pruneTo {
			break
		}
		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s]", logPrefix), "table", table, "block", blockNum)
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = tx.Delete(table, k); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
		}
	}
	return nil
}

func WriteDBSchemaVersion(tx kv.RwTx) error {
	var version [12]byte
	binary.BigEndian.PutUint32(version[:], kv.DBSchemaVersion.Major)
	binary.BigEndian.PutUint32(version[4:], kv.DBSchemaVersion.Minor)
	binary.BigEndian.PutUint32(version[8:], kv.DBSchemaVersion.Patch)
	if err := tx.Put(kv.DatabaseInfo, kv.DBSchemaVersionKey, version[:]); err != nil {
		return fmt.Errorf("writing DB schema version: %w", err)
	}
	return nil
}

func ReadDBSchemaVersion(tx kv.Tx) (major, minor, patch uint32, ok bool, err error) {
	existingVersion, err := tx.GetOne(kv.DatabaseInfo, kv.DBSchemaVersionKey)
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("reading DB schema version: %w", err)
	}
	if existingVersion == nil {
		return 0, 0, 0, false, nil
	}
	if len(existingVersion) != 12 {
		return 0, 0, 0, false, fmt.Errorf("incorrect length of DB schema version: %d", len(existingVersion))
	}

	major = binary.BigEndian.Uint32(existingVersion)
	minor = binary.BigEndian.Uint32(existingVersion[4:])
	patch = binary.BigEndian.Uint32(existingVersion[8:])
	return major, minor, patch, true, nil
}

func ReadDBCommitmentHistoryEnabled(tx kv.Tx) (bool, bool, error) {
	commitmentHistoryEnabled, err := tx.GetOne(kv.DatabaseInfo, kv.CommitmentLayoutFlagKey)
	if err != nil {
		return false, false, fmt.Errorf("reading DB commitment history enabled flag: %w", err)
	}
	if commitmentHistoryEnabled == nil {
		return false, false, nil
	}
	if len(commitmentHistoryEnabled) != 1 {
		return false, false, fmt.Errorf("incorrect length of DB commitment history enabled flag: %d", len(commitmentHistoryEnabled))
	}
	if bytes.Equal(commitmentHistoryEnabled, kv.CommitmentLayoutFlagEnabledVal) {
		return true, true, nil
	}
	if bytes.Equal(commitmentHistoryEnabled, kv.CommitmentLayoutFlagDisabledVal) {
		return false, true, nil
	}
	return false, false, fmt.Errorf("incorrect value of DB commitment history enabled flag: %x", commitmentHistoryEnabled)
}

func WriteDBCommitmentHistoryEnabled(tx kv.RwTx, enabled bool) error {
	var value []byte
	if enabled {
		value = kv.CommitmentLayoutFlagEnabledVal
	} else {
		value = kv.CommitmentLayoutFlagDisabledVal
	}
	if err := tx.Put(kv.DatabaseInfo, kv.CommitmentLayoutFlagKey, value); err != nil {
		return fmt.Errorf("writing DB commitment history enabled flag: %w", err)
	}
	return nil
}

type RCacheV2Query struct {
	BlockNum  uint64
	BlockHash common.Hash
	TxnHash   common.Hash
	TxNum     uint64

	DontCalcBloom bool // avoid calculating bloom (can be bottleneck)
}

// doesn't do DeriveFieldsV4ForCachedReceipt
func ReceiptCacheV2Stream(tx kv.TemporalTx, fromTxNum, toTxNum uint64) (stream.Duo[uint64, *types.Receipt], error) {
	it, err := tx.Debug().TraceKey(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, fromTxNum, toTxNum)
	if err != nil {
		return nil, err
	}

	return stream.TransformDuoV(it, func(txNum uint64, v []byte) (uint64, *types.Receipt, error) {
		if len(v) == 0 {
			return txNum, nil, nil
		}

		//fmt.Println("stream", "txnum", txNum, "v", len(v))

		receipt := &types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, receipt); err != nil {
			return txNum, nil, fmt.Errorf("%w, of txNum %d, len(v)=%d", err, txNum, len(v))
		}
		res := (*types.Receipt)(receipt)
		//res.DeriveFieldsV4ForCachedReceipt(query.BlockHash, query.BlockNum, query.TxnHash, !query.DontCalcBloom)
		return txNum, res, nil
	}), nil
}

func ReadReceiptCacheV2(tx kv.TemporalTx, query RCacheV2Query) (*types.Receipt, bool, error) {
	v, ok, err := tx.HistorySeek(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, query.TxNum+1 /*history storing value BEFORE-change*/)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if len(v) == 0 {
		return nil, false, nil
	}

	// Convert the receipts from their storage form to their internal representation
	receipt := &types.ReceiptForStorage{}
	if err := rlp.DecodeBytes(v, receipt); err != nil {
		return nil, false, fmt.Errorf("%w, of block %d, len(v)=%d", err, query.BlockNum, len(v))
	}
	res := (*types.Receipt)(receipt)
	res.DeriveFieldsV4ForCachedReceipt(query.BlockHash, query.BlockNum, query.TxnHash, !query.DontCalcBloom)
	return res, true, nil
}

func ReadReceiptsCacheV2(tx kv.TemporalTx, block *types.Block, txNumReader rawdbv3.TxNumsReader) (res types.Receipts, err error) {
	blockHash := block.Hash()
	blockNum := block.NumberU64()
	transactions := block.Transactions()

	minTxNum, err := txNumReader.Min(context.Background(), tx, blockNum)
	if err != nil {
		return
	}
	maxTxNum, err := txNumReader.Max(context.Background(), tx, blockNum)
	if err != nil {
		return
	}

	receiptIdx := 0
	for txNum := minTxNum; txNum < maxTxNum+1; txNum++ {
		v, ok, err := tx.HistorySeek(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, txNum+1)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if len(v) == 0 {
			continue
		}

		// Convert the receipts from their storage form to their internal representation
		receipt := &types.ReceiptForStorage{}
		if err := rlp.DecodeBytes(v, receipt); err != nil {
			return nil, fmt.Errorf("ReadReceiptsCacheV2: deserialize %d, len(v)=%d, %w", blockNum, len(v), err)
		}
		x := (*types.Receipt)(receipt)
		txnIdx := int(receipt.TransactionIndex)
		if txnIdx < 0 || txnIdx != receiptIdx || txnIdx >= len(transactions) {
			// RCacheV2 is read in txNum order. The stored transaction index must
			// match the next receipt position, otherwise derive the block instead.
			return nil, nil
		}
		txn := transactions[txnIdx]
		x.DeriveFieldsV4ForCachedReceipt(blockHash, blockNum, txn.Hash(), true)
		res = append(res, x)
		receiptIdx++
	}
	if receiptIdx != len(transactions) {
		// Block receipt RPCs require the full ordered receipt set.
		return nil, nil
	}
	return res, nil
}

func WriteReceiptCacheV2(tx kv.TemporalPutDel, receipt *types.Receipt, txNum uint64) error {
	var w rawtemporaldb.ReceiptWriter
	return w.Append(tx, receipt, txNum)
}
