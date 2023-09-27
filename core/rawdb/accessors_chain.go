// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/gballet/go-verkle"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/rlp"
)

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

// ReadCanonicalHash retrieves the hash assigned to a canonical block number.
func ReadCanonicalHash(db kv.Getter, number uint64) (common.Hash, error) {
	data, err := db.GetOne(kv.HeaderCanonical, hexutility.EncodeTs(number))
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed ReadCanonicalHash: %w, number=%d", err, number)
	}
	if len(data) == 0 {
		return common.Hash{}, nil
	}
	return common.BytesToHash(data), nil
}

// WriteCanonicalHash stores the hash assigned to a canonical block number.
func WriteCanonicalHash(db kv.Putter, hash common.Hash, number uint64) error {
	if err := db.Put(kv.HeaderCanonical, hexutility.EncodeTs(number), hash.Bytes()); err != nil {
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
	if err := tx.ForEach(kv.HeaderCanonical, hexutility.EncodeTs(blockFrom), func(blockNumBytes, blockHash []byte) error {
		if markChainAsBad {
			if err := tx.Delete(kv.HeaderNumber, blockHash); err != nil {
				return err
			}
			if err := tx.Put(kv.BadHeaderNumber, blockHash, blockNumBytes); err != nil {
				return err
			}
		}
		return tx.Delete(kv.HeaderCanonical, blockNumBytes)
	}); err != nil {
		return fmt.Errorf("TruncateCanonicalHash: %w", err)
	}
	return nil
}

// IsCanonicalHashDeprecated determines whether a header with the given hash is on the canonical chain.
func IsCanonicalHashDeprecated(db kv.Getter, hash common.Hash) (bool, *uint64, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return false, nil, nil
	}
	canonicalHash, err := ReadCanonicalHash(db, *number)
	if err != nil {
		return false, nil, err
	}
	return canonicalHash != (common.Hash{}) && canonicalHash == hash, number, nil
}

func IsCanonicalHash(db kv.Getter, hash common.Hash, number uint64) (bool, error) {
	canonicalHash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return false, err
	}
	return canonicalHash != (common.Hash{}) && canonicalHash == hash, nil
}

// ReadHeaderNumber returns the header number assigned to a hash.
func ReadHeaderNumber(db kv.Getter, hash common.Hash) *uint64 {
	data, err := db.GetOne(kv.HeaderNumber, hash.Bytes())
	if err != nil {
		log.Error("ReadHeaderNumber failed", "err", err)
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) != 8 {
		log.Error("ReadHeaderNumber got wrong data len", "len", len(data))
		return nil
	}
	number := binary.BigEndian.Uint64(data)
	return &number
}
func ReadBadHeaderNumber(db kv.Getter, hash common.Hash) (*uint64, error) {
	data, err := db.GetOne(kv.BadHeaderNumber, hash.Bytes())
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 8 {
		return nil, fmt.Errorf("ReadHeaderNumber got wrong data len: %d", len(data))
	}
	number := binary.BigEndian.Uint64(data)
	return &number, nil
}

// WriteHeaderNumber stores the hash->number mapping.
func WriteHeaderNumber(db kv.Putter, hash common.Hash, number uint64) error {
	if err := db.Put(kv.HeaderNumber, hash[:], hexutility.EncodeTs(number)); err != nil {
		return err
	}
	return nil
}

// ReadHeadHeaderHash retrieves the hash of the current canonical head header.
func ReadHeadHeaderHash(db kv.Getter) common.Hash {
	data, err := db.GetOne(kv.HeadHeaderKey, []byte(kv.HeadHeaderKey))
	if err != nil {
		log.Error("ReadHeadHeaderHash failed", "err", err)
	}
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadHeaderHash stores the hash of the current canonical head header.
func WriteHeadHeaderHash(db kv.Putter, hash common.Hash) error {
	if err := db.Put(kv.HeadHeaderKey, []byte(kv.HeadHeaderKey), hash.Bytes()); err != nil {
		return fmt.Errorf("failed to store last header's hash: %w", err)
	}
	return nil
}

// ReadHeadBlockHash retrieves the hash of the current canonical head block.
func ReadHeadBlockHash(db kv.Getter) common.Hash {
	data, err := db.GetOne(kv.HeadBlockKey, []byte(kv.HeadBlockKey))
	if err != nil {
		log.Error("ReadHeadBlockHash failed", "err", err)
	}
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadBlockHash stores the head block's hash.
func WriteHeadBlockHash(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.HeadBlockKey, []byte(kv.HeadBlockKey), hash.Bytes()); err != nil {
		log.Crit("Failed to store last block's hash", "err", err)
	}
}

// ReadForkchoiceHead retrieves headBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceHead(db kv.Getter) common.Hash {
	data, err := db.GetOne(kv.LastForkchoice, []byte("headBlockHash"))
	if err != nil {
		log.Error("ReadForkchoiceHead failed", "err", err)
	}
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteForkchoiceHead stores headBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceHead(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("headBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to store head block hash", "err", err)
	}
}

// ReadForkchoiceSafe retrieves safeBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceSafe(db kv.Getter) common.Hash {
	data, err := db.GetOne(kv.LastForkchoice, []byte("safeBlockHash"))
	if err != nil {
		log.Error("ReadForkchoiceSafe failed", "err", err)
		return common.Hash{}
	}

	if len(data) == 0 {
		return common.Hash{}
	}

	return common.BytesToHash(data)
}

// WriteForkchoiceSafe stores safeBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceSafe(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("safeBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to store safe block hash", "err", err)
	}
}

// ReadForkchoiceFinalized retrieves finalizedBlockHash from the last Engine API forkChoiceUpdated.
func ReadForkchoiceFinalized(db kv.Getter) common.Hash {
	data, err := db.GetOne(kv.LastForkchoice, []byte("finalizedBlockHash"))
	if err != nil {
		log.Error("ReadForkchoiceFinalize failed", "err", err)
		return common.Hash{}
	}

	if len(data) == 0 {
		return common.Hash{}
	}

	return common.BytesToHash(data)
}

// WriteForkchoiceFinalized stores finalizedBlockHash from the last Engine API forkChoiceUpdated.
func WriteForkchoiceFinalized(db kv.Putter, hash common.Hash) {
	if err := db.Put(kv.LastForkchoice, []byte("finalizedBlockHash"), hash[:]); err != nil {
		log.Crit("Failed to safe finalized block hash", "err", err)
	}
}

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db kv.Getter, hash common.Hash, number uint64) rlp.RawValue {
	data, err := db.GetOne(kv.Headers, dbutils.HeaderKey(number, hash))
	if err != nil {
		log.Error("ReadHeaderRLP failed", "err", err)
	}
	return data
}

// ReadHeader retrieves the block header corresponding to the hash.
func ReadHeader(db kv.Getter, hash common.Hash, number uint64) *types.Header {
	data := ReadHeaderRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil
	}
	return header
}

func ReadCurrentBlockNumber(db kv.Getter) *uint64 {
	headHash := ReadHeadHeaderHash(db)
	return ReadHeaderNumber(db, headHash)
}

func ReadCurrentHeader(db kv.Getter) *types.Header {
	headHash := ReadHeadHeaderHash(db)
	headNumber := ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil
	}
	return ReadHeader(db, headHash, *headNumber)
}

func ReadHeadersByNumber(db kv.Tx, number uint64) ([]*types.Header, error) {
	var res []*types.Header
	c, err := db.Cursor(kv.Headers)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	prefix := hexutility.EncodeTs(number)
	for k, v, err := c.Seek(prefix); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, prefix) {
			break
		}

		header := new(types.Header)
		if err := rlp.Decode(bytes.NewReader(v), header); err != nil {
			return nil, fmt.Errorf("invalid block header RLP: hash=%x, err=%w", k[8:], err)
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
		encoded   = hexutility.EncodeTs(number)
		headerKey = dbutils.HeaderKey(number, hash)
	)
	if err := db.Put(kv.HeaderNumber, hash[:], encoded); err != nil {
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
	if err := db.Put(kv.HeaderNumber, hash[:], hexutility.EncodeTs(number)); err != nil {
		return err
	}
	return nil
}

// DeleteHeader - dangerous, use PruneBlocks/TruncateBlocks methods
func DeleteHeader(db kv.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.Headers, dbutils.HeaderKey(number, hash)); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(kv.HeaderNumber, hash.Bytes()); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db kv.Tx, hash common.Hash, number uint64) rlp.RawValue {
	body, _ := ReadBodyWithTransactions(db, hash, number)
	bodyRlp, err := rlp.EncodeToBytes(body)
	if err != nil {
		log.Error("ReadBodyRLP failed", "err", err)
	}
	return bodyRlp
}
func ReadStorageBodyRLP(db kv.Getter, hash common.Hash, number uint64) rlp.RawValue {
	bodyRlp, err := db.GetOne(kv.BlockBody, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		log.Error("ReadBodyRLP failed", "err", err)
	}
	return bodyRlp
}

func ReadStorageBody(db kv.Getter, hash common.Hash, number uint64) (types.BodyForStorage, error) {
	bodyRlp, err := db.GetOne(kv.BlockBody, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		log.Error("ReadBodyRLP failed", "err", err)
	}
	bodyForStorage := new(types.BodyForStorage)
	if err := rlp.DecodeBytes(bodyRlp, bodyForStorage); err != nil {
		return types.BodyForStorage{}, err
	}
	return *bodyForStorage, nil
}

func TxnByIdxInBlock(db kv.Getter, blockHash common.Hash, blockNum uint64, txIdxInBlock int) (types.Transaction, error) {
	b, err := ReadBodyForStorageByKey(db, dbutils.BlockBodyKey(blockNum, blockHash))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	v, err := db.GetOne(kv.EthTx, hexutility.EncodeTs(b.BaseTxId+1+uint64(txIdxInBlock)))
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	txn, err := types.DecodeTransaction(v)
	if err != nil {
		return nil, err
	}
	return txn, nil
}

func CanonicalTransactions(db kv.Getter, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txs := make([]types.Transaction, amount)
	i := uint32(0)
	if err := db.ForAmount(kv.EthTx, hexutility.EncodeTs(baseTxId), amount, func(k, v []byte) error {
		var decodeErr error
		if txs[i], decodeErr = types.UnmarshalTransactionFromBinary(v); decodeErr != nil {
			return decodeErr
		}
		i++
		return nil
	}); err != nil {
		return nil, err
	}
	txs = txs[:i] // user may request big "amount", but db can return small "amount". Return as much as we found.
	return txs, nil
}

func NonCanonicalTransactions(db kv.Getter, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txs := make([]types.Transaction, amount)
	i := uint32(0)
	if err := db.ForAmount(kv.NonCanonicalTxs, hexutility.EncodeTs(baseTxId), amount, func(k, v []byte) error {
		var decodeErr error
		if txs[i], decodeErr = types.DecodeTransaction(v); decodeErr != nil {
			return decodeErr
		}
		i++
		return nil
	}); err != nil {
		return nil, err
	}
	txs = txs[:i] // user may request big "amount", but db can return small "amount". Return as much as we found.
	return txs, nil
}

func WriteTransactions(db kv.RwTx, txs []types.Transaction, baseTxId uint64) error {
	txId := baseTxId
	txIdKey := make([]byte, 8)
	buf := bytes.NewBuffer(nil)
	for _, tx := range txs {
		binary.BigEndian.PutUint64(txIdKey, txId)
		txId++

		buf.Reset()
		if err := rlp.Encode(buf, tx); err != nil {
			return fmt.Errorf("broken tx rlp: %w", err)
		}

		if err := db.Append(kv.EthTx, txIdKey, buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func WriteRawTransactions(tx kv.RwTx, txs [][]byte, baseTxId uint64) error {
	txId := baseTxId
	txIdKey := make([]byte, 8)
	for _, txn := range txs {
		binary.BigEndian.PutUint64(txIdKey, txId)
		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := tx.Append(kv.EthTx, txIdKey, txn); err != nil {
			return fmt.Errorf("txId=%d, baseTxId=%d, %w", txId, baseTxId, err)
		}
		txId++
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

// ReadBodyByNumber - returns canonical block body
func ReadBodyByNumber(db kv.Tx, number uint64) (*types.Body, uint64, uint32, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, 0, 0, nil
	}
	body, baseTxId, txAmount := ReadBody(db, hash, number)
	return body, baseTxId, txAmount, nil
}

func ReadBodyWithTransactions(db kv.Getter, hash common.Hash, number uint64) (*types.Body, error) {
	body, baseTxId, txAmount := ReadBody(db, hash, number)
	if body == nil {
		return nil, nil
	}
	var err error
	body.Transactions, err = CanonicalTransactions(db, baseTxId, txAmount)
	if err != nil {
		return nil, err
	}
	return body, err
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
		baseTxId, txAmount, err := types.DecodeOnlyTxMetadataFromBody(bodyRlp)
		if err != nil {
			return nil, err
		}

		binary.BigEndian.PutUint64(encNum, baseTxId)
		if err = db.ForAmount(kv.EthTx, encNum, txAmount, func(k, v []byte) error {
			res = append(res, v)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return
}

// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
func ResetSequence(tx kv.RwTx, bucket string, newValue uint64) error {
	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, newValue)
	if err := tx.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return err
	}
	return nil
}

func ReadBodyForStorageByKey(db kv.Getter, k []byte) (*types.BodyForStorage, error) {
	bodyRlp, err := db.GetOne(kv.BlockBody, k)
	if err != nil {
		return nil, err
	}
	if len(bodyRlp) == 0 {
		return nil, nil
	}
	bodyForStorage := new(types.BodyForStorage)
	if err := rlp.DecodeBytes(bodyRlp, bodyForStorage); err != nil {
		return nil, err
	}

	return bodyForStorage, nil
}

func ReadBody(db kv.Getter, hash common.Hash, number uint64) (*types.Body, uint64, uint32) {
	data := ReadStorageBodyRLP(db, hash, number)
	if len(data) == 0 {
		return nil, 0, 0
	}
	bodyForStorage := new(types.BodyForStorage)
	err := rlp.DecodeBytes(data, bodyForStorage)
	if err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil, 0, 0
	}
	body := new(types.Body)
	body.Uncles = bodyForStorage.Uncles
	body.Withdrawals = bodyForStorage.Withdrawals

	if bodyForStorage.TxAmount < 2 {
		panic(fmt.Sprintf("block body hash too few txs amount: %d, %d", number, bodyForStorage.TxAmount))
	}
	return body, bodyForStorage.BaseTxId + 1, bodyForStorage.TxAmount - 2 // 1 system txn in the begining of block, and 1 at the end
}

func HasSenders(db kv.Getter, hash common.Hash, number uint64) (bool, error) {
	return db.Has(kv.Senders, dbutils.BlockBodyKey(number, hash))
}

func ReadSenders(db kv.Getter, hash common.Hash, number uint64) ([]common.Address, error) {
	data, err := db.GetOne(kv.Senders, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, fmt.Errorf("readSenders failed: %w", err)
	}
	senders := make([]common.Address, len(data)/length.Addr)
	for i := 0; i < len(senders); i++ {
		copy(senders[i][:], data[i*length.Addr:])
	}
	return senders, nil
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
	baseTxnID, err := db.IncrementSequence(kv.EthTx, uint64(len(body.Transactions))+2)
	if err != nil {
		return false, err
	}
	data := types.BodyForStorage{
		BaseTxId:    baseTxnID,
		TxAmount:    uint32(len(body.Transactions)) + 2, /*system txs*/
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}
	if err = WriteBodyForStorage(db, hash, number, &data); err != nil {
		return false, fmt.Errorf("WriteBodyForStorage: %w", err)
	}
	firstNonSystemTxnID := baseTxnID + 1
	if err = WriteRawTransactions(db, body.Transactions, firstNonSystemTxnID); err != nil {
		return false, fmt.Errorf("WriteRawTransactions: %w", err)
	}
	return true, nil
}

func WriteBody(db kv.RwTx, hash common.Hash, number uint64, body *types.Body) (err error) {
	// Pre-processing
	body.SendersFromTxs()
	baseTxId, err := db.IncrementSequence(kv.EthTx, uint64(len(body.Transactions))+2)
	if err != nil {
		return err
	}
	data := types.BodyForStorage{
		BaseTxId:    baseTxId,
		TxAmount:    uint32(len(body.Transactions)) + 2,
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}
	if err = WriteBodyForStorage(db, hash, number, &data); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}
	if err = WriteTransactions(db, body.Transactions, baseTxId+1); err != nil {
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
func DeleteBody(db kv.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.BlockBody, dbutils.BlockBodyKey(number, hash)); err != nil {
		log.Crit("Failed to delete block body", "err", err)
	}
}

func AppendCanonicalTxNums(tx kv.RwTx, from uint64) (err error) {
	nextBaseTxNum := -1
	if from > 0 {
		nextBaseTxNumFromDb, err := rawdbv3.TxNums.Max(tx, from-1)
		if err != nil {
			return err
		}
		nextBaseTxNum = int(nextBaseTxNumFromDb)
		nextBaseTxNum++
	}
	for blockNum := from; ; blockNum++ {
		h, err := ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		if h == (common.Hash{}) {
			break
		}

		data := ReadStorageBodyRLP(tx, h, blockNum)
		if len(data) == 0 {
			break
		}
		bodyForStorage := types.BodyForStorage{}
		if err := rlp.DecodeBytes(data, &bodyForStorage); err != nil {
			return err
		}

		nextBaseTxNum += int(bodyForStorage.TxAmount)
		err = rawdbv3.TxNums.Append(tx, blockNum, uint64(nextBaseTxNum-1))
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadTd retrieves a block's total difficulty corresponding to the hash.
func ReadTd(db kv.Getter, hash common.Hash, number uint64) (*big.Int, error) {
	data, err := db.GetOne(kv.HeaderTD, dbutils.HeaderKey(number, hash))
	if err != nil {
		return nil, fmt.Errorf("failed ReadTd: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		return nil, fmt.Errorf("invalid block total difficulty RLP: %x, %w", hash, err)
	}
	return td, nil
}

func ReadTdByHash(db kv.Getter, hash common.Hash) (*big.Int, error) {
	headNumber := ReadHeaderNumber(db, hash)
	if headNumber == nil {
		return nil, nil
	}
	return ReadTd(db, hash, *headNumber)
}

// WriteTd stores the total difficulty of a block into the database.
func WriteTd(db kv.Putter, hash common.Hash, number uint64, td *big.Int) error {
	data, err := rlp.EncodeToBytes(td)
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
	if err := tx.ForEach(kv.HeaderTD, hexutility.EncodeTs(blockFrom), func(k, _ []byte) error {
		return tx.Delete(kv.HeaderTD, k)
	}); err != nil {
		return fmt.Errorf("TruncateTd: %w", err)
	}
	return nil
}

// ReadRawReceipts retrieves all the transaction receipts belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadReceipts instead if the metadata is needed.
func ReadRawReceipts(db kv.Tx, blockNum uint64) types.Receipts {
	// Retrieve the flattened receipt slice
	data, err := db.GetOne(kv.Receipts, hexutility.EncodeTs(blockNum))
	if err != nil {
		log.Error("ReadRawReceipts failed", "err", err)
	}
	if len(data) == 0 {
		return nil
	}
	var receipts types.Receipts
	if err := cbor.Unmarshal(&receipts, bytes.NewReader(data)); err != nil {
		log.Error("receipt unmarshal failed", "err", err)
		return nil
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, blockNum)

	it, err := db.Prefix(kv.Log, prefix)
	if err != nil {
		log.Error("logs fetching failed", "err", err)
		return nil
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			log.Error("logs fetching failed", "err", err)
			return nil
		}
		var logs types.Logs
		if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
			err = fmt.Errorf("receipt unmarshal failed:  %w", err)
			log.Error("logs fetching failed", "err", err)
			return nil
		}

		txIndex := int(binary.BigEndian.Uint32(k[8:]))

		// only return logs from real txs (not from block's stateSyncReceipt)
		if txIndex < len(receipts) {
			receipts[txIndex].Logs = logs
		}
	}

	return receipts
}

// ReadReceipts retrieves all the transaction receipts belonging to a block, including
// its corresponding metadata fields. If it is unable to populate these metadata
// fields then nil is returned.
//
// The current implementation populates these metadata fields by reading the receipts'
// corresponding block body, so if the block body is not found it will return nil even
// if the receipt itself is stored.
func ReadReceipts(db kv.Tx, block *types.Block, senders []common.Address) types.Receipts {
	if block == nil {
		return nil
	}
	// We're deriving many fields from the block body, retrieve beside the receipt
	receipts := ReadRawReceipts(db, block.NumberU64())
	if receipts == nil {
		return nil
	}
	if len(senders) > 0 {
		block.SendersToTxs(senders)
	} else {
		senders = block.Body().SendersFromTxs()
	}
	if err := receipts.DeriveFields(block.Hash(), block.NumberU64(), block.Transactions(), senders); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", block.Hash(), "number", block.NumberU64(), "err", err, "stack", dbg.Stack())
		return nil
	}
	return receipts
}

// WriteReceipts stores all the transaction receipts belonging to a block.
func WriteReceipts(tx kv.Putter, number uint64, receipts types.Receipts) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	for txId, r := range receipts {
		if len(r.Logs) == 0 {
			continue
		}

		buf.Reset()
		err := cbor.Marshal(buf, r.Logs)
		if err != nil {
			return fmt.Errorf("encode block logs for block %d: %w", number, err)
		}

		if err = tx.Put(kv.Log, dbutils.LogKey(number, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing logs for block %d: %w", number, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %w", number, err)
	}

	if err = tx.Put(kv.Receipts, hexutility.EncodeTs(number), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %w", number, err)
	}
	return nil
}

// AppendReceipts stores all the transaction receipts belonging to a block.
func AppendReceipts(tx kv.StatelessWriteTx, blockNumber uint64, receipts types.Receipts) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	for txId, r := range receipts {
		if len(r.Logs) == 0 {
			continue
		}

		buf.Reset()
		err := cbor.Marshal(buf, r.Logs)
		if err != nil {
			return fmt.Errorf("encode block receipts for block %d: %w", blockNumber, err)
		}

		if err = tx.Append(kv.Log, dbutils.LogKey(blockNumber, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing receipts for block %d: %w", blockNumber, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %w", blockNumber, err)
	}

	if err = tx.Append(kv.Receipts, hexutility.EncodeTs(blockNumber), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %w", blockNumber, err)
	}
	return nil
}

// TruncateReceipts removes all receipt for given block number or newer
func TruncateReceipts(db kv.RwTx, number uint64) error {
	if err := db.ForEach(kv.Receipts, hexutility.EncodeTs(number), func(k, _ []byte) error {
		return db.Delete(kv.Receipts, k)
	}); err != nil {
		return err
	}

	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, number)
	if err := db.ForEach(kv.Log, from, func(k, _ []byte) error {
		return db.Delete(kv.Log, k)
	}); err != nil {
		return err
	}
	return nil
}

func ReceiptsAvailableFrom(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(kv.Receipts)
	if err != nil {
		return math.MaxUint64, err
	}
	defer c.Close()
	k, _, err := c.First()
	if err != nil {
		return math.MaxUint64, err
	}
	if len(k) == 0 {
		return math.MaxUint64, nil
	}
	return binary.BigEndian.Uint64(k), nil
}

// ReadBlock retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body. If either the header or body could not
// be retrieved nil is returned.
//
// Note, due to concurrent download of header and block body the header and thus
// canonical hash can be stored in the database but the body data not (yet).
func ReadBlock(tx kv.Getter, hash common.Hash, number uint64) *types.Block {
	header := ReadHeader(tx, hash, number)
	if header == nil {
		return nil
	}
	body, _ := ReadBodyWithTransactions(tx, hash, number)
	if body == nil {
		return nil
	}
	return types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles, body.Withdrawals)
}

// HasBlock - is more efficient than ReadBlock because doesn't read transactions.
// It's is not equivalent of HasHeader because headers and bodies written by different stages
func HasBlock(db kv.Getter, hash common.Hash, number uint64) bool {
	body := ReadStorageBodyRLP(db, hash, number)
	return len(body) > 0
}

func ReadBlockWithSenders(db kv.Getter, hash common.Hash, number uint64) (*types.Block, []common.Address, error) {
	block := ReadBlock(db, hash, number)
	if block == nil {
		return nil, nil, nil
	}
	senders, err := ReadSenders(db, hash, number)
	if err != nil {
		return nil, nil, err
	}
	if len(senders) != block.Transactions().Len() {
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
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
// doesn't change sequences of kv.EthTx and kv.NonCanonicalTxs
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func PruneBlocks(tx kv.RwTx, blockTo uint64, blocksDeleteLimit int) error {
	c, err := tx.Cursor(kv.Headers)
	if err != nil {
		return err
	}
	defer c.Close()

	// find first non-genesis block
	firstK, _, err := c.Seek(hexutility.EncodeTs(1))
	if err != nil {
		return err
	}
	if firstK == nil { //nothing to delete
		return err
	}
	blockFrom := binary.BigEndian.Uint64(firstK)
	stopAtBlock := cmp.Min(blockTo, blockFrom+uint64(blocksDeleteLimit))

	var b *types.BodyForStorage

	for k, _, err := c.Current(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		n := binary.BigEndian.Uint64(k)
		if n >= stopAtBlock { // [from, to)
			break
		}

		b, err = ReadBodyForStorageByKey(tx, k)
		if err != nil {
			return err
		}
		if b == nil {
			log.Debug("PruneBlocks: block body not found", "height", n)
		} else {
			txIDBytes := make([]byte, 8)
			for txID := b.BaseTxId; txID < b.BaseTxId+uint64(b.TxAmount); txID++ {
				binary.BigEndian.PutUint64(txIDBytes, txID)
				if err = tx.Delete(kv.EthTx, txIDBytes); err != nil {
					return err
				}
			}
		}
		// Copying k because otherwise the same memory will be reused
		// for the next key and Delete below will end up deleting 1 more record than required
		kCopy := common.Copy(k)
		if err = tx.Delete(kv.Senders, kCopy); err != nil {
			return err
		}
		if err = tx.Delete(kv.BlockBody, kCopy); err != nil {
			return err
		}
		if err = tx.Delete(kv.Headers, kCopy); err != nil {
			return err
		}
	}

	return nil
}

// PruneBorBlocks - delete [1, to) old blocks after moving it to snapshots.
// keeps genesis in db: [1, to)
// doesn't change sequences of kv.EthTx and kv.NonCanonicalTxs
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func PruneBorBlocks(tx kv.RwTx, blockTo uint64, blocksDeleteLimit int) error {
	c, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer c.Close()
	var blockNumBytes [8]byte
	binary.BigEndian.PutUint64(blockNumBytes[:], blockTo)
	k, v, err := c.Seek(blockNumBytes[:])
	if err != nil {
		return err
	}
	var eventIdTo uint64 = math.MaxUint64
	if k != nil {
		eventIdTo = binary.BigEndian.Uint64(v)
	}
	c1, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return err
	}
	defer c1.Close()
	counter := blocksDeleteLimit
	for k, _, err = c1.First(); err == nil && k != nil && counter > 0; k, _, err = c1.Next() {
		eventId := binary.BigEndian.Uint64(k)
		if eventId >= eventIdTo {
			break
		}
		if err = c1.DeleteCurrent(); err != nil {
			return err
		}
		counter--
	}
	if err != nil {
		return err
	}
	var firstSpanToKeep uint64
	if blockTo > zerothSpanEnd {
		firstSpanToKeep = 1 + (blockTo-zerothSpanEnd-1)/spanLength
	}
	c2, err := tx.RwCursor(kv.BorSpans)
	if err != nil {
		return err
	}
	defer c2.Close()
	counter = blocksDeleteLimit
	for k, _, err := c2.First(); err == nil && k != nil && counter > 0; k, _, err = c2.Next() {
		spanId := binary.BigEndian.Uint64(k)
		if spanId >= firstSpanToKeep {
			break
		}
		if err = c2.DeleteCurrent(); err != nil {
			return err
		}
		counter--
	}
	return nil
}

func TruncateCanonicalChain(ctx context.Context, db kv.RwTx, from uint64) error {
	return db.ForEach(kv.HeaderCanonical, hexutility.EncodeTs(from), func(k, _ []byte) error {
		return db.Delete(kv.HeaderCanonical, k)
	})
}

// TruncateBlocks - delete block >= blockFrom
// does decrement sequences of kv.EthTx and kv.NonCanonicalTxs
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func TruncateBlocks(ctx context.Context, tx kv.RwTx, blockFrom uint64) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	if blockFrom < 1 { //protect genesis
		blockFrom = 1
	}
	return tx.ForEach(kv.Headers, hexutility.EncodeTs(blockFrom), func(k, v []byte) error {
		b, err := ReadBodyForStorageByKey(tx, k)
		if err != nil {
			return err
		}
		if b != nil {
			txIDBytes := make([]byte, 8)
			for txID := b.BaseTxId; txID < b.BaseTxId+uint64(b.TxAmount); txID++ {
				binary.BigEndian.PutUint64(txIDBytes, txID)
				if err = tx.Delete(kv.EthTx, txIDBytes); err != nil {
					return err
				}
			}
		}
		// Copying k because otherwise the same memory will be reused
		// for the next key and Delete below will end up deleting 1 more record than required
		kCopy := common.Copy(k)
		if err := tx.Delete(kv.Senders, kCopy); err != nil {
			return err
		}
		if err := tx.Delete(kv.BlockBody, kCopy); err != nil {
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
func ReadTotalIssued(db kv.Getter, number uint64) (*big.Int, error) {
	data, err := db.GetOne(kv.Issuance, hexutility.EncodeTs(number))
	if err != nil {
		return nil, err
	}

	return new(big.Int).SetBytes(data), nil
}

func WriteTotalIssued(db kv.Putter, number uint64, totalIssued *big.Int) error {
	return db.Put(kv.Issuance, hexutility.EncodeTs(number), totalIssued.Bytes())
}

func ReadTotalBurnt(db kv.Getter, number uint64) (*big.Int, error) {
	data, err := db.GetOne(kv.Issuance, append([]byte("burnt"), hexutility.EncodeTs(number)...))
	if err != nil {
		return nil, err
	}

	return new(big.Int).SetBytes(data), nil
}

func WriteTotalBurnt(db kv.Putter, number uint64, totalBurnt *big.Int) error {
	return db.Put(kv.Issuance, append([]byte("burnt"), hexutility.EncodeTs(number)...), totalBurnt.Bytes())
}

func ReadHeaderByNumber(db kv.Getter, number uint64) *types.Header {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		log.Error("ReadCanonicalHash failed", "err", err)
		return nil
	}
	if hash == (common.Hash{}) {
		return nil
	}

	return ReadHeader(db, hash, number)
}

func ReadHeaderByHash(db kv.Getter, hash common.Hash) (*types.Header, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	return ReadHeader(db, hash, *number), nil
}

func DeleteNewerEpochs(tx kv.RwTx, number uint64) error {
	if err := tx.ForEach(kv.PendingEpoch, hexutility.EncodeTs(number), func(k, v []byte) error {
		return tx.Delete(kv.Epoch, k)
	}); err != nil {
		return err
	}
	return tx.ForEach(kv.Epoch, hexutility.EncodeTs(number), func(k, v []byte) error {
		return tx.Delete(kv.Epoch, k)
	})
}
func ReadEpoch(tx kv.Tx, blockNum uint64, blockHash common.Hash) (transitionProof []byte, err error) {
	k := make([]byte, dbutils.NumberLength+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[dbutils.NumberLength:], blockHash[:])
	return tx.GetOne(kv.Epoch, k)
}
func FindEpochBeforeOrEqualNumber(tx kv.Tx, n uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, err error) {
	c, err := tx.Cursor(kv.Epoch)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	defer c.Close()
	seek := hexutility.EncodeTs(n)
	k, v, err := c.Seek(seek)
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	if k != nil {
		num := binary.BigEndian.Uint64(k)
		if num == n {
			return n, common.BytesToHash(k[dbutils.NumberLength:]), v, nil
		}
	}
	k, v, err = c.Prev()
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	if k == nil {
		return 0, common.Hash{}, nil, nil
	}
	return binary.BigEndian.Uint64(k), common.BytesToHash(k[dbutils.NumberLength:]), v, nil
}

func WriteEpoch(tx kv.RwTx, blockNum uint64, blockHash common.Hash, transitionProof []byte) (err error) {
	k := make([]byte, dbutils.NumberLength+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[dbutils.NumberLength:], blockHash[:])
	return tx.Put(kv.Epoch, k, transitionProof)
}

func ReadPendingEpoch(tx kv.Tx, blockNum uint64, blockHash common.Hash) (transitionProof []byte, err error) {
	k := make([]byte, 8+32)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[8:], blockHash[:])
	return tx.GetOne(kv.PendingEpoch, k)
}

func WritePendingEpoch(tx kv.RwTx, blockNum uint64, blockHash common.Hash, transitionProof []byte) (err error) {
	k := make([]byte, 8+32)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[8:], blockHash[:])
	return tx.Put(kv.PendingEpoch, k, transitionProof)
}

// Transitioned returns true if the block number comes after POS transition or is the last POW block
func Transitioned(db kv.Getter, blockNum uint64, terminalTotalDifficulty *big.Int) (trans bool, err error) {
	if terminalTotalDifficulty == nil {
		return false, nil
	}

	if terminalTotalDifficulty.Cmp(common.Big0) == 0 {
		return true, nil
	}
	header := ReadHeaderByNumber(db, blockNum)
	if header == nil {
		return false, nil
	}

	if header.Difficulty.Cmp(common.Big0) == 0 {
		return true, nil
	}

	headerTd, err := ReadTd(db, header.Hash(), blockNum)
	if err != nil {
		return false, err
	}

	return headerTd.Cmp(terminalTotalDifficulty) >= 0, nil
}

// IsPosBlock returns true if the block number comes after POS transition or is the last POW block
func IsPosBlock(db kv.Getter, blockHash common.Hash) (trans bool, err error) {
	header, err := ReadHeaderByHash(db, blockHash)
	if err != nil {
		return false, err
	}
	if header == nil {
		return false, nil
	}

	return header.Difficulty.Cmp(common.Big0) == 0, nil
}

var SnapshotsKey = []byte("snapshots")
var SnapshotsHistoryKey = []byte("snapshots_history")

func ReadSnapshots(tx kv.Tx) ([]string, []string, error) {
	v, err := tx.GetOne(kv.DatabaseInfo, SnapshotsKey)
	if err != nil {
		return nil, nil, err
	}
	var res, resHist []string
	_ = json.Unmarshal(v, &res)

	v, err = tx.GetOne(kv.DatabaseInfo, SnapshotsHistoryKey)
	if err != nil {
		return nil, nil, err
	}
	_ = json.Unmarshal(v, &resHist)
	return res, resHist, nil
}

func WriteSnapshots(tx kv.RwTx, list, histList []string) error {
	res, err := json.Marshal(list)
	if err != nil {
		return err
	}
	if err := tx.Put(kv.DatabaseInfo, SnapshotsKey, res); err != nil {
		return err
	}
	res, err = json.Marshal(histList)
	if err != nil {
		return err
	}
	if err := tx.Put(kv.DatabaseInfo, SnapshotsHistoryKey, res); err != nil {
		return err
	}
	return nil
}

// PruneTable has `limit` parameter to avoid too large data deletes per one sync cycle - better delete by small portions to reduce db.FreeList size
func PruneTable(tx kv.RwTx, table string, pruneTo uint64, ctx context.Context, limit int) error {
	c, err := tx.RwCursor(table)

	if err != nil {
		return fmt.Errorf("failed to create cursor for pruning %w", err)
	}
	defer c.Close()

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
		select {
		case <-ctx.Done():
			return common.ErrStopped
		default:
		}
		if err = c.DeleteCurrent(); err != nil {
			return fmt.Errorf("failed to remove for block %d: %w", blockNum, err)
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

func ReadVerkleRoot(tx kv.Tx, blockNum uint64) (common.Hash, error) {
	root, err := tx.GetOne(kv.VerkleRoots, hexutility.EncodeTs(blockNum))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(root), nil
}

func WriteVerkleRoot(tx kv.RwTx, blockNum uint64, root common.Hash) error {
	return tx.Put(kv.VerkleRoots, hexutility.EncodeTs(blockNum), root[:])
}

func WriteVerkleNode(tx kv.RwTx, node verkle.VerkleNode) error {
	var (
		root    common.Hash
		encoded []byte
		err     error
	)
	root = node.Commitment().Bytes()
	encoded, err = node.Serialize()
	if err != nil {
		return err
	}

	return tx.Put(kv.VerkleTrie, root[:], encoded)
}

func ReadVerkleNode(tx kv.RwTx, root common.Hash) (verkle.VerkleNode, error) {
	encoded, err := tx.GetOne(kv.VerkleTrie, root[:])
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return verkle.New(), nil
	}
	return verkle.ParseNode(encoded, 0, root[:])
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
	if len(existingVersion) == 0 {
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
