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
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/rlp"
)

// ReadCanonicalHash retrieves the hash assigned to a canonical block number.
func ReadCanonicalHash(db ethdb.KVGetter, number uint64) (common.Hash, error) {
	data, err := db.GetOne(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(number))
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed ReadCanonicalHash: %w, number=%d", err, number)
	}
	if len(data) == 0 {
		return common.Hash{}, nil
	}
	return common.BytesToHash(data), nil
}

// WriteCanonicalHash stores the hash assigned to a canonical block number.
func WriteCanonicalHash(db ethdb.Putter, hash common.Hash, number uint64) error {
	if err := db.Put(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(number), hash.Bytes()); err != nil {
		return fmt.Errorf("failed to store number to hash mapping: %w", err)
	}
	return nil
}

// DeleteCanonicalHash removes the number to hash canonical mapping.
func DeleteCanonicalHash(db ethdb.Deleter, number uint64) error {
	if err := db.Delete(dbutils.HeaderCanonicalBucket, dbutils.EncodeBlockNumber(number), nil); err != nil {
		return fmt.Errorf("failed to delete number to hash mapping: %w", err)
	}
	return nil
}

// ReadHeaderNumber returns the header number assigned to a hash.
func ReadHeaderNumber(db ethdb.KVGetter, hash common.Hash) *uint64 {
	data, err := db.GetOne(dbutils.HeaderNumberBucket, hash.Bytes())
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

// WriteHeaderNumber stores the hash->number mapping.
func WriteHeaderNumber(db ethdb.Putter, hash common.Hash, number uint64) {
	enc := dbutils.EncodeBlockNumber(number)
	if err := db.Put(dbutils.HeaderNumberBucket, hash[:], enc); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
}

// DeleteHeaderNumber removes hash->number mapping.
func DeleteHeaderNumber(db ethdb.Deleter, hash common.Hash) {
	if err := db.Delete(dbutils.HeaderNumberBucket, hash[:], nil); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadHeadHeaderHash retrieves the hash of the current canonical head header.
func ReadHeadHeaderHash(db ethdb.KVGetter) common.Hash {
	data, err := db.GetOne(dbutils.HeadHeaderKey, []byte(dbutils.HeadHeaderKey))
	if err != nil {
		log.Error("ReadHeadHeaderHash failed", "err", err)
	}
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadHeaderHash stores the hash of the current canonical head header.
func WriteHeadHeaderHash(db ethdb.Putter, hash common.Hash) error {
	if err := db.Put(dbutils.HeadHeaderKey, []byte(dbutils.HeadHeaderKey), hash.Bytes()); err != nil {
		return fmt.Errorf("failed to store last header's hash: %w", err)
	}
	return nil
}

// ReadHeadBlockHash retrieves the hash of the current canonical head block.
func ReadHeadBlockHash(db ethdb.KVGetter) common.Hash {
	data, err := db.GetOne(dbutils.HeadBlockKey, []byte(dbutils.HeadBlockKey))
	if err != nil {
		log.Error("ReadHeadBlockHash failed", "err", err)
	}
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadBlockHash stores the head block's hash.
func WriteHeadBlockHash(db ethdb.Putter, hash common.Hash) {
	if err := db.Put(dbutils.HeadBlockKey, []byte(dbutils.HeadBlockKey), hash.Bytes()); err != nil {
		log.Crit("Failed to store last block's hash", "err", err)
	}
}

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db ethdb.KVGetter, hash common.Hash, number uint64) rlp.RawValue {
	data, err := db.GetOne(dbutils.HeadersBucket, dbutils.HeaderKey(number, hash))
	if err != nil {
		log.Error("ReadHeaderRLP failed", "err", err)
	}
	return data
}

// HasHeader verifies the existence of a block header corresponding to the hash.
func HasHeader(db ethdb.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.HeadersBucket, dbutils.HeaderKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadHeader retrieves the block header corresponding to the hash.
func ReadHeader(db ethdb.KVGetter, hash common.Hash, number uint64) *types.Header {
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

func ReadCurrentBlockNumber(db ethdb.KVGetter) *uint64 {
	headHash := ReadHeadHeaderHash(db)
	return ReadHeaderNumber(db, headHash)
}

func ReadCurrentHeader(db ethdb.KVGetter) *types.Header {
	headHash := ReadHeadHeaderHash(db)
	headNumber := ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil
	}
	return ReadHeader(db, headHash, *headNumber)
}

// Deprecated, use ReadCurrentBlock
func ReadCurrentBlockDeprecated(db ethdb.Getter) *types.Block {
	headHash := ReadHeadBlockHash(db)
	headNumber := ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil
	}
	return ReadBlockDeprecated(db, headHash, *headNumber)
}

func ReadCurrentBlock(db ethdb.Tx) *types.Block {
	headHash := ReadHeadBlockHash(db)
	headNumber := ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil
	}
	return ReadBlock(db, headHash, *headNumber)
}

func ReadHeadersByNumber(db ethdb.Tx, number uint64) ([]*types.Header, error) {
	var res []*types.Header
	c, err := db.Cursor(dbutils.HeadersBucket)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	prefix := dbutils.EncodeBlockNumber(number)
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
func WriteHeader(db ethdb.Putter, header *types.Header) {
	var (
		hash    = header.Hash()
		number  = header.Number.Uint64()
		encoded = dbutils.EncodeBlockNumber(number)
	)
	if err := db.Put(dbutils.HeaderNumberBucket, hash[:], encoded); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		log.Crit("Failed to RLP encode header", "err", err)
	}
	if err := db.Put(dbutils.HeadersBucket, dbutils.HeaderKey(number, hash), data); err != nil {
		log.Crit("Failed to store header", "err", err)
	}
}

// DeleteHeader removes all block header data associated with a hash.
func DeleteHeader(db ethdb.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.HeadersBucket, dbutils.HeaderKey(number, hash), nil); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(dbutils.HeaderNumberBucket, hash.Bytes(), nil); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db ethdb.Tx, hash common.Hash, number uint64) rlp.RawValue {
	body := ReadBody(db, hash, number)
	bodyRlp, err := rlp.EncodeToBytes(body)
	if err != nil {
		log.Error("ReadBodyRLP failed", "err", err)
	}
	return bodyRlp
}

func ReadStorageBodyRLP(db ethdb.KVGetter, hash common.Hash, number uint64) rlp.RawValue {
	bodyRlp, err := db.GetOne(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		log.Error("ReadBodyRLP failed", "err", err)
	}
	return bodyRlp
}

// Deprecated, use ReadTransactions
func ReadTransactionsDeprecated(db ethdb.Getter, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txIdKey := make([]byte, 8)
	reader := bytes.NewReader(nil)
	stream := rlp.NewStream(reader, 0)
	txs := make([]types.Transaction, amount)
	binary.BigEndian.PutUint64(txIdKey, baseTxId)
	i := uint32(0)
	if err := db.Walk(dbutils.EthTx, txIdKey, 0, func(k, txData []byte) (bool, error) {
		var decodeErr error
		reader.Reset(txData)
		stream.Reset(reader, 0)
		if txs[i], decodeErr = types.DecodeTransaction(stream); decodeErr != nil {
			return false, decodeErr
		}
		i++
		return i < amount, nil
	}); err != nil {
		return nil, err
	}

	txs = txs[:i] // user may request big "amount", but db can return small "amount". Return as much as we found.
	return txs, nil
}

func ReadTransactions(db ethdb.Tx, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txIdKey := make([]byte, 8)
	reader := bytes.NewReader(nil)
	stream := rlp.NewStream(reader, 0)
	txs := make([]types.Transaction, amount)
	binary.BigEndian.PutUint64(txIdKey, baseTxId)
	i := uint32(0)

	if err := db.ForAmount(dbutils.EthTx, txIdKey, amount, func(k, v []byte) error {
		var decodeErr error
		reader.Reset(v)
		stream.Reset(reader, 0)
		if txs[i], decodeErr = types.DecodeTransaction(stream); decodeErr != nil {
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

func WriteTransactionsDeprecated(db ethdb.Database, txs []types.Transaction, baseTxId uint64) error {
	txId := baseTxId
	buf := bytes.NewBuffer(nil)
	for _, tx := range txs {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, txId)
		txId++

		buf.Reset()
		if err := rlp.Encode(buf, tx); err != nil {
			return fmt.Errorf("broken tx rlp: %w", err)
		}

		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := db.Append(dbutils.EthTx, txIdKey, common.CopyBytes(buf.Bytes())); err != nil {
			return err
		}
	}
	return nil
}

func WriteTransactions(db ethdb.RwTx, txs []types.Transaction, baseTxId uint64) error {
	txId := baseTxId
	buf := bytes.NewBuffer(nil)
	c, err := db.RwCursor(dbutils.EthTx)
	if err != nil {
		return err
	}
	defer c.Close()

	for _, tx := range txs {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, txId)
		txId++

		buf.Reset()
		if err := rlp.Encode(buf, tx); err != nil {
			return fmt.Errorf("broken tx rlp: %w", err)
		}

		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := c.Append(txIdKey, common.CopyBytes(buf.Bytes())); err != nil {
			return err
		}
	}
	return nil
}

func WriteRawTransactions(db ethdb.RwTx, txs [][]byte, baseTxId uint64) error {
	txId := baseTxId
	c, err := db.RwCursor(dbutils.EthTx)
	if err != nil {
		return err
	}
	defer c.Close()

	for _, tx := range txs {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, txId)
		txId++
		// If next Append returns KeyExists error - it means you need to open transaction in App code before calling this func. Batch is also fine.
		if err := c.Append(txIdKey, tx); err != nil {
			return err
		}
	}
	return nil
}

// WriteBodyRLP stores an RLP encoded block body into the database.
func WriteBodyRLP(db ethdb.Putter, hash common.Hash, number uint64, rlp rlp.RawValue) {
	if err := db.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash), rlp); err != nil {
		log.Crit("Failed to store block body", "err", err)
	}
}

// HasBody verifies the existence of a block body corresponding to the hash.
func HasBody(db ethdb.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadBodyDeprecated retrieves the block body corresponding to the hash.
// Deprecated, use ReadBody
func ReadBodyDeprecated(db ethdb.Getter, hash common.Hash, number uint64) *types.Body {
	body, baseTxId, txAmount := ReadBodyWithoutTransactions(db, hash, number)
	if body == nil {
		return nil
	}
	var err error
	body.Transactions, err = ReadTransactionsDeprecated(db, baseTxId, txAmount)
	if err != nil {
		log.Error("failed ReadTransaction", "hash", hash, "block", number, "err", err)
		return nil
	}
	return body
}

func ReadBodyByNumber(db ethdb.Tx, number uint64) (*types.Body, uint64, uint32, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, 0, 0, nil
	}
	body, baseTxId, txAmount := ReadBodyWithoutTransactions(db, hash, number)
	return body, baseTxId, txAmount, nil
}

func ReadBody(db ethdb.Tx, hash common.Hash, number uint64) *types.Body {
	body, baseTxId, txAmount := ReadBodyWithoutTransactions(db, hash, number)
	if body == nil {
		return nil
	}
	var err error
	body.Transactions, err = ReadTransactions(db, baseTxId, txAmount)
	if err != nil {
		log.Error("failed ReadTransaction", "hash", hash, "block", number, "err", err)
		return nil
	}
	return body
}

func ReadBodyWithoutTransactions(db ethdb.KVGetter, hash common.Hash, number uint64) (*types.Body, uint64, uint32) {
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
	return body, bodyForStorage.BaseTxId, bodyForStorage.TxAmount
}

func ReadSenders(db ethdb.KVGetter, hash common.Hash, number uint64) ([]common.Address, error) {
	data, err := db.GetOne(dbutils.Senders, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, fmt.Errorf("readSenders failed: %w", err)
	}
	senders := make([]common.Address, len(data)/common.AddressLength)
	for i := 0; i < len(senders); i++ {
		copy(senders[i][:], data[i*common.AddressLength:])
	}
	return senders, nil
}

// WriteBodyDeprecated - writes body in Network format, later staged sync will convert it into Storage format
func WriteBodyDeprecated(db ethdb.Database, hash common.Hash, number uint64, body *types.Body) error {
	// Pre-processing
	body.SendersFromTxs()
	baseTxId, err := db.IncrementSequence(dbutils.EthTx, uint64(len(body.Transactions)))
	if err != nil {
		return err
	}
	data, err := rlp.EncodeToBytes(types.BodyForStorage{
		BaseTxId: baseTxId,
		TxAmount: uint32(len(body.Transactions)),
		Uncles:   body.Uncles,
	})
	if err != nil {
		return fmt.Errorf("failed to RLP encode body: %w", err)
	}
	WriteBodyRLP(db, hash, number, data)
	err = WriteTransactionsDeprecated(db, body.Transactions, baseTxId)
	if err != nil {
		return fmt.Errorf("failed to WriteTransactions: %w", err)
	}
	return nil
}

func WriteRawBody(db ethdb.RwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	baseTxId, err := db.IncrementSequence(dbutils.EthTx, uint64(len(body.Transactions)))
	if err != nil {
		return err
	}
	data, err := rlp.EncodeToBytes(types.BodyForStorage{
		BaseTxId: baseTxId,
		TxAmount: uint32(len(body.Transactions)),
		Uncles:   body.Uncles,
	})
	if err != nil {
		return fmt.Errorf("failed to RLP encode body: %w", err)
	}
	WriteBodyRLP(db, hash, number, data)
	err = WriteRawTransactions(db, body.Transactions, baseTxId)
	if err != nil {
		return fmt.Errorf("failed to WriteRawTransactions: %w", err)
	}
	return nil
}

func WriteBody(db ethdb.RwTx, hash common.Hash, number uint64, body *types.Body) error {
	// Pre-processing
	body.SendersFromTxs()
	baseTxId, err := db.IncrementSequence(dbutils.EthTx, uint64(len(body.Transactions)))
	if err != nil {
		return err
	}
	data, err := rlp.EncodeToBytes(types.BodyForStorage{
		BaseTxId: baseTxId,
		TxAmount: uint32(len(body.Transactions)),
		Uncles:   body.Uncles,
	})
	if err != nil {
		return fmt.Errorf("failed to RLP encode body: %w", err)
	}
	WriteBodyRLP(db, hash, number, data)
	err = WriteTransactions(db, body.Transactions, baseTxId)
	if err != nil {
		return fmt.Errorf("failed to WriteTransactions: %w", err)
	}
	return nil
}

func WriteSenders(db ethdb.RwTx, hash common.Hash, number uint64, senders []common.Address) error {
	data := make([]byte, common.AddressLength*len(senders))
	for i, sender := range senders {
		copy(data[i*common.AddressLength:], sender[:])
	}
	if err := db.Put(dbutils.Senders, dbutils.BlockBodyKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block senders: %w", err)
	}
	return nil
}

// DeleteBody removes all block body data associated with a hash.
func DeleteBody(db ethdb.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash), nil); err != nil {
		log.Crit("Failed to delete block body", "err", err)
	}
}

// ReadTd retrieves a block's total difficulty corresponding to the hash.
func ReadTd(db ethdb.KVGetter, hash common.Hash, number uint64) (*big.Int, error) {
	data, err := db.GetOne(dbutils.HeaderTDBucket, dbutils.HeaderKey(number, hash))
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

func ReadTdByHash(db ethdb.KVGetter, hash common.Hash) (*big.Int, error) {
	headNumber := ReadHeaderNumber(db, hash)
	if headNumber == nil {
		return nil, nil
	}
	return ReadTd(db, hash, *headNumber)
}

// WriteTd stores the total difficulty of a block into the database.
func WriteTd(db ethdb.Putter, hash common.Hash, number uint64, td *big.Int) error {
	data, err := rlp.EncodeToBytes(td)
	if err != nil {
		return fmt.Errorf("failed to RLP encode block total difficulty: %w", err)
	}
	if err := db.Put(dbutils.HeaderTDBucket, dbutils.HeaderKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block total difficulty: %w", err)
	}
	return nil
}

// DeleteTd removes all block total difficulty data associated with a hash.
func DeleteTd(db ethdb.Deleter, hash common.Hash, number uint64) error {
	if err := db.Delete(dbutils.HeaderTDBucket, dbutils.HeaderKey(number, hash), nil); err != nil {
		return fmt.Errorf("failed to delete block total difficulty: %w", err)
	}
	return nil
}

// HasReceipts verifies the existence of all the transaction receipts belonging
// to a block.
func HasReceipts(db ethdb.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(number)); !has || err != nil {
		return false
	}
	return true
}

// ReadRawReceipts retrieves all the transaction receipts belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadReceipts instead if the metadata is needed.
func ReadRawReceipts(db ethdb.Tx, blockNum uint64) types.Receipts {
	// Retrieve the flattened receipt slice
	data, err := db.GetOne(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(blockNum))
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
	if err := db.ForPrefix(dbutils.Log, prefix, func(k, v []byte) error {
		var logs types.Logs
		if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
			return fmt.Errorf("receipt unmarshal failed:  %w", err)
		}

		receipts[binary.BigEndian.Uint32(k[8:])].Logs = logs
		return nil
	}); err != nil {
		log.Error("logs fetching failed", "err", err)
		return nil
	}

	return receipts
}
func ReadRawReceiptsDeprecated(db ethdb.Getter, hash common.Hash, number uint64) types.Receipts {
	// Retrieve the flattened receipt slice
	data, err := db.GetOne(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(number))
	if err != nil {
		log.Error("ReadRawReceipts failed", "err", err)
	}
	if len(data) == 0 {
		return nil
	}
	var receipts types.Receipts
	if err := cbor.Unmarshal(&receipts, bytes.NewReader(data)); err != nil {
		log.Error("receipt unmarshal failed", "hash", hash, "err", err)
		return nil
	}

	if err := db.Walk(dbutils.Log, dbutils.LogKey(number, 0), 8*8, func(k, v []byte) (bool, error) {
		var logs types.Logs
		if err := cbor.Unmarshal(&logs, bytes.NewReader(v)); err != nil {
			return false, fmt.Errorf("receipt unmarshal failed: %x, %w", hash, err)
		}

		receipts[binary.BigEndian.Uint32(k[8:])].Logs = logs
		return true, nil
	}); err != nil {
		log.Error("logs fetching failed", "hash", hash, "err", err)
		return nil
	}

	return receipts
}

// ReadReceipts retrieves all the transaction receipts belonging to a block, including
// its correspoinding metadata fields. If it is unable to populate these metadata
// fields then nil is returned.
//
// The current implementation populates these metadata fields by reading the receipts'
// corresponding block body, so if the block body is not found it will return nil even
// if the receipt itself is stored.
func ReadReceipts(db ethdb.Tx, block *types.Block, senders []common.Address) types.Receipts {
	if block == nil {
		return nil
	}
	// We're deriving many fields from the block body, retrieve beside the receipt
	receipts := ReadRawReceipts(db, block.NumberU64())
	if receipts == nil {
		return nil
	}
	block.Body().SendersToTxs(senders)
	if err := receipts.DeriveFields(block.Hash(), block.NumberU64(), block.Transactions(), senders); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", block.Hash(), "number", block.NumberU64(), "err", err)
		return nil
	}
	return receipts
}
func ReadReceiptsDeprecated(db ethdb.Getter, hash common.Hash, number uint64) types.Receipts {
	// We're deriving many fields from the block body, retrieve beside the receipt
	receipts := ReadRawReceiptsDeprecated(db, hash, number)
	if receipts == nil {
		return nil
	}
	body := ReadBodyDeprecated(db, hash, number)
	if body == nil {
		log.Error("Missing body but have receipt", "hash", hash, "number", number)
		return nil
	}
	senders, err := ReadSenders(db, hash, number)
	if err != nil {
		log.Error("Failed to read Senders", "hash", hash, "number", number, "err", err)
		return nil
	}
	if senders == nil {
		return nil
	}
	if err = receipts.DeriveFields(hash, number, body.Transactions, senders); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", hash, "number", number, "err", err)
		return nil
	}
	return receipts
}
func ReadReceiptsByHashDeprecated(db ethdb.Getter, hash common.Hash) types.Receipts {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil
	}
	receipts := ReadReceiptsDeprecated(db, hash, *number)
	if receipts == nil {
		return nil
	}
	return receipts
}

func ReadReceiptsByHash(db ethdb.Tx, hash common.Hash) (types.Receipts, error) {
	b, s, err := ReadBlockByHashWithSenders(db, hash)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	receipts := ReadReceipts(db, b, s)
	if receipts == nil {
		return nil, nil
	}
	return receipts, nil
}
func ReadReceiptsByNumber(db ethdb.Getter, number uint64) types.Receipts {
	h, _ := ReadCanonicalHash(db, number)
	return ReadReceiptsDeprecated(db, h, number)
}

// WriteReceipts stores all the transaction receipts belonging to a block.
func WriteReceipts(tx ethdb.Putter, number uint64, receipts types.Receipts) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	for txId, r := range receipts {
		if len(r.Logs) == 0 {
			continue
		}

		buf.Reset()
		err := cbor.Marshal(buf, r.Logs)
		if err != nil {
			return fmt.Errorf("encode block logs for block %d: %v", number, err)
		}

		if err = tx.Put(dbutils.Log, dbutils.LogKey(number, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing logs for block %d: %v", number, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %v", number, err)
	}

	if err = tx.Put(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(number), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %v", number, err)
	}
	return nil
}

// AppendReceipts stores all the transaction receipts belonging to a block.
func AppendReceipts(tx ethdb.RwTx, blockNumber uint64, receipts types.Receipts) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	for txId, r := range receipts {
		//fmt.Printf("1: %d,%x\n", txId, r.TxHash)
		if len(r.Logs) == 0 {
			continue
		}

		buf.Reset()
		err := cbor.Marshal(buf, r.Logs)
		if err != nil {
			return fmt.Errorf("encode block receipts for block %d: %v", blockNumber, err)
		}

		if err = tx.Append(dbutils.Log, dbutils.LogKey(blockNumber, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing receipts for block %d: %v", blockNumber, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %v", blockNumber, err)
	}

	if err = tx.Append(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(blockNumber), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %v", blockNumber, err)
	}
	return nil
}

// DeleteReceipts removes all receipt data associated with a block hash.
func DeleteReceipts(db ethdb.RwTx, number uint64) error {
	if err := db.Delete(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(number), nil); err != nil {
		return fmt.Errorf("receipts delete failed: %d, %w", number, err)
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, number)
	if err := db.ForPrefix(dbutils.Log, prefix, func(k, v []byte) error {
		return db.Delete(dbutils.Log, k, nil)
	}); err != nil {
		return err
	}
	return nil
}

// DeleteNewerReceipts removes all receipt for given block number or newer
func DeleteNewerReceipts(db ethdb.RwTx, number uint64) error {
	if err := db.ForEach(dbutils.BlockReceiptsPrefix, dbutils.ReceiptsKey(number), func(k, v []byte) error {
		return db.Delete(dbutils.BlockReceiptsPrefix, k, nil)
	}); err != nil {
		return err
	}

	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, number)
	if err := db.ForEach(dbutils.Log, from, func(k, v []byte) error {
		return db.Delete(dbutils.Log, k, nil)
	}); err != nil {
		return err
	}
	return nil
}

// ReadBlockDeprecated retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body. If either the header or body could not
// be retrieved nil is returned.
//
// Note, due to concurrent download of header and block body the header and thus
// canonical hash can be stored in the database but the body data not (yet).
// Deprecated
func ReadBlockDeprecated(db ethdb.Getter, hash common.Hash, number uint64) *types.Block {
	header := ReadHeader(db, hash, number)
	if header == nil {
		return nil
	}
	body := ReadBodyDeprecated(db, hash, number)
	if body == nil {
		return nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
}

func ReadBlock(db ethdb.Tx, hash common.Hash, number uint64) *types.Block {
	header := ReadHeader(db, hash, number)
	if header == nil {
		return nil
	}
	body := ReadBody(db, hash, number)
	if body == nil {
		return nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
}

func HasBlockDeprecated(db ethdb.Getter, hash common.Hash, number uint64) bool {
	body := ReadStorageBodyRLP(db, hash, number)
	return len(body) > 0
}

// HasBlock - is more efficient than ReadBlock because doesn't read transactions.
// It's is not equivalent of HasHeader because headers and bodies written by different stages
func HasBlock(db ethdb.KVGetter, hash common.Hash, number uint64) bool {
	body := ReadStorageBodyRLP(db, hash, number)
	return len(body) > 0
}

func ReadBlockWithSenders(db ethdb.Tx, hash common.Hash, number uint64) (*types.Block, []common.Address, error) {
	block := ReadBlock(db, hash, number)
	if block == nil {
		return nil, nil, nil
	}
	senders, err := ReadSenders(db, hash, number)
	if err != nil {
		return nil, nil, err
	}
	if len(senders) != block.Transactions().Len() {
		return nil, nil, nil
	}
	block.Body().SendersToTxs(senders)
	return block, senders, nil
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlockDeprecated(ctx context.Context, db ethdb.Database, block *types.Block) error {
	if err := WriteBodyDeprecated(db, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		return err
	}
	WriteHeader(db, block.Header())
	return nil
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlock(db ethdb.RwTx, block *types.Block) error {
	if err := WriteBody(db, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		return err
	}
	WriteHeader(db, block.Header())
	return nil
}

// DeleteBlock removes all block data associated with a hash.
func DeleteBlock(db ethdb.RwTx, hash common.Hash, number uint64) error {
	if err := DeleteReceipts(db, number); err != nil {
		return err
	}
	DeleteHeader(db, hash, number)
	DeleteBody(db, hash, number)
	if err := DeleteTd(db, hash, number); err != nil {
		return err
	}
	return nil
}

func ReadBlockByNumberDeprecated(db ethdb.Getter, number uint64) (*types.Block, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}

	return ReadBlockDeprecated(db, hash, number), nil
}
func ReadBlockByNumber(db ethdb.Tx, number uint64) (*types.Block, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}

	return ReadBlock(db, hash, number), nil
}

func ReadBlockByNumberWithSenders(db ethdb.Tx, number uint64) (*types.Block, []common.Address, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil, nil
	}

	return ReadBlockWithSenders(db, hash, number)
}

func ReadBlockByHashDeprecated(db ethdb.Getter, hash common.Hash) (*types.Block, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	return ReadBlockDeprecated(db, hash, *number), nil
}

func ReadBlockByHash(db ethdb.Tx, hash common.Hash) (*types.Block, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	return ReadBlock(db, hash, *number), nil
}

func ReadBlockByHashWithSenders(db ethdb.Tx, hash common.Hash) (*types.Block, []common.Address, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil, nil
	}
	return ReadBlockWithSenders(db, hash, *number)
}

func ReadBlocksByHash(db ethdb.Getter, hash common.Hash, n int) (blocks []*types.Block, err error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	for i := 0; i < n; i++ {
		block := ReadBlockDeprecated(db, hash, *number)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		hash = block.ParentHash()
		*number--
	}
	return
}

func ReadHeaderByNumber(db ethdb.KVGetter, number uint64) *types.Header {
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

func ReadHeaderByHash(db ethdb.KVGetter, hash common.Hash) (*types.Header, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	return ReadHeader(db, hash, *number), nil
}

func ReadAncestor(db ethdb.KVGetter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	if ancestor > number {
		return common.Hash{}, 0
	}
	if ancestor == 1 {
		// in this case it is cheaper to just read the header
		if header := ReadHeader(db, hash, number); header != nil {
			return header.ParentHash, number - 1
		}
		return common.Hash{}, 0
	}
	for ancestor != 0 {
		h, err := ReadCanonicalHash(db, number)
		if err != nil {
			panic(err)
		}
		if h == hash {
			ancestorHash, err := ReadCanonicalHash(db, number-ancestor)
			if err != nil {
				panic(err)
			}
			h, err := ReadCanonicalHash(db, number)
			if err != nil {
				panic(err)
			}
			if h == hash {
				number -= ancestor
				return ancestorHash, number
			}
		}
		if *maxNonCanonical == 0 {
			return common.Hash{}, 0
		}
		*maxNonCanonical--
		ancestor--
		header := ReadHeader(db, hash, number)
		if header == nil {
			return common.Hash{}, 0
		}
		hash = header.ParentHash
		number--
	}
	return hash, number
}
