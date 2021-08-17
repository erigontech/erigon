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
	"encoding/binary"
	"fmt"
	"math"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

// ReadCanonicalHash retrieves the hash assigned to a canonical block number.
func ReadCanonicalHash(db kv.Getter, number uint64) (common.Hash, error) {
	data, err := db.GetOne(kv.HeaderCanonical, dbutils.EncodeBlockNumber(number))
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
	if err := db.Put(kv.HeaderCanonical, dbutils.EncodeBlockNumber(number), hash.Bytes()); err != nil {
		return fmt.Errorf("failed to store number to hash mapping: %w", err)
	}
	return nil
}

// DeleteCanonicalHash removes the number to hash canonical mapping.
func DeleteCanonicalHash(db kv.Deleter, number uint64) error {
	if err := db.Delete(kv.HeaderCanonical, dbutils.EncodeBlockNumber(number), nil); err != nil {
		return fmt.Errorf("failed to delete number to hash mapping: %w", err)
	}
	return nil
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

// WriteHeaderNumber stores the hash->number mapping.
func WriteHeaderNumber(db kv.Putter, hash common.Hash, number uint64) {
	enc := dbutils.EncodeBlockNumber(number)
	if err := db.Put(kv.HeaderNumber, hash[:], enc); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
}

// DeleteHeaderNumber removes hash->number mapping.
func DeleteHeaderNumber(db kv.Deleter, hash common.Hash) {
	if err := db.Delete(kv.HeaderNumber, hash[:], nil); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
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

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db kv.Getter, hash common.Hash, number uint64) rlp.RawValue {
	data, err := db.GetOne(kv.Headers, dbutils.HeaderKey(number, hash))
	if err != nil {
		log.Error("ReadHeaderRLP failed", "err", err)
	}
	return data
}

// HasHeader verifies the existence of a block header corresponding to the hash.
func HasHeader(db kv.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(kv.Headers, dbutils.HeaderKey(number, hash)); !has || err != nil {
		return false
	}
	return true
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

func ReadCurrentBlock(db kv.Tx) *types.Block {
	headHash := ReadHeadBlockHash(db)
	headNumber := ReadHeaderNumber(db, headHash)
	if headNumber == nil {
		return nil
	}
	return ReadBlock(db, headHash, *headNumber)
}

func ReadHeadersByNumber(db kv.Tx, number uint64) ([]*types.Header, error) {
	var res []*types.Header
	c, err := db.Cursor(kv.Headers)
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
func WriteHeader(db kv.Putter, header *types.Header) {
	var (
		hash    = header.Hash()
		number  = header.Number.Uint64()
		encoded = dbutils.EncodeBlockNumber(number)
	)
	if err := db.Put(kv.HeaderNumber, hash[:], encoded); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		log.Crit("Failed to RLP encode header", "err", err)
	}
	if err := db.Put(kv.Headers, dbutils.HeaderKey(number, hash), data); err != nil {
		log.Crit("Failed to store header", "err", err)
	}
}

// DeleteHeader removes all block header data associated with a hash.
func DeleteHeader(db kv.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.Headers, dbutils.HeaderKey(number, hash), nil); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(kv.HeaderNumber, hash.Bytes(), nil); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db kv.Tx, hash common.Hash, number uint64) rlp.RawValue {
	body := ReadBodyWithTransactions(db, hash, number)
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

func ReadTransactions(db kv.Getter, baseTxId uint64, amount uint32) ([]types.Transaction, error) {
	if amount == 0 {
		return []types.Transaction{}, nil
	}
	txIdKey := make([]byte, 8)
	reader := bytes.NewReader(nil)
	stream := rlp.NewStream(reader, 0)
	txs := make([]types.Transaction, amount)
	binary.BigEndian.PutUint64(txIdKey, baseTxId)
	i := uint32(0)

	if err := db.ForAmount(kv.EthTx, txIdKey, amount, func(k, v []byte) error {
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

func WriteTransactions(db kv.RwTx, txs []types.Transaction, baseTxId uint64) error {
	txId := baseTxId
	buf := bytes.NewBuffer(nil)
	c, err := db.RwCursor(kv.EthTx)
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

func WriteRawTransactions(db kv.RwTx, txs [][]byte, baseTxId uint64) error {
	txId := baseTxId
	c, err := db.RwCursor(kv.EthTx)
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
func WriteBodyRLP(db kv.Putter, hash common.Hash, number uint64, rlp rlp.RawValue) {
	if err := db.Put(kv.BlockBody, dbutils.BlockBodyKey(number, hash), rlp); err != nil {
		log.Crit("Failed to store block body", "err", err)
	}
}

// HasBody verifies the existence of a block body corresponding to the hash.
func HasBody(db kv.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(kv.BlockBody, dbutils.BlockBodyKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

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

func ReadBodyWithTransactions(db kv.Getter, hash common.Hash, number uint64) *types.Body {
	body, baseTxId, txAmount := ReadBody(db, hash, number)
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

func RawTransactionsRange(db kv.Getter, from, to uint64) (res [][]byte, err error) {
	blockKey := make([]byte, dbutils.NumberLength+common.HashLength)
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
	return body, bodyForStorage.BaseTxId, bodyForStorage.TxAmount
}

func ReadSenders(db kv.Getter, hash common.Hash, number uint64) ([]common.Address, error) {
	data, err := db.GetOne(kv.Senders, dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, fmt.Errorf("readSenders failed: %w", err)
	}
	senders := make([]common.Address, len(data)/common.AddressLength)
	for i := 0; i < len(senders); i++ {
		copy(senders[i][:], data[i*common.AddressLength:])
	}
	return senders, nil
}

func WriteRawBody(db kv.RwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	baseTxId, err := db.IncrementSequence(kv.EthTx, uint64(len(body.Transactions)))
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

func WriteBody(db kv.RwTx, hash common.Hash, number uint64, body *types.Body) error {
	// Pre-processing
	body.SendersFromTxs()
	baseTxId, err := db.IncrementSequence(kv.EthTx, uint64(len(body.Transactions)))
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

func WriteSenders(db kv.RwTx, hash common.Hash, number uint64, senders []common.Address) error {
	data := make([]byte, common.AddressLength*len(senders))
	for i, sender := range senders {
		copy(data[i*common.AddressLength:], sender[:])
	}
	if err := db.Put(kv.Senders, dbutils.BlockBodyKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store block senders: %w", err)
	}
	return nil
}

// DeleteBody removes all block body data associated with a hash.
func DeleteBody(db kv.Deleter, hash common.Hash, number uint64) {
	if err := db.Delete(kv.BlockBody, dbutils.BlockBodyKey(number, hash), nil); err != nil {
		log.Crit("Failed to delete block body", "err", err)
	}
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

// DeleteTd removes all block total difficulty data associated with a hash.
func DeleteTd(db kv.Deleter, hash common.Hash, number uint64) error {
	if err := db.Delete(kv.HeaderTD, dbutils.HeaderKey(number, hash), nil); err != nil {
		return fmt.Errorf("failed to delete block total difficulty: %w", err)
	}
	return nil
}

// HasReceipts verifies the existence of all the transaction receipts belonging
// to a block.
func HasReceipts(db kv.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(kv.Receipts, dbutils.ReceiptsKey(number)); !has || err != nil {
		return false
	}
	return true
}

// ReadRawReceipts retrieves all the transaction receipts belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadReceipts instead if the metadata is needed.
func ReadRawReceipts(db kv.Tx, blockNum uint64) types.Receipts {
	// Retrieve the flattened receipt slice
	data, err := db.GetOne(kv.Receipts, dbutils.ReceiptsKey(blockNum))
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
	if err := db.ForPrefix(kv.Log, prefix, func(k, v []byte) error {
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

// ReadReceipts retrieves all the transaction receipts belonging to a block, including
// its correspoinding metadata fields. If it is unable to populate these metadata
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
	block.SendersToTxs(senders)
	if err := receipts.DeriveFields(block.Hash(), block.NumberU64(), block.Transactions(), senders); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", block.Hash(), "number", block.NumberU64(), "err", err)
		return nil
	}
	return receipts
}

func ReadReceiptsByHash(db kv.Tx, hash common.Hash) (types.Receipts, error) {
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
			return fmt.Errorf("encode block logs for block %d: %v", number, err)
		}

		if err = tx.Put(kv.Log, dbutils.LogKey(number, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing logs for block %d: %v", number, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %v", number, err)
	}

	if err = tx.Put(kv.Receipts, dbutils.ReceiptsKey(number), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %v", number, err)
	}
	return nil
}

// AppendReceipts stores all the transaction receipts belonging to a block.
func AppendReceipts(tx kv.RwTx, blockNumber uint64, receipts types.Receipts) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	for txId, r := range receipts {
		if len(r.Logs) == 0 {
			continue
		}

		buf.Reset()
		err := cbor.Marshal(buf, r.Logs)
		if err != nil {
			return fmt.Errorf("encode block receipts for block %d: %v", blockNumber, err)
		}

		if err = tx.Append(kv.Log, dbutils.LogKey(blockNumber, uint32(txId)), buf.Bytes()); err != nil {
			return fmt.Errorf("writing receipts for block %d: %v", blockNumber, err)
		}
	}

	buf.Reset()
	err := cbor.Marshal(buf, receipts)
	if err != nil {
		return fmt.Errorf("encode block receipts for block %d: %v", blockNumber, err)
	}

	if err = tx.Append(kv.Receipts, dbutils.ReceiptsKey(blockNumber), buf.Bytes()); err != nil {
		return fmt.Errorf("writing receipts for block %d: %v", blockNumber, err)
	}
	return nil
}

// DeleteReceipts removes all receipt data associated with a block hash.
func DeleteReceipts(db kv.RwTx, number uint64) error {
	if err := db.Delete(kv.Receipts, dbutils.ReceiptsKey(number), nil); err != nil {
		return fmt.Errorf("receipts delete failed: %d, %w", number, err)
	}

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, number)
	if err := db.ForPrefix(kv.Log, prefix, func(k, v []byte) error {
		return db.Delete(kv.Log, k, nil)
	}); err != nil {
		return err
	}
	return nil
}

// DeleteNewerReceipts removes all receipt for given block number or newer
func DeleteNewerReceipts(db kv.RwTx, number uint64) error {
	if err := db.ForEach(kv.Receipts, dbutils.ReceiptsKey(number), func(k, v []byte) error {
		return db.Delete(kv.Receipts, k, nil)
	}); err != nil {
		return err
	}

	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, number)
	if err := db.ForEach(kv.Log, from, func(k, v []byte) error {
		return db.Delete(kv.Log, k, nil)
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
	body := ReadBodyWithTransactions(tx, hash, number)
	if body == nil {
		return nil
	}
	//return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles)
	return types.NewBlockFromStorage(hash, header, body.Transactions, body.Uncles)
}

// HasBlock - is more efficient than ReadBlock because doesn't read transactions.
// It's is not equivalent of HasHeader because headers and bodies written by different stages
func HasBlock(db kv.Getter, hash common.Hash, number uint64) bool {
	body := ReadStorageBodyRLP(db, hash, number)
	return len(body) > 0
}

func ReadBlockWithSenders(db kv.Tx, hash common.Hash, number uint64) (*types.Block, []common.Address, error) {
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
	block.SendersToTxs(senders)
	return block, senders, nil
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlock(db kv.RwTx, block *types.Block) error {
	if err := WriteBody(db, block.Hash(), block.NumberU64(), block.Body()); err != nil {
		return err
	}
	WriteHeader(db, block.Header())
	return nil
}

// DeleteBlock removes all block data associated with a hash.
func DeleteBlock(db kv.RwTx, hash common.Hash, number uint64) error {
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

func ReadBlockByNumber(db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}

	return ReadBlock(db, hash, number), nil
}

func ReadBlockByNumberWithSenders(db kv.Tx, number uint64) (*types.Block, []common.Address, error) {
	hash, err := ReadCanonicalHash(db, number)
	if err != nil {
		return nil, nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil, nil
	}

	return ReadBlockWithSenders(db, hash, number)
}

func ReadBlockByHash(db kv.Tx, hash common.Hash) (*types.Block, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil
	}
	return ReadBlock(db, hash, *number), nil
}

func ReadBlockByHashWithSenders(db kv.Tx, hash common.Hash) (*types.Block, []common.Address, error) {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil, nil, nil
	}
	return ReadBlockWithSenders(db, hash, *number)
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

func ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
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

func DeleteNewerEpochs(tx kv.RwTx, number uint64) error {
	if err := tx.ForEach(kv.PendingEpoch, dbutils.EncodeBlockNumber(number), func(k, v []byte) error {
		return tx.Delete(kv.Epoch, k, nil)
	}); err != nil {
		return err
	}
	return tx.ForEach(kv.Epoch, dbutils.EncodeBlockNumber(number), func(k, v []byte) error {
		return tx.Delete(kv.Epoch, k, nil)
	})
}
func ReadEpoch(tx kv.Tx, blockNum uint64, blockHash common.Hash) (transitionProof []byte, err error) {
	k := make([]byte, dbutils.NumberLength+common.HashLength)
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
	seek := dbutils.EncodeBlockNumber(n)
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
	k := make([]byte, dbutils.NumberLength+common.HashLength)
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
