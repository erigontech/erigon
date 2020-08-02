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
	"math/big"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// ReadCanonicalHash retrieves the hash assigned to a canonical block number.
func ReadCanonicalHash(db DatabaseReader, number uint64) common.Hash {
	data, _ := db.Get(dbutils.HeaderPrefix, dbutils.HeaderHashKey(number))
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func readCanonicalHash(tx ethdb.Tx, number uint64) (common.Hash, error) {
	data, err := tx.Bucket(dbutils.HeaderPrefix).Get(dbutils.HeaderHashKey(number))
	if err != nil {
		return common.Hash{}, err
	}
	if len(data) == 0 {
		return common.Hash{}, nil
	}
	return common.BytesToHash(data), nil
}

// WriteCanonicalHash stores the hash assigned to a canonical block number.
func WriteCanonicalHash(db DatabaseWriter, hash common.Hash, number uint64) {
	if err := db.Put(dbutils.HeaderPrefix, dbutils.HeaderHashKey(number), hash.Bytes()); err != nil {
		log.Crit("Failed to store number to hash mapping", "err", err)
	}
}

// DeleteCanonicalHash removes the number to hash canonical mapping.
func DeleteCanonicalHash(db DatabaseDeleter, number uint64) {
	if err := db.Delete(dbutils.HeaderPrefix, dbutils.HeaderHashKey(number)); err != nil {
		log.Crit("Failed to delete number to hash mapping", "err", err)
	}
}

// ReadAllHashes retrieves all the hashes assigned to blocks at a certain heights,
// both canonical and reorged forks included.
func ReadAllHashes(db DatabaseReader, number uint64) []common.Hash {
	//prefix := headerKeyPrefix(number)

	hashes := make([]common.Hash, 0, 1)
	/*
		it := db.NewIteratorWithPrefix(prefix)
		defer it.Release()

		for it.Next() {
			if key := it.Key(); len(key) == len(prefix)+32 {
				hashes = append(hashes, common.BytesToHash(key[len(key)-32:]))
			}
		}
	*/
	return hashes
}

// ReadHeaderNumber returns the header number assigned to a hash.
func ReadHeaderNumber(db DatabaseReader, hash common.Hash) *uint64 {
	data, _ := db.Get(dbutils.HeaderNumberPrefix, hash.Bytes())
	if len(data) != 8 {
		return nil
	}
	number := binary.BigEndian.Uint64(data)
	return &number
}

// WriteHeaderNumber stores the hash->number mapping.
func WriteHeaderNumber(db DatabaseWriter, hash common.Hash, number uint64) {
	enc := dbutils.EncodeBlockNumber(number)
	if err := db.Put(dbutils.HeaderNumberPrefix, hash[:], enc); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
}

// DeleteHeaderNumber removes hash->number mapping.
func DeleteHeaderNumber(db DatabaseDeleter, hash common.Hash) {
	if err := db.Delete(dbutils.HeaderNumberPrefix, hash[:]); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadHeadHeaderHash retrieves the hash of the current canonical head header.
func ReadHeadHeaderHash(db DatabaseReader) common.Hash {
	data, _ := db.Get(dbutils.HeadHeaderKey, dbutils.HeadHeaderKey)
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadHeaderHash stores the hash of the current canonical head header.
func WriteHeadHeaderHash(db DatabaseWriter, hash common.Hash) {
	if err := db.Put(dbutils.HeadHeaderKey, dbutils.HeadHeaderKey, hash.Bytes()); err != nil {
		log.Crit("Failed to store last header's hash", "err", err)
	}
}

// ReadHeadBlockHash retrieves the hash of the current canonical head block.
func ReadHeadBlockHash(db DatabaseReader) common.Hash {
	data, _ := db.Get(dbutils.HeadBlockKey, dbutils.HeadBlockKey)
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadBlockHash stores the head block's hash.
func WriteHeadBlockHash(db DatabaseWriter, hash common.Hash) {
	if err := db.Put(dbutils.HeadBlockKey, dbutils.HeadBlockKey, hash.Bytes()); err != nil {
		log.Crit("Failed to store last block's hash", "err", err)
	}
}

// ReadHeadFastBlockHash retrieves the hash of the current fast-sync head block.
func ReadHeadFastBlockHash(db DatabaseReader) common.Hash {
	data, _ := db.Get(dbutils.HeadFastBlockKey, dbutils.HeadFastBlockKey)
	if len(data) == 0 {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

// WriteHeadFastBlockHash stores the hash of the current fast-sync head block.
func WriteHeadFastBlockHash(db DatabaseWriter, hash common.Hash) {
	if err := db.Put(dbutils.HeadFastBlockKey, dbutils.HeadFastBlockKey, hash.Bytes()); err != nil {
		log.Crit("Failed to store last fast block's hash", "err", err)
	}
}

// ReadFastTrieProgress retrieves the number of tries nodes fast synced to allow
// reporting correct numbers across restarts.
func ReadFastTrieProgress(db DatabaseReader) uint64 {
	data, _ := db.Get(dbutils.FastTrieProgressKey, dbutils.FastTrieProgressKey)
	if len(data) == 0 {
		return 0
	}
	return new(big.Int).SetBytes(data).Uint64()
}

// WriteFastTrieProgress stores the fast sync trie process counter to support
// retrieving it across restarts.
func WriteFastTrieProgress(db DatabaseWriter, count uint64) {
	if err := db.Put(dbutils.FastTrieProgressKey, dbutils.FastTrieProgressKey, new(big.Int).SetUint64(count).Bytes()); err != nil {
		log.Crit("Failed to store fast sync trie progress", "err", err)
	}
}

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db DatabaseReader, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash))
	return data
}

func readHeaderRLP(tx ethdb.Tx, hash common.Hash, number uint64) (rlp.RawValue, error) {
	data, err := tx.Bucket(dbutils.HeaderPrefix).Get(dbutils.HeaderKey(number, hash))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// HasHeader verifies the existence of a block header corresponding to the hash.
func HasHeader(db DatabaseReader, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadHeader retrieves the block header corresponding to the hash.
func ReadHeader(db DatabaseReader, hash common.Hash, number uint64) *types.Header {
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

func readHeader(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Header, error) {
	data, err := readHeaderRLP(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		log.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil, err
	}
	return header, nil
}

// WriteHeader stores a block header into the database and also stores the hash-
// to-number mapping.
func WriteHeader(ctx context.Context, db DatabaseWriter, header *types.Header) {
	var (
		hash    = header.Hash()
		number  = header.Number.Uint64()
		encoded = dbutils.EncodeBlockNumber(number)
	)
	if common.IsCanceled(ctx) {
		return
	}
	if err := db.Put(dbutils.HeaderNumberPrefix, hash[:], encoded); err != nil {
		log.Crit("Failed to store hash to number mapping", "err", err)
	}
	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		log.Crit("Failed to RLP encode header", "err", err)
	}
	if err := db.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash), data); err != nil {
		log.Crit("Failed to store header", "err", err)
	}
}

// DeleteHeader removes all block header data associated with a hash.
func DeleteHeader(db DatabaseDeleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash)); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(dbutils.HeaderNumberPrefix, hash.Bytes()); err != nil {
		log.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// deleteHeaderWithoutNumber removes only the block header but does not remove
// the hash to number mapping.
func deleteHeaderWithoutNumber(db DatabaseDeleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.HeaderPrefix, dbutils.HeaderKey(number, hash)); err != nil {
		log.Crit("Failed to delete header", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db DatabaseReader, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash))
	if debug.IsBlockCompressionEnabled() && len(data) > 0 {
		var err error
		data, err = snappy.Decode(nil, data)
		if err != nil {
			log.Warn("err on decode block", "err", err)
		}
	}
	return data
}

func readBodyRLP(tx ethdb.Tx, hash common.Hash, number uint64) (rlp.RawValue, error) {
	data, err := tx.Bucket(dbutils.BlockBodyPrefix).Get(dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, err
	}
	if debug.IsBlockCompressionEnabled() && len(data) > 0 {
		var err error
		data, err = snappy.Decode(nil, data)
		if err != nil {
			log.Warn("err on decode block", "err", err)
		}
	}
	return data, nil
}

// WriteBodyRLP stores an RLP encoded block body into the database.
func WriteBodyRLP(ctx context.Context, db DatabaseWriter, hash common.Hash, number uint64, rlp rlp.RawValue) {
	if common.IsCanceled(ctx) {
		return
	}
	if debug.IsBlockCompressionEnabled() {
		rlp = snappy.Encode(nil, rlp)
	}
	if err := db.Put(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash), rlp); err != nil {
		log.Crit("Failed to store block body", "err", err)
	}
}

// HasBody verifies the existence of a block body corresponding to the hash.
func HasBody(db DatabaseReader, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadBody retrieves the block body corresponding to the hash.
func ReadBody(db DatabaseReader, hash common.Hash, number uint64) *types.Body {
	data := ReadBodyRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}

func readBody(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Body, error) {
	data, err := readBodyRLP(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil, err
	}
	return body, nil
}

func ReadSenders(db DatabaseReader, hash common.Hash, number uint64) []common.Address {
	data, _ := db.Get(dbutils.Senders, dbutils.BlockBodyKey(number, hash))
	senders := make([]common.Address, len(data)/common.AddressLength)
	for i := 0; i < len(senders); i++ {
		copy(senders[i][:], data[i*common.AddressLength:])
	}
	return senders
}

func readSenders(tx ethdb.Tx, hash common.Hash, number uint64) ([]common.Address, error) {
	data, err := tx.Bucket(dbutils.Senders).Get(dbutils.BlockBodyKey(number, hash))
	if err != nil {
		return nil, err
	}
	senders := make([]common.Address, len(data)/common.AddressLength)
	for i := 0; i < len(senders); i++ {
		copy(senders[i][:], data[i*common.AddressLength:])
	}
	return senders, nil
}

// WriteBody storea a block body into the database.
func WriteBody(ctx context.Context, db DatabaseWriter, hash common.Hash, number uint64, body *types.Body) {
	if common.IsCanceled(ctx) {
		return
	}
	// Pre-processing
	body.SendersFromTxs()
	data, err := rlp.EncodeToBytes(body)
	if err != nil {
		log.Crit("Failed to RLP encode body", "err", err)
	}
	WriteBodyRLP(ctx, db, hash, number, data)
}

func WriteSenders(ctx context.Context, db DatabaseWriter, hash common.Hash, number uint64, senders []common.Address) {
	if common.IsCanceled(ctx) {
		return
	}
	data := make([]byte, common.AddressLength*len(senders))
	for i, sender := range senders {
		copy(data[i*common.AddressLength:], sender[:])
	}
	if err := db.Put(dbutils.Senders, dbutils.BlockBodyKey(number, hash), data); err != nil {
		log.Crit("Failed to store block senders", "err", err)
	}
}

// DeleteBody removes all block body data associated with a hash.
func DeleteBody(db DatabaseDeleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.BlockBodyPrefix, dbutils.BlockBodyKey(number, hash)); err != nil {
		log.Crit("Failed to delete block body", "err", err)
	}
}

// ReadTdRLP retrieves a block's total difficulty corresponding to the hash in RLP encoding.
func ReadTdRLP(db DatabaseReader, hash common.Hash, number uint64) rlp.RawValue {
	//data, _ := db.Ancient(freezerDifficultyTable, number)
	data := []byte{}
	if len(data) == 0 {
		data, _ = db.Get(dbutils.HeaderPrefix, dbutils.HeaderTDKey(number, hash))
		// In the background freezer is moving data from leveldb to flatten files.
		// So during the first check for ancient db, the data is not yet in there,
		// but when we reach into leveldb, the data was already moved. That would
		// result in a not found error.
		if len(data) == 0 {
			//data, _ = db.Ancient(freezerDifficultyTable, number)
		}
	}
	return nil // Can't find the data anywhere.
}

// ReadTd retrieves a block's total difficulty corresponding to the hash.
func ReadTd(db DatabaseReader, hash common.Hash, number uint64) *big.Int {
	data, _ := db.Get(dbutils.HeaderPrefix, dbutils.HeaderTDKey(number, hash))
	if len(data) == 0 {
		return nil
	}
	td := new(big.Int)
	if err := rlp.Decode(bytes.NewReader(data), td); err != nil {
		log.Error("Invalid block total difficulty RLP", "hash", hash, "err", err)
		return nil
	}
	return td
}

// WriteTd stores the total difficulty of a block into the database.
func WriteTd(db DatabaseWriter, hash common.Hash, number uint64, td *big.Int) {
	data, err := rlp.EncodeToBytes(td)
	if err != nil {
		log.Crit("Failed to RLP encode block total difficulty", "err", err)
	}
	if err := db.Put(dbutils.HeaderPrefix, dbutils.HeaderTDKey(number, hash), data); err != nil {
		log.Crit("Failed to store block total difficulty", "err", err)
	}
}

// DeleteTd removes all block total difficulty data associated with a hash.
func DeleteTd(db DatabaseDeleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.HeaderPrefix, dbutils.HeaderTDKey(number, hash)); err != nil {
		log.Crit("Failed to delete block total difficulty", "err", err)
	}
}

// HasReceipts verifies the existence of all the transaction receipts belonging
// to a block.
func HasReceipts(db DatabaseReader, hash common.Hash, number uint64) bool {
	if has, err := db.Has(dbutils.BlockReceiptsPrefix, dbutils.BlockReceiptsKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadReceiptsRLP retrieves all the transaction receipts belonging to a block in RLP encoding.
func ReadReceiptsRLP(db DatabaseReader, hash common.Hash, number uint64) rlp.RawValue {
	data := []byte{}
	//data, _ := db.Ancient(freezerReceiptTable, number)
	if len(data) == 0 {
		data, _ = db.Get(dbutils.BlockReceiptsPrefix, dbutils.BlockReceiptsKey(number, hash))
		// In the background freezer is moving data from leveldb to flatten files.
		// So during the first check for ancient db, the data is not yet in there,
		// but when we reach into leveldb, the data was already moved. That would
		// result in a not found error.
		if len(data) == 0 {
			//data, _ = db.Ancient(freezerReceiptTable, number)
		}
	}
	return nil // Can't find the data anywhere.
}

// ReadRawReceipts retrieves all the transaction receipts belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadReceipts instead if the metadata is needed.
func ReadRawReceipts(db DatabaseReader, hash common.Hash, number uint64) types.Receipts {
	// Retrieve the flattened receipt slice
	data, _ := db.Get(dbutils.BlockReceiptsPrefix, dbutils.BlockReceiptsKey(number, hash))
	if len(data) == 0 {
		return nil
	}
	// Convert the receipts from their storage form to their internal representation
	storageReceipts := []*types.ReceiptForStorage{}
	if err := rlp.DecodeBytes(data, &storageReceipts); err != nil {
		log.Error("Invalid receipt array RLP", "hash", hash, "err", err)
		return nil
	}
	receipts := make(types.Receipts, len(storageReceipts))
	for i, storageReceipt := range storageReceipts {
		receipts[i] = (*types.Receipt)(storageReceipt)
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
func ReadReceipts(db DatabaseReader, hash common.Hash, number uint64, config *params.ChainConfig) types.Receipts {
	// We're deriving many fields from the block body, retrieve beside the receipt
	receipts := ReadRawReceipts(db, hash, number)
	if receipts == nil {
		return nil
	}
	body := ReadBody(db, hash, number)
	if body == nil {
		log.Error("Missing body but have receipt", "hash", hash, "number", number)
		return nil
	}
	if err := receipts.DeriveFields(config, hash, number, body.Transactions); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", hash, "number", number, "err", err)
		return nil
	}
	return receipts
}

// WriteReceipts stores all the transaction receipts belonging to a block.
func WriteReceipts(db DatabaseWriter, hash common.Hash, number uint64, receipts types.Receipts) {
	// Convert the receipts into their storage form and serialize them
	storageReceipts := make([]*types.ReceiptForStorage, len(receipts))
	for i, receipt := range receipts {
		storageReceipts[i] = (*types.ReceiptForStorage)(receipt)
	}
	bytes, err := rlp.EncodeToBytes(storageReceipts)
	if err != nil {
		log.Crit("Failed to encode block receipts", "err", err)
	}
	// Store the flattened receipt slice
	if err := db.Put(dbutils.BlockReceiptsPrefix, dbutils.BlockReceiptsKey(number, hash), bytes); err != nil {
		log.Crit("Failed to store block receipts", "err", err)
	}
}

// DeleteReceipts removes all receipt data associated with a block hash.
func DeleteReceipts(db DatabaseDeleter, hash common.Hash, number uint64) {
	if err := db.Delete(dbutils.BlockReceiptsPrefix, dbutils.BlockReceiptsKey(number, hash)); err != nil {
		log.Crit("Failed to delete block receipts", "err", err)
	}
}

// ReadBlock retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body. If either the header or body could not
// be retrieved nil is returned.
//
// Note, due to concurrent download of header and block body the header and thus
// canonical hash can be stored in the database but the body data not (yet).
func ReadBlock(db DatabaseReader, hash common.Hash, number uint64) *types.Block {
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

func readBlock(tx ethdb.Tx, hash common.Hash, number uint64) (*types.Block, error) {
	header, err := readHeader(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	body, err := readBody(tx, hash, number)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	return types.NewBlockWithHeader(header).WithBody(body.Transactions, body.Uncles), nil
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlock(ctx context.Context, db DatabaseWriter, block *types.Block) {
	WriteBody(ctx, db, block.Hash(), block.NumberU64(), block.Body())
	WriteHeader(ctx, db, block.Header())
}

// WriteAncientBlock writes entire block data into ancient store and returns the total written size.
/*
func WriteAncientBlock(db ethdb.AncientWriter, block *types.Block, receipts types.Receipts, td *big.Int) int {
	// Encode all block components to RLP format.
	headerBlob, err := rlp.EncodeToBytes(block.Header())
	if err != nil {
		log.Crit("Failed to RLP encode block header", "err", err)
	}
	bodyBlob, err := rlp.EncodeToBytes(block.Body())
	if err != nil {
		log.Crit("Failed to RLP encode body", "err", err)
	}
	storageReceipts := make([]*types.ReceiptForStorage, len(receipts))
	for i, receipt := range receipts {
		storageReceipts[i] = (*types.ReceiptForStorage)(receipt)
	}
	receiptBlob, err := rlp.EncodeToBytes(storageReceipts)
	if err != nil {
		log.Crit("Failed to RLP encode block receipts", "err", err)
	}
	tdBlob, err := rlp.EncodeToBytes(td)
	if err != nil {
		log.Crit("Failed to RLP encode block total difficulty", "err", err)
	}
	// Write all blob to flatten files.
	err = db.AppendAncient(block.NumberU64(), block.Hash().Bytes(), headerBlob, bodyBlob, receiptBlob, tdBlob)
	if err != nil {
		log.Crit("Failed to write block data to ancient store", "err", err)
	}
	return len(headerBlob) + len(bodyBlob) + len(receiptBlob) + len(tdBlob) + common.HashLength
}
*/

// DeleteBlock removes all block data associated with a hash.
func DeleteBlock(db DatabaseDeleter, hash common.Hash, number uint64) {
	DeleteReceipts(db, hash, number)
	DeleteHeader(db, hash, number)
	DeleteBody(db, hash, number)
	DeleteTd(db, hash, number)
}

// DeleteBlockWithoutNumber removes all block data associated with a hash, except
// the hash to number mapping.
func DeleteBlockWithoutNumber(db DatabaseDeleter, hash common.Hash, number uint64) {
	DeleteReceipts(db, hash, number)
	deleteHeaderWithoutNumber(db, hash, number)
	DeleteBody(db, hash, number)
	DeleteTd(db, hash, number)
}

// FindCommonAncestor returns the last common ancestor of two block headers
func FindCommonAncestor(db DatabaseReader, a, b *types.Header) *types.Header {
	for bn := b.Number.Uint64(); a.Number.Uint64() > bn; {
		a = ReadHeader(db, a.ParentHash, a.Number.Uint64()-1)
		if a == nil {
			return nil
		}
	}
	for an := a.Number.Uint64(); an < b.Number.Uint64(); {
		b = ReadHeader(db, b.ParentHash, b.Number.Uint64()-1)
		if b == nil {
			return nil
		}
	}
	for a.Hash() != b.Hash() {
		a = ReadHeader(db, a.ParentHash, a.Number.Uint64()-1)
		if a == nil {
			return nil
		}
		b = ReadHeader(db, b.ParentHash, b.Number.Uint64()-1)
		if b == nil {
			return nil
		}
	}
	return a
}

func ReadBlockWithSenders(db DatabaseReader, number uint64) (*types.Block, common.Hash, error) {
	kv := db.(ethdb.HasKV).KV()
	var block *types.Block
	var hash common.Hash
	if err := kv.View(context.Background(), func(tx ethdb.Tx) error {
		var err error
		hash, err = readCanonicalHash(tx, number)
		if err != nil {
			return err
		}
		block, err := readBlock(tx, hash, number)
		if err != nil {
			return err
		}
		if block == nil {
			return nil
		}
		senders, err := readSenders(tx, hash, number)
		if err != nil {
			return err
		}
		block.Body().SendersToTxs(senders)

		return nil
	}); err != nil {
		return nil, common.Hash{}, err
	}

	return block, hash, nil
}

func ReadBlockByNumber(db DatabaseReader, number uint64) *types.Block {
	hash := ReadCanonicalHash(db, number)
	if hash == (common.Hash{}) {
		return nil
	}

	return ReadBlock(db, hash, number)
}

func ReadBlockByHash(db DatabaseReader, hash common.Hash) *types.Block {
	number := ReadHeaderNumber(db, hash)
	if number == nil {
		return nil
	}
	return ReadBlock(db, hash, *number)
}

// FIXME: implement in Turbo-Geth
// WriteAncientBlock writes entire block data into ancient store and returns the total written size.
func WriteAncientBlock(db DatabaseWriter, block *types.Block, receipts types.Receipts, td *big.Int) int {
	panic("not implemented")
}
