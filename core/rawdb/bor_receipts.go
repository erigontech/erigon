package rawdb

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

var (
	// bor receipt key
	borReceiptKey = types.BorReceiptKey

	// borTxLookupPrefix + hash -> transaction/receipt lookup metadata
	borTxLookupPrefix = []byte("matic-bor-tx-lookup-")
)

// borTxLookupKey = borTxLookupPrefix + bor tx hash
func borTxLookupKey(hash common.Hash) []byte {
	return append(borTxLookupPrefix, hash.Bytes()...)
}

// HasBorReceipt verifies the existence of all block receipt belonging
// to a block.
func HasBorReceipts(db kv.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(kv.BorReceipts, borReceiptKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadBorReceiptRLP retrieves the block receipt belonging to a block in RLP encoding.
func ReadBorReceiptRLP(db kv.Getter, hash common.Hash, number uint64) rlp.RawValue {
	data, err := db.GetOne(kv.BorReceipts, borReceiptKey(number, hash))
	if err != nil {
		log.Error("ReadBorReceiptRLP failed", "err", err)
	}
	return data
}

// ReadRawBorReceipt retrieves the block receipt belonging to a block.
// The receipt metadata fields are not guaranteed to be populated, so they
// should not be used. Use ReadBorReceipt instead if the metadata is needed.
func ReadRawBorReceipt(db kv.Tx, hash common.Hash, number uint64) *types.Receipt {
	// Retrieve the flattened receipt slice
	data := ReadBorReceiptRLP(db, hash, number)
	if data == nil || len(data) == 0 {
		return nil
	}

	// Convert the receipts from their storage form to their internal representation
	var storageReceipt types.ReceiptForStorage
	if err := rlp.DecodeBytes(data, &storageReceipt); err != nil {
		log.Error("Invalid receipt array RLP", "hash", hash, "err", err)
		return nil
	}

	return (*types.Receipt)(&storageReceipt)
}

// ReadBorReceipt retrieves all the bor block receipts belonging to a block, including
// its correspoinding metadata fields. If it is unable to populate these metadata
// fields then nil is returned.
func ReadBorReceipt(db kv.Tx, hash common.Hash, number uint64) *types.Receipt {
	// We're deriving many fields from the block body, retrieve beside the receipt
	borReceipt := ReadRawBorReceipt(db, hash, number)
	if borReceipt == nil {
		return nil
	}

	// We're deriving many fields from the block body, retrieve beside the receipt
	receipts := ReadRawReceipts(db, number)
	if receipts == nil {
		receipts = make(types.Receipts, 0)
	}

	body, _, _ := ReadBody(db, hash, number)
	if body == nil {
		log.Error("Missing body but have bor receipt", "hash", hash, "number", number)
		return nil
	}

	if err := types.DeriveFieldsForBorReceipt(borReceipt, hash, number, receipts); err != nil {
		log.Error("Failed to derive bor receipt fields", "hash", hash, "number", number, "err", err)
		return nil
	}
	return borReceipt
}

// WriteBorReceipt stores all the bor receipt belonging to a block.
func WriteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64, borReceipt *types.ReceiptForStorage) {
	// Convert the bor receipt into their storage form and serialize them
	bytes, err := rlp.EncodeToBytes(borReceipt)
	if err != nil {
		log.Crit("Failed to encode bor receipt", "err", err)
	}

	// Store the flattened receipt slice
	if err := tx.Put(kv.BorReceipts, borReceiptKey(number, hash), bytes); err != nil {
		log.Crit("Failed to store bor receipt", "err", err)
	}
}

// DeleteBorReceipt removes receipt data associated with a block hash.
func DeleteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64) {
	key := borReceiptKey(number, hash)

	if err := tx.Delete(kv.BorReceipts, key, nil); err != nil {
		log.Crit("Failed to delete bor receipt", "err", err)
	}
}

// ReadBorTransactionWithBlockHash retrieves a specific bor (fake) transaction by tx hash and block hash, along with
// its added positional metadata.
func ReadBorTransactionWithBlockHash(db kv.Tx, txHash common.Hash, blockHash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	blockNumber := ReadBorTxLookupEntry(db, txHash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}

	body, _, _ := ReadBody(db, blockHash, *blockNumber)
	if body == nil {
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash)
		return nil, common.Hash{}, 0, 0
	}

	var tx types.Transaction = types.NewBorTransaction()
	return &tx, blockHash, *blockNumber, uint64(len(body.Transactions))
}

// ReadBorTransaction retrieves a specific bor (fake) transaction by hash, along with
// its added positional metadata.
func ReadBorTransaction(db kv.Tx, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	blockNumber := ReadBorTxLookupEntry(db, hash)
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0
	}

	blockHash, _ := ReadCanonicalHash(db, *blockNumber)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0
	}

	body, _, _ := ReadBody(db, blockHash, *blockNumber)
	if body == nil {
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash)
		return nil, common.Hash{}, 0, 0
	}

	var tx types.Transaction = types.NewBorTransaction()
	return &tx, blockHash, *blockNumber, uint64(len(body.Transactions))
}

//
// Indexes for reverse lookup
//

// ReadBorTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the bor transaction or bor receipt using tx hash.
func ReadBorTxLookupEntry(db kv.Tx, txHash common.Hash) *uint64 {
	data, _ := db.GetOne(kv.BorTxLookup, borTxLookupKey(txHash))
	if len(data) == 0 {
		return nil
	}

	number := new(big.Int).SetBytes(data).Uint64()
	return &number
}

// WriteBorTxLookupEntry stores a positional metadata for bor transaction using block hash and block number
func WriteBorTxLookupEntry(db kv.RwTx, hash common.Hash, number uint64) {
	txHash := types.GetDerivedBorTxHash(borReceiptKey(number, hash))
	if err := db.Put(kv.BorTxLookup, borTxLookupKey(txHash), big.NewInt(0).SetUint64(number).Bytes()); err != nil {
		log.Crit("Failed to store bor transaction lookup entry", "err", err)
	}
}

// DeleteBorTxLookupEntry removes bor transaction data associated with block hash and block number
func DeleteBorTxLookupEntry(db kv.RwTx, hash common.Hash, number uint64) {
	txHash := types.GetDerivedBorTxHash(borReceiptKey(number, hash))
	DeleteBorTxLookupEntryByTxHash(db, txHash)
}

// DeleteBorTxLookupEntryByTxHash removes bor transaction data associated with a bor tx hash.
func DeleteBorTxLookupEntryByTxHash(db kv.RwTx, txHash common.Hash) {
	if err := db.Delete(kv.BorTxLookup, borTxLookupKey(txHash), nil); err != nil {
		log.Crit("Failed to delete bor transaction lookup entry", "err", err)
	}
}
