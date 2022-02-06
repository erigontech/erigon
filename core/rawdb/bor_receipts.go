package rawdb

import (
	"errors"
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
)

func borTxLookupKey(hash common.Hash) []byte {
	return append(hash.Bytes(), hash.Bytes()...)
}

// HasBorReceipt verifies the existence of all block receipt belonging
// to a block.
func HasBorReceipts(db kv.Has, hash common.Hash, number uint64) bool {
	if has, err := db.Has(kv.BorReceipts, borReceiptKey(number)); !has || err != nil {
		return false
	}
	return true
}

// ReadBorReceiptRLP retrieves the block receipt belonging to a block in RLP encoding.
func ReadBorReceiptRLP(db kv.Getter, hash common.Hash, number uint64) rlp.RawValue {
	data, err := db.GetOne(kv.BorReceipts, borReceiptKey(number))
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

	data := ReadStorageBodyRLP(db, hash, number)
	if len(data) == 0 {
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
func WriteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64, borReceipt *types.ReceiptForStorage) error {
	// Convert the bor receipt into their storage form and serialize them
	bytes, err := rlp.EncodeToBytes(borReceipt)
	if err != nil {
		return err
	}

	// Store the flattened receipt slice
	if err := tx.Append(kv.BorReceipts, borReceiptKey(number), bytes); err != nil {
		return err
	}

	return nil
}

// DeleteBorReceipt removes receipt data associated with a block hash.
func DeleteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64) {
	key := borReceiptKey(number)

	if err := tx.Delete(kv.BorReceipts, key, nil); err != nil {
		log.Crit("Failed to delete bor receipt", "err", err)
	}
}

// ReadBorTransactionWithBlockHash retrieves a specific bor (fake) transaction by tx hash and block hash, along with
// its added positional metadata.
func ReadBorTransactionWithBlockHash(db kv.Tx, txHash common.Hash, blockHash common.Hash) (*types.Transaction, common.Hash, uint64, uint64, error) {
	blockNumber, err := ReadBorTxLookupEntry(db, txHash)

	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}

	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0, nil
	}

	bodyForStorage, err := ReadStorageBody(db, blockHash, *blockNumber)
	if err != nil{
		return nil, common.Hash{}, 0, 0, nil
	}

	var tx types.Transaction = types.NewBorTransaction()
	return &tx, blockHash, *blockNumber, uint64(bodyForStorage.TxAmount), nil
}

// ReadBorTransaction retrieves a specific bor (fake) transaction by hash, along with
// its added positional metadata.
func ReadBorTransaction(db kv.Tx, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64, error) {
	blockNumber, err := ReadBorTxLookupEntry(db, hash)

	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}

	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0, errors.New("missing block number")
	}

	blockHash, _ := ReadCanonicalHash(db, *blockNumber)
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0, errors.New("missing block hash")
	}

	bodyForStorage, err := ReadStorageBody(db, hash, *blockNumber)
	if err != nil{
		return nil, common.Hash{}, 0, 0, nil
	}
	
	var tx types.Transaction = types.NewBorTransaction()
	return &tx, blockHash, *blockNumber, uint64(bodyForStorage.TxAmount), nil
}

//
// Indexes for reverse lookup
//

// ReadBorTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the bor transaction or bor receipt using tx hash.
func ReadBorTxLookupEntry(db kv.Tx, txHash common.Hash) (*uint64, error) {
	data, err := db.GetOne(kv.BorTxLookup, borTxLookupKey(txHash))

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, errors.New("no data found")
	}

	number := new(big.Int).SetBytes(data).Uint64()
	return &number, nil
}

// WriteBorTxLookupEntry stores a positional metadata for bor transaction using block hash and block number
func WriteBorTxLookupEntry(db kv.RwTx, hash common.Hash, number uint64) error {
	txHash := types.GetDerivedBorTxHash(borReceiptKey(number))
	if err := db.Put(kv.BorTxLookup, borTxLookupKey(txHash), big.NewInt(0).SetUint64(number).Bytes()); err != nil {
		return err
	}
	return nil
}

// DeleteBorTxLookupEntry removes bor transaction data associated with block hash and block number
func DeleteBorTxLookupEntry(db kv.RwTx, hash common.Hash, number uint64) error {
	txHash := types.GetDerivedBorTxHash(borReceiptKey(number))
	if err := DeleteBorTxLookupEntryByTxHash(db, txHash); err != nil {
		return err
	}

	return nil
}

// DeleteBorTxLookupEntryByTxHash removes bor transaction data associated with a bor tx hash.
func DeleteBorTxLookupEntryByTxHash(db kv.RwTx, txHash common.Hash) error {
	if err := db.Delete(kv.BorTxLookup, borTxLookupKey(txHash), nil); err != nil {
		return err
	}
	return nil
}
