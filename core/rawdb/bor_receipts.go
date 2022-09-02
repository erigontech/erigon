package rawdb

import (
	"bytes"
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
)

var (
	// bor receipt key
	borReceiptKey = types.BorReceiptKey
)

// HasBorReceipt verifies the existence of all block receipt belonging
// to a block.
func HasBorReceipts(db kv.Has, number uint64) bool {
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
	if len(data) == 0 {
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

	if err := types.DeriveFieldsForBorReceipt(borReceipt, hash, number, receipts); err != nil {
		log.Error("Failed to derive bor receipt fields", "hash", hash, "number", number, "err", err)
		return nil
	}
	return borReceipt
}

// WriteBorReceipt stores all the bor receipt belonging to a block (storing the state sync recipt and log).
func WriteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64, borReceipt *types.ReceiptForStorage) error {
	// Convert the bor receipt into their storage form and serialize them
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	cbor.Marshal(buf, borReceipt.Logs)
	if err := tx.Append(kv.Log, dbutils.LogKey(number, uint32(borReceipt.TransactionIndex)), buf.Bytes()); err != nil {
		return err
	}

	buf.Reset()
	err := cbor.Marshal(buf, borReceipt)
	if err != nil {
		return err
	}
	// Store the flattened receipt slice
	if err := tx.Append(kv.BorReceipts, borReceiptKey(number), buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// DeleteBorReceipt removes receipt data associated with a block hash.
func DeleteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64) {
	key := borReceiptKey(number)

	// we delete Bor Receipt log too
	borReceipt := ReadBorReceipt(tx, hash, number)
	if borReceipt != nil {
		if err := tx.Delete(kv.Log, dbutils.LogKey(number, uint32(borReceipt.TransactionIndex))); err != nil {
			log.Crit("Failed to delete bor log", "err", err)
		}
	}

	if err := tx.Delete(kv.BorReceipts, key); err != nil {
		log.Crit("Failed to delete bor receipt", "err", err)
	}
}

/*
// ReadBorTransactionWithBlockHash retrieves a specific bor (fake) transaction by tx hash and block hash, along with
// its added positional metadata.
func ReadBorTransactionWithBlockHash(db kv.Tx, borTxHash common.Hash, blockHash common.Hash) (*types.Transaction, common.Hash, uint64, uint64, error) {
	blockNumber, err := ReadTxLookupEntry(db, borTxHash)
	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0, errors.New("missing block number")
	}

	bodyForStorage, err := ReadStorageBody(db, blockHash, *blockNumber)
	if err != nil {
		return nil, common.Hash{}, 0, 0, nil
	}

	var tx types.Transaction = types.NewBorTransaction()
	return &tx, blockHash, *blockNumber, uint64(bodyForStorage.TxAmount), nil
}
*/

// ReadBorTransaction returns a specific bor (fake) transaction by txn hash, along with
// its added positional metadata.
func ReadBorTransaction(db kv.Tx, borTxHash common.Hash) (types.Transaction, common.Hash, uint64, uint64, error) {
	blockNumber, err := ReadBorTxLookupEntry(db, borTxHash)
	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}
	if blockNumber == nil {
		return nil, common.Hash{}, 0, 0, errors.New("missing block number")
	}

	return computeBorTransactionForBlockNumber(db, *blockNumber)
}

func ReadBorTxLookupEntry(db kv.Tx, borTxHash common.Hash) (*uint64, error) {
	blockNumBytes, err := db.GetOne(kv.BorTxLookup, borTxHash.Bytes())
	if err != nil {
		return nil, err
	}
	if blockNumBytes == nil {
		return nil, nil
	}

	blockNum := (new(big.Int).SetBytes(blockNumBytes)).Uint64()
	return &blockNum, nil
}

// ReadBorTransactionForBlockNumber returns a bor (fake) transaction by block number, along with
// its added positional metadata.
func ReadBorTransactionForBlockNumber(db kv.Tx, blockNumber uint64) (types.Transaction, common.Hash, uint64, uint64, error) {
	if !HasBorReceipts(db, blockNumber) {
		return nil, common.Hash{}, 0, 0, nil
	}
	return computeBorTransactionForBlockNumber(db, blockNumber)
}

func computeBorTransactionForBlockNumber(db kv.Tx, blockNumber uint64) (types.Transaction, common.Hash, uint64, uint64, error) {
	blockHash, err := ReadCanonicalHash(db, blockNumber)
	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}
	if blockHash == (common.Hash{}) {
		return nil, common.Hash{}, 0, 0, errors.New("missing block hash")
	}

	return computeBorTransactionForBlockNumberAndHash(db, blockNumber, blockHash)
}

// ReadBorTransactionForBlockNumberAndHash returns a bor (fake) transaction by block number and block hash, along with
// its added positional metadata.
func ReadBorTransactionForBlockNumberAndHash(db kv.Tx, blockNumber uint64, blockHash common.Hash) (types.Transaction, common.Hash, uint64, uint64, error) {
	if !HasBorReceipts(db, blockNumber) {
		return nil, common.Hash{}, 0, 0, nil
	}
	return computeBorTransactionForBlockNumberAndHash(db, blockNumber, blockHash)
}

func computeBorTransactionForBlockNumberAndHash(db kv.Tx, blockNumber uint64, blockHash common.Hash) (types.Transaction, common.Hash, uint64, uint64, error) {
	bodyForStorage, err := ReadStorageBody(db, blockHash, blockNumber)
	if err != nil {
		return nil, common.Hash{}, 0, 0, err
	}

	var tx types.Transaction = types.NewBorTransaction()
	return tx, blockHash, blockNumber, uint64(bodyForStorage.TxAmount), nil
}

// ReadBorTransactionForBlock retrieves a specific bor (fake) transaction associated with a block, along with
// its added positional metadata.
func ReadBorTransactionForBlock(db kv.Tx, block *types.Block) (types.Transaction, common.Hash, uint64, uint64) {
	if !HasBorReceipts(db, block.NumberU64()) {
		return nil, common.Hash{}, 0, 0
	}
	return computeBorTransactionForBlock(db, block)
}

func computeBorTransactionForBlock(db kv.Tx, block *types.Block) (types.Transaction, common.Hash, uint64, uint64) {
	var tx types.Transaction = types.NewBorTransaction()
	return tx, block.Hash(), block.NumberU64(), uint64(len(block.Transactions()))
}

// TruncateBorReceipts removes all bor receipt for given block number or newer
func TruncateBorReceipts(db kv.RwTx, number uint64) error {
	if err := db.ForEach(kv.BorReceipts, dbutils.EncodeBlockNumber(number), func(k, _ []byte) error {
		return db.Delete(kv.BorReceipts, k)
	}); err != nil {
		return err
	}
	return nil
}
