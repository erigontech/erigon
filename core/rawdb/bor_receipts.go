package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

	common2 "github.com/ledgerwatch/erigon-lib/common"
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

func ReadRawBorReceipt(db kv.Tx, number uint64) (*types.Receipt, bool, error) {
	data, err := db.GetOne(kv.BorReceipts, borReceiptKey(number))
	if err != nil {
		return nil, false, fmt.Errorf("ReadBorReceipt failed getting bor receipt with blockNumber=%d, err=%s", number, err)
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var borReceipt *types.Receipt
	err = cbor.Unmarshal(&borReceipt, bytes.NewReader(data))
	if err == nil {
		return borReceipt, false, nil
	}

	// Convert the receipts from their storage form to their internal representation
	var borStorageReceipt types.ReceiptForStorage
	if err := rlp.DecodeBytes(data, &borStorageReceipt); err != nil {
		log.Error("Invalid receipt array RLP", "err", err)
		return nil, true, err
	}

	return (*types.Receipt)(&borStorageReceipt), true, nil
}

func ReadBorReceipt(db kv.Tx, blockHash common.Hash, blockNumber uint64, receipts types.Receipts) (*types.Receipt, error) {
	borReceipt, hasEmbeddedLogs, err := ReadRawBorReceipt(db, blockNumber)
	if err != nil {
		return nil, err
	}

	if borReceipt == nil {
		return nil, nil
	}

	if !hasEmbeddedLogs {
		logsData, err := db.GetOne(kv.Log, dbutils.LogKey(blockNumber, uint32(len(receipts))))
		if err != nil {
			return nil, fmt.Errorf("ReadBorReceipt failed getting bor logs with blockNumber=%d, err=%s", blockNumber, err)
		}
		if logsData != nil {
			var logs types.Logs
			if err = cbor.Unmarshal(&logs, bytes.NewReader(logsData)); err != nil {
				return nil, fmt.Errorf("logs unmarshal failed:  %w", err)
			}
			borReceipt.Logs = logs
		}
	}

	types.DeriveFieldsForBorReceipt(borReceipt, blockHash, blockNumber, receipts)

	return borReceipt, nil
}

// WriteBorReceipt stores all the bor receipt belonging to a block (storing the state sync recipt and log).
func WriteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64, borReceipt *types.Receipt) error {
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

/*
// DeleteBorReceipt removes receipt data associated with a block hash.
func DeleteBorReceipt(tx kv.RwTx, hash common.Hash, number uint64) {
	key := borReceiptKey(number)

	// we delete Bor Receipt log too
	borReceipt, err := ReadBorReceipt(tx, number)
	if err != nil {
		log.Error("Failted to read bor receipt", "err", err)
	}
	if borReceipt != nil {
		if err := tx.Delete(kv.Log, dbutils.LogKey(number, uint32(borReceipt.TransactionIndex))); err != nil {
			log.Error("Failed to delete bor log", "err", err)
		}
	}

	if err := tx.Delete(kv.BorReceipts, key); err != nil {
		log.Error("Failed to delete bor receipt", "err", err)
	}
}

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
	if err := db.ForEach(kv.BorReceipts, common2.EncodeTs(number), func(k, _ []byte) error {
		return db.Delete(kv.BorReceipts, k)
	}); err != nil {
		return err
	}
	return nil
}
