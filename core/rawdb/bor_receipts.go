package rawdb

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/rlp"
)

var (
	// bor receipt key
	borReceiptKey = types.BorReceiptKey
)

// HasBorReceipts verifies the existence of all block receipt belonging to a block.
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

func ReadBorReceipt(db kv.Tx, blockHash libcommon.Hash, blockNumber uint64, receipts types.Receipts) (*types.Receipt, error) {
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

// WriteBorReceipt stores all the bor receipt belonging to a block (storing the state sync receipt and log).
func WriteBorReceipt(tx kv.RwTx, number uint64, borReceipt *types.Receipt) error {
	// Convert the bor receipt into their storage form and serialize them
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err := cbor.Marshal(buf, borReceipt.Logs); err != nil {
		return err
	}
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

func ReadBorTxLookupEntry(db kv.Getter, borTxHash libcommon.Hash) (*uint64, error) {
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

// ReadBorTransactionForBlock retrieves a specific bor (fake) transaction associated with a block, along with
// its added positional metadata.
func ReadBorTransactionForBlock(db kv.Tx, blockNum uint64) types.Transaction {
	if !HasBorReceipts(db, blockNum) {
		return nil
	}
	return types.NewBorTransaction()
}

// TruncateBorReceipts removes all bor receipt for given block number or newer
func TruncateBorReceipts(db kv.RwTx, number uint64) error {
	if err := db.ForEach(kv.BorReceipts, hexutility.EncodeTs(number), func(k, _ []byte) error {
		return db.Delete(kv.BorReceipts, k)
	}); err != nil {
		return err
	}
	return nil
}
