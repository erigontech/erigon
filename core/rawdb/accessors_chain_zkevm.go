package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/dbg"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
)

func DeleteCumulativeGasUsed(tx kv.RwTx, blockFrom uint64) error {
	if err := tx.ForEach(kv.CumulativeGasIndex, hexutility.EncodeTs(blockFrom), func(k, v []byte) error {
		return tx.Delete(kv.CumulativeGasIndex, k)
	}); err != nil {
		return fmt.Errorf("TruncateCanonicalHash: %w", err)
	}
	return nil
}

func DeleteTransactions(db kv.RwTx, txsCount, baseTxId uint64, blockHash *libcommon.Hash) error {
	for id := baseTxId; id < baseTxId+txsCount; id++ {
		txIdKey := make([]byte, 8)
		binary.BigEndian.PutUint64(txIdKey, id)

		var err error
		if blockHash != nil {
			key := append(txIdKey, blockHash.Bytes()...)
			db.Delete(kv.EthTxV3, key)
		} else {
			db.Delete(kv.EthTx, txIdKey)
		}

		if err != nil {
			return fmt.Errorf("error deleting tx: %w", err)
		}
	}

	return nil
}

func DeleteBodyAndTransactions(tx kv.RwTx, blockNum uint64, blockHash libcommon.Hash) error {
	key := dbutils.BlockBodyKey(blockNum, blockHash)

	v, err := tx.GetOne(kv.BlockBody, key)
	if err != nil {
		return err
	}

	var body types.BodyForStorage
	if err := rlp.DecodeBytes(v, &body); err != nil {
		return fmt.Errorf("failed to decode body: %w", err)
	}

	txs, err := CanonicalTransactions(tx, body.BaseTxId, body.TxAmount)
	if err != nil {
		return fmt.Errorf("failed to read txs: %w", err)
	}

	// delete body for storage
	deleteBody(tx, blockHash, blockNum)

	// delete transactions
	if err := DeleteTransactions(tx, uint64(len(txs)), body.BaseTxId+1, nil); err != nil {
		return fmt.Errorf("failed to delete txs: %w", err)
	}

	return nil
}

func WriteBodyAndTransactions(db kv.RwTx, hash libcommon.Hash, number uint64, txs []ethTypes.Transaction, data *types.BodyForStorage) error {
	var err error
	if err = WriteBodyForStorage(db, hash, number, data); err != nil {
		return fmt.Errorf("failed to write body: %w", err)
	}
	transactionV3, _ := kvcfg.TransactionsV3.Enabled(db.(kv.Tx))
	if transactionV3 {
		err = OverwriteTransactions(db, txs, data.BaseTxId+1, &hash)
	} else {
		err = OverwriteTransactions(db, txs, data.BaseTxId+1, nil)
	}
	if err != nil {
		return fmt.Errorf("failed to WriteTransactions: %w", err)
	}
	return nil
}

func OverwriteTransactions(db kv.RwTx, txs []types.Transaction, baseTxId uint64, blockHash *libcommon.Hash) error {
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
		if blockHash != nil {
			key := append(txIdKey, blockHash.Bytes()...)
			if err := db.Put(kv.EthTxV3, key, common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}
		} else {
			if err := db.Put(kv.EthTx, txIdKey, common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}
		}
	}
	return nil
}

func GetBodyTransactions(tx kv.RwTx, fromBlockNum, toBlockNum uint64) (*[]types.Transaction, error) {
	var transactions []types.Transaction
	if err := tx.ForEach(kv.BlockBody, hexutility.EncodeTs(fromBlockNum), func(k, v []byte) error {
		blocNum := binary.BigEndian.Uint64(k[:8])
		if blocNum < fromBlockNum || blocNum > toBlockNum {
			return nil
		}

		var body types.BodyForStorage
		if err := rlp.DecodeBytes(v, &body); err != nil {
			return fmt.Errorf("failed to decode body: %w", err)
		}

		txs, err := CanonicalTransactions(tx, body.BaseTxId, body.TxAmount)
		if err != nil {
			return fmt.Errorf("failed to read txs: %w", err)
		}
		transactions = append(transactions, txs...)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("TruncateBodies: %w", err)
	}
	return &transactions, nil
}

func DeleteForkchoiceFinalized(db kv.Deleter) error {
	if err := db.Delete(kv.LastForkchoice, []byte("finalizedBlockHash")); err != nil {
		return fmt.Errorf("failed to delete LastForkchoice: %w", err)
	}

	return nil
}

// WriteHeader stores a block header into the database and also stores the hash-
// to-number mapping.
func WriteHeader_zkEvm(db kv.Putter, header *types.Header) error {
	var (
		hash    = header.Hash()
		number  = header.Number.Uint64()
		encoded = hexutility.EncodeTs(number)
	)
	if err := db.Put(kv.HeaderNumber, hash[:], encoded); err != nil {
		return fmt.Errorf("failed to store hash to number mapping: %W", err)
	}
	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("failed to RLP encode header: %W", err)
	}
	if err := db.Put(kv.Headers, dbutils.HeaderKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store header: %W", err)
	}

	return nil
}

// WriteHeader stores a block header into the database and also stores the hash-
// to-number mapping.
func WriteHeaderWithhash(db kv.Putter, hash libcommon.Hash, header *types.Header) error {
	var (
		number  = header.Number.Uint64()
		encoded = hexutility.EncodeTs(number)
	)
	if err := db.Put(kv.HeaderNumber, hash[:], encoded); err != nil {
		return fmt.Errorf("failed to store hash to number mapping: %W", err)
	}
	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("failed to RLP encode header: %W", err)
	}
	if err := db.Put(kv.Headers, dbutils.HeaderKey(number, hash), data); err != nil {
		return fmt.Errorf("failed to store header: %W", err)
	}

	return nil
}

// ReadReceipts retrieves all the transaction receipts belonging to a block, including
// its corresponding metadata fields. If it is unable to populate these metadata
// fields then nil is returned.
//
// The current implementation populates these metadata fields by reading the receipts'
// corresponding block body, so if the block body is not found it will return nil even
// if the receipt itself is stored.
func ReadReceipts_zkEvm(db kv.Tx, block *types.Block, senders []libcommon.Address) types.Receipts {
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
	}

	//[hack] there was a cumulativeGasUsed bug priod to forkid8, so we need to check for it
	hermezDb := hermez_db.NewHermezDbReader(db)
	forkBlocks, err := hermezDb.GetAllForkBlocks()
	if err != nil {
		log.Error("Failed to get fork blocks", "err", err, "stack", dbg.Stack())
		return nil
	}

	forkid8BlockNum := uint64(0)
	highestForkId := uint64(0)
	for forkId, forkBlock := range forkBlocks {
		if forkId > highestForkId {
			highestForkId = forkId
		}
		if forkId == 8 {
			forkid8BlockNum = forkBlock
			break
		}
	}

	// if we don't have forkid8 and highest saved is lower, then we are lower than forkid
	// otherwise we are higher than forkid8 but don't have it saved so it should be treated as if it was 0
	if forkid8BlockNum == 0 && highestForkId < 8 {
		forkid8BlockNum = math.MaxUint64
	}

	if err := receipts.DeriveFields_zkEvm(forkid8BlockNum, block.Hash(), block.NumberU64(), block.Transactions(), senders); err != nil {
		log.Error("Failed to derive block receipts fields", "hash", block.Hash(), "number", block.NumberU64(), "err", err, "stack", dbg.Stack())
		return nil
	}
	return receipts
}
