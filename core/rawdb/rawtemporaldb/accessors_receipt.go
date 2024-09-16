package rawtemporaldb

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rlp"
)

func ReadReceipt(tx kv.TemporalTx, txnID kv.TxnId, rawLogs types.Logs, txnIdx int, blockHash common.Hash, blockNum uint64, txn types.Transaction) (*types.Receipt, error) {
	v, ok, err := tx.AppendableGet(kv.ReceiptsAppendable, txnID)
	if err != nil {
		return nil, err
	}
	if !ok || v != nil {
		// The transaction type and hash can be retrieved from the transaction itself
		return nil, nil
	}

	r := &types.Receipt{}
	if err := rlp.DecodeBytes(v, (*types.ReceiptForStorage)(r)); err != nil {
		return nil, err
	}
	r.Logs = rawLogs

	var prevReceipt *types.Receipt
	if r.FirstLogIndex > 0 {
		prevReceipt = &types.Receipt{}
		v, ok, err := tx.AppendableGet(kv.ReceiptsAppendable, txnID-1)
		if err != nil {
			return nil, err
		}
		if ok && v != nil {
			if err := rlp.DecodeBytes(v, (*types.ReceiptForStorage)(r)); err != nil {
				return nil, err
			}
		}
	}
	if err := r.DeriveFieldsV3ForSingleReceipt(txnIdx, blockHash, blockNum, txn, prevReceipt); err != nil {
		return nil, err
	}
	return r, nil
}

// ReadReceipts retrieves all the transaction receipts belonging to a block, including
// its corresponding metadata fields. If it is unable to populate these metadata
// fields then nil is returned.
//
// The current implementation populates these metadata fields by reading the receipts'
// corresponding block body, so if the block body is not found it will return nil even
// if the receipt itself is stored.
func ReadReceipts(tx kv.TemporalTx) types.Receipts {
	panic("TODO: implement me")
}

func AppendReceipts(tx kv.TemporalPutDel, txnID kv.TxnId, r *types.Receipt) error {
	v, err := rlp.EncodeToBytes(r)
	if err != nil {
		return err
	}
	err = tx.AppendablePut(kv.ReceiptsAppendable, txnID, v)
	if err != nil {
		return err
	}
	return nil
}
func AppendEmptyReceipts(tx kv.TemporalPutDel, txnID kv.TxnId) error {
	return tx.AppendablePut(kv.ReceiptsAppendable, txnID, nil)
}

func AppendReceipts2(tx kv.TemporalPutDel, txnID kv.TxnId, r types.ReceiptsForStorage) error {
	v, err := rlp.EncodeToBytes(r)
	if err != nil {
		return err
	}

	err = tx.AppendablePut(kv.ReceiptsAppendable, txnID, v)
	if err != nil {
		return err
	}
	return nil
}
