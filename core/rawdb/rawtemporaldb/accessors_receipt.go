package rawtemporaldb

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
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

var (
	cumulativeGasUsedKey = []byte("c")
	firstLogIndex        = []byte("i")
)

func WriteCumulativeGasUsed(tx kv.TemporalPutDel, cumulativeGasUsed uint64) error {
	return tx.DomainPut(kv.ReceiptDomain, cumulativeGasUsedKey, nil, hexutility.EncodeTs(cumulativeGasUsed), nil, 0)
}
func WriteFirstLogIndex(tx kv.TemporalPutDel, firstLogIndex uint64) error {
	firstLogIndex = uint32(r.Logs[0].Index)
	return tx.DomainPut(kv.ReceiptDomain, firstLogIndex, nil, hexutility.EncodeTs(firstLogIndex), nil, 0)
}

func AppendReceipts(tx kv.TemporalPutDel, txnID kv.TxnId, r *types.Receipt) error {
	if r == nil {
		return tx.AppendablePut(kv.ReceiptsAppendable, txnID, nil)
	}
	v, err := rlp.EncodeToBytes(r)
	if err != nil {
		return err
	}
	return tx.AppendablePut(kv.ReceiptsAppendable, txnID, v)
}

func AppendReceipts2(tx kv.TemporalPutDel, txnID kv.TxnId, r types.ReceiptsForStorage) error {
	if r == nil {
		return tx.AppendablePut(kv.ReceiptsAppendable, txnID, nil)
	}
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
