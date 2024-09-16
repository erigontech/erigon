package rawtemporaldb

import (
	"encoding/binary"

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
	firstLogIndexKey     = []byte("i")
)

func AppendReceipt3(tx kv.TemporalPutDel, cumulativeGasUsed uint64, firstLogIndex uint32) error {
	if err := tx.DomainPut(kv.ReceiptDomain, cumulativeGasUsedKey, nil, hexutility.EncodeTs(cumulativeGasUsed), nil, 0); err != nil {
		return err
	}
	var enc [4]byte
	binary.BigEndian.PutUint32(enc[:], firstLogIndex)
	if err := tx.DomainPut(kv.ReceiptDomain, firstLogIndexKey, nil, enc[:], nil, 0); err != nil {
		return err
	}
	return nil
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
