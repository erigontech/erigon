package rawtemporaldb

import (
	"encoding/binary"

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
	if r.FirstLogIndexWithinBlock > 0 {
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
	CumulativeGasUsedInBlockKey     = []byte("c")
	CumulativeBlobGasUsedInBlockKey = []byte("b")
	CumulativeGasUseTotalKey        = []byte("t")
	FirstLogIndexKey                = []byte("i")
)

func AppendReceipt(ttx kv.TemporalPutDel, receipt *types.Receipt, cumulativeBlobGasUsed uint64) error {
	var cumulativeGasUsedInBlock uint64
	var FirstLogIndexWithinBlock uint32
	if receipt != nil {
		cumulativeGasUsedInBlock = receipt.CumulativeGasUsed
		FirstLogIndexWithinBlock = receipt.FirstLogIndexWithinBlock
	}
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], cumulativeGasUsedInBlock)
	if err := ttx.DomainPut(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, buf[:i], nil, 0); err != nil {
		return err
	}

	i = binary.PutUvarint(buf[:], cumulativeBlobGasUsed)
	if err := ttx.DomainPut(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, nil, buf[:i], nil, 0); err != nil {
		return err
	}

	i = binary.PutUvarint(buf[:], uint64(FirstLogIndexWithinBlock))
	if err := ttx.DomainPut(kv.ReceiptDomain, FirstLogIndexKey, nil, buf[:i], nil, 0); err != nil {
		return err
	}
	return nil
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
