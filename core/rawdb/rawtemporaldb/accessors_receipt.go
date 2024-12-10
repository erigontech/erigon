package rawtemporaldb

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/types"
)

var (
	CumulativeGasUsedInBlockKey     = []byte{0x0}
	CumulativeBlobGasUsedInBlockKey = []byte{0x1}
	FirstLogIndexKey                = []byte{0x2}
)

// `ReadReceipt` does fill `rawLogs` calulated fields. but we don't need it anymore.
func ReceiptAsOfWithApply(tx kv.TemporalTx, txNum uint64, rawLogs types.Logs, txnIdx int, blockHash common.Hash, blockNum uint64, txn types.Transaction) (*types.Receipt, error) {
	cumulativeGasUsedBeforeTxn, cumulativeBlobGasUsed, firstLogIndexWithinBlock, err := ReceiptAsOf(tx, txNum+1)
	if err != nil {
		return nil, err
	}
	//if txnIdx == 0 {
	//logIndex always 0
	//}

	r := &types.Receipt{
		Logs:                     rawLogs,
		CumulativeGasUsed:        cumulativeGasUsedBeforeTxn,
		FirstLogIndexWithinBlock: firstLogIndexWithinBlock,
	}
	_ = cumulativeBlobGasUsed

	if err := r.DeriveFieldsV3ForSingleReceipt(txnIdx, blockHash, blockNum, txn, cumulativeGasUsedBeforeTxn); err != nil {
		return nil, err
	}
	return r, nil
}

func ReceiptAsOf(tx kv.TemporalTx, txNum uint64) (cumGasUsed uint64, cumBlobGasused uint64, firstLogIndexWithinBlock uint32, err error) {
	var v []byte
	var ok bool

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		cumGasUsed = uvarint(v)
	}

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		cumBlobGasused = uvarint(v)
	}

	//if txnIdx == 0 {
	//logIndex always 0
	//}

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, FirstLogIndexKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		firstLogIndexWithinBlock = uint32(uvarint(v))
	}
	return
}

var prevTxNum uint64

func AppendReceipt(ttx kv.TemporalPutDel, receipt *types.Receipt, cumBlobGasUsed uint64) error {
	if casted, ok := ttx.(*state.SharedDomains); ok {
		if prevTxNum > 0 {
			if prevTxNum >= casted.TxNum() {
				panic(fmt.Errorf("assert: %d > %d", prevTxNum, casted.TxNum()))
			} else {
				prevTxNum = casted.TxNum()
			}
		}
	}
	var cumGasUsedInBlock uint64
	var firstLogIndexWithinBlock uint32
	if receipt != nil {
		cumGasUsedInBlock = receipt.CumulativeGasUsed
		firstLogIndexWithinBlock = receipt.FirstLogIndexWithinBlock
	}

	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], cumGasUsedInBlock)
		if err := ttx.DomainPut(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, buf[:i], nil, 0); err != nil {
			return err
		}
	}

	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], cumBlobGasUsed)
		if err := ttx.DomainPut(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, nil, buf[:i], nil, 0); err != nil {
			return err
		}
	}

	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], uint64(firstLogIndexWithinBlock))
		if err := ttx.DomainPut(kv.ReceiptDomain, FirstLogIndexKey, nil, buf[:i], nil, 0); err != nil {
			return err
		}
	}
	return nil
}

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}
