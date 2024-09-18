package rawtemporaldb

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
)

var (
	CumulativeGasUsedInBlockKey     = []byte("c")
	CumulativeBlobGasUsedInBlockKey = []byte("b")
	CumulativeGasUseTotalKey        = []byte("t")
	FirstLogIndexKey                = []byte("i")
)

// `ReadReceipt` does fill `rawLogs` calulated fields. but we don't need it anymore.
func ReceiptAsOf(tx kv.TemporalTx, txNum uint64, rawLogs types.Logs, txnIdx int, blockHash common.Hash, blockNum uint64, txn types.Transaction) (*types.Receipt, error) {
	v, ok, err := tx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, txNum)
	if err != nil || !ok || v == nil {
		panic(err)
		return nil, err
	}
	cumulativeGasUsedBeforeTxn, _ := binary.Uvarint(v)

	v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, nil, txNum)
	if err != nil || !ok || v == nil {
		panic(err)
		return nil, err
	}
	cumulativeBlobGasUsed, _ := binary.Uvarint(v)

	if txnIdx == 0 {
		//logIndex always 0
	}

	v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, FirstLogIndexKey, nil, txNum)
	if err != nil || !ok || v == nil {
		panic(err)
		return nil, err
	}
	firstLogIndexWithinBlock, _ := binary.Uvarint(v)
	{
		v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, FirstLogIndexKey, nil, txNum-1)
		if err != nil || !ok || v == nil {
			panic(err)
			return nil, err
		}
		a, _ := binary.Uvarint(v)
		v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, FirstLogIndexKey, nil, txNum+1)
		if err != nil || !ok || v == nil {
			panic(err)
			return nil, err
		}
		b, _ := binary.Uvarint(v)
		fmt.Printf("[dbg] lgidx: %d, %d, %d, idx=%d\n", a, firstLogIndexWithinBlock, b, txnIdx)
	}

	v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, txNum)
	if err != nil || !ok || v == nil {
		panic(err)
		return nil, err
	}
	cumulativeGasUsedAfterTxn, _ := binary.Uvarint(v)

	{
		v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, txNum-1)
		if err != nil || !ok || v == nil {
			panic(err)
			return nil, err
		}
		a, _ := binary.Uvarint(v)
		v, ok, err = tx.DomainGetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, nil, txNum+1)
		if err != nil || !ok || v == nil {
			panic(err)
			return nil, err
		}
		b, _ := binary.Uvarint(v)
		fmt.Printf("[dbg] cum: %d, %d, %d, idx=%d\n", a, cumulativeBlobGasUsed, b, txnIdx)
	}

	r := &types.Receipt{
		Logs:                     rawLogs,
		CumulativeGasUsed:        cumulativeGasUsedAfterTxn,
		FirstLogIndexWithinBlock: uint32(firstLogIndexWithinBlock),
	}
	_ = cumulativeBlobGasUsed

	if err := r.DeriveFieldsV3ForSingleReceipt(txnIdx, blockHash, blockNum, txn, cumulativeGasUsedBeforeTxn); err != nil {
		return nil, err
	}
	return r, nil
}

func AppendReceipt(ttx kv.TemporalPutDel, receipt *types.Receipt, cumulativeBlobGasUsed uint64) error {
	var cumulativeGasUsedInBlock uint64
	var firstLogIndexWithinBlock uint32
	if receipt != nil {
		cumulativeGasUsedInBlock = receipt.CumulativeGasUsed
		firstLogIndexWithinBlock = receipt.FirstLogIndexWithinBlock
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

	i = binary.PutUvarint(buf[:], uint64(firstLogIndexWithinBlock))
	if err := ttx.DomainPut(kv.ReceiptDomain, FirstLogIndexKey, nil, buf[:i], nil, 0); err != nil {
		return err
	}
	return nil
}
