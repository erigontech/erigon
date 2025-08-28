package rawtemporaldb

import (
	"encoding/binary"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

var (
	CumulativeGasUsedInBlockKey     = []byte{0x0}
	CumulativeBlobGasUsedInBlockKey = []byte{0x1}
	LogIndexAfterTxKey              = []byte{0x2}
)

/**
logIndexAfterTx- log index to be used by next transaction's first log.

rationale:

1. before this firstLogIdx was used -- but for scenario where execution begins from middle of a block,
   it was difficult to find the logIdx to use for the first executing tx. (ReceiptDomain doesn't have logs,
   so can't find prevTx.FirstLogIdx + len(logs)).

2. lastLogIndex (of current tx) was considered -- but it's tricky to handle the case where tx has no logs.
   i.e. should the next tx's first log be lastLogIdx+1 or lastLogIdx?
**/

func ReceiptAsOf(tx kv.TemporalTx, txNum uint64) (cumGasUsed uint64, cumBlobGasused uint64, logIndexAfterTx uint32, err error) {
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

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, LogIndexAfterTxKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		logIndexAfterTx = uint32(uvarint(v))
	}
	return
}

func AppendReceipt(tx kv.TemporalPutDel, logIndexAfterTx uint32, cumGasUsedInBlock, cumBlobGasUsed uint64, txNum uint64) error {
	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], cumGasUsedInBlock)
		if err := tx.DomainPut(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, buf[:i], txNum, nil, 0); err != nil {
			return err
		}
	}

	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], cumBlobGasUsed)
		if err := tx.DomainPut(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, buf[:i], txNum, nil, 0); err != nil {
			return err
		}
	}

	{
		var buf [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(buf[:], uint64(logIndexAfterTx))
		if err := tx.DomainPut(kv.ReceiptDomain, LogIndexAfterTxKey, buf[:i], txNum, nil, 0); err != nil {
			return err
		}
	}
	return nil
}

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}

func ReceiptStoresFirstLogIdx(tx kv.TemporalTx) bool {
	// this stored firstLogIdx;
	// latter versions (v2_0 onwards) stores lastLogIdx
	// this check allows to put some ifchecks to handle
	// both cases and maintain backward compatibility of
	// snapshots.
	return tx.Debug().CurrentDomainVersion(kv.ReceiptDomain).Less(version.V1_1)
}
