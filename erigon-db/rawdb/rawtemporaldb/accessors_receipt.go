package rawtemporaldb

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
)

var (
	CumulativeGasUsedInBlockKey     = []byte{0x0}
	CumulativeBlobGasUsedInBlockKey = []byte{0x1}
	FirstLogIndexKey                = []byte{0x2}
)

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

func AppendReceipt(tx kv.TemporalPutDel, receipt *types.Receipt, cumBlobGasUsed uint64, txNum uint64) error {
	var cumGasUsedInBlock uint64
	var firstLogIndexWithinBlock uint32
	if receipt != nil {
		cumGasUsedInBlock = receipt.CumulativeGasUsed
		firstLogIndexWithinBlock = receipt.FirstLogIndexWithinBlock
	}

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
		i := binary.PutUvarint(buf[:], uint64(firstLogIndexWithinBlock))
		if err := tx.DomainPut(kv.ReceiptDomain, FirstLogIndexKey, buf[:i], txNum, nil, 0); err != nil {
			return err
		}
	}
	return nil
}

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}
