package rawtemporaldb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
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

// ReceiptCacheKey is the single RCacheDomain key; the value is the encoded receipt.
var ReceiptCacheKey = []byte{0x0}

// ReceiptWriter is a reusable, single-goroutine encoder for the receipt domains.
// It hands its own scratch to DomainPut, so the value bytes are only valid until
// the next call: a kv.TemporalPutDel must copy what it keeps.
type ReceiptWriter struct {
	buf          [binary.MaxVarintLen64]byte
	rlpEncodeBuf bytes.Buffer
}

// Append stores the receipt in RCacheDomain.
func (w *ReceiptWriter) Append(tx kv.TemporalPutDel, receipt *types.Receipt, txNum uint64) error {
	var toWrite []byte

	if receipt != nil {
		if len(receipt.Logs) > 0 && int(receipt.FirstLogIndexWithinBlock) != int(receipt.Logs[0].Index) {
			panic(fmt.Sprintf("assert: FirstLogIndexWithinBlock is wrong: %d %d, blockNum=%d", receipt.FirstLogIndexWithinBlock, receipt.Logs[0].Index, receipt.BlockNumber.Uint64()))
		}

		storageReceipt := (*types.ReceiptForStorage)(receipt)
		w.rlpEncodeBuf.Reset()
		if err := rlp.Encode(&w.rlpEncodeBuf, storageReceipt); err != nil {
			return fmt.Errorf("WriteReceiptCache: %w", err)
		}
		toWrite = w.rlpEncodeBuf.Bytes()
		if dbg.AssertEnabled {
			storageReceipt2 := &types.ReceiptForStorage{}
			if err := rlp.DecodeBytes(toWrite, storageReceipt2); err != nil {
				panic(fmt.Sprintf("assert: receipt encode/decode round-trip: %v", err))
			}
			if storageReceipt.ContractAddress != storageReceipt2.ContractAddress {
				panic(fmt.Sprintf("assert: %x, %x\n", storageReceipt.ContractAddress, storageReceipt2.ContractAddress))
			}
			if storageReceipt.FirstLogIndexWithinBlock != storageReceipt2.FirstLogIndexWithinBlock {
				panic(fmt.Sprintf("assert: %x, %x\n", storageReceipt.FirstLogIndexWithinBlock, storageReceipt2.FirstLogIndexWithinBlock))
			}
			if storageReceipt.TransactionIndex != storageReceipt2.TransactionIndex {
				panic(fmt.Sprintf("assert: TransactionIndex mismatch: %d, %d\n", storageReceipt.TransactionIndex, storageReceipt2.TransactionIndex))
			}
		}
	} else {
		toWrite = []byte{}
	}

	if err := tx.DomainPut(kv.RCacheDomain, ReceiptCacheKey, toWrite, txNum, nil); err != nil {
		return fmt.Errorf("WriteReceiptCache: %w", err)
	}
	return nil
}

func (w *ReceiptWriter) AppendMetadata(tx kv.TemporalPutDel, logIndexAfterTx uint32, cumGasUsedInBlock, cumBlobGasUsed uint64, txNum uint64) error {
	i := binary.PutUvarint(w.buf[:], cumGasUsedInBlock)
	if err := tx.DomainPut(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, w.buf[:i], txNum, nil); err != nil {
		return err
	}
	i = binary.PutUvarint(w.buf[:], cumBlobGasUsed)
	if err := tx.DomainPut(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, w.buf[:i], txNum, nil); err != nil {
		return err
	}
	i = binary.PutUvarint(w.buf[:], uint64(logIndexAfterTx))
	if err := tx.DomainPut(kv.ReceiptDomain, LogIndexAfterTxKey, w.buf[:i], txNum, nil); err != nil {
		return err
	}
	return nil
}

func AppendReceiptMetadata(tx kv.TemporalPutDel, logIndexAfterTx uint32, cumGasUsedInBlock, cumBlobGasUsed uint64, txNum uint64) error {
	var w ReceiptWriter
	return w.AppendMetadata(tx, logIndexAfterTx, cumGasUsedInBlock, cumBlobGasUsed, txNum)
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

// ReceiptValueForTest is the value encoding, for assertions in tests.
func ReceiptValueForTest(v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], v)
	return buf[:i]
}
