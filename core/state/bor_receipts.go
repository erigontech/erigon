package state

import (
	"encoding/binary"
	"encoding/json"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/shards"
)

type BorReceiptsWriter struct {
	rs *StateV3
}

func NewBorReceiptsWriter(rs *StateV3, accumulator *shards.Accumulator) *BorReceiptsWriter {
	return &BorReceiptsWriter{
		rs: rs,
	}
}

func (bw *BorReceiptsWriter) AddReceipt(blockNum uint64, receipt *types.Receipt) error {
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNum)

	receiptBytes, err := json.Marshal(receipt)
	if err != nil {
		return err
	}

	return bw.rs.domains.DomainPut(kv.BorReceiptDomain, blockNumBytes, nil, receiptBytes, nil, 0)
}

type BorReceiptsReader struct {
	txNum     uint64
	trace     bool
	tx        kv.TemporalGetter
	composite []byte
}

func NewBorReceiptsReader(tx kv.TemporalGetter) *BorReceiptsReader {
	return &BorReceiptsReader{
		tx:        tx,
		composite: make([]byte, 20+32),
	}
}

func (r *BorReceiptsReader) ReadReceipt(blockNum uint64) (*types.Receipt, bool, error) {
	blockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumBytes, blockNum)

	receiptBytes, _, err := r.tx.DomainGet(kv.BorReceiptDomain, blockNumBytes[:], nil)
	if err != nil {
		return nil, false, err
	}
	if len(receiptBytes) == 0 {
		return nil, false, nil
	}

	var receipt types.Receipt
	err = json.Unmarshal(receiptBytes, &receipt)

	return &receipt, true, nil
}
