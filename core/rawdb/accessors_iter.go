package rawdb

import (
	"encoding/binary"
	"fmt"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
)

type CanonicalTxnIds struct {
	canonicalMarkers iter.KV
	tx               kv.Tx

	// input params
	fromTxNum, toTxNum int
	orderAscend        order.By
	limit              int

	// private fields
	currentTxNum      int
	hasNext           bool
	endOfCurrentBlock uint64
}

// TxnIdsOfCanonicalBlocks - returns non-canonical txnIds of canonical block range
// [fromTxNum, toTxNum)
// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
func TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (iter.U64, error) {
	if asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum >= toTxNum {
		return nil, fmt.Errorf("fromTxNum >= toTxNum: %d, %d", fromTxNum, toTxNum)
	}
	if !asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum <= toTxNum {
		return nil, fmt.Errorf("fromTxNum <= toTxNum: %d, %d", fromTxNum, toTxNum)
	}

	it := &CanonicalTxnIds{tx: tx, fromTxNum: fromTxNum, toTxNum: toTxNum, orderAscend: asc, limit: limit, currentTxNum: -1}
	if err := it.init(); err != nil {
		it.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	if !it.HasNext() {
		it.Close()
		return iter.EmptyU64, nil
	}
	return it, nil
}

func (s *CanonicalTxnIds) init() (err error) {
	tx := s.tx
	var from, to []byte
	if s.fromTxNum >= 0 {
		ok, blockFrom, err := rawdbv3.TxNums.FindBlockNum(tx, uint64(s.fromTxNum))
		if err != nil {
			return err
		}
		if ok {
			from = hexutility.EncodeTs(blockFrom)
		}
	}

	if s.toTxNum >= 0 {
		ok, blockTo, err := rawdbv3.TxNums.FindBlockNum(tx, uint64(s.toTxNum))
		if err != nil {
			return err
		}
		if ok {
			to = hexutility.EncodeTs(blockTo + 1)
		}
	}

	if s.orderAscend {
		s.canonicalMarkers, err = tx.RangeAscend(kv.HeaderCanonical, from, to, -1)
		if err != nil {
			return err
		}
	} else {
		s.canonicalMarkers, err = tx.RangeDescend(kv.HeaderCanonical, from, to, -1)
		if err != nil {
			return err
		}
	}
	if err := s.advance(); err != nil {
		return err
	}
	return nil
}

func (s *CanonicalTxnIds) advance() (err error) {
	var endOfBlock bool
	if s.currentTxNum < 0 {
		endOfBlock = true
	} else {
		if s.orderAscend {
			s.currentTxNum++
			endOfBlock = s.currentTxNum >= int(s.endOfCurrentBlock)
		} else {
			s.currentTxNum--
			endOfBlock = s.currentTxNum <= int(s.endOfCurrentBlock)
		}
	}

	if !endOfBlock {
		return nil
	}

	if !s.canonicalMarkers.HasNext() {
		s.currentTxNum = -1
		return nil
	}

	k, v, err := s.canonicalMarkers.Next()
	if err != nil {
		return err
	}
	blockNum := binary.BigEndian.Uint64(k)
	blockHash := common2.BytesToHash(v)
	body, err := readBodyForStorage(s.tx, blockHash, blockNum)
	if err != nil {
		return err
	}
	if body == nil {
		return fmt.Errorf("body not found: %d, %x", blockNum, blockHash)
	}

	if s.orderAscend {
		s.currentTxNum = int(body.BaseTxId)
		s.endOfCurrentBlock = body.BaseTxId + uint64(body.TxAmount) // 2 system txs already included in TxAmount
	} else {
		s.currentTxNum = int(body.BaseTxId) + int(body.TxAmount) - 1 // 2 system txs already included in TxAmount
		s.endOfCurrentBlock = body.BaseTxId - 1                      // and one of them is baseTxId
	}
	return nil
}

func (s *CanonicalTxnIds) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.currentTxNum < 0 { // EndOfTable
		return false
	}
	if s.toTxNum < 0 { //no boundaries
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	return (bool(s.orderAscend) && s.currentTxNum < s.toTxNum) ||
		(!bool(s.orderAscend) && s.currentTxNum > s.toTxNum)
}

func (s *CanonicalTxnIds) Next() (uint64, error) {
	s.limit--
	v := uint64(s.currentTxNum)
	if err := s.advance(); err != nil {
		return 0, err
	}
	return v, nil
}

func (s *CanonicalTxnIds) Close() {
	if s == nil {
		return
	}
	if s.canonicalMarkers != nil {
		s.canonicalMarkers.Close()
		s.canonicalMarkers = nil
	}
}
