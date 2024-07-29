// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"encoding/binary"
	"fmt"

	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
)

type CanonicalTxnIds struct {
	canonicalMarkers stream.KV
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
type CanonicalReader struct {
}

func NewCanonicalReader() *CanonicalReader {
	return &CanonicalReader{}
}
func (*CanonicalReader) TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (stream.U64, error) {
	return TxnIdsOfCanonicalBlocks(tx, fromTxNum, toTxNum, asc, limit)
}
func (*CanonicalReader) TxNum2ID(tx kv.Tx, blockNum uint64, blockHash common2.Hash, txNum uint64) (kv.TxnId, error) {
	if blockNum == 0 {
		return kv.TxnId(txNum), nil
	}
	b, err := readBodyForStorage(tx, blockHash, blockNum)
	if err != nil {
		return 0, err
	}
	if b == nil { // freezed and pruned
		_min, err := rawdbv3.TxNums.Min(tx, blockNum)
		if err != nil {
			return 0, err
		}
		_max, err := rawdbv3.TxNums.Max(tx, blockNum)
		if err != nil {
			return 0, err
		}
		if txNum < _min || txNum > _max {
			return 0, fmt.Errorf("TxNum2ID: txNum=%d out of range: %d, %d", txNum, _min, _max)
		}
		return kv.TxnId(txNum), nil
	}
	return kv.TxnId(b.BaseTxnID), nil
}

func (*CanonicalReader) BaseTxnID(tx kv.Tx, blockNum uint64, blockHash common2.Hash) (kv.TxnId, error) {
	if blockNum == 0 {
		return kv.TxnId(0), nil
	}

	//TODO: what if body is in db and files?
	b, err := readBodyForStorage(tx, blockHash, blockNum)
	if err != nil {
		return 0, err
	}
	if b == nil { // freezed and pruned
		_min, err := rawdbv3.TxNums.Min(tx, blockNum)
		if err != nil {
			return 0, err
		}
		return kv.TxnId(_min), nil
	}
	return kv.TxnId(b.BaseTxnID), nil
}

func (*CanonicalReader) LastFrozenTxNum(tx kv.Tx) (kv.TxnId, error) {
	n, ok, err := ReadFirstNonGenesisHeaderNumber(tx)
	if err != nil {
		return 0, err
	}
	if !ok {
		//seq, err := tx.ReadSequence(kv.EthTx)
		//seq-1
		_, _lastTxNumInFiles, err := rawdbv3.TxNums.Last(tx)
		return kv.TxnId(_lastTxNumInFiles), err

	}
	_max, err := rawdbv3.TxNums.Max(tx, n)
	if err != nil {
		return 0, err
	}
	return kv.TxnId(_max), nil
}

// TxnIdsOfCanonicalBlocks - returns non-canonical txnIds of canonical block range
// [fromTxNum, toTxNum)
// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
func TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (stream.U64, error) {
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
		return stream.EmptyU64, nil
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

	if !endOfBlock || s.currentTxNum == int(s.endOfCurrentBlock) {
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
		s.currentTxNum = int(body.BaseTxnID)
		s.endOfCurrentBlock = body.BaseTxnID.LastSystemTx(body.TxCount)
	} else {
		s.currentTxNum = int(body.BaseTxnID.LastSystemTx(body.TxCount))
		s.endOfCurrentBlock = body.BaseTxnID.U64()
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
