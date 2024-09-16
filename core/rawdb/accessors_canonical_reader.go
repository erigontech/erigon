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
	"github.com/erigontech/erigon/core/types"
)

type CanonicalTxnIds struct {
	canonicalMarkers stream.KV
	tx               kv.Tx

	// input params
	fromTxNum, toTxNum int
	orderAscend        order.By
	limit              int

	// private fields
	fromTxnID, toTxnID kv.TxnId
	currentTxnID       kv.TxnId

	//current block info
	currentBody       *types.BodyForStorage
	endOfCurrentBlock kv.TxnId

	txNumsReader rawdbv3.TxNumsReader
	r            *CanonicalReader
}
type CanonicalReader struct {
	txNumsReader rawdbv3.TxNumsReader
}

func NewCanonicalReader(txNumsReader rawdbv3.TxNumsReader) *CanonicalReader {
	return &CanonicalReader{txNumsReader: txNumsReader}
}

// TxNum2ID - returns non-canonical txnIds of canonical block range
// [fromTxNum, toTxNum)
// To get all canonical blocks, use fromTxNum=0, toTxNum=-1
// For reverse iteration use order.Desc and fromTxNum=-1, toTxNum=-1
func (c *CanonicalReader) TxnIdsOfCanonicalBlocks(tx kv.Tx, fromTxNum, toTxNum int, asc order.By, limit int) (stream.U64, error) {
	if asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum >= toTxNum {
		return nil, fmt.Errorf("fromTxNum >= toTxNum: %d, %d", fromTxNum, toTxNum)
	}
	if !asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum <= toTxNum {
		return nil, fmt.Errorf("fromTxNum <= toTxNum: %d, %d", fromTxNum, toTxNum)
	}

	it := &CanonicalTxnIds{r: c, tx: tx, txNumsReader: c.txNumsReader, fromTxNum: fromTxNum, toTxNum: toTxNum, orderAscend: asc, limit: limit}
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

func (c *CanonicalReader) TxNum2ID(tx kv.Tx, txNum uint64) (blockNum uint64, txnID kv.TxnId, ok bool, err error) {
	ok, blockNum, err = c.txNumsReader.FindBlockNum(tx, txNum)
	if err != nil {
		return 0, 0, false, err
	}
	if !ok {
		return 0, 0, false, nil
	}

	_minTxNum, err := c.txNumsReader.Min(tx, blockNum)
	if err != nil {
		return 0, 0, false, err
	}
	var offset uint64
	if txNum > _minTxNum {
		offset = txNum - _minTxNum
	}

	blockHash, err := tx.GetOne(kv.HeaderCanonical, hexutility.EncodeTs(blockNum))
	if err != nil {
		return 0, 0, false, err
	}
	body, err := readBodyForStorage(tx, common2.BytesToHash(blockHash), blockNum)
	if err != nil {
		return 0, 0, false, err
	}
	if body == nil { // data frozen: TxNum == TxnID
		return blockNum, kv.TxnId(txNum), true, nil
	}
	return blockNum, kv.TxnId(body.BaseTxnID) + kv.TxnId(offset), true, nil
}

func (c *CanonicalReader) BaseTxnID(tx kv.Tx, blockNum uint64, blockHash common2.Hash) (kv.TxnId, error) {
	if blockNum == 0 {
		return kv.TxnId(0), nil
	}

	//TODO: what if body is in db and files?
	b, err := readBodyForStorage(tx, blockHash, blockNum)
	if err != nil {
		return 0, err
	}
	if b == nil { // freezed and pruned
		_min, err := c.txNumsReader.Min(tx, blockNum)
		if err != nil {
			return 0, err
		}
		return kv.TxnId(_min), nil
	}
	return kv.TxnId(b.BaseTxnID + 1), nil

}

func (c *CanonicalReader) LastFrozenTxNum(tx kv.Tx) (kv.TxnId, error) {
	n, ok, err := ReadFirstNonGenesisHeaderNumber(tx)
	if err != nil {
		return 0, err
	}
	if !ok {
		//seq, err := tx.ReadSequence(kv.EthTx)
		//seq-1
		_, _lastTxNumInFiles, err := c.txNumsReader.Last(tx)
		return kv.TxnId(_lastTxNumInFiles), err

	}
	_max, err := c.txNumsReader.Max(tx, n)
	if err != nil {
		return 0, err
	}
	return kv.TxnId(_max), nil
}

func (s *CanonicalTxnIds) init() (err error) {
	var fromBlockNumBytes, toBlockNumBytes []byte
	if s.fromTxNum >= 0 {
		var fromBlockNum uint64
		var ok bool
		//ok, blockFrom, err := s.txNumsReader.FindBlockNum(tx, uint64(s.fromTxNum))
		fromBlockNum, s.fromTxnID, ok, err = s.r.TxNum2ID(s.tx, uint64(s.fromTxNum))
		if err != nil {
			return err
		}
		if ok {
			fromBlockNumBytes = hexutility.EncodeTs(fromBlockNum)
		}
		//fmt.Printf("[dbg] fromBlockNum %d\n", fromBlockNum)
	}

	if s.toTxNum >= 0 {
		var blockTo uint64
		var ok bool
		//ok, blockTo, err := s.txNumsReader.FindBlockNum(tx, uint64(s.toTxNum))
		blockTo, s.toTxnID, ok, err = s.r.TxNum2ID(s.tx, uint64(s.toTxNum))
		if err != nil {
			return err
		}
		if ok {
			toBlockNumBytes = hexutility.EncodeTs(blockTo + 1) // [from, to)
		}
		//fmt.Printf("[dbg] fromBlockNum %d\n", blockTo)
	}

	if s.orderAscend {
		s.canonicalMarkers, err = s.tx.RangeAscend(kv.HeaderCanonical, fromBlockNumBytes, toBlockNumBytes, -1)
		if err != nil {
			return err
		}
	} else {
		s.canonicalMarkers, err = s.tx.RangeDescend(kv.HeaderCanonical, fromBlockNumBytes, toBlockNumBytes, -1)
		if err != nil {
			return err
		}
	}

	s.currentTxnID = s.fromTxnID

	if !s.canonicalMarkers.HasNext() {
		return nil
	}
	if err := s.advanceBlockNum(); err != nil {
		return err
	}

	return nil
}

func (s *CanonicalTxnIds) advanceBlockNum() (err error) {
	k, v, err := s.canonicalMarkers.Next()
	if err != nil {
		return err
	}
	blockNum := binary.BigEndian.Uint64(k)
	blockHash := common2.BytesToHash(v)
	s.currentBody, err = readBodyForStorage(s.tx, blockHash, blockNum)
	if err != nil {
		return err
	}
	if s.currentBody == nil { // TxnID is equal to TxNum
		if s.orderAscend {
			_maxTxNum, err := rawdbv3.TxNums.Max(s.tx, blockNum)
			if err != nil {
				return err
			}
			s.endOfCurrentBlock = kv.TxnId(_maxTxNum)
		} else {
			_minTxNum, err := rawdbv3.TxNums.Min(s.tx, blockNum)
			if err != nil {
				return err
			}
			s.endOfCurrentBlock = kv.TxnId(_minTxNum)
		}
		return nil
	}

	if s.orderAscend {
		s.endOfCurrentBlock = kv.TxnId(s.currentBody.BaseTxnID.LastSystemTx(s.currentBody.TxCount))
	} else {
		s.endOfCurrentBlock = kv.TxnId(s.currentBody.BaseTxnID)
	}
	return nil
}

func (s *CanonicalTxnIds) advance() (err error) {
	var blockIsOver bool

	if s.orderAscend {
		s.currentTxnID++
		blockIsOver = s.currentTxnID > s.endOfCurrentBlock
	} else {
		if s.currentTxnID == 0 {
			s.currentBody = nil
			return nil
		}
		s.currentTxnID--
		blockIsOver = s.currentTxnID < s.endOfCurrentBlock
	}
	if !blockIsOver {
		return nil
	}

	if !s.canonicalMarkers.HasNext() {
		s.currentBody = nil
		return nil
	}
	if err := s.advanceBlockNum(); err != nil {
		return err
	}
	if s.currentBody != nil {
		if s.orderAscend {
			s.currentTxnID = kv.TxnId(s.currentBody.BaseTxnID)
		} else {
			s.currentTxnID = kv.TxnId(s.currentBody.BaseTxnID.LastSystemTx(s.currentBody.TxCount))
		}
	}
	return nil
}

func (s *CanonicalTxnIds) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if bool(s.orderAscend) && s.currentTxnID > s.endOfCurrentBlock && !s.canonicalMarkers.HasNext() { // EndOfTable
		return false
	}
	if !bool(s.orderAscend) && s.currentTxnID < s.endOfCurrentBlock && !s.canonicalMarkers.HasNext() { // EndOfTable
		return false
	}
	if s.toTxNum < 0 { //no boundaries
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	return (bool(s.orderAscend) && s.currentTxnID < s.toTxnID) ||
		(!bool(s.orderAscend) && s.currentTxnID > s.toTxnID)
}

func (s *CanonicalTxnIds) Next() (uint64, error) {
	s.limit--
	v := uint64(s.currentTxnID)
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
