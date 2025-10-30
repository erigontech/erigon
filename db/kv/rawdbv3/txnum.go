// Copyright 2021 The Erigon Authors
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

package rawdbv3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

type ErrTxNumsAppendWithGap struct {
	appendBlockNum uint64
	lastBlockNum   uint64
	stack          string
}

func (e ErrTxNumsAppendWithGap) LastBlock() uint64 {
	return e.lastBlockNum
}

func (e ErrTxNumsAppendWithGap) Error() string {
	return fmt.Sprintf(
		"append with gap blockNum=%d, but current height=%d, stack: %s",
		e.appendBlockNum, e.lastBlockNum, e.stack,
	)
}

func (e ErrTxNumsAppendWithGap) Is(err error) bool {
	var target ErrTxNumsAppendWithGap
	return errors.As(err, &target)
}

type TxBlockIndex interface {
	MaxTxNum(tx kv.Tx, c kv.Cursor, blockNum uint64) (maxTxNum uint64, ok bool, err error)
	BlockNumber(tx kv.Tx, txNum uint64) (blockNum uint64, ok bool, err error)
}

type TxNumsReader struct {
	index TxBlockIndex
}

// thread safe
type DefaultTxBlockIndex struct{}

func (d *DefaultTxBlockIndex) MaxTxNum(tx kv.Tx, c kv.Cursor, blockNum uint64) (maxTxNum uint64, ok bool, err error) {
	if c == nil {
		c, err := tx.Cursor(kv.MaxTxNum)
		if err != nil {
			return 0, false, err
		}
		defer c.Close()
	}
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	_, v, err := c.SeekExact(k[:])
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	if len(v) != 8 {
		return 0, false, fmt.Errorf("DefaultReadTxNumFunc: seems broken TxNum value: %x", v)
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func (d *DefaultTxBlockIndex) BlockNumber(tx kv.Tx, txNum uint64) (blockNum uint64, ok bool, err error) {
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, false, err
	}
	defer c.Close()

	_blk, v, err := c.Last()
	if err != nil {
		return 0, false, err
	}
	if _blk == nil {
		return 0, false, nil
	}
	if len(_blk) != 8 {
		return 0, false, fmt.Errorf("DefaultReadTxNumFunc: seems broken TxNum value: %x", _blk)
	}

	lastBlockNum := binary.BigEndian.Uint64(_blk)
	txNumInDb := binary.BigEndian.Uint64(v)

	if txNumInDb < txNum {
		return 0, false, nil
	}

	if lastBlockNum > uint64(math.MaxInt32) {
		panic(fmt.Sprintf("block number is too big: %d", lastBlockNum))
	}

	{
		// check genesis block
		firstMaxTxNum, ok, err := d.MaxTxNum(tx, c, 0)
		if err != nil {
			return 0, false, fmt.Errorf("DefaultReadTxNumFunc first maxtxnum error: %w", err)
		}
		if ok {
			if txNum <= firstMaxTxNum {
				return 0, true, nil
			}
		}
	}

	_blk, err = SecondKeyC(c) // first key is always genesis; can be found in blocks...
	if err != nil {
		return 0, false, err
	}
	if len(_blk) != 8 {
		return 0, false, fmt.Errorf("DefaultReadTxNumFunc Second: seems broken TxNum value: %x", _blk)
	}
	secondBlockNum := binary.BigEndian.Uint64(_blk)

	blockNum = uint64(sort.Search(int(lastBlockNum+1), func(sblk int) bool {
		if err != nil {
			return true
		}
		if secondBlockNum > uint64(sblk) {
			return false
		}
		var maxTxNum uint64
		maxTxNum, ok, err = d.MaxTxNum(tx, c, uint64(sblk))
		if err != nil {
			return true
		}

		if !ok {
			_fb, _ft, _ := c.First()
			_lb, _lt, _ := c.Last()
			fb := binary.BigEndian.Uint64(_fb)
			lt := binary.BigEndian.Uint64(_lt)
			ft := binary.BigEndian.Uint64(_ft)
			lb := binary.BigEndian.Uint64(_lb)
			err = fmt.Errorf("BlockNum(%d): seems broken TxNum value: %d -> %d; db has: (%d-%d, %d-%d)", sblk, txNum, maxTxNum, fb, ft, lb, lt)
			return true
		}
		return maxTxNum >= txNum
	}))
	if err != nil {
		return 0, false, err
	}
	return blockNum, true, nil
}

// DefaultTxNums - default implementation of TxNums
var TxNums TxNumsReader = TxNumsReader{
	index: DefaultTxBlockIndexInstance,
}
var DefaultTxBlockIndexInstance = &DefaultTxBlockIndex{}

func (TxNumsReader) WithCustomReadTxNumFunc(f TxBlockIndex) TxNumsReader {
	return TxNumsReader{index: f}
}

// Max - returns maxTxNum in given block. If block not found - return last available value (`latest`/`pending` state)
func (t TxNumsReader) Max(tx kv.Tx, blockNum uint64) (maxTxNum uint64, err error) {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	maxTxNum, ok, err := t.index.MaxTxNum(tx, c, blockNum)
	if err != nil {
		return 0, err
	}

	if !ok {
		_, v, err := c.Last()
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, nil
		}
		return binary.BigEndian.Uint64(v), nil
	}
	return maxTxNum, nil
}

// Min = `max(blockNum-1)+1` returns minTxNum in given block. If block not found - return last available value (`latest`/`pending` state)
func (t TxNumsReader) Min(tx kv.Tx, blockNum uint64) (minTxNum uint64, err error) {
	if blockNum == 0 {
		return 0, nil
	}
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	minTxNum, ok, err := t.index.MaxTxNum(tx, c, blockNum-1)
	if err != nil {
		return 0, err
	}

	if !ok {
		_, v, err := c.Last()
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, nil
		}
		return binary.BigEndian.Uint64(v) + 1, nil
	}
	return minTxNum + 1, nil
}

func (t TxNumsReader) Append(tx kv.RwTx, blockNum, maxTxNum uint64) (err error) {
	lastK, err := LastKey(tx, kv.MaxTxNum)
	if err != nil {
		return err
	}
	if len(lastK) != 0 {
		lastBlockNum := binary.BigEndian.Uint64(lastK)
		if lastBlockNum > 1 && lastBlockNum+1 != blockNum { //allow genesis
			return ErrTxNumsAppendWithGap{appendBlockNum: blockNum, lastBlockNum: lastBlockNum, stack: dbg.Stack()}
		}
	}

	var k, v [8]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	binary.BigEndian.PutUint64(v[:], maxTxNum)
	if err := tx.Append(kv.MaxTxNum, k[:], v[:]); err != nil {
		return err
	}
	return nil
}

func (TxNumsReader) Truncate(tx kv.RwTx, blockNum uint64) (err error) {
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], blockNum)
	c, err := tx.RwCursor(kv.MaxTxNum)
	if err != nil {
		return err
	}
	defer c.Close()
	prevBlockNum := blockNum
	for k, _, err := c.Seek(seek[:]); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		currentBlockNum := binary.BigEndian.Uint64(k)
		if currentBlockNum != prevBlockNum+1 /*no gaps, only growing*/ &&
			currentBlockNum != blockNum /*to prevent first item error*/ {
			return fmt.Errorf("bad block num: current num is %d but previous is %d", currentBlockNum, prevBlockNum)
		}
		if err = tx.Delete(kv.MaxTxNum, k); err != nil {
			return err
		}
		prevBlockNum = currentBlockNum
		//if err = c.DeleteCurrent(); err != nil {
		//	return err
		//}
	}
	return nil
}
func (t TxNumsReader) FindBlockNum(tx kv.Tx, endTxNumMinimax uint64) (blockNum uint64, ok bool, err error) {
	return t.index.BlockNumber(tx, endTxNumMinimax)
}

func (TxNumsReader) Last(tx kv.Tx) (blockNum, txNum uint64, err error) {
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	k, v, err := c.Last()
	if err != nil {
		return 0, 0, err
	}
	if k == nil || v == nil {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v), nil
}
func (TxNumsReader) First(tx kv.Tx) (blockNum, txNum uint64, err error) {
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	k, v, err := c.First()
	if err != nil {
		return 0, 0, err
	}
	if k == nil || v == nil {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v), nil
}

// LastKey
func LastKey(tx kv.Tx, table string) ([]byte, error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return nil, err
	}
	return k, nil
}

// Last - candidate on move to kv.Tx interface
func Last(tx kv.Tx, table string) ([]byte, []byte, error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	k, v, err := c.Last()
	if err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

// SecondKey - useful if table always has zero-key (for example genesis block)
func SecondKey(tx kv.Tx, table string) ([]byte, error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return SecondKeyC(c)
}

func SecondKeyC(c kv.Cursor) ([]byte, error) {
	_, _, err := c.First()
	if err != nil {
		return nil, err
	}
	k, _, err := c.Next()
	if err != nil {
		return nil, err
	}
	return k, nil
}

// MapTxNum2BlockNumIter - enrich iterator by TxNumbers, adding more info:
//   - blockNum
//   - txIndex in block: -1 means first system tx
//   - isFinalTxn: last system-txn. BlockRewards and similar things - are attribute to this virtual txn.
//   - blockNumChanged: means this and previous txNum belongs to different blockNumbers
//
// Expect: `it` to return sorted txNums, then blockNum will not change until `it.Next() < maxTxNumInBlock`
//
//	it allow certain optimizations.
type MapTxNum2BlockNumIter struct {
	it          stream.U64
	tx          kv.Tx
	orderAscend bool

	blockNum                         uint64
	minTxNumInBlock, maxTxNumInBlock uint64

	txNumsReader TxNumsReader
}

func TxNums2BlockNums(tx kv.Tx, txNumsReader TxNumsReader, it stream.U64, by order.By) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, txNumsReader: txNumsReader, it: it, orderAscend: bool(by)}
}
func (i *MapTxNum2BlockNumIter) Close() {
	if i.it != nil {
		i.it.Close()
		i.it = nil
	}
}
func (i *MapTxNum2BlockNumIter) HasNext() bool { return i.it.HasNext() }
func (i *MapTxNum2BlockNumIter) Next() (txNum, blockNum uint64, txIndex int, isFinalTxn, blockNumChanged bool, err error) {
	txNum, err = i.it.Next()
	if err != nil {
		return txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err
	}

	// txNums are sorted, it means blockNum will not change until `txNum < maxTxNumInBlock`
	if i.maxTxNumInBlock == 0 || (i.orderAscend && txNum > i.maxTxNumInBlock) || (!i.orderAscend && txNum < i.minTxNumInBlock) {
		blockNumChanged = true

		var ok bool
		i.blockNum, ok, err = i.txNumsReader.FindBlockNum(i.tx, txNum)
		if err != nil {
			return
		}
		if !ok {
			_lb, _lt, _ := i.txNumsReader.Last(i.tx)
			_fb, _ft, _ := i.txNumsReader.First(i.tx)
			return txNum, i.blockNum, txIndex, isFinalTxn, blockNumChanged, fmt.Errorf("can't find blockNumber by txnID=%d; last in db: (%d-%d, %d-%d)", txNum, _fb, _lb, _ft, _lt)
		}
	}
	blockNum = i.blockNum

	// if block number changed, calculate all related field
	if blockNumChanged {
		i.minTxNumInBlock, err = i.txNumsReader.Min(i.tx, blockNum)
		if err != nil {
			return
		}
		i.maxTxNumInBlock, err = i.txNumsReader.Max(i.tx, blockNum)
		if err != nil {
			return
		}
	}

	txIndex = int(txNum) - int(i.minTxNumInBlock) - 1
	if i.orderAscend {
		isFinalTxn = txNum == i.maxTxNumInBlock
	} else {
		isFinalTxn = txNum == i.minTxNumInBlock
	}
	return
}
