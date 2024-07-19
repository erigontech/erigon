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
	"sort"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
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

type txNums struct{}

var TxNums txNums

// Min - returns maxTxNum in given block. If block not found - return last available value (`latest`/`pending` state)
func (txNums) Max(tx kv.Tx, blockNum uint64) (maxTxNum uint64, err error) {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], blockNum)
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	_, v, err := c.SeekExact(k[:])
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		_, v, err = c.Last()
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, nil
		}
	}
	return binary.BigEndian.Uint64(v), nil
}

// Min = `max(blockNum-1)+1` returns minTxNum in given block. If block not found - return last available value (`latest`/`pending` state)
func (txNums) Min(tx kv.Tx, blockNum uint64) (maxTxNum uint64, err error) {
	if blockNum == 0 {
		return 0, nil
	}
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], blockNum-1)
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	_, v, err := c.SeekExact(k[:])
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		_, v, err = c.Last()
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, nil
		}
	}
	return binary.BigEndian.Uint64(v) + 1, nil
}

func (txNums) Append(tx kv.RwTx, blockNum, maxTxNum uint64) (err error) {
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
func (txNums) WriteForGenesis(tx kv.RwTx, maxTxNum uint64) (err error) {
	var k, v [8]byte
	binary.BigEndian.PutUint64(k[:], 0)
	binary.BigEndian.PutUint64(v[:], maxTxNum)
	return tx.Put(kv.MaxTxNum, k[:], v[:])
}
func (txNums) Truncate(tx kv.RwTx, blockNum uint64) (err error) {
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], blockNum)
	c, err := tx.RwCursor(kv.MaxTxNum)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(seek[:]); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		if err = tx.Delete(kv.MaxTxNum, k); err != nil {
			return err
		}
		//if err = c.DeleteCurrent(); err != nil {
		//	return err
		//}
	}
	return nil
}
func (txNums) FindBlockNum(tx kv.Tx, endTxNumMinimax uint64) (ok bool, blockNum uint64, err error) {
	var seek [8]byte
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return false, 0, err
	}
	defer c.Close()

	lastK, _, err := c.Last()
	if err != nil {
		return false, 0, err
	}
	if lastK == nil {
		return false, 0, nil
	}
	if len(lastK) != 8 {
		return false, 0, fmt.Errorf("seems broken TxNum value: %x", lastK)
	}
	lastBlockNum := binary.BigEndian.Uint64(lastK)

	blockNum = uint64(sort.Search(int(lastBlockNum+1), func(i int) bool {
		if err != nil { // don't loose errors from prev iterations
			return true
		}

		binary.BigEndian.PutUint64(seek[:], uint64(i))
		var v, found []byte
		found, v, err = c.SeekExact(seek[:])
		if err != nil {
			return true
		}
		if len(v) != 8 {
			_lb, _lt, _ := TxNums.Last(tx)
			err = fmt.Errorf("FindBlockNum(%d): seems broken TxNum value: %x -> (%x, %x); last in db: (%d, %d)", endTxNumMinimax, seek, found, v, _lb, _lt)
			return true
		}
		return binary.BigEndian.Uint64(v) >= endTxNumMinimax
	}))
	if err != nil {
		return false, 0, err
	}
	if blockNum > lastBlockNum {
		return false, 0, nil
	}
	return true, blockNum, nil
}
func (txNums) Last(tx kv.Tx) (blockNum, txNum uint64, err error) {
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	lastK, lastV, err := c.Last()
	if err != nil {
		return 0, 0, err
	}
	if lastK == nil || lastV == nil {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint64(lastK), binary.BigEndian.Uint64(lastV), nil
}
func (txNums) First(tx kv.Tx) (blockNum, txNum uint64, err error) {
	c, err := tx.Cursor(kv.MaxTxNum)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()

	lastK, lastV, err := c.First()
	if err != nil {
		return 0, 0, err
	}
	if lastK == nil || lastV == nil {
		return 0, 0, nil
	}
	return binary.BigEndian.Uint64(lastK), binary.BigEndian.Uint64(lastV), nil
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
	_, _, err = c.First()
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
}

func TxNums2BlockNums(tx kv.Tx, it stream.U64, by order.By) *MapTxNum2BlockNumIter {
	return &MapTxNum2BlockNumIter{tx: tx, it: it, orderAscend: bool(by)}
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
		ok, i.blockNum, err = TxNums.FindBlockNum(i.tx, txNum)
		if err != nil {
			return
		}
		if !ok {
			return txNum, i.blockNum, txIndex, isFinalTxn, blockNumChanged, fmt.Errorf("can't find blockNumber by txnID=%d", txNum)
		}
	}
	blockNum = i.blockNum

	// if block number changed, calculate all related field
	if blockNumChanged {
		i.minTxNumInBlock, err = TxNums.Min(i.tx, blockNum)
		if err != nil {
			return
		}
		i.maxTxNumInBlock, err = TxNums.Max(i.tx, blockNum)
		if err != nil {
			return
		}
	}

	txIndex = int(txNum) - int(i.minTxNumInBlock) - 1
	isFinalTxn = txNum == i.maxTxNumInBlock
	return
}
