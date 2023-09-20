/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package rawdbv3

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/kv"
)

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
			return fmt.Errorf("append with gap blockNum=%d, but current heigh=%d", blockNum, lastBlockNum)
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
		if err = c.DeleteCurrent(); err != nil {
			return err
		}

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

	cnt, err := c.Count()
	if err != nil {
		return false, 0, err
	}

	blockNum = uint64(sort.Search(int(cnt), func(i int) bool {
		binary.BigEndian.PutUint64(seek[:], uint64(i))
		var v []byte
		_, v, err = c.SeekExact(seek[:])
		return binary.BigEndian.Uint64(v) >= endTxNumMinimax
	}))
	if err != nil {
		return false, 0, err
	}
	if blockNum == cnt {
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
