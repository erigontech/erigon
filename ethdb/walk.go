// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/changeset"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

// splitCursor implements cursor with two keys
// it is used to ignore incarnations in the middle
// of composite storage key, but without
// reconstructing the key
// Instead, the key is split into two parts and
// functions `Seek` and `Next` deliver both
// parts as well as the corresponding value
type splitCursor struct {
	c          Cursor // Unlerlying cursor
	startkey   []byte // Starting key (also contains bits that need to be preserved)
	matchBytes int
	mask       uint8
	part1end   int // Position in the key where the first part ends
	part2start int // Position in the key where the second part starts
	part3start int // Position in the key where the third part starts
}

func NewSplitCursor(c Cursor, startkey []byte, matchBits int, part1end, part2start, part3start int) *splitCursor {
	var sc splitCursor
	sc.c = c
	sc.startkey = startkey
	sc.part1end = part1end
	sc.part2start = part2start
	sc.part3start = part3start
	sc.matchBytes, sc.mask = Bytesmask(matchBits)
	return &sc
}

func (sc *splitCursor) matchKey(k []byte) bool {
	if k == nil {
		return false
	}
	if sc.matchBytes == 0 {
		return true
	}
	if len(k) < sc.matchBytes {
		return false
	}
	if !bytes.Equal(k[:sc.matchBytes-1], sc.startkey[:sc.matchBytes-1]) {
		return false
	}
	return (k[sc.matchBytes-1] & sc.mask) == (sc.startkey[sc.matchBytes-1] & sc.mask)
}

func (sc *splitCursor) Seek() (key1, key2, key3, val []byte, err error) {
	k, v, err1 := sc.c.Seek(sc.startkey)
	if err1 != nil {
		return nil, nil, nil, nil, err1
	}
	if !sc.matchKey(k) {
		return nil, nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v, nil
}

func (sc *splitCursor) Next() (key1, key2, key3, val []byte, err error) {
	k, v, err1 := sc.c.Next()
	if err1 != nil {
		return nil, nil, nil, nil, err1
	}
	if !sc.matchKey(k) {
		return nil, nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v, nil
}

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// GetModifiedAccounts returns a list of addresses that were modified in the block range
func GetModifiedAccounts(tx Tx, startNum, endNum uint64) ([]common.Address, error) {

	changedAddrs := make(map[common.Address]struct{})
	startCode := dbutils.EncodeTimestamp(startNum)

	c := tx.Cursor(dbutils.PlainAccountChangeSetBucket)
	defer c.Close()

	for k, v, err := c.Seek(startCode); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, fmt.Errorf("iterating over account changeset for %v: %w", k, err)
		}
		currentNum, _ := dbutils.DecodeTimestamp(k)
		if currentNum > endNum {
			break
		}

		walker := func(addr, _ []byte) error {
			changedAddrs[common.BytesToAddress(addr)] = struct{}{}
			return nil
		}
		if err := changeset.AccountChangeSetPlainBytes(v).Walk(walker); err != nil {
			return nil, fmt.Errorf("iterating over account changeset for %v: %w", k, err)
		}
	}

	if len(changedAddrs) == 0 {
		return nil, nil
	}

	idx := 0
	result := make([]common.Address, len(changedAddrs))
	for addr := range changedAddrs {
		copy(result[idx][:], addr[:])
		idx++
	}

	return result, nil
}
