// Copyright 2022 The Erigon Authors
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

package historyv2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/erigon-lib/common/length"
	"github.com/erigontech/erigon/erigon-lib/kv"
)

var (
	ErrNotFound = errors.New("not found")
)

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  length.Addr + length.Hash + length.Incarnation,
	}
}

func EncodeStorage(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	sort.Sort(s)
	keyPart := length.Addr + length.Incarnation
	for _, cs := range s.Changes {
		newK := make([]byte, length.BlockNum+keyPart)
		binary.BigEndian.PutUint64(newK, blockN)
		copy(newK[8:], cs.Key[:keyPart])
		newV := make([]byte, 0, length.Hash+len(cs.Value))
		newV = append(append(newV, cs.Key[keyPart:]...), cs.Value...)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}

func DecodeStorage(dbKey, dbValue []byte) (uint64, []byte, []byte, error) {
	blockN := binary.BigEndian.Uint64(dbKey)
	if len(dbValue) < length.Hash {
		return 0, nil, nil, fmt.Errorf("storage changes purged for block %d", blockN)
	}
	k := make([]byte, length.Addr+length.Incarnation+length.Hash)
	dbKey = dbKey[length.BlockNum:] // remove BlockN bytes
	copy(k, dbKey)
	copy(k[len(dbKey):], dbValue[:length.Hash])
	v := dbValue[length.Hash:]
	if len(v) == 0 {
		v = nil
	}

	return blockN, k, v, nil
}

func FindStorage(c kv.CursorDupSort, blockNumber uint64, k []byte) ([]byte, error) {
	addWithInc, loc := k[:length.Addr+length.Incarnation], k[length.Addr+length.Incarnation:]
	seek := make([]byte, length.BlockNum+length.Addr+length.Incarnation)
	binary.BigEndian.PutUint64(seek, blockNumber)
	copy(seek[8:], addWithInc)
	v, err := c.SeekBothRange(seek, loc)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(v, loc) {
		return nil, ErrNotFound
	}
	return v[length.Hash:], nil
}
