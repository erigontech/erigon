/*
   Copyright 2022 Erigon contributors

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

package historyv2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type Encoder func(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error
type Decoder func(dbKey, dbValue []byte) (blockN uint64, k, v []byte, err error)

func NewAccountChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  length.Addr,
	}
}

func EncodeAccounts(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	sort.Sort(s)
	newK := hexutility.EncodeTs(blockN)
	for _, cs := range s.Changes {
		newV := make([]byte, len(cs.Key)+len(cs.Value))
		copy(newV, cs.Key)
		copy(newV[len(cs.Key):], cs.Value)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}

func DecodeAccounts(dbKey, dbValue []byte) (uint64, []byte, []byte, error) {
	blockN := binary.BigEndian.Uint64(dbKey)
	if len(dbValue) < length.Addr {
		return 0, nil, nil, fmt.Errorf("account changes purged for block %d", blockN)
	}
	k := dbValue[:length.Addr]
	v := dbValue[length.Addr:]
	return blockN, k, v, nil
}

func FindAccount(c kv.CursorDupSort, blockNumber uint64, key []byte) ([]byte, error) {
	k := hexutility.EncodeTs(blockNumber)
	v, err := c.SeekBothRange(k, key)
	if err != nil {
		return nil, err
	}
	_, k, v, err = DecodeAccounts(k, v)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(k, key) {
		return nil, nil
	}
	return v, nil
}
