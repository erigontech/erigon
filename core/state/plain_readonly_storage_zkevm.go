// Copyright 2019 The go-ethereum Authors
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

package state

import (
	"fmt"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcfg"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
)

// State at the beginning of blockNr
type PlainStateReadAccountStorage struct {
	storageHistoryC kv.Cursor
	storageChangesC kv.CursorDupSort
	tx              kv.Tx
	blockNr         uint64
	trace           bool
}

func NewPlainStateReadAccountStorage(tx kv.Tx, blockNr uint64) *PlainStateReadAccountStorage {
	histV3, _ := kvcfg.HistoryV3.Enabled(tx)
	if histV3 {
		panic("Please use HistoryStateReaderV3 with HistoryV3")
	}
	ps := &PlainStateReadAccountStorage{
		tx:      tx,
		blockNr: blockNr,
	}

	c2, _ := tx.Cursor(kv.StorageHistory)
	c4, _ := tx.CursorDupSort(kv.StorageChangeSet)

	ps.storageHistoryC = c2
	ps.storageChangesC = c4
	return ps
}

func (s *PlainStateReadAccountStorage) Close() {
	s.storageHistoryC.Close()
	s.storageChangesC.Close()
}

func (s *PlainStateReadAccountStorage) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
	s.storageChangesC.First()
	s.storageHistoryC.First()
}

func (s *PlainStateReadAccountStorage) SetTrace(trace bool) {
	s.trace = trace
}

func (s *PlainStateReadAccountStorage) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := historyv2read.GetAsOf(s.tx, s.storageHistoryC, s.storageChangesC, true /* storage */, compositeKey, s.blockNr)
	if err != nil {
		return nil, err
	}
	if s.trace {
		fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, *key, enc)
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}
