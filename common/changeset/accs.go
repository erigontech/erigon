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

package changeset

import (
	common2 "github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/historyv2"

	"github.com/erigontech/erigon/ethdb"
)

// GetModifiedAccounts returns a list of addresses that were modified in the block range
// [startNum:endNum)
func GetModifiedAccounts(db kv.Tx, startNum, endNum uint64) ([]libcommon.Address, error) {
	changedAddrs := make(map[libcommon.Address]struct{})
	if err := ForRange(db, kv.AccountChangeSet, startNum, endNum, func(blockN uint64, k, v []byte) error {
		changedAddrs[libcommon.BytesToAddress(k)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	if len(changedAddrs) == 0 {
		return nil, nil
	}

	idx := 0
	result := make([]libcommon.Address, len(changedAddrs))
	for addr := range changedAddrs {
		copy(result[idx][:], addr[:])
		idx++
	}

	return result, nil
}

// [from:to)
func ForRange(db kv.Tx, bucket string, from, to uint64, walker func(blockN uint64, k, v []byte) error) error {
	var blockN uint64
	c, err := db.Cursor(bucket)
	if err != nil {
		return err
	}
	defer c.Close()
	return ethdb.Walk(c, hexutility.EncodeTs(from), 0, func(k, v []byte) (bool, error) {
		var err error
		blockN, k, v, err = historyv2.FromDBFormat(k, v)
		if err != nil {
			return false, err
		}
		if blockN >= to {
			return false, nil
		}
		if err = walker(blockN, k, v); err != nil {
			return false, err
		}
		return true, nil
	})
}

// RewindData generates rewind data for all plain buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db kv.Tx, timestampSrc, timestampDst uint64, changes *etl.Collector, quit <-chan struct{}) error {
	if err := walkAndCollect(
		changes.Collect,
		db, kv.AccountChangeSet,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return err
	}

	if err := walkAndCollect(
		changes.Collect,
		db, kv.StorageChangeSet,
		timestampDst+1, timestampSrc,
		quit,
	); err != nil {
		return err
	}

	return nil
}

func walkAndCollect(collectorFunc func([]byte, []byte) error, db kv.Tx, bucket string, timestampDst, timestampSrc uint64, quit <-chan struct{}) error {
	return ForRange(db, bucket, timestampDst, timestampSrc+1, func(bl uint64, k, v []byte) error {
		if err := common2.Stopped(quit); err != nil {
			return err
		}
		if innerErr := collectorFunc(common2.Copy(k), common2.Copy(v)); innerErr != nil {
			return innerErr
		}
		return nil
	})
}
