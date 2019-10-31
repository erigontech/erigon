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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/petar/GoLLRB/llrb"
)

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// Generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func rewindData(db Getter, timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	// Collect list of buckets and keys that need to be considered
	m := make(map[string]map[string][]byte)
	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)
	if err := db.Walk(dbutils.ChangeSetBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		ca, err := dbutils.Decode(v)
		if err != nil {
			return false, err
		}

		if ca.KeyCount() > 0 {
			bucketStr := string(common.CopyBytes(bucket))
			var t map[string][]byte
			var ok bool
			if t, ok = m[bucketStr]; !ok {
				t = make(map[string][]byte)
				m[bucketStr] = t
			}

			err = ca.Walk(func(k, v []byte) error {
				if _, ok = t[string(k)]; !ok {
					t[string(k)] = v
				}

				return nil
			})
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	for bucketStr, t := range m {
		bucket := []byte(bucketStr)
		for keyStr, value := range t {
			key := []byte(keyStr)
			if err := df(bucket, key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func GetModifiedAccounts(db Getter, starttimestamp, endtimestamp uint64) ([]common.Address, error) {
	t := llrb.New()
	startCode := dbutils.EncodeTimestamp(starttimestamp)
	if err := db.Walk(dbutils.ChangeSetBucket, startCode, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := dbutils.DecodeTimestamp(k)
		if !bytes.Equal(bucket, dbutils.AccountsHistoryBucket) {
			return true, nil
		}
		if timestamp > endtimestamp {
			return false, nil
		}
		d, err := dbutils.Decode(v)
		if err != nil {
			return false, err
		}
		err = d.Walk(func(k, v []byte) error {
			t.ReplaceOrInsert(&PutItem{key: k, value: nil})
			return nil
		})
		if err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return nil, err
	}
	accounts := make([]common.Address, t.Len())
	if t.Len() == 0 {
		return accounts, nil
	}
	idx := 0
	var extErr error
	min, _ := t.Min().(*PutItem)
	if min == nil {
		return accounts, nil
	}
	t.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
		item := i.(*PutItem)
		value, err := db.Get(dbutils.PreimagePrefix, item.key)
		if err != nil {
			extErr = fmt.Errorf("Could not get preimage for key %x", item.key)
			return false
		}
		copy(accounts[idx][:], value)
		idx++
		return true
	})
	if extErr != nil {
		return nil, extErr
	}
	return accounts, nil
}
