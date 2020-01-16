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
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// Generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db Getter, timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	// Collect list of buckets and keys that need to be considered
	m := make(map[string]map[string][]byte)
	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)
	if err := db.Walk(dbutils.AccountChangeSetBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, _ := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}

		if dbutils.Len(v) > 0 {
			bucketStr := string(dbutils.AccountsHistoryBucket)
			var t map[string][]byte
			var ok bool
			if t, ok = m[bucketStr]; !ok {
				t = make(map[string][]byte)
				m[bucketStr] = t
			}

			err := dbutils.Walk(v, func(k, vv []byte) error {
				if _, ok = t[string(k)]; !ok {
					t[string(k)] = vv
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
	if err := db.Walk(dbutils.StorageChangeSetBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, _ := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}

		if dbutils.Len(v) > 0 {
			bucketStr := string(dbutils.StorageHistoryBucket)
			var t map[string][]byte
			var ok bool
			if t, ok = m[bucketStr]; !ok {
				t = make(map[string][]byte)
				m[bucketStr] = t
			}

			err := dbutils.Walk(v, func(k, vv []byte) error {
				if _, ok = t[string(k)]; !ok {
					t[string(k)] = vv
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

func GetModifiedAccounts(db Getter, startTimestamp, endTimestamp uint64) ([]common.Address, error) {
	var keys [][]byte
	startCode := dbutils.EncodeTimestamp(startTimestamp)
	if err := db.Walk(dbutils.AccountChangeSetBucket, startCode, 0, func(k, v []byte) (bool, error) {
		keyTimestamp, _ := dbutils.DecodeTimestamp(k)

		if keyTimestamp > endTimestamp {
			return false, nil
		}
		err := dbutils.Walk(v, func(k, _ []byte) error {
			keys = append(keys, k)
			return nil
		})
		if err != nil {
			return false, err
		}

		return true, nil
	}); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}
	accounts := make([]common.Address, len(keys))
	idx := 0

	for _, key := range keys {
		value, err := db.Get(dbutils.PreimagePrefix, key)
		if err != nil {
			return nil, fmt.Errorf("could not get preimage for key %x", key)
		}
		copy(accounts[idx][:], value)
		idx++
	}
	return accounts, nil
}
