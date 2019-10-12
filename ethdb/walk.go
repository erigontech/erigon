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
	fmt.Println("-------------------rewindData------------", timestampSrc, timestampDst)
	defer fmt.Println("-------------------rewindData------------",timestampSrc, timestampDst)
	// Collect list of buckets and keys that need to be considered
	m := make(map[string]map[string][]byte)
	suffixDst := encodeTimestamp(timestampDst + 1)
	if err := db.Walk(dbutils.SuffixBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := decodeTimestamp(k)
		fmt.Println("ethdb/walk.go:39 timestamp", timestamp)
		if timestamp > timestampSrc {
			return false, nil
		}
		ca,err:=dbutils.Decode(v)
		if err!=nil {
			fmt.Println("ethdb/walk.go:43", err)
			return false, err
		}
		for i:=range ca.Changes {
			fmt.Printf("-- %x = %x \n", ca.Changes[i].Key, ca.Changes[i].Key)
		}
		if ca.KeyCount() > 0 {
			bucketStr := string(common.CopyBytes(bucket))
			var t map[string][]byte
			var ok bool
			if t, ok = m[bucketStr]; !ok {
				t = make(map[string][]byte)
				m[bucketStr] = t
			}

			err=ca.Walk(func(k, v []byte) error {
				t[string(k)]=v
				return nil
			})
			if err!=nil {
				fmt.Println("ethdb/walk.go:60", err)
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		fmt.Println("ethdb/walk.go:68. err", err)
		return err
	}
	for bucketStr, t := range m {
		bucket := []byte(bucketStr)
		for keyStr, value := range t {
			key := []byte(keyStr)
			fmt.Println("ethdb/walk.go:78")
			value2, _ := db.GetAsOf(bucket[1:], bucket, key, timestampDst+1)
			fmt.Printf("===================check for block %v %x(%x)==========\n",timestampDst+1, key, bucket)
			fmt.Printf("value suffix %x\n",value)
			fmt.Printf("value2 db    %x\n",value2)
			fmt.Printf("===================end check for %x==========\n", key)
			if err := df(bucket, key, value); err != nil {
				fmt.Println("ethdb/walk.go:74", err)
				return err
			}
		}
	}
	return nil
}

func GetModifiedAccounts(db Getter, starttimestamp, endtimestamp uint64) ([]common.Address, error) {
	t := llrb.New()
	startCode := encodeTimestamp(starttimestamp)
	if err := db.Walk(dbutils.SuffixBucket, startCode, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := decodeTimestamp(k)
		if !bytes.Equal(bucket, dbutils.AccountsHistoryBucket) {
			return true, nil
		}
		if timestamp > endtimestamp {
			return false, nil
		}
		d,err:=dbutils.Decode(v)
		if err!=nil {
			return false, err
		}
		err = d.Walk(func(k, v []byte) error {
			t.ReplaceOrInsert(&PutItem{key: k, value: nil})
			return nil
		})
		if err!=nil {
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
