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
	"encoding/binary"
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
	m := make(map[string]map[string]struct{})
	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)
	if err := db.Walk(dbutils.SuffixBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		keycount := int(binary.BigEndian.Uint32(v))
		if keycount > 0 {
			bucketStr := string(common.CopyBytes(bucket))
			var t map[string]struct{}
			var ok bool
			if t, ok = m[bucketStr]; !ok {
				t = make(map[string]struct{})
				m[bucketStr] = t
			}
			i := 4
			for ki := 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				t[string(common.CopyBytes(v[i:i+l]))] = struct{}{}
				i += l
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	for bucketStr, t := range m {
		//t := m[bucketStr]
		bucket := []byte(bucketStr)
		for keyStr := range t {
			key := []byte(keyStr)
			value, err := db.GetAsOf(bucket[1:], bucket, key, timestampDst+1)
			if err != nil {
				value = nil
			}
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
	if err := db.Walk(dbutils.SuffixBucket, startCode, 0, func(k, v []byte) (bool, error) {
		timestamp, bucket := dbutils.DecodeTimestamp(k)
		if !bytes.Equal(bucket, dbutils.AccountsHistoryBucket) {
			return true, nil
		}
		if timestamp > endtimestamp {
			return false, nil
		}
		keycount := int(binary.BigEndian.Uint32(v))
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			t.ReplaceOrInsert(&PutItem{key: common.CopyBytes(v[i : i+l]), value: nil})
			i += l
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
