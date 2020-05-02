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
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/changeset"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// RewindData generates rewind data for all buckets between the timestamp
// timestapSrc is the current timestamp, and timestamp Dst is where we rewind
func RewindData(db Getter, timestampSrc, timestampDst uint64) (map[string][]byte, map[string][]byte, error) {
	// Collect list of buckets and keys that need to be considered
	accountMap := make(map[string][]byte)
	suffixDst := dbutils.EncodeTimestamp(timestampDst + 1)
	if err := db.Walk(dbutils.AccountChangeSetBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, _ := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}
		if changeset.Len(v) > 0 {
			walker := func(kk, vv []byte) error {
				if _, ok := accountMap[string(kk)]; !ok {
					accountMap[string(kk)] = vv
				}
				return nil
			}
			v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
			if innerErr := changeset.AccountChangeSetBytes(v).Walk(walker); innerErr != nil {
				return false, innerErr
			}
		}
		return true, nil
	}); err != nil {
		return nil, nil, err
	}
	storageMap := make(map[string][]byte)
	if err := db.Walk(dbutils.StorageChangeSetBucket, suffixDst, 0, func(k, v []byte) (bool, error) {
		timestamp, _ := dbutils.DecodeTimestamp(k)
		if timestamp > timestampSrc {
			return false, nil
		}

		if changeset.Len(v) > 0 {
			walker := func(kk, vv []byte) error {
				if _, ok := storageMap[string(kk)]; !ok {
					storageMap[string(kk)] = vv
				}
				return nil
			}
			v = common.CopyBytes(v) // Making copy because otherwise it will be invalid after the transaction
			if innerErr := changeset.StorageChangeSetBytes(v).Walk(walker); innerErr != nil {
				return false, innerErr
			}
		}
		return true, nil
	}); err != nil {
		return nil, nil, err
	}
	return accountMap, storageMap, nil
}

func GetModifiedAccounts(db Getter, startTimestamp, endTimestamp uint64) ([]common.Address, error) {
	keys := make(map[common.Hash]struct{})
	startCode := dbutils.EncodeTimestamp(startTimestamp)
	if err := db.Walk(dbutils.AccountChangeSetBucket, startCode, 0, func(k, v []byte) (bool, error) {
		keyTimestamp, _ := dbutils.DecodeTimestamp(k)

		if keyTimestamp > endTimestamp {
			return false, nil
		}

		walker := func(addrHash, _ []byte) error {
			keys[common.BytesToHash(addrHash)] = struct{}{}
			return nil
		}

		if innerErr := changeset.AccountChangeSetBytes(v).Walk(walker); innerErr != nil {
			return false, innerErr
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

	for key := range keys {
		value, err := db.Get(dbutils.PreimagePrefix, key[:])
		if err != nil {
			return nil, fmt.Errorf("could not get preimage for key %x", key)
		}
		copy(accounts[idx][:], value)
		idx++
	}
	return accounts, nil
}

// GetCurrentAccountIncarnation reads the latest incarnation of a contract from the database.
func GetCurrentAccountIncarnation(db Getter, addrHash common.Hash) (incarnation uint64, found bool, err error) {
	var incarnationBytes [common.IncarnationLength]byte
	startkey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	var fixedbits uint = 8 * common.HashLength
	copy(startkey, addrHash[:])
	err = db.Walk(dbutils.CurrentStateBucket, startkey, fixedbits, func(k, v []byte) (bool, error) {
		copy(incarnationBytes[:], k[common.HashLength:])
		found = true
		return false, nil
	})
	if err != nil {
		return
	}
	if found {
		incarnation = (^binary.BigEndian.Uint64(incarnationBytes[:]))
	}
	return
}
