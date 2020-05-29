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

	"github.com/ledgerwatch/turbo-geth/common/changeset"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

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
