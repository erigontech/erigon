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

package rawdb

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
)

// ReadAccountDeprecated reading account object from multiple buckets of db
func ReadAccountDeprecated(db ethdb.DatabaseReader, addrHash common.Hash, acc *accounts.Account) (bool, error) {
	addrHashBytes := addrHash[:]
	enc, err := db.Get(dbutils.HashedAccountsBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}

func ReadAccount(db ethdb.Tx, addrHash common.Address, acc *accounts.Account) (bool, error) {
	addrHashBytes := addrHash[:]
	enc, err := db.GetOne(dbutils.PlainStateBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	if len(enc) == 0 {
		return false, nil
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}
