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
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

// ReadAccount reading account object from multiple buckets of db
func ReadAccount(db DatabaseReader, addrHash common.Hash, acc *accounts.Account) (bool, error) {
	addrHashBytes := addrHash[:]
	enc, err := db.Get(dbutils.CurrentStateBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}

func WriteAccount(db DatabaseWriter, addrHash common.Hash, acc accounts.Account) error {
	//fmt.Printf("WriteAccount: %x %x\n", addrHash, acc.Root)
	addrHashBytes := addrHash[:]
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	return db.Put(dbutils.CurrentStateBucket, addrHashBytes, value)
}

type DatabaseReaderDeleter interface {
	DatabaseReader
	DatabaseDeleter
}

func DeleteAccount(db DatabaseReaderDeleter, addrHash common.Hash) error {
	addrHashBytes := addrHash[:]

	if err := db.Delete(dbutils.CurrentStateBucket, addrHashBytes); err != nil {
		return err
	}

	return nil
}
