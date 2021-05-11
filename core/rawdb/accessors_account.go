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
	"github.com/ledgerwatch/turbo-geth/ethdb"
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

func ReadAccount(db ethdb.Tx, addrHash common.Hash, acc *accounts.Account) (bool, error) {
	addrHashBytes := addrHash[:]
	enc, err := db.GetOne(dbutils.HashedAccountsBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}

func WriteAccount(db ethdb.Putter, addrHash common.Hash, acc accounts.Account) error {
	//fmt.Printf("WriteAccount: %x %x\n", addrHash, acc.Root)
	addrHashBytes := addrHash[:]
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	return db.Put(dbutils.HashedAccountsBucket, addrHashBytes, value)
}

func DeleteAccount(db ethdb.Deleter, addrHash common.Hash) error {
	return db.Delete(dbutils.HashedAccountsBucket, addrHash[:], nil)
}

func PlainReadAccount(db ethdb.KVGetter, address common.Address, acc *accounts.Account) (bool, error) {
	enc, err := db.GetOne(dbutils.PlainStateBucket, address[:])
	if err != nil {
		return false, err
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}

func PlainWriteAccount(db ethdb.Putter, address common.Address, acc accounts.Account) error {
	//fmt.Printf("PlainWriteAccount: %x %x\n", addrHash, acc.Root)
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	return db.Put(dbutils.PlainStateBucket, address[:], value)
}

func PlainDeleteAccount(db ethdb.Deleter, address common.Address) error {
	return db.Delete(dbutils.PlainStateBucket, address[:], nil)
}
