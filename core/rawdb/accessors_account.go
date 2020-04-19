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

// ReadAccount retrieves the version number of the database.
func ReadAccount(db DatabaseReader, addrHash common.Hash, acc *accounts.Account) (bool, error) {
	addrHashBytes := addrHash[:]
	enc, err := db.Get(dbutils.CurrentStateBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	root, err := db.Get(dbutils.IntermediateTrieHashBucket, addrHashBytes)
	if err != nil {
		return false, err
	}
	if enc == nil || root == nil {
		return false, nil
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	acc.Root = common.BytesToHash(root)
	return true, nil
}

func WriteAccount(db DatabaseWriter, addrHash common.Hash, acc accounts.Account) error {
	addrHashBytes := addrHash[:]
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	if err := db.Put(dbutils.CurrentStateBucket, addrHashBytes, value); err != nil {
		return err
	}
	if err := db.Put(dbutils.IntermediateTrieHashBucket, addrHashBytes, acc.Root.Bytes()); err != nil {
		return err
	}
	return nil
}

func DeleteAccount(db DatabaseDeleter, addrHash common.Hash) error {
	addrHashBytes := addrHash[:]
	if err := db.Delete(dbutils.CurrentStateBucket, addrHashBytes); err != nil {
		return err
	}
	if err := db.Delete(dbutils.IntermediateTrieHashBucket, addrHashBytes); err != nil {
		return err
	}
	return nil
}
