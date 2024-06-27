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
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/core/types/accounts"
)

func ReadAccount(db kv.Getter, addr libcommon.Address, acc *accounts.Account) (bool, error) {
	enc, err := db.GetOne(kv.PlainState, addr[:])
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

func ReadAccountByHash(db kv.Tx, addr libcommon.Hash, acc *accounts.Account) (bool, error) {
	enc, err := db.GetOne(kv.HashedAccounts, addr[:])
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

func WriteAccount(db kv.RwTx, addrHash libcommon.Hash, acc accounts.Account) error {
	addrHashBytes := addrHash[:]
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	return db.Put(kv.HashedAccounts, addrHashBytes, value)
}

func DeleteAccount(db kv.RwTx, addrHash libcommon.Hash) error {
	return db.Delete(kv.HashedAccounts, addrHash[:])
}

func PlainReadAccount(db kv.Tx, address libcommon.Address, acc *accounts.Account) (bool, error) {
	enc, err := db.GetOne(kv.PlainState, address[:])
	if err != nil {
		return false, err
	}
	if err = acc.DecodeForStorage(enc); err != nil {
		return false, err
	}
	return true, nil
}

func PlainWriteAccount(db kv.RwTx, address libcommon.Address, acc accounts.Account) error {
	//fmt.Printf("PlainWriteAccount: %x %x\n", addrHash, acc.Root)
	value := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(value)
	return db.Put(kv.PlainState, address[:], value)
}

func PlainDeleteAccount(db kv.RwTx, address libcommon.Address) error {
	return db.Delete(kv.PlainState, address[:])
}
