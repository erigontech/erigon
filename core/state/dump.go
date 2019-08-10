// Copyright 2014 The go-ethereum Authors
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

package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

type DumpAccount struct {
	Balance     string            `json:"balance"`
	Nonce       uint64            `json:"nonce"`
	Root        string            `json:"root"`
	CodeHash    string            `json:"codeHash"`
	Code        string            `json:"code,omitempty"`
	Storage     map[string]string `json:"storage,omitempty"`
	StorageSize *uint64           `json:",omitempty"`
}

type Dump struct {
	Root     string                 `json:"root"`
	Accounts map[string]DumpAccount `json:"accounts"`
}

func (self *TrieDbState) RawDump() Dump {
	dump := Dump{
		Root:     fmt.Sprintf("%x", self.t.Hash()),
		Accounts: make(map[string]DumpAccount),
	}
	var acc accounts.Account
	var prefix [32]byte
	err := self.db.Walk(AccountsBucket, prefix[:], 0, func(k, v []byte) (bool, error) {
		addr := self.GetKey(k)
		var err error
		if err = acc.Decode(v); err != nil {
			return false, err
		}
		var code []byte

		if !acc.IsEmptyCodeHash() {
			if code, err = self.db.Get(CodeBucket, acc.CodeHash[:]); err != nil {
				return false, err
			}
		}
		account := DumpAccount{
			Balance:  acc.Balance.String(),
			Nonce:    acc.Nonce,
			Root:     common.Bytes2Hex(acc.Root[:]),
			CodeHash: common.Bytes2Hex(acc.CodeHash[:]),
			Code:     common.Bytes2Hex(code),
			Storage:  make(map[string]string),
		}
		if acc.HasStorageSize {
			var storageSize = acc.StorageSize
			account.StorageSize = &storageSize
		}


		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutUvarint(buf, acc.GetIncarnation())


		err = self.db.Walk(StorageBucket, GenerateStoragePrefix(common.BytesToAddress(addr), acc.GetIncarnation()), uint(common.AddressLength*8+binary.MaxVarintLen64), func(ks, vs []byte) (bool, error) {
			key := self.GetKey(ks[common.AddressLength+binary.MaxVarintLen64:]) //remove account address and version from composite key
			account.Storage[common.BytesToHash(key).String()] = common.Bytes2Hex(vs)
			return true, nil
		})
		if err != nil {
			return false, err
		}
		dump.Accounts["0x"+common.Bytes2Hex(addr)] = account
		return true, nil
	})
	if err != nil {
		panic(err)
	}
	return dump
}

func (self *TrieDbState) Dump() []byte {
	b, err := json.MarshalIndent(self.RawDump(), "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}

	return b
}
