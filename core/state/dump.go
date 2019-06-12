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
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
)

type DumpAccount struct {
	Balance     string            `json:"balance"`
	Nonce       uint64            `json:"nonce"`
	Root        string            `json:"root"`
	CodeHash    string            `json:"codeHash"`
	Code        string            `json:"code"`
	Storage     map[string]string `json:"storage"`
	StorageSize *uint64           `json:"storagesize,omitempty"`
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
	var prefix [32]byte
	err := self.db.Walk(AccountsBucket, prefix[:], 0, func(k, v []byte) (bool, error) {
		addr := self.GetKey(k)
		data, err := encodingToAccount(v)
		if err != nil {
			return false, err
		}
		var code []byte
		if !bytes.Equal(data.CodeHash[:], emptyCodeHash) {
			if code, err = self.db.Get(CodeBucket, data.CodeHash[:]); err != nil {
				return false, err
			}
		}
		account := DumpAccount{
			Balance:     data.Balance.String(),
			Nonce:       data.Nonce,
			Root:        common.Bytes2Hex(data.Root[:]),
			CodeHash:    common.Bytes2Hex(data.CodeHash),
			Code:        common.Bytes2Hex(code),
			Storage:     make(map[string]string),
			StorageSize: data.StorageSize,
		}
		err = self.db.Walk(StorageBucket, addr, uint(len(addr)*8), func(ks, vs []byte) (bool, error) {
			account.Storage[common.Bytes2Hex(self.GetKey(ks))] = common.Bytes2Hex(vs)
			return true, nil
		})
		if err != nil {
			return false, err
		}
		dump.Accounts[common.Bytes2Hex(addr)] = account
		return true, nil
	})
	if err != nil {
		panic(err)
	}
	return dump
}

func (self *TrieDbState) Dump() []byte {
	json, err := json.MarshalIndent(self.RawDump(), "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}

	return json
}
