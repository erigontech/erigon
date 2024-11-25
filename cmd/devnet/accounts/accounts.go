// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package accounts

import (
	"crypto/ecdsa"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/crypto"
	"github.com/erigontech/erigon/core"
)

const DevAddress = "0x67b1d87101671b127f5f8714789C7192f7ad340e"

type Account struct {
	Name    string
	Address libcommon.Address
	sigKey  *ecdsa.PrivateKey
}

func init() {
	core.DevnetSignKey = func(addr libcommon.Address) *ecdsa.PrivateKey {
		return SigKey(addr)
	}

	devnetEtherbaseAccount := &Account{
		"DevnetEtherbase",
		core.DevnetEtherbase,
		core.DevnetSignPrivateKey,
	}
	accountsByAddress[core.DevnetEtherbase] = devnetEtherbaseAccount
	accountsByName[devnetEtherbaseAccount.Name] = devnetEtherbaseAccount
}

var accountsByAddress = map[libcommon.Address]*Account{}
var accountsByName = map[string]*Account{}

func NewAccount(name string) *Account {
	if account, ok := accountsByName[name]; ok {
		return account
	}

	sigKey, _ := crypto.GenerateKey()

	account := &Account{
		Name:    name,
		Address: crypto.PubkeyToAddress(sigKey.PublicKey),
		sigKey:  sigKey,
	}

	accountsByAddress[account.Address] = account
	accountsByName[name] = account

	return account
}

func (a *Account) SigKey() *ecdsa.PrivateKey {
	return a.sigKey
}

func GetAccount(account string) *Account {
	if account, ok := accountsByName[account]; ok {
		return account
	}

	if account, ok := accountsByAddress[libcommon.HexToAddress(account)]; ok {
		return account
	}

	return nil
}

func SigKey(source interface{}) *ecdsa.PrivateKey {
	switch source := source.(type) {
	case libcommon.Address:
		if account, ok := accountsByAddress[source]; ok {
			return account.sigKey
		}

		if source == core.DevnetEtherbase {
			return core.DevnetSignPrivateKey
		}
	case string:
		if account := GetAccount(source); account != nil {
			return account.sigKey
		}
	}

	return nil
}
