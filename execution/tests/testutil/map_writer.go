// Copyright 2026 The Erigon Authors
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

package testutil

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	_ state.StateWriter = (*MapWriter)(nil)
)

type MapWriter struct {
	Changes map[string]([]byte)
}

func NewMapWriter() *MapWriter {
	return &MapWriter{Changes: make(map[string]([]byte))}
}

func (mw *MapWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	if account.Equals(original) {
		return nil
	}
	addressValue := address.Value()
	mw.Changes[string(addressValue[:])] = accounts.SerialiseV3(account)
	return nil
}

func (mw *MapWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	return nil
}

func (mw *MapWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	return nil
}

func (mw *MapWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	var addressValue common.Address
	if !address.IsNil() {
		addressValue = address.Value()
	}
	var keyValue common.Hash
	if !key.IsNil() {
		keyValue = key.Value()
	}
	composite := append(addressValue[:], keyValue[:]...)
	v := value.Bytes()
	mw.Changes[string(composite)] = v
	return nil
}

func (mw *MapWriter) CreateContract(address accounts.Address) error {
	return nil
}
