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

package state

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
)

var _ StateWriter = (*WriterV4)(nil)

type WriterV4 struct {
	sd    *state.SharedDomains
	tx    kv.Tx
	trace bool
}

func NewWriterV4(domains *state.SharedDomains, tx kv.Tx) *WriterV4 {
	return &WriterV4{
		sd:    domains,
		tx:    tx,
		trace: false,
	}
}

func (w *WriterV4) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash)
	}
	if original.Incarnation > account.Incarnation {
		if err := w.sd.DomainDel(kv.CodeDomain, w.tx, address.Bytes(), nil, nil, 0); err != nil {
			return err
		}
		if err := w.sd.DomainDelPrefix(kv.StorageDomain, w.tx, address[:]); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	return w.sd.DomainPut(kv.AccountsDomain, w.tx, address.Bytes(), nil, value, nil, 0)
}

func (w *WriterV4) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	return w.sd.DomainPut(kv.CodeDomain, w.tx, address.Bytes(), nil, code, nil, 0)
}

func (w *WriterV4) DeleteAccount(address common.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del account: %x\n", address)
	}
	return w.sd.DomainDel(kv.AccountsDomain, w.tx, address.Bytes(), nil, nil, 0)
}

func (w *WriterV4) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	vb := value.Bytes32() // using [32]byte instead of []byte to avoid heap escape
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, vb[32-value.ByteLen():])
	}
	return w.sd.DomainPut(kv.StorageDomain, w.tx, address[:], key[:], vb[32-value.ByteLen():], nil, 0)
}

func (w *WriterV4) DeleteAccountStorage(address common.Address, incarnation uint64, key common.Hash) error {
	if w.trace {
		fmt.Printf("storage delete: %x,%x\n", address, key)
	}
	return w.sd.DomainDel(kv.StorageDomain, w.tx, address[:], key[:], nil, 0)
}

func (w *WriterV4) CreateContract(address common.Address) (err error) {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	//seems don't need delete code here - tests starting fail
	//if err = sd.DomainDel(kv.CodeDomain, address[:], nil, nil); err != nil {
	//	return err
	//}
	return w.sd.DomainDelPrefix(kv.StorageDomain, w.tx, address[:])
}
