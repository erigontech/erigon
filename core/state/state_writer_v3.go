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

	"github.com/erigontech/erigon/turbo/shards"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
)

// Writer - used by parallel workers to accumulate updates and then send them to conflict-resolution.
type Writer struct {
	tx          kv.TemporalPutDel
	trace       bool
	accumulator *shards.Accumulator
}

func NewWriter(tx kv.TemporalPutDel, accumulator *shards.Accumulator) *Writer {
	return &Writer{
		tx:          tx,
		accumulator: accumulator,
		//trace: true,
	}
}

func (w *Writer) ResetWriteSet() {}

func (w *Writer) WriteSet() map[string]*libstate.KvList {
	return nil
}

func (w *Writer) PrevAndDels() (map[string][]byte, map[string]*accounts.Account, map[string][]byte, map[string]uint64) {
	return nil, nil, nil, nil
}

func (w *Writer) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if w.trace {
		fmt.Printf("acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	if original.Incarnation > account.Incarnation {
		//del, before create: to clanup code/storage
		if err := w.tx.DomainDel(kv.CodeDomain, address[:], nil, 0); err != nil {
			return err
		}
		if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
			return err
		}
	}
	value := accounts.SerialiseV3(account)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, account.Incarnation, value)
	}

	if err := w.tx.DomainPut(kv.AccountsDomain, address[:], nil, value, nil, 0); err != nil {
		return err
	}
	return nil
}

func (w *Writer) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	if err := w.tx.DomainPut(kv.CodeDomain, address[:], nil, code, nil, 0); err != nil {
		return err
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	return nil
}

func (w *Writer) DeleteAccount(address common.Address, original *accounts.Account) error {
	if w.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
		return err
	}
	if err := w.tx.DomainDel(kv.CodeDomain, address[:], nil, 0); err != nil {
		return err
	}
	if err := w.tx.DomainDel(kv.AccountsDomain, address[:], nil, 0); err != nil {
		return err
	}
	// if w.accumulator != nil { TODO: investigate later. basically this will always panic. keeping this out should be fine anyway.
	// 	w.accumulator.DeleteAccount(address)
	// }
	return nil
}

func (w *Writer) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	composite := append(address.Bytes(), key.Bytes()...)
	v := value.Bytes()
	if w.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, *key, v)
	}
	if len(v) == 0 {
		return w.tx.DomainDel(kv.StorageDomain, composite, nil, 0)
	}
	if w.accumulator != nil && key != nil && value != nil {
		k := *key
		w.accumulator.ChangeStorage(address, incarnation, k, v)
	}

	return w.tx.DomainPut(kv.StorageDomain, composite, nil, v, nil, 0)
}

func (w *Writer) CreateContract(address common.Address) error {
	if w.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	//if err := w.tx.DomainDelPrefix(kv.StorageDomain, address[:]); err != nil {
	//	return err
	//}
	return nil
}
