// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	//FirstContractIncarnation - first incarnation for contract accounts. After 1 it increases by 1.
	FirstContractIncarnation = 1
	//NonContractIncarnation incarnation for non contracts
	NonContractIncarnation = 0
)

type StateReader interface {
	ReadAccountData(address common.Address) (*accounts.Account, error)
	ReadAccountDataForDebug(address common.Address) (*accounts.Account, error)
	ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error)
	HasStorage(address common.Address) (bool, error)
	ReadAccountCode(address common.Address) ([]byte, error)
	ReadAccountCodeSize(address common.Address) (int, error)
	ReadAccountIncarnation(address common.Address) (uint64, error)
}

type HistoricalStateReader interface {
	GetTxNum() uint64
	SetTxNum(txNum uint64)
}

type StateWriter interface {
	UpdateAccountData(address common.Address, original, account *accounts.Account) error
	UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error
	DeleteAccount(address common.Address, original *accounts.Account) error
	WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error
	CreateContract(address common.Address) error
}

type NoopWriter struct {
	trace bool
}

var noopWriter = &NoopWriter{}

func NewNoopWriter(trace ...bool) *NoopWriter {
	if len(trace) == 0 {
		return noopWriter
	}
	return &NoopWriter{trace[0]}
}

func (nw *NoopWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	if nw.trace {
		fmt.Printf("acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}\n", address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash)
	}
	return nil
}

func (nw *NoopWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	if nw.trace {
		fmt.Printf("del acc: %x\n", address)
	}
	return nil
}

func (nw *NoopWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if nw.trace {
		fmt.Printf("code: %x, %x, valLen: %d\n", address.Bytes(), codeHash, len(code))
	}
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	if original == value {
		return nil
	}
	if nw.trace {
		fmt.Printf("storage: %x,%x,%x\n", address, key, &value)
	}
	return nil
}

func (nw *NoopWriter) CreateContract(address common.Address) error {
	if nw.trace {
		fmt.Printf("create contract: %x\n", address)
	}
	return nil
}

type NoopReader struct {
}

var noopReader = &NoopReader{}

func NewNoopReader() *NoopReader {
	return noopReader
}

func (*NoopReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	return nil, nil
}
func (*NoopReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return nil, nil
}
func (*NoopReader) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (*NoopReader) HasStorage(address common.Address) (bool, error)               { return false, nil }
func (*NoopReader) ReadAccountCode(address common.Address) ([]byte, error)        { return nil, nil }
func (*NoopReader) ReadAccountCodeSize(address common.Address) (int, error)       { return 0, nil }
func (*NoopReader) ReadAccountIncarnation(address common.Address) (uint64, error) { return 0, nil }
