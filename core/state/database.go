// Copyright 2019 The go-ethereum Authors
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

//nolint:scopelint
package state

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

const (
	//FirstContractIncarnation - first incarnation for contract accounts. After 1 it increases by 1.
	FirstContractIncarnation = 1
	//NonContractIncarnation incarnation for non contracts
	NonContractIncarnation = 0
)

type StateReader interface {
	ReadAccountData(address common.Address) (*accounts.Account, error)
	ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error)
	ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error)
	ReadAccountIncarnation(address common.Address) (uint64, error)
}

type StateWriter interface {
	UpdateAccountData(address common.Address, original, account *accounts.Account) error
	UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error
	DeleteAccount(address common.Address, original *accounts.Account) error
	WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error
	CreateContract(address common.Address) error
}

type WriterWithChangeSets interface {
	StateWriter
	WriteChangeSets() error
	WriteHistory() error
}

type NoopWriter struct {
}

var noopWriter = &NoopWriter{}

func NewNoopWriter() *NoopWriter {
	return noopWriter
}

func (nw *NoopWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	return nil
}

func (nw *NoopWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (nw *NoopWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	return nil
}

func (nw *NoopWriter) CreateContract(address common.Address) error {
	return nil
}

func (nw *NoopWriter) WriteChangeSets() error {
	return nil
}

func (nw *NoopWriter) WriteHistory() error {
	return nil
}
