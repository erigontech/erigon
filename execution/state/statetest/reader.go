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

// Package statetest provides an in-memory, pre-seedable state.StateReader for
// unit tests. It lets a test stand up a writable IntraBlockState without a real
// database: seed any pre-existing accounts/code/storage with the With* builders,
// then call State(). This is the shared harness used to unit-test state-mutating
// logic (e.g. balance transfers, fork state changes) in isolation.
package statetest

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Reader is an in-memory implementation of state.StateReader for tests.
type Reader struct {
	accounts    map[accounts.Address]*accounts.Account
	code        map[accounts.Address][]byte
	storage     map[accounts.Address]map[accounts.StorageKey]uint256.Int
	trace       bool
	tracePrefix string
}

var _ state.StateReader = (*Reader)(nil)

// NewReader returns an empty in-memory reader. Every unseeded account reads as
// non-existent, matching a fresh state.
func NewReader() *Reader {
	return &Reader{
		accounts: map[accounts.Address]*accounts.Account{},
		code:     map[accounts.Address][]byte{},
		storage:  map[accounts.Address]map[accounts.StorageKey]uint256.Int{},
	}
}

func (r *Reader) ensure(addr accounts.Address) *accounts.Account {
	acc, ok := r.accounts[addr]
	if !ok {
		a := accounts.NewAccount()
		acc = &a
		r.accounts[addr] = acc
	}
	return acc
}

// WithAccount seeds a full account. Returns the reader for chaining.
func (r *Reader) WithAccount(addr accounts.Address, acc accounts.Account) *Reader {
	cp := acc
	r.accounts[addr] = &cp
	return r
}

// WithBalance seeds (or creates) an account with the given balance.
func (r *Reader) WithBalance(addr accounts.Address, bal *uint256.Int) *Reader {
	r.ensure(addr).Balance.Set(bal)
	return r
}

// WithNonce seeds (or creates) an account with the given nonce.
func (r *Reader) WithNonce(addr accounts.Address, nonce uint64) *Reader {
	r.ensure(addr).Nonce = nonce
	return r
}

// WithCode seeds (or creates) an account with the given bytecode, deriving its
// code hash.
func (r *Reader) WithCode(addr accounts.Address, code []byte) *Reader {
	r.ensure(addr).CodeHash = accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(code)))
	r.code[addr] = code
	return r
}

// WithStorage seeds a storage slot (and ensures the account exists).
func (r *Reader) WithStorage(addr accounts.Address, key accounts.StorageKey, val *uint256.Int) *Reader {
	if r.storage[addr] == nil {
		r.storage[addr] = map[accounts.StorageKey]uint256.Int{}
	}
	r.storage[addr][key] = *val
	r.ensure(addr)
	return r
}

// State returns a fresh writable IntraBlockState backed by this reader.
func (r *Reader) State() *state.IntraBlockState {
	return state.New(r)
}

func (r *Reader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	acc, ok := r.accounts[addr]
	if !ok {
		return nil, nil
	}
	return acc.SelfCopy(), nil
}

func (r *Reader) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(addr)
}

func (r *Reader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if s, ok := r.storage[addr]; ok {
		if v, ok := s[key]; ok {
			return v, true, nil
		}
	}
	return uint256.Int{}, false, nil
}

func (r *Reader) HasStorage(addr accounts.Address) (bool, error) {
	return len(r.storage[addr]) > 0, nil
}

func (r *Reader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	return r.code[addr], nil
}

func (r *Reader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	return len(r.code[addr]), nil
}

func (r *Reader) ReadAccountIncarnation(addr accounts.Address) (uint64, error) {
	if acc, ok := r.accounts[addr]; ok {
		return acc.Incarnation, nil
	}
	return 0, nil
}

func (r *Reader) SetTrace(trace bool, tracePrefix string) {
	r.trace = trace
	r.tracePrefix = tracePrefix
}

func (r *Reader) Trace() bool { return r.trace }

func (r *Reader) TracePrefix() string { return r.tracePrefix }
