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

package tests

import (
"github.com/erigontech/erigon-lib/common"
"github.com/erigontech/erigon-lib/types/accounts"
"github.com/erigontech/erigon/core/state"
"github.com/holiman/uint256"
)

// DirectStateReader is a custom state reader that uses the in-memory state directly
// This is used for testing to ensure the RPC layer correctly reflects state changes
type DirectStateReader struct {
statedb *state.IntraBlockState
}

// NewDirectStateReader creates a new DirectStateReader
func NewDirectStateReader(statedb *state.IntraBlockState) *DirectStateReader {
return &DirectStateReader{statedb: statedb}
}

// ReadAccountData reads account data directly from the in-memory state
func (r *DirectStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
// IntraBlockState doesn't have a GetAccount method, so we need to create an account from the state
acc := &accounts.Account{}
balance, err := r.statedb.GetBalance(address)
if err != nil {
	return nil, err
}
if balance == nil {
	return nil, nil
}

nonce, err := r.statedb.GetNonce(address)
if err != nil {
	return nil, err
}

codeHash, err := r.statedb.GetCodeHash(address)
if err != nil {
	return nil, err
}

acc.Balance.Set(balance)
acc.Nonce = nonce
acc.CodeHash = codeHash

return acc, nil
}

// ReadAccountDataForDebug reads account data directly from the in-memory state for debug purposes
func (r *DirectStateReader) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
// Same implementation as ReadAccountData
acc := &accounts.Account{}
balance, err := r.statedb.GetBalance(address)
if err != nil {
	return nil, err
}
if balance == nil {
	return nil, nil
}

nonce, err := r.statedb.GetNonce(address)
if err != nil {
	return nil, err
}

codeHash, err := r.statedb.GetCodeHash(address)
if err != nil {
	return nil, err
}

acc.Balance.Set(balance)
acc.Nonce = nonce
acc.CodeHash = codeHash

return acc, nil
}

// ReadAccountStorage reads account storage directly from the in-memory state
func (r *DirectStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
value := new(uint256.Int)
err := r.statedb.GetState(address, key, value)
if err != nil {
	return nil, err
}
if value.IsZero() {
	return nil, nil
}
return value.Bytes(), nil
}

// ReadAccountCode reads account code directly from the in-memory state
func (r *DirectStateReader) ReadAccountCode(address common.Address, incarnation uint64) ([]byte, error) {
return r.statedb.GetCode(address)
}

// ReadAccountCodeSize reads account code size directly from the in-memory state
func (r *DirectStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64) (int, error) {
code, err := r.statedb.GetCode(address)
if err != nil {
	return 0, err
}
return len(code), nil
}

// ReadAccountIncarnation reads account incarnation directly from the in-memory state
func (r *DirectStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
incarnation, err := r.statedb.GetIncarnation(address)
return incarnation, err
}

