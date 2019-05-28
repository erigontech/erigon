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

package state

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

type TraceDbState struct {
	currentDb ethdb.Database
}

func NewTraceDbState(db ethdb.Database) *TraceDbState {
	return &TraceDbState{
		currentDb: db,
	}
}

func (tds *TraceDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.currentDb.Get(AccountsBucket, buf[:])
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (tds *TraceDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.currentDb.Get(StorageBucket, append(address[:], buf[:]...))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (tds *TraceDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return tds.currentDb.Get(CodeBucket, codeHash[:])
}

func (tds *TraceDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := tds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (tds *TraceDbState) UpdateAccountData(address common.Address, original, account *Account) error {
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	return tds.currentDb.Put(AccountsBucket, addrHash[:], data)
}

func (tds *TraceDbState) DeleteAccount(address common.Address, original *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	return tds.currentDb.Delete(AccountsBucket, addrHash[:])
}

func (tds *TraceDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return tds.currentDb.Put(CodeBucket, codeHash[:], code)
}

func (tds *TraceDbState) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var seckey common.Hash
	h.sha.Read(seckey[:])
	compositeKey := append(address[:], seckey[:]...)
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	if len(v) == 0 {
		return tds.currentDb.Delete(StorageBucket, compositeKey)
	} else {
		return tds.currentDb.Put(StorageBucket, compositeKey, vv)
	}
}
