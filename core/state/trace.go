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
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type TraceDbState struct {
	currentDb ethdb.Database
}

func NewTraceDbState(db ethdb.Database) *TraceDbState {
	return &TraceDbState{
		currentDb: db,
	}
}

func (tds *TraceDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	buf, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	var acc accounts.Account
	if ok, err := rawdb.ReadAccount(tds.currentDb, buf, &acc); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &acc, nil
}

func (tds *TraceDbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	buf, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	enc, err := tds.currentDb.Get(dbutils.CurrentStateBucket, dbutils.GenerateCompositeStorageKey(addrHash, incarnation, buf))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (tds *TraceDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return tds.currentDb.Get(dbutils.CodeBucket, codeHash[:])
}

func (tds *TraceDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := tds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (tds *TraceDbState) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	return rawdb.WriteAccount(tds.currentDb, addrHash, *account)
}

func (tds *TraceDbState) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	return rawdb.DeleteAccount(tds.currentDb, addrHash)
}

func (tds *TraceDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return tds.currentDb.Put(dbutils.CodeBucket, codeHash[:], code)
}

func (tds *TraceDbState) WriteAccountStorage(address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	seckey, err := common.HashData(key[:])
	if err != nil {
		return err
	}
	compositeKey := append(address[:], seckey[:]...)
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	if len(v) == 0 {
		return tds.currentDb.Delete(dbutils.CurrentStateBucket, compositeKey)
	}
	return tds.currentDb.Put(dbutils.CurrentStateBucket, compositeKey, vv)
}
