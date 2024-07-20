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
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types/accounts"
)

var _ StateReader = (*ReaderV4)(nil)

type ReaderV4 struct {
	tx kv.TemporalGetter
}

func NewReaderV4(tx kv.TemporalGetter) *ReaderV4 {
	return &ReaderV4{tx: tx}
}

func (r *ReaderV4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, _, err := r.tx.DomainGet(kv.AccountsDomain, address.Bytes(), nil)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *ReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) (enc []byte, err error) {
	enc, _, err = r.tx.DomainGet(kv.StorageDomain, address.Bytes(), key.Bytes())
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *ReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (code []byte, err error) {
	if codeHash == emptyCodeHashH {
		return nil, nil
	}
	code, _, err = r.tx.DomainGet(kv.CodeDomain, address.Bytes(), nil)
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

func (r *ReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *ReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}

func (r *ReaderV4) ReadCommitment(prefix []byte) (enc []byte, err error) {
	enc, _, err = r.tx.DomainGet(kv.CommitmentDomain, prefix, nil)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

type SimReaderV4 struct {
	tx kv.RwTx
}

func NewSimReaderV4(tx kv.RwTx) *SimReaderV4 {
	return &SimReaderV4{tx: tx}
}

func (r *SimReaderV4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, err := r.tx.GetOne(kv.TblAccountVals, address.Bytes())
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *SimReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) (enc []byte, err error) {
	enc, err = r.tx.GetOne(kv.TblStorageVals, libcommon.Append(address.Bytes(), key.Bytes()))
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *SimReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (code []byte, err error) {
	if codeHash == emptyCodeHashH {
		return nil, nil
	}
	code, err = r.tx.GetOne(kv.TblCodeVals, address.Bytes())
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

func (r *SimReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *SimReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}

func (r *SimReaderV4) ReadCommitment(prefix []byte) (enc []byte, err error) {
	enc, err = r.tx.GetOne(kv.TblCommitmentVals, prefix)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}
