package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types/accounts"
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
