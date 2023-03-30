package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/state/temporal"

	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateReader = (*ReaderV4)(nil)

type ReaderV4 struct {
	tx kv.TemporalTx
}

func NewReaderV4(tx kv.TemporalTx) *ReaderV4 {
	return &ReaderV4{tx: tx}
}

func (r *ReaderV4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, ok, err := r.tx.DomainGet(temporal.AccountsDomain, address.Bytes(), nil)
	if err != nil {
		return nil, err
	}
	if !ok || len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *ReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	enc, ok, err := r.tx.DomainGet(temporal.StorageDomain, address.Bytes(), key.Bytes())
	if err != nil {
		return nil, err
	}
	if !ok || len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *ReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	if codeHash == emptyCodeHashH {
		return nil, nil
	}
	code, ok, err := r.tx.DomainGet(temporal.CodeDomain, address.Bytes(), nil)
	if err != nil {
		return nil, err
	}
	if !ok || len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

func (r *ReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *ReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	panic(1)
	return 0, nil
}

func (r *ReaderV4) ReadCommitment(prefix []byte) ([]byte, error) {
	enc, ok, err := r.tx.DomainGet(temporal.CommitmentDomain, prefix, nil)
	if err != nil {
		return nil, err
	}
	if !ok || len(enc) == 0 {
		return nil, nil
	}
	return enc, nil

}
