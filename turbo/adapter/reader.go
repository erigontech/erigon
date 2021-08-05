package adapter

import (
	"bytes"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

type StateReader struct {
	blockNr uint64
	tx      kv.Tx
}

func NewStateReader(tx kv.Tx, blockNr uint64) *StateReader {
	return &StateReader{
		tx:      tx,
		blockNr: blockNr,
	}
}

func (r *StateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := state.GetAsOf(r.tx, false /* storage */, address[:], r.blockNr+1)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *StateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	return state.GetAsOf(r.tx, true /* storage */, compositeKey, r.blockNr+1)
}

func (r *StateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], crypto.Keccak256(nil)) {
		return nil, nil
	}
	var val []byte
	v, err := r.tx.GetOne(kv.Code, codeHash[:])
	if err != nil {
		return nil, err
	}
	val = common.CopyBytes(v)
	return val, nil
}

func (r *StateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (r *StateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}
