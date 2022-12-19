package adapter

import (
	"bytes"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

type StateReader struct {
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort
	blockNr                      uint64
	tx                           kv.Tx
}

func NewStateReader(tx kv.Tx, blockNr uint64) *StateReader {
	c1, _ := tx.Cursor(kv.AccountsHistory)
	c2, _ := tx.Cursor(kv.StorageHistory)
	c3, _ := tx.CursorDupSort(kv.AccountChangeSet)
	c4, _ := tx.CursorDupSort(kv.StorageChangeSet)
	return &StateReader{
		tx:          tx,
		blockNr:     blockNr,
		accHistoryC: c1, storageHistoryC: c2, accChangesC: c3, storageChangesC: c4,
	}
}

func (r *StateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := historyv2read.GetAsOf(r.tx, r.accHistoryC, r.accChangesC, false /* storage */, address[:], r.blockNr+1)
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
	return historyv2read.GetAsOf(r.tx, r.storageHistoryC, r.storageChangesC, true /* storage */, compositeKey, r.blockNr+1)
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
