package state

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Implements StateReader by wrapping database only, without trie
type DbStateReader struct {
	db ethdb.Getter
}

func NewDbStateReader(db ethdb.Getter) *DbStateReader {
	return &DbStateReader{
		db: db,
	}
}

func (dbr *DbStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	var a accounts.Account
	if ok, err := rawdb.ReadAccount(dbr.db, addrHash, &a); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &a, nil
}

func (dbr *DbStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	seckey, err1 := common.HashData(key[:])
	if err1 != nil {
		return nil, err1
	}
	enc, err2 := dbr.db.Get(dbutils.CurrentStateBucket, dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
	if err2 != nil && err2 != ethdb.ErrKeyNotFound {
		return nil, err2
	}
	return enc, nil
}

func (dbr *DbStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	code, err = dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	return code, err
}

func (dbr *DbStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (codeSize int, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	var code []byte
	code, err = dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (dbr *DbStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return 0, err
	}
	incarnation, found, err := ethdb.GetCurrentAccountIncarnation(dbr.db, addrHash)
	if err != nil {
		return 0, err
	}
	if found {
		return incarnation, nil
	}
	return 0, nil
}
