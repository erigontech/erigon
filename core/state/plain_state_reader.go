package state

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var _ StateReader = (*PlainStateReader)(nil)

// PlainStateReader reads data from so called "plain state".
// Data in the plain state is stored using un-hashed account/storage items
// as opposed to the "normal" state that uses hashes of merkle paths to store items.
type PlainStateReader struct {
	// Typically, the data will be scattered across "normal" and "plain" states.
	// therefore we need fallback in case the data isn't found in the "plain" state.
	fallback StateReader

	db                     ethdb.Getter
	uncommitedIncarnations map[common.Address]uint64
}

func NewPlainStateReaderWithFallback(db ethdb.Getter, incarnations map[common.Address]uint64, fallback StateReader) *PlainStateReader {
	return &PlainStateReader{
		fallback:               fallback,
		db:                     db,
		uncommitedIncarnations: incarnations,
	}
}

func (r *PlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := r.db.Get(dbutils.PlainStateBucket, address[:])
	if err == nil {
		acc := &accounts.Account{}
		if err = acc.DecodeForStorage(enc); err != nil {
			return nil, err
		}
		return acc, nil
	} else if !entryNotFound(err) {
		return nil, err
	}
	return r.fallback.ReadAccountData(address)
}

func (r *PlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, err := r.db.Get(dbutils.PlainStateBucket, dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key))
	if err == nil {
		return enc, nil
	} else if !entryNotFound(err) {
		return nil, err
	}
	return r.fallback.ReadAccountStorage(address, incarnation, key)
}

func (r *PlainStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	// intentionally left blank: we read the code by `codeHash` only
	return r.fallback.ReadAccountCode(address, codeHash)
}

func (r *PlainStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	// intentionally left blank: we read the code size by `codeHash` only
	return r.fallback.ReadAccountCodeSize(address, codeHash)
}

func (r *PlainStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := r.uncommitedIncarnations[address]; ok {
		return inc, nil
	}

	incarnation, found, err := ethdb.PlainGetCurrentAccountIncarnation(r.db, address)
	if err != nil {
		return 0, err
	}
	if found {
		return incarnation, nil
	}

	return r.fallback.ReadAccountIncarnation(address)
}

func entryNotFound(err error) bool {
	return err == ethdb.ErrKeyNotFound
}
