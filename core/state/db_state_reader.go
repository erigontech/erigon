package state

import (
	"bytes"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Implements StateReader by wrapping database only, without trie
type DbStateReader struct {
	db             ethdb.Getter
	incarnationMap map[common.Address]uint64
	accountCache   *lru.Cache
	storageCache   *lru.Cache
	codeCache      *lru.Cache
	codeSizeCache  *lru.Cache
}

func NewDbStateReader(db ethdb.Getter, incarnationMap map[common.Address]uint64) *DbStateReader {
	return &DbStateReader{
		db:             db,
		incarnationMap: incarnationMap,
	}
}

func (dbr *DbStateReader) SetAccountCache(accountCache *lru.Cache) {
	dbr.accountCache = accountCache
}

func (dbr *DbStateReader) SetStorageCache(storageCache *lru.Cache) {
	dbr.storageCache = storageCache
}

func (dbr *DbStateReader) SetCodeCache(codeCache *lru.Cache) {
	dbr.codeCache = codeCache
}

func (dbr *DbStateReader) SetCodeSizeCache(codeSizeCache *lru.Cache) {
	dbr.codeSizeCache = codeSizeCache
}

func (dbr *DbStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	if dbr.accountCache != nil {
		if cached, ok := dbr.accountCache.Get(address); ok {
			if cached == nil {
				return nil, nil
			}
			return cached.(*accounts.Account), nil
		}
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	var a accounts.Account
	if ok, err := rawdb.ReadAccount(dbr.db, addrHash, &a); err != nil {
		return nil, err
	} else if !ok {
		if dbr.accountCache != nil {
			dbr.accountCache.Add(address, nil)
		}
		return nil, nil
	}
	if dbr.accountCache != nil {
		dbr.accountCache.Add(address, a.SelfCopy())
	}
	return &a, nil
}

func (dbr *DbStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	var storageKeyP *[20 + 32]byte
	if dbr.storageCache != nil {
		var storageKey [20 + 32]byte
		copy(storageKey[:], address[:])
		copy(storageKey[20:], key[:])
		if cached, ok := dbr.storageCache.Get(storageKey); ok {
			if cached == nil {
				return nil, nil
			}
			return cached.([]byte), nil
		}
		storageKeyP = &storageKey
	}
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
	if dbr.storageCache != nil {
		dbr.storageCache.Add(*storageKeyP, enc)
	}
	return enc, nil
}

func (dbr *DbStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if dbr.codeCache != nil {
		if cached, ok := dbr.codeCache.Get(address); ok {
			if cached == nil {
				return nil, nil
			}
			return cached.([]byte), nil
		}
	}
	code, err = dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	if dbr.codeCache != nil {
		dbr.codeCache.Add(address, code)
	}
	if dbr.codeSizeCache != nil {
		dbr.codeSizeCache.Add(address, len(code))
	}
	return code, err
}

func (dbr *DbStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (codeSize int, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if dbr.codeSizeCache != nil {
		if cached, ok := dbr.codeSizeCache.Get(address); ok {
			return cached.(int), nil
		}
	}
	var code []byte
	code, err = dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	if dbr.codeSizeCache != nil {
		dbr.codeSizeCache.Add(address, len(code))
	}
	return len(code), nil
}

func (dbr *DbStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := dbr.incarnationMap[address]; ok {
		return inc, nil
	}
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
