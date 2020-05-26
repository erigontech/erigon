package state

import (
	"bytes"
	"encoding/binary"

	"github.com/VictoriaMetrics/fastcache"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Implements StateReader by wrapping database only, without trie
type DbStateReader struct {
	db             ethdb.Getter
	accountCache   *fastcache.Cache
	storageCache   *fastcache.Cache
	codeCache      *fastcache.Cache
	codeSizeCache  *fastcache.Cache
}

func NewDbStateReader(db ethdb.Getter) *DbStateReader {
	return &DbStateReader{
		db:             db,
	}
}

func (dbr *DbStateReader) SetAccountCache(accountCache *fastcache.Cache) {
	dbr.accountCache = accountCache
}

func (dbr *DbStateReader) SetStorageCache(storageCache *fastcache.Cache) {
	dbr.storageCache = storageCache
}

func (dbr *DbStateReader) SetCodeCache(codeCache *fastcache.Cache) {
	dbr.codeCache = codeCache
}

func (dbr *DbStateReader) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	dbr.codeSizeCache = codeSizeCache
}

func (dbr *DbStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var enc []byte
	var ok bool
	if dbr.accountCache != nil {
		enc, ok = dbr.accountCache.HasGet(nil, address[:])
	}
	if !ok {
		var err error
		if addrHash, err1 := common.HashData(address[:]); err1 == nil {
			enc, err = dbr.db.Get(dbutils.CurrentStateBucket, addrHash[:])
		} else {
			return nil, err1
		}
		if err != nil && !entryNotFound(err) {
			return nil, err
		}
	}
	if !ok && dbr.accountCache != nil {
		dbr.accountCache.Set(address[:], enc)
	}
	if enc == nil {
		return nil, nil
	}
	acc := &accounts.Account{}
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return acc, nil
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
	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey)
	if dbr.storageCache != nil {
		if enc, ok := dbr.storageCache.HasGet(nil, compositeKey); ok {
			return enc, nil
		}
	}
	enc, err2 := dbr.db.Get(dbutils.CurrentStateBucket, compositeKey)
	if err2 != nil && !entryNotFound(err2) {
		return nil, err2
	}
	if dbr.storageCache != nil {
		dbr.storageCache.Set(compositeKey, enc)
	}
	return enc, nil
}

func (dbr *DbStateReader) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if dbr.codeCache != nil {
		if code, ok := dbr.codeCache.HasGet(nil, address[:]); ok {
			return code, nil
		}
	}
	code, err := dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	if dbr.codeCache != nil && len(code) <= 1024 {
		dbr.codeCache.Set(address[:], code)
	}
	if dbr.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dbr.codeSizeCache.Set(address[:], b[:])
	}
	return code, err
}

func (dbr *DbStateReader) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (codeSize int, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if dbr.codeSizeCache != nil {
		if b, ok := dbr.codeSizeCache.HasGet(nil, address[:]); ok {
			return int(binary.BigEndian.Uint32(b)), nil
		}
	}
	var code []byte
	code, err = dbr.db.Get(dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	if dbr.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dbr.codeSizeCache.Set(address[:], b[:])
	}
	return len(code), nil
}

func (dbr *DbStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if b, err := dbr.db.Get(dbutils.IncarnationMapBucket, address[:]); err == nil {
		return binary.BigEndian.Uint64(b), nil
	} else if entryNotFound(err) {
		return 0, nil
	} else {
		return 0, err
	}
}
