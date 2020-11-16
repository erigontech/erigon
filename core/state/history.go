package state

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

//MaxChangesetsSearch -
const MaxChangesetsSearch = 256

func GetAsOf(tx ethdb.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	v, err := FindByHistory(tx, storage, key, timestamp)
	if err == nil {
		dat = make([]byte, len(v))
		copy(dat, v)
		return dat, nil
	}
	if !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	v, err = tx.GetOne(dbutils.PlainStateBucket, key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	dat = make([]byte, len(v))
	copy(dat, v)
	return dat, nil
}

func FindByHistory(tx ethdb.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var hBucket string
	if storage {
		hBucket = dbutils.StorageHistoryBucket
	} else {
		hBucket = dbutils.AccountsHistoryBucket
	}
	c := tx.Cursor(hBucket)
	defer c.Close()
	k, v, seekErr := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if seekErr != nil {
		return nil, seekErr
	}
	if k == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	if storage {
		if !bytes.Equal(k[:common.AddressLength], key[:common.AddressLength]) ||
			!bytes.Equal(k[common.AddressLength:common.AddressLength+common.HashLength], key[common.AddressLength+common.IncarnationLength:]) {
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		if !bytes.HasPrefix(k, key) {
			return nil, ethdb.ErrKeyNotFound
		}
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	var data []byte
	if ok {
		// set == true if this change was from empty record (non-existent account) to non-empty
		// In such case, we do not need to examine changeSet and return empty data
		if set && !storage {
			return []byte{}, nil
		}
		csBucket, _ := dbutils.ChangeSetByIndexBucket(storage)
		c := tx.CursorDupSort(csBucket)
		defer c.Close()
		var err error
		if storage {
			data, err = changeset.Mapper[csBucket].WalkerAdapter(c).(changeset.StorageChangeSetPlain).FindWithIncarnation(changeSetBlock, key)
		} else {
			data, err = changeset.Mapper[csBucket].WalkerAdapter(c).Find(changeSetBlock, key)
		}
		if err != nil {
			if !errors.Is(err, changeset.ErrNotFound) {
				return nil, fmt.Errorf("finding %x in the changeset %d: %w", key, changeSetBlock, err)
			}
			return nil, ethdb.ErrKeyNotFound
		}
	} else {
		//fmt.Printf("Not Found changeSetBlock in [%s]\n", index)
		return nil, ethdb.ErrKeyNotFound
	}

	//restore codehash
	if !storage {
		var acc accounts.Account
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			var codeHash []byte
			var err error
			codeHash, err = tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
			if err != nil {
				return nil, err
			}
			if len(codeHash) > 0 {
				acc.CodeHash = common.BytesToHash(codeHash)
			}
			data = make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(data)
		}
		return data, nil
	}

	return data, nil
}

func WalkAsOf(db ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	//fmt.Printf("WalkAsOf %x %x %x %d %d\n", bucket, hBucket, startkey, fixedbits, timestamp)
	if !(bucket == dbutils.PlainStateBucket || bucket == dbutils.CurrentStateBucket) {
		return fmt.Errorf("unsupported state bucket: %s", string(bucket))
	}
	if hBucket == dbutils.AccountsHistoryBucket {
		return walkAsOfThinAccounts(db, bucket, hBucket, startkey, fixedbits, timestamp, walker)
	} else if hBucket == dbutils.StorageHistoryBucket {
		return walkAsOfThinStorage(db, bucket, hBucket, startkey, fixedbits, timestamp, func(k1, k2, v []byte) (bool, error) {
			return walker(append(common.CopyBytes(k1), k2...), v)
		})
	}

	panic(fmt.Sprintf("Not implemented for arbitrary buckets: %s, %s", string(bucket), string(hBucket)))
}

func walkAsOfThinStorage(tx ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	var err error
	startkeyNoInc := dbutils.CompositeKeyWithoutIncarnation(startkey)
	part1End := common.HashLength
	part2Start := common.HashLength + common.IncarnationLength
	part3Start := common.HashLength + common.IncarnationLength + common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength + common.IncarnationLength
		part3Start = common.AddressLength + common.IncarnationLength + common.HashLength
	}

	//for storage
	mainCursor := ethdb.NewSplitCursor(
		tx.Cursor(bucket),
		startkey,
		fixedbits,
		part1End,
		part2Start,
		part3Start,
	)
	fixetBitsForHistory := fixedbits - 8*common.IncarnationLength
	if fixetBitsForHistory < 0 {
		fixetBitsForHistory = 0
	}

	part1End = common.HashLength
	part2Start = common.HashLength
	part3Start = common.HashLength * 2
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength
		part3Start = common.AddressLength + common.HashLength
	}

	//for historic data
	var historyCursor historyCursor = ethdb.NewSplitCursor(
		tx.Cursor(dbutils.StorageHistoryBucket),
		startkeyNoInc,
		fixetBitsForHistory,
		part1End,   /* part1end */
		part2Start, /* part2start */
		part3Start, /* part3start */
	)

	part1End = common.HashLength
	part2Start = common.HashLength + common.IncarnationLength
	part3Start = common.HashLength + common.IncarnationLength + common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength + common.IncarnationLength
		part3Start = common.AddressLength + common.IncarnationLength + common.HashLength
	}

	addrHash, keyHash, _, v, err1 := mainCursor.Seek()
	if err1 != nil {
		return err1
	}

	hAddrHash, hKeyHash, _, hV, err2 := historyCursor.Seek()
	if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
		return err2
	}
	goOn := true
	for goOn {
		cmp, br := common.KeyCmp(addrHash, hAddrHash)
		if br {
			break
		}
		if cmp == 0 {
			cmp, br = common.KeyCmp(keyHash, hKeyHash)
		}
		if br {
			break
		}

		//next key in state
		if cmp < 0 {
			goOn, err = walker(addrHash, keyHash, v)
		} else {
			if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
				return err2
			}
			if len(hV) > 0 && err2 == nil { // Skip accounts did not exist
				goOn, err = walker(hAddrHash, hKeyHash, hV)
			} else if errors.Is(err2, ErrNotInHistory) && cmp == 0 {
				goOn, err = walker(addrHash, keyHash, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if cmp <= 0 {
				addrHash, keyHash, _, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
			}
			if cmp >= 0 {
				hAddrHash, hKeyHash, _, hV, err2 = historyCursor.Next()
				if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
					return err2
				}
			}
		}
	}
	return err
}

func walkAsOfThinAccounts(tx ethdb.Tx, bucket string, hBucket string, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	fixedbytes, mask := ethdb.Bytesmask(fixedbits)

	mainCursor := tx.Cursor(bucket)
	part1End := common.HashLength
	part2Start := common.HashLength
	part3Start := common.HashLength
	maxKeyLen := common.HashLength
	if bucket == dbutils.PlainStateBucket {
		part1End = common.AddressLength
		part2Start = common.AddressLength
		part3Start = common.AddressLength
		maxKeyLen = common.AddressLength
	}

	var hCursor historyCursor = ethdb.NewSplitCursor(
		tx.Cursor(dbutils.AccountsHistoryBucket),
		startkey,
		fixedbits,
		part1End,   /* part1end */
		part2Start, /* part2start */
		part3Start, /* part3start */
	)

	k, v, err1 := mainCursor.Seek(startkey)
	if err1 != nil {
		return err1
	}
	for k != nil && len(k) > maxKeyLen {
		k, v, err1 = mainCursor.Next()
		if err1 != nil {
			return err1
		}
	}
	hK, _, _, hV, err2 := hCursor.Seek()
	if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
		return err2
	}

	goOn := true
	var err error
	for goOn {
		//exit or next conditions
		if k != nil && fixedbits > 0 && !bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) {
			k = nil
		}
		if k != nil && fixedbits > 0 && (k[fixedbytes-1]&mask) != (startkey[fixedbytes-1]&mask) {
			k = nil
		}
		var cmp int
		cmp, br := common.KeyCmp(k, hK)
		if br {
			break
		}
		if cmp < 0 {
			goOn, err = walker(k, v)
		} else {
			if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
				return err2
			}
			if len(hV) > 0 && err2 == nil { // Skip accounts did not exist
				goOn, err = walker(hK, hV)
			} else if errors.Is(err2, ErrNotInHistory) && cmp == 0 {
				goOn, err = walker(k, v)
			}
		}
		if goOn {
			if cmp <= 0 {
				k, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
				for k != nil && len(k) > maxKeyLen {
					k, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
			if cmp >= 0 {
				hK, _, _, hV, err2 = hCursor.Next()
				if err2 != nil && !errors.Is(err2, ErrNotInHistory) {
					return err2
				}
			}
		}
	}
	return err

}

func findInHistory(hK, hV []byte, timestamp uint64, csWalker changeset.Walker2) ([]byte, bool, error) {
	index := dbutils.WrapHistoryIndex(hV)
	if changeSetBlock, set, ok := index.Search(timestamp); ok {
		// set == true if this change was from empty record (non-existent account) to non-empty
		// In such case, we do not need to examine changeSet and simply skip the record
		if !set {
			// Extract value from the changeSet
			data, err := csWalker.Find(changeSetBlock, hK)
			if err != nil {
				return nil, false, fmt.Errorf("could not find key %x in the ChangeSet record for index entry %d (query timestamp %d): %v",
					hK,
					changeSetBlock,
					timestamp,
					err)
			}
			return data, true, nil
		}
		return nil, true, nil
	}
	return nil, false, nil
}

type historyCursor interface {
	Seek() (key1, key2, key3, val []byte, err error)
	Next() (key1, key2, key3, val []byte, err error)
}

var ErrNotInHistory = errors.New("not in history")
