package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
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

	ch := tx.Cursor(hBucket)
	defer ch.Close()
	k, v, seekErr := ch.Seek(dbutils.IndexChunkKey32(key, uint32(timestamp)))
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
	index := roaring.New()
	if _, err := index.FromBuffer(v); err != nil {
		return nil, err
	}
	found, ok := bitmapdb.SeekInBitmap(index, uint32(timestamp))
	changeSetBlock := uint64(found)

	var data []byte
	if ok {
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

// startKey is the concatenation of address and incarnation (BigEndian 8 byte)
func WalkAsOfStorage(tx ethdb.Tx, address common.Address, incarnation uint64, startLocation common.Hash, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	var startkey = make([]byte, common.AddressLength+common.IncarnationLength+common.HashLength)
	copy(startkey, address.Bytes())
	binary.BigEndian.PutUint64(startkey[common.AddressLength:], incarnation)
	copy(startkey[common.AddressLength+common.IncarnationLength:], startLocation.Bytes())

	var startkeyNoInc = make([]byte, common.AddressLength+common.HashLength)
	copy(startkeyNoInc, address.Bytes())
	copy(startkeyNoInc[common.AddressLength:], startLocation.Bytes())

	//for storage
	mCursor := tx.Cursor(dbutils.PlainStateBucket)
	defer mCursor.Close()
	mainCursor := ethdb.NewSplitCursor(
		mCursor,
		startkey,
		8*(common.AddressLength+common.IncarnationLength),
		common.AddressLength,                                            /* part1end */
		common.AddressLength+common.IncarnationLength,                   /* part2start */
		common.AddressLength+common.IncarnationLength+common.HashLength, /* part3start */
	)

	//for historic data
	shCursor := tx.Cursor(dbutils.StorageHistoryBucket)
	defer shCursor.Close()
	var hCursor = ethdb.NewSplitCursor(
		shCursor,
		startkeyNoInc,
		8*common.AddressLength,
		common.AddressLength,                   /* part1end */
		common.AddressLength,                   /* part2start */
		common.AddressLength+common.HashLength, /* part3start */
	)
	csCursor := tx.CursorDupSort(dbutils.PlainStorageChangeSetBucket)
	defer csCursor.Close()

	addr, loc, _, v, err1 := mainCursor.Seek()
	if err1 != nil {
		return err1
	}

	hAddr, hLoc, tsEnc, hV, err2 := hCursor.Seek()
	if err2 != nil {
		return err2
	}
	for hLoc != nil && binary.BigEndian.Uint32(tsEnc) < uint32(timestamp) {
		if hAddr, hLoc, tsEnc, hV, err2 = hCursor.Next(); err2 != nil {
			return err2
		}
	}
	var err error
	goOn := true
	for goOn {
		cmp, br := common.KeyCmp(addr, hAddr)
		if br {
			break
		}
		if cmp == 0 {
			cmp, br = common.KeyCmp(loc, hLoc)
		}
		if br {
			break
		}

		//next key in state
		if cmp < 0 {
			goOn, err = walker(addr, loc, v)
		} else {
			index := roaring.New()
			if _, err = index.FromBuffer(hV); err != nil {
				return err
			}
			found, ok := bitmapdb.SeekInBitmap(index, uint32(timestamp))
			changeSetBlock := uint64(found)

			if ok {
				// Extract value from the changeSet
				csKey := make([]byte, 8+common.AddressLength+common.IncarnationLength)
				copy(csKey[:], dbutils.EncodeBlockNumber(changeSetBlock))
				copy(csKey[8:], address[:]) // address + incarnation
				binary.BigEndian.PutUint64(csKey[8+common.AddressLength:], incarnation)
				kData, data, err3 := csCursor.SeekBothRange(csKey, hLoc)
				if err3 != nil {
					return err3
				}
				if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hLoc) {
					return fmt.Errorf("inconsistent storage changeset and history kData %x, csKey %x, data %x, hLoc %x", kData, csKey, data, hLoc)
				}
				data = data[common.HashLength:]
				if len(data) > 0 { // Skip deleted entries
					goOn, err = walker(hAddr, hLoc, data)
				}
			} else if cmp == 0 {
				goOn, err = walker(addr, loc, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if cmp <= 0 {
				if addr, loc, _, v, err1 = mainCursor.Next(); err1 != nil {
					return err1
				}
			}
			if cmp >= 0 {
				hLoc0 := hLoc
				for hLoc != nil && (bytes.Equal(hLoc0, hLoc) || binary.BigEndian.Uint32(tsEnc) < uint32(timestamp)) {
					if hAddr, hLoc, tsEnc, hV, err2 = hCursor.Next(); err2 != nil {
						return err2
					}
				}
			}
		}
	}
	return nil
}

func WalkAsOfAccounts(tx ethdb.Tx, startAddress common.Address, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	mainCursor := tx.Cursor(dbutils.PlainStateBucket)
	defer mainCursor.Close()
	ahCursor := tx.Cursor(dbutils.AccountsHistoryBucket)
	defer ahCursor.Close()
	var hCursor = ethdb.NewSplitCursor(
		ahCursor,
		startAddress.Bytes(),
		0,                      /* fixedBits */
		common.AddressLength,   /* part1end */
		common.AddressLength,   /* part2start */
		common.AddressLength+8, /* part3start */
	)
	csCursor := tx.CursorDupSort(dbutils.PlainAccountChangeSetBucket)
	defer csCursor.Close()

	k, v, err1 := mainCursor.Seek(startAddress.Bytes())
	if err1 != nil {
		return err1
	}
	for k != nil && len(k) > common.AddressLength {
		k, v, err1 = mainCursor.Next()
		if err1 != nil {
			return err1
		}
	}
	hK, tsEnc, _, hV, err2 := hCursor.Seek()
	if err2 != nil {
		return err2
	}
	for hK != nil && binary.BigEndian.Uint32(tsEnc) < uint32(timestamp) {
		hK, tsEnc, _, hV, err2 = hCursor.Next()
		if err2 != nil {
			return err2
		}
	}

	goOn := true
	var err error
	for goOn {
		//exit or next conditions
		cmp, br := common.KeyCmp(k, hK)
		if br {
			break
		}
		if cmp < 0 {
			goOn, err = walker(k, v)
		} else {
			index := roaring.New()
			_, err = index.FromBuffer(hV)
			if err != nil {
				return err
			}
			found, ok := bitmapdb.SeekInBitmap(index, uint32(timestamp))
			changeSetBlock := uint64(found)
			if ok {
				// Extract value from the changeSet
				csKey := dbutils.EncodeBlockNumber(changeSetBlock)
				kData, data, err3 := csCursor.SeekBothRange(dbutils.EncodeBlockNumber(changeSetBlock), hK)
				if err3 != nil {
					return err3
				}
				if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hK) {
					return fmt.Errorf("inconsistent account history and changesets, kData %x, csKey %x, data %x, hK %x", kData, csKey, data, hK)
				}
				data = data[common.AddressLength:]
				if len(data) > 0 { // Skip accounts did not exist
					goOn, err = walker(hK, data)
				}
			} else if cmp == 0 {
				goOn, err = walker(k, v)
			}
		}
		if err != nil {
			return err
		}
		if goOn {
			if cmp <= 0 {
				k, v, err1 = mainCursor.Next()
				if err1 != nil {
					return err1
				}
				for k != nil && len(k) > common.AddressLength {
					k, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
			if cmp >= 0 {
				hK0 := hK
				for hK != nil && (bytes.Equal(hK0, hK) || binary.BigEndian.Uint32(tsEnc) < uint32(timestamp)) {
					hK, tsEnc, _, hV, err1 = hCursor.Next()
					if err1 != nil {
						return err1
					}
				}
			}
		}
	}
	return err

}
