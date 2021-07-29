package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
)

func GetAsOf(tx kv.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	v, err := FindByHistory(tx, storage, key, timestamp)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	return tx.GetOne(kv.PlainState, key)
}

func FindByHistory(tx kv.Tx, storage bool, key []byte, timestamp uint64) ([]byte, error) {
	var csBucket string
	if storage {
		csBucket = kv.StorageChangeSet
	} else {
		csBucket = kv.AccountChangeSet
	}

	ch, err := tx.Cursor(changeset.Mapper[csBucket].IndexBucket)
	if err != nil {
		return nil, err
	}
	defer ch.Close()
	k, v, seekErr := ch.Seek(changeset.Mapper[csBucket].IndexChunkKey(key, timestamp))
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
	index := roaring64.New()
	if _, err := index.ReadFrom(bytes.NewReader(v)); err != nil {
		return nil, err
	}
	found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
	changeSetBlock := found

	var data []byte
	if ok {
		c, err := tx.CursorDupSort(csBucket)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		if storage {
			data, err = changeset.Mapper[csBucket].Find(c, changeSetBlock, key)
		} else {
			data, err = changeset.Mapper[csBucket].Find(c, changeSetBlock, key)
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
			codeHash, err = tx.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(key, acc.Incarnation))
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
func WalkAsOfStorage(tx kv.Tx, address common.Address, incarnation uint64, startLocation common.Hash, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	var startkey = make([]byte, common.AddressLength+common.IncarnationLength+common.HashLength)
	copy(startkey, address.Bytes())
	binary.BigEndian.PutUint64(startkey[common.AddressLength:], incarnation)
	copy(startkey[common.AddressLength+common.IncarnationLength:], startLocation.Bytes())

	var startkeyNoInc = make([]byte, common.AddressLength+common.HashLength)
	copy(startkeyNoInc, address.Bytes())
	copy(startkeyNoInc[common.AddressLength:], startLocation.Bytes())

	//for storage
	mCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
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
	shCursor, err := tx.Cursor(kv.StorageHistory)
	if err != nil {
		return err
	}
	defer shCursor.Close()
	var hCursor = ethdb.NewSplitCursor(
		shCursor,
		startkeyNoInc,
		8*common.AddressLength,
		common.AddressLength,                   /* part1end */
		common.AddressLength,                   /* part2start */
		common.AddressLength+common.HashLength, /* part3start */
	)
	csCursor, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return err
	}
	defer csCursor.Close()

	addr, loc, _, v, err1 := mainCursor.Seek()
	if err1 != nil {
		return err1
	}

	hAddr, hLoc, tsEnc, hV, err2 := hCursor.Seek()
	if err2 != nil {
		return err2
	}
	for hLoc != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
		if hAddr, hLoc, tsEnc, hV, err2 = hCursor.Next(); err2 != nil {
			return err2
		}
	}
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
			index := roaring64.New()
			if _, err = index.ReadFrom(bytes.NewReader(hV)); err != nil {
				return err
			}
			found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
			changeSetBlock := found

			if ok {
				// Extract value from the changeSet
				csKey := make([]byte, 8+common.AddressLength+common.IncarnationLength)
				copy(csKey, dbutils.EncodeBlockNumber(changeSetBlock))
				copy(csKey[8:], address[:]) // address + incarnation
				binary.BigEndian.PutUint64(csKey[8+common.AddressLength:], incarnation)
				kData := csKey
				data, err3 := csCursor.SeekBothRange(csKey, hLoc)
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
				for hLoc != nil && (bytes.Equal(hLoc0, hLoc) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
					if hAddr, hLoc, tsEnc, hV, err2 = hCursor.Next(); err2 != nil {
						return err2
					}
				}
			}
		}
	}
	return nil
}

func WalkAsOfAccounts(tx kv.Tx, startAddress common.Address, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	mainCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer mainCursor.Close()
	ahCursor, err := tx.Cursor(kv.AccountsHistory)
	if err != nil {
		return err
	}
	defer ahCursor.Close()
	var hCursor = ethdb.NewSplitCursor(
		ahCursor,
		startAddress.Bytes(),
		0,                      /* fixedBits */
		common.AddressLength,   /* part1end */
		common.AddressLength,   /* part2start */
		common.AddressLength+8, /* part3start */
	)
	csCursor, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
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
	for hK != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
		hK, tsEnc, _, hV, err2 = hCursor.Next()
		if err2 != nil {
			return err2
		}
	}

	goOn := true
	for goOn {
		//exit or next conditions
		cmp, br := common.KeyCmp(k, hK)
		if br {
			break
		}
		if cmp < 0 {
			goOn, err = walker(k, v)
		} else {
			index := roaring64.New()
			_, err = index.ReadFrom(bytes.NewReader(hV))
			if err != nil {
				return err
			}
			found, ok := bitmapdb.SeekInBitmap64(index, timestamp)
			changeSetBlock := found
			if ok {
				// Extract value from the changeSet
				csKey := dbutils.EncodeBlockNumber(changeSetBlock)
				kData := csKey
				data, err3 := csCursor.SeekBothRange(csKey, hK)
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
				for hK != nil && (bytes.Equal(hK0, hK) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
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
