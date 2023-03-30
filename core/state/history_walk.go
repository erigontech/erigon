package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

// startKey is the concatenation of address and incarnation (BigEndian 8 byte)
func WalkAsOfStorage(tx kv.Tx, address libcommon.Address, incarnation uint64, startLocation libcommon.Hash, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	var startkey = make([]byte, length.Addr+length.Incarnation+length.Hash)
	copy(startkey, address.Bytes())
	binary.BigEndian.PutUint64(startkey[length.Addr:], incarnation)
	copy(startkey[length.Addr+length.Incarnation:], startLocation.Bytes())

	var startkeyNoInc = make([]byte, length.Addr+length.Hash)
	copy(startkeyNoInc, address.Bytes())
	copy(startkeyNoInc[length.Addr:], startLocation.Bytes())

	//for storage
	mCursor, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	defer mCursor.Close()
	mainCursor := ethdb.NewSplitCursor(
		mCursor,
		startkey,
		8*(length.Addr+length.Incarnation),
		length.Addr,                    /* part1end */
		length.Addr+length.Incarnation, /* part2start */
		length.Addr+length.Incarnation+length.Hash, /* part3start */
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
		8*length.Addr,
		length.Addr,             /* part1end */
		length.Addr,             /* part2start */
		length.Addr+length.Hash, /* part3start */
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
				csKey := make([]byte, 8+length.Addr+length.Incarnation)
				copy(csKey, dbutils.EncodeBlockNumber(changeSetBlock))
				copy(csKey[8:], address[:]) // address + incarnation
				binary.BigEndian.PutUint64(csKey[8+length.Addr:], incarnation)
				kData := csKey
				data, err3 := csCursor.SeekBothRange(csKey, hLoc)
				if err3 != nil {
					return err3
				}
				if !bytes.Equal(kData, csKey) || !bytes.HasPrefix(data, hLoc) {
					return fmt.Errorf("inconsistent storage changeset and history kData %x, csKey %x, data %x, hLoc %x", kData, csKey, data, hLoc)
				}
				data = data[length.Hash:]
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

func WalkAsOfAccounts(tx kv.Tx, startAddress libcommon.Address, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
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
		0,             /* fixedBits */
		length.Addr,   /* part1end */
		length.Addr,   /* part2start */
		length.Addr+8, /* part3start */
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
	for k != nil && len(k) > length.Addr {
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
				data = data[length.Addr:]
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
				for k != nil && len(k) > length.Addr {
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
