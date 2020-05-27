// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/changeset"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

func walkAsOfThinAccounts(db KV, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.View(context.Background(), func(tx Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return fmt.Errorf("currentStateBucket not found")
		}
		hB := tx.Bucket(dbutils.AccountsHistoryBucket)
		if hB == nil {
			return fmt.Errorf("accountsHistoryBucket not found")
		}
		csB := tx.Bucket(dbutils.AccountChangeSetBucket)
		if csB == nil {
			return fmt.Errorf("accountChangeBucket not found")
		}
		//for state
		mainCursor := b.Cursor()
		//for historic data
		historyCursor := newSplitCursor(
			hB,
			startkey,
			fixedbits,
			common.HashLength,   /* part1end */
			common.HashLength,   /* part2start */
			common.HashLength+8, /* part3start */
		)
		k, v, err1 := mainCursor.Seek(startkey)
		if err1 != nil {
			return err1
		}
		for k != nil && len(k) > common.HashLength {
			k, v, err1 = mainCursor.Next()
			if err1 != nil {
				return err1
			}
		}
		hK, tsEnc, _, hV, err1 := historyCursor.Seek()
		if err1 != nil {
			return err1
		}
		for hK != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
			hK, tsEnc, _, hV, err1 = historyCursor.Next()
			if err1 != nil {
				return err1
			}
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
			if k == nil {
				if hK == nil {
					break
				} else {
					cmp = 1
				}
			} else if hK == nil {
				cmp = -1
			} else {
				cmp = bytes.Compare(k, hK)
			}
			if cmp < 0 {
				goOn, err = walker(k, v)
			} else {
				index := dbutils.WrapHistoryIndex(hV)
				if changeSetBlock, set, ok := index.Search(timestamp); ok {
					// set == true if this change was from empty record (non-existent account) to non-empty
					// In such case, we do not need to examine changeSet and simply skip the record
					if !set {
						// Extract value from the changeSet
						csKey := dbutils.EncodeTimestamp(changeSetBlock)
						changeSetData, _ := csB.Get(csKey)
						if changeSetData == nil {
							return fmt.Errorf("could not find ChangeSet record for index entry %d (query timestamp %d)", changeSetBlock, timestamp)
						}
						data, err1 := changeset.AccountChangeSetBytes(changeSetData).FindLast(hK)
						if err1 != nil {
							return fmt.Errorf("could not find key %x in the ChangeSet record for index entry %d (query timestamp %d)",
								hK,
								changeSetBlock,
								timestamp,
							)
						}
						if len(data) > 0 { // Skip accounts did not exist
							goOn, err = walker(hK, data)
						}
					}
				} else if cmp == 0 {
					goOn, err = walker(k, v)
				}
			}
			if goOn {
				if cmp <= 0 {
					k, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
					for k != nil && len(k) > common.HashLength {
						k, v, err1 = mainCursor.Next()
						if err1 != nil {
							return err1
						}
					}
				}
				if cmp >= 0 {
					hK0 := hK
					for hK != nil && (bytes.Equal(hK0, hK) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
						hK, tsEnc, _, hV, err1 = historyCursor.Next()
						if err1 != nil {
							return err1
						}
					}
				}
			}
		}
		return err
	})
	return err
}

// splitCursor implements cursor with two keys
// it is used to ignore incarnations in the middle
// of composite storage key, but without
// reconstructing the key
// Instead, the key is split into two parts and
// functions `Seek` and `Next` deliver both
// parts as well as the corresponding value
type splitCursor struct {
	c          Cursor // Unlerlying bolt cursor
	startkey   []byte // Starting key (also contains bits that need to be preserved)
	matchBytes int
	mask       uint8
	part1end   int // Position in the key where the first part ends
	part2start int // Position in the key where the second part starts
	part3start int // Position in the key where the third part starts
}

func newSplitCursor(b Bucket, startkey []byte, matchBits int, part1end, part2start, part3start int) *splitCursor {
	var sc splitCursor
	sc.c = b.Cursor()
	sc.startkey = startkey
	sc.part1end = part1end
	sc.part2start = part2start
	sc.part3start = part3start
	sc.matchBytes, sc.mask = Bytesmask(matchBits)
	return &sc
}

func (sc *splitCursor) matchKey(k []byte) bool {
	if k == nil {
		return false
	}
	if sc.matchBytes == 0 {
		return true
	}
	if len(k) < sc.matchBytes {
		return false
	}
	if !bytes.Equal(k[:sc.matchBytes-1], sc.startkey[:sc.matchBytes-1]) {
		return false
	}
	return (k[sc.matchBytes-1] & sc.mask) == (sc.startkey[sc.matchBytes-1] & sc.mask)
}

func (sc *splitCursor) Seek() (key1, key2, key3, val []byte, err error) {
	k, v, err1 := sc.c.Seek(sc.startkey)
	if err1 != nil {
		return nil, nil, nil, nil, err1
	}
	if !sc.matchKey(k) {
		return nil, nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v, nil
}

func (sc *splitCursor) Next() (key1, key2, key3, val []byte, err error) {
	k, v, err1 := sc.c.Next()
	if err1 != nil {
		return nil, nil, nil, nil, err1
	}
	if !sc.matchKey(k) {
		return nil, nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v, nil
}

func walkAsOfThinStorage(db KV, startkey []byte, fixedbits int, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	err := db.View(context.Background(), func(tx Tx) error {
		b := tx.Bucket(dbutils.CurrentStateBucket)
		if b == nil {
			return fmt.Errorf("storageBucket not found")
		}
		hB := tx.Bucket(dbutils.StorageHistoryBucket)
		if hB == nil {
			return fmt.Errorf("storageHistoryBucket not found")
		}
		csB := tx.Bucket(dbutils.StorageChangeSetBucket)
		if csB == nil {
			return fmt.Errorf("storageChangeBucket not found")
		}
		startkeyNoInc := make([]byte, len(startkey)-common.IncarnationLength)
		copy(startkeyNoInc, startkey[:common.HashLength])
		copy(startkeyNoInc[common.HashLength:], startkey[common.HashLength+common.IncarnationLength:])
		//for storage
		mainCursor := newSplitCursor(
			b,
			startkey,
			fixedbits,
			common.HashLength, /* part1end */
			common.HashLength+common.IncarnationLength,                   /* part2start */
			common.HashLength+common.IncarnationLength+common.HashLength, /* part3start */
		)
		//for historic data
		historyCursor := newSplitCursor(
			hB,
			startkeyNoInc,
			fixedbits-8*common.IncarnationLength,
			common.HashLength,   /* part1end */
			common.HashLength,   /* part2start */
			common.HashLength*2, /* part3start */
		)
		addrHash, keyHash, _, v, err1 := mainCursor.Seek()
		if err1 != nil {
			return err1
		}
		hAddrHash, hKeyHash, tsEnc, hV, err2 := historyCursor.Seek()
		if err2 != nil {
			return err2
		}
		for hKeyHash != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
			hAddrHash, hKeyHash, tsEnc, hV, err2 = historyCursor.Next()
			if err2 != nil {
				return err2
			}
		}
		goOn := true
		var err error
		for goOn {
			var cmp int
			if keyHash == nil {
				if hKeyHash == nil {
					break
				} else {
					cmp = 1
				}
			} else if hKeyHash == nil {
				cmp = -1
			} else {
				cmp = bytes.Compare(keyHash, hKeyHash)
			}
			if cmp < 0 {
				goOn, err = walker(addrHash, keyHash, v)
			} else {
				index := dbutils.WrapHistoryIndex(hV)
				if changeSetBlock, set, ok := index.Search(timestamp); ok {
					// set == true if this change was from empty record (non-existent storage item) to non-empty
					// In such case, we do not need to examine changeSet and simply skip the record
					if !set {
						// Extract value from the changeSet
						csKey := dbutils.EncodeTimestamp(changeSetBlock)
						changeSetData, _ := csB.Get(csKey)
						if changeSetData == nil {
							return fmt.Errorf("could not find ChangeSet record for index entry %d (query timestamp %d)", changeSetBlock, timestamp)
						}
						data, err1 := changeset.StorageChangeSetBytes(changeSetData).FindWithoutIncarnation(hAddrHash, hKeyHash)
						if err1 != nil {
							return fmt.Errorf("could not find key %x%x in the ChangeSet record for index entry %d (query timestamp %d): %v",
								hAddrHash, hKeyHash,
								changeSetBlock,
								timestamp,
								err1,
							)
						}
						if len(data) > 0 { // Skip deleted entries
							goOn, err = walker(hAddrHash, hKeyHash, data)
						}
					}
				} else if cmp == 0 {
					goOn, err = walker(addrHash, keyHash, v)
				}
			}
			if goOn {
				if cmp <= 0 {
					addrHash, keyHash, _, v, err1 = mainCursor.Next()
					if err1 != nil {
						return err1
					}
				}
				if cmp >= 0 {
					hKeyHash0 := hKeyHash
					for hKeyHash != nil && (bytes.Equal(hKeyHash0, hKeyHash) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
						hAddrHash, hKeyHash, tsEnc, hV, err2 = historyCursor.Next()
						if err2 != nil {
							return err2
						}
					}
				}
			}
		}
		return err
	})
	return err
}

func WalkAsOf(db KV, bucket, hBucket, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	//fmt.Printf("WalkAsOf %x %x %x %d %d\n", bucket, hBucket, startkey, fixedbits, timestamp)
	if bytes.Equal(bucket, dbutils.CurrentStateBucket) && bytes.Equal(hBucket, dbutils.AccountsHistoryBucket) {
		return walkAsOfThinAccounts(db, startkey, fixedbits, timestamp, walker)
	} else if bytes.Equal(bucket, dbutils.CurrentStateBucket) && bytes.Equal(hBucket, dbutils.StorageHistoryBucket) {
		return walkAsOfThinStorage(db, startkey, fixedbits, timestamp, func(k1, k2, v []byte) (bool, error) {
			return walker(append(common.CopyBytes(k1), k2...), v)
		})
	}
	panic("Not implemented for arbitrary buckets")
}

var EndSuffix = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

func GetModifiedAccounts(db Getter, startTimestamp, endTimestamp uint64) ([]common.Address, error) {
	keys := make(map[common.Hash]struct{})
	startCode := dbutils.EncodeTimestamp(startTimestamp)
	if err := db.Walk(dbutils.AccountChangeSetBucket, startCode, 0, func(k, v []byte) (bool, error) {
		keyTimestamp, _ := dbutils.DecodeTimestamp(k)

		if keyTimestamp > endTimestamp {
			return false, nil
		}

		walker := func(addrHash, _ []byte) error {
			keys[common.BytesToHash(addrHash)] = struct{}{}
			return nil
		}

		if innerErr := changeset.AccountChangeSetBytes(v).Walk(walker); innerErr != nil {
			return false, innerErr
		}

		return true, nil
	}); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}
	accounts := make([]common.Address, len(keys))
	idx := 0

	for key := range keys {
		value, err := db.Get(dbutils.PreimagePrefix, key[:])
		if err != nil {
			return nil, fmt.Errorf("could not get preimage for key %x", key)
		}
		copy(accounts[idx][:], value)
		idx++
	}
	return accounts, nil
}
