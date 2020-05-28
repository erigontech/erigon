// Copyright 2014 The go-ethereum Authors
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

// Package ethdb defines the interfaces for an Ethereum data store.
package ethdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/log"
)

var OpenFileLimit = 64

const HeapSize = 512 * 1024 * 1024

// BoltDatabase is a wrapper over BoltDb,
// compatible with the Database interface.
type BoltDatabase struct {
	db  *bolt.DB   // BoltDB instance
	log log.Logger // Contextual logger tracking the database path
	id  uint64

	stopNetInterface context.CancelFunc
	netAddr          string
}

// NewBoltDatabase returns a BoltDB wrapper.
func NewWrapperBoltDatabase(db *bolt.DB) *BoltDatabase {
	logger := log.New()
	return &BoltDatabase{
		db:  db,
		log: logger,
		id:  id(),
	}
}

// NewBoltDatabase returns a BoltDB wrapper.
func NewBoltDatabase(file string) (*BoltDatabase, error) {
	logger := log.New("database", file)

	// Create necessary directories
	if err := os.MkdirAll(path.Dir(file), os.ModePerm); err != nil {
		return nil, err
	}
	// Open the db and recover any potential corruptions
	db, errOpen := bolt.Open(file, 0600, &bolt.Options{KeysPrefixCompressionDisable: true})
	// (Re)check for errors and abort if opening of the db failed
	if errOpen != nil {
		return nil, errOpen
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range dbutils.Buckets {
			if _, err := tx.CreateBucketIfNotExists(bucket, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &BoltDatabase{
		db:  db,
		log: logger,
		id:  id(),
	}, nil
}

// Put inserts or updates a single entry.
func (db *BoltDatabase) Put(bucket, key []byte, value []byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket, false)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
	return err
}

func (db *BoltDatabase) MultiPut(tuples ...[]byte) (uint64, error) {
	var savedTx *bolt.Tx
	err := db.db.Update(func(tx *bolt.Tx) error {
		for bucketStart := 0; bucketStart < len(tuples); {
			bucketEnd := bucketStart
			for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
			}
			b, err := tx.CreateBucketIfNotExists(tuples[bucketStart], false)
			if err != nil {
				return err
			}
			l := (bucketEnd - bucketStart) / 3
			pairs := make([][]byte, 2*l)
			for i := 0; i < l; i++ {
				pairs[2*i] = tuples[bucketStart+3*i+1]
				pairs[2*i+1] = tuples[bucketStart+3*i+2]
			}
			if err := b.MultiPut(pairs...); err != nil {
				return err
			}
			bucketStart = bucketEnd
		}
		savedTx = tx
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint64(savedTx.Stats().Write), nil
}

// Type which expecting sequence of triplets: bucket, key, value, ....
// It sorts entries by bucket name, then inside bucket clusters sort by keys
type MultiPutTuples [][]byte

func (t MultiPutTuples) Len() int { return len(t) / 3 }

func (t MultiPutTuples) Less(i, j int) bool {
	cmp := bytes.Compare(t[i*3], t[j*3])
	if cmp == -1 {
		return true
	}
	if cmp == 0 {
		return bytes.Compare(t[i*3+1], t[j*3+1]) == -1
	}
	return false
}

func (t MultiPutTuples) Swap(i, j int) {
	t[i*3], t[j*3] = t[j*3], t[i*3]
	t[i*3+1], t[j*3+1] = t[j*3+1], t[i*3+1]
	t[i*3+2], t[j*3+2] = t[j*3+2], t[i*3+2]
}

func (db *BoltDatabase) Has(bucket, key []byte) (bool, error) {
	var has bool
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			has = false
		} else {
			v, _ := b.Get(key)
			has = v != nil
		}
		return nil
	})
	return has, err
}

func (db *BoltDatabase) DiskSize() int64 {
	return int64(db.db.Size())
}

// Get returns the value for a given key if it's present.
func (db *BoltDatabase) Get(bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v, _ := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}


func (db *BoltDatabase) GetLastKey(bucket []byte) ([]byte, error) {
	var last []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b!=nil {
			l,_:=b.Cursor().Last()
			last = common.CopyBytes(l)
		}

		return nil
	})
	if last == nil {
		return nil, ErrKeyNotFound
	}
	return last, err
}


// GetIndexChunk returns proper index chunk or return error if index is not created.
// key must contain inverted block number in the end
func (db *BoltDatabase) GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			c := b.Cursor()
			k, v := c.Seek(dbutils.IndexChunkKey(key, timestamp))
			if !bytes.HasPrefix(k, dbutils.CompositeKeyWithoutIncarnation(key)) {
				return ErrKeyNotFound
			}
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

// getChangeSetByBlockNoLock returns changeset by block and bucket
func (db *BoltDatabase) GetChangeSetByBlock(hBucket []byte, timestamp uint64) ([]byte, error) {
	key := dbutils.EncodeTimestamp(timestamp)

	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket))
		if b == nil {
			return nil
		}

		v, _ := b.Get(key)
		if v != nil {
			dat = make([]byte, len(v))
			copy(dat, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dat, nil
}

// GetAsOf returns the value valid as of a given timestamp.
func (db *BoltDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		v, err := BoltDBFindByHistory(tx, hBucket, key, timestamp)
		if err != nil {
			log.Debug("BoltDB BoltDBFindByHistory err", "err", err)
		} else {
			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
		{
			v, _ := tx.Bucket(bucket).Get(key)
			if v == nil {
				return ErrKeyNotFound
			}

			dat = make([]byte, len(v))
			copy(dat, v)
			return nil
		}
	})
	return dat, err
}

func HackAddRootToAccountBytes(accNoRoot []byte, root []byte) (accWithRoot []byte, err error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(accNoRoot); err != nil {
		return nil, err
	}
	acc.Root = common.BytesToHash(root)
	accWithRoot = make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(accWithRoot)
	return accWithRoot, nil
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

func (db *BoltDatabase) Walk(bucket, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
			goOn, err := walker(k, v)
			if err != nil {
				return err
			}
			if !goOn {
				break
			}
			k, v = c.Next()
		}
		return nil
	})
	return err
}

func (db *BoltDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil {
			// Adjust rangeIdx if needed
			if fixedbytes > 0 {
				cmp := int(-1)
				for cmp != 0 {
					cmp = bytes.Compare(k[:fixedbytes-1], startkey[:fixedbytes-1])
					if cmp == 0 {
						k1 := k[fixedbytes-1] & mask
						k2 := startkey[fixedbytes-1] & mask
						if k1 < k2 {
							cmp = -1
						} else if k1 > k2 {
							cmp = 1
						}
					}
					if cmp < 0 {
						k, v = c.SeekTo(startkey)
						if k == nil {
							return nil
						}
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
					}
				}
			}
			if len(v) > 0 {
				if err := walker(rangeIdx, k, v); err != nil {
					return err
				}
			}
			k, v = c.Next()
		}
		return nil
	})
	return err
}

func (db *BoltDatabase) walkAsOfThinAccounts(startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.db.View(func(tx *bolt.Tx) error {
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
		k, v := mainCursor.Seek(startkey)
		for k != nil && len(k) > common.HashLength {
			k, v = mainCursor.Next()
		}
		hK, tsEnc, _, hV := historyCursor.Seek()
		for hK != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
			hK, tsEnc, _, hV = historyCursor.Next()
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
					k, v = mainCursor.Next()
					for k != nil && len(k) > common.HashLength {
						k, v = mainCursor.Next()
					}
				}
				if cmp >= 0 {
					hK0 := hK
					for hK != nil && (bytes.Equal(hK0, hK) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
						hK, tsEnc, _, hV = historyCursor.Next()
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
	c          *bolt.Cursor // Unlerlying bolt cursor
	startkey   []byte       // Starting key (also contains bits that need to be preserved)
	matchBytes int
	mask       uint8
	part1end   int // Position in the key where the first part ends
	part2start int // Position in the key where the second part starts
	part3start int // Position in the key where the third part starts
}

func newSplitCursor(b *bolt.Bucket, startkey []byte, matchBits int, part1end, part2start, part3start int) *splitCursor {
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

func (sc *splitCursor) Seek() (key1, key2, key3, val []byte) {
	k, v := sc.c.Seek(sc.startkey)
	if !sc.matchKey(k) {
		return nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v
}

func (sc *splitCursor) Next() (key1, key2, key3, val []byte) {
	k, v := sc.c.Next()
	if !sc.matchKey(k) {
		return nil, nil, nil, nil
	}
	return k[:sc.part1end], k[sc.part2start:sc.part3start], k[sc.part3start:], v
}

func (db *BoltDatabase) walkAsOfThinStorage(startkey []byte, fixedbits int, timestamp uint64, walker func(k1, k2, v []byte) (bool, error)) error {
	err := db.db.View(func(tx *bolt.Tx) error {
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
		addrHash, keyHash, _, v := mainCursor.Seek()
		hAddrHash, hKeyHash, tsEnc, hV := historyCursor.Seek()
		for hKeyHash != nil && binary.BigEndian.Uint64(tsEnc) < timestamp {
			hAddrHash, hKeyHash, tsEnc, hV = historyCursor.Next()
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
					addrHash, keyHash, _, v = mainCursor.Next()
				}
				if cmp >= 0 {
					hKeyHash0 := hKeyHash
					for hKeyHash != nil && (bytes.Equal(hKeyHash0, hKeyHash) || binary.BigEndian.Uint64(tsEnc) < timestamp) {
						hAddrHash, hKeyHash, tsEnc, hV = historyCursor.Next()
					}
				}
			}
		}
		return err
	})
	return err
}

func (db *BoltDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits int, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	//fmt.Printf("WalkAsOf %x %x %x %d %d\n", bucket, hBucket, startkey, fixedbits, timestamp)
	if bytes.Equal(bucket, dbutils.CurrentStateBucket) && bytes.Equal(hBucket, dbutils.AccountsHistoryBucket) {
		return db.walkAsOfThinAccounts(startkey, fixedbits, timestamp, walker)
	} else if bytes.Equal(bucket, dbutils.CurrentStateBucket) && bytes.Equal(hBucket, dbutils.StorageHistoryBucket) {
		return db.walkAsOfThinStorage(startkey, fixedbits, timestamp, func(k1, k2, v []byte) (bool, error) {
			return walker(append(common.CopyBytes(k1), k2...), v)
		})
	}
	panic("Not implemented for arbitrary buckets")
}

// Delete deletes the key from the queue and database
func (db *BoltDatabase) Delete(bucket, key []byte) error {
	// Execute the actual operation
	err := db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			return b.Delete(key)
		} else {
			return nil
		}
	})
	return err
}

func (db *BoltDatabase) DeleteBucket(bucket []byte) error {
	err := db.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucket); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (db *BoltDatabase) Close() {
	if err := db.db.Close(); err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}

func (db *BoltDatabase) Keys() ([][]byte, error) {
	var keys [][]byte
	err := db.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			var nameCopy = make([]byte, len(name))
			copy(nameCopy, name)
			return b.ForEach(func(k, _ []byte) error {
				var kCopy = make([]byte, len(k))
				copy(kCopy, k)
				keys = append(append(keys, nameCopy), kCopy)
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

func (db *BoltDatabase) KV() *bolt.DB {
	return db.db
}

func (db *BoltDatabase) AbstractKV() KV {
	return &BoltKV{bolt: db.db}
}

func (db *BoltDatabase) NewBatch() DbWithPendingMutations {
	m := &mutation{
		db:   db,
		puts: newPuts(),
	}
	return m
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (db *BoltDatabase) IdealBatchSize() int {
	return 50 * 1024 * 1024 // 50 Mb
}

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (db *BoltDatabase) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (db *BoltDatabase) TruncateAncients(items uint64) error {
	return errNotSupported
}

func (db *BoltDatabase) ID() uint64 {
	return db.id
}

func InspectDatabase(db Database) error {
	// FIXME: implement in Turbo-Geth
	// see https://github.com/ethereum/go-ethereum/blob/f5d89cdb72c1e82e9deb54754bef8dd20bf12591/core/rawdb/database.go#L224
	return errNotSupported
}

func NewDatabaseWithFreezer(db Database, dir, suffix string) (Database, error) {
	// FIXME: implement freezer in Turbo-Geth
	return db, nil
}

func BoltDBFindByHistory(tx *bolt.Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	var keyF []byte
	if bytes.Equal(dbutils.StorageHistoryBucket, hBucket) {
		keyF = make([]byte, len(key)-common.IncarnationLength)
		copy(keyF, key[:common.HashLength])
		copy(keyF[common.HashLength:], key[common.HashLength+common.IncarnationLength:])
	} else {
		keyF = common.CopyBytes(key)
	}

	c := hB.Cursor()
	k, v := c.Seek(dbutils.IndexChunkKey(key, timestamp))
	if !bytes.HasPrefix(k, keyF) {
		return nil, ErrKeyNotFound
	}
	index := dbutils.WrapHistoryIndex(v)

	changeSetBlock, set, ok := index.Search(timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}
	// set == true if this change was from empty record (non-existent account) to non-empty
	// In such case, we do not need to examine changeSet and return empty data
	if set {
		return []byte{}, nil
	}
	csB := tx.Bucket(dbutils.ChangeSetByIndexBucket(hBucket))
	if csB == nil {
		return nil, ErrKeyNotFound
	}

	csKey := dbutils.EncodeTimestamp(changeSetBlock)
	changeSetData, _ := csB.Get(csKey)

	var (
		data []byte
		err  error
	)
	switch {
	case bytes.Equal(dbutils.AccountsHistoryBucket, hBucket):
		data, err = changeset.AccountChangeSetBytes(changeSetData).FindLast(key)
	case bytes.Equal(dbutils.StorageHistoryBucket, hBucket):
		data, err = changeset.StorageChangeSetBytes(changeSetData).FindWithoutIncarnation(key[:common.HashLength], key[common.HashLength+common.IncarnationLength:])
	}
	if err != nil {
		return nil, ErrKeyNotFound
	}

	//restore codehash
	if bytes.Equal(dbutils.AccountsHistoryBucket, hBucket) {
		var acc accounts.Account
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			codeBucket := tx.Bucket(dbutils.ContractCodeBucket)
			codeHash, _ := codeBucket.Get(dbutils.GenerateStoragePrefix(key, acc.Incarnation))
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
