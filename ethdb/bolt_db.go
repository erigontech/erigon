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
	"github.com/ledgerwatch/turbo-geth/common"
	"os"
	"path"

	"github.com/ledgerwatch/turbo-geth/common/debug"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/bolt"
)

var OpenFileLimit = 64

const HeapSize = 512 * 1024 * 1024

// BoltDatabase is a wrapper over BoltDb,
// compatible with the Database interface.
type BoltDatabase struct {
	db  *bolt.DB   // BoltDB instance
	log log.Logger // Contextual logger tracking the database path
	id  uint64
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
	db, err := bolt.Open(file, 0600, &bolt.Options{})
	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
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
		b, err := tx.CreateBucketIfNotExists(bucket, true)
		if err != nil {
			return err
		}
		return b.Put(key, value)
	})
	return err
}

// PutS adds a new entry to the historical buckets:
// hBucket (unless changeSetBucketOnly) and ChangeSet.
func (db *BoltDatabase) PutS(hBucket, key, value []byte, timestamp uint64, changeSetBucketOnly bool) error {
	composite, encodedTS := dbutils.CompositeKeySuffix(key, timestamp)
	changeSetKey := dbutils.CompositeChangeSetKey(encodedTS, hBucket)
	err := db.db.Update(func(tx *bolt.Tx) error {
		if !changeSetBucketOnly {
			hb, err := tx.CreateBucketIfNotExists(hBucket, true)
			if err != nil {
				return err
			}
			switch {
			case debug.IsThinHistory() && bytes.Equal(hBucket, dbutils.AccountsHistoryBucket):
				b, _ := hb.Get(key)
				b, err = AppendToIndex(b, timestamp)
				if err != nil {
					log.Error("PutS AppendChangedOnIndex err", "err", err)
					return err
				}
				if err = hb.Put(key, b); err != nil {
					return err
				}
			case debug.IsThinHistory() && bytes.Equal(hBucket, dbutils.StorageHistoryBucket):
				b, _ := hb.Get(key[:common.HashLength+common.IncarnationLength])
				b, err = AppendToStorageIndex(b, key[common.HashLength+common.IncarnationLength:common.HashLength+common.IncarnationLength+common.HashLength], timestamp)
				if err != nil {
					log.Error("PutS AppendChangedOnIndex err", "err", err)
					return err
				}
				if err = hb.Put(key, b); err != nil {
					return err
				}
			default:
				if err = hb.Put(composite, value); err != nil {
					return err
				}
			}
		}

		sb, err := tx.CreateBucketIfNotExists(dbutils.ChangeSetBucket, true)
		if err != nil {
			return err
		}

		dat, _ := sb.Get(changeSetKey)
		dat, err = addToChangeSet(dat, key, value)
		if err != nil {
			log.Error("PutS DecodeChangeSet changeSet err", "err", err)
			return err
		}
		// s.Sort(dat) not sorting it here. seems that this Puts is only for testing.
		return sb.Put(changeSetKey, dat)
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

// getChangeSetByBlockNoLock returns changeset by block and bucket
func (db *BoltDatabase) GetChangeSetByBlock(hBucket []byte, timestamp uint64) ([]byte, error) {
	key := dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(timestamp), hBucket)
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbutils.ChangeSetBucket)
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
		switch {
		case debug.IsThinHistory() && bytes.Equal(hBucket, dbutils.AccountsHistoryBucket):
			v, err := BoltDBFindByHistory(tx, hBucket, key, timestamp)
			if err != nil {
				log.Debug("BoltDB BoltDBFindByHistory err", "err", err)
			} else {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		case debug.IsThinHistory() && bytes.Equal(hBucket, dbutils.StorageHistoryBucket):
			v, err := BoltDBFindStorageByHistory(tx, hBucket, key, timestamp)
			if err != nil {
				log.Debug("BoltDB BoltDBFindStorageByHistory err", "err", err)
			} else {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		default:
			composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
			hB := tx.Bucket(hBucket)
			if hB == nil {
				return ErrKeyNotFound
			}
			hC := hB.Cursor()
			hK, hV := hC.Seek(composite)
			if hK != nil && bytes.HasPrefix(hK, key) {
				dat = make([]byte, len(hV))
				copy(dat, hV)
				return nil
			}
		}
		{
			b := tx.Bucket(bucket)
			if b == nil {
				return ErrKeyNotFound
			}
			c := b.Cursor()

			k, v := c.Seek(key)
			if k != nil && bytes.Equal(k, key) {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		}

		return ErrKeyNotFound
	})
	return dat, err
}

func Bytesmask(fixedbits uint) (fixedbytes int, mask byte) {
	fixedbytes = int((fixedbits + 7) / 8)
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

func (db *BoltDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
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

func (db *BoltDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) error) error {
	if len(startkeys) == 0 {
		return nil
	}
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

func (db *BoltDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func(k []byte, v []byte) (bool, error)) error {
	if debug.IsThinHistory() {
		panic("WalkAsOf")
	}

	fixedbytes, mask := Bytesmask(fixedbits)
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(encodedTS)
	keyBuffer := make([]byte, l+len(EndSuffix))
	err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		hB := tx.Bucket(hBucket)
		if hB == nil {
			return nil
		}
		//for state
		mainCursor := b.Cursor()
		//for historic data
		historyCursor := hB.Cursor()
		k, v := mainCursor.Seek(startkey)
		hK, hV := historyCursor.Seek(startkey)
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
			if hK != nil && fixedbits > 0 && !bytes.Equal(hK[:fixedbytes-1], startkey[:fixedbytes-1]) {
				hK = nil
			}
			if hK != nil && fixedbits > 0 && (hK[fixedbytes-1]&mask) != (startkey[fixedbytes-1]&mask) {
				hK = nil
			}

			// historical key points to an old block
			if hK != nil && bytes.Compare(hK[l:], encodedTS) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], encodedTS)
				// update historical key/value to the desired block
				hK, hV = historyCursor.SeekTo(keyBuffer[:sl])
				continue
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
				cmp = bytes.Compare(k, hK[:l])
			}
			if cmp < 0 {
				goOn, err = walker(k, v)
			} else {
				goOn, err = walker(hK[:l], hV)
			}
			if goOn {
				if cmp <= 0 {
					k, v = mainCursor.Next()
				}
				if cmp >= 0 {
					copy(keyBuffer, hK[:l])
					copy(keyBuffer[l:], EndSuffix)
					hK, hV = historyCursor.SeekTo(keyBuffer)
				}
			}
		}
		return err
	})
	return err
}

func (db *BoltDatabase) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) error) error {
	if debug.IsThinHistory() {
		panic("MultiWalkAsOf")
	}

	if len(startkeys) == 0 {
		return nil
	}
	keyIdx := 0 // What is the current key we are extracting
	fixedbytes, mask := Bytesmask(fixedbits[keyIdx])
	startkey := startkeys[keyIdx]
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(encodedTS)
	keyBuffer := make([]byte, l+len(EndSuffix))
	if err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		hB := tx.Bucket(hBucket)
		if hB == nil {
			return nil
		}
		mainCursor := b.Cursor()
		historyCursor := hB.Cursor()
		additionalHistoryCursor := hB.Cursor()
		k, v := mainCursor.Seek(startkey)
		hK, hV := historyCursor.Seek(startkey)
		goOn := true
		var err error
		for goOn { // k != nil
			fit := k != nil
			hKFit := hK != nil
			if fixedbytes > 0 {
				cmp := 1
				hCmp := 1
				for cmp != 0 && hCmp != 0 {
					if k != nil {
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
							k, v = mainCursor.SeekTo(startkey)
							if k == nil {
								cmp = 1
							}
						}
					}
					if hK != nil {
						hCmp = bytes.Compare(hK[:fixedbytes-1], startkey[:fixedbytes-1])
						if hCmp == 0 {
							k1 := hK[fixedbytes-1] & mask
							k2 := startkey[fixedbytes-1] & mask
							if k1 < k2 {
								hCmp = -1
							} else if k1 > k2 {
								hCmp = 1
							}
						}
						if hCmp < 0 {
							hK, hV = historyCursor.SeekTo(startkey)
							if hK == nil {
								hCmp = 1
							}
						}
					}
					if cmp > 0 && hCmp > 0 {
						keyIdx++
						if keyIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = Bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
					}
				}
				fit = cmp == 0
				hKFit = hCmp == 0
			}
			if hKFit && bytes.Compare(hK[l:], encodedTS) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], encodedTS)
				hK, hV = historyCursor.SeekTo(keyBuffer[:sl])
				continue
			}
			var cmp int
			if !fit {
				if !hKFit {
					break
				} else {
					cmp = 1
				}
			} else if !hKFit {
				cmp = -1
			} else {
				cmp = bytes.Compare(k, hK[:l])
			}
			if cmp < 0 {
				//additional check to keep historyCursor on the same position
				hK1, _ := additionalHistoryCursor.Seek(k)
				if bytes.HasPrefix(hK1, k) {
					err = walker(keyIdx, k, v)
					goOn = err == nil
				}
			} else {
				err = walker(keyIdx, hK[:l], hV)
				goOn = err == nil
			}
			if goOn {
				if cmp <= 0 {
					k, v = mainCursor.Next()
				}
				if cmp >= 0 {
					copy(keyBuffer, hK[:l])
					copy(keyBuffer[l:], EndSuffix)
					hK, hV = historyCursor.SeekTo(keyBuffer)
				}
			}
		}
		return err
	}); err != nil {
		return err
	}
	return nil
}

func (db *BoltDatabase) RewindData(timestampSrc, timestampDst uint64, df func(hBucket, key, value []byte) error) error {
	return RewindData(db, timestampSrc, timestampDst, df)
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

// DeleteTimestamp removes data for a given timestamp (block number)
// from all historical buckets (incl. ChangeSet).
func (db *BoltDatabase) DeleteTimestamp(timestamp uint64) error {
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	err := db.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(dbutils.ChangeSetBucket)
		if sb == nil {
			return nil
		}
		var keys [][]byte
		c := sb.Cursor()
		for k, v := c.Seek(encodedTS); k != nil && bytes.HasPrefix(k, encodedTS); k, v = c.Next() {
			// k = encodedTS + hBucket
			hb := tx.Bucket(k[len(encodedTS):])
			if hb == nil {
				return nil
			}
			err := dbutils.Walk(v, func(kk, _ []byte) error {
				kk = append(kk, encodedTS...)
				return hb.Delete(kk)
			})
			if err != nil {
				return err
			}
			keys = append(keys, k)
		}
		for _, k := range keys {
			if err := sb.Delete(k); err != nil {
				return err
			}
		}
		return nil
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

func (db *BoltDatabase) DB() *bolt.DB {
	return db.db
}

func (db *BoltDatabase) NewBatch() DbWithPendingMutations {
	m := &mutation{
		db:               db,
		puts:             newPuts(),
		changeSetByBlock: make(map[uint64]map[string]*dbutils.ChangeSet),
	}
	return m
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (db *BoltDatabase) IdealBatchSize() int {
	return 100 * 1024
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
