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

// +build !js

package ethdb

import (
	"bytes"
	"errors"
	"os"
	"path"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/bolt"
	"github.com/petar/GoLLRB/llrb"
)

var OpenFileLimit = 64
var ErrKeyNotFound = errors.New("boltdb: key not found in range")

const HeapSize = 512 * 1024 * 1024

type BoltDatabase struct {
	fn string   // filename for reporting
	db *bolt.DB // BoltDB instance

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// NewBoltDatabase returns a BoltDB wrapped object.
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
		fn:  file,
		db:  db,
		log: logger,
	}, nil
}

// Path returns the path to the database directory.
func (db *BoltDatabase) Path() string {
	return db.fn
}

// Put puts the given key / value to the queue
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

// Put puts the given key / value to the queue
func (db *BoltDatabase) PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error {
	composite, suffix := dbutils.CompositeKeySuffix(key, timestamp)
	suffixkey := make([]byte, len(suffix)+len(hBucket))
	copy(suffixkey, suffix)
	copy(suffixkey[len(suffix):], hBucket)
	err := db.db.Update(func(tx *bolt.Tx) error {
		if !noHistory {
			hb, err := tx.CreateBucketIfNotExists(hBucket, true)
			if err != nil {
				return err
			}
			if err = hb.Put(composite, value); err != nil {
				return err
			}
		}

		sb, err := tx.CreateBucketIfNotExists(dbutils.ChangeSetBucket, true)
		if err != nil {
			return err
		}

		dat, _ := sb.Get(suffixkey)
		sh, err := dbutils.Decode(dat)
		if err != nil {
			log.Error("PutS Decode suffix err", "err", err)
			return err
		}
		sh = sh.Add(key, value)
		dat, err = dbutils.Encode(sh)
		if err != nil {
			log.Error("PutS Decode suffix err", "err", err)
			return err
		}

		return sb.Put(suffixkey, dat)
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

func (db *BoltDatabase) Size() int {
	return db.db.Size()
}

// Get returns the given key if it's present.
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

func (db *BoltDatabase) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	return db.Get(hBucket, composite)
}

// GetAsOf returns the first pair (k, v) where key is a prefix of k, or nil
// if there are not such (k, v)
func (db *BoltDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	var dat []byte
	err := db.db.View(func(tx *bolt.Tx) error {
		{
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

func bytesmask(fixedbits uint) (fixedbytes int, mask byte) {
	fixedbytes = int((fixedbits + 7) / 8)
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

func (db *BoltDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
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

func (db *BoltDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	if len(startkeys) == 0 {
		return nil
	}
	keyIdx := 0 // What is the current key we are extracting
	fixedbytes, mask := bytesmask(fixedbits[keyIdx])
	startkey := startkeys[keyIdx]
	if err := db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		k, v := c.Seek(startkey)
		for k != nil {
			// Adjust keyIdx if needed
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
						keyIdx++
						if keyIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
					}
				}
			}
			if len(v) > 0 {
				_, err := walker(keyIdx, k, v)
				if err != nil {
					return err
				}
			}
			k, v = c.Next()
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (db *BoltDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
	suffix := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(suffix)
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
		c := b.Cursor()
		hC := hB.Cursor()
		k, v := c.Seek(startkey)
		hK, hV := hC.Seek(startkey)
		goOn := true
		var err error
		for goOn {
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
			if hK != nil && bytes.Compare(hK[l:], suffix) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], suffix)
				// update historical key/value to the desired block
				hK, hV = hC.SeekTo(keyBuffer[:sl])
				continue
			}

			var cmp int
			if k == nil {
				if hK == nil {
					goOn = false
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
					k, v = c.Next()
				}
				if cmp >= 0 {
					copy(keyBuffer, hK[:l])
					copy(keyBuffer[l:], EndSuffix)
					hK, hV = hC.SeekTo(keyBuffer)
				}
			}
		}
		return err
	})
	return err
}

func (db *BoltDatabase) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	if len(startkeys) == 0 {
		return nil
	}
	keyIdx := 0 // What is the current key we are extracting
	fixedbytes, mask := bytesmask(fixedbits[keyIdx])
	startkey := startkeys[keyIdx]
	suffix := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(suffix)
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
		c := b.Cursor()
		hC := hB.Cursor()
		hC1 := hB.Cursor()
		k, v := c.Seek(startkey)
		hK, hV := hC.Seek(startkey)
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
							k, v = c.SeekTo(startkey)
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
							hK, hV = hC.SeekTo(startkey)
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
						fixedbytes, mask = bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
					}
				}
				fit = cmp == 0
				hKFit = hCmp == 0
			}
			if hKFit && bytes.Compare(hK[l:], suffix) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], suffix)
				hK, hV = hC.SeekTo(keyBuffer[:sl])
				continue
			}
			var cmp int
			if !fit {
				if !hKFit {
					goOn = false
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
				hK1, _ := hC1.Seek(k)
				if bytes.HasPrefix(hK1, k) {
					goOn, err = walker(keyIdx, k, v)
				}
			} else {
				goOn, err = walker(keyIdx, hK[:l], hV)
			}
			if goOn {
				if cmp <= 0 {
					k, v = c.Next()
				}
				if cmp >= 0 {
					copy(keyBuffer, hK[:l])
					copy(keyBuffer[l:], EndSuffix)
					hK, hV = hC.SeekTo(keyBuffer)
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
	return rewindData(db, timestampSrc, timestampDst, df)
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

// Deletes all keys with specified suffix from all the buckets
func (db *BoltDatabase) DeleteTimestamp(timestamp uint64) error {
	suffix := dbutils.EncodeTimestamp(timestamp)
	err := db.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(dbutils.ChangeSetBucket)
		if sb == nil {
			return nil
		}
		var keys [][]byte
		c := sb.Cursor()
		for k, v := c.Seek(suffix); k != nil && bytes.HasPrefix(k, suffix); k, v = c.Next() {
			hb := tx.Bucket(k[len(suffix):])
			if hb == nil {
				return nil
			}
			changedAccounts, err := dbutils.Decode(v)
			if err != nil {
				return err
			}
			err = changedAccounts.Walk(func(kk, _ []byte) error {
				kk = append(kk, suffix...)
				if err := hb.Delete(kk); err != nil {
					return err
				}
				return nil
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
	// Stop the metrics collection to avoid internal database races
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
		db.quitChan = nil
	}
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
		db:         db,
		puts:       make(map[string]*llrb.LLRB),
		suffixkeys: make(map[uint64]map[string][]dbutils.Change),
	}
	return m
}
