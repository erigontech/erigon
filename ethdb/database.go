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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"

	"github.com/ledgerwatch/bolt"
	"github.com/petar/GoLLRB/llrb"
)

var OpenFileLimit = 64
var ErrKeyNotFound = errors.New("boltdb: key not found in range")
var SuffixBucket = []byte("SUFFIX")

const HeapSize = 512 * 1024 * 1024

type BoltDatabase struct {
	fn string   // filename for reporting
	db *bolt.DB // BoltDB instance

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
	boltDBWriteLock sync.Mutex
}

// NewBoltDatabase returns a LevelDB wrapped object.
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

func compositeKeySuffix(key []byte, timestamp uint64) (composite, suffix []byte) {
	suffix = encodeTimestamp(timestamp)
	composite = make([]byte, len(key)+len(suffix))
	copy(composite, key)
	copy(composite[len(key):], suffix)
	return composite, suffix
}

func historyBucket(bucket []byte) []byte {
	hb := make([]byte, len(bucket)+1)
	hb[0] = byte('h')
	copy(hb[1:], bucket)
	return hb
}

// Put puts the given key / value to the queue
func (db *BoltDatabase) PutS(hBucket, key, value []byte, timestamp uint64) error {
	composite, suffix := compositeKeySuffix(key, timestamp)
	suffixkey := make([]byte, len(suffix)+len(hBucket))
	copy(suffixkey, suffix)
	err := db.db.Update(func(tx *bolt.Tx) error {
		hb, err := tx.CreateBucketIfNotExists(hBucket, true)
		if err != nil {
			return err
		}
		if err = hb.Put(composite, value); err != nil {
			return err
		}
		sb, err := tx.CreateBucketIfNotExists(SuffixBucket, true)
		if err != nil {
			return err
		}
		dat, _ := sb.Get(suffixkey)
		var l int
		if dat == nil {
			l = 4
		} else {
			l = len(dat)
		}
		dv := make([]byte, l+1+len(key))
		copy(dv, dat)
		binary.BigEndian.PutUint32(dv, 1+binary.BigEndian.Uint32(dv)) // Increment the counter of keys
		dv[l] = byte(len(key))
		copy(dv[l+1:], key)
		return sb.Put(suffixkey, dv)
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
	composite, _ := compositeKeySuffix(key, timestamp)
	return db.Get(hBucket, composite)
}

// GetAsOf returns the first pair (k, v) where key is a prefix of k, or nil
// if there are not such (k, v)
func (db *BoltDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := compositeKeySuffix(key, timestamp)
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
						if _, err := walker(keyIdx, nil, nil); err != nil {
							return err
						}
						if keyIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
						//k, v = c.SeekTo(startkey)
						//if k == nil {
						//	return nil
						//}
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
	for keyIdx < len(startkeys) {
		keyIdx++
		if _, err := walker(keyIdx, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func (db *BoltDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
	suffix := encodeTimestamp(timestamp)
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
			if hK != nil && bytes.Compare(hK[l:], suffix) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], suffix)
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
	suffix := encodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(suffix)
	keyBuffer := make([]byte, l+len(EndSuffix))
	if err := db.db.View(func(tx *bolt.Tx) error {
		pre := tx.Bucket([]byte("secure-key-"))
		if pre == nil {
			return nil
		}
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
			kFit := k != nil
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
						if _, err := walker(keyIdx, nil, nil); err != nil {
							return err
						}
						if keyIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
					}
				}
				kFit = cmp == 0
				hKFit = hCmp == 0
			}
			if hKFit && bytes.Compare(hK[l:], suffix) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], suffix)
				hK, hV = hC.SeekTo(keyBuffer[:sl])
				continue
			}
			var cmp int
			if !kFit {
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
				if l == 32 {
					//addr := pre.Get(k)
					//fmt.Printf("c %x (%x): %x\n", k, addr, v)
				} else {
					//fmt.Printf("c %x: %x\n", k, v)
				}
				hK1, _ := hC1.Seek(k)
				if bytes.HasPrefix(hK1, k) {
					goOn, err = walker(keyIdx, k, v)
				}
			} else {
				if hKFit && bytes.Equal(hK[l:], suffix) && timestamp == 3436035 {
					if l == 32 {
						addr, _ := pre.Get(hK[:l])
						fmt.Printf("h %x (%x): %x\n", hK[:l], addr, hV)
					} else {
						item, _ := pre.Get(hK[20:52])
						fmt.Printf("h %x (%x): %x\n", hK[:l], item, hV)
					}
				}
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
	for keyIdx < len(startkeys) {
		keyIdx++
		if _, err := walker(keyIdx, nil, nil); err != nil {
			return err
		}
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
	suffix := encodeTimestamp(timestamp)
	err := db.db.Update(func(tx *bolt.Tx) error {
		sb := tx.Bucket(SuffixBucket)
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
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				kk := make([]byte, l+len(suffix))
				copy(kk, v[i:i+l])
				copy(kk[l:], suffix)
				if err := hb.Delete(kk); err != nil {
					return err
				}
				i += l
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

func (db *BoltDatabase) Keys() [][]byte {
	var keys [][]byte
	db.db.View(func(tx *bolt.Tx) error {
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
	return keys
}

type PutItem struct {
	key, value []byte
}

func (a *PutItem) Less(b llrb.Item) bool {
	bi := b.(*PutItem)
	return bytes.Compare(a.key, bi.key) < 0
}

type mutation struct {
	puts       map[string]*llrb.LLRB // Map buckets to RB tree containing items
	suffixkeys map[uint64]map[string][][]byte
	mu         sync.RWMutex
	db         Database
}

func (db *BoltDatabase) NewBatch() Mutation {
	m := &mutation{
		db:         db,
		puts:       make(map[string]*llrb.LLRB),
		suffixkeys: make(map[uint64]map[string][][]byte),
	}
	return m
}

func (m *mutation) getMem(bucket, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.puts[string(bucket)]; ok {
		i := t.Get(&PutItem{key: key})
		if i == nil {
			return nil, false
		}
		if item, ok := i.(*PutItem); ok {
			if item.value == nil {
				return nil, true
			}
			v := make([]byte, len(item.value))
			copy(v, item.value)
			return v, true
		}
		return nil, false
	} else {
		return nil, false
	}
}

// Can only be called from the worker thread
func (m *mutation) Get(bucket, key []byte) ([]byte, error) {
	if value, ok := m.getMem(bucket, key); ok {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := compositeKeySuffix(key, timestamp)
	return m.Get(hBucket, composite)
}

func (m *mutation) getNoLock(bucket, key []byte) ([]byte, error) {
	if t, ok := m.puts[string(bucket)]; ok {
		i := t.Get(&PutItem{key: key})
		if i != nil {
			if item, ok := i.(*PutItem); ok {
				if item.value == nil {
					return nil, ErrKeyNotFound
				}
				return common.CopyBytes(item.value), nil
			}
		}
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(bucket, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.puts[string(bucket)]; ok {
		return t.Has(&PutItem{key: key})
	}
	return false
}

func (m *mutation) Has(bucket, key []byte) (bool, error) {
	if m.hasMem(bucket, key) {
		return true, nil
	}
	if m.db != nil {
		return m.db.Has(bucket, key)
	}
	return false, nil
}

func (m *mutation) Size() int {
	if m.db == nil {
		return 0
	}
	return m.db.Size()
}

func (m *mutation) Put(bucket, key []byte, value []byte) error {
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bb)]; !ok {
		t = llrb.New()
		m.puts[string(bb)] = t
	}
	t.ReplaceOrInsert(&PutItem{key: k, value: v})
	return nil
}

// Assumes that bucket, key, and value won't be modified
func (m *mutation) PutS(hBucket, key, value []byte, timestamp uint64) error {
	//fmt.Printf("PutS bucket %x key %x value %x timestamp %d\n", bucket, key, value, timestamp)
	composite, _ := compositeKeySuffix(key, timestamp)
	suffix_m, ok := m.suffixkeys[timestamp]
	if !ok {
		suffix_m = make(map[string][][]byte)
		m.suffixkeys[timestamp] = suffix_m
	}
	suffix_l, ok := suffix_m[string(hBucket)]
	if !ok {
		suffix_l = [][]byte{}
	}
	suffix_l = append(suffix_l, key)
	suffix_m[string(hBucket)] = suffix_l
	m.mu.Lock()
	defer m.mu.Unlock()
	var ht *llrb.LLRB
	if ht, ok = m.puts[string(hBucket)]; !ok {
		ht = llrb.New()
		m.puts[string(hBucket)] = ht
	}
	ht.ReplaceOrInsert(&PutItem{key: composite, value: value})
	return nil
}

func (m *mutation) MultiPut(tuples ...[]byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		var t *llrb.LLRB
		var ok bool
		if t, ok = m.puts[string(tuples[i])]; !ok {
			t = llrb.New()
			m.puts[string(tuples[i])] = t
		}
		t.ReplaceOrInsert(&PutItem{key: tuples[i+1], value: tuples[i+2]})
	}
	return 0, nil
}

func (m *mutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	return size
}

func (m *mutation) getAsOfMem(hBucket, key []byte, timestamp uint64) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(hBucket)]; !ok {
		return nil, false
	}
	composite, _ := compositeKeySuffix(key, timestamp)
	var dat []byte
	t.AscendGreaterOrEqual(&PutItem{key: composite}, func(i llrb.Item) bool {
		item := i.(*PutItem)
		if !bytes.HasPrefix(item.key, key) {
			return false
		}
		if item.value == nil {
			return true
		}
		dat = make([]byte, len(item.value))
		copy(dat, item.value)
		return false
	})
	if dat != nil {
		return dat, true
	}
	return nil, false
}

func (m *mutation) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	if m.db == nil {
		panic("Not implemented")
	} else {
		return m.db.GetAsOf(bucket, hBucket, key, timestamp)
	}
}

func (m *mutation) walkMem(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
	m.mu.RLock()
	defer m.mu.RUnlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bucket)]; !ok {
		return nil
	}
	for nextkey := startkey; nextkey != nil; {
		from := nextkey
		nextkey = nil
		var extErr error
		t.AscendGreaterOrEqual(&PutItem{key: from}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			if item.value == nil {
				return true
			}
			if fixedbits > 0 && (!bytes.Equal(item.key[:fixedbytes-1], startkey[:fixedbytes-1]) || (item.key[fixedbytes-1]&mask) != (startkey[fixedbytes-1]&mask)) {
				return true
			}
			goOn, err := walker(item.key, item.value)
			if err != nil {
				extErr = err
				return false
			}
			return goOn
		})
		if extErr != nil {
			return extErr
		}
	}
	return nil
}

func (m *mutation) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	if m.db == nil {
		return m.walkMem(bucket, startkey, fixedbits, walker)
	} else {
		return m.db.Walk(bucket, startkey, fixedbits, walker)
	}
}

func (m *mutation) multiWalkMem(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (m *mutation) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	if m.db == nil {
		return m.multiWalkMem(bucket, startkeys, fixedbits, walker)
	} else {
		return m.db.MultiWalk(bucket, startkeys, fixedbits, walker)
	}
}

func (m *mutation) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	if m.db == nil {
		panic("Not implemented")
	} else {
		return m.db.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
	}
}

func (m *mutation) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	if m.db == nil {
		panic("Not implemented")
	} else {
		return m.db.MultiWalkAsOf(bucket, hBucket, startkeys, fixedbits, timestamp, walker)
	}
}

func (m *mutation) RewindData(timestampSrc, timestampDst uint64, df func(hBucket, key, value []byte) error) error {
	return rewindData(m, timestampSrc, timestampDst, df)
}

func (m *mutation) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bb)]; !ok {
		t = llrb.New()
		m.puts[string(bb)] = t
	}
	k := make([]byte, len(key))
	copy(k, key)
	t.ReplaceOrInsert(&PutItem{key: k, value: nil})
	return nil
}

// Deletes all keys with specified suffix from all the buckets
func (m *mutation) DeleteTimestamp(timestamp uint64) error {
	suffix := encodeTimestamp(timestamp)
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(SuffixBucket)]; !ok {
		t = llrb.New()
		m.puts[string(SuffixBucket)] = t
	}
	err := m.Walk(SuffixBucket, suffix, uint(8*len(suffix)), func(k, v []byte) (bool, error) {
		hBucket := k[len(suffix):]
		keycount := int(binary.BigEndian.Uint32(v))
		var ht *llrb.LLRB
		var ok bool
		if keycount > 0 {
			hBucketStr := string(common.CopyBytes(hBucket))
			if ht, ok = m.puts[hBucketStr]; !ok {
				ht = llrb.New()
				m.puts[hBucketStr] = ht
			}
		}
		for i, ki := 4, 0; ki < keycount; ki++ {
			l := int(v[i])
			i++
			kk := make([]byte, l+len(suffix))
			copy(kk, v[i:i+l])
			copy(kk[l:], suffix)
			ht.ReplaceOrInsert(&PutItem{key: kk, value: nil})
			i += l
		}
		t.ReplaceOrInsert(&PutItem{key: common.CopyBytes(k), value: nil})
		return true, nil
	})
	return err
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if len(m.suffixkeys) > 0 {
		if t, ok = m.puts[string(SuffixBucket)]; !ok {
			t = llrb.New()
			m.puts[string(SuffixBucket)] = t
		}
	}
	for timestamp, suffix_m := range m.suffixkeys {
		suffix := encodeTimestamp(timestamp)
		for bucketStr, suffix_l := range suffix_m {
			hBucket := []byte(bucketStr)
			suffixkey := make([]byte, len(suffix)+len(hBucket))
			copy(suffixkey, suffix)
			copy(suffixkey[len(suffix):], hBucket)
			dat, err := m.getNoLock(SuffixBucket, suffixkey)
			if err != nil && err != ErrKeyNotFound {
				return 0, err
			}
			var l int
			if dat == nil {
				l = 4
			} else {
				l = len(dat)
			}
			newlen := len(suffix_l)
			for _, key := range suffix_l {
				newlen += len(key)
			}
			dv := make([]byte, l+newlen)
			copy(dv, dat)
			binary.BigEndian.PutUint32(dv, uint32(len(suffix_l))+binary.BigEndian.Uint32(dv))
			i := l
			for _, key := range suffix_l {
				dv[i] = byte(len(key))
				i++
				copy(dv[i:], key)
				i += len(key)
			}
			t.ReplaceOrInsert(&PutItem{key: suffixkey, value: dv})
		}
	}
	m.suffixkeys = make(map[uint64]map[string][][]byte)
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	tuples := make([][]byte, size*3)
	var index int
	for bucketStr, bt := range m.puts {
		bt.AscendGreaterOrEqual(&PutItem{}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			tuples[index] = []byte(bucketStr)
			index++
			tuples[index] = item.key
			index++
			tuples[index] = item.value
			index++
			return true
		})
	}
	var written uint64
	var putErr error
	if written, putErr = m.db.MultiPut(tuples...); putErr != nil {
		return 0, putErr
	}
	m.puts = make(map[string]*llrb.LLRB)
	return written, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suffixkeys = make(map[uint64]map[string][][]byte)
	m.puts = make(map[string]*llrb.LLRB)
}

func (m *mutation) Keys() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	pairs := make([][]byte, 2*size)
	idx := 0
	for bucketStr, bt := range m.puts {
		bt.AscendGreaterOrEqual(&PutItem{}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			pairs[idx] = []byte(bucketStr)
			idx++
			pairs[idx] = item.key
			idx++
			return true
		})
	}
	return pairs
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) NewBatch() Mutation {
	mm := &mutation{
		db:         m,
		puts:       make(map[string]*llrb.LLRB),
		suffixkeys: make(map[uint64]map[string][][]byte),
	}
	return mm
}

func (m *mutation) MemCopy() Database {
	panic("Not implemented")
}
