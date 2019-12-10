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
package remote

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

// BoltDatabase is a wrapper over BoltDb,
// compatible with the Database interface.
type RemoteBoltDatabase struct {
	db  *DB        // BoltDB instance
	log log.Logger // Contextual logger tracking the database path
}

// NewRemoteBoltDatabase returns a BoltDB wrapper.
func NewRemoteBoltDatabase(db *DB) *RemoteBoltDatabase {
	logger := log.New()

	return &RemoteBoltDatabase{
		db:  db,
		log: logger,
	}
}

func (db *RemoteBoltDatabase) Has(bucket, key []byte) (bool, error) {
	var has bool
	err := db.db.View(context.Background(), func(tx *Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			has = false
		} else {
			v := b.Get(key)
			has = v != nil
		}
		return nil
	})
	return has, err
}

// Get returns the value for a given key if it's present.
func (db *RemoteBoltDatabase) Get(bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.db.View(context.Background(), func(tx *Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ethdb.ErrKeyNotFound
	}
	return dat, err
}

// GetS returns the value that was recorded in a given historical bucket for an exact timestamp.
func (db *RemoteBoltDatabase) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	return db.Get(hBucket, composite)
}

// GetAsOf returns the value valid as of a given timestamp.
func (db *RemoteBoltDatabase) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	var dat []byte
	err := db.db.View(context.Background(), func(tx *Tx) error {
		{
			hB := tx.Bucket(hBucket)
			if hB == nil {
				return ethdb.ErrKeyNotFound
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
				return ethdb.ErrKeyNotFound
			}
			c := b.Cursor()
			k, v := c.Seek(key)
			if k != nil && bytes.Equal(k, key) {
				dat = make([]byte, len(v))
				copy(dat, v)
				return nil
			}
		}

		return ethdb.ErrKeyNotFound
	})
	return dat, err
}

func (db *RemoteBoltDatabase) Walk(bucket, startkey []byte, fixedbits uint, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := ethdb.Bytesmask(fixedbits)
	err := db.db.View(context.Background(), func(tx *Tx) error {
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

func (db *RemoteBoltDatabase) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) error) error {
	if len(startkeys) == 0 {
		return nil
	}
	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.db.View(context.Background(), func(tx *Tx) error {
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
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
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

func (db *RemoteBoltDatabase) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	fixedbytes, mask := ethdb.Bytesmask(fixedbits)
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(encodedTS)
	keyBuffer := make([]byte, l+len(ethdb.EndSuffix))
	err := db.db.View(context.Background(), func(tx *Tx) error {
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
			if hK != nil && bytes.Compare(hK[l:], encodedTS) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], encodedTS)
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
					copy(keyBuffer[l:], ethdb.EndSuffix)
					hK, hV = hC.SeekTo(keyBuffer)
				}
			}
		}
		return err
	})
	return err
}

func (db *RemoteBoltDatabase) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) error) error {
	if len(startkeys) == 0 {
		return nil
	}
	keyIdx := 0 // What is the current key we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[keyIdx])
	startkey := startkeys[keyIdx]
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	l := len(startkey)
	sl := l + len(encodedTS)
	keyBuffer := make([]byte, l+len(ethdb.EndSuffix))
	if err := db.db.View(context.Background(), func(tx *Tx) error {
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
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[keyIdx])
						startkey = startkeys[keyIdx]
					}
				}
				fit = cmp == 0
				hKFit = hCmp == 0
			}
			if hKFit && bytes.Compare(hK[l:], encodedTS) < 0 {
				copy(keyBuffer, hK[:l])
				copy(keyBuffer[l:], encodedTS)
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
					err = walker(keyIdx, k, v)
					goOn = err == nil
				}
			} else {
				err = walker(keyIdx, hK[:l], hV)
				goOn = err == nil
			}
			if goOn {
				if cmp <= 0 {
					k, v = c.Next()
				}
				if cmp >= 0 {
					copy(keyBuffer, hK[:l])
					copy(keyBuffer[l:], ethdb.EndSuffix)
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

func (db *RemoteBoltDatabase) RewindData(timestampSrc, timestampDst uint64, df func(hBucket, key, value []byte) error) error {
	return ethdb.RewindData(db, timestampSrc, timestampDst, df)
}

func (db *RemoteBoltDatabase) Close() {
	if err := db.db.Close(); err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}
