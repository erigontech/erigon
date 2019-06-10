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

package ethdb

import (
	"github.com/ledgerwatch/bolt"

	"github.com/ledgerwatch/turbo-geth/log"
)

func NewMemDatabase() *BoltDatabase {
	logger := log.New("database", "in-memory")

	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		panic(err)
	}
	return &BoltDatabase{
		fn:  "in-memory",
		db:  db,
		log: logger,
	}
}

func NewMemDatabase2() (*BoltDatabase, *bolt.DB) {
	logger := log.New("database", "in-memory")

	db, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		panic(err)
	}
	return &BoltDatabase{
		fn:  "in-memory",
		db:  db,
		log: logger,
	}, db
}

func (db *BoltDatabase) MemCopy() Database {
	//rnd:=rand.Int()
	//_,s1,l1,_:=runtime.Caller(1)
	//_,s2,l2,_:=runtime.Caller(2)
	//_,s3,l3,_:=runtime.Caller(3)
	//_,s4,l4,_:=runtime.Caller(4)
	//_,s5,l5,_:=runtime.Caller(5)
	//fmt.Println(rnd, " -- ", s1, l1)
	//fmt.Println(rnd, " -- ", s2, l2)
	//fmt.Println(rnd, " -- ", s3, l3)
	//fmt.Println(rnd, " -- ", s4, l4)
	//fmt.Println(rnd, " -- ", s5, l5)
	logger := log.New("database", "in-memory")

	// Open the db and recover any potential corruptions
	mem, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		panic(err)
	}

	db.boltDBWriteLock.Lock()
	defer db.boltDBWriteLock.Unlock()

	if err := mem.Update(func(writeTx *bolt.Tx) error {
		return db.db.View(func(readTx *bolt.Tx) error {
			return readTx.ForEach(func(name []byte, b *bolt.Bucket) error {
				newBucketToWrite, err := writeTx.CreateBucket(name, true)
				if err != nil {
					return err
				}
				return b.ForEach(func(k, v []byte) error {
					if err := newBucketToWrite.Put(k, v); err != nil {
						return err
					}
					return nil
				})
			})
		})
	}); err != nil {
		panic(err)
	}
	return &BoltDatabase{
		fn:  "in-memory",
		db:  mem,
		log: logger,
	}
}
