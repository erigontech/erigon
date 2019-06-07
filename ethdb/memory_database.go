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
	logger := log.New("database", "in-memory")

	// Open the db and recover any potential corruptions
	mem, err := bolt.Open("in-memory", 0600, &bolt.Options{MemOnly: true})
	if err != nil {
		panic(err)
	}
	if err := mem.Update(func(memTx *bolt.Tx) error {
		return db.db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				memB, err := memTx.CreateBucket(name, true)
				if err != nil {
					return err
				}
				return b.ForEach(func(k, v []byte) error {
					if err := memB.Put(k, v); err != nil {
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
