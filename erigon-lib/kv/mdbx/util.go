/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mdbx

import (
	mdbxbind "github.com/erigontech/mdbx-go/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

func MustOpen(path string) kv.RwDB {
	db, err := Open(path, log.New(), false)
	if err != nil {
		panic(err)
	}
	return db
}

func MustOpenRo(path string) kv.RoDB {
	db, err := Open(path, log.New(), true)
	if err != nil {
		panic(err)
	}
	return db
}

// Open - main method to open database.
func Open(path string, logger log.Logger, readOnly bool) (kv.RwDB, error) {
	var db kv.RwDB
	var err error
	opts := NewMDBX(logger).Path(path)
	if readOnly {
		opts = opts.Flags(func(flags uint) uint { return flags | mdbxbind.Readonly })
	}
	db, err = opts.Open()

	if err != nil {
		return nil, err
	}
	return db, nil
}
