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
	"github.com/ledgerwatch/turbo-geth/common/debug"
)

// NewMemoryDatabase is just an alias to simplify rebasing (the same name as `rawdb.NewMemoryDatabase` in vanilla geth)
func NewMemoryDatabase() *ObjectDatabase {
	return NewMemDatabase()
}

func NewMemDatabase() *ObjectDatabase {
	switch debug.TestDB() {
	case "lmdb":
		return NewObjectDatabase(NewLMDB().InMem().MustOpen())
	case "mdbx": //nolint:goconst
		return NewObjectDatabase(NewMDBX().InMem().MustOpen())
	default:
		return NewObjectDatabase(NewMDBX().InMem().MustOpen())
	}
}
