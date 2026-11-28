// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package membatchwithdb

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/stream"
)

// MemCursorPosition returns the in-memory entry an overlay cursor is positioned at.
func MemCursorPosition(c kv.Cursor) (k []byte, valid bool) {
	mc := c.(*memoryMutationCursor).memCursor
	return mc.current.k, mc.valid
}

// RangeFiltersDb reports whether an overlay range stream checks its db rows against tombstones.
func RangeFiltersDb(it stream.KV) bool {
	var db stream.KV
	switch r := it.(type) {
	case *rangeIter:
		db = r.iterDb
	case *rangeDupSortIter:
		db = r.iterDb
	}
	_, ok := db.(*stream.FilteredDuo[[]byte, []byte])
	return ok
}
