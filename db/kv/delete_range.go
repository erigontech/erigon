// Copyright 2024 The Erigon Authors
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

package kv

import "fmt"

// DeleteRange removes keys in [from, to) from table and returns the number of
// entries deleted. from==nil starts at the first key; to==nil deletes through
// the last key. It picks the cheapest native bulk range-delete the bounds allow,
// so tx must implement HasDeleteRange; emulating it by iterating would silently
// turn a B-tree cut into a full per-key scan.
func DeleteRange(tx RwTx, table string, from, to []byte) (uint64, error) {
	dr, ok := tx.(HasDeleteRange)
	if !ok {
		panic(fmt.Sprintf("%T does not implement kv.HasDeleteRange", tx))
	}
	switch {
	case from == nil && to != nil:
		return dr.DeleteBefore(table, to)
	case from != nil && to == nil:
		return dr.DeleteAfter(table, from)
	default:
		return dr.DeleteRange(table, from, to)
	}
}
