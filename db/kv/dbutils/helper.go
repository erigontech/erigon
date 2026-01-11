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

package dbutils

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

// NextNibblesSubtree does []byte++. Returns false if overflow.
func NextNibblesSubtree(in []byte, out *[]byte) bool {
	r := (*out)[:len(in)]
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 15 { // max value of nibbles
			r[i]++
			*out = r
			return true
		}

		r = r[:i] // make it shorter, because in tries after 11ff goes 12, but not 1200
	}
	*out = r
	return false
}

func WarmupTable(ctx context.Context, db kv.RoDB, table string, ord order.By) error {
	return db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(table)
		if err != nil {
			return err
		}
		defer cursor.Close()

		i := 0
		if ord == order.Asc {
			for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
				if err != nil {
					return err
				}
				if i%10 == 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
				}
				i++
			}
		} else {
			for k, _, err := cursor.Last(); k != nil; k, _, err = cursor.Prev() {
				if err != nil {
					return err
				}
				if i%10 == 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
				}
				i++
			}
		}

		return nil
	})
}
