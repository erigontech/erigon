// Copyright 2021 The Erigon Authors
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

package iter

// Iterators - composable high-level abstraction to iterate over. It's more high-level than kv.Cursor and provides less controll, less features, but enough to build an app.
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
//  Invariants:
//   1. HasNext() is Idempotent
//   2. K, V are valid at-least 2 .Next() calls! It allows zero-copy composition of iterators. Example: iter.Union
//		- 1 value used by User and 1 value used internally by iter.Union
//   3. No `Close` method: all streams produced by TemporalTx will be closed inside `tx.Rollback()` (by casting to `kv.Closer`)
//   4. automatically checks cancelation of `ctx` passed to `db.Begin(ctx)`, can skip this
//     check in loops on stream. Duo has very limited API - user has no way to
//     terminate it - but user can specify more strict conditions when creating stream (then server knows better when to stop)

// Uno - return 1 item. Example:
//
//	for s.HasNext() {
//		v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type Uno[V any] interface {
	Next() (V, error)
	//NextBatch() ([]V, error)
	HasNext() bool
	Close()
}

// Duo - return 2 items - usually called Key and Value (or `k` and `v`)
// Example:
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type Duo[K, V any] interface {
	Next() (K, V, error)
	HasNext() bool
	Close()
}

// Trio - return 3 items - usually called Key and Value (or `k` and `v`)
// Example:
//
//	for s.HasNext() {
//		k, v1, v2, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type Trio[K, V1, V2 any] interface {
	Next() (K, V1, V2, error)
	HasNext() bool
	Close()
}

// Deprecated - use Trio
type DualS[K, V any] interface {
	Next() (K, V, uint64, error)
	HasNext() bool
}
