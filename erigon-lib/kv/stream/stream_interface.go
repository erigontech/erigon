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

package stream

import "errors"

// Streams - it's iterator-like composable high-level abstraction - which designed for inter-process communication and between processes:
//  - cancelable
//  - return errors
//  - batch-friendly
//  - server-side-streaming-friendly: no client-side granular management. all required data described by constructor (query) then only iterate over results.
//  - more high-level than kv.Cursor
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
//  Invariants:
//   1. HasNext() is Idempotent
//   2. K, V are valid at-least 2 .Next() calls! It allows zero-copy composition of streams. Example: stream.Union
//		- 1 value used by User and 1 value used internally by stream.Union
//   3. No `Close` method: all streams produced by TemporalTx will be closed inside `tx.Rollback()` (by casting to `kv.Closer`)
//   4. automatically checks cancellation of `ctx` passed to `db.Begin(ctx)`, can skip this
//     check in loops on stream. Duo has very limited API - user has no way to
//     terminate it - but user can specify more strict conditions when creating stream (then server knows better when to stop)

// Indicates the iterator has no more elements. Meant to be returned by implementations of Next()
// when there are no more elements.
var ErrIteratorExhausted = errors.New("iterator exhausted")

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
