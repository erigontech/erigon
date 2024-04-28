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
}

// Deprecated - use Trio
type DualS[K, V any] interface {
	Next() (K, V, uint64, error)
	HasNext() bool
}
