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
//     check in loops on stream. Dual has very limited API - user has no way to
//     terminate it - but user can specify more strict conditions when creating stream (then server knows better when to stop)

// Dual - return 2 items - usually called Key and Value (or `k` and `v`)
// Example:
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type Dual[K, V any] interface {
	Next() (K, V, error)
	HasNext() bool
}

// Unary - return 1 item. Example:
//
//	for s.HasNext() {
//		v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}
type Unary[V any] interface {
	Next() (V, error)
	//NextBatch() ([]V, error)
	HasNext() bool
}

// KV - return 2 items of type []byte - usually called Key and Value (or `k` and `v`). Example:
//
//	for s.HasNext() {
//		k, v, err := s.Next()
//		if err != nil {
//			return err
//		}
//	}

// often used shortcuts
type (
	U64 Unary[uint64]
	KV  Dual[[]byte, []byte]
)

func ToU64Arr(s U64) ([]uint64, error)           { return ToArr[uint64](s) }
func ToKVArray(s KV) ([][]byte, [][]byte, error) { return ToDualArray[[]byte, []byte](s) }

func ToArrU64Must(s U64) []uint64 {
	arr, err := ToArr[uint64](s)
	if err != nil {
		panic(err)
	}
	return arr
}
func ToArrKVMust(s KV) ([][]byte, [][]byte) {
	keys, values, err := ToDualArray[[]byte, []byte](s)
	if err != nil {
		panic(err)
	}
	return keys, values
}

func CountU64(s U64) (int, error) { return Count[uint64](s) }
func CountKV(s KV) (int, error)   { return CountDual[[]byte, []byte](s) }

func TransformKV(it KV, transform func(k, v []byte) ([]byte, []byte, error)) *TransformDualIter[[]byte, []byte] {
	return TransformDual[[]byte, []byte](it, transform)
}

// internal types
type (
	NextPageUnary[T any]   func(pageToken string) (arr []T, nextPageToken string, err error)
	NextPageDual[K, V any] func(pageToken string) (keys []K, values []V, nextPageToken string, err error)
)

func PaginateKV(f NextPageDual[[]byte, []byte]) *PaginatedDual[[]byte, []byte] {
	return PaginateDual[[]byte, []byte](f)
}
func PaginateU64(f NextPageUnary[uint64]) *Paginated[uint64] {
	return Paginate[uint64](f)
}
