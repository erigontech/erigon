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

func ToArray[T any](s Uno[T]) (res []T, err error) {
	for s.HasNext() {
		k, err := s.Next()
		if err != nil {
			return res, err
		}
		res = append(res, k)
	}
	return res, nil
}

func ToArrayDuo[K, V any](s Duo[K, V]) (keys []K, values []V, err error) {
	for s.HasNext() {
		k, v, err := s.Next()
		if err != nil {
			return keys, values, err
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func Count[T any](s Uno[T]) (cnt int, err error) {
	for s.HasNext() {
		_, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}

func CountDuo[K, V any](s Duo[K, V]) (cnt int, err error) {
	for s.HasNext() {
		_, _, err := s.Next()
		if err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, err
}
