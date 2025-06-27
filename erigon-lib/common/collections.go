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

package common

import (
	"math/rand"
	"strconv"
)

func SliceMap[T any, U any](s []T, mapFunc func(T) U) []U {
	out := make([]U, 0, len(s))
	for _, x := range s {
		out = append(out, mapFunc(x))
	}
	return out
}

func Filter[T any](s []T, filter func(T) bool) []T {
	var out []T
	for _, x := range s {
		if filter(x) {
			out = append(out, x)
		}
	}
	return out
}

func SliceShuffle[T any](s []T) {
	rand.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
}

func SliceTakeLast[T any](s []T, count int) []T {
	length := len(s)
	if length > count {
		return s[length-count:]
	}
	return s
}

func ParseMapKeysIntoUint64[T any](m map[string]T) map[uint64]T {
	res := make(map[uint64]T)
	for k, v := range m {
		keyU64, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			panic(err)
		}
		res[keyU64] = v
	}
	return res
}
