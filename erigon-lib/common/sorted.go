/*
   Copyright 2022 The Erigon contributors

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

package common

import (
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	slices.Sort(keys)
	return keys
}

func RemoveDuplicatesFromSorted[T constraints.Ordered](slice []T) []T {
	for i := 1; i < len(slice); i++ {
		if slice[i] == slice[i-1] {
			slice = append(slice[:i], slice[i+1:]...)
			i--
		}
	}
	return slice
}
