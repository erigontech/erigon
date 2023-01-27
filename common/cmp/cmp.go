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

package cmp

import (
	"golang.org/x/exp/constraints"
)

// InRange - ensure val is in [min,max] range
func InRange[T constraints.Ordered](min, max, val T) T {
	if min >= val {
		return min
	}
	if max <= val {
		return max
	}
	return val
}

func Min[T constraints.Ordered](a, b T) T {
	if a <= b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a >= b {
		return a
	}
	return b
}
