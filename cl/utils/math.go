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

package utils

import (
	"math"
	"time"

	"github.com/thomaso-mirodin/intmath/u64"
)

func IsPowerOf2(n uint64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func PowerOf2(n uint64) uint64 {
	if n >= 64 {
		panic("integer overflow")
	}
	return 1 << n
}

var squareRootTable = map[uint64]uint64{
	4:       2,
	16:      4,
	64:      8,
	256:     16,
	1024:    32,
	4096:    64,
	16384:   128,
	65536:   256,
	262144:  512,
	1048576: 1024,
	4194304: 2048,
}

func IntegerSquareRoot(n uint64) uint64 {
	if v, ok := squareRootTable[n]; ok {
		return v
	}
	if n >= 1<<52 {
		return u64.Sqrt(n)
	}

	return uint64(math.Sqrt(float64(n)))
}

// ETA estimates the time to process `remaining` items at `ratePerSec` items per
// second, truncated to whole seconds. Returns "n/a" for a non-positive rate and
// clamps to the max representable duration when the rate is extremely slow.
func ETA(remaining uint64, ratePerSec float64) string {
	if ratePerSec <= 0 {
		return "n/a"
	}
	seconds := float64(remaining) / ratePerSec
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64).Truncate(time.Second).String()
	}
	return time.Duration(seconds * float64(time.Second)).Truncate(time.Second).String()
}
