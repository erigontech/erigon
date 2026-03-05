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

package bitutil

import (
	"math"
	"testing"
)

func TestSelect64(t *testing.T) {
	if res := Select64(5270498307387724361, 14); res != 41 {
		panic(res)
	}
	if res := Select64(5270498307387724361, 6); res != 18 {
		panic(res)
	}
	if res := Select64(uint64(math.MaxUint64), 62); res != 62 {
		panic(res)
	}
	if res := Select64(210498307387724361, 14); res != 35 {
		panic(res)
	}
}
