// Copyright 2026 The Erigon Authors
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

package vm

import "os"

// layoutPad only shifts the package's code layout; it never runs unless LAYOUT_PAD_RUN is set.
//
//go:noinline
func layoutPad(n uint64) uint64 {
	x := n
	for i := uint64(0); i < 64; i++ {
		x = x*6364136223846793005 + 1442695040888963407
		x ^= x >> 29
		x += i * 0x9E3779B97F4A7C15
		x ^= x << 17
	}
	return x
}

func init() {
	if os.Getenv("LAYOUT_PAD_RUN") != "" {
		_ = layoutPad(uint64(len(os.Args)))
	}
}
