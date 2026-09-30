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

// Package simdhex is encoding/hex's lowercase encoder with a vector path on amd64.
package simdhex

import (
	"encoding/hex"
	"slices"
)

// AppendEncode is hex.AppendEncode.
func AppendEncode(dst, src []byte) []byte {
	n := hex.EncodedLen(len(src))
	dst = slices.Grow(dst, n)
	Encode(dst[len(dst):len(dst)+n], src)
	return dst[:len(dst)+n]
}
