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

//go:build go1.27 && goexperiment.simd && amd64

package hexutil

import "testing"

// CI runners have AVX2, so the fallback of a simd build on an older CPU only runs here.
func TestEncodeHexWithoutAVX2(t *testing.T) {
	saved := hasAVX2
	hasAVX2 = false
	t.Cleanup(func() { hasAVX2 = saved })
	checkEncodeHex(t)
}
