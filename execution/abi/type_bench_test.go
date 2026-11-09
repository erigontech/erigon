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

package abi

import (
	"fmt"
	"testing"
)

func BenchmarkNewTypeTuple(b *testing.B) {
	components := make([]ArgumentMarshaling, 64)
	for i := range components {
		components[i] = ArgumentMarshaling{Name: fmt.Sprintf("field%d", i), Type: "uint256"}
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := NewType("tuple", "", components); err != nil {
			b.Fatal(err)
		}
	}
}
