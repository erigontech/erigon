// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package state

import (
	"bytes"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/common"
)

func BenchmarkCutOriginal(b *testing.B) {
	value := libcommon.HexToHash("0x01")
	for i := 0; i < b.N; i++ {
		bytes.TrimLeft(value[:], "\x00")
	}
}

func BenchmarkCutsetterFn(b *testing.B) {
	value := libcommon.HexToHash("0x01")
	cutSetFn := func(r rune) bool { return r == 0 }
	for i := 0; i < b.N; i++ {
		bytes.TrimLeftFunc(value[:], cutSetFn)
	}
}

func BenchmarkCutCustomTrim(b *testing.B) {
	value := libcommon.HexToHash("0x01")
	for i := 0; i < b.N; i++ {
		common.TrimLeftZeroes(value[:])
	}
}
