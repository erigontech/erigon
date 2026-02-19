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

package block_collector

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

// BenchmarkEncodeBlockBuffer benchmarks buffer construction in encodeBlock.
// Compares optimized (pre-allocated) vs old (nested append) approaches.
func BenchmarkEncodeBlockBuffer(b *testing.B) {
	version := byte(clparams.DenebVersion)
	parentRoot := common.HexToHash("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	requestsHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	// Typical block size (~100KB)
	encodedPayload := make([]byte, 100*1024)

	b.Run("Optimized", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 1+32+32+len(encodedPayload))
			buf[0] = version
			copy(buf[1:], parentRoot[:])
			copy(buf[33:], requestsHash[:])
			copy(buf[65:], encodedPayload)
			_ = buf
		}
	})

	b.Run("Old", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := append([]byte{version}, append(append(parentRoot[:], requestsHash[:]...), encodedPayload...)...)
			_ = buf
		}
	})
}
