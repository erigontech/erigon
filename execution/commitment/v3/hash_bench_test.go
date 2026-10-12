// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v3

import (
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
)

func BenchmarkZZStorageLeafRef(b *testing.B) {
	v := make([]byte, 32)
	rand.New(rand.NewSource(3)).Read(v)
	suffix := make([]byte, 33)
	buf := make([]byte, 0, 256)
	b.Run("storageLeafRef", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = storageLeafRef(suffix, v, buf[:0])
		}
	})
	b.Run("leafRef_account", func(b *testing.B) {
		payload := accountConsensusRLP(3, uint256.NewInt(12345), empty.RootHash[:], empty.CodeHash[:], nil)
		b.ReportAllocs()
		for range b.N {
			_ = leafRef(suffix, payload, buf[:0])
		}
	})
}

func storageLeafRefDirect(suffix []byte, payload []byte, dst []byte) []byte {
	innerLen := 1 + len(payload)
	if len(payload) == 1 && payload[0] < 0x80 {
		innerLen = 1
	}
	outerLen := 1 + innerLen
	if innerLen == 1 && payload[0] < 0x80 {
		outerLen = 1
	}
	contentLen := rlp.StringLen(suffix) + outerLen
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen)+contentLen)...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeStringToBuf(suffix, dst[pos:])
	if outerLen == 1 {
		dst[pos] = payload[0]
		pos++
	} else {
		if innerLen == 1 {
			dst[pos] = 0x81
			dst[pos+1] = payload[0]
			pos += 2
		} else {
			dst[pos] = byte(0x80 + outerLen - 1)
			dst[pos+1] = byte(0x80 + len(payload))
			pos += 2
			pos += copy(dst[pos:], payload)
		}
	}
	out := dst[start:pos]
	if len(out) < 32 {
		return out
	}
	h := keccak.Sum256(out)
	return append(dst[:start], h[:]...)
}

func BenchmarkZZStorageLeafRefDirect(b *testing.B) {
	v := make([]byte, 32)
	rand.New(rand.NewSource(3)).Read(v)
	suffix := make([]byte, 33)
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for range b.N {
		_ = storageLeafRefDirect(suffix, v, buf[:0])
	}
}
