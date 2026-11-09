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

package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func BenchmarkZeroGammas(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		ZeroGammas(1000)
	}
}

func BenchmarkRandomPolynomial(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := RandomPolynomial(rand.Reader, 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncryptedMessageUnmarshal(b *testing.B) {
	m := encryptedMessage()
	for i := range 1000 {
		var blk Block
		copy(blk[:], bytes.Repeat([]byte{byte(i)}, 32))
		m.C3 = append(m.C3, blk)
	}
	d := m.Marshal()
	b.ReportAllocs()
	for b.Loop() {
		var out EncryptedMessage
		if err := out.Unmarshal(d); err != nil {
			b.Fatal(err)
		}
	}
}
