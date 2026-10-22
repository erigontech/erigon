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

package jsonstream

import (
	"io"
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
)

// BenchmarkWriteString measures both paths in one binary, so the jsoniter
// sub-benchmark is the baseline the fast one is compared against:
// benchstat -col /impl pairs them per shape.
func BenchmarkWriteString(b *testing.B) {
	longClean := "0x" + strings.Repeat("ab", 2048)
	for _, tc := range []struct{ name, val string }{
		{"op3", "ADD"},
		{"op6", "SWAP16"},
		{"hex18", "0x1234567890abcdef"},
		{"storageKey66", "0x" + strings.Repeat("ab", 32)},
		{"escapeEarly", `a "quoted" value`},
		{"clean4k", longClean},
		{"escapeLate4k", longClean + `"`},
		{"quotes4k", strings.Repeat(`"`, 4096)},
		{"backslashes4k", strings.Repeat("\\", 4096)},
		{"ctrl4k", strings.Repeat("\x00", 4096)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("impl=jsoniter", func(b *testing.B) {
				benchWriteString(b, (*jsoniter.Stream).WriteString, tc.val)
			})
			b.Run("impl=fast", func(b *testing.B) {
				benchWriteString(b, writeStringFast, tc.val)
			})
		})
	}
}

func BenchmarkWriteObjectField(b *testing.B) {
	for _, tc := range []struct{ name, val string }{
		{"gas", "gas"},
		{"storageKey66", "0x" + strings.Repeat("ab", 32)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("impl=jsoniter", func(b *testing.B) {
				benchWriteString(b, (*jsoniter.Stream).WriteObjectField, tc.val)
			})
			b.Run("impl=fast", func(b *testing.B) {
				benchWriteString(b, writeObjectFieldFast, tc.val)
			})
		})
	}
}

// benchWriteString drives one write path over a fixed value, recycling the
// buffer instead of flushing so the measurement holds no io and no growth.
func benchWriteString(b *testing.B, write func(*jsoniter.Stream, string), val string) {
	b.Helper()
	s := jsoniter.NewStream(jsoniter.ConfigDefault, io.Discard, InitialBufferSize)
	b.ReportAllocs()
	for b.Loop() {
		write(s, val)
		if len(s.Buffer()) >= FlushThreshold {
			s.SetBuffer(s.Buffer()[:0])
		}
	}
}
