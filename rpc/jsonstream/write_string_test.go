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
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	jsoniter "github.com/json-iterator/go"
)

// parityCases covers every shape the scanner distinguishes: below one word,
// exactly one word, several words plus a tail, and an escape landing on each of
// the eight lanes so borrow contamination inside a word cannot go unnoticed.
func parityCases() []string {
	cases := []string{
		"", "a", "abc", "1234567", "12345678", "123456789",
		"0x" + strings.Repeat("ab", 32),
		`has "quotes"`, `back\slash`, "tab\there", "nl\nhere", "\x00\x01\x1f",
		strings.Repeat("clean", 40),
		"<html>&amp;", "unicode é€😀", "  ",
		strings.Repeat("\x80", 8), "abcdefgh\xff", "\xed\xa0\x80abcdefgh",
	}
	for _, dirty := range []byte{'"', '\\', 0x00, 0x0a, 0x1f} {
		for offset := range 24 {
			b := []byte(strings.Repeat("c", 24))
			b[offset] = dirty
			cases = append(cases, string(b))
		}
	}
	r := rand.New(rand.NewSource(5))
	for range 20000 {
		b := make([]byte, r.Intn(70))
		r.Read(b)
		cases = append(cases, string(b))
	}
	return cases
}

// TestWriteStringFastMatchesJsoniter pins that bulk-copying escape-free runs
// produces exactly what jsoniter's per-byte path would, including the escapes it
// deliberately does not apply (HTML characters are left alone: Erigon uses
// WriteString, not WriteStringWithHTMLEscaped).
func TestWriteStringFastMatchesJsoniter(t *testing.T) {
	for _, val := range parityCases() {
		want := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 64)
		want.WriteString(val)

		got := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 64)
		writeStringFast(got, val)

		require.Equal(t, string(want.Buffer()), string(got.Buffer()), "value %q", val)
	}
}

func TestWriteObjectFieldFastMatchesJsoniter(t *testing.T) {
	for _, name := range parityCases() {
		want := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 64)
		want.WriteObjectField(name)

		got := jsoniter.NewStream(jsoniter.ConfigDefault, nil, 64)
		writeObjectFieldFast(got, name)

		require.Equal(t, string(want.Buffer()), string(got.Buffer()), "field %q", name)
	}
}

// TestWriteStringThroughWrappers exercises the composition the parity test
// cannot see: a string written through New crosses the flush threshold, keeps
// the field/comma stack straight, and survives a value larger than the buffer.
func TestWriteStringThroughWrappers(t *testing.T) {
	t.Run("field and value stack", func(t *testing.T) {
		var out bytes.Buffer
		s := New(&out)
		s.WriteObjectStart()
		s.WriteObjectField(`odd"name`)
		s.WriteString("0x" + strings.Repeat("ab", 32))
		s.WriteMore()
		s.WriteObjectField("clean")
		s.WriteString("short")
		require.NoError(t, s.ClosePending(0))
		require.NoError(t, s.Flush())

		var decoded map[string]string
		require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
		require.Equal(t, "0x"+strings.Repeat("ab", 32), decoded[`odd"name`])
		require.Equal(t, "short", decoded["clean"])
	})

	t.Run("crosses flush threshold", func(t *testing.T) {
		var out bytes.Buffer
		s := New(&out)
		s.WriteArrayStart()
		val := strings.Repeat("c", 4096)
		n := 2*FlushThreshold/len(val) + 1
		for i := range n {
			if i > 0 {
				s.WriteMore()
			}
			s.WriteString(val)
		}
		s.WriteArrayEnd()
		require.NoError(t, s.Flush())
		require.Greater(t, out.Len(), FlushThreshold)

		var decoded []string
		require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
		require.Len(t, decoded, n)
		require.Equal(t, val, decoded[0])
	})

	t.Run("value larger than the threshold", func(t *testing.T) {
		var out bytes.Buffer
		s := New(&out)
		val := strings.Repeat("c", 3*FlushThreshold) + `"tail`
		s.WriteString(val)
		require.NoError(t, s.Flush())

		var decoded string
		require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
		require.Equal(t, val, decoded)
	})

	t.Run("failed writer drops instead of buffering", func(t *testing.T) {
		s := New(goneWriter{}).(*StackStream)
		val := strings.Repeat("c", 4096)
		for range 20 * FlushThreshold / len(val) {
			s.WriteString(val)
		}
		require.Less(t, len(s.stream.Buffer()), 2*FlushThreshold)
		require.Error(t, s.Flush(), "the failure must still be reported")
	})
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
