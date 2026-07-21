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

package common

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func fixedFormat(s fmt.State, c rune, typeName string, b []byte) {
	hexb := make([]byte, 2+len(b)*2)
	copy(hexb, "0x")
	hex.Encode(hexb[2:], b)

	switch c {
	case 'x', 'X':
		if !s.Flag('#') {
			hexb = hexb[2:]
		}
		if c == 'X' {
			hexb = bytes.ToUpper(hexb)
		}
		fallthrough
	case 'v', 's':
		s.Write(hexb)
	case 'q':
		q := []byte{'"'}
		s.Write(q)
		s.Write(hexb)
		s.Write(q)
	case 'd':
		fmt.Fprint(s, b)
	default:
		fmt.Fprintf(s, "%%!%c(%s=%x)", c, typeName, b)
	}
}

func fixedSetBytes(dst, src []byte) {
	if len(src) > len(dst) {
		src = src[len(src)-len(dst):]
	}
	copy(dst[len(dst)-len(src):], src)
}

func fixedTerminalString(b []byte) string {
	return fmt.Sprintf("%x…%x", b[:3], b[len(b)-3:])
}

// fixedGenerate fills a random-length tail of b with random bytes, the shared
// body of every fixed-size type's testing/quick Generator. intn and u32 are
// supplied by the caller so this works with both math/rand and math/rand/v2,
// whose Rand types differ only in Intn vs IntN.
func fixedGenerate(b []byte, intn func(int) int, u32 func() uint32) {
	m := intn(len(b))
	for i := len(b) - 1; i > m; i-- {
		b[i] = byte(u32())
	}
}
