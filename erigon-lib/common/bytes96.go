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

package common

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
)

var (
	blsSignatureT = reflect.TypeOf(Bytes96{})
)

type Bytes96 [length.Bytes96]byte

// Hex converts a hash to a hex string.
func (b Bytes96) Hex() string { return hexutil.Encode(b[:]) }

// UnmarshalJSON parses a hash in hex syntax.
func (b *Bytes96) UnmarshalJSON(input []byte) error {
	return hexutil.UnmarshalFixedJSON(blsSignatureT, input, b[:])
}

// UnmarshalText parses a hash in hex syntax.
func (b *Bytes96) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("BLSSignature", input, b[:])
}

// MarshalText returns the hex representation of a.
func (b Bytes96) MarshalText() ([]byte, error) {
	bl := b[:]
	result := make([]byte, len(b)*2+2)
	copy(result, hexPrefix)
	hex.Encode(result[2:], bl)
	return result, nil
}

// Format implements fmt.Formatter.
// Hash supports the %v, %s, %v, %x, %X and %d format verbs.
func (b Bytes96) Format(s fmt.State, c rune) {
	hexb := make([]byte, 2+len(b)*2)
	copy(hexb, "0x")
	hex.Encode(hexb[2:], b[:])

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
		fmt.Fprint(s, ([len(b)]byte)(b))
	default:
		fmt.Fprintf(s, "%%!%c(hash=%x)", c, b)
	}
}

// String implements the stringer interface and is used also by the logger when
// doing full logging into a file.
func (b Bytes96) String() string {
	return b.Hex()
}

// SetBytes sets the hash to the value of i.
// If b is larger than len(h), b will be cropped from the left.
func (b *Bytes96) SetBytes(i []byte) {
	if len(i) > len(b) {
		i = i[len(i)-length.Hash:]
	}

	copy(b[length.Hash-len(i):], i)
}

// Generate implements testing/quick.Generator.
func (b Bytes96) Generate(rand *rand.Rand, size int) reflect.Value {
	m := rand.Intn(len(b))
	for i := len(b) - 1; i > m; i-- {
		b[i] = byte(rand.Uint32())
	}
	return reflect.ValueOf(b)
}

// Value implements valuer for database/sql.
func (b Bytes96) Value() (driver.Value, error) {
	return b[:], nil
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes96) TerminalString() string {
	return fmt.Sprintf("%x…%x", b[:3], b[len(b)-3:])
}
