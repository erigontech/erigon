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
	"database/sql/driver"
	"fmt"
	"math/rand"
	"reflect"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

var (
	blsSignatureT = reflect.TypeFor[Bytes96]()
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
func (b Bytes96) MarshalText() ([]byte, error) { return hexutil.Bytes(b[:]).MarshalText() }

// Format implements fmt.Formatter.
// Hash supports the %v, %s, %v, %x, %X and %d format verbs.
func (b Bytes96) Format(s fmt.State, c rune) { fixedFormat(s, c, "hash", b[:]) }

// String implements the stringer interface and is used also by the logger when
// doing full logging into a file.
func (b Bytes96) String() string { return b.Hex() }

// SetBytes sets the hash to the value of i.
// If b is larger than len(h), b will be cropped from the left.
func (b *Bytes96) SetBytes(i []byte) { fixedSetBytes(b[:], i) }

// Generate implements testing/quick.Generator.
func (b Bytes96) Generate(rand *rand.Rand, size int) reflect.Value {
	fixedGenerate(b[:], rand.Intn, rand.Uint32)
	return reflect.ValueOf(b)
}

// Value implements valuer for database/sql.
func (b Bytes96) Value() (driver.Value, error) { return b[:], nil }

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes96) TerminalString() string { return fixedTerminalString(b[:]) }
